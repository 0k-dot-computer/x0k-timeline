//! Channel-based activity sink for daemon consumption
//!
//! Activities flow: ObservabilityLayer -> ChannelActivitySink -> TIMELINE_CHANNEL -> daemon flush task
//!
//! Activities are routed to the unified timeline channel as `TimelineRecord::Activity`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;
use x0k_timeline_record::record::{
    route_activity_storage_tier, ActivityOperation, ActivitySink, CompletedActivity, TimelineRecord,
};

// Legacy activity channel — kept for backwards compat during transition.
// New code should use take_timeline_receiver() instead.
static ACTIVITY_CHANNEL: OnceLock<ActivityChannel> = OnceLock::new();

type ActivityChannel = (
    mpsc::Sender<ActivityOperation>,
    Arc<std::sync::Mutex<Option<mpsc::Receiver<ActivityOperation>>>>,
);

const ACTIVITY_CHANNEL_CAPACITY: usize = 10_000;

/// Take the legacy activity receiver from the global channel.
///
/// Returns `None` if already taken or not yet initialized.
/// Kept for backwards compat — prefer `take_timeline_receiver()`.
pub fn take_activity_receiver() -> Option<mpsc::Receiver<ActivityOperation>> {
    let (_, receiver_holder) = ACTIVITY_CHANNEL.get_or_init(|| {
        let (tx, rx) = mpsc::channel(ACTIVITY_CHANNEL_CAPACITY);
        (tx, Arc::new(std::sync::Mutex::new(Some(rx))))
    });

    receiver_holder.lock().ok()?.take()
}

/// Get the legacy activity sender (creates channel if needed)
fn get_activity_sender() -> &'static mpsc::Sender<ActivityOperation> {
    let (sender, _) = ACTIVITY_CHANNEL.get_or_init(|| {
        let (tx, rx) = mpsc::channel(ACTIVITY_CHANNEL_CAPACITY);
        (tx, Arc::new(std::sync::Mutex::new(Some(rx))))
    });
    sender
}

enum ActivityDelivery {
    Record(CompletedActivity),
    Update(CompletedActivity),
}

/// Channel-based sink that queues activities to the unified timeline channel.
///
/// Activities use an internal unbounded queue and a dedicated forwarding thread
/// so they can block on the global timeline channel without blocking tracing
/// callbacks. This gives activities a reliable path into the timeline channel.
#[derive(Debug)]
pub struct ChannelActivitySink {
    dropped_count: AtomicU64,
    tx: std::sync::mpsc::Sender<ActivityDelivery>,
}

impl Default for ChannelActivitySink {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelActivitySink {
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<ActivityDelivery>();
        std::thread::Builder::new()
            .name("timeline-activity-forwarder".into())
            .spawn(move || {
                let timeline_sender = crate::timeline_channel::get_timeline_sender().clone();
                let legacy_sender = get_activity_sender().clone();

                while let Ok(delivery) = rx.recv() {
                    let (activity, is_update) = match delivery {
                        ActivityDelivery::Record(activity) => (activity, false),
                        ActivityDelivery::Update(activity) => (activity, true),
                    };

                    let tier = route_activity_storage_tier(
                        &activity.context.activity_type,
                        &activity.context.metadata,
                    );
                    let record = TimelineRecord::Activity(activity.clone(), tier);
                    if let Err(error) = timeline_sender.blocking_send(record) {
                        crate::timeline_channel::record_timeline_drop(
                            "activity",
                            "activity_forwarder",
                            "closed",
                            1,
                        );
                        tracing::warn!(%error, "Failed to forward activity to timeline channel");
                    }

                    let op = if is_update {
                        ActivityOperation::Update(activity)
                    } else {
                        ActivityOperation::Record(activity)
                    };
                    let _ = legacy_sender.try_send(op);
                }
            })
            .expect("Failed to spawn timeline activity forwarder");

        Self {
            dropped_count: AtomicU64::new(0),
            tx,
        }
    }

    /// Get count of activities that could not be enqueued for forwarding.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    fn send_activity(&self, activity: CompletedActivity, is_update: bool) {
        let delivery = if is_update {
            ActivityDelivery::Update(activity)
        } else {
            ActivityDelivery::Record(activity)
        };

        if self.tx.send(delivery).is_err() {
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
            crate::timeline_channel::record_timeline_drop(
                "activity",
                "activity_enqueue",
                "closed",
                1,
            );
        }
    }
}

impl ActivitySink for ChannelActivitySink {
    fn record_activity(&self, activity: CompletedActivity) {
        self.send_activity(activity, false);
    }

    fn update_activity(&self, activity: CompletedActivity) {
        self.send_activity(activity, true);
    }
}
