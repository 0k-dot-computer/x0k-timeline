//! Activity sink implementations
//!
//! Provides `ParquetActivitySink` for routing activities to unified Parquet timeline
//! storage via an mpsc channel to a flush task.

use crate::routing::route_activity;
use crate::types::{ActivitySink, CompletedActivity, TimelineRecord};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error};

/// Parquet-based activity sink
///
/// Routes activities to the unified timeline flush task via an mpsc channel.
/// The flush task handles batching and partitioning by storage tier.
pub struct ParquetActivitySink {
    /// Channel to the unified timeline flush task
    sender: mpsc::Sender<TimelineRecord>,
    /// Master switch — when false, all writes are skipped
    enabled: Arc<AtomicBool>,
}

impl ParquetActivitySink {
    /// Create a new Parquet activity sink
    pub fn new(sender: mpsc::Sender<TimelineRecord>, enabled: Arc<AtomicBool>) -> Self {
        Self { sender, enabled }
    }

    /// Get a clone of the sender for sending spans directly
    pub fn sender(&self) -> mpsc::Sender<TimelineRecord> {
        self.sender.clone()
    }
}

impl ActivitySink for ParquetActivitySink {
    fn record_activity(&self, activity: CompletedActivity) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let tier = route_activity(&activity.context.activity_type, &activity.context.metadata);

        debug!(
            activity_id = %activity.context.activity_id.0,
            activity_type = %activity.context.activity_type,
            tier = ?tier,
            "Recording activity to Parquet timeline"
        );

        let record = TimelineRecord::Activity(activity, tier);
        if let Err(e) = self.sender.try_send(record) {
            error!(error = %e, "Failed to send activity to timeline channel");
        }
    }

    fn update_activity(&self, activity: CompletedActivity) {
        self.record_activity(activity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::StorageTier;
    use crate::types::ActivityContext;
    use std::collections::HashMap;
    use x0k_timeline_record::ActivityId;

    fn create_test_activity(activity_type: &str) -> CompletedActivity {
        CompletedActivity {
            context: ActivityContext {
                activity_id: ActivityId::new(),
                activity_type: activity_type.to_string(),
                parent_id: None,
                actor: None,
                started_at: 1704067200000,
                metadata: HashMap::new(),
                typed_metadata_keys: Default::default(),
                correlation_id: None,
                trace_id: None,
                span_id: None,
                parent_span_id: None,
                activity_data: None,
                lazy_data: None,
                source_file: None,
                source_line: None,
                source_function: None,
                source_module: None,
                span_name: None,
            },
            ended_at: 1704067201000,
            duration_ms: 1000,
        }
    }

    #[tokio::test]
    async fn test_parquet_sink_sends_to_channel() {
        let (tx, mut rx) = mpsc::channel(16);
        let enabled = Arc::new(AtomicBool::new(true));
        let sink = ParquetActivitySink::new(tx, enabled);

        let activity = create_test_activity("agent.spawned");
        sink.record_activity(activity);

        let record = rx.try_recv().unwrap();
        match record {
            TimelineRecord::Activity(a, tier) => {
                assert_eq!(a.context.activity_type, "agent.spawned");
                assert!(matches!(tier, StorageTier::Central));
            }
            _ => panic!("Expected Activity record"),
        }
    }

    #[tokio::test]
    async fn test_parquet_sink_disabled() {
        let (tx, mut rx) = mpsc::channel(16);
        let enabled = Arc::new(AtomicBool::new(false));
        let sink = ParquetActivitySink::new(tx, enabled);

        let activity = create_test_activity("agent.spawned");
        sink.record_activity(activity);

        assert!(rx.try_recv().is_err(), "Should not send when disabled");
    }
}
