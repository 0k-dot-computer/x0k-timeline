//! Unified timeline channel for observability data
//!
//! Single global channel carrying `TimelineRecord` (activities + spans).
//! Replaces the separate `SPAN_CHANNEL` and `ACTIVITY_CHANNEL`.
//!
//! Flow: ObservabilityLayer → ChannelSinks → TIMELINE_CHANNEL → daemon flush task

use crate::activity::{build_activity_record, ActivityRecordSpec};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::mpsc;
use tracing::warn;
use x0k_timeline_record::record::{
    now_micros, StorageTier, TimelineRecord, STORAGE_TIER_CENTRAL_VALUE, STORAGE_TIER_KEY,
};

/// Global unified timeline channel
static TIMELINE_CHANNEL: OnceLock<TimelineChannel> = OnceLock::new();

type TimelineChannel = (
    mpsc::Sender<TimelineRecord>,
    Arc<Mutex<Option<mpsc::Receiver<TimelineRecord>>>>,
);

/// Channel capacity for timeline submissions.
///
/// Set high enough to absorb bursts when many processes forward traces
/// to the daemon (single-writer architecture). 50k × ~500 bytes ≈ 25 MB.
const TIMELINE_CHANNEL_CAPACITY: usize = 50_000;

static TIMELINE_DROP_STATS: OnceLock<Mutex<HashMap<DropKey, DropStats>>> = OnceLock::new();
static TIMELINE_DROP_REPORTER: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DropKey {
    record_kind: &'static str,
    stage: &'static str,
    reason: &'static str,
}

#[derive(Clone, Copy, Debug, Default)]
struct DropStats {
    total: u64,
    pending: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DropSummary {
    record_kind: &'static str,
    stage: &'static str,
    reason: &'static str,
    count: u64,
    total: u64,
}

/// Take the timeline receiver from the global channel.
///
/// Returns `None` if already taken or not yet initialized.
/// The daemon calls this once to get the receiver for its unified flush task.
pub fn take_timeline_receiver() -> Option<mpsc::Receiver<TimelineRecord>> {
    let (_, receiver_holder) = TIMELINE_CHANNEL.get_or_init(|| {
        let (tx, rx) = mpsc::channel(TIMELINE_CHANNEL_CAPACITY);
        (tx, Arc::new(Mutex::new(Some(rx))))
    });

    receiver_holder.lock().ok()?.take()
}

/// Get the timeline sender (creates channel if needed)
pub fn get_timeline_sender() -> &'static mpsc::Sender<TimelineRecord> {
    let (sender, _) = TIMELINE_CHANNEL.get_or_init(|| {
        let (tx, rx) = mpsc::channel(TIMELINE_CHANNEL_CAPACITY);
        (tx, Arc::new(Mutex::new(Some(rx))))
    });
    sender
}

fn drop_stats() -> &'static Mutex<HashMap<DropKey, DropStats>> {
    TIMELINE_DROP_STATS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn record_kind(record: &TimelineRecord) -> &'static str {
    match record {
        TimelineRecord::Activity(_, _) => "activity",
        TimelineRecord::Span(_) => "span",
        TimelineRecord::LogEvent(_) => "log",
        TimelineRecord::Conversation(_) => "conversation",
    }
}

fn drain_drop_summaries() -> Vec<DropSummary> {
    let Ok(mut guard) = drop_stats().lock() else {
        return Vec::new();
    };

    let mut summaries = Vec::new();
    for (key, stats) in guard.iter_mut() {
        if stats.pending == 0 {
            continue;
        }
        summaries.push(DropSummary {
            record_kind: key.record_kind,
            stage: key.stage,
            reason: key.reason,
            count: stats.pending,
            total: stats.total,
        });
        stats.pending = 0;
    }
    summaries
}

fn build_drop_summary_record(summary: &DropSummary) -> TimelineRecord {
    let mut metadata = HashMap::new();
    metadata.insert(
        STORAGE_TIER_KEY.to_string(),
        STORAGE_TIER_CENTRAL_VALUE.to_string(),
    );
    metadata.insert(
        "drop.record_kind".to_string(),
        summary.record_kind.to_string(),
    );
    metadata.insert("drop.stage".to_string(), summary.stage.to_string());
    metadata.insert("drop.reason".to_string(), summary.reason.to_string());
    metadata.insert("drop.count".to_string(), summary.count.to_string());
    metadata.insert("drop.total".to_string(), summary.total.to_string());

    let now = now_micros();
    let description = format!(
        "Dropped {} {} timeline records at {} ({})",
        summary.count, summary.record_kind, summary.stage, summary.reason
    );
    build_activity_record(ActivityRecordSpec {
        activity_type: "system.timeline.records_dropped",
        activity_data: Some(description),
        metadata,
        storage_tier: StorageTier::Central,
        correlation_id: None,
        parent_activity_id: None,
        started_at_us: Some(now),
        ended_at_us: Some(now),
        trace_id: None,
        activity_id: None,
        span_id: None,
        parent_span_id: None,
        source_file: None,
        source_line: None,
        source_function: None,
        source_module: crate::software_module_id(),
        span_name: Some("timeline_drop_report"),
    })
}

fn emit_drop_summary(summary: DropSummary) {
    let record = build_drop_summary_record(&summary);
    if let Err(error) = get_timeline_sender().blocking_send(record) {
        warn!(%error, "Failed to emit timeline drop summary");
    }
}

pub fn spawn_drop_reporter_if_needed() {
    TIMELINE_DROP_REPORTER.get_or_init(|| {
        std::thread::Builder::new()
            .name("timeline-drop-reporter".into())
            .spawn(|| loop {
                std::thread::sleep(std::time::Duration::from_secs(2));
                for summary in drain_drop_summaries() {
                    emit_drop_summary(summary);
                }
            })
            .expect("Failed to spawn timeline drop reporter");
    });
}

pub fn record_timeline_drop(
    record_kind: &'static str,
    stage: &'static str,
    reason: &'static str,
    count: u64,
) {
    // Lazily start the reporter on the first drop — nothing else spawns it
    // (2026-07-05: it was defined but never called anywhere, so every drop
    // was tallied into stats that nothing drained; the timeline silently
    // lost records with zero `system.timeline.records_dropped` accounting,
    // which mis-diagnosed two debugging sessions).
    spawn_drop_reporter_if_needed();
    let Ok(mut guard) = drop_stats().lock() else {
        return;
    };
    let stats = guard
        .entry(DropKey {
            record_kind,
            stage,
            reason,
        })
        .or_default();
    stats.total = stats.total.saturating_add(count);
    stats.pending = stats.pending.saturating_add(count);
}

pub fn record_timeline_drop_for_record(
    record: &TimelineRecord,
    stage: &'static str,
    reason: &'static str,
) {
    record_timeline_drop(record_kind(record), stage, reason, 1);
}

// The error is tokio's, and it hands the record back to the caller, so its
// size is the record's.
#[allow(clippy::result_large_err)]
pub fn try_send_record(
    sender: &mpsc::Sender<TimelineRecord>,
    record: TimelineRecord,
    stage: &'static str,
) -> Result<(), mpsc::error::TrySendError<TimelineRecord>> {
    match sender.try_send(record) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(record)) => {
            record_timeline_drop_for_record(&record, stage, "full");
            Err(mpsc::error::TrySendError::Full(record))
        }
        Err(mpsc::error::TrySendError::Closed(record)) => {
            record_timeline_drop_for_record(&record, stage, "closed");
            Err(mpsc::error::TrySendError::Closed(record))
        }
    }
}

#[allow(clippy::result_large_err)]
pub fn try_send_global(
    record: TimelineRecord,
    stage: &'static str,
) -> Result<(), mpsc::error::TrySendError<TimelineRecord>> {
    try_send_record(get_timeline_sender(), record, stage)
}

pub fn send_global_record(record: TimelineRecord, stage: &'static str) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let sender = get_timeline_sender().clone();
        handle.spawn(async move {
            if let Err(error) = sender.send(record).await {
                record_timeline_drop_for_record(&error.0, stage, "closed");
            }
        });
    } else if let Err(error) = get_timeline_sender().blocking_send(record) {
        record_timeline_drop_for_record(&error.0, stage, "closed");
    }
}

#[cfg(test)]
pub(crate) fn drain_drop_summaries_for_test() -> Vec<(String, String, String, u64, u64)> {
    let summaries = drain_drop_summaries();
    let Ok(mut guard) = drop_stats().lock() else {
        return summaries
            .into_iter()
            .map(|summary| {
                (
                    summary.record_kind.to_string(),
                    summary.stage.to_string(),
                    summary.reason.to_string(),
                    summary.count,
                    summary.total,
                )
            })
            .collect();
    };
    guard.clear();
    summaries
        .into_iter()
        .map(|summary| {
            (
                summary.record_kind.to_string(),
                summary.stage.to_string(),
                summary.reason.to_string(),
                summary.count,
                summary.total,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use x0k_timeline_record::record::LogEventData;

    fn test_log_event(id: &str) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData {
            id: id.to_string(),
            timestamp_us: 1,
            level: "info".to_string(),
            message: Some("test".to_string()),
            fields_json: None,
            source_file: None,
            source_line: None,
            module: None,
            target: "test".to_string(),
            software_module_id: None,
            workstream_id: None,
            intent_id: None,
            session_id: None,
            pid: None,
            trace_id: None,
            traceparent: None,
            tracestate: None,
            request_id: None,
            parent_request_id: None,
            role: None,
            mode: None,
            origin_system: None,
            origin_subsystem: None,
            agent_profile: None,
            sandbox_id: None,
            invocation_method: None,
            executed_by_json: None,
            authorization_chain_json: None,
            authorization_proof_refs_json: None,
            artifact_id: None,
            native_artifact_id: None,
            document_id: None,
            document_revision_id: None,
            file_path: None,
            event_type: Some("test.log".to_string()),
        })
    }

    #[test]
    fn test_try_send_records_drop_when_channel_is_full() {
        let _ = drain_drop_summaries_for_test();

        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(test_log_event("first")).unwrap();

        let err = try_send_record(&tx, test_log_event("second"), "unit_test");
        assert!(matches!(err, Err(mpsc::error::TrySendError::Full(_))));

        // The counters are process-wide and the crash-queue tests record drops
        // concurrently; only this test's stage is its own.
        let summaries: Vec<_> = drain_drop_summaries_for_test()
            .into_iter()
            .filter(|summary| summary.1 == "unit_test")
            .collect();
        assert_eq!(
            summaries,
            vec![(
                "log".to_string(),
                "unit_test".to_string(),
                "full".to_string(),
                1,
                1,
            )]
        );
    }

    #[test]
    fn test_build_drop_summary_record_uses_queryable_activity_shape() {
        let record = build_drop_summary_record(&DropSummary {
            record_kind: "log",
            stage: "unit_test",
            reason: "full",
            count: 3,
            total: 7,
        });

        let TimelineRecord::Activity(activity, tier) = record else {
            panic!("drop summary should emit an activity record");
        };

        assert_eq!(tier, StorageTier::Central);
        assert_eq!(
            activity.context.activity_type,
            "system.timeline.records_dropped"
        );
        assert_eq!(
            activity
                .context
                .metadata
                .get(STORAGE_TIER_KEY)
                .map(String::as_str),
            Some(STORAGE_TIER_CENTRAL_VALUE)
        );
        assert_eq!(
            activity
                .context
                .metadata
                .get("drop.record_kind")
                .map(String::as_str),
            Some("log")
        );
        assert_eq!(
            activity
                .context
                .metadata
                .get("drop.stage")
                .map(String::as_str),
            Some("unit_test")
        );
        assert_eq!(
            activity
                .context
                .metadata
                .get("drop.count")
                .map(String::as_str),
            Some("3")
        );
        assert!(activity
            .context
            .activity_data
            .as_deref()
            .is_some_and(|description| description.contains("Dropped 3 log timeline records")));
    }
}
