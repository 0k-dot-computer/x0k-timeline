//! Building one activity record and putting it on the timeline.

use crate::timeline_channel;
use std::collections::HashMap;
use x0k_timeline_record::record::{
    now_micros, ActivityContext, CompletedActivity, StorageTier, TimelineRecord,
};
use x0k_timeline_record::ActivityId;

#[derive(Debug, Clone)]
pub struct ActivityRecordSpec<'a> {
    pub activity_type: &'a str,
    pub activity_data: Option<String>,
    pub metadata: HashMap<String, String>,
    pub storage_tier: StorageTier,
    pub correlation_id: Option<String>,
    pub parent_activity_id: Option<ActivityId>,
    pub started_at_us: Option<u64>,
    pub ended_at_us: Option<u64>,
    pub trace_id: Option<String>,
    pub activity_id: Option<ActivityId>,
    pub span_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub source_file: Option<&'a str>,
    pub source_line: Option<u32>,
    pub source_function: Option<&'a str>,
    pub source_module: Option<&'a str>,
    pub span_name: Option<&'a str>,
}

pub fn build_activity_record(mut spec: ActivityRecordSpec<'_>) -> TimelineRecord {
    let now = now_micros();
    let started_at = spec.started_at_us.unwrap_or(now);
    let ended_at = spec.ended_at_us.unwrap_or(now);
    let duration_ms = ended_at.saturating_sub(started_at) / 1_000;

    if let Some(module_id) = crate::software_module_id() {
        spec.metadata
            .entry("activity.software_module".to_string())
            .or_insert_with(|| module_id.to_string());
    }
    // The writer fills the pid column from this, falling back to its own pid,
    // so a record forwarded from another process names its emitter.
    spec.metadata
        .entry("pid".to_string())
        .or_insert_with(|| std::process::id().to_string());

    let activity = CompletedActivity {
        context: ActivityContext {
            activity_id: spec.activity_id.unwrap_or_default(),
            activity_type: spec.activity_type.to_string(),
            parent_id: spec.parent_activity_id,
            actor: None,
            started_at,
            metadata: spec.metadata,
            typed_metadata_keys: Default::default(),
            correlation_id: spec.correlation_id,
            trace_id: spec.trace_id,
            span_id: spec.span_id,
            parent_span_id: spec.parent_span_id,
            activity_data: spec.activity_data,
            lazy_data: None,
            source_file: spec.source_file.map(ToOwned::to_owned),
            source_line: spec.source_line,
            source_function: spec.source_function.map(ToOwned::to_owned),
            source_module: spec
                .source_module
                .map(ToOwned::to_owned)
                .or_else(|| crate::software_module_id().map(ToOwned::to_owned)),
            span_name: spec.span_name.map(ToOwned::to_owned),
        },
        ended_at,
        duration_ms,
    };

    TimelineRecord::Activity(activity, spec.storage_tier)
}

pub fn emit_activity_record(spec: ActivityRecordSpec<'_>) -> String {
    let record = build_activity_record(spec);
    let activity_id = match &record {
        TimelineRecord::Activity(activity, _) => activity.context.activity_id.0.clone(),
        _ => unreachable!("activity record helper only emits activities"),
    };
    timeline_channel::send_global_record(record, "activity_record");
    activity_id
}
