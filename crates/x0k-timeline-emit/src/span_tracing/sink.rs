//! Sink trait for writing span trace data to storage
//!
//! The sink abstracts the storage backend, allowing the layer to write
//! cause-linked artifacts without knowing the underlying implementation.
//!
//! # Channel Architecture
//!
//! Spans flow: ObservabilityLayer -> ChannelSpanSink -> TIMELINE_CHANNEL -> flush task
//!
//! The channel uses `TimelineRecord::Span(SpanData)` so both spans and activities
//! share the same unified channel consumed by a single daemon flush task.

use super::SpanContext;
use std::sync::atomic::{AtomicU64, Ordering};
use x0k_timeline_record::record::TimelineRecord;
use x0k_timeline_record::span::SpanData;

/// Convert SpanContext (internal) to SpanData (x0k-types)
fn span_context_to_data(ctx: &SpanContext) -> SpanData {
    use x0k_timeline_record::JsonValue;
    use x0k_timeline_record::span::SpanEventData;

    let fields = ctx
        .fields
        .iter()
        .filter_map(|(k, v)| {
            JsonValue::from_value(v.clone())
                .ok()
                .map(|jv| (k.clone(), jv))
        })
        .collect();

    let events = ctx
        .events
        .iter()
        .map(|e| {
            let event_fields = e
                .fields
                .iter()
                .filter_map(|(k, v)| {
                    JsonValue::from_value(v.clone())
                        .ok()
                        .map(|jv| (k.clone(), jv))
                })
                .collect();

            SpanEventData {
                timestamp: e.timestamp,
                level: e.level.clone(),
                message: e.message.clone(),
                fields: event_fields,
                source_file: e.source_file.clone(),
                source_line: e.source_line,
            }
        })
        .collect();

    SpanData {
        span_id: ctx.span_id.clone(),
        trace_id: ctx.trace_id.clone(),
        parent_span_id: ctx.parent_span_id.clone(),
        start_time: ctx.start_time,
        duration_ms: ctx.duration_ms,
        level: ctx.level.clone(),
        function: ctx.function.clone(),
        module: ctx.module.clone(),
        source_file: ctx.source_file.clone(),
        source_line: ctx.source_line,
        span_name: ctx.span_name.clone(),
        target: ctx.target.clone(),
        fields,
        events,
        workstream_id: ctx.workstream_id.clone(),
        intent_id: ctx.intent_id.clone(),
        software_module_id: ctx.software_module_id.clone(),
        session_id: ctx.session_id.clone(),
        pid: ctx.pid,
    }
}

/// Trait for sinks that receive completed span data
///
/// This is the internal trait used by the ObservabilityLayer. It takes SpanContext
/// which is the internal representation during span lifetime.
pub trait SpanSink: Send + Sync {
    fn write_span(&self, span: &SpanContext);
    fn get_latest_cause(&self, entity: &str, attribute: &str) -> Option<String>;
    fn flush(&self);
}

/// Channel-based sink that sends spans to the unified timeline channel.
///
/// Converts SpanContext to SpanData, wraps in TimelineRecord::Span, and sends
/// to the global timeline channel consumed by the daemon's flush task.
#[derive(Debug, Default)]
pub struct ChannelSpanSink {
    dropped_count: AtomicU64,
}

impl ChannelSpanSink {
    pub fn new() -> Self {
        Self {
            dropped_count: AtomicU64::new(0),
        }
    }

    /// Get count of dropped spans (channel was full)
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }
}

impl SpanSink for ChannelSpanSink {
    fn write_span(&self, span: &SpanContext) {
        let span_data = span_context_to_data(span);

        // Send to unified timeline channel (for daemon consumption)
        if crate::timeline_channel::try_send_global(TimelineRecord::Span(span_data), "span_sink")
            .is_err()
        {
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_latest_cause(&self, _entity: &str, _attribute: &str) -> Option<String> {
        None
    }

    fn flush(&self) {}
}

/// No-op sink for testing or when span tracing is disabled
#[derive(Debug, Default)]
pub struct NoopSpanSink;

impl SpanSink for NoopSpanSink {
    fn write_span(&self, _span: &SpanContext) {}
    fn get_latest_cause(&self, _entity: &str, _attribute: &str) -> Option<String> {
        None
    }
    fn flush(&self) {}
}
