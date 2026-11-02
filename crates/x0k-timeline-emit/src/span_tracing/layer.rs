//! Span tracing types and global sink management
//!
//! Provides the core types (`SpanContext`, `SpanEvent`) used by the unified
//! `ObservabilityLayer`, plus the global span sink registration.
//!
//! The `SpanTracingLayer` that previously lived here has been replaced by
//! `crate::observability::ObservabilityLayer`.

use super::sink::SpanSink;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};

/// Global span sink that can be set after logging initialization
///
/// This allows the sink to be created by a crate that depends on both
/// x0k-env and the sink implementation (e.g., x0k-timeline-sink).
static GLOBAL_SPAN_SINK: OnceLock<Arc<dyn SpanSink>> = OnceLock::new();

/// Set the global span sink for persistence
///
/// This should be called once after logging initialization by a crate
/// that has access to the sink implementation.
pub fn set_span_sink(sink: Arc<dyn SpanSink>) {
    if GLOBAL_SPAN_SINK.set(sink).is_err() {
        tracing::warn!("Span sink already set, ignoring duplicate call");
    }
}

/// Get the global span sink (if set)
pub fn get_span_sink() -> Option<&'static Arc<dyn SpanSink>> {
    GLOBAL_SPAN_SINK.get()
}

/// Context for an in-progress span
#[derive(Debug, Clone, Default)]
pub struct SpanContext {
    /// Unique span ID (16-char hex)
    pub span_id: String,

    /// Trace ID inherited from parent (32-char hex)
    pub trace_id: String,

    /// Parent span ID (if this is a child span)
    pub parent_span_id: Option<String>,

    /// Span start timestamp (milliseconds since Unix epoch)
    pub start_time: u64,

    /// Span duration (filled on close)
    pub duration_ms: Option<u64>,

    /// Tracing level (info, debug, trace, warn, error)
    pub level: String,

    /// Function name (from `#[instrument]` or span name)
    pub function: Option<String>,

    /// Module path (e.g., "x0k_core::sync")
    pub module: Option<String>,

    /// Source file path
    pub source_file: Option<String>,

    /// Source line number
    pub source_line: Option<u32>,

    /// Span name from tracing metadata
    pub span_name: String,

    /// Target (typically the module path)
    pub target: String,

    /// Fields recorded on the span
    pub fields: std::collections::HashMap<String, serde_json::Value>,

    /// Events attached to this span (only populated when buffer_events is true)
    pub events: Vec<SpanEvent>,

    /// Workstream ID (from environment/context)
    pub workstream_id: Option<String>,

    /// Task ID (from environment/context)
    pub intent_id: Option<String>,

    /// Software module ID (from daemon/binary name)
    /// Used for timeline visibility and activity tracking
    pub software_module_id: Option<String>,

    /// Logical session ID (from X0K_SESSION_ID env var)
    /// Links spans to a specific agent session; NULL for non-agent processes
    pub session_id: Option<String>,

    /// Process ID of the emitting process
    pub pid: Option<u32>,
}

/// An event attached to a span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event timestamp (milliseconds since Unix epoch)
    pub timestamp: u64,

    /// Event level
    pub level: String,

    /// Event message
    pub message: Option<String>,

    /// Event fields
    pub fields: std::collections::HashMap<String, serde_json::Value>,

    /// Source file
    pub source_file: Option<String>,

    /// Source line
    pub source_line: Option<u32>,
}
