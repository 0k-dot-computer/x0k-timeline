//! Span capture, filtering and forwarding for the timeline.
//!
//! The layer captures every `#[instrument]` span that the [`SpanFilterState`]
//! admits and hands it to the process's [`SpanSink`]. The filter state comes
//! from the embedder: x0k-env builds it from config and passes the same
//! builder to the override sync, so this crate reads no config of its own.
//!
//! # Auto-Persist Feature
//!
//! With the `auto-persist` feature, `run_timeline_forwarder_task` forwards
//! the process's records to the timeline writer at a `TimelineEndpoint` and
//! buffers them in a crash-durable segment while the writer is unreachable.
//!
//! The timeline writer opts out - it sets `X0K_SPAN_DAEMON_MODE=1` and manages
//! timeline consumption directly via `x0k_activity_sink::run_timeline_flush_task()`.

mod config;
mod layer;
mod sink;

#[cfg(feature = "auto-persist")]
mod auto_persist;

#[cfg(feature = "auto-persist")]
mod crash_queue;

#[cfg(feature = "auto-persist")]
pub mod override_sync;

pub use config::{
    known_modules, parse_level, KnownModule, ModuleRule, SpanFilterState, SpanTracingConfig,
};
pub use layer::{get_span_sink, set_span_sink, SpanContext, SpanEvent};
pub use sink::{ChannelSpanSink, NoopSpanSink, SpanSink};

#[cfg(feature = "auto-persist")]
pub use auto_persist::{is_daemon_mode, run_timeline_forwarder_task, TimelineEndpoint};

/// Disk-backed postcard crash-queue fallback (no columnar deps). Exposed for the
/// fresh-injection oracle integration test; the production entry point is
/// [`run_timeline_forwarder_task`], which routes here on daemon-unreachable.
#[cfg(feature = "auto-persist")]
#[doc(hidden)]
pub use crash_queue::{
    crash_queue_path_for_test, read_panic_records, run_crash_queue_fallback, write_panic_record,
};

#[cfg(feature = "auto-persist")]
pub use override_sync::spawn_override_sync_task;
