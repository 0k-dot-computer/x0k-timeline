//! The timeline's emit role (`x0k:architecture/observability-timeline` §4):
//! what a process links to put its spans, activities and log events on the
//! timeline.
//!
//! - [`observability`]: the tracing layer that captures spans and activities.
//! - [`span_tracing`]: span filtering, sinks, the trace override sync and, with
//!   the `auto-persist` feature, forwarding to the timeline writer with the
//!   crash-durable segment as fallback.
//! - [`activity`]: activity records and their sinks.
//! - [`timeline_channel`]: the in-process channel records travel on.
//! - [`trace_context`]: W3C trace context carried across process boundaries.
//!
//! The crate reads no configuration, path or profile. Its embedder (x0k-env's
//! `init_logging`) builds the filter state, names the `TimelineEndpoint`
//! and sets the [`software_module_id`].

use std::sync::OnceLock;

pub mod activity;
pub mod observability;
pub mod span_tracing;
pub mod timeline_channel;
pub mod trace_context;

static SOFTWARE_MODULE_ID: OnceLock<String> = OnceLock::new();

/// Set the software module id records are attributed to. The first call wins:
/// a process has one module id for its lifetime.
pub fn set_software_module_id(module_id: String) {
    let _ = SOFTWARE_MODULE_ID.set(module_id);
}

/// The software module id for the current process, once its embedder set it.
pub fn software_module_id() -> Option<&'static str> {
    SOFTWARE_MODULE_ID.get().map(String::as_str)
}
