//! Task-local trace context storage and propagation
//!
//! This module provides async-aware storage for trace context,
//! enabling automatic propagation through async task hierarchies.
//!
//! # Usage
//!
//! ```rust,ignore
//! use x0k_env::trace_context;
//! use x0k_timeline_record::trace_context::TraceContext;
//!
//! // Run code with a specific trace context
//! let ctx = TraceContext::new_root();
//! trace_context::with_context(ctx.clone(), async {
//!     // All nested async calls can access the context
//!     let current = trace_context::current();
//!     assert!(current.is_some());
//! }).await;
//!
//! // Create a child span for making an IPC call
//! let child = trace_context::child_span();
//! ```

use std::cell::RefCell;
use x0k_timeline_record::trace_context::{TraceBaggage, TraceContext};

tokio::task_local! {
    /// Current trace context for this async task
    static CURRENT_TRACE: RefCell<Option<TraceContext>>;
}

/// Get the current trace context (if set)
///
/// Returns `None` if no trace context has been set for this task,
/// or if called from outside an async task.
pub fn current() -> Option<TraceContext> {
    CURRENT_TRACE
        .try_with(|ctx| ctx.borrow().clone())
        .ok()
        .flatten()
}

/// Run a future with a specific trace context
///
/// The provided context will be accessible via [`current()`] within
/// the future and all nested async calls.
///
/// # Example
///
/// ```rust,ignore
/// let ctx = TraceContext::new_root();
/// trace_context::with_context(ctx, async {
///     // This code can access the trace context
///     let current = trace_context::current().unwrap();
///     // Make IPC calls, they will inherit this context
/// }).await;
/// ```
pub async fn with_context<F, T>(ctx: TraceContext, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CURRENT_TRACE.scope(RefCell::new(Some(ctx)), f).await
}

/// The trace and parent span a W3C `traceparent` names, as hex: a span
/// opened with it in a `traceparent` field joins that trace under that span
/// (`observability`). `None` for a value that does not parse.
pub fn remote_parent(traceparent: &str) -> Option<(String, Option<String>)> {
    let heard = TraceContext::from_traceparent(traceparent).ok()?;
    Some((heard.trace_id.to_hex(), heard.parent_span_id.map(|parent| parent.to_hex())))
}

/// Create a child span from current context (or new root if none)
///
/// This is the primary way to create a new span when making an
/// outgoing IPC call or starting a new unit of work.
///
/// - If there's a current trace context, creates a child span that
///   inherits the trace_id and baggage
/// - If there's no current context, creates a new root trace
pub fn child_span() -> TraceContext {
    current()
        .map(|ctx| ctx.child())
        .unwrap_or_else(TraceContext::new_root)
}

/// Read trace context from environment variables
///
/// Parses the `TRACEPARENT` environment variable (W3C format) and
/// optionally reads baggage from `TRACESTATE` and x0k-specific
/// environment variables.
///
/// This is typically called at process startup to continue a trace
/// that was propagated from a parent process.
///
/// # Environment Variables
///
/// - `TRACEPARENT`: W3C traceparent header format
/// - `TRACESTATE`: W3C tracestate header with x0k baggage
/// - `X0K_SESSION_ID`: Session ID (fallback if not in TRACESTATE)
/// - `X0K_WORKSTREAM_ID`: Workstream ID (fallback)
/// - `X0K_INTENT_ID`: Intent ID (fallback, with GROVE_TASK_ID for backwards compat)
pub fn from_env() -> TraceContext {
    // Try to parse TRACEPARENT
    if let Ok(traceparent) = std::env::var("TRACEPARENT") {
        if let Ok(mut ctx) = TraceContext::from_traceparent(&traceparent) {
            // Parse TRACESTATE if available
            if let Ok(tracestate) = std::env::var("TRACESTATE") {
                ctx.parse_tracestate(&tracestate);
            }

            // Also check individual environment variables as fallbacks
            populate_baggage_from_env(&mut ctx.baggage);

            // Create a new span for this process (inheriting trace_id)
            return ctx;
        }
    }

    // No valid parent context - create a new root
    let mut ctx = TraceContext::new_root();
    populate_baggage_from_env(&mut ctx.baggage);
    ctx
}

/// Populate baggage from individual environment variables
fn populate_baggage_from_env(baggage: &mut TraceBaggage) {
    if baggage.session_id.is_none() {
        if let Ok(session) = std::env::var("X0K_SESSION_ID") {
            baggage.session_id = Some(session);
        }
    }
    if baggage.workstream_id.is_none() {
        if let Ok(workstream) = std::env::var("X0K_WORKSTREAM_ID") {
            baggage.workstream_id = Some(workstream);
        }
    }
    if baggage.intent_id.is_none() {
        // Check X0K_INTENT_ID first, fall back to GROVE_TASK_ID for backwards compat
        if let Ok(intent) =
            std::env::var("X0K_INTENT_ID").or_else(|_| std::env::var("GROVE_TASK_ID"))
        {
            baggage.intent_id = Some(intent);
        }
    }
}

/// Export trace context to environment variables for spawning child processes
///
/// Returns a list of (key, value) pairs that should be set in the
/// child process's environment.
///
/// # Example
///
/// ```rust,ignore
/// let ctx = trace_context::child_span();
/// let env_vars = trace_context::to_env_vars(&ctx);
///
/// let mut cmd = std::process::Command::new("child-process");
/// for (key, value) in env_vars {
///     cmd.env(key, value);
/// }
/// ```
pub fn to_env_vars(ctx: &TraceContext) -> Vec<(String, String)> {
    let mut vars = vec![
        ("TRACEPARENT".to_string(), ctx.to_traceparent()),
        ("TRACESTATE".to_string(), ctx.to_tracestate()),
    ];

    // Also set individual baggage variables for tools that don't parse tracestate
    if let Some(ref session) = ctx.baggage.session_id {
        vars.push(("X0K_SESSION_ID".to_string(), session.clone()));
    }
    if let Some(ref workstream) = ctx.baggage.workstream_id {
        vars.push(("X0K_WORKSTREAM_ID".to_string(), workstream.clone()));
    }
    if let Some(ref intent) = ctx.baggage.intent_id {
        vars.push(("X0K_INTENT_ID".to_string(), intent.clone()));
    }

    vars
}

/// Format environment variables as a shell-compatible string
///
/// Useful for constructing command lines with env prefix.
///
/// # Example
///
/// ```rust,ignore
/// let ctx = trace_context::child_span();
/// let env_string = trace_context::to_env_string(&ctx);
/// // env_string: "TRACEPARENT=00-abc...-def...-01 TRACESTATE=x0k=..."
/// ```
pub fn to_env_string(ctx: &TraceContext) -> String {
    to_env_vars(ctx)
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, shell_escape(&v)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Simple shell escaping for environment variable values
fn shell_escape(s: &str) -> String {
    // If the string contains special characters, quote it
    if s.chars()
        .any(|c| c.is_whitespace() || c == '\'' || c == '"' || c == '\\' || c == '$')
    {
        format!("'{}'", s.replace('\'', "'\\''"))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_with_context_and_current() {
        let ctx = TraceContext::new_root();
        let trace_id = ctx.trace_id;

        let result = with_context(ctx, async {
            let current = current().expect("should have context");
            assert_eq!(current.trace_id, trace_id);
            42
        })
        .await;

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_current_without_context() {
        // Outside of with_context, current() returns None
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn test_child_span_with_context() {
        let parent = TraceContext::new_root();
        let parent_trace_id = parent.trace_id;
        let parent_span_id = parent.span_id;

        with_context(parent, async {
            let child = child_span();
            // Same trace
            assert_eq!(child.trace_id, parent_trace_id);
            // Different span
            assert_ne!(child.span_id, parent_span_id);
            // Links to parent
            assert_eq!(child.parent_span_id, Some(parent_span_id));
        })
        .await;
    }

    #[tokio::test]
    async fn test_child_span_without_context() {
        // Without context, child_span creates a new root
        let ctx = child_span();
        assert!(ctx.trace_id.is_valid());
        assert!(ctx.span_id.is_valid());
        assert!(ctx.parent_span_id.is_none()); // Root has no parent
    }

    #[test]
    fn test_to_env_vars() {
        let mut ctx = TraceContext::new_root();
        ctx.baggage.session_id = Some("sess-123".to_string());
        ctx.baggage.workstream_id = Some("ws-456".to_string());
        ctx.baggage.intent_id = Some("task-789".to_string());

        let vars = to_env_vars(&ctx);

        let var_map: std::collections::HashMap<_, _> = vars.into_iter().collect();

        assert!(var_map.contains_key("TRACEPARENT"));
        assert!(var_map.contains_key("TRACESTATE"));
        assert_eq!(var_map.get("X0K_SESSION_ID"), Some(&"sess-123".to_string()));
        assert_eq!(
            var_map.get("X0K_WORKSTREAM_ID"),
            Some(&"ws-456".to_string())
        );
        assert_eq!(var_map.get("X0K_INTENT_ID"), Some(&"task-789".to_string()));
    }

    /// Serializes the tests here that set and clear process environment.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn test_from_env_with_traceparent() {
        let _guard = env_lock();
        // Set up environment
        std::env::set_var(
            "TRACEPARENT",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        std::env::set_var("TRACESTATE", "x0k=session1;ws1;task1;seed1");

        let ctx = from_env();

        assert_eq!(ctx.trace_id.to_hex(), "0af7651916cd43dd8448eb211c80319c");
        // Note: span_id is different because from_env creates a new span
        assert_eq!(ctx.parent_span_id.unwrap().to_hex(), "b7ad6b7169203331");
        assert_eq!(ctx.baggage.session_id, Some("session1".to_string()));
        assert_eq!(ctx.baggage.workstream_id, Some("ws1".to_string()));
        assert_eq!(ctx.baggage.intent_id, Some("task1".to_string()));
        assert_eq!(ctx.baggage.seed_id, Some("seed1".to_string()));

        // Cleanup
        std::env::remove_var("TRACEPARENT");
        std::env::remove_var("TRACESTATE");
    }

    #[test]
    fn test_from_env_without_traceparent() {
        let _guard = env_lock();
        // Ensure TRACEPARENT is not set
        std::env::remove_var("TRACEPARENT");
        std::env::remove_var("TRACESTATE");
        std::env::set_var("X0K_WORKSTREAM_ID", "ws-fallback");

        let ctx = from_env();

        // Should create a new root trace
        assert!(ctx.parent_span_id.is_none());
        // But still pick up baggage from env
        assert_eq!(ctx.baggage.workstream_id, Some("ws-fallback".to_string()));

        // Cleanup
        std::env::remove_var("X0K_WORKSTREAM_ID");
    }

    #[test]
    fn test_shell_escape() {
        assert_eq!(shell_escape("simple"), "simple");
        assert_eq!(shell_escape("with space"), "'with space'");
        assert_eq!(shell_escape("with'quote"), "'with'\\''quote'");
    }
}
