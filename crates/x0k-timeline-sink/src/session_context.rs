//! Session context for sandbox-aware operations
//!
//! Local copy of session context to avoid circular dependency on x0k-env.

/// Session context read from environment variables.
///
/// These are set by the spawner (bwrap.rs) when launching isolated agents:
/// - `X0K_SANDBOX_ID`: Unique session ID for this isolated environment
/// - `X0K_INTENT_ID`: The Grove intent being worked on
/// - `X0K_WORKSTREAM_ID`: The workstream this session belongs to
/// - `X0K_TRACE_ID`: W3C trace ID for cross-process correlation
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionContext {
    /// X0K_SANDBOX_ID - unique session ID for the isolated environment
    pub sandbox_id: Option<String>,
    /// X0K_INTENT_ID - the Grove intent being worked on
    pub intent_id: Option<String>,
    /// X0K_WORKSTREAM_ID - the workstream this session belongs to
    pub workstream_id: Option<String>,
    /// X0K_TRACE_ID - W3C trace ID (32 hex chars) for cross-process correlation
    pub trace_id: Option<String>,
    /// X0K_SESSION_ID - logical session ID for agent sessions
    pub session_id: Option<String>,
    /// Process ID of the current process
    pub pid: u32,
}

impl SessionContext {
    /// Read session context from environment variables.
    pub fn from_env() -> Self {
        Self {
            sandbox_id: std::env::var("X0K_SANDBOX_ID").ok(),
            intent_id: std::env::var("X0K_INTENT_ID").ok(),
            workstream_id: std::env::var("X0K_WORKSTREAM_ID").ok(),
            trace_id: std::env::var("X0K_TRACE_ID").ok(),
            session_id: std::env::var("X0K_SESSION_ID").ok(),
            pid: std::process::id(),
        }
    }

    /// Create a session context with explicit values (for testing).
    pub fn new(
        sandbox_id: Option<String>,
        intent_id: Option<String>,
        workstream_id: Option<String>,
    ) -> Self {
        Self {
            sandbox_id,
            intent_id,
            workstream_id,
            trace_id: None,
            session_id: None,
            pid: std::process::id(),
        }
    }

    /// Create a session context with all fields (for testing).
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Check if we're running in a sandbox environment.
    pub fn is_sandboxed(&self) -> bool {
        self.sandbox_id.is_some()
    }

    /// Check if we have a task context.
    pub fn has_intent(&self) -> bool {
        self.intent_id.is_some()
    }

    /// Check if we have a trace context.
    pub fn has_trace(&self) -> bool {
        self.trace_id.is_some()
    }
}
