//! Predicate constants for activity artifacts in Dialog-DB
//!
//! These predicates define the schema for activity tracking in Dialog-DB.
//! Based on the activity schema design document.

/// Namespace for activity entities
pub const C0K_ACTIVITY: &str = "x0k:activity";

/// Namespace for workstream entities
/// Format: x0k:workstream/{workstream_id}
pub const C0K_WORKSTREAM: &str = "x0k:workstream";

/// Namespace for binary execution entities (time-travel by binary run)
/// Format: x0k:exec/{binary}/{context}
pub const C0K_EXEC: &str = "x0k:exec";

/// Predicate constants for activity artifacts
///
/// Activities capture "what happened" in the system - agent lifecycle events,
/// task state changes, module executions, errors, and decision points.
pub mod activity {
    /// Activity type (e.g., "agent.spawned", "task.started", "module.invoked")
    /// Value: String
    pub const TYPE: &str = "activity/type";

    /// Actor who performed the activity
    /// Value: Entity (x0k:actor/{uuid})
    pub const ACTOR: &str = "activity/actor";

    /// Timestamp when activity occurred
    /// Value: SignedInt (microseconds since epoch)
    pub const TIMESTAMP: &str = "activity/timestamp";

    /// Target entity of the activity (task, seed, etc.)
    /// Value: Entity (x0k:task/{id}, x0k:seed/{id}, etc.)
    pub const TARGET: &str = "activity/target";

    /// Type of the target entity ("task", "seed", "software-module", etc.)
    /// Value: String
    pub const TARGET_TYPE: &str = "activity/target-type";

    /// Software module involved in this activity
    /// Value: Entity (x0k:software-module/{id})
    pub const SOFTWARE_MODULE: &str = "activity/software-module";

    /// Correlation ID for tracking related activities across process boundaries
    /// Value: String
    pub const CORRELATION_ID: &str = "activity/correlation-id";

    /// Parent activity ID for causality chain
    /// Value: Entity (x0k:activity/{parent-uuid})
    pub const PARENT_ID: &str = "activity/parent-id";

    /// W3C Trace ID for distributed tracing (128-bit, 32 hex chars)
    /// Stays constant across all spans within a trace
    /// Value: String
    pub const TRACE_ID: &str = "activity/trace-id";

    /// Span ID for this activity (64-bit, 16 hex chars)
    /// Unique identifier for this unit of work
    /// Value: String
    pub const SPAN_ID: &str = "activity/span-id";

    /// Parent span ID in the trace (64-bit, 16 hex chars)
    /// Links to the span that caused this one
    /// Value: String
    pub const PARENT_SPAN_ID: &str = "activity/parent-span-id";

    /// Session ID for grouping activities within a session
    /// Value: String
    pub const SESSION_ID: &str = "activity/session-id";

    /// Workstream this activity belongs to
    /// Value: Entity (x0k:workstream/{uuid})
    pub const WORKSTREAM: &str = "activity/workstream";

    /// Human-readable description of the activity
    /// Value: String
    pub const DESCRIPTION: &str = "activity/description";

    /// Previous value for state change activities
    /// Value: String
    pub const OLD_VALUE: &str = "activity/old-value";

    /// New value for state change activities
    /// Value: String
    pub const NEW_VALUE: &str = "activity/new-value";

    /// Error message for failed activities
    /// Value: String
    pub const ERROR_MESSAGE: &str = "activity/error-message";

    /// JSON-encoded additional metadata
    /// Value: String (JSON)
    pub const METADATA: &str = "activity/metadata";

    /// Duration of the activity in milliseconds
    /// Value: SignedInt
    pub const DURATION_MS: &str = "activity/duration-ms";

    /// Ended timestamp (for completed activities)
    /// Value: SignedInt (microseconds since epoch)
    pub const ENDED_AT: &str = "activity/ended-at";

    // Source location fields for log filtering

    /// Source file path (relative to workspace)
    /// Value: String
    pub const SOURCE_FILE: &str = "activity/source-file";

    /// Source line number
    /// Value: SignedInt
    pub const SOURCE_LINE: &str = "activity/source-line";

    /// Function/span name (from `#[instrument]` or manual span)
    /// Value: String
    pub const SOURCE_FUNCTION: &str = "activity/source-function";

    /// Module path (e.g., "x0k_core::sync")
    /// Value: String
    pub const SOURCE_MODULE: &str = "activity/source-module";

    /// Span name from tracing (metadata.name())
    /// For `#[instrument]` this is the function name, for manual spans it's the explicit name
    /// Used for hierarchical call stack views
    /// Value: String
    pub const SPAN_NAME: &str = "activity/span-name";
}

/// Predicates for workstream entities
///
/// Workstreams own activities as attributes, enabling efficient queries like
/// "get all activities for workstream X" via direct entity lookup.
pub mod workstream {
    /// Link to an activity owned by this workstream
    /// Value: Entity (x0k:activity/{activity_id})
    /// Multiple activities can be linked to one workstream.
    pub const ACTIVITY: &str = "workstream/activity";
}

/// Predicates for binary execution entities (time-travel by binary run)
///
/// Tracks execution history of binaries, enabling comparison between runs
/// (e.g., "what changed from the last run that crashed?").
pub mod exec {
    /// Latest run metadata (JSON blob with cause)
    /// Value: String (JSON) - includes started_at, ended_at, exit_code, workstream, etc.
    /// This predicate uses causes: each run supersedes the previous in same context
    pub const LATEST_RUN: &str = "exec/latest-run";
}

/// Activity type prefixes for routing decisions
///
/// Activities are routed to central (durable) or ephemeral storage based on their type prefix.
pub mod activity_types {
    /// Agent lifecycle events (spawned, completed, failed, paused, resumed)
    pub const AGENT_PREFIX: &str = "agent";

    /// Task state changes (created, started, completed, abandoned)
    pub const TASK_PREFIX: &str = "task";

    /// Module execution events (registered, invoked, succeeded, failed)
    pub const MODULE_PREFIX: &str = "module";

    /// System-level events (error, warning, startup, shutdown)
    pub const SYSTEM_PREFIX: &str = "system";

    /// Human-AI interaction events (decision.requested, decision.provided)
    pub const DECISION_PREFIX: &str = "decision";

    /// Process output (ephemeral - stdout, stderr)
    pub const PROCESS_PREFIX: &str = "process";

    /// Build/test output (ephemeral)
    pub const BUILD_PREFIX: &str = "build";

    /// Document mutation events (created, mutated, deleted)
    pub const DOCUMENT_PREFIX: &str = "document";

    /// Usage record events (permanent retention)
    pub const USAGE_PREFIX: &str = "usage";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_format() {
        // Predicates should use namespace/name format
        assert!(activity::TYPE.contains('/'));
        assert!(activity::ACTOR.contains('/'));
        assert!(activity::TIMESTAMP.contains('/'));
    }

    #[test]
    fn test_namespace_format() {
        // Namespace should use x0k: prefix
        assert!(C0K_ACTIVITY.starts_with("x0k:"));
    }
}
