//! Semantic activity context tracking layered on top of tracing spans
//!
//! This module keeps a lightweight semantic activity stack while the real
//! structural hierarchy still lives in tracing span IDs / parent span IDs.
//! Since TaskScope already preserves spans across async boundaries, we only
//! need enough local state to annotate semantic activity relationships.

use std::cell::RefCell;
use tracing::Span;
use x0k_timeline_record::ActivityId;

// Thread-local storage for the current semantic activity ID.
// This is for semantic activity relationships, not structural tracing parentage.
thread_local! {
    static CURRENT_ACTIVITY_ID: RefCell<Option<ActivityId>> = const { RefCell::new(None) };
}

/// Create a span tagged with the current semantic activity relationship.
/// Structural tracing parentage still comes from the ambient tracing context.
pub fn activity_span(activity_id: &ActivityId, activity_type: &str) -> Span {
    // Check if there's a semantic parent activity ID in thread-local storage
    let parent_id = CURRENT_ACTIVITY_ID.with(|id| id.borrow().clone());

    let span = if let Some(parent_id) = parent_id {
        // Record semantic parentage on the span fields for later activity projection.
        tracing::span!(
            tracing::Level::INFO,
            "activity",
            activity.id = %activity_id.0,
            activity.parent_id = %parent_id.0,
            activity.type = %activity_type
        )
    } else {
        // No semantic parent activity - create a root semantic activity span
        tracing::span!(
            tracing::Level::INFO,
            "activity",
            activity.id = %activity_id.0,
            activity.type = %activity_type
        )
    };

    span
}

/// Guard that sets and clears the current semantic activity ID
pub struct ActivityGuard {
    previous: Option<ActivityId>,
}

impl ActivityGuard {
    /// Create a new activity guard that sets the current semantic activity ID
    pub fn new(activity_id: ActivityId) -> Self {
        let previous = CURRENT_ACTIVITY_ID.with(|id| {
            let prev = id.borrow().clone();
            *id.borrow_mut() = Some(activity_id);
            prev
        });
        Self { previous }
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        // Restore the previous semantic activity ID when the guard is dropped
        CURRENT_ACTIVITY_ID.with(|id| {
            *id.borrow_mut() = self.previous.clone();
        });
    }
}

/// Create a child activity span with explicit semantic parentage
pub fn child_activity_span(
    parent_id: &ActivityId,
    activity_id: &ActivityId,
    activity_type: &str,
) -> Span {
    tracing::span!(
        tracing::Level::INFO,
        "activity",
        activity.id = %activity_id.0,
        activity.parent_id = %parent_id.0,
        activity.type = %activity_type
    )
}

/// Get the current semantic activity ID from thread-local storage, if set.
pub fn current_activity_id() -> Option<String> {
    CURRENT_ACTIVITY_ID.with(|id| id.borrow().as_ref().map(|a| a.0.clone()))
}

/// Create a test activity span that will be tracked by ObservabilityLayer
pub fn test_activity_span(test_name: &str) -> Span {
    tracing::span!(
        tracing::Level::INFO,
        "activity",
        activity.id = %uuid::Uuid::new_v4(),
        activity.type = "test_execution",
        activity.description = %format!("Running test: {}", test_name),
        test.name = %test_name
    )
}
