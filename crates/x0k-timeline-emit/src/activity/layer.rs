//! Activity tracking types
//!
//! Provides the `ActorResolver` trait used by the unified
//! `ObservabilityLayer` for resolving actors from span attributes.
//!
//! The `ActivityTrackingLayer` that previously lived here has been replaced
//! by `crate::observability::ObservabilityLayer`.

use tracing::span;

/// Trait for resolving actors from span attributes
pub trait ActorResolver: Send + Sync {
    /// The id of the actor span attributes name
    fn resolve_actor(&self, attrs: &span::Attributes) -> Option<String>;
}
