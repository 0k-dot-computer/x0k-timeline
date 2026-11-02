//! Activity routing logic for timeline storage classification.
//!
//! `StorageTier` is a routing/query label. Physical retention is tracked
//! separately via `RetentionClass`.

// Re-export canonical StorageTier from x0k-types
pub use x0k_timeline_record::record::{
    route_activity_storage_tier, RetentionClass, StorageTier, LEGACY_DURABILITY_KEY,
    STORAGE_TIER_CENTRAL_VALUE, STORAGE_TIER_EPHEMERAL_VALUE, STORAGE_TIER_KEY,
    STORAGE_TIER_RECORD_VALUE,
};

/// Route an activity to the appropriate storage tier classification.
///
/// Routing priority:
/// 1. Explicit `activity.storage_tier` metadata field
/// 2. Legacy `activity.durability` metadata field
/// 2. Activity type prefix matching
/// 3. Default to ephemeral for unknown types
pub fn route_activity(
    activity_type: &str,
    metadata: &std::collections::HashMap<String, String>,
) -> StorageTier {
    route_activity_storage_tier(activity_type, metadata)
}

/// Default routing based on activity type prefix
fn default_route(activity_type: &str) -> StorageTier {
    route_activity_storage_tier(activity_type, &std::collections::HashMap::new())
}

/// Check if an activity type maps to a non-ephemeral storage tier.
pub fn is_durable(activity_type: &str) -> bool {
    matches!(
        default_route(activity_type),
        StorageTier::Central | StorageTier::Record
    )
}

/// Check if an activity type maps to the ephemeral storage tier.
pub fn is_ephemeral(activity_type: &str) -> bool {
    default_route(activity_type) == StorageTier::Ephemeral
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_agent_activities_are_durable() {
        assert_eq!(default_route("agent.spawned"), StorageTier::Central);
        assert_eq!(default_route("agent.completed"), StorageTier::Central);
        assert_eq!(default_route("agent.failed"), StorageTier::Central);
        assert_eq!(default_route("agent.paused"), StorageTier::Central);
    }

    #[test]
    fn test_task_activities_are_durable() {
        assert_eq!(default_route("task.created"), StorageTier::Central);
        assert_eq!(default_route("task.started"), StorageTier::Central);
        assert_eq!(default_route("task.completed"), StorageTier::Central);
        assert_eq!(default_route("task.abandoned"), StorageTier::Central);
    }

    #[test]
    fn test_module_activities_are_durable() {
        assert_eq!(default_route("module.registered"), StorageTier::Central);
        assert_eq!(default_route("module.invoked"), StorageTier::Central);
        assert_eq!(default_route("module.succeeded"), StorageTier::Central);
        assert_eq!(default_route("module.failed"), StorageTier::Central);
    }

    #[test]
    fn test_system_activities_are_durable() {
        assert_eq!(default_route("system.error"), StorageTier::Central);
        assert_eq!(default_route("system.startup"), StorageTier::Central);
        assert_eq!(default_route("system.shutdown"), StorageTier::Central);
    }

    #[test]
    fn test_decision_activities_are_record_tier() {
        assert_eq!(default_route("decision.requested"), StorageTier::Record);
        assert_eq!(default_route("decision.provided"), StorageTier::Record);
        // Record tier is still considered durable
        assert!(is_durable("decision.requested"));
    }

    #[test]
    fn test_document_activities_are_durable() {
        assert_eq!(default_route("document.mutated"), StorageTier::Central);
        assert_eq!(default_route("document.created"), StorageTier::Central);
        assert_eq!(default_route("document.deleted"), StorageTier::Central);
    }

    #[test]
    fn test_usage_activities_are_record_tier() {
        assert_eq!(default_route("usage.tokens"), StorageTier::Record);
        assert_eq!(default_route("usage.api_call"), StorageTier::Record);
        assert_eq!(default_route("usage.session"), StorageTier::Record);
    }

    #[test]
    fn test_process_activities_are_ephemeral() {
        assert_eq!(default_route("process.stdout"), StorageTier::Ephemeral);
        assert_eq!(default_route("process.stderr"), StorageTier::Ephemeral);
    }

    #[test]
    fn test_span_activities_are_ephemeral() {
        assert_eq!(default_route("span.entered"), StorageTier::Ephemeral);
        assert_eq!(default_route("span.exited"), StorageTier::Ephemeral);
    }

    #[test]
    fn test_build_activities_are_ephemeral() {
        assert_eq!(default_route("build.started"), StorageTier::Ephemeral);
        assert_eq!(default_route("build.output"), StorageTier::Ephemeral);
    }

    #[test]
    fn test_unknown_activities_are_ephemeral() {
        assert_eq!(default_route("unknown.something"), StorageTier::Ephemeral);
        assert_eq!(default_route("custom.event"), StorageTier::Ephemeral);
    }

    #[test]
    fn test_explicit_durability_override() {
        let mut metadata = std::collections::HashMap::new();

        // Override the routed query class from ephemeral to central
        metadata.insert(
            STORAGE_TIER_KEY.to_string(),
            STORAGE_TIER_CENTRAL_VALUE.to_string(),
        );
        assert_eq!(
            route_activity("process.stdout", &metadata),
            StorageTier::Central
        );

        // Force durable activity to ephemeral
        metadata.insert(
            STORAGE_TIER_KEY.to_string(),
            STORAGE_TIER_EPHEMERAL_VALUE.to_string(),
        );
        assert_eq!(
            route_activity("agent.spawned", &metadata),
            StorageTier::Ephemeral
        );
    }

    #[test]
    fn test_explicit_record_durability_override() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            STORAGE_TIER_KEY.to_string(),
            STORAGE_TIER_RECORD_VALUE.to_string(),
        );
        assert_eq!(
            route_activity("process.stdout", &metadata),
            StorageTier::Record
        );
    }

    #[test]
    fn test_invalid_durability_falls_through() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(STORAGE_TIER_KEY.to_string(), "invalid".to_string());

        // Should fall through to default routing
        assert_eq!(
            route_activity("agent.spawned", &metadata),
            StorageTier::Central
        );
        assert_eq!(
            route_activity("process.stdout", &metadata),
            StorageTier::Ephemeral
        );
    }

    #[test]
    fn test_legacy_durability_key_still_supported() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            LEGACY_DURABILITY_KEY.to_string(),
            STORAGE_TIER_RECORD_VALUE.to_string(),
        );
        assert_eq!(
            route_activity("process.stdout", &metadata),
            StorageTier::Record
        );
    }

    #[test]
    fn test_is_durable_helper() {
        assert!(is_durable("agent.spawned"));
        assert!(is_durable("task.started"));
        assert!(is_durable("usage.tokens"));
        assert!(!is_durable("process.stdout"));
    }

    #[test]
    fn test_is_ephemeral_helper() {
        assert!(is_ephemeral("process.stdout"));
        assert!(is_ephemeral("span.entered"));
        assert!(!is_ephemeral("agent.spawned"));
    }
}
