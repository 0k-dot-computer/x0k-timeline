//! Data migration support for protocol versioning
//!
//! This module provides traits and utilities for migrating data structures
//! between different protocol versions, enabling backward compatibility.

use crate::ProtocolVersion;
use std::collections::HashMap;
use std::fmt::Debug;

/// Trait for types that can be migrated between protocol versions
pub trait Migratable: Sized {
    /// The type identifier for this data structure
    fn type_id() -> &'static str;

    /// Migrate this value to a different protocol version
    fn migrate_to(&self, target_version: ProtocolVersion) -> Result<Self, MigrationError>;

    /// Get the version this value was created with
    fn version(&self) -> ProtocolVersion;
}

/// Error type for migration operations
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("No migration path from version {from} to {to}")]
    NoMigrationPath { from: u16, to: u16 },

    #[error("Migration failed: {0}")]
    MigrationFailed(String),

    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u16),

    #[error("Data corruption during migration: {0}")]
    DataCorruption(String),
}

/// A migration function that transforms data from one version to another
pub type MigrationFn<T> = Box<dyn Fn(&T) -> Result<T, MigrationError> + Send + Sync>;

/// Registry of migration paths for a specific type
pub struct MigrationRegistry<T> {
    /// Map from (source_version, target_version) to migration function
    migrations: HashMap<(u16, u16), MigrationFn<T>>,
}

impl<T: Clone + Debug> MigrationRegistry<T> {
    /// Create a new migration registry for a type
    pub fn new(_type_id: &'static str) -> Self {
        let _ = _type_id; // Unused parameter for future compatibility
        Self {
            migrations: HashMap::new(),
        }
    }

    /// Register a migration path from one version to another
    pub fn register_migration(
        &mut self,
        from_version: ProtocolVersion,
        to_version: ProtocolVersion,
        migration: MigrationFn<T>,
    ) {
        self.migrations
            .insert((from_version.0, to_version.0), migration);
    }

    /// Register a bidirectional migration (forward and backward)
    pub fn register_bidirectional<F, B>(
        &mut self,
        version_a: ProtocolVersion,
        version_b: ProtocolVersion,
        forward: F,
        backward: B,
    ) where
        F: Fn(&T) -> Result<T, MigrationError> + Send + Sync + 'static,
        B: Fn(&T) -> Result<T, MigrationError> + Send + Sync + 'static,
    {
        self.migrations
            .insert((version_a.0, version_b.0), Box::new(forward));
        self.migrations
            .insert((version_b.0, version_a.0), Box::new(backward));
    }

    /// Migrate data from one version to another
    pub fn migrate(
        &self,
        data: &T,
        from_version: ProtocolVersion,
        to_version: ProtocolVersion,
    ) -> Result<T, MigrationError> {
        // If versions are the same, no migration needed
        if from_version == to_version {
            return Ok(data.clone());
        }

        // Try direct migration
        if let Some(migration_fn) = self.migrations.get(&(from_version.0, to_version.0)) {
            return migration_fn(data);
        }

        // Try multi-hop migration (simple case: via intermediate version)
        // In a production system, you'd want a proper path-finding algorithm
        for intermediate in [ProtocolVersion::CURRENT, ProtocolVersion::MIN_SUPPORTED] {
            if intermediate == from_version || intermediate == to_version {
                continue;
            }

            if self
                .migrations
                .contains_key(&(from_version.0, intermediate.0))
                && self
                    .migrations
                    .contains_key(&(intermediate.0, to_version.0))
            {
                // Found a path: from -> intermediate -> to
                let intermediate_data = self.migrations[&(from_version.0, intermediate.0)](data)?;
                return self.migrations[&(intermediate.0, to_version.0)](&intermediate_data);
            }
        }

        Err(MigrationError::NoMigrationPath {
            from: from_version.0,
            to: to_version.0,
        })
    }
}

/// Helper macro to define versioned structs with migration support
#[macro_export]
macro_rules! versioned_struct {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident: $field_type:ty
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        pub struct $name {
            /// Protocol version this struct was created with
            #[serde(default = "default_version")]
            pub _version: $crate::ProtocolVersion,
            $(
                $(#[$field_meta])*
                pub $field: $field_type,
            )*
        }

        fn default_version() -> $crate::ProtocolVersion {
            $crate::ProtocolVersion::CURRENT
        }

        impl $name {
            /// Create a new instance with the current protocol version
            pub fn new($($field: $field_type),*) -> Self {
                Self {
                    _version: $crate::ProtocolVersion::CURRENT,
                    $($field),*
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestDataV1 {
        id: u32,
        name: String,
    }

    #[test]
    fn test_migration_registry() {
        let mut registry = MigrationRegistry::<TestDataV1>::new("TestData");

        // Register a simple identity migration
        registry.register_migration(
            ProtocolVersion(1),
            ProtocolVersion(2),
            Box::new(|data| Ok(data.clone())),
        );

        let data = TestDataV1 {
            id: 42,
            name: "test".to_string(),
        };

        let migrated = registry
            .migrate(&data, ProtocolVersion(1), ProtocolVersion(2))
            .unwrap();
        assert_eq!(migrated.id, data.id);
        assert_eq!(migrated.name, data.name);
    }

    #[test]
    fn test_no_migration_needed() {
        let registry = MigrationRegistry::<TestDataV1>::new("TestData");

        let data = TestDataV1 {
            id: 42,
            name: "test".to_string(),
        };

        // Same version should return clone without migration
        let result = registry
            .migrate(&data, ProtocolVersion(1), ProtocolVersion(1))
            .unwrap();
        assert_eq!(result.id, data.id);
    }

    #[test]
    fn test_migration_path_not_found() {
        let registry = MigrationRegistry::<TestDataV1>::new("TestData");

        let data = TestDataV1 {
            id: 42,
            name: "test".to_string(),
        };

        // No migration registered
        let result = registry.migrate(&data, ProtocolVersion(1), ProtocolVersion(3));
        assert!(matches!(
            result,
            Err(MigrationError::NoMigrationPath { .. })
        ));
    }

    #[test]
    fn test_versioned_struct_macro() {
        versioned_struct! {
            /// Test struct with versioning
            pub struct VersionedTest {
                id: u64,
                data: String,
            }
        }

        let test = VersionedTest::new(123, "test".to_string());
        assert_eq!(test._version, ProtocolVersion::CURRENT);
        assert_eq!(test.id, 123);
        assert_eq!(test.data, "test");
    }
}
