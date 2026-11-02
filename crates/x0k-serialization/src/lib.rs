//! Zero-overhead serialization abstraction layer for x0k
//!
//! This crate provides compile-time serialization format selection for zero runtime overhead.
//! Exactly one format feature must be enabled: bincode-format, postcard-format, or json-format.

mod migration;
mod versioned_message;

pub use migration::{Migratable, MigrationError, MigrationRegistry};
pub use versioned_message::{
    VersionNegotiation, VersionedMessage, C0K_MAGIC, DEVICE_ADMISSION_PROGRESS, PAIRING_GRANTS_REISSUE,
};

// Re-export rkyv for types that need to derive Archive
pub use rkyv;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

// Re-export postcard compatibility derive macro and trait
// - PostcardSerializable: the derive macro for user types
// - PostcardSerializableTrait: the marker trait (use in bounds)
#[cfg(feature = "postcard-format")]
pub use postcard_serializable::PostcardSerializable;
#[cfg(feature = "postcard-format")]
pub use postcard_serializable_types::PostcardSerializable as PostcardSerializableTrait;

// Internal alias for the trait (used in function bounds within this crate)
#[cfg(feature = "postcard-format")]
use postcard_serializable_types::PostcardSerializable as PostcardSerializableMarker;

/// Serialization format selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Network format (postcard) - compact for bandwidth
    Network,
    /// Local storage format (rkyv) - zero-copy reads
    Local,
}

/// Errors that can occur during serialization/deserialization
#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    #[error("Deserialization failed: {0}")]
    DeserializationFailed(String),

    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u16, actual: u16 },

    #[error("Rkyv error: {0}")]
    Rkyv(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Protocol version for network communication and data compatibility
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct ProtocolVersion(pub u16);

impl ProtocolVersion {
    /// Current protocol version
    pub const CURRENT: Self = Self(3);

    /// Minimum supported protocol version
    pub const MIN_SUPPORTED: Self = Self(3);

    /// Check if this version is supported
    pub fn is_supported(&self) -> bool {
        *self >= Self::MIN_SUPPORTED && *self <= Self::CURRENT
    }
}

/// Zero-overhead serialization functions that compile to direct format calls
pub mod serialize {
    use super::*;

    /// Serialize a value to bytes using the compile-time selected format.
    ///
    /// When using `postcard-format`, the type must implement `PostcardSerializable`
    /// (via `#[derive(PostcardSerializable)]`) to ensure compile-time validation
    /// of postcard compatibility.
    #[inline]
    #[cfg(feature = "postcard-format")]
    pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>, SerializationError>
    where
        T: PostcardSerializableMarker,
    {
        postcard::to_allocvec(value)
            .map_err(|e| SerializationError::SerializationFailed(e.to_string()))
    }

    /// Serialize a value to bytes using the compile-time selected format.
    #[inline]
    #[cfg(not(feature = "postcard-format"))]
    pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>, SerializationError>
    where
        T: Serialize + ?Sized,
    {
        #[cfg(feature = "bincode-format")]
        {
            return bincode::serialize(value)
                .map_err(|e| SerializationError::SerializationFailed(e.to_string()));
        }

        #[cfg(feature = "json-format")]
        {
            return serde_json::to_vec(value)
                .map_err(|e| SerializationError::SerializationFailed(e.to_string()));
        }

        #[cfg(not(any(feature = "bincode-format", feature = "json-format")))]
        {
            compile_error!("At least one serialization format feature must be enabled");
        }
    }

    /// Deserialize bytes into a value using the compile-time selected format.
    ///
    /// When using `postcard-format`, the type must implement `PostcardSerializable`
    /// (via `#[derive(PostcardSerializable)]`) to ensure compile-time validation
    /// of postcard compatibility.
    #[inline]
    #[cfg(feature = "postcard-format")]
    pub fn from_bytes<T>(data: &[u8]) -> Result<T, SerializationError>
    where
        T: PostcardSerializableMarker,
    {
        postcard::from_bytes(data)
            .map_err(|e| SerializationError::DeserializationFailed(e.to_string()))
    }

    /// Deserialize bytes into a value using the compile-time selected format.
    #[inline]
    #[cfg(not(feature = "postcard-format"))]
    pub fn from_bytes<T>(data: &[u8]) -> Result<T, SerializationError>
    where
        T: DeserializeOwned,
    {
        #[cfg(feature = "bincode-format")]
        {
            return bincode::deserialize(data)
                .map_err(|e| SerializationError::DeserializationFailed(e.to_string()));
        }

        #[cfg(feature = "json-format")]
        {
            return serde_json::from_slice(data)
                .map_err(|e| SerializationError::DeserializationFailed(e.to_string()));
        }

        #[cfg(not(any(feature = "bincode-format", feature = "json-format")))]
        {
            compile_error!("At least one serialization format feature must be enabled");
        }
    }

    /// Get the name of the currently selected format
    pub const fn format_name() -> &'static str {
        #[cfg(feature = "bincode-format")]
        return "bincode";

        #[cfg(feature = "postcard-format")]
        return "postcard";

        #[cfg(feature = "json-format")]
        return "json";
    }

    /// Check if the current format supports zero-copy deserialization
    pub const fn supports_zero_copy() -> bool {
        #[cfg(feature = "postcard-format")]
        return true;

        #[cfg(any(feature = "bincode-format", feature = "json-format"))]
        return false;
    }

    /// Serialize a value WITHOUT compile-time PostcardSerializable validation.
    ///
    /// # Safety
    /// This is an escape hatch for types that contain external types (e.g., from
    /// cryptographic crates) that cannot implement PostcardSerializable. Use with
    /// care - these types don't have compile-time validation of postcard compatibility.
    ///
    /// Prefer `to_bytes()` for all types that can derive PostcardSerializable.
    #[inline]
    #[cfg(feature = "postcard-format")]
    pub fn to_bytes_unchecked<T>(value: &T) -> Result<Vec<u8>, SerializationError>
    where
        T: Serialize,
    {
        postcard::to_allocvec(value)
            .map_err(|e| SerializationError::SerializationFailed(e.to_string()))
    }

    /// Deserialize bytes WITHOUT compile-time PostcardSerializable validation.
    ///
    /// # Safety
    /// This is an escape hatch for types that contain external types (e.g., from
    /// cryptographic crates) that cannot implement PostcardSerializable. Use with
    /// care - these types don't have compile-time validation of postcard compatibility.
    ///
    /// Prefer `from_bytes()` for all types that can derive PostcardSerializable.
    #[inline]
    #[cfg(feature = "postcard-format")]
    pub fn from_bytes_unchecked<T>(data: &[u8]) -> Result<T, SerializationError>
    where
        T: DeserializeOwned,
    {
        postcard::from_bytes(data)
            .map_err(|e| SerializationError::DeserializationFailed(e.to_string()))
    }

    /// Escape hatch for non-postcard formats (same as regular functions)
    #[inline]
    #[cfg(not(feature = "postcard-format"))]
    pub fn to_bytes_unchecked<T>(value: &T) -> Result<Vec<u8>, SerializationError>
    where
        T: Serialize + ?Sized,
    {
        to_bytes(value)
    }

    /// Escape hatch for non-postcard formats (same as regular functions)
    #[inline]
    #[cfg(not(feature = "postcard-format"))]
    pub fn from_bytes_unchecked<T>(data: &[u8]) -> Result<T, SerializationError>
    where
        T: DeserializeOwned,
    {
        from_bytes(data)
    }

    /// Serialize a value for local storage using rkyv (zero-copy reads)
    #[inline]
    pub fn to_bytes_local<T>(value: &T) -> Result<Vec<u8>, SerializationError>
    where
        T: RkyvSerialize<rkyv::ser::serializers::AllocSerializer<256>>,
    {
        rkyv::to_bytes::<_, 256>(value)
            .map(|bytes| bytes.to_vec())
            .map_err(|e| SerializationError::Rkyv(e.to_string()))
    }

    /// Deserialize bytes from local storage using rkyv (zero-copy)
    /// Returns a reference to the archived data without copying
    #[inline]
    pub fn from_bytes_local<T>(data: &[u8]) -> Result<&T::Archived, SerializationError>
    where
        T: Archive,
        T::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
    {
        rkyv::check_archived_root::<T>(data).map_err(|e| SerializationError::Rkyv(e.to_string()))
    }

    /// Deserialize bytes from local storage and convert to owned value
    /// Use this when you need an owned value instead of a reference
    #[inline]
    pub fn from_bytes_local_owned<T>(data: &[u8]) -> Result<T, SerializationError>
    where
        T: Archive,
        T::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
        T::Archived: RkyvDeserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        let archived = rkyv::check_archived_root::<T>(data)
            .map_err(|e| SerializationError::Rkyv(e.to_string()))?;

        let deserialized = archived
            .deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
            .map_err(|e| SerializationError::Rkyv(e.to_string()))?;

        Ok(deserialized)
    }

    /// Serialize with format selection (postcard-format)
    #[inline]
    #[cfg(feature = "postcard-format")]
    pub fn to_bytes_with_format<T>(
        value: &T,
        format: SerializationFormat,
    ) -> Result<Vec<u8>, SerializationError>
    where
        T: PostcardSerializableMarker + RkyvSerialize<rkyv::ser::serializers::AllocSerializer<256>>,
    {
        match format {
            SerializationFormat::Network => to_bytes(value),
            SerializationFormat::Local => to_bytes_local(value),
        }
    }

    /// Serialize with format selection (non-postcard formats)
    #[inline]
    #[cfg(not(feature = "postcard-format"))]
    pub fn to_bytes_with_format<T>(
        value: &T,
        format: SerializationFormat,
    ) -> Result<Vec<u8>, SerializationError>
    where
        T: Serialize + RkyvSerialize<rkyv::ser::serializers::AllocSerializer<256>>,
    {
        match format {
            SerializationFormat::Network => to_bytes(value),
            SerializationFormat::Local => to_bytes_local(value),
        }
    }
}

/// Protocol-versioned serialization with wire-format version header.
///
/// Wire format: `[C0K_MAGIC: 4 bytes][version_u16_le: 2 bytes][payload: N bytes]`
///
/// All wire paths (daemon IPC, socket transport, P2P) use this format so that
/// any version mismatch is detected immediately on deserialization.
pub mod versioned {
    use super::*;
    use crate::versioned_message::C0K_MAGIC;

    /// Size of the version header: 4 bytes magic + 2 bytes version
    pub const VERSION_HEADER_LEN: usize = 6;

    /// Serialize with version header (postcard path).
    ///
    /// Produces: `[C0K_MAGIC][version_u16_le][postcard payload]`
    #[inline]
    pub fn serialize_with_version<T: Serialize>(
        value: &T,
        version: ProtocolVersion,
    ) -> Result<Vec<u8>, SerializationError> {
        let payload = crate::serialize::to_bytes_unchecked(value)?;
        let mut buf = Vec::with_capacity(VERSION_HEADER_LEN + payload.len());
        buf.extend_from_slice(&C0K_MAGIC);
        buf.extend_from_slice(&version.0.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Deserialize a versioned postcard message.
    ///
    /// Validates magic bytes and version range, returns `(value, wire_version)`.
    #[inline]
    pub fn deserialize_with_version<T: DeserializeOwned>(
        data: &[u8],
    ) -> Result<(T, ProtocolVersion), SerializationError> {
        if data.len() < VERSION_HEADER_LEN {
            return Err(SerializationError::DeserializationFailed(
                "Message too short for version header".to_string(),
            ));
        }

        // Validate magic
        if data[..4] != C0K_MAGIC {
            return Err(SerializationError::DeserializationFailed(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                C0K_MAGIC,
                &data[..4]
            )));
        }

        // Parse version
        let version = ProtocolVersion(u16::from_le_bytes([data[4], data[5]]));
        if !version.is_supported() {
            return Err(SerializationError::VersionMismatch {
                expected: ProtocolVersion::CURRENT.0,
                actual: version.0,
            });
        }

        // Deserialize payload
        let value: T = crate::serialize::from_bytes_unchecked(&data[VERSION_HEADER_LEN..])?;
        Ok((value, version))
    }

    /// Serialize with version header (rkyv path).
    ///
    /// Produces: `[C0K_MAGIC][version_u16_le][rkyv payload]`
    #[inline]
    pub fn serialize_with_version_local<T>(
        value: &T,
        version: ProtocolVersion,
    ) -> Result<Vec<u8>, SerializationError>
    where
        T: RkyvSerialize<rkyv::ser::serializers::AllocSerializer<256>>,
    {
        let payload = crate::serialize::to_bytes_local(value)?;
        let mut buf = Vec::with_capacity(VERSION_HEADER_LEN + payload.len());
        buf.extend_from_slice(&C0K_MAGIC);
        buf.extend_from_slice(&version.0.to_le_bytes());
        buf.extend_from_slice(&payload);
        Ok(buf)
    }

    /// Deserialize a versioned rkyv message.
    ///
    /// Validates magic bytes and version range, returns `(value, wire_version)`.
    #[inline]
    pub fn deserialize_with_version_local<T>(
        data: &[u8],
    ) -> Result<(T, ProtocolVersion), SerializationError>
    where
        T: Archive,
        T::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
        T::Archived: RkyvDeserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        if data.len() < VERSION_HEADER_LEN {
            return Err(SerializationError::DeserializationFailed(
                "Message too short for version header".to_string(),
            ));
        }

        // Validate magic
        if data[..4] != C0K_MAGIC {
            return Err(SerializationError::DeserializationFailed(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                C0K_MAGIC,
                &data[..4]
            )));
        }

        // Parse version
        let version = ProtocolVersion(u16::from_le_bytes([data[4], data[5]]));
        if !version.is_supported() {
            return Err(SerializationError::VersionMismatch {
                expected: ProtocolVersion::CURRENT.0,
                actual: version.0,
            });
        }

        // Copy payload to an aligned buffer — rkyv requires alignment that the
        // 6-byte prefix breaks. Vec allocation guarantees sufficient alignment.
        let payload = data[VERSION_HEADER_LEN..].to_vec();
        let value: T = crate::serialize::from_bytes_local_owned(&payload)?;
        Ok((value, version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postcard_serializable::PostcardSerializable;
    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
    use serde::{Deserialize, Serialize};

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Default,
        Serialize,
        Deserialize,
        PostcardSerializable,
        Archive,
        RkyvDeserialize,
        RkyvSerialize,
    )]
    #[archive(compare(PartialEq), check_bytes)]
    struct TestData {
        id: u64,
        name: String,
        data: Vec<u8>,
    }

    #[test]
    fn test_roundtrip() {
        let original = TestData {
            id: 42,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        let serialized = serialize::to_bytes(&original).unwrap();
        let deserialized: TestData = serialize::from_bytes(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_format_info() {
        // Test that format info functions work
        let name = serialize::format_name();
        let zero_copy = serialize::supports_zero_copy();

        #[cfg(feature = "bincode-format")]
        {
            assert_eq!(name, "bincode");
            assert!(!zero_copy);
        }

        #[cfg(feature = "postcard-format")]
        {
            assert_eq!(name, "postcard");
            assert!(zero_copy);
        }

        #[cfg(feature = "json-format")]
        {
            assert_eq!(name, "json");
            assert!(!zero_copy);
        }
    }

    #[test]
    fn test_versioned_serialization() {
        let data = TestData {
            id: 123,
            name: "versioned".to_string(),
            data: vec![10, 20, 30],
        };

        let version = ProtocolVersion::CURRENT;
        let serialized = versioned::serialize_with_version(&data, version).unwrap();

        // Verify header structure
        assert_eq!(&serialized[..4], &C0K_MAGIC);
        assert_eq!(
            u16::from_le_bytes([serialized[4], serialized[5]]),
            version.0
        );
        assert!(serialized.len() > versioned::VERSION_HEADER_LEN);

        let (deserialized, wire_version): (TestData, _) =
            versioned::deserialize_with_version(&serialized).unwrap();
        assert_eq!(data, deserialized);
        assert_eq!(wire_version, version);
    }

    #[test]
    fn test_versioned_rkyv_roundtrip() {
        let data = TestData {
            id: 456,
            name: "rkyv versioned".to_string(),
            data: vec![7, 8, 9],
        };

        let version = ProtocolVersion::CURRENT;
        let serialized = versioned::serialize_with_version_local(&data, version).unwrap();

        // Verify header
        assert_eq!(&serialized[..4], &C0K_MAGIC);
        assert_eq!(
            u16::from_le_bytes([serialized[4], serialized[5]]),
            version.0
        );

        let (deserialized, wire_version): (TestData, _) =
            versioned::deserialize_with_version_local(&serialized).unwrap();
        assert_eq!(data, deserialized);
        assert_eq!(wire_version, version);
    }

    #[test]
    fn test_versioned_invalid_magic() {
        let mut bad_data = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00];
        bad_data.extend_from_slice(&[0; 10]);

        let result: Result<(TestData, _), _> = versioned::deserialize_with_version(&bad_data);
        assert!(result.is_err());
        match result {
            Err(SerializationError::DeserializationFailed(msg)) => {
                assert!(msg.contains("Invalid magic bytes"));
            }
            other => panic!("Expected DeserializationFailed, got {:?}", other),
        }
    }

    #[test]
    fn test_versioned_unsupported_version() {
        // Build a valid-magic but unsupported version message
        let data = TestData {
            id: 1,
            name: "x".to_string(),
            data: vec![],
        };
        let mut serialized =
            versioned::serialize_with_version(&data, ProtocolVersion::CURRENT).unwrap();
        // Overwrite version to something unsupported (version 99)
        serialized[4] = 99;
        serialized[5] = 0;

        let result: Result<(TestData, _), _> = versioned::deserialize_with_version(&serialized);
        assert!(matches!(
            result,
            Err(SerializationError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn test_versioned_too_short() {
        let result: Result<(TestData, _), _> = versioned::deserialize_with_version(&[0x01, 0x02]);
        assert!(result.is_err());
    }

    #[test]
    fn test_rkyv_local_serialization() {
        let original = TestData {
            id: 999,
            name: "rkyv test".to_string(),
            data: vec![5, 10, 15, 20, 25],
        };

        // Test local serialization with rkyv
        let serialized = serialize::to_bytes_local(&original).unwrap();

        // Test zero-copy deserialization
        let archived = serialize::from_bytes_local::<TestData>(&serialized).unwrap();
        assert_eq!(archived.id, 999);
        assert_eq!(archived.name.as_str(), "rkyv test");
        assert_eq!(archived.data.as_slice(), &[5, 10, 15, 20, 25]);

        // Test deserializing to owned value
        let deserialized = serialize::from_bytes_local_owned::<TestData>(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_format_selection() {
        let data = TestData {
            id: 456,
            name: "format test".to_string(),
            data: vec![1, 2, 3],
        };

        // Test network format (postcard)
        let network_bytes =
            serialize::to_bytes_with_format(&data, SerializationFormat::Network).unwrap();
        let network_deserialized: TestData = serialize::from_bytes(&network_bytes).unwrap();
        assert_eq!(data, network_deserialized);

        // Test local format (rkyv)
        let local_bytes =
            serialize::to_bytes_with_format(&data, SerializationFormat::Local).unwrap();
        let local_deserialized =
            serialize::from_bytes_local_owned::<TestData>(&local_bytes).unwrap();
        assert_eq!(data, local_deserialized);

        // Verify that rkyv produces larger output (due to alignment)
        println!(
            "Network size: {} bytes, Local size: {} bytes",
            network_bytes.len(),
            local_bytes.len()
        );
        // Typically rkyv will be larger due to alignment padding
    }
}
