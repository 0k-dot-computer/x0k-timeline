//! Versioned message wrapper for protocol compatibility
//!
//! This module provides a wrapper that includes version information with every message,
//! enabling protocol evolution while maintaining backward compatibility.

use crate::{serialize, ProtocolVersion, SerializationError};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postcard-format")]
use postcard_serializable_types::PostcardSerializable;

/// Magic bytes to identify x0k protocol messages
pub const C0K_MAGIC: [u8; 4] = [b'L', b'3', b'0', 0x01];

/// Wrapper for versioned network messages
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VersionedMessage<T> {
    /// Magic bytes for protocol identification
    pub magic: [u8; 4],
    /// Protocol version used for this message
    pub version: ProtocolVersion,
    /// The actual message payload
    pub payload: T,
}

#[cfg(feature = "postcard-format")]
impl<T: PostcardSerializable> PostcardSerializable for VersionedMessage<T> {}

#[cfg(feature = "postcard-format")]
impl<T: PostcardSerializable> VersionedMessage<T> {
    /// Create a new versioned message with current protocol version
    pub fn new(payload: T) -> Self {
        Self {
            magic: C0K_MAGIC,
            version: ProtocolVersion::CURRENT,
            payload,
        }
    }

    /// Create a versioned message with a specific version
    pub fn with_version(payload: T, version: ProtocolVersion) -> Self {
        Self {
            magic: C0K_MAGIC,
            version,
            payload,
        }
    }

    /// Serialize the versioned message to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        serialize::to_bytes(self)
    }

    /// Deserialize a versioned message from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, SerializationError> {
        // First check if we have enough bytes for magic
        if data.len() < 4 {
            return Err(SerializationError::DeserializationFailed(
                "Message too short to contain magic bytes".to_string(),
            ));
        }

        let msg: Self = serialize::from_bytes(data)?;

        // Verify magic bytes
        if msg.magic != C0K_MAGIC {
            return Err(SerializationError::DeserializationFailed(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                C0K_MAGIC, msg.magic
            )));
        }

        // Check version compatibility
        if !msg.version.is_supported() {
            return Err(SerializationError::VersionMismatch {
                expected: ProtocolVersion::CURRENT.0,
                actual: msg.version.0,
            });
        }

        Ok(msg)
    }
}

#[cfg(not(feature = "postcard-format"))]
impl<T: Serialize> VersionedMessage<T> {
    /// Create a new versioned message with current protocol version
    pub fn new(payload: T) -> Self {
        Self {
            magic: C0K_MAGIC,
            version: ProtocolVersion::CURRENT,
            payload,
        }
    }

    /// Create a versioned message with a specific version
    pub fn with_version(payload: T, version: ProtocolVersion) -> Self {
        Self {
            magic: C0K_MAGIC,
            version,
            payload,
        }
    }

    /// Serialize the versioned message to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        serialize::to_bytes(self)
    }
}

#[cfg(not(feature = "postcard-format"))]
impl<T: for<'de> Deserialize<'de>> VersionedMessage<T> {
    /// Deserialize a versioned message from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, SerializationError> {
        // First check if we have enough bytes for magic
        if data.len() < 4 {
            return Err(SerializationError::DeserializationFailed(
                "Message too short to contain magic bytes".to_string(),
            ));
        }

        let msg: Self = serialize::from_bytes(data)?;

        // Verify magic bytes
        if msg.magic != C0K_MAGIC {
            return Err(SerializationError::DeserializationFailed(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                C0K_MAGIC, msg.magic
            )));
        }

        // Check version compatibility
        if !msg.version.is_supported() {
            return Err(SerializationError::VersionMismatch {
                expected: ProtocolVersion::CURRENT.0,
                actual: msg.version.0,
            });
        }

        Ok(msg)
    }
}

/// Version negotiation message for connection handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionNegotiation {
    /// Minimum protocol version supported
    pub min_version: ProtocolVersion,
    /// Maximum protocol version supported
    pub max_version: ProtocolVersion,
    /// Preferred protocol version
    pub preferred_version: ProtocolVersion,
    /// Optional feature flags for this version
    pub features: Vec<String>,
}

/// The feature a device advertises when it accepts device-admission
/// progress frames on its core stream; an admitter sends them to no peer
/// whose stream did not advertise it.
pub const DEVICE_ADMISSION_PROGRESS: &str = "device_admission_progress";
/// The feature a node advertises when it understands
/// `ContactHandshakeMessage::GrantsReissued`.
pub const PAIRING_GRANTS_REISSUE: &str = "pairing_grants_reissue";

impl VersionNegotiation {
    /// Create a version negotiation message with current capabilities
    pub fn current() -> Self {
        Self {
            min_version: ProtocolVersion::MIN_SUPPORTED,
            max_version: ProtocolVersion::CURRENT,
            preferred_version: ProtocolVersion::CURRENT,
            features: vec![
                "binary_serialization".to_string(),
                "event_journal_sync".to_string(),
                DEVICE_ADMISSION_PROGRESS.to_string(),
                PAIRING_GRANTS_REISSUE.to_string(),
            ],
        }
    }

    /// Negotiate the best common version with a peer
    pub fn negotiate_with(&self, peer: &VersionNegotiation) -> Option<ProtocolVersion> {
        // Find the overlap range
        let min = self.min_version.0.max(peer.min_version.0);
        let max = self.max_version.0.min(peer.max_version.0);

        if min > max {
            // No common version
            return None;
        }

        // Use the highest common version
        Some(ProtocolVersion(max))
    }

    /// Serialize to versioned postcard bytes.
    ///
    /// Single source of truth for VersionNegotiation wire format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        crate::versioned::serialize_with_version(self, ProtocolVersion::CURRENT)
    }

    /// Deserialize from versioned postcard bytes.
    ///
    /// Single source of truth for VersionNegotiation wire format.
    pub fn from_bytes(data: &[u8]) -> Result<(Self, ProtocolVersion), SerializationError> {
        crate::versioned::deserialize_with_version::<VersionNegotiation>(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postcard_serializable::PostcardSerializable;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, PostcardSerializable)]
    struct TestMessage {
        id: u64,
        data: String,
    }

    #[test]
    fn test_versioned_message_roundtrip() {
        let msg = TestMessage {
            id: 42,
            data: "test".to_string(),
        };

        let versioned = VersionedMessage::new(msg.clone());
        let bytes = versioned.to_bytes().unwrap();
        let decoded: VersionedMessage<TestMessage> = VersionedMessage::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.magic, C0K_MAGIC);
        assert_eq!(decoded.version, ProtocolVersion::CURRENT);
        assert_eq!(decoded.payload, msg);
    }

    #[test]
    fn test_version_negotiation() {
        let v1 = VersionNegotiation {
            min_version: ProtocolVersion(2),
            max_version: ProtocolVersion(4),
            preferred_version: ProtocolVersion(4),
            features: vec![],
        };

        let v2 = VersionNegotiation {
            min_version: ProtocolVersion(3),
            max_version: ProtocolVersion(5),
            preferred_version: ProtocolVersion(5),
            features: vec![],
        };

        let negotiated = v1.negotiate_with(&v2);
        assert_eq!(negotiated, Some(ProtocolVersion(4)));

        // Test no overlap
        let v3 = VersionNegotiation {
            min_version: ProtocolVersion(6),
            max_version: ProtocolVersion(7),
            preferred_version: ProtocolVersion(7),
            features: vec![],
        };

        let negotiated = v1.negotiate_with(&v3);
        assert_eq!(negotiated, None);
    }

    #[test]
    fn test_version_negotiation_roundtrip() {
        let neg = VersionNegotiation::current();
        let bytes = neg.to_bytes().unwrap();
        let (decoded, version) = VersionNegotiation::from_bytes(&bytes).unwrap();

        assert_eq!(version, ProtocolVersion::CURRENT);
        assert_eq!(decoded.min_version, ProtocolVersion::MIN_SUPPORTED);
        assert_eq!(decoded.max_version, ProtocolVersion::CURRENT);
        assert_eq!(decoded.preferred_version, ProtocolVersion::CURRENT);
        assert_eq!(decoded.features, neg.features);
    }

    #[test]
    fn test_invalid_magic() {
        let mut bytes = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Wrong magic
        bytes.extend_from_slice(&[0; 100]); // Some data

        let result: Result<VersionedMessage<TestMessage>, _> = VersionedMessage::from_bytes(&bytes);
        assert!(result.is_err());

        if let Err(SerializationError::DeserializationFailed(msg)) = result {
            assert!(msg.contains("Invalid magic bytes") || msg.contains("Failed to deserialize"));
        } else {
            panic!("Expected deserialization error");
        }
    }
}
