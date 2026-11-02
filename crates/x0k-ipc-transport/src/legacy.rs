//! Transport abstraction layer for x0k IPC
//!
//! Provides pluggable transport implementations with shared memory (Linux) and TCP (fallback) support.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite};

#[path = "envelope.rs"]
pub mod envelope;
#[path = "framing.rs"]
pub mod framing;
#[path = "local_socket.rs"]
pub mod local_socket;
#[path = "tcp.rs"]
pub mod tcp;

/// Latency class for a transport
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LatencyClass {
    /// < 1ms (shared memory)
    UltraLow,
    /// 1-10ms (Unix domain sockets)
    VeryLow,
    /// 10-50ms (TCP localhost)
    Low,
    /// 50-200ms (TCP remote)
    Medium,
}

impl fmt::Display for LatencyClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LatencyClass::UltraLow => write!(f, "ultra-low (<1ms)"),
            LatencyClass::VeryLow => write!(f, "very-low (1-10ms)"),
            LatencyClass::Low => write!(f, "low (10-50ms)"),
            LatencyClass::Medium => write!(f, "medium (50-200ms)"),
        }
    }
}

/// Transport configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub enum TransportConfig {
    /// TCP transport configuration
    Tcp { host: String, port: u16 },
    /// Local socket transport (Unix domain sockets on Unix, named pipes on Windows)
    LocalSocket { path: String },
    /// Auto-detect best transport for platform
    #[default]
    Auto,
}


/// IPC connection trait - represents an established bidirectional connection
#[async_trait]
pub trait IpcConnection: Send + Sync {
    /// Split connection into read and write halves
    fn split(self: Box<Self>) -> (Box<dyn IpcReadHalf>, Box<dyn IpcWriteHalf>);

    /// Get the latency class of this connection
    fn latency_class(&self) -> LatencyClass;

    /// The process at the other end, as the kernel reports it
    /// (`SO_PEERCRED`), never as the peer claims; `None` where the
    /// transport cannot say.
    fn peer_pid(&self) -> Option<u32> {
        None
    }

    /// The effective uid of the process at the other end, as the kernel
    /// reports it; `None` where the transport cannot say, as over TCP.
    fn peer_uid(&self) -> Option<u32> {
        None
    }
}

/// Read half of a split connection
#[async_trait]
pub trait IpcReadHalf: AsyncRead + Send + Unpin + 'static + std::any::Any {}

/// Write half of a split connection
#[async_trait]
pub trait IpcWriteHalf: AsyncWrite + Send + Unpin + 'static {}

/// IPC listener trait - listens for incoming connections
#[async_trait]
pub trait IpcListener: Send + Sync {
    /// Accept a new connection
    async fn accept(&mut self) -> Result<(Box<dyn IpcConnection>, String)>;

    /// Get the local address/identifier for this listener
    fn local_addr(&self) -> String;
}

/// Transport trait - factory for creating connections and listeners
#[async_trait]
pub trait IpcTransport: Send + Sync {
    /// Connect to a server
    async fn connect(&self, config: &TransportConfig) -> Result<Box<dyn IpcConnection>>;

    /// Bind a listener
    async fn bind(&self, config: &TransportConfig) -> Result<Box<dyn IpcListener>>;

    /// Get the name of this transport
    fn name(&self) -> &'static str;

    /// Get the expected latency class for this transport
    fn latency_class(&self) -> LatencyClass;
}

/// Restrict a file/socket to owner-only access.
/// Unix: chmod 0600. Windows: no-op (auth token provides real security).
pub fn restrict_file_permissions(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Check if a host string refers to localhost
fn is_localhost(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "0.0.0.0" | "::")
}

/// Parse transport preference from environment variable
///
/// Checks `X0K_IPC_TRANSPORT` environment variable for explicit transport selection.
/// Valid values: "tcp", "local" (or "local_socket", "uds"), "auto"
fn get_transport_from_env() -> Option<String> {
    std::env::var("X0K_IPC_TRANSPORT").ok()
}

/// Auto-detect and connect using the best available transport
///
/// Selection priority:
/// 1. Environment variable override (X0K_IPC_TRANSPORT)
/// 2. Explicit config (Tcp or SharedMemory)
/// 3. Auto-detection based on platform and host
///
/// Auto-detection logic:
/// - Remote host → TCP (only option)
/// - Localhost on Linux → Try shared memory, fall back to TCP
/// - Localhost on other platforms → TCP
pub async fn connect_auto(config: &TransportConfig) -> Result<Box<dyn IpcConnection>> {
    let transport = select_transport_for_config(config, get_transport_from_env().as_deref())?;

    // Try to connect with selected transport
    match transport.connect(config).await {
        Ok(conn) => {
            tracing::debug!(
                "Connected using {} transport ({})",
                transport.name(),
                transport.latency_class()
            );
            Ok(conn)
        }
        Err(e) => {
            // If not TCP, try falling back to TCP
            if transport.name() != "tcp" {
                tracing::warn!(
                    "Failed to connect with {} transport: {}. Falling back to TCP",
                    transport.name(),
                    e
                );

                let tcp_transport = tcp::TcpTransport;
                let conn = tcp_transport.connect(config).await?;
                tracing::info!("Fell back to TCP transport successfully");
                Ok(conn)
            } else {
                Err(e)
            }
        }
    }
}

/// Auto-detect and bind using the best available transport
///
/// See `connect_auto` for selection logic.
pub async fn bind_auto(config: &TransportConfig) -> Result<Box<dyn IpcListener>> {
    let transport = select_transport_for_config(config, get_transport_from_env().as_deref())?;

    // Try to bind with selected transport
    match transport.bind(config).await {
        Ok(listener) => {
            tracing::info!(
                "Listening with {} transport at {} ({})",
                transport.name(),
                listener.local_addr(),
                transport.latency_class()
            );
            Ok(listener)
        }
        Err(e) => {
            // If not TCP, try falling back to TCP
            if transport.name() != "tcp" {
                tracing::warn!(
                    "Failed to bind with {} transport: {}. Falling back to TCP",
                    transport.name(),
                    e
                );

                let tcp_transport = tcp::TcpTransport;
                let listener = tcp_transport.bind(config).await?;
                tracing::info!(
                    "Fell back to TCP transport successfully at {}",
                    listener.local_addr()
                );
                Ok(listener)
            } else {
                Err(e)
            }
        }
    }
}

/// Select the best transport for the given configuration
///
/// This function only selects the transport - it doesn't attempt connection.
/// Fallback logic is handled by `connect_auto` and `bind_auto`.
///
/// `env_override` is the value of `X0K_IPC_TRANSPORT`, read by the caller,
/// so selection reads no process-global state.
fn select_transport_for_config(
    config: &TransportConfig,
    env_override: Option<&str>,
) -> Result<Box<dyn IpcTransport>> {
    // The environment override wins over the config
    if let Some(env_transport) = env_override {
        match env_transport.to_lowercase().as_str() {
            "tcp" => {
                tracing::debug!("Using TCP transport (forced by X0K_IPC_TRANSPORT)");
                return Ok(Box::new(tcp::TcpTransport));
            }
            "local" | "local_socket" | "uds" => {
                tracing::debug!("Using local socket transport (forced by X0K_IPC_TRANSPORT)");
                return Ok(Box::new(local_socket::LocalSocketTransport));
            }
            "auto" => {
                tracing::debug!("Auto-detecting transport (X0K_IPC_TRANSPORT=auto)");
                // Continue to auto-detection below
            }
            other => {
                anyhow::bail!(
                    "Invalid X0K_IPC_TRANSPORT value: '{}'. Valid values: tcp, local, auto",
                    other
                )
            }
        }
    }

    // Handle explicit configuration
    match config {
        TransportConfig::Tcp { .. } => {
            tracing::debug!("Using TCP transport (explicit config)");
            Ok(Box::new(tcp::TcpTransport))
        }
        TransportConfig::LocalSocket { .. } => {
            tracing::debug!("Using local socket transport (explicit config)");
            Ok(Box::new(local_socket::LocalSocketTransport))
        }
        TransportConfig::Auto => {
            // Auto-detect based on platform and target host

            // Extract host from config to determine if it's local or remote
            let is_local = match config {
                TransportConfig::Tcp { host, .. } => is_localhost(host),
                _ => true, // Auto mode defaults to local
            };

            if !is_local {
                // Remote connection - must use TCP
                tracing::debug!("Auto-selected TCP transport (remote host)");
                return Ok(Box::new(tcp::TcpTransport));
            }

            // Local connection - prefer local sockets (cross-platform, 5-20ms latency)
            tracing::debug!("Auto-selected local socket transport for localhost");
            Ok(Box::new(local_socket::LocalSocketTransport))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_class_ordering() {
        assert!(LatencyClass::UltraLow < LatencyClass::VeryLow);
        assert!(LatencyClass::VeryLow < LatencyClass::Low);
        assert!(LatencyClass::Low < LatencyClass::Medium);
    }

    #[test]
    fn test_default_config() {
        let config = TransportConfig::default();
        assert!(matches!(config, TransportConfig::Auto));
    }

    #[test]
    fn test_is_localhost() {
        assert!(is_localhost("localhost"));
        assert!(is_localhost("127.0.0.1"));
        assert!(is_localhost("::1"));
        assert!(is_localhost("0.0.0.0"));
        assert!(is_localhost("::"));

        assert!(!is_localhost("192.168.1.1"));
        assert!(!is_localhost("example.com"));
        assert!(!is_localhost("10.0.0.1"));
    }

    #[test]
    fn test_transport_selection_explicit_tcp() {
        let config = TransportConfig::Tcp {
            host: "127.0.0.1".to_string(),
            port: 30301,
        };

        let transport = select_transport_for_config(&config, None).unwrap();
        assert_eq!(transport.name(), "tcp");
    }

    #[test]
    fn test_transport_selection_auto_defaults_to_local_socket() {
        // Without an override, localhost selects the local socket
        let config = TransportConfig::Auto;
        let transport = select_transport_for_config(&config, None).unwrap();
        assert_eq!(transport.name(), "local_socket");
    }

    #[test]
    fn test_env_override_tcp() {
        let config = TransportConfig::Auto;
        let transport = select_transport_for_config(&config, Some("tcp")).unwrap();
        assert_eq!(transport.name(), "tcp");
    }

    #[test]
    fn test_env_override_invalid() {
        let config = TransportConfig::Auto;
        let result = select_transport_for_config(&config, Some("invalid"));

        match result {
            Err(e) => {
                assert!(e.to_string().contains("Invalid X0K_IPC_TRANSPORT"));
            }
            Ok(_) => {
                panic!("Expected error for invalid transport, but got Ok");
            }
        }
    }

    #[tokio::test]
    async fn test_connect_auto_uses_tcp() {
        let config = TransportConfig::Tcp {
            host: "127.0.0.1".to_string(),
            port: 0, // Will fail to connect, but that's ok for this test
        };

        // This should attempt to connect with TCP
        // It will fail because nothing is listening, but we're just testing the selection
        let result = connect_auto(&config).await;
        assert!(result.is_err()); // Expected to fail - nothing listening
    }
}
