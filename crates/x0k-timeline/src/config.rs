//! Where the service keeps its state and what it listens on.
//!
//! Each comes from the environment, so whatever starts the service decides.
//! The defaults suit a service started by hand. The store's own location is
//! [`crate::timeline_dir`].

use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;

use crate::otlp::OTLP_HTTP_PORT;

/// The state root: `X0K_HOME`, else `~/.0k`.
pub fn state_root() -> PathBuf {
    std::env::var_os("X0K_HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::home_dir().map(|home| home.join(".0k")))
        .unwrap_or_else(|| PathBuf::from("/tmp/.0k"))
}

/// This service's directory under [`state_root`]: `profiles/<X0K_PROFILE>`, with the
/// profile `main` when none is named. Two profiles are two separate services.
pub fn state_dir() -> PathBuf {
    let profile = std::env::var("X0K_PROFILE").unwrap_or_else(|_| "main".to_string());
    state_root().join("profiles").join(profile)
}

/// The socket writers forward to: `X0K_TIMELINE_SOCKET`, else
/// `x0k-timeline.sock` in [`state_dir`].
pub fn ingest_socket() -> String {
    std::env::var("X0K_TIMELINE_SOCKET").unwrap_or_else(|_| {
        state_dir()
            .join("x0k-timeline.sock")
            .to_string_lossy()
            .into_owned()
    })
}

/// The OTLP/HTTP port: `X0K_TIMELINE_OTLP_PORT`, else the port exporters
/// default to. An unreadable value is refused rather than guessed at.
pub fn otlp_port() -> Result<u16, std::num::ParseIntError> {
    match std::env::var("X0K_TIMELINE_OTLP_PORT") {
        Ok(port) => port.parse(),
        Err(_) => Ok(OTLP_HTTP_PORT),
    }
}

/// The address of the query tools' MCP listener:
/// `X0K_TIMELINE_QUERY_MCP_LISTEN`, else port 3456 on loopback.
pub fn mcp_addr() -> Result<SocketAddr, AddrParseError> {
    std::env::var("X0K_TIMELINE_QUERY_MCP_LISTEN")
        .as_deref()
        .unwrap_or("127.0.0.1:3456")
        .parse()
}

/// Whether the writer starts paused: `X0K_TIMELINE_PAUSED` set to `1` or
/// `true`.
pub fn paused_at_start() -> bool {
    matches!(
        std::env::var("X0K_TIMELINE_PAUSED").as_deref(),
        Ok("1") | Ok("true")
    )
}
