//! Local socket transport implementation using interprocess crate
//!
//! This transport uses interprocess::local_socket for cross-platform local IPC.
//! On Unix platforms, this uses Unix domain sockets; on Windows, it uses named pipes.
//! Expected latency: 5-20ms (much better than TCP's 50-200ms, simpler than shared memory's <5ms)

// The connection's read and write halves are held apart for its life, which the
// `split` that interprocess deprecates is for.
#![allow(deprecated)]

use crate::{
    IpcConnection, IpcListener, IpcReadHalf, IpcTransport, IpcWriteHalf, LatencyClass,
    TransportConfig,
};
use anyhow::{Context as _, Result, anyhow};
use async_trait::async_trait;
use interprocess::local_socket::{
    GenericFilePath, ListenerOptions, ToFsName,
    tokio::{RecvHalf, SendHalf, Stream, prelude::*},
};
#[cfg(target_os = "linux")]
use std::os::fd::AsFd;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::debug;

/// Get the socket path for the x0k daemon
///
/// When running in sandbox mode (X0K_SANDBOX_ID is set), requires X0K_IPC_SOCKET_PATH
/// to be explicitly set. This prevents cross-sandbox daemon leakage where a sandboxed
/// process might accidentally connect to the system daemon or another sandbox's daemon.
fn get_socket_path() -> Result<String> {
    // Check environment variable override
    if let Ok(path) = std::env::var("X0K_IPC_SOCKET_PATH") {
        return Ok(path);
    }

    // In sandbox mode (X0K_SANDBOX_ID set), NEVER fall back to system socket.
    // This prevents cross-sandbox daemon leakage.
    if std::env::var("X0K_SANDBOX_ID").is_ok() {
        return Err(anyhow!(
            "X0K_IPC_SOCKET_PATH is required when running in sandbox mode (X0K_SANDBOX_ID is set). \
             Falling back to system daemon is not allowed to prevent cross-sandbox leakage."
        ));
    }

    // Default to ~/.0k/x0kd.sock (in the directory-bound path for sandbox compatibility).
    // Fall back to the per-profile runtime dir when HOME is unavailable —
    // previously this hit /tmp/x0kd.sock, which collided between concurrent profiles.
    // Duplicates `x0k_env::runtime_dir()` logic to avoid a cyclic dep edge
    // from x0k-ipc-transport into x0k-env (x0k-env's own crates depend on transport).
    let default_path = dirs::home_dir()
        .map(|h| h.join(".0k/x0kd.sock").to_string_lossy().to_string())
        .unwrap_or_else(|| {
            let runtime_base = std::env::var("X0K_RUNTIME_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| {
                    let xdg =
                        std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());
                    let profile = std::env::var("X0K_PROFILE")
                        .ok()
                        .and_then(|v| {
                            let v = v.trim().to_string();
                            if v.is_empty() { None } else { Some(v) }
                        })
                        .unwrap_or_else(|| "default".to_string());
                    std::path::PathBuf::from(xdg).join("x0k").join(profile)
                });
            let _ = std::fs::create_dir_all(&runtime_base);
            runtime_base.join("x0kd.sock").to_string_lossy().to_string()
        });

    Ok(default_path)
}

/// Local socket transport implementation
pub struct LocalSocketTransport;

/// Local socket connection wrapper
pub struct LocalSocketConnection {
    stream: Stream,
}

impl LocalSocketConnection {
    fn new(stream: Stream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl IpcConnection for LocalSocketConnection {
    fn split(self: Box<Self>) -> (Box<dyn IpcReadHalf>, Box<dyn IpcWriteHalf>) {
        let (read, write) = self.stream.split();
        (
            Box::new(LocalSocketReadHalf(read)) as Box<dyn IpcReadHalf>,
            Box::new(LocalSocketWriteHalf(write)) as Box<dyn IpcWriteHalf>,
        )
    }

    fn latency_class(&self) -> LatencyClass {
        // Local sockets are very low latency: 5-20ms typical
        LatencyClass::VeryLow
    }

    fn peer_pid(&self) -> Option<u32> {
        use interprocess::local_socket::traits::StreamCommon as _;
        u32::try_from(self.stream.peer_creds().ok()?.pid()?).ok()
    }

    #[cfg(unix)]
    fn peer_uid(&self) -> Option<u32> {
        use interprocess::local_socket::traits::StreamCommon as _;
        self.stream.peer_creds().ok()?.euid()
    }
}

/// Local socket read half wrapper
pub struct LocalSocketReadHalf(RecvHalf);

impl LocalSocketReadHalf {
    /// Get the underlying file descriptor number (Unix only)
    ///
    /// This is useful for debugging socket lifecycle issues.
    #[cfg(unix)]
    pub fn raw_fd(&self) -> i32 {
        use std::os::fd::AsFd;
        // RecvHalf is an enum - on Unix it has UdSocket variant
        match &self.0 {
            RecvHalf::UdSocket(inner) => inner.as_fd().as_raw_fd(),
        }
    }
}

impl IpcReadHalf for LocalSocketReadHalf {}

impl AsyncRead for LocalSocketReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

/// Local socket write half wrapper
pub struct LocalSocketWriteHalf(SendHalf);

impl IpcWriteHalf for LocalSocketWriteHalf {}

impl AsyncWrite for LocalSocketWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Extract peer process info from a local socket stream.
///
/// On Linux, uses `SO_PEERCRED` to get the connecting process's PID.
/// On other platforms, returns a generic identifier.
fn peer_info_from_stream(stream: &Stream) -> String {
    #[cfg(target_os = "linux")]
    {
        let Stream::UdSocket(ref inner) = *stream;
        let fd = inner.as_fd().as_raw_fd();
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let ret = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut cred as *mut libc::ucred as *mut libc::c_void,
                &mut len,
            )
        };
        if ret == 0 {
            format!("pid={}", cred.pid)
        } else {
            debug!("SO_PEERCRED failed: {}", std::io::Error::last_os_error());
            "local".to_string()
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = stream;
        "local".to_string()
    }
}

/// Local socket listener wrapper
pub struct LocalSocketListenerWrapper {
    listener: interprocess::local_socket::tokio::Listener,
    local_addr: String,
}

#[async_trait]
impl IpcListener for LocalSocketListenerWrapper {
    async fn accept(&mut self) -> Result<(Box<dyn IpcConnection>, String)> {
        let stream = self
            .listener
            .accept()
            .await
            .context("Failed to accept connection on local socket")?;

        let peer_info = peer_info_from_stream(&stream);

        let conn = Box::new(LocalSocketConnection::new(stream)) as Box<dyn IpcConnection>;
        Ok((conn, peer_info))
    }

    fn local_addr(&self) -> String {
        self.local_addr.clone()
    }
}

#[async_trait]
impl IpcTransport for LocalSocketTransport {
    async fn connect(&self, config: &TransportConfig) -> Result<Box<dyn IpcConnection>> {
        let socket_path = match config {
            TransportConfig::LocalSocket { path } => path.clone(),
            TransportConfig::Auto => get_socket_path()?,
            _ => return Err(anyhow!("Invalid configuration for local socket transport")),
        };

        debug!("Connecting to local socket: {}", socket_path);

        let name = socket_path
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("Failed to create socket name from path: {}", socket_path))?;

        let stream = Stream::connect(name)
            .await
            .with_context(|| format!("Failed to connect to local socket: {}", socket_path))?;

        Ok(Box::new(LocalSocketConnection::new(stream)))
    }

    async fn bind(&self, config: &TransportConfig) -> Result<Box<dyn IpcListener>> {
        let socket_path = match config {
            TransportConfig::LocalSocket { path } => path.clone(),
            TransportConfig::Auto => get_socket_path()?,
            _ => return Err(anyhow!("Invalid configuration for local socket transport")),
        };

        debug!("Binding local socket: {}", socket_path);

        // Remove existing socket file if it exists (cleanup stale sockets)
        let _ = std::fs::remove_file(&socket_path);

        let name = socket_path
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("Failed to create socket name from path: {}", socket_path))?;

        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .with_context(|| format!("Failed to bind local socket: {}", socket_path))?;

        // Restrict socket to owner-only access (defense-in-depth)
        if let Err(e) = crate::restrict_file_permissions(std::path::Path::new(&socket_path)) {
            tracing::warn!("Failed to restrict socket permissions: {}", e);
        }

        Ok(Box::new(LocalSocketListenerWrapper {
            listener,
            local_addr: socket_path,
        }))
    }

    fn name(&self) -> &'static str {
        "local_socket"
    }

    fn latency_class(&self) -> LatencyClass {
        LatencyClass::VeryLow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Mutex to ensure env var tests don't interfere with each other
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_get_socket_path_default() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Clear both env vars to test default behavior
        // SAFETY: We hold the ENV_MUTEX, ensuring no concurrent env var access in tests
        unsafe {
            std::env::remove_var("X0K_IPC_SOCKET_PATH");
            std::env::remove_var("X0K_SANDBOX_ID");
        }

        let result = get_socket_path();
        assert!(result.is_ok());
        let path = result.unwrap();
        // Default path should be ~/.0k/x0kd.sock or $X0K_RUNTIME_DIR/x0kd.sock.
        // The latter form always ends in `/x0kd.sock` regardless of instance.
        assert!(
            path.ends_with(".0k/x0kd.sock") || path.ends_with("/x0kd.sock"),
            "Expected path ending with .0k/x0kd.sock or /x0kd.sock, got: {}",
            path
        );
    }

    #[test]
    fn test_get_socket_path_with_override() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let custom_path = "/custom/socket.sock";
        // SAFETY: We hold the ENV_MUTEX, ensuring no concurrent env var access in tests
        unsafe {
            std::env::set_var("X0K_IPC_SOCKET_PATH", custom_path);
            std::env::remove_var("X0K_SANDBOX_ID");
        }

        let result = get_socket_path();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), custom_path);

        // SAFETY: cleanup
        unsafe {
            std::env::remove_var("X0K_IPC_SOCKET_PATH");
        }
    }

    #[test]
    fn test_get_socket_path_sandbox_mode_with_socket_path() {
        let _lock = ENV_MUTEX.lock().unwrap();
        let sandbox_socket = "/workspace/.0k/x0kd-abc123.sock";
        // SAFETY: We hold the ENV_MUTEX, ensuring no concurrent env var access in tests
        unsafe {
            std::env::set_var("X0K_SANDBOX_ID", "abc123");
            std::env::set_var("X0K_IPC_SOCKET_PATH", sandbox_socket);
        }

        let result = get_socket_path();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), sandbox_socket);

        // SAFETY: cleanup
        unsafe {
            std::env::remove_var("X0K_SANDBOX_ID");
            std::env::remove_var("X0K_IPC_SOCKET_PATH");
        }
    }

    #[test]
    fn test_get_socket_path_sandbox_mode_without_socket_path_fails() {
        let _lock = ENV_MUTEX.lock().unwrap();
        // Set sandbox ID but NOT socket path - this should fail
        // SAFETY: We hold the ENV_MUTEX, ensuring no concurrent env var access in tests
        unsafe {
            std::env::set_var("X0K_SANDBOX_ID", "abc123");
            std::env::remove_var("X0K_IPC_SOCKET_PATH");
        }

        let result = get_socket_path();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("X0K_IPC_SOCKET_PATH is required"));
        assert!(err_msg.contains("sandbox mode"));

        // SAFETY: cleanup
        unsafe {
            std::env::remove_var("X0K_SANDBOX_ID");
        }
    }

    #[tokio::test]
    async fn test_local_socket_transport_basic() {
        let socket_path = format!("/tmp/test_c0k_{}.sock", std::process::id());
        let config = TransportConfig::LocalSocket {
            path: socket_path.clone(),
        };

        let transport = LocalSocketTransport;

        // Bind listener
        let mut listener = transport.bind(&config).await.unwrap();

        // Spawn server task
        let server_task = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let (mut read, mut write) = conn.split();

            let mut buf = vec![0u8; 1024];
            let n = read.read(&mut buf).await.unwrap();
            let received = String::from_utf8(buf[..n].to_vec()).unwrap();

            write.write_all(received.as_bytes()).await.unwrap();
            write.flush().await.unwrap();
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Connect client
        let conn = transport.connect(&config).await.unwrap();
        let (mut read, mut write) = conn.split();

        // Send message
        write.write_all(b"Hello, Local Socket!").await.unwrap();
        write.flush().await.unwrap();

        // Read response
        let mut buf = vec![0u8; 1024];
        let n = read.read(&mut buf).await.unwrap();
        let response = String::from_utf8(buf[..n].to_vec()).unwrap();

        assert_eq!(response, "Hello, Local Socket!");

        server_task.await.unwrap();

        // Cleanup is handled automatically by interprocess
    }

    /// An accepted connection names its peer by the kernel's credentials:
    /// a client in this process is this process's pid.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_accepted_connection_names_its_peer_pid() {
        let dir = tempfile::tempdir().unwrap();
        let config = TransportConfig::LocalSocket { path: dir.path().join("peer.sock").display().to_string() };
        let transport = LocalSocketTransport;
        let mut listener = transport.bind(&config).await.unwrap();
        let accepted = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let _client = transport.connect(&config).await.unwrap();
        let server_side = accepted.await.unwrap();
        assert_eq!(server_side.peer_pid(), Some(std::process::id()));
    }
}
