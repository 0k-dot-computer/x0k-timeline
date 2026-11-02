//! TCP transport implementation

use crate::{
    IpcConnection, IpcListener, IpcReadHalf, IpcTransport, IpcWriteHalf, LatencyClass,
    TransportConfig,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

/// TCP transport implementation
pub struct TcpTransport;

/// TCP connection wrapper
pub struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl IpcConnection for TcpConnection {
    fn split(self: Box<Self>) -> (Box<dyn IpcReadHalf>, Box<dyn IpcWriteHalf>) {
        let (read, write) = self.stream.into_split();
        (
            Box::new(TcpReadHalf(read)) as Box<dyn IpcReadHalf>,
            Box::new(TcpWriteHalf(write)) as Box<dyn IpcWriteHalf>,
        )
    }

    fn latency_class(&self) -> LatencyClass {
        // TCP on localhost is typically 10-50ms
        LatencyClass::Low
    }
}

/// TCP read half wrapper
pub struct TcpReadHalf(OwnedReadHalf);

impl IpcReadHalf for TcpReadHalf {}

impl AsyncRead for TcpReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

/// TCP write half wrapper
pub struct TcpWriteHalf(OwnedWriteHalf);

impl IpcWriteHalf for TcpWriteHalf {}

impl AsyncWrite for TcpWriteHalf {
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

/// TCP listener wrapper
pub struct TcpListenerWrapper {
    listener: TcpListener,
    local_addr: String,
}

#[async_trait]
impl IpcListener for TcpListenerWrapper {
    async fn accept(&mut self) -> Result<(Box<dyn IpcConnection>, String)> {
        let (stream, addr) = self.listener.accept().await?;
        stream.set_nodelay(true)?;
        let conn = Box::new(TcpConnection::new(stream)) as Box<dyn IpcConnection>;
        Ok((conn, addr.to_string()))
    }

    fn local_addr(&self) -> String {
        self.local_addr.clone()
    }
}

#[async_trait]
impl IpcTransport for TcpTransport {
    async fn connect(&self, config: &TransportConfig) -> Result<Box<dyn IpcConnection>> {
        let (host, port) = match config {
            TransportConfig::Tcp { host, port } => (host.as_str(), *port),
            TransportConfig::Auto => ("127.0.0.1", 30301), // Default x0k daemon port
            _ => return Err(anyhow!("Invalid configuration for TCP transport")),
        };

        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Box::new(TcpConnection::new(stream)))
    }

    async fn bind(&self, config: &TransportConfig) -> Result<Box<dyn IpcListener>> {
        let (host, port) = match config {
            TransportConfig::Tcp { host, port } => (host.as_str(), *port),
            TransportConfig::Auto => ("127.0.0.1", 30301),
            _ => return Err(anyhow!("Invalid configuration for TCP transport")),
        };

        let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
        let local_addr = listener.local_addr()?.to_string();

        Ok(Box::new(TcpListenerWrapper {
            listener,
            local_addr,
        }))
    }

    fn name(&self) -> &'static str {
        "tcp"
    }

    fn latency_class(&self) -> LatencyClass {
        LatencyClass::Low
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tcp_transport_loopback() {
        // Bind listener
        let config = TransportConfig::Tcp {
            host: "127.0.0.1".to_string(),
            port: 0, // OS assigns port
        };

        let transport = TcpTransport;
        let mut listener = transport.bind(&config).await.unwrap();

        // Extract actual port
        let addr = listener.local_addr();
        let port: u16 = addr.split(':').next_back().unwrap().parse().unwrap();

        // Spawn accept task
        let accept_task = tokio::spawn(async move { listener.accept().await.unwrap() });

        // Connect
        let connect_config = TransportConfig::Tcp {
            host: "127.0.0.1".to_string(),
            port,
        };
        let _client_conn = transport.connect(&connect_config).await.unwrap();

        // Complete accept
        let (_server_conn, _client_addr) = accept_task.await.unwrap();
    }
}
