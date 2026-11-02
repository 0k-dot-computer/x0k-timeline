//! Length-prefixed message framing for IPC
//!
//! Provides simple send/recv functions that handle:
//! - 4-byte little-endian length prefix
//! - Postcard binary serialization via x0k-serialization
//!
//! # Example
//!
//! ```ignore
//! use x0k_ipc_transport::framing::{send, recv};
//!
//! // Send a message
//! send(&mut writer, &MyRequest { ... }).await?;
//!
//! // Receive a response
//! let response: MyResponse = recv(&mut reader).await?;
//! ```

use anyhow::{Context, Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use x0k_serialization::{ProtocolVersion, versioned};

/// Maximum message size (10 MB), enforced on both ends: [`send`] refuses a
/// larger message before writing a byte, and [`recv`] refuses a larger length
/// prefix. Public so a sender that batches (the timeline crash-queue drain) can
/// size its requests to fit instead of discovering the cap by failing.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Send a length-prefixed versioned message
///
/// Format: [4-byte LE length][versioned postcard payload]
/// where versioned payload = `[C0K_MAGIC:4][version:2][postcard bytes]`
pub async fn send<T: Serialize, W: AsyncWrite + Unpin>(writer: &mut W, msg: &T) -> Result<()> {
    let data = versioned::serialize_with_version(msg, ProtocolVersion::CURRENT)
        .context("Failed to serialize message")?;

    if data.len() > MAX_MESSAGE_SIZE {
        return Err(anyhow!("Message too large: {} bytes", data.len()));
    }

    let len = (data.len() as u32).to_le_bytes();
    writer
        .write_all(&len)
        .await
        .context("Failed to write length prefix")?;
    writer
        .write_all(&data)
        .await
        .context("Failed to write message body")?;
    writer.flush().await.context("Failed to flush")?;

    Ok(())
}

/// Receive a length-prefixed message
///
/// Returns Ok(Some(msg)) on success, Ok(None) on clean disconnect (EOF on length read)
pub async fn recv<T: DeserializeOwned, R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<T>> {
    // Read 4-byte length prefix
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None); // Clean disconnect
        }
        Err(e) => {
            return Err(anyhow!("Failed to read length prefix: {}", e));
        }
    }

    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(anyhow!("Message too large: {} bytes", len));
    }

    // Read message body
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .context("Failed to read message body")?;

    // Deserialize versioned message
    let (msg, _version) = versioned::deserialize_with_version(&buf).with_context(|| {
        format!(
            "Failed to deserialize message ({} bytes, first 32: {:?})",
            buf.len(),
            &buf[..buf.len().min(32)]
        )
    })?;
    Ok(Some(msg))
}

/// Receive a length-prefixed message, returning error on disconnect
///
/// Unlike `recv`, this returns an error instead of None on disconnect.
/// Use when you expect more messages and disconnect is unexpected.
pub async fn recv_required<T: DeserializeOwned, R: AsyncRead + Unpin>(reader: &mut R) -> Result<T> {
    recv(reader)
        .await?
        .ok_or_else(|| anyhow!("Unexpected disconnect"))
}

/// Serve one framed connection: receive each request, answer it with
/// `respond`, send the answer, until the peer disconnects.
pub async fn serve_framed<Req, Resp, R, W, F, Fut>(reader: &mut R, writer: &mut W, mut respond: F) -> Result<()>
where
    Req: DeserializeOwned,
    Resp: Serialize,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut(Req) -> Fut,
    Fut: std::future::Future<Output = Resp>,
{
    while let Some(request) = recv::<Req, _>(reader).await? {
        let response = respond(request).await;
        send(writer, &response).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestMessage {
        id: u64,
        name: String,
        data: Vec<u8>,
    }

    #[tokio::test]
    async fn test_roundtrip() {
        let msg = TestMessage {
            id: 42,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        // Write to buffer
        let mut buf = Vec::new();
        send(&mut buf, &msg).await.unwrap();

        // Read back
        let mut cursor = std::io::Cursor::new(buf);
        let received: TestMessage = recv_required(&mut cursor).await.unwrap();

        assert_eq!(msg, received);
    }

    #[tokio::test]
    async fn test_empty_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let result: Option<TestMessage> = recv(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let msg1 = TestMessage {
            id: 1,
            name: "first".to_string(),
            data: vec![],
        };
        let msg2 = TestMessage {
            id: 2,
            name: "second".to_string(),
            data: vec![255],
        };

        let mut buf = Vec::new();
        send(&mut buf, &msg1).await.unwrap();
        send(&mut buf, &msg2).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let r1: TestMessage = recv_required(&mut cursor).await.unwrap();
        let r2: TestMessage = recv_required(&mut cursor).await.unwrap();

        assert_eq!(msg1, r1);
        assert_eq!(msg2, r2);
    }
}
