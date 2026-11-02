//! The writer's operations, called over its socket. A tool may run in the
//! resident service or in a process of its own, so it reaches the writer the
//! one way both can.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use x0k_ipc_transport::envelope::{RequestEnvelope, ResponseEnvelope};
use x0k_ipc_transport::local_socket::LocalSocketTransport;
use x0k_ipc_transport::{framing, IpcTransport, TransportConfig};
use x0k_serialization::{serialize, PostcardSerializableTrait};

/// How long a tool waits on the writer before answering without it.
const CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// Call `method` on the writer at `socket`.
pub async fn call_at<Op, Answer>(socket: &str, method: &str, op: &Op) -> Result<Answer>
where
    Op: Serialize + PostcardSerializableTrait,
    Answer: DeserializeOwned + PostcardSerializableTrait,
{
    tokio::time::timeout(CALL_TIMEOUT, exchange_with_writer(socket, method, op))
        .await
        .with_context(|| format!("the writer at {socket} did not answer"))?
}

/// Call `method` on the configured writer.
pub async fn call_writer<Op, Answer>(method: &str, op: &Op) -> Result<Answer>
where
    Op: Serialize + PostcardSerializableTrait,
    Answer: DeserializeOwned + PostcardSerializableTrait,
{
    call_at(&crate::config::ingest_socket(), method, op).await
}

async fn exchange_with_writer<Op, Answer>(socket: &str, method: &str, op: &Op) -> Result<Answer>
where
    Op: Serialize + PostcardSerializableTrait,
    Answer: DeserializeOwned + PostcardSerializableTrait,
{
    let config = TransportConfig::LocalSocket {
        path: socket.to_string(),
    };
    let connection = LocalSocketTransport.connect(&config).await?;
    let (mut reader, mut writer) = connection.split();
    let envelope = RequestEnvelope {
        id: 1,
        method: method.to_string(),
        payload: serialize::to_bytes(op)?,
        traceparent: None,
        tracestate: None,
    };
    framing::send(&mut writer, &envelope).await?;
    let response: ResponseEnvelope = framing::recv(&mut reader)
        .await?
        .ok_or_else(|| anyhow!("the writer hung up"))?;
    let bytes = response.result.map_err(|refused| anyhow!(refused))?;
    Ok(serialize::from_bytes(&bytes)?)
}
