//! The request and response frames of a method-routed local socket.

use serde::{Deserialize, Serialize};

/// Wire format for requests
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestEnvelope {
    /// Unique request ID for correlation
    pub id: u64,
    /// Operation method name (e.g., "dev.build_project")
    pub method: String,
    /// Serialized operation payload
    pub payload: Vec<u8>,
    /// W3C traceparent header for distributed tracing
    /// Format: "00-{trace_id}-{span_id}-{flags}"
    pub traceparent: Option<String>,
    /// W3C tracestate header with x0k baggage
    /// Format: "x0k={session_id};{workstream_id};{task_id};{seed_id}"
    pub tracestate: Option<String>,
}

// Manual impl - envelope is just a container of postcard-safe primitives
impl x0k_serialization::PostcardSerializableTrait for RequestEnvelope {}

/// Wire format for responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    /// Request ID this response correlates to
    pub id: u64,
    /// Result: Ok(serialized_result) or Err(error_message)
    pub result: Result<Vec<u8>, String>,
    /// W3C traceparent header echoing the handler's span
    /// Useful for correlating client-side logs with server-side processing
    pub traceparent: Option<String>,
}

impl Default for ResponseEnvelope {
    fn default() -> Self {
        Self {
            id: 0,
            result: Ok(Vec::new()),
            traceparent: None,
        }
    }
}

// Manual impl - envelope is just a container of postcard-safe primitives
impl x0k_serialization::PostcardSerializableTrait for ResponseEnvelope {}
