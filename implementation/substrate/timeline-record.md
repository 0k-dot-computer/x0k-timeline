# x0k-timeline-record: the types the timeline owns

```turtle folio:document
implementation:substrate\/timeline-record a x0k:Implementation ;
    x0k:status "draft" ;
    x0k:summary "x0k-timeline-record: the record, span, ingest and trace-override types the timeline's emitter, writer and reader share, in a crate that depends on no x0k type, so the timeline can stand without x0k-types." ;
    x0k:concerns "timeline", "observability", "publication" ;
    x0k:implements architecture:timeline-public-surface ;
    x0k:motivatedBy intent:b4caf1b6-9c28-6feb-fc5c-57a0f5698e4d ;
    folio:tangleCrate "crates/x0k-timeline-record" ;
    folio:tangleRoot "src/lib.rs" .
```

The timeline's emitter, writer and reader pass these types among themselves,
and x0k's own processes pass them in. They describe the timeline and nothing
else, so the timeline owns them. x0k-types depends on this crate and names
what it holds, which leaves x0k's callers where they were; the timeline's own
crates import it directly and need nothing from x0k-types for their record.

The only other x0k crate beneath this one is x0k-serialization, whose derive
gives the postcard checks. A record is still postcard on the wire and rows in
a segment. Becoming a shape in canonical CBOR is a later change to these
types (a265f9fe).

<a name="chunk-manifest"></a><sub>[`Cargo.toml`](../../crates/x0k-timeline-record/Cargo.toml) · `#manifest`</sub>

```toml {#manifest file="Cargo.toml"}
[package]
name = "x0k-timeline-record"
version = "0.1.1"
edition = { workspace = true }
license = "MIT"
description = "The types the timeline's emitter, writer and reader share: the record, spans, ingest and trace overrides."
repository = "https://github.com/0k-dot-computer/x0k-timeline"
readme = "../../README.md"
keywords = ["timeline", "observability", "opentelemetry", "tracing", "mcp"]

[dependencies]
hex = "0.4"
postcard-serializable-types = { version = "0.1.0" }
rkyv = { version = "0.7", features = ["validation", "alloc", "size_32", "uuid"] }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true, features = ["v4"] }
x0k-serialization = { path = "../x0k-serialization" , version = "0.1.0" }

[dev-dependencies]
postcard = "1.0"
```

<a name="chunk-crate-root"></a><sub>[`src/lib.rs`](../../crates/x0k-timeline-record/src/lib.rs) · `#crate-root`</sub>

```rust {#crate-root file="src/lib.rs"}
//! The types the timeline owns. See the chapter for what each module holds.

pub mod json;
pub mod overrides;
pub mod pause;
pub mod process;
pub mod record;
pub mod span;
pub mod trace_context;

pub use json::JsonValue;
pub use record::{now_micros, ActivityId};
```

## JSON in a postcard field

Postcard carries no self-describing values, so a span's fields hold JSON as
text.

<a name="chunk-json"></a><sub>[`src/json.rs`](../../crates/x0k-timeline-record/src/json.rs) · `#json`</sub>

```rust {#json file="src/json.rs"}
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use x0k_serialization::PostcardSerializable;

/// A JSON value stored as a string for Postcard compatibility
///
/// Postcard cannot serialize serde_json::Value directly because it contains
/// arbitrary nested structures. This wrapper stores JSON as a string while
/// providing convenient conversion methods.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    PostcardSerializable,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
pub struct JsonValue(String);

impl JsonValue {
    /// Create a new JsonValue from a serde_json::Value
    pub fn from_value(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        Ok(JsonValue(serde_json::to_string(&value)?))
    }

    /// Parse the JSON string back into a serde_json::Value
    pub fn to_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.0)
    }

    /// Create a JsonValue from a raw JSON string
    pub fn from_json_str(json: String) -> Self {
        JsonValue(json)
    }

    /// Get the raw JSON string
    pub fn as_json_str(&self) -> &str {
        &self.0
    }

    /// Create a JsonValue containing a JSON null value
    pub fn null() -> Self {
        JsonValue("null".to_string())
    }

    /// Create a JsonValue containing a JSON string value
    pub fn string(s: impl Into<String>) -> Self {
        JsonValue(serde_json::to_string(&s.into()).unwrap_or_else(|_| "null".to_string()))
    }

    /// Create a JsonValue containing a JSON number value
    pub fn number(n: impl Into<serde_json::Number>) -> Self {
        JsonValue(serde_json::to_string(&n.into()).unwrap_or_else(|_| "null".to_string()))
    }

    /// Create a JsonValue containing a JSON boolean value
    pub fn bool(b: bool) -> Self {
        JsonValue(serde_json::to_string(&b).unwrap_or_else(|_| "null".to_string()))
    }

    /// Create a JsonValue containing a JSON array
    pub fn array(arr: Vec<serde_json::Value>) -> Self {
        JsonValue(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string()))
    }

    /// Create a JsonValue containing a JSON object
    pub fn object(obj: serde_json::Map<String, serde_json::Value>) -> Self {
        JsonValue(serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string()))
    }

    /// Try to get the value as a string if it's a JSON string
    /// Returns an owned String to avoid lifetime issues
    pub fn as_string(&self) -> Option<String> {
        match self.to_value() {
            Ok(serde_json::Value::String(s)) => Some(s),
            _ => None,
        }
    }

    /// Legacy method for compatibility - prefer as_string()
    pub fn as_str(&self) -> Option<String> {
        self.as_string()
    }

    /// Try to get the value as an i64 if it's a JSON number
    pub fn as_i64(&self) -> Option<i64> {
        if let Ok(value) = self.to_value() {
            value.as_i64()
        } else {
            None
        }
    }

    /// Try to get the value as a u64 if it's a JSON number
    pub fn as_u64(&self) -> Option<u64> {
        if let Ok(value) = self.to_value() {
            value.as_u64()
        } else {
            None
        }
    }

    /// Try to get the value as an f64 if it's a JSON number
    pub fn as_f64(&self) -> Option<f64> {
        if let Ok(value) = self.to_value() {
            value.as_f64()
        } else {
            None
        }
    }

    /// Try to get the value as a bool if it's a JSON boolean
    pub fn as_bool(&self) -> Option<bool> {
        if let Ok(value) = self.to_value() {
            value.as_bool()
        } else {
            None
        }
    }

    /// Check if the value is null
    pub fn is_null(&self) -> bool {
        self.0 == "null"
    }

    /// Try to get the value as an array if it's a JSON array
    pub fn as_array(&self) -> Option<Vec<serde_json::Value>> {
        match self.to_value() {
            Ok(serde_json::Value::Array(arr)) => Some(arr),
            _ => None,
        }
    }

    /// Try to get the value as an object if it's a JSON object
    pub fn as_object(&self) -> Option<serde_json::Map<String, serde_json::Value>> {
        match self.to_value() {
            Ok(serde_json::Value::Object(obj)) => Some(obj),
            _ => None,
        }
    }
}

impl From<serde_json::Value> for JsonValue {
    fn from(value: serde_json::Value) -> Self {
        JsonValue(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
    }
}

impl From<String> for JsonValue {
    fn from(json: String) -> Self {
        JsonValue(json)
    }
}

impl From<&str> for JsonValue {
    fn from(json: &str) -> Self {
        JsonValue(json.to_string())
    }
}

impl From<bool> for JsonValue {
    fn from(b: bool) -> Self {
        JsonValue::bool(b)
    }
}

impl From<i32> for JsonValue {
    fn from(n: i32) -> Self {
        JsonValue(n.to_string())
    }
}

impl From<i64> for JsonValue {
    fn from(n: i64) -> Self {
        JsonValue(n.to_string())
    }
}

impl From<f64> for JsonValue {
    fn from(n: f64) -> Self {
        JsonValue(serde_json::to_string(&n).unwrap_or_else(|_| "null".to_string()))
    }
}

impl std::fmt::Display for JsonValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
```

## Spans

A span is what one traced function did: its identity in the trace, its timing,
where it ran, and the events inside it.

<a name="chunk-span"></a><sub>[`src/span.rs`](../../crates/x0k-timeline-record/src/span.rs) · `#span`</sub>

```rust {#span file="src/span.rs"}
use crate::json::JsonValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use x0k_serialization::PostcardSerializable;

/// Event attached to a span during its lifetime
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct SpanEventData {
    /// Event timestamp (microseconds since Unix epoch)
    pub timestamp: u64,

    /// Event level (trace, debug, info, warn, error)
    pub level: String,

    /// Event message
    pub message: Option<String>,

    /// Event fields (stored as JSON strings for postcard compatibility)
    pub fields: HashMap<String, JsonValue>,

    /// Source file
    pub source_file: Option<String>,

    /// Source line
    pub source_line: Option<u32>,
}

/// Span data for submission to the tracing system
///
/// This is a lightweight structure designed to be sent over the SDK
/// without introducing circular dependencies with x0k-env.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct SpanData {
    /// Unique span ID (16-char hex)
    pub span_id: String,

    /// Trace ID inherited from parent (32-char hex)
    pub trace_id: String,

    /// Parent span ID (if this is a child span)
    pub parent_span_id: Option<String>,

    /// Span start timestamp (microseconds since Unix epoch)
    pub start_time: u64,

    /// Span duration in milliseconds
    pub duration_ms: Option<u64>,

    /// Tracing level (trace, debug, info, warn, error)
    pub level: String,

    /// Function name (from `#[instrument]` or span name)
    pub function: Option<String>,

    /// Module path (e.g., "x0k_core::sync")
    pub module: Option<String>,

    /// Source file path
    pub source_file: Option<String>,

    /// Source line number
    pub source_line: Option<u32>,

    /// Span name from tracing metadata
    pub span_name: String,

    /// Target (typically the module path)
    pub target: String,

    /// Fields recorded on the span (stored as JSON strings for postcard compatibility)
    pub fields: HashMap<String, JsonValue>,

    /// Events attached to this span
    pub events: Vec<SpanEventData>,

    /// Workstream ID (from environment/context)
    pub workstream_id: Option<String>,

    /// Task ID (from environment/context)
    pub intent_id: Option<String>,

    /// Software module ID (from daemon/binary name)
    /// Used for timeline visibility and activity tracking
    pub software_module_id: Option<String>,

    /// Logical session ID (from X0K_SESSION_ID env var)
    /// Links spans to a specific agent session; NULL for non-agent processes
    pub session_id: Option<String>,

    /// Process ID of the emitting process
    pub pid: Option<u32>,
}
```

## The record

A timeline record is a span, a log event or a completed activity. The actor is
the id the writer stores in its `actor` column. What an id means, and who
holds it, is x0k's, so the timeline never sees more of an actor than this
string. `ActivitySink` is the seam the emitter hands completed activities to.

<a name="chunk-record"></a><sub>[`src/record.rs`](../../crates/x0k-timeline-record/src/record.rs) · `#record`</sub>

```rust {#record file="src/record.rs"}
use crate::span::SpanData;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use x0k_serialization::PostcardSerializable;

/// Get the current time as microseconds since Unix epoch
///
/// This is the primary timestamp function for x0k. All other timestamp
/// functions should be based on this for consistency.
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_micros() as u64
}

/// Unique identifier for a semantic activity milestone
///
/// Activity identifiers are not tracing span IDs. Structural hierarchy belongs
/// to `span_id` / `parent_span_id`, while `ActivityId` is for operator-visible
/// milestones and optional semantic causality edges such as delegation/resume.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PostcardSerializable)]
pub struct ActivityId(pub String);

impl ActivityId {
    /// Create a new random event ID
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create an event ID from a string
    pub fn from(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl Default for ActivityId {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for storing completed activities
pub trait ActivitySink: Send + Sync {
    /// Record a completed activity
    fn record_activity(&self, activity: CompletedActivity);

    /// Update an existing activity (e.g., test status change)
    fn update_activity(&self, activity: CompletedActivity) {
        self.record_activity(activity);
    }
}

/// Context for an in-progress activity.
///
/// This struct captures the state of an activity while it is in progress.
/// It is populated by the observability layer from tracing span fields.
///
/// Most callers should not construct this directly. For normal activity
/// emission, use the helpers your emitter provides.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ActivityContext {
    /// Unique identifier for this activity
    pub activity_id: ActivityId,

    /// Type of activity (e.g., "agent.spawned", "task.started")
    pub activity_type: String,

    /// Parent activity ID for causality chain
    pub parent_id: Option<ActivityId>,

    /// Id of the actor who performed the activity
    pub actor: Option<String>,

    /// Timestamp when activity started (microseconds since Unix epoch)
    pub started_at: u64,

    /// Metadata extracted from span fields
    pub metadata: HashMap<String, String>,

    /// Keys of `metadata` the emitter recorded as a JSON scalar — a number or
    /// a bool — rather than a string. Their `metadata` value is that scalar's
    /// JSON text (`"5"`, `"true"`), and the sink writes it back into
    /// `metadata_json` as the scalar, so a recorded type survives to the row
    /// (observability-timeline §5). A key absent here is a string.
    #[serde(default)]
    pub typed_metadata_keys: BTreeSet<String>,

    /// Correlation ID for tracking related activities across process boundaries
    pub correlation_id: Option<String>,

    /// W3C Trace ID for distributed tracing (128-bit, 32 hex chars)
    #[serde(default)]
    pub trace_id: Option<String>,

    /// Span ID for this activity (64-bit, 16 hex chars)
    #[serde(default)]
    pub span_id: Option<String>,

    /// Parent span ID in the trace (64-bit, 16 hex chars)
    #[serde(default)]
    pub parent_span_id: Option<String>,

    /// JSON data containing all domain-specific information
    pub activity_data: Option<String>,

    /// Raw data that needs lazy serialization
    #[serde(skip)]
    pub lazy_data: Option<LazySerializable>,

    // Source location fields
    /// Source file path (relative to workspace or absolute)
    #[serde(default)]
    pub source_file: Option<String>,

    /// Source line number
    #[serde(default)]
    pub source_line: Option<u32>,

    /// Function/span name (from `#[instrument]` or manual span)
    #[serde(default)]
    pub source_function: Option<String>,

    /// Module path (e.g., "x0k_core::sync")
    #[serde(default)]
    pub source_module: Option<String>,

    /// Span name from tracing (metadata.name())
    #[serde(default)]
    pub span_name: Option<String>,
}

/// A completed activity with timing information.
///
/// This is the low-level sink payload, not the preferred construction API for
/// regular callers. Most emitters should use the shared helpers in
/// the emitter so canonical fields stay consistent.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct CompletedActivity {
    /// The activity context captured during execution
    pub context: ActivityContext,

    /// Timestamp when activity ended (microseconds since Unix epoch)
    pub ended_at: u64,

    /// Duration of the activity in milliseconds
    pub duration_ms: u64,
}

impl CompletedActivity {
    /// Create a new completed activity from a context
    pub fn from_context(context: ActivityContext) -> Self {
        let ended_at = now_micros();
        let duration_ms = ended_at.saturating_sub(context.started_at) / 1_000;

        Self {
            context,
            ended_at,
            duration_ms,
        }
    }

    /// Get the activity type
    pub fn activity_type(&self) -> &str {
        &self.context.activity_type
    }

    /// Get the activity ID
    pub fn activity_id(&self) -> &ActivityId {
        &self.context.activity_id
    }
}

/// Operation type for the activity channel
#[derive(Debug)]
pub enum ActivityOperation {
    /// Record a new completed activity
    Record(CompletedActivity),
    /// Update an existing activity (e.g., test status change)
    Update(CompletedActivity),
}

/// Metadata key for overriding the timeline storage tier classification.
pub const STORAGE_TIER_KEY: &str = "activity.storage_tier";
/// Legacy metadata key for storage tier classification.
pub const LEGACY_DURABILITY_KEY: &str = "activity.durability";

/// Storage tier value for operator-facing, session-meaningful activity classes.
pub const STORAGE_TIER_CENTRAL_VALUE: &str = "central";
/// Storage tier value for debug-heavy or local-only activity classes.
pub const STORAGE_TIER_EPHEMERAL_VALUE: &str = "ephemeral";
/// Storage tier value for record activities that should never be reaped by default.
pub const STORAGE_TIER_RECORD_VALUE: &str = "record";

/// Storage tier for a timeline record.
///
/// This is a routing and query-classification label, not the physical retention
/// policy. Physical retention is tracked separately by `RetentionClass`.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, PostcardSerializable,
)]
pub enum StorageTier {
    /// Operator-facing activity class.
    #[default]
    Central,
    /// Debug-heavy or local-only activity class.
    Ephemeral,
    /// Record activity class. These entries default to the permanent record
    /// retention class.
    Record,
}

/// Physical retention class for a timeline record.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, PostcardSerializable,
)]
pub enum RetentionClass {
    /// Short-lived tracing spans.
    #[default]
    Span,
    /// Short-lived freestanding log events.
    Log,
    /// Operator events and activities.
    Event,
    /// Record entries that are never reaped by default.
    Record,
}

impl RetentionClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Span => "span",
            Self::Log => "log",
            Self::Event => "event",
            Self::Record => "record",
        }
    }
}

/// Route an activity into the timeline storage tier classification.
///
/// Explicit metadata overrides take precedence. `activity.durability` remains
/// supported as a legacy alias for `activity.storage_tier`.
pub fn route_activity_storage_tier(
    activity_type: &str,
    metadata: &HashMap<String, String>,
) -> StorageTier {
    if let Some(override_value) = metadata
        .get(STORAGE_TIER_KEY)
        .or_else(|| metadata.get(LEGACY_DURABILITY_KEY))
    {
        match override_value.as_str() {
            STORAGE_TIER_CENTRAL_VALUE => return StorageTier::Central,
            STORAGE_TIER_EPHEMERAL_VALUE => return StorageTier::Ephemeral,
            STORAGE_TIER_RECORD_VALUE => return StorageTier::Record,
            _ => {}
        }
    }

    match activity_type.split('.').next().unwrap_or("") {
        "agent" | "task" | "module" | "system" | "document" => StorageTier::Central,
        "usage" | "decision" => StorageTier::Record,
        _ => StorageTier::Ephemeral,
    }
}

/// A freestanding log event (not attached to any tracked span)
///
/// Captured by the ObservabilityLayer when `on_event` fires for an event
/// whose parent span is not tracked. Written to Parquet with `entry_kind = "log"`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct LogEventData {
    /// Unique event ID
    pub id: String,
    /// Timestamp in microseconds since epoch
    pub timestamp_us: u64,
    /// Log level (info, debug, warn, error, trace)
    pub level: String,
    /// Log message
    pub message: Option<String>,
    /// Structured fields as JSON
    pub fields_json: Option<String>,
    /// Source file
    pub source_file: Option<String>,
    /// Source line
    pub source_line: Option<u32>,
    /// Module path
    pub module: Option<String>,
    /// Target (usually module path)
    pub target: String,
    /// Software module ID (daemon/binary name)
    pub software_module_id: Option<String>,
    /// Workstream ID
    pub workstream_id: Option<String>,
    /// Intent ID
    pub intent_id: Option<String>,
    /// Logical session ID (from X0K_SESSION_ID env var)
    pub session_id: Option<String>,
    /// Process ID of the emitting process
    pub pid: Option<u32>,
    /// W3C Trace ID for correlating with spans/activities
    pub trace_id: Option<String>,
    /// W3C traceparent header when the event derives from a canonical request
    pub traceparent: Option<String>,
    /// W3C tracestate header when the event derives from a canonical request
    pub tracestate: Option<String>,
    /// Canonical request ID when present
    pub request_id: Option<String>,
    /// Canonical parent request ID when present
    pub parent_request_id: Option<String>,
    /// Logical execution role when present
    pub role: Option<String>,
    /// Execution mode when present
    pub mode: Option<String>,
    /// Origin system when present
    pub origin_system: Option<String>,
    /// Origin subsystem when present
    pub origin_subsystem: Option<String>,
    /// Agent profile when present
    pub agent_profile: Option<String>,
    /// Sandbox identifier when present
    pub sandbox_id: Option<String>,
    /// Invocation method when present
    pub invocation_method: Option<String>,
    /// Direct executor serialized as JSON
    pub executed_by_json: Option<String>,
    /// Authority chain serialized as JSON
    pub authorization_chain_json: Option<String>,
    /// Authorization proof refs serialized as JSON
    pub authorization_proof_refs_json: Option<String>,
    /// Canonical artifact ID when present
    pub artifact_id: Option<String>,
    /// Backend-native artifact reference when present
    pub native_artifact_id: Option<String>,
    /// Canonical document ID when present
    pub document_id: Option<String>,
    /// Canonical document revision ID when present
    pub document_revision_id: Option<String>,
    /// Canonical file path reference when present
    pub file_path: Option<String>,
    /// Structured event type (e.g., "sidecar.tool_decision")
    pub event_type: Option<String>,
}

/// Lightweight metadata for a conversation entry emitted to the timeline.
///
/// Text content is NOT stored here — it stays in the Loro session document
/// and is resolved lazily at query time by `query_timeline_sql`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ConversationMeta {
    /// Composite ID: "{session_id}:{seq}"
    pub id: String,
    /// Agent session ID (used to locate the Loro document)
    pub session_id: String,
    /// Sequence number within the session conversation
    pub seq: u64,
    /// Microseconds since Unix epoch
    pub timestamp_us: u64,
    /// Entry type: "message", "query_complete", "tool_executed", etc.
    pub entry_type: String,
    /// Role for messages: "user", "assistant", "system"; None for non-message entries
    pub role: Option<String>,
    /// Query ID if this entry belongs to a query cycle
    pub query_id: Option<String>,
    /// Duration in milliseconds (for QueryComplete, ToolExecuted)
    pub duration_ms: Option<u64>,
    /// Workstream ID (from env)
    pub workstream_id: Option<String>,
    /// Intent ID (from env)
    pub intent_id: Option<String>,
    /// W3C Trace ID for correlating with span/activity timeline data
    pub trace_id: Option<String>,
    /// Short summary populated at write time (error text, query stats, etc.)
    /// Full content resolved lazily from Loro by query_timeline_sql when available.
    pub description: Option<String>,
}

/// A record that can be flushed to the unified timeline Parquet store
///
/// Both activities and spans flow through the same channel and flush task.
/// The flush task partitions by storage tier at write time.
#[derive(Debug, Clone, Serialize, Deserialize, PostcardSerializable)]
#[allow(clippy::large_enum_variant)]
pub enum TimelineRecord {
    /// A completed activity with its storage tier
    Activity(CompletedActivity, StorageTier),
    /// A completed tracing span. Retention is controlled separately via
    /// `RetentionClass::Span`.
    Span(SpanData),
    /// A freestanding log event. Retention is controlled separately via
    /// `RetentionClass::Log`.
    LogEvent(LogEventData),
    /// A conversation entry (metadata only, content resolved from Loro at query time)
    Conversation(ConversationMeta),
}

impl TimelineRecord {
    /// How long this record is kept, and whether a paused writer keeps it.
    pub fn retention_class(&self) -> RetentionClass {
        match self {
            Self::Span(_) => RetentionClass::Span,
            Self::LogEvent(event) => event.retention_class(),
            Self::Activity(_, StorageTier::Record) => RetentionClass::Record,
            Self::Activity(..) | Self::Conversation(_) => RetentionClass::Event,
        }
    }
}

impl LogEventData {
    /// How long this log row is kept. A process observation outlives the
    /// logs around it, because it is the evidence left when they are gone.
    pub fn retention_class(&self) -> RetentionClass {
        match self.event_type.as_deref() {
            Some(event_type) if crate::process::is_observation(event_type) => RetentionClass::Event,
            _ => RetentionClass::Log,
        }
    }
}

impl Default for TimelineRecord {
    fn default() -> Self {
        Self::LogEvent(LogEventData::default())
    }
}

// --- Timeline ingestion (single-writer IPC protocol) ---

/// IPC method for timeline ingestion.
///
/// Used by the forwarder in `x0k-env` and the daemon handler in `x0k-dev-daemon`.
pub const INGEST_TIMELINE_METHOD: &str = "dev.ingest_timeline";

/// Request to ingest timeline records from a forwarding client.
///
/// Non-daemon processes send batches of TimelineRecord via IPC instead of
/// writing parquet files directly. The daemon's flush task handles batching,
/// WAL, and parquet writes as the single writer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct IngestTimeline {
    pub records: Vec<TimelineRecord>,
}

/// Result of timeline ingestion
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct IngestTimelineResult {
    /// Number of records the writer took, so a client need not keep them.
    /// While the writer is paused this includes those it discarded.
    pub accepted: usize,
    /// How many of those the paused writer discarded.
    pub discarded: usize,
}

/// A wrapper that defers serialization of data until it's needed
///
/// This allows us to store complex data structures in tracing spans without
/// paying the serialization cost on the hot path. Serialization only happens
/// when the data is actually converted to JSON in the background async task.
#[derive(Debug, Clone)]
pub enum LazySerializable {
    /// Raw structured data that needs serialization
    Raw(SerializableData),
    /// Already serialized JSON string (for backwards compatibility)
    Json(String),
}

/// Types of data that can be lazily serialized
#[derive(Debug, Clone)]
pub enum SerializableData {
    /// Observatory event data with context
    EventData {
        event_type: String,
        dedup_key: String,
        context: String,
        properties: HashMap<String, String>,
    },
    /// Activity details data
    ActivityData { data: serde_json::Value },
    /// Generic JSON value
    JsonValue(serde_json::Value),
    /// Key-value pairs
    Properties(HashMap<String, String>),
}

impl LazySerializable {
    /// Create from raw event data (most common case)
    pub fn from_event_data(
        event_type: String,
        dedup_key: String,
        context: String,
        properties: HashMap<String, String>,
    ) -> Self {
        Self::Raw(SerializableData::EventData {
            event_type,
            dedup_key,
            context,
            properties,
        })
    }

    /// Create from activity data
    pub fn from_activity_data(data: serde_json::Value) -> Self {
        Self::Raw(SerializableData::ActivityData { data })
    }

    /// Create from generic JSON value
    pub fn from_json(value: serde_json::Value) -> Self {
        Self::Raw(SerializableData::JsonValue(value))
    }

    /// Create from properties HashMap
    pub fn from_properties(properties: HashMap<String, String>) -> Self {
        Self::Raw(SerializableData::Properties(properties))
    }

    /// Create from already-serialized JSON (for compatibility)
    pub fn from_json_string(json: String) -> Self {
        Self::Json(json)
    }

    /// Serialize to JSON string (should be called in background task)
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        match self {
            Self::Json(json) => Ok(json.clone()),
            Self::Raw(data) => data.to_json_string(),
        }
    }

    /// Check if this is raw data that needs serialization
    pub fn is_raw(&self) -> bool {
        matches!(self, Self::Raw(_))
    }

    /// Get a lightweight preview for logging (doesn't serialize)
    pub fn preview(&self) -> String {
        match self {
            Self::Json(json) => format!("Json({} chars)", json.len()),
            Self::Raw(data) => data.preview(),
        }
    }
}

impl SerializableData {
    /// Serialize to JSON string
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        match self {
            Self::EventData {
                event_type,
                dedup_key,
                context,
                properties,
            } => {
                let json_value = serde_json::json!({
                    "event_type": event_type,
                    "dedup_key": dedup_key,
                    "context": context,
                    "properties": properties,
                });
                serde_json::to_string(&json_value)
            }
            Self::ActivityData { data } => serde_json::to_string(data),
            Self::JsonValue(value) => serde_json::to_string(value),
            Self::Properties(props) => serde_json::to_string(props),
        }
    }

    /// Get a lightweight preview for logging
    pub fn preview(&self) -> String {
        match self {
            Self::EventData { event_type, .. } => format!("EventData({})", event_type),
            Self::ActivityData { .. } => "ActivityData".to_string(),
            Self::JsonValue(_) => "JsonValue".to_string(),
            Self::Properties(props) => format!("Properties({} keys)", props.len()),
        }
    }
}

impl fmt::Display for LazySerializable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LazySerializable[{}]", self.preview())
    }
}

impl fmt::Display for SerializableData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.preview())
    }
}

impl Serialize for LazySerializable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Json(json) => match serde_json::from_str::<serde_json::Value>(json) {
                Ok(value) => value.serialize(serializer),
                Err(_) => json.serialize(serializer),
            },
            Self::Raw(data) => match data.to_json_string() {
                Ok(json) => match serde_json::from_str::<serde_json::Value>(&json) {
                    Ok(value) => value.serialize(serializer),
                    Err(_) => json.serialize(serializer),
                },
                Err(_) => serializer.serialize_str("{}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_micros() {
        let micros = now_micros();
        assert!(micros > 1_577_836_800_000_000); // the start of 2020, in microseconds
    }

    #[test]
    fn test_activity_context_creation() {
        let context = ActivityContext {
            activity_id: ActivityId::new(),
            activity_type: "test.activity".to_string(),
            parent_id: None,
            actor: None,
            started_at: now_micros(),
            metadata: HashMap::new(),
            typed_metadata_keys: Default::default(),
            correlation_id: None,
            trace_id: None,
            span_id: None,
            parent_span_id: None,
            activity_data: None,
            lazy_data: None,
            source_file: None,
            source_line: None,
            source_function: None,
            source_module: None,
            span_name: None,
        };

        assert_eq!(context.activity_type, "test.activity");
        assert!(context.started_at > 0);
    }

    #[test]
    fn test_completed_activity_from_context() {
        let context = ActivityContext {
            activity_id: ActivityId::new(),
            activity_type: "test.activity".to_string(),
            parent_id: None,
            actor: None,
            started_at: now_micros() - 1_000_000,
            metadata: HashMap::new(),
            typed_metadata_keys: Default::default(),
            correlation_id: None,
            trace_id: None,
            span_id: None,
            parent_span_id: None,
            activity_data: None,
            lazy_data: None,
            source_file: None,
            source_line: None,
            source_function: None,
            source_module: None,
            span_name: None,
        };

        let completed = CompletedActivity::from_context(context);
        assert!(completed.duration_ms >= 900);
        assert!(completed.duration_ms <= 2000);
    }

    #[test]
    fn test_activity_context_with_trace_context() {
        let context = ActivityContext {
            activity_id: ActivityId::new(),
            activity_type: "test.activity".to_string(),
            parent_id: None,
            actor: None,
            started_at: now_micros(),
            metadata: HashMap::new(),
            typed_metadata_keys: Default::default(),
            correlation_id: Some("corr-123".to_string()),
            trace_id: Some("00112233445566778899aabbccddeeff".to_string()),
            span_id: Some("0011223344556677".to_string()),
            parent_span_id: Some("8899aabbccddeeff".to_string()),
            activity_data: None,
            lazy_data: None,
            source_file: Some("src/sync.rs".to_string()),
            source_line: Some(42),
            source_function: Some("process_file".to_string()),
            source_module: Some("x0k_core::sync".to_string()),
            span_name: Some("process_file".to_string()),
        };

        assert_eq!(
            context.trace_id.as_deref(),
            Some("00112233445566778899aabbccddeeff")
        );
        assert_eq!(context.span_id.as_deref(), Some("0011223344556677"));
    }

    #[test]
    fn test_lazy_event_data_serialization() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());

        let lazy = LazySerializable::from_event_data(
            "test_event".to_string(),
            "dedup123".to_string(),
            "sync_send".to_string(),
            props,
        );

        assert!(lazy.is_raw());
        let json = lazy.to_json_string().unwrap();
        assert!(json.contains("test_event"));
    }

    #[test]
    fn test_lazy_json_string_passthrough() {
        let original_json = r#"{"test": "value"}"#;
        let lazy = LazySerializable::from_json_string(original_json.to_string());
        let result = lazy.to_json_string().unwrap();
        assert_eq!(result, original_json);
    }
}
```

## Trace overrides

These are the wire types for changing trace levels at run time. Overrides are
scoped by owner so they clean themselves up: a client's lapse with its
heartbeat, a session's end with the session, a timer's with the timer. The
service holds the registry; each process polls for the effective set.

<a name="chunk-overrides"></a><sub>[`src/overrides.rs`](../../crates/x0k-timeline-record/src/overrides.rs) · `#overrides`</sub>

```rust {#overrides file="src/overrides.rs"}
use serde::{Deserialize, Serialize};
use x0k_serialization::PostcardSerializable;

// --- Method constants ---

pub const SET_TRACE_OVERRIDE_METHOD: &str = "dev.set_trace_override";
pub const CLEAR_TRACE_OVERRIDE_METHOD: &str = "dev.clear_trace_override";
pub const LIST_TRACE_OVERRIDES_METHOD: &str = "dev.list_trace_overrides";
pub const HEARTBEAT_MCP_CLIENT_METHOD: &str = "dev.heartbeat_mcp_client";
pub const GET_EFFECTIVE_OVERRIDES_METHOD: &str = "dev.get_effective_overrides";
pub const CLEAR_OVERRIDES_FOR_SESSION_METHOD: &str = "dev.clear_overrides_for_session";

// --- Shared owner representation (used in list responses) ---

/// Wire representation of an override owner.
///
/// Clients don't usually care about the owner — it's mostly for `list` and
/// debugging. `effective_set()` on the daemon collapses ownership away.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, PostcardSerializable)]
pub enum OverrideOwnerWire {
    /// MCP server process identified by UUID; cleared by heartbeat GC.
    McpClient(String),
    /// x0k agent session id; cleared by orchestrator on session removal.
    Session(String),
    /// Wall-clock timer identified by UUID; cleared by daemon sleep task.
    Timer(String),
}

impl Default for OverrideOwnerWire {
    fn default() -> Self {
        // Postcard wire-format compatibility check requires Default; an empty
        // McpClient is the cheapest synthetic value and is never read at runtime.
        OverrideOwnerWire::McpClient(String::new())
    }
}

/// Wire representation of an active override (used by `list_trace_overrides`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ActiveOverrideWire {
    /// Stable id for explicit removal.
    pub id: String,
    /// Owner — drives the cleanup mechanism.
    pub owner: OverrideOwnerWire,
    /// Rust module path prefix the override applies to (e.g. `"x0k_agent::sdk_sidecar"`).
    pub module_prefix: String,
    /// Level string: `"trace" | "debug" | "info" | "warn" | "error"`.
    pub level: String,
    /// Microsecond timestamp when the override was created.
    pub created_at_us: u64,
}

// --- dev.set_trace_override ---

/// Request to register a new trace level override.
///
/// Exactly one scope wins in this order:
///
/// 1. `scope = Some("session:<id>")`  → bind to `Session(<id>)`
/// 2. `scope = Some("duration:<secs>")` → bind to a `Timer` (daemon-managed)
/// 3. `mcp_client_id = Some(uuid)` and no scope → bind to `McpClient(uuid)`
/// 4. None of the above → error
///
/// The typical call from an MCP tool always sets `mcp_client_id` and omits
/// `scope`, which gives the "lives as long as my MCP process" behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct SetTraceOverride {
    /// Rust module path prefix. Accepts any prefix that matches
    /// `module_path!()` at the callsite (e.g. `"x0k_agent"`, `"x0k_agent::sdk_sidecar"`).
    pub module: String,
    /// Level string: `"trace" | "debug" | "info" | "warn" | "error"`.
    pub level: String,
    /// Optional explicit scope. `None` defaults to `McpClient(mcp_client_id)`.
    #[serde(default)]
    pub scope: Option<String>,
    /// Calling MCP server's stable client id. Required when `scope` is `None`.
    #[serde(default)]
    pub mcp_client_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct SetTraceOverrideResult {
    /// Stable id for later removal via `clear_trace_override`.
    pub id: String,
    /// Owner the daemon actually assigned (echoed back for confirmation).
    pub owner: OverrideOwnerWire,
    /// Echoed module prefix (normalized).
    pub module_prefix: String,
    /// Echoed level string (normalized to lowercase).
    pub level: String,
}

// --- dev.clear_trace_override ---

/// Clear one or more overrides. Exactly one of `id` or `session_id` must be set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ClearTraceOverride {
    /// Override id returned by `SetTraceOverride`. Mutually exclusive with `session_id`.
    #[serde(default)]
    pub id: Option<String>,
    /// Clear all overrides bound to this session id.
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ClearTraceOverrideResult {
    /// Number of overrides actually removed.
    pub removed: u32,
}

// --- dev.list_trace_overrides ---

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ListTraceOverrides {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ListTraceOverridesResult {
    pub overrides: Vec<ActiveOverrideWire>,
}

// --- dev.heartbeat_mcp_client ---

/// Heartbeat from an MCP server process; the daemon uses these to detect
/// dead clients and GC their overrides.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct HeartbeatMcpClient {
    pub mcp_client_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct HeartbeatMcpClientResult {}

// --- dev.get_effective_overrides ---

/// Poll request sent by every client's `override_sync` task.
///
/// `since_version` allows the daemon to short-circuit: if `since_version`
/// matches the current registry version, the daemon may return an empty
/// `overrides` list with the same `version`, and the client skips re-applying
/// state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct GetEffectiveOverrides {
    #[serde(default)]
    pub since_version: u64,
}

/// One effective (prefix, level) pair after collapsing per-prefix
/// most-verbose-wins.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct EffectiveOverride {
    pub module_prefix: String,
    pub level: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct GetEffectiveOverridesResult {
    /// Monotonic registry version; bumped on every mutation.
    pub version: u64,
    /// `true` when `version == since_version` and `overrides` is empty.
    /// Clients can use this to skip the apply step entirely.
    #[serde(default)]
    pub unchanged: bool,
    /// Full collapsed override set. Empty when `unchanged = true`.
    #[serde(default)]
    pub overrides: Vec<EffectiveOverride>,
}

// --- dev.clear_overrides_for_session ---

/// Called by the orchestrator from `SessionRegistry::remove` to clean up
/// overrides scoped to that session. Fire-and-forget (result is empty).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ClearOverridesForSession {
    pub session_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ClearOverridesForSessionResult {
    pub removed: u32,
}

// --- Helpers ---

/// Parse a level string. Accepts `"trace" | "debug" | "info" | "warn" |
/// "warning" | "error"` (case-insensitive). Returns `None` on unknown input.
///
/// Both the daemon and the x0k-env sync task use this so error messages stay
/// consistent.
pub fn parse_level_str(s: &str) -> Option<&'static str> {
    match s.to_ascii_lowercase().as_str() {
        "trace" => Some("trace"),
        "debug" => Some("debug"),
        "info" => Some("info"),
        "warn" | "warning" => Some("warn"),
        "error" => Some("error"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_level_str_known_levels() {
        assert_eq!(parse_level_str("trace"), Some("trace"));
        assert_eq!(parse_level_str("DEBUG"), Some("debug"));
        assert_eq!(parse_level_str("Info"), Some("info"));
        assert_eq!(parse_level_str("warning"), Some("warn"));
        assert_eq!(parse_level_str("warn"), Some("warn"));
        assert_eq!(parse_level_str("error"), Some("error"));
    }

    #[test]
    fn parse_level_str_unknown() {
        assert_eq!(parse_level_str("verbose"), None);
        assert_eq!(parse_level_str(""), None);
    }
}
```

## Pausing the writer

A user can stop the timeline keeping records while the writer runs
(`timeline-public-surface` §7). Every operation answers the same state, so a
caller never reads it twice.

<a name="chunk-pause"></a><sub>[`src/pause.rs`](../../crates/x0k-timeline-record/src/pause.rs) · `#pause`</sub>

```rust {#pause file="src/pause.rs"}
//! The writer's pause operations (`timeline-public-surface` §7).

use serde::{Deserialize, Serialize};
use x0k_serialization::PostcardSerializable;

pub const PAUSE_TIMELINE_METHOD: &str = "timeline.pause";
pub const RESUME_TIMELINE_METHOD: &str = "timeline.resume";
pub const TIMELINE_PAUSE_STATE_METHOD: &str = "timeline.pause_state";

/// Pause the writer. A writer already paused stays as it is, with its reason.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct PauseTimeline {
    /// Why, which the pause's row and `describe_timeline` carry.
    pub reason: String,
}

/// Resume the writer. A writer not paused stays as it is.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ResumeTimeline {}

/// Read the writer's pause state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PostcardSerializable)]
pub struct ReadPauseState {}

/// Rows discarded while paused, by retention class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, PostcardSerializable)]
pub struct DiscardedRows {
    pub span: u64,
    pub log: u64,
    pub event: u64,
}

/// What every pause operation answers. For a resume, the state it ended.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, PostcardSerializable)]
pub struct PauseState {
    pub paused: bool,
    pub since_us: Option<u64>,
    pub reason: Option<String>,
    pub discarded: DiscardedRows,
}
```

## What an observer saw of a process

A process cannot record its own death, so something watching it does
(`timeline-public-surface` §8). The record is a `log` row named for what
happened. The row's own process is the observer and its attributes name the
subject, so a reader can always tell a claim from an observation. Nothing
here depends on who the observer is, so a supervisor, a wrapper or a shell
hook can each write one. Names follow OpenTelemetry's `process.*` attributes
where they exist; it has none for a signal, an OOM kill or a cause, so those
are the timeline's own.

<a name="chunk-process"></a><sub>[`src/process.rs`](../../crates/x0k-timeline-record/src/process.rs) · `#process`</sub>

```rust {#process file="src/process.rs"}
//! The process observation: what an observer saw of a process.

use serde_json::{Map, Value};

/// A process was started.
pub const SPAWNED: &str = "process.spawned";
/// A running process came under this observer, inherited from another.
pub const ADOPTED: &str = "process.adopted";
/// A process ended.
pub const EXIT: &str = "process.exit";
/// A bounded wait on a process ran out, with the process left as it was.
pub const ABANDONED: &str = "process.abandoned";

/// The subject's process id.
pub const PID: &str = "process.pid";
/// The subject's command.
pub const COMMAND: &str = "process.command";
/// The code a process exited with.
pub const EXIT_CODE: &str = "process.exit.code";
/// Why a process ended: `exited`, `signalled` or `unknown`.
pub const CAUSE: &str = "timeline.process.cause";
/// The signal that ended a process.
pub const SIGNAL: &str = "timeline.process.signal";
/// Whether an OOM kill ended it: `yes`, `no` or `unknown`. An observer that
/// could not know says `unknown`, never `no`.
pub const OOM: &str = "timeline.process.oom";
/// The bound, in milliseconds, that a wait ran out against.
pub const BOUND_MS: &str = "timeline.process.bound_ms";

/// Whether `event_type` names a process observation.
pub fn is_observation(event_type: &str) -> bool {
    matches!(event_type, SPAWNED | ADOPTED | EXIT | ABANDONED)
}

/// Why a row named as an observation is not one.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Misfit {
    #[error("{event} needs {attribute}")]
    Missing { event: String, attribute: &'static str },
    #[error("{event}: {attribute} must be one of {allowed}")]
    NotOneOf { event: String, attribute: &'static str, allowed: &'static str },
}

/// Whether the attributes of a row named `event_type` make an observation.
/// A row that is not named as one always fits.
pub fn check_observation(event_type: &str, attributes: &Map<String, Value>) -> Result<(), Misfit> {
    if !is_observation(event_type) {
        return Ok(());
    }
    let missing = |attribute| Misfit::Missing { event: event_type.to_string(), attribute };
    let integer = |attribute| match attributes.get(attribute).map(Value::is_i64) {
        Some(true) => Ok(()),
        _ => Err(missing(attribute)),
    };
    let one_of = |attribute, allowed: &'static [&'static str], listed| {
        let value = attributes.get(attribute).ok_or_else(|| missing(attribute))?;
        match value.as_str() {
            Some(text) if allowed.contains(&text) => Ok(text.to_string()),
            _ => Err(Misfit::NotOneOf {
                event: event_type.to_string(),
                attribute,
                allowed: listed,
            }),
        }
    };
    integer(PID)?;
    match event_type {
        EXIT => {
            one_of(OOM, &["yes", "no", "unknown"], "yes, no, unknown")?;
            match one_of(CAUSE, &["exited", "signalled", "unknown"], "exited, signalled, unknown")?
                .as_str()
            {
                "exited" => integer(EXIT_CODE),
                "signalled" => match attributes.get(SIGNAL).and_then(Value::as_str) {
                    Some(signal) if !signal.is_empty() => Ok(()),
                    _ => Err(missing(SIGNAL)),
                },
                _ => Ok(()),
            }
        }
        ABANDONED => integer(BOUND_MS),
        _ => Ok(()),
    }
}
```

## Trace context

A span's identity on the wire is W3C trace context, and the record, the emitter and
every reader share one spelling of it, so it lives here with them.

<a name="chunk-trace-context"></a><sub>[`src/trace_context.rs`](../../crates/x0k-timeline-record/src/trace_context.rs) · `#trace-context`</sub>

```rust {#trace-context file="src/trace_context.rs"}
//! W3C Trace Context compatible types for correlation propagation
//!
//! This module provides types for distributed tracing that follow the
//! [W3C Trace Context](https://www.w3.org/TR/trace-context/) specification.
//!
//! # Core Concepts
//!
//! - **trace_id**: Unique identifier for an entire causal chain (stays constant)
//! - **span_id**: Unique identifier for a single unit of work (changes at each boundary)
//! - **parent_span_id**: Links to the span that caused this one (forms the trace tree)
//!
//! # Wire Format
//!
//! The `traceparent` header format is:
//! ```text
//! traceparent: 00-{trace_id}-{span_id}-{flags}
//!              │   │          │        │
//!              │   │          │        └─ 8-bit flags (01 = sampled)
//!              │   │          └─ 16-char hex (64-bit span ID)
//!              │   └─ 32-char hex (128-bit trace ID)
//!              └─ Version (always "00")
//! ```
//!
//! The `tracestate` header carries x0k-specific baggage:
//! ```text
//! tracestate: x0k={session_id};{workstream_id};{intent_id}
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Error type for trace context parsing
#[derive(Debug, Error)]
pub enum TraceContextError {
    #[error("Invalid traceparent format: {0}")]
    InvalidFormat(String),
    #[error("Invalid hex encoding: {0}")]
    InvalidHex(String),
    #[error("Invalid version: expected '00', got '{0}'")]
    InvalidVersion(String),
}

/// 128-bit trace identifier (32 hex chars)
///
/// The trace ID uniquely identifies an entire distributed trace.
/// It stays constant across all spans within the trace.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId([u8; 16]);

impl TraceId {
    /// Generate a new random trace ID
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }

    /// Create a trace ID from raw bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Parse a trace ID from a 32-character hex string
    pub fn from_hex(s: &str) -> Result<Self, TraceContextError> {
        if s.len() != 32 {
            return Err(TraceContextError::InvalidHex(format!(
                "expected 32 hex chars, got {}",
                s.len()
            )));
        }
        let bytes = hex::decode(s).map_err(|e| TraceContextError::InvalidHex(e.to_string()))?;
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Convert to a 32-character hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Check if this is a valid (non-zero) trace ID
    pub fn is_valid(&self) -> bool {
        self.0 != [0u8; 16]
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({})", self.to_hex())
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// 64-bit span identifier (16 hex chars)
///
/// The span ID uniquely identifies a single unit of work within a trace.
/// A new span ID is generated at each process/service boundary.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId([u8; 8]);

impl SpanId {
    /// Generate a new random span ID
    pub fn generate() -> Self {
        // Use first 8 bytes of a UUID for randomness
        let uuid_bytes = uuid::Uuid::new_v4().into_bytes();
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&uuid_bytes[..8]);
        Self(arr)
    }

    /// Create a span ID from raw bytes
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    /// Parse a span ID from a 16-character hex string
    pub fn from_hex(s: &str) -> Result<Self, TraceContextError> {
        if s.len() != 16 {
            return Err(TraceContextError::InvalidHex(format!(
                "expected 16 hex chars, got {}",
                s.len()
            )));
        }
        let bytes = hex::decode(s).map_err(|e| TraceContextError::InvalidHex(e.to_string()))?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Convert to a 16-character hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Check if this is a valid (non-zero) span ID
    pub fn is_valid(&self) -> bool {
        self.0 != [0u8; 8]
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId({})", self.to_hex())
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// x0k-specific baggage propagated with trace context
///
/// This carries domain-specific context that should flow with the trace,
/// such as session identifiers, workstream IDs, and task references.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceBaggage {
    /// Agent session identifier
    pub session_id: Option<String>,
    /// Workstream ID (isolated work context)
    pub workstream_id: Option<String>,
    /// Grove task ID being worked on
    pub intent_id: Option<String>,
    /// Grove seed ID (high-level theme)
    pub seed_id: Option<String>,
}

impl TraceBaggage {
    /// Create empty baggage
    pub fn new() -> Self {
        Self::default()
    }

    /// Create baggage with workstream context
    pub fn with_workstream(workstream_id: impl Into<String>) -> Self {
        Self {
            workstream_id: Some(workstream_id.into()),
            ..Default::default()
        }
    }

    /// Create baggage with task context
    pub fn with_intent(intent_id: impl Into<String>, workstream_id: Option<String>) -> Self {
        Self {
            intent_id: Some(intent_id.into()),
            workstream_id,
            ..Default::default()
        }
    }

    /// Builder: set session ID
    pub fn session(mut self, id: impl Into<String>) -> Self {
        self.session_id = Some(id.into());
        self
    }

    /// Builder: set workstream ID
    pub fn workstream(mut self, id: impl Into<String>) -> Self {
        self.workstream_id = Some(id.into());
        self
    }

    /// Builder: set task ID
    pub fn task(mut self, id: impl Into<String>) -> Self {
        self.intent_id = Some(id.into());
        self
    }

    /// Builder: set seed ID
    pub fn seed(mut self, id: impl Into<String>) -> Self {
        self.seed_id = Some(id.into());
        self
    }
}

/// Complete trace context for propagation
///
/// This is the primary type for carrying distributed tracing context
/// across process and service boundaries. It combines the W3C standard
/// fields with x0k-specific baggage.
///
/// # Example
///
/// ```rust
/// use x0k_timeline_record::trace_context::{TraceContext, TraceBaggage};
///
/// // Create a new root trace
/// let mut ctx = TraceContext::new_root();
/// ctx.baggage = TraceBaggage::new()
///     .workstream("ws-123")
///     .task("task-456");
///
/// // Convert to wire format
/// let traceparent = ctx.to_traceparent();
/// let tracestate = ctx.to_tracestate();
///
/// // Create a child span for a downstream service
/// let child_ctx = ctx.child();
/// assert_eq!(child_ctx.trace_id, ctx.trace_id); // Same trace
/// assert_ne!(child_ctx.span_id, ctx.span_id);   // New span
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    /// 128-bit trace ID shared across all spans in the trace
    pub trace_id: TraceId,
    /// 64-bit span ID unique to this unit of work
    pub span_id: SpanId,
    /// Parent span ID (if this span has a parent)
    pub parent_span_id: Option<SpanId>,
    /// Trace flags (bit 0 = sampled)
    pub flags: u8,
    /// x0k-specific baggage
    pub baggage: TraceBaggage,
}

/// Flag indicating the trace is being sampled/recorded
pub const TRACE_FLAG_SAMPLED: u8 = 0x01;

impl TraceContext {
    /// Create a new root trace (no parent)
    ///
    /// This generates fresh trace and span IDs and sets the sampled flag.
    pub fn new_root() -> Self {
        Self {
            trace_id: TraceId::generate(),
            span_id: SpanId::generate(),
            parent_span_id: None,
            flags: TRACE_FLAG_SAMPLED,
            baggage: TraceBaggage::default(),
        }
    }

    /// Create a child span within this trace
    ///
    /// The child inherits:
    /// - The same trace_id (links to the same distributed trace)
    /// - A new span_id (unique to the child operation)
    /// - parent_span_id set to this span's span_id
    /// - The same flags and baggage
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::generate(),
            parent_span_id: Some(self.span_id),
            flags: self.flags,
            baggage: self.baggage.clone(),
        }
    }

    /// Check if this trace is being sampled/recorded
    pub fn is_sampled(&self) -> bool {
        self.flags & TRACE_FLAG_SAMPLED != 0
    }

    /// Parse from W3C traceparent header
    ///
    /// Format: `00-{trace_id}-{parent_id}-{flags}`
    ///
    /// Note: This creates a NEW span_id for the receiving service.
    /// The span_id in the header becomes our parent_span_id.
    pub fn from_traceparent(header: &str) -> Result<Self, TraceContextError> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return Err(TraceContextError::InvalidFormat(format!(
                "expected 4 parts separated by '-', got {}",
                parts.len()
            )));
        }

        let version = parts[0];
        if version != "00" {
            return Err(TraceContextError::InvalidVersion(version.to_string()));
        }

        let trace_id = TraceId::from_hex(parts[1])?;
        let parent_span_id = SpanId::from_hex(parts[2])?;
        let flags = u8::from_str_radix(parts[3], 16)
            .map_err(|e| TraceContextError::InvalidHex(format!("flags: {}", e)))?;

        Ok(Self {
            trace_id,
            span_id: SpanId::generate(), // New span for this service
            parent_span_id: Some(parent_span_id),
            flags,
            baggage: TraceBaggage::default(),
        })
    }

    /// Format as W3C traceparent header
    ///
    /// Format: `00-{trace_id}-{span_id}-{flags}`
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id.to_hex(),
            self.span_id.to_hex(),
            self.flags
        )
    }

    /// Parse x0k baggage from tracestate header
    ///
    /// Looks for the `x0k=` vendor entry and parses its value as:
    /// `x0k={session_id};{workstream_id};{intent_id};{seed_id}`
    ///
    /// Empty segments are treated as None.
    pub fn parse_tracestate(&mut self, header: &str) {
        for pair in header.split(',') {
            let pair = pair.trim();
            if let Some(value) = pair.strip_prefix("x0k=") {
                let parts: Vec<&str> = value.split(';').collect();

                if !parts.is_empty() && !parts[0].is_empty() {
                    self.baggage.session_id = Some(parts[0].to_string());
                }
                if parts.len() > 1 && !parts[1].is_empty() {
                    self.baggage.workstream_id = Some(parts[1].to_string());
                }
                if parts.len() > 2 && !parts[2].is_empty() {
                    self.baggage.intent_id = Some(parts[2].to_string());
                }
                if parts.len() > 3 && !parts[3].is_empty() {
                    self.baggage.seed_id = Some(parts[3].to_string());
                }
                break;
            }
        }
    }

    /// Format x0k baggage as tracestate header value
    ///
    /// Format: `x0k={session_id};{workstream_id};{intent_id};{seed_id}`
    /// Empty/None values are represented as empty strings.
    pub fn to_tracestate(&self) -> String {
        format!(
            "x0k={};{};{};{}",
            self.baggage.session_id.as_deref().unwrap_or(""),
            self.baggage.workstream_id.as_deref().unwrap_or(""),
            self.baggage.intent_id.as_deref().unwrap_or(""),
            self.baggage.seed_id.as_deref().unwrap_or("")
        )
    }

    /// Create context with specific baggage
    pub fn with_baggage(mut self, baggage: TraceBaggage) -> Self {
        self.baggage = baggage;
        self
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new_root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_generate_and_hex() {
        let id = TraceId::generate();
        assert!(id.is_valid());

        let hex = id.to_hex();
        assert_eq!(hex.len(), 32);

        let parsed = TraceId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_span_id_generate_and_hex() {
        let id = SpanId::generate();
        assert!(id.is_valid());

        let hex = id.to_hex();
        assert_eq!(hex.len(), 16);

        let parsed = SpanId::from_hex(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let ctx = TraceContext::new_root();
        let header = ctx.to_traceparent();

        // Verify format: 00-{32 hex}-{16 hex}-{2 hex}
        let parts: Vec<&str> = header.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_eq!(parts[3].len(), 2);

        // Parse it back - note that parsed context gets a NEW span_id
        let parsed = TraceContext::from_traceparent(&header).unwrap();
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.parent_span_id, Some(ctx.span_id));
        assert_ne!(parsed.span_id, ctx.span_id); // New span for receiver
    }

    #[test]
    fn test_traceparent_w3c_example() {
        // W3C example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
        let header = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let ctx = TraceContext::from_traceparent(header).unwrap();

        assert_eq!(ctx.trace_id.to_hex(), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(ctx.parent_span_id.unwrap().to_hex(), "b7ad6b7169203331");
        assert_eq!(ctx.flags, 0x01);
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_tracestate_roundtrip() {
        let mut ctx = TraceContext::new_root();
        ctx.baggage = TraceBaggage::new()
            .session("session-123")
            .workstream("ws-456")
            .task("task-789")
            .seed("seed-abc");

        let tracestate = ctx.to_tracestate();
        assert_eq!(tracestate, "x0k=session-123;ws-456;task-789;seed-abc");

        // Parse into new context
        let mut parsed = TraceContext::new_root();
        parsed.parse_tracestate(&tracestate);

        assert_eq!(parsed.baggage.session_id, Some("session-123".to_string()));
        assert_eq!(parsed.baggage.workstream_id, Some("ws-456".to_string()));
        assert_eq!(parsed.baggage.intent_id, Some("task-789".to_string()));
        assert_eq!(parsed.baggage.seed_id, Some("seed-abc".to_string()));
    }

    #[test]
    fn test_tracestate_partial() {
        let mut ctx = TraceContext::new_root();
        ctx.parse_tracestate("x0k=session1;;task2;");

        assert_eq!(ctx.baggage.session_id, Some("session1".to_string()));
        assert_eq!(ctx.baggage.workstream_id, None);
        assert_eq!(ctx.baggage.intent_id, Some("task2".to_string()));
        assert_eq!(ctx.baggage.seed_id, None);
    }

    #[test]
    fn test_tracestate_with_other_vendors() {
        let mut ctx = TraceContext::new_root();
        ctx.parse_tracestate("vendor1=value1, x0k=sess;ws;task;seed, vendor2=value2");

        assert_eq!(ctx.baggage.session_id, Some("sess".to_string()));
        assert_eq!(ctx.baggage.workstream_id, Some("ws".to_string()));
    }

    #[test]
    fn test_child_span() {
        let parent = TraceContext::new_root();
        let parent_baggage = TraceBaggage::new().workstream("ws-1").task("task-1");
        let parent = parent.with_baggage(parent_baggage);

        let child = parent.child();

        // Same trace
        assert_eq!(child.trace_id, parent.trace_id);
        // Different span
        assert_ne!(child.span_id, parent.span_id);
        // Links to parent
        assert_eq!(child.parent_span_id, Some(parent.span_id));
        // Inherits baggage
        assert_eq!(child.baggage.workstream_id, parent.baggage.workstream_id);
        assert_eq!(child.baggage.intent_id, parent.baggage.intent_id);
        // Inherits flags
        assert_eq!(child.flags, parent.flags);
    }

    #[test]
    fn test_postcard_serialization_roundtrip() {
        let ctx = TraceContext::new_root().with_baggage(
            TraceBaggage::new()
                .session("sess")
                .workstream("ws")
                .task("task"),
        );

        let bytes = postcard::to_allocvec(&ctx).unwrap();
        let decoded: TraceContext = postcard::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.trace_id, ctx.trace_id);
        assert_eq!(decoded.span_id, ctx.span_id);
        assert_eq!(decoded.parent_span_id, ctx.parent_span_id);
        assert_eq!(decoded.flags, ctx.flags);
        assert_eq!(decoded.baggage, ctx.baggage);
    }

    #[test]
    fn test_invalid_traceparent_format() {
        // Wrong number of parts
        assert!(TraceContext::from_traceparent("00-abc-def").is_err());

        // Wrong version
        assert!(TraceContext::from_traceparent(
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        )
        .is_err());

        // Wrong trace_id length
        assert!(TraceContext::from_traceparent("00-0af7651916cd43dd-b7ad6b7169203331-01").is_err());
    }
}
```

