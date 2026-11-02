//! x0k Activity Sink
//!
//! Routes activities and spans to unified Parquet timeline storage at
//! `~/.0k/activities/timeline`. All records go to a single store;
//! `StorageTier` is retained as a query/routing column and `retention_class`
//! records the physical retention policy.
//!
//! # Usage
//!
//! ```ignore
//! use x0k_activity_sink::{ParquetActivitySink, TimelineRecord};
//! use x0k_activity_sink::run_timeline_flush_task;
//!
//! // Create channel and flush task
//! let (tx, rx) = tokio::sync::mpsc::channel(512);
//! let sink = ParquetActivitySink::new(tx, enabled);
//!
//! // Use with ObservabilityLayer
//! let layer = ObservabilityLayer::builder()
//!     .with_activity_sink(Arc::new(sink))
//!     .build();
//! ```
//!
//! # Activity Classification
//!
//! Activities are classified by type prefix into `StorageTier` (written as
//! a query/routing column for DataFusion queries):
//!
//! | Prefix | Tier | Examples |
//! |--------|------|----------|
//! | `agent.*` | Central | `agent.spawned`, `agent.completed` |
//! | `task.*` | Central | `task.started`, `task.completed` |
//! | `module.*` | Central | `module.invoked`, `module.failed` |
//! | `system.*` | Central | `system.error`, `system.startup` |
//! | `decision.*` | Central | `decision.requested` |
//! | `usage.*` | Record | `usage.tokens`, `usage.api_call` |
//! | `process.*` | Ephemeral | `process.stdout`, `process.stderr` |
//! | `build.*` | Ephemeral | `build.started`, `build.output` |
//!
//! Physical retention is controlled separately by `retention_class`:
//! - `span` for tracing spans
//! - `log` for freestanding log events
//! - `event` for activities and conversations
//! - `record` for never-reaped record activities
//!
//! You can override activity classification with the `activity.storage_tier`
//! metadata field (`activity.durability` remains a legacy alias):
//! - `"central"` - Force central tier
//! - `"ephemeral"` - Force ephemeral tier
//! - `"record"` - Force record tier (permanent, never reaped)

pub mod config;
pub mod predicates;
pub mod pause;
pub mod routing;
pub mod segment;
pub mod session_context;
pub mod sink;
pub mod types;

// Columnar surface — the in-process Arrow/Parquet encode + Arrow-IPC WAL. Only
// the daemon writer and the Timeline Query Service build with the `parquet`
// feature; thin clients link none of this (no arrow/parquet/datafusion).
#[cfg(feature = "parquet")]
pub mod flush_task;
#[cfg(feature = "parquet")]
pub mod memtable;
#[cfg(feature = "parquet")]
pub mod unified_schema;
#[cfg(feature = "parquet")]
pub mod wal;

// Note: artifacts.rs (Dialog-DB artifact factory) has been removed.
// Activities are now written via ParquetActivitySink → unified Parquet timeline.

pub use session_context::SessionContext;

// Re-export main types
pub use config::{
    FlushConfig, SinkConfig, SpanBatchConfig, TieredRetentionConfig, ENV_SPAN_BATCH_SIZE,
    ENV_SPAN_BATCH_TIMEOUT_MS,
};
#[cfg(feature = "parquet")]
pub use flush_task::{run_timeline_flush_task, BatchCallback};
pub use pause::WriterPause;
pub use routing::{is_durable, is_ephemeral, route_activity, RetentionClass, StorageTier};
pub use sink::ParquetActivitySink;
pub use types::{
    ActivityContext, ActivityOperation, ActivitySink, CompletedActivity, LazySerializable,
    SerializableData, TimelineRecord,
};

// Re-export from x0k-types for convenience
pub use x0k_timeline_record::ActivityId;
