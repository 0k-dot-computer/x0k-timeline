//! # x0k-timeline — the timeline service
//!
//! The timeline pipeline has three roles (`observability-timeline` §9):
//! **emit** (every client builds a `TimelineRecord` and forwards it over IPC),
//! **write** (one local writer owns the columnar encode → parquet + the shared
//! WAL), and **query**. This crate runs the last two as one supervised service.
//!
//! - [`writer`] is the machine's one timeline writer. It listens on
//!   [`config::ingest_socket`] for `IngestTimeline` batches,
//!   owns the flush task that writes the shared WAL
//!   (`{timeline_dir}/wal/{pid}.wal`) and the prefixed Parquet files, takes in
//!   the crash-queue segments of clients that exited while it was unreachable,
//!   and serves the trace-override registry ([`trace_override_registry`]) every
//!   client polls. The [`rules`] engine is a tap on its write path.
//! - [`service`] serves the timeline MCP query tools over the same parquet
//!   store and WAL, through [`engine`]. An `entry_kind='conversation'` row is
//!   returned as stored, with a NULL description: parquet never held its text.
//!
//! The service is the one place that links the columnar stack (arrow, parquet,
//! datafusion and the `json_extract` UDF). It stands without `x0k-dev-daemon`:
//! the dev daemon can crash, rebuild, or stop and the timeline is still written
//! and still answers queries.
//!
//! ## Parity
//!
//! The query surface is preserved bit-for-bit. The 58-column unified schema
//! ([`schema::timeline_columns`]) rides unchanged; the causality column is
//! literally `parent_id`; `storage_tier`/`retention_class` are sink-time
//! routing the writer populates (the query path never sets them); recent
//! rows stay instantly queryable via the shared WAL read. The
//! `timeline-parity-corpus.json` oracle pins this.

pub mod call_stack;
pub mod cli;
pub mod config;
pub mod engine;
pub mod instance;
pub mod mcp_http;
pub mod otlp;
pub mod resident;
pub mod rules;
pub mod schema;
pub mod seq;
pub mod service;
pub mod trace_override_registry;
pub mod writer;
pub mod writer_client;

pub use call_stack::{CallStackNode, QueryCallStack, QueryCallStackResult, TimelineEntryJson};
pub use engine::{
    arrow_col_to_json, batches_to_rows, inject_limit, timeline_dir, validate_select_only,
    EventFamilies, TimelineEngine,
};
pub use instance::{InstanceLock, InstanceLockError};
pub use schema::{
    entry_kinds, examples, timeline_columns, ColumnDescription, DescribeTimelineResult,
    EntryKindDescription, SqlExample,
};
pub use service::{
    DescribeTimeline, QueryTimelineSql, QueryTimelineSqlResult, TimelineQueryHandler,
};
pub use writer::{
    spawn_timeline_pipeline, PipelineConfig, TimelineWriter, TIMELINE_CHANNEL_CAPACITY,
};
