//! The timeline writer: the machine's one columnar writer
//! (`observability-timeline` §4, §9).
//!
//! Every `x0k-env` client forwards `IngestTimeline { records }` batches to
//! [`crate::config::ingest_socket`]. The writer feeds them into this
//! process's timeline channel, whose single consumer — the flush task of
//! `x0k-timeline-sink` — routes each row, writes the shared WAL
//! (`{timeline_dir}/wal/{pid}.wal`, this process's pid), flushes prefixed
//! Parquet files, replays the WAL of a writer that is gone, and takes in the
//! crash-queue segment of every client that exited while the writer was
//! unreachable. The same socket serves the trace-override registry, which every
//! client already reaches here.
//!
//! The writer runs inside the timeline service (`x0k-timeline`) beside
//! the query surface, so it stands without `x0k-dev-daemon`: stopping the dev
//! daemon stops neither timeline writes nor timeline queries.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, Mutex, Notify, Semaphore};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use x0k_activity_sink::{TimelineRecord, WriterPause};
use x0k_ipc_transport::envelope::{RequestEnvelope, ResponseEnvelope};
use x0k_ipc_transport::local_socket::LocalSocketTransport;
use x0k_ipc_transport::{framing, IpcConnection, IpcListener, IpcTransport, TransportConfig};
use x0k_serialization::{serialize, PostcardSerializableTrait, SerializationError};
use x0k_timeline_record::pause::{
    PauseState, PauseTimeline, ReadPauseState, ResumeTimeline, PAUSE_TIMELINE_METHOD,
    RESUME_TIMELINE_METHOD, TIMELINE_PAUSE_STATE_METHOD,
};
use x0k_timeline_record::record::{
    now_micros, ActivityContext, CompletedActivity, IngestTimeline, IngestTimelineResult,
    StorageTier, INGEST_TIMELINE_METHOD,
};
use x0k_timeline_record::overrides::{
    parse_level_str, ClearOverridesForSession, ClearOverridesForSessionResult, ClearTraceOverride,
    ClearTraceOverrideResult, GetEffectiveOverrides, GetEffectiveOverridesResult,
    HeartbeatMcpClient, HeartbeatMcpClientResult, ListTraceOverrides, ListTraceOverridesResult,
    OverrideOwnerWire, SetTraceOverride, SetTraceOverrideResult,
    CLEAR_OVERRIDES_FOR_SESSION_METHOD, CLEAR_TRACE_OVERRIDE_METHOD,
    GET_EFFECTIVE_OVERRIDES_METHOD, HEARTBEAT_MCP_CLIENT_METHOD, LIST_TRACE_OVERRIDES_METHOD,
    SET_TRACE_OVERRIDE_METHOD,
};

use crate::trace_override_registry::{OverrideOwner, TraceOverrideRegistry};

/// Concurrent client connections the writer serves; one more is refused
/// rather than queued, so the accept loop never blocks.
const MAX_CONNECTIONS: usize = 128;

/// Where the timeline pipeline keeps its state.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The Parquet store and its `wal/` subdirectory.
    pub timeline_dir: PathBuf,
    /// The crash-queue segment directory the writer takes orphaned segments
    /// from (`{profile}/timeline-crash-queue`).
    pub segment_dir: PathBuf,
}

impl PipelineConfig {
    /// The configured pipeline: the store at [`crate::timeline_dir`] and the
    /// segment directory under [`crate::config::state_dir`].
    pub fn from_env() -> Self {
        Self {
            timeline_dir: crate::timeline_dir(),
            segment_dir: x0k_activity_sink::segment::segment_dir(&crate::config::state_dir()),
        }
    }
}

/// Capacity of the channel the writer feeds the flush task through, high enough
/// to absorb a burst from many processes: 50k rows of about 500 bytes is 25 MB.
pub const TIMELINE_CHANNEL_CAPACITY: usize = 50_000;

/// Open the store and start the flush task that consumes `receiver`: the
/// columnar encode, the shared WAL, retention, and the intake of orphaned
/// crash-queue segments. Also starts the startup reap and compaction of the
/// store. Returns the flush task, or `None` when the store cannot be opened.
pub async fn spawn_timeline_pipeline(
    receiver: mpsc::Receiver<TimelineRecord>,
    shutdown: Arc<Notify>,
    pause: Arc<WriterPause>,
    config: PipelineConfig,
) -> Option<tokio::task::JoinHandle<()>> {
    if let Err(e) = std::fs::create_dir_all(config.timeline_dir.join("wal")) {
        warn!(path = %config.timeline_dir.display(), error = %e, "failed to create timeline dir");
    }
    let store = match x0k_parquet::ParquetStore::with_table_name(&config.timeline_dir, "timeline")
    {
        Ok(store) => Arc::new(store),
        Err(e) => {
            error!(error = %e, path = %config.timeline_dir.display(), "failed to open the timeline store; nothing will be written");
            return None;
        }
    };
    spawn_startup_timeline_maintenance(store.clone());

    let session_ctx = x0k_activity_sink::SessionContext::from_env();
    let segment_dir = config.segment_dir;
    let handle = tokio::spawn(async move {
        x0k_activity_sink::run_timeline_flush_task(
            receiver,
            shutdown,
            pause,
            session_ctx,
            store,
            None,
            None,
            Some(segment_dir),
        )
        .await;
    });
    info!(path = %config.timeline_dir.display(), "timeline flush task started (Parquet + WAL + SHM)");
    Some(handle)
}

/// Reap stale files by tier and compact a backlog of small files once at
/// startup. The flush task's periodic pass handles the steady state; this
/// clears an inherited backlog faster.
fn spawn_startup_timeline_maintenance(store: Arc<x0k_parquet::ParquetStore>) {
    {
        let store = store.clone();
        let retention = x0k_activity_sink::TieredRetentionConfig::from_config();
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let cutoffs = retention.tier_cutoffs(now_micros);
        tokio::spawn(async move {
            match store.retain_tiered(&cutoffs).await {
                Ok(0) => {}
                Ok(n) => info!(deleted = n, "startup reap: cleaned stale timeline files (tiered)"),
                Err(e) => warn!(error = %e, "startup reap failed"),
            }
        });
    }

    tokio::spawn(async move {
        let options = x0k_parquet::CompactionOptions {
            min_input_files: 32,
            max_input_files: 1024,
            target_rows: 100_000,
            min_file_age: Duration::from_secs(5 * 60),
        };
        let mut total_input_files = 0usize;
        let mut total_output_files = 0usize;
        let mut total_rows = 0usize;
        let mut skipped_attempts = 0usize;

        for _ in 0..512 {
            match store.compact_once(options.clone()).await {
                Ok(result) if result.input_files > 0 => {
                    total_input_files += result.input_files;
                    total_output_files += result.output_files;
                    total_rows += result.rows_written;
                    skipped_attempts = 0;
                    tokio::task::yield_now().await;
                }
                Ok(result) if result.attempted => {
                    skipped_attempts += 1;
                    if skipped_attempts >= 3 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Ok(_) => break,
                Err(e) => {
                    warn!(error = %e, "startup timeline compaction failed");
                    break;
                }
            }
        }

        if total_input_files > 0 {
            info!(
                input_files = total_input_files,
                output_files = total_output_files,
                rows = total_rows,
                "startup timeline compaction completed"
            );
        }
    });
}

/// The writer's request surface: `IngestTimeline` and the trace-override
/// methods, served on the timeline socket.
#[derive(Clone)]
pub struct TimelineWriter {
    /// The flush task's channel.
    timeline_tx: mpsc::Sender<TimelineRecord>,
    /// The rules engine's tap: a clone of each ingested record, dropped when
    /// the tap is full so the write path never waits on it.
    rules_tx: Option<mpsc::Sender<TimelineRecord>>,
    trace_overrides: TraceOverrideRegistry,
    pause: Arc<WriterPause>,
    /// Holds a pause or resume to one at a time, so each writes one row.
    pause_turn: Arc<Mutex<()>>,
}

impl TimelineWriter {
    /// A writer that hands ingested records to `timeline_tx`.
    pub fn new(timeline_tx: mpsc::Sender<TimelineRecord>) -> Self {
        Self {
            timeline_tx,
            rules_tx: None,
            trace_overrides: TraceOverrideRegistry::new(),
            pause: Arc::default(),
            pause_turn: Arc::default(),
        }
    }

    /// Share `pause` with the flush task, so records that reach the pipeline
    /// by another way than this writer are held to it too.
    pub fn with_pause(mut self, pause: Arc<WriterPause>) -> Self {
        self.pause = pause;
        self
    }

    /// Tap every ingested record into the rules engine
    /// ([`crate::rules::run_rules_evaluation_task`]).
    pub fn with_rules_tap(mut self, rules_tx: mpsc::Sender<TimelineRecord>) -> Self {
        self.rules_tx = Some(rules_tx);
        self
    }

    /// The trace-override registry this writer serves.
    pub fn trace_overrides(&self) -> &TraceOverrideRegistry {
        &self.trace_overrides
    }

    /// Bind the timeline socket (replacing a stale socket file).
    pub async fn bind(socket_path: &str) -> Result<Box<dyn IpcListener>> {
        if let Some(parent) = std::path::Path::new(socket_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let config = TransportConfig::LocalSocket {
            path: socket_path.to_string(),
        };
        LocalSocketTransport.bind(&config).await
    }

    /// Accept connections on `listener` until `shutdown` is notified.
    pub async fn serve(self, mut listener: Box<dyn IpcListener>, shutdown: Arc<Notify>) {
        let writer = Arc::new(self);
        let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("timeline writer: shutdown signalled, stopping accept loop");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((connection, peer_addr)) => {
                            let Ok(permit) = semaphore.clone().try_acquire_owned() else {
                                error!("timeline writer: connection limit reached ({MAX_CONNECTIONS}), rejecting {peer_addr}");
                                drop(connection);
                                continue;
                            };
                            let writer = Arc::clone(&writer);
                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = serve_client(connection, writer).await {
                                    let text = e.to_string();
                                    if !text.contains("EOF")
                                        && !text.contains("end of file")
                                        && !text.contains("Closed")
                                    {
                                        error!(peer = %peer_addr, error = %e, "timeline writer: connection error");
                                    }
                                }
                            });
                        }
                        Err(e) => error!(error = %e, "timeline writer: accept failed"),
                    }
                }
            }
        }
    }

    /// Route one request by method name to its handler; the result is the
    /// serialized response payload.
    pub async fn dispatch(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, DispatchError> {
        match method {
            INGEST_TIMELINE_METHOD => {
                serve_op::<IngestTimeline, _, _, _>(payload, |op| async move {
                    self.ingest_timeline(&op).await
                })
                .await
            }
            SET_TRACE_OVERRIDE_METHOD => {
                serve_op::<SetTraceOverride, _, _, _>(payload, |op| async move {
                    self.set_trace_override(&op)
                })
                .await
            }
            CLEAR_TRACE_OVERRIDE_METHOD => {
                serve_op::<ClearTraceOverride, _, _, _>(payload, |op| async move {
                    self.clear_trace_override(&op)
                })
                .await
            }
            LIST_TRACE_OVERRIDES_METHOD => {
                serve_op::<ListTraceOverrides, _, _, _>(payload, |_op| async move {
                    Ok(self.list_trace_overrides())
                })
                .await
            }
            HEARTBEAT_MCP_CLIENT_METHOD => {
                serve_op::<HeartbeatMcpClient, _, _, _>(payload, |op| async move {
                    self.heartbeat_mcp_client(&op)
                })
                .await
            }
            GET_EFFECTIVE_OVERRIDES_METHOD => {
                serve_op::<GetEffectiveOverrides, _, _, _>(payload, |op| async move {
                    Ok(self.effective_overrides(&op))
                })
                .await
            }
            CLEAR_OVERRIDES_FOR_SESSION_METHOD => {
                serve_op::<ClearOverridesForSession, _, _, _>(payload, |op| async move {
                    Ok(self.clear_overrides_for_session(&op))
                })
                .await
            }
            PAUSE_TIMELINE_METHOD => {
                serve_op::<PauseTimeline, _, _, _>(payload, |op| async move { self.pause(&op).await })
                    .await
            }
            RESUME_TIMELINE_METHOD => {
                serve_op::<ResumeTimeline, _, _, _>(payload, |_op| async move { self.resume().await })
                    .await
            }
            TIMELINE_PAUSE_STATE_METHOD => {
                serve_op::<ReadPauseState, _, _, _>(payload, |_op| async move {
                    Ok(self.pause_state())
                })
                .await
            }
            unknown => Err(DispatchError::UnknownMethod(unknown.to_string())),
        }
    }

    /// `dev.ingest_timeline`: hand forwarded records to the flush task.
    ///
    /// Answers with how many records the pipeline took, in order; a closed
    /// pipeline stops the count, and the client keeps the rest in its segment.
    /// A paused writer takes every record and discards all but the record
    /// class, so a client sees no failure from a pause.
    pub async fn ingest_timeline(&self, op: &IngestTimeline) -> Result<IngestTimelineResult> {
        let (mut accepted, mut discarded) = (0usize, 0usize);
        for record in &op.records {
            if !self.pause.keeps(record) {
                accepted += 1;
                discarded += 1;
                continue;
            }
            let mut record = record.clone();
            // `lazy_data` is `#[serde(skip)]`, so a forwarded record carries
            // none; resolve one anyway rather than lose its payload.
            if let TimelineRecord::Activity(ref mut activity, _) = record {
                if activity.context.activity_data.is_none() {
                    if let Some(ref lazy) = activity.context.lazy_data {
                        activity.context.activity_data = lazy.to_json_string().ok();
                    }
                }
                activity.context.lazy_data = None;
            }

            if let Some(ref rules_tx) = self.rules_tx {
                let _ = rules_tx.try_send(record.clone());
            }

            match self.timeline_tx.send(record).await {
                Ok(()) => accepted += 1,
                Err(_) => break,
            }
        }
        Ok(IngestTimelineResult { accepted, discarded })
    }

    /// The pause state as it stands.
    pub fn pause_state(&self) -> PauseState {
        self.pause.state()
    }

    /// `timeline.pause`: write the pause's row, then stop keeping records. A
    /// row the pipeline cannot take refuses the pause, so no gap goes
    /// unexplained. A writer already paused stays as it is.
    pub async fn pause(&self, op: &PauseTimeline) -> Result<PauseState> {
        let _turn = self.pause_turn.lock().await;
        if self.pause.is_paused() {
            return Ok(self.pause.state());
        }
        let at = now_micros();
        let metadata = [("timeline.pause.reason".to_string(), op.reason.clone())];
        let row = pause_row("timeline.paused", at, op.reason.clone(), metadata.into());
        self.timeline_tx
            .send(row)
            .await
            .map_err(|_| anyhow!("the timeline pipeline is closed, so the pause has no row"))?;
        self.pause.begin(op.reason.clone(), at);
        info!(reason = %op.reason, "timeline writer paused");
        Ok(self.pause.state())
    }

    /// `timeline.resume`: go back to keeping records, then write the row with
    /// what the pause discarded. Answers the state it ended. A writer not
    /// paused stays as it is.
    pub async fn resume(&self) -> Result<PauseState> {
        let _turn = self.pause_turn.lock().await;
        if !self.pause.is_paused() {
            return Ok(self.pause.state());
        }
        let ended = self.pause.end();
        let discarded = ended.discarded;
        let metadata = [
            ("timeline.discarded.span".to_string(), discarded.span.to_string()),
            ("timeline.discarded.log".to_string(), discarded.log.to_string()),
            ("timeline.discarded.event".to_string(), discarded.event.to_string()),
        ];
        let text = format!(
            "resumed after discarding {} span, {} log and {} event rows",
            discarded.span, discarded.log, discarded.event
        );
        let row = pause_row("timeline.resumed", now_micros(), text, metadata.into());
        if self.timeline_tx.send(row).await.is_err() {
            warn!("the timeline pipeline is closed, so the resume has no row");
        }
        info!("timeline writer resumed");
        Ok(ended)
    }

    /// `dev.set_trace_override`
    pub fn set_trace_override(&self, op: &SetTraceOverride) -> Result<SetTraceOverrideResult> {
        // Normalize level early so callers get a clear error.
        let level = parse_level_str(&op.level).ok_or_else(|| {
            anyhow!(
                "invalid level '{}' (expected one of: trace, debug, info, warn, error)",
                op.level
            )
        })?;

        // Resolve owner from scope or fall back to caller's mcp_client_id.
        let (owner, override_id) = match op.scope.as_deref() {
            Some(scope) if scope.starts_with("session:") => {
                let session_id = scope.trim_start_matches("session:").to_string();
                if session_id.is_empty() {
                    return Err(anyhow!("scope 'session:' requires a session id"));
                }
                let owner = OverrideOwner::Session(session_id);
                let id = self
                    .trace_overrides
                    .add(owner.clone(), op.module.clone(), level.to_string());
                (owner, id)
            }
            Some(scope) if scope.starts_with("duration:") => {
                let secs_str = scope.trim_start_matches("duration:");
                let secs: u64 = secs_str.parse().map_err(|_| {
                    anyhow!("scope 'duration:N' requires an integer number of seconds")
                })?;
                if secs == 0 {
                    return Err(anyhow!("scope 'duration:N' requires N > 0"));
                }
                let id = self.trace_overrides.add_duration(
                    op.module.clone(),
                    level.to_string(),
                    Duration::from_secs(secs),
                );
                // The timer owner lives inside add_duration; the response
                // carries its wire-equivalent.
                (OverrideOwner::Timer(id), id)
            }
            Some(other) => {
                return Err(anyhow!(
                    "unknown scope '{}'; expected 'session:<id>' or 'duration:<secs>'",
                    other
                ));
            }
            None => {
                let client_str = op.mcp_client_id.as_deref().ok_or_else(|| {
                    anyhow!(
                        "no scope and no mcp_client_id provided; \
                         set scope=\"session:<id>\" or scope=\"duration:<secs>\" \
                         when calling without an MCP client identity"
                    )
                })?;
                let client_id = Uuid::parse_str(client_str)
                    .map_err(|e| anyhow!("invalid mcp_client_id '{}': {}", client_str, e))?;
                let owner = OverrideOwner::McpClient(client_id);
                let id = self
                    .trace_overrides
                    .add(owner.clone(), op.module.clone(), level.to_string());
                (owner, id)
            }
        };

        info!(
            id = %override_id,
            ?owner,
            module = %op.module,
            level,
            "trace override registered"
        );

        Ok(SetTraceOverrideResult {
            id: override_id.to_string(),
            owner: owner_to_wire(&owner),
            module_prefix: op.module.clone(),
            level: level.to_string(),
        })
    }

    /// `dev.clear_trace_override`
    pub fn clear_trace_override(&self, op: &ClearTraceOverride) -> Result<ClearTraceOverrideResult> {
        if let Some(id_str) = op.id.as_deref() {
            let id = Uuid::parse_str(id_str)
                .map_err(|e| anyhow!("invalid override id '{}': {}", id_str, e))?;
            let removed = self.trace_overrides.remove_by_id(id);
            return Ok(ClearTraceOverrideResult {
                removed: if removed { 1 } else { 0 },
            });
        }
        if let Some(session_id) = op.session_id.as_deref() {
            let owner = OverrideOwner::Session(session_id.to_string());
            let removed = self.trace_overrides.remove_by_owner(&owner) as u32;
            return Ok(ClearTraceOverrideResult { removed });
        }
        Err(anyhow!(
            "clear_trace_override requires either `id` or `session_id`"
        ))
    }

    /// `dev.list_trace_overrides`
    pub fn list_trace_overrides(&self) -> ListTraceOverridesResult {
        ListTraceOverridesResult {
            overrides: self.trace_overrides.list_active_wire(),
        }
    }

    /// `dev.heartbeat_mcp_client`
    pub fn heartbeat_mcp_client(&self, op: &HeartbeatMcpClient) -> Result<HeartbeatMcpClientResult> {
        let client_id = Uuid::parse_str(&op.mcp_client_id)
            .map_err(|e| anyhow!("invalid mcp_client_id '{}': {}", op.mcp_client_id, e))?;
        self.trace_overrides.heartbeat_mcp_client(client_id);
        debug!(%client_id, "trace override heartbeat");
        Ok(HeartbeatMcpClientResult::default())
    }

    /// `dev.get_effective_overrides`
    pub fn effective_overrides(&self, op: &GetEffectiveOverrides) -> GetEffectiveOverridesResult {
        let (version, unchanged, overrides) = self.trace_overrides.effective_set(op.since_version);
        GetEffectiveOverridesResult {
            version,
            unchanged,
            overrides,
        }
    }

    /// `dev.clear_overrides_for_session`
    pub fn clear_overrides_for_session(
        &self,
        op: &ClearOverridesForSession,
    ) -> ClearOverridesForSessionResult {
        let owner = OverrideOwner::Session(op.session_id.clone());
        let removed = self.trace_overrides.remove_by_owner(&owner) as u32;
        if removed > 0 {
            info!(
                session_id = %op.session_id,
                removed,
                "cleared trace overrides on session removal"
            );
        }
        ClearOverridesForSessionResult { removed }
    }
}

/// Serve one client connection: versioned-framing requests in, responses out,
/// until the client disconnects.
async fn serve_client(connection: Box<dyn IpcConnection>, writer: Arc<TimelineWriter>) -> Result<()> {
    let (mut reader, mut sender) = connection.split();
    framing::serve_framed(&mut reader, &mut sender, |envelope: RequestEnvelope| {
        let writer = Arc::clone(&writer);
        async move {
            let result = match writer.dispatch(&envelope.method, &envelope.payload).await {
                Ok(bytes) => Ok(bytes),
                Err(e) => Err(e.to_string()),
            };
            ResponseEnvelope {
                id: envelope.id,
                result,
                traceparent: envelope.traceparent,
            }
        }
    })
    .await
}

/// Why a request's payload produced no response bytes.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// The payload is not the operation the method names.
    #[error("Deserialize error: {0}")]
    Deserialize(#[source] SerializationError),
    /// The operation's handler failed.
    #[error("{0}")]
    Handler(#[source] anyhow::Error),
    /// The handler's result did not serialize.
    #[error("Serialize error: {0}")]
    Serialize(#[source] SerializationError),
    /// No handler serves the method.
    #[error("Unknown method: {0}")]
    UnknownMethod(String),
}

/// Decode `payload` as an `Op`, run `handler` on it, and encode its answer.
async fn serve_op<Op, Answer, F, Fut>(payload: &[u8], handler: F) -> Result<Vec<u8>, DispatchError>
where
    Op: serde::de::DeserializeOwned + PostcardSerializableTrait,
    Answer: serde::Serialize + PostcardSerializableTrait,
    F: FnOnce(Op) -> Fut,
    Fut: Future<Output = Result<Answer>>,
{
    let op: Op = serialize::from_bytes(payload).map_err(DispatchError::Deserialize)?;
    let answer = handler(op).await.map_err(DispatchError::Handler)?;
    serialize::to_bytes(&answer).map_err(DispatchError::Serialize)
}

/// A pause or resume as a record-class row, so a query can tell a paused
/// window from a quiet one.
fn pause_row(
    event_type: &str,
    at_us: u64,
    description: String,
    metadata: std::collections::HashMap<String, String>,
) -> TimelineRecord {
    let context = ActivityContext {
        activity_type: event_type.to_string(),
        started_at: at_us,
        activity_data: Some(description),
        metadata,
        ..Default::default()
    };
    let activity = CompletedActivity {
        context,
        ended_at: at_us,
        duration_ms: 0,
    };
    TimelineRecord::Activity(activity, StorageTier::Record)
}

/// The wire form of an override's owner.
fn owner_to_wire(owner: &OverrideOwner) -> OverrideOwnerWire {
    match owner {
        OverrideOwner::McpClient(u) => OverrideOwnerWire::McpClient(u.to_string()),
        OverrideOwner::Session(s) => OverrideOwnerWire::Session(s.clone()),
        OverrideOwner::Timer(u) => OverrideOwnerWire::Timer(u.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use x0k_timeline_record::record::LogEventData;

    fn log_record(id: &str) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData {
            id: id.to_string(),
            level: "INFO".to_string(),
            target: "timeline_writer_test".to_string(),
            message: Some("ingested".to_string()),
            ..Default::default()
        })
    }

    /// An ingested batch reaches the flush task's channel whole and in order,
    /// and the rules tap sees each record too.
    #[tokio::test]
    async fn ingest_hands_every_record_to_the_pipeline_in_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let (rules_tx, mut rules_rx) = mpsc::channel(16);
        let writer = TimelineWriter::new(tx).with_rules_tap(rules_tx);
        let op = IngestTimeline {
            records: vec![log_record("a"), log_record("b"), log_record("c")],
        };
        let result = writer.ingest_timeline(&op).await.unwrap();
        assert_eq!(result.accepted, 3);
        for want in ["a", "b", "c"] {
            match rx.recv().await {
                Some(TimelineRecord::LogEvent(log)) => assert_eq!(log.id, want),
                other => panic!("pipeline got {other:?}"),
            }
            assert!(rules_rx.recv().await.is_some(), "rules tap missed {want}");
        }
    }

    /// A closed pipeline stops the count, so the client keeps what was not
    /// taken in its segment instead of believing it written.
    #[tokio::test]
    async fn a_closed_pipeline_accepts_nothing() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let writer = TimelineWriter::new(tx);
        let op = IngestTimeline {
            records: vec![log_record("a")],
        };
        assert_eq!(writer.ingest_timeline(&op).await.unwrap().accepted, 0);
    }

    /// The registry answers through the dispatch table under the wire method
    /// names clients and the x0k-dev MCP bridge send.
    #[tokio::test]
    async fn trace_override_methods_dispatch_by_wire_name() {
        let (tx, _rx) = mpsc::channel(1);
        let writer = TimelineWriter::new(tx);
        let set = SetTraceOverride {
            module: "x0k_timeline_query".to_string(),
            level: "debug".to_string(),
            scope: Some("session:s1".to_string()),
            mcp_client_id: None,
        };
        let bytes = writer
            .dispatch(SET_TRACE_OVERRIDE_METHOD, &serialize::to_bytes(&set).unwrap())
            .await
            .unwrap();
        let set_result: SetTraceOverrideResult = serialize::from_bytes(&bytes).unwrap();
        assert_eq!(set_result.module_prefix, "x0k_timeline_query");

        let bytes = writer
            .dispatch(
                LIST_TRACE_OVERRIDES_METHOD,
                &serialize::to_bytes(&ListTraceOverrides::default()).unwrap(),
            )
            .await
            .unwrap();
        let listed: ListTraceOverridesResult = serialize::from_bytes(&bytes).unwrap();
        assert_eq!(listed.overrides.len(), 1);
        assert_eq!(listed.overrides[0].id, set_result.id);

        let clear = ClearOverridesForSession {
            session_id: "s1".to_string(),
        };
        let bytes = writer
            .dispatch(CLEAR_OVERRIDES_FOR_SESSION_METHOD, &serialize::to_bytes(&clear).unwrap())
            .await
            .unwrap();
        let cleared: ClearOverridesForSessionResult = serialize::from_bytes(&bytes).unwrap();
        assert_eq!(cleared.removed, 1);

        assert!(writer.dispatch("dev.build_project", &[]).await.is_err());
    }
}
