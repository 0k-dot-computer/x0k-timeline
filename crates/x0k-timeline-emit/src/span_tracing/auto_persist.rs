//! Auto-persist and timeline forwarding tasks spawned by init_logging
//!
//! Provides two strategies for timeline persistence:
//!
//! 1. **Forwarding** (default): Sends `TimelineRecord`s over IPC to the timeline
//!    service (`x0k-timeline`, at the [`TimelineEndpoint`]'s socket),
//!    which acts as the single writer to the parquet timeline store. This avoids
//!    the file-sprawl problem of many processes each writing small parquet files.
//!
//! 2. **Crash-queue** (fallback): When the daemon is unreachable, records are
//!    buffered to a lightweight disk-backed *postcard* file (no arrow/parquet/
//!    datafusion) and drained back through the forward path on reconnect. See
//!    [`crate::span_tracing::crash_queue`]. This is what lets ordinary clients
//!    avoid linking the columnar stack while still never losing records — the
//!    daemon remains the single authoritative writer of the parquet + shared WAL.
//!
//! # When to use
//!
//! - **SDK clients/tools**: Enable the `auto-persist` feature (on by default) and
//!   timeline data will be automatically persisted without any additional code.
//!
//! - **The timeline writer** (and any process that consumes the timeline channel
//!   itself): sets `X0K_SPAN_DAEMON_MODE=1` before calling `init_logging()` and
//!   manages timeline consumption directly via
//!   `x0k_activity_sink::run_timeline_flush_task()`. Every other daemon forwards
//!   like any client.
//!
//! # Environment Variables
//!
//! - `X0K_SPAN_DAEMON_MODE`: When set to "1" or "true", disables auto-persist
//!   even if the feature is enabled. Daemons use this to manage timeline consumption
//!   directly with proper shutdown handling.
//! - `X0K_TIMELINE_FORWARD`: When set to "0" or "false", disables IPC forwarding
//!   and falls back to per-process auto-persist. Default: enabled.

use tokio::sync::mpsc;
use tracing::debug;
use x0k_ipc_transport::{IpcReadHalf, IpcWriteHalf};
use x0k_timeline_record::record::{TimelineRecord, INGEST_TIMELINE_METHOD};

/// An established, split IPC connection to the timeline writer, plus the
/// monotonic request-id counter. Shared by the live forwarding loop and the
/// crash-queue drain so both speak the identical `IngestTimeline` wire.
pub(crate) struct ForwarderConn {
    reader: Box<dyn IpcReadHalf>,
    writer: Box<dyn IpcWriteHalf>,
    request_id: u64,
}

/// Connect to the timeline writer's socket, returning a ready [`ForwarderConn`].
pub(crate) async fn try_connect_forwarder(socket_path: &str) -> anyhow::Result<ForwarderConn> {
    use x0k_ipc_transport::local_socket::LocalSocketTransport;
    use x0k_ipc_transport::{IpcTransport, TransportConfig};

    let config = TransportConfig::LocalSocket {
        path: socket_path.to_string(),
    };
    let conn = LocalSocketTransport.connect(&config).await?;
    let (reader, writer) = conn.split();
    Ok(ForwarderConn {
        reader,
        writer,
        request_id: 0,
    })
}

/// Resolve `lazy_data` on activity records before serialization
/// (lazy_data is `#[serde(skip)]`, same pattern as flush_task.rs:292-297).
fn resolve_lazy_data(batch: &mut [TimelineRecord]) {
    for record in batch {
        if let TimelineRecord::Activity(ref mut activity, _) = record {
            if activity.context.activity_data.is_none() {
                if let Some(ref lazy) = activity.context.lazy_data {
                    activity.context.activity_data = lazy.to_json_string().ok();
                }
            }
            activity.context.lazy_data = None;
        }
    }
}

/// Room left in an IPC frame for everything around the `IngestTimeline`
/// payload: the request envelope (id, method, option tags) and the versioned
/// framing header. The real overhead is under 64 bytes; the margin keeps a
/// request that passes this check from ever tripping the frame cap in `send`.
pub(crate) const REQUEST_ENVELOPE_HEADROOM: usize = 4096;

/// How the sink answered one `IngestTimeline` request.
#[derive(Debug)]
pub(crate) enum ForwardOutcome {
    /// Every record was accepted.
    Delivered,
    /// Only the first `accepted` records were taken: the sink's pipeline closed
    /// part-way through the request.
    Partial { accepted: usize },
    /// The request was refused as a whole: the sink answered with an error
    /// (for the timeline writer, a payload that does not deserialize, or a sink with
    /// no timeline pipeline), or the records would not encode. The connection
    /// is still usable.
    Rejected { reason: String },
    /// The encoded request is larger than the IPC frame cap, so nothing was
    /// sent. The connection is still usable.
    TooLarge { bytes: usize },
    /// Sending or receiving failed; the connection is no longer usable.
    Transport { error: String },
}

/// Borrowed twin of [`x0k_timeline_record::record::IngestTimeline`]. Serde encodes a slice and a `Vec` as the
/// same sequence, so this serializes byte-for-byte like the owned request and
/// lets a caller send any sub-slice of its records without cloning them.
#[derive(serde::Serialize)]
#[serde(rename = "IngestTimeline")]
struct IngestTimelineRef<'a> {
    records: &'a [TimelineRecord],
}

/// Send `records` over `conn` as one `IngestTimeline` request and classify the
/// sink's answer. Records are assumed already lazy-resolved by the caller.
///
/// An empty slice is a valid request: a healthy sink accepts it (0 of 0), so it
/// doubles as a probe of whether the sink takes this method at all.
pub(crate) async fn forward_records(
    conn: &mut ForwarderConn,
    records: &[TimelineRecord],
) -> ForwardOutcome {
    use x0k_ipc_transport::framing;

    let payload =
        match x0k_serialization::serialize::to_bytes_unchecked(&IngestTimelineRef { records }) {
            Ok(p) => p,
            Err(e) => {
                return ForwardOutcome::Rejected {
                    reason: format!("encode failed: {e}"),
                }
            }
        };
    if payload.len() + REQUEST_ENVELOPE_HEADROOM > framing::MAX_MESSAGE_SIZE {
        return ForwardOutcome::TooLarge {
            bytes: payload.len(),
        };
    }

    // Wrap in a wire-compatible RequestEnvelope
    // (field order must match x0k_transport::socket_transport::RequestEnvelope exactly)
    let envelope = RequestEnvelope {
        id: conn.request_id,
        method: INGEST_TIMELINE_METHOD.to_string(),
        payload,
        traceparent: None,
        tracestate: None,
    };
    conn.request_id = conn.request_id.wrapping_add(1);

    if let Err(e) = framing::send(&mut conn.writer, &envelope).await {
        return ForwardOutcome::Transport {
            error: e.to_string(),
        };
    }

    match framing::recv::<ResponseEnvelope, _>(&mut conn.reader).await {
        Ok(Some(resp)) => match resp.result {
            Ok(result_bytes) => match x0k_serialization::serialize::from_bytes_unchecked::<
                x0k_timeline_record::record::IngestTimelineResult,
            >(&result_bytes)
            {
                Ok(result) if result.accepted < records.len() => ForwardOutcome::Partial {
                    accepted: result.accepted,
                },
                // An Ok reply whose count does not decode still means the sink
                // processed the request; treat it as delivered rather than
                // resend (and duplicate) it.
                _ => ForwardOutcome::Delivered,
            },
            Err(reason) => ForwardOutcome::Rejected { reason },
        },
        Ok(None) => ForwardOutcome::Transport {
            error: "sink closed the connection".to_string(),
        },
        Err(e) => ForwardOutcome::Transport {
            error: e.to_string(),
        },
    }
}

/// Send one batch of records over `conn` as an `IngestTimeline` request and await
/// the daemon's accept count.
///
/// On any failure (encode, send, disconnect, rejection or partial accept)
/// returns `Err` with the records the daemon did NOT accept so the caller can
/// preserve them. A rejected batch is handed back too: the crash-queue drain,
/// which is where the live forwarder sends it, isolates and quarantines the
/// record the sink refused instead of dropping the whole batch unseen.
/// Records are assumed already lazy-resolved by the caller.
pub(crate) async fn forward_batch(
    conn: &mut ForwarderConn,
    mut batch: Vec<TimelineRecord>,
) -> Result<(), Vec<TimelineRecord>> {
    match forward_records(conn, &batch).await {
        ForwardOutcome::Delivered => Ok(()),
        // Preserve only the unaccepted remainder.
        ForwardOutcome::Partial { accepted } => Err(batch.split_off(accepted)),
        ForwardOutcome::Rejected { .. }
        | ForwardOutcome::TooLarge { .. }
        | ForwardOutcome::Transport { .. } => Err(batch),
    }
}

/// Where a process's timeline records go: the timeline writer's socket, and
/// the directory holding per-process crash-queue segments while the writer is
/// unreachable. The embedder decides both; x0k-env derives them from the
/// active profile.
#[derive(Clone, Debug)]
pub struct TimelineEndpoint {
    pub socket_path: String,
    pub segment_dir: std::path::PathBuf,
}

impl TimelineEndpoint {
    /// The endpoint for a profile: its segments live in the profile's segment
    /// directory, where the timeline writer looks for them.
    pub fn for_profile(socket_path: String, profile_dir: &std::path::Path) -> Self {
        Self {
            socket_path,
            segment_dir: x0k_activity_sink::segment::segment_dir(profile_dir),
        }
    }
}

/// Entry point: try IPC forwarding, fall back to the disk-backed crash-queue.
///
/// If forwarding fails (daemon unreachable, connection error), records are
/// buffered to a lightweight postcard crash-queue and drained on reconnect —
/// no in-process columnar (arrow/parquet/datafusion) work happens in clients.
pub async fn run_timeline_forwarder_task(
    receiver: mpsc::Receiver<TimelineRecord>,
    endpoint: TimelineEndpoint,
) {
    super::crash_queue::adopt_segment_dir(&endpoint.segment_dir);
    if !is_forwarding_enabled() {
        debug!("Timeline forwarding disabled; using disk-backed crash-queue");
        super::crash_queue::run_crash_queue_fallback(receiver, Vec::new(), endpoint).await;
        return;
    }

    match run_forwarding_loop(&endpoint.socket_path, receiver).await {
        Ok(()) => debug!("Timeline forwarder exited cleanly"),
        Err((err, receiver, pending)) => {
            tracing::warn!(
                error = %err,
                preserved = pending.len(),
                "Timeline forwarding failed, falling back to disk-backed crash-queue"
            );
            super::crash_queue::run_crash_queue_fallback(receiver, pending, endpoint).await;
        }
    }
}

/// Run the IPC forwarding loop: connect, then forward batches.
///
/// Returns `Ok(())` on clean shutdown (channel closed), or `Err((error, receiver, pending))`
/// on connection failure so the caller can fall back to the crash-queue.
async fn run_forwarding_loop(
    socket_path: &str,
    receiver: mpsc::Receiver<TimelineRecord>,
) -> Result<
    (),
    (
        anyhow::Error,
        mpsc::Receiver<TimelineRecord>,
        Vec<TimelineRecord>,
    ),
> {
    let mut conn = match try_connect_forwarder(socket_path).await {
        Ok(c) => c,
        Err(e) => return Err((e, receiver, Vec::new())),
    };
    // On first healthy connect, recover any panic rows left by crashed processes.
    super::crash_queue::sweep_stray_panic_queues_on(&mut conn).await;
    run_forwarding_loop_on(conn, receiver).await
}

/// Run the forwarding loop over an already-established connection.
///
/// Shared by first-connect and by the crash-queue's post-drain resume so the
/// reconnect path reuses the exact same batching + accept semantics.
pub(crate) async fn run_forwarding_loop_on(
    mut conn: ForwarderConn,
    mut receiver: mpsc::Receiver<TimelineRecord>,
) -> Result<
    (),
    (
        anyhow::Error,
        mpsc::Receiver<TimelineRecord>,
        Vec<TimelineRecord>,
    ),
> {
    loop {
        // Wait for at least one record
        let first = match receiver.recv().await {
            Some(r) => r,
            None => return Ok(()), // Channel closed, clean shutdown
        };

        // Drain up to 199 more records without waiting (batch of up to 200)
        let mut batch = Vec::with_capacity(200);
        batch.push(first);
        while batch.len() < 200 {
            match receiver.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }

        resolve_lazy_data(&mut batch);

        if let Err(pending) = forward_batch(&mut conn, batch).await {
            return Err((
                anyhow::anyhow!("Timeline forward batch failed or partially accepted"),
                receiver,
                pending,
            ));
        }
    }
}

/// Wire-compatible envelope types for IPC communication.
///
/// These mirror `x0k_transport::socket_transport::{RequestEnvelope, ResponseEnvelope}`
/// exactly (same field order, same types). Defined inline to avoid circular dependency
/// `x0k-timeline-emit → x0k-transport → x0k-env → x0k-timeline-emit`.
///
/// IMPORTANT: If fields are added/reordered in x0k-transport/src/socket_transport.rs:54,
/// they must be updated here too.
#[derive(serde::Serialize, serde::Deserialize)]
struct RequestEnvelope {
    id: u64,
    method: String,
    payload: Vec<u8>,
    traceparent: Option<String>,
    tracestate: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ResponseEnvelope {
    id: u64,
    result: Result<Vec<u8>, String>,
    traceparent: Option<String>,
}

/// Check if timeline forwarding is enabled
fn is_forwarding_enabled() -> bool {
    match std::env::var("X0K_TIMELINE_FORWARD") {
        Ok(v) => v != "0" && !v.eq_ignore_ascii_case("false"),
        Err(_) => true, // Enabled by default
    }
}

// The former in-process parquet fallback (`run_auto_persist_timeline_task` +
// `merge_pending_records`) has been replaced by the disk-backed postcard
// crash-queue in `super::crash_queue`. Clients no longer link the columnar
// stack to survive a daemon outage; the daemon remains the single authoritative
// writer of the parquet timeline + shared WAL.

/// Check if daemon mode is enabled
///
/// Returns true if `X0K_SPAN_DAEMON_MODE` is set to "1" or "true".
/// Daemons set this to disable auto-persist and manage span consumption directly.
pub fn is_daemon_mode() -> bool {
    std::env::var("X0K_SPAN_DAEMON_MODE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn test_is_daemon_mode_false_by_default() {
        let _guard = env_lock();
        std::env::remove_var("X0K_SPAN_DAEMON_MODE");
        assert!(!is_daemon_mode());
    }

    #[test]
    fn test_is_daemon_mode_true_when_set_to_1() {
        let _guard = env_lock();
        std::env::set_var("X0K_SPAN_DAEMON_MODE", "1");
        assert!(is_daemon_mode());
        std::env::remove_var("X0K_SPAN_DAEMON_MODE");
    }

    #[test]
    fn test_is_daemon_mode_case_insensitive() {
        let _guard = env_lock();
        std::env::set_var("X0K_SPAN_DAEMON_MODE", "TRUE");
        assert!(is_daemon_mode());
        std::env::remove_var("X0K_SPAN_DAEMON_MODE");
    }

    #[test]
    fn test_forwarding_disabled_by_env() {
        let _guard = env_lock();
        std::env::set_var("X0K_TIMELINE_FORWARD", "0");
        assert!(!is_forwarding_enabled());
        std::env::set_var("X0K_TIMELINE_FORWARD", "false");
        assert!(!is_forwarding_enabled());
        std::env::set_var("X0K_TIMELINE_FORWARD", "1");
        assert!(is_forwarding_enabled());
        std::env::remove_var("X0K_TIMELINE_FORWARD");
        assert!(is_forwarding_enabled()); // default: enabled
    }
}
