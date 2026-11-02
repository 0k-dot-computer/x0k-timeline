//! Disk-backed postcard crash-queue: the lightweight fallback that replaces
//! the in-process parquet writer when the timeline writer is unreachable.
//!
//! # Why this exists
//!
//! Historically, when IPC forwarding to the dev-daemon failed, every client
//! fell back to writing parquet *in-process* (`run_auto_persist_timeline_task`).
//! That fallback is the reason ordinary clients had to link the entire columnar
//! stack (arrow + parquet + datafusion) — a large binary-size and compile-time
//! tax that contributed to the 15GB-VPS OOM link. The daemon is the single
//! authoritative writer to the parquet timeline + shared WAL; clients never
//! needed their own columnar encoder, only a way to *not lose records* while
//! the daemon is momentarily down.
//!
//! This module provides exactly that, with zero columnar dependencies:
//!
//! - **Buffer**: when forwarding fails, records are appended to a per-process,
//!   length-prefixed *postcard* file under the active profile's state dir. This
//!   is the same identity-preserving `TimelineRecord` the forward wire carries —
//!   no lossy remap — so a drained record is bit-identical to one forwarded live.
//! - **Drain**: a reconnect loop keeps trying the daemon; when it comes back, the
//!   on-disk queue is replayed through the normal `IngestTimeline` forward path
//!   and the delivered prefix removed. Live records continue to be buffered while
//!   disconnected.
//!
//! The queue is this process's local segment (`observability-timeline` §4,
//! `x0k_activity_sink::segment`): a process that exits while the daemon is down
//! leaves it on disk, and the daemon takes it in once this process no longer
//! holds its owner lock. The daemon remains the only process that owns the
//! columnar encode and the shared WAL the query surface reads.
//!
//! # How a drain proceeds
//!
//! The queue is sent in requests that fit the IPC frame cap
//! ([`x0k_ipc_transport::framing::MAX_MESSAGE_SIZE`], 10 MiB): at most
//! `QueueLimits::chunk_records` records and `QueueLimits::chunk_bytes` bytes
//! each. Sending the whole queue as one request is what wedged `grove-projections`
//! on 2026-09-22: once more than 10 MiB had queued during a daemon restart, the
//! single request could never be framed, `send` refused it locally, and every
//! retry re-appended the entire queue, record by record, for 2.5 days.
//!
//! A request the sink refuses is split in halves until the refused record stands
//! alone. A lone record is **quarantined** — moved to `{key}.quarantine`, counted
//! as a `system.timeline.records_dropped` drop (stage `crash_queue`, reason
//! `quarantined`), logged once with its shape, and skipped — only when the sink
//! is demonstrably healthy: it must accept an empty request right after refusing
//! the record. A sink that refuses even that is refusing everything (no timeline
//! pipeline, wrong method), so the drain stops and keeps every record. A record
//! too large for any frame, or one that does not decode, is quarantined without
//! asking the sink.
//!
//! A drain that stops early removes what it delivered or quarantined with one
//! atomic rewrite of the remainder (write, fsync, rename); a drain that finishes
//! truncates the file; a drain that resolved nothing writes nothing. Consecutive
//! stalled drains back off from 5 s to 5 min.
//!
//! # Bound
//!
//! The queue file is capped at `QueueLimits::max_bytes` (32 MiB by default).
//! A full queue **refuses new records**, counting each as a
//! `system.timeline.records_dropped` drop (stage `crash_queue`, reason `full`).
//! Refuse-new over drop-oldest because (1) it costs no I/O once full, while
//! drop-oldest on an append-only file means rewriting it whenever the bound is
//! reached — and this queue fills exactly when the sink is gone for good, on a
//! machine where the churn is the harm; (2) it keeps the start of the outage,
//! the records leading into the failure, which a postmortem needs more than the
//! repetitive tail; (3) it is the policy one layer up, where the timeline
//! channel's `try_send` refuses new records when full under the same counter.
//!
//! # File format
//!
//! `{state_dir}/timeline-crash-queue/{key}.queue`, with `{key}.lock` held by this
//! process for its whole life (`x0k_activity_sink::segment`)
//!
//! A flat sequence of frames, each `[u32 LE length][postcard(TimelineRecord)]`,
//! each appended with a single write. Append-only while buffering; after a drain,
//! truncated to empty or atomically replaced by its undelivered remainder.
//! `{key}.quarantine` holds quarantined frames in the same format, capped at
//! `QueueLimits::quarantine_max_bytes`. A per-process key (pid and nonce) keeps
//! two processes from sharing one queue file, sandboxes with their own pid
//! namespace included (the shared-resource-two-writers failure mode).

use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, warn};
use x0k_activity_sink::segment::{self, encode_frame, index_frames, read_segment, SegmentPaths};
use x0k_timeline_record::record::TimelineRecord;

use super::auto_persist::{forward_records, try_connect_forwarder, ForwardOutcome, ForwarderConn};
use crate::timeline_channel::{record_timeline_drop, record_timeline_drop_for_record};

/// Directory holding this process's crash-queue files, adopted from its
/// [`super::TimelineEndpoint`] by the first forwarder or fallback to start.
static SEGMENT_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Adopt the process's segment directory. The first call wins: a process has
/// one owner key and one segment, so its directory does not change.
pub(crate) fn adopt_segment_dir(dir: &std::path::Path) {
    let _ = SEGMENT_DIR.set(dir.to_path_buf());
}

/// Directory holding per-process crash-queue files for this process.
fn crash_queue_dir() -> PathBuf {
    SEGMENT_DIR
        .get()
        .cloned()
        .expect("the forwarder or fallback adopts the segment directory before any queue use")
}

/// This process's segment owner key, minted once: its pid and a random nonce,
/// unique across the pid namespaces that share one segment directory.
fn owner_key() -> &'static str {
    static KEY: OnceLock<String> = OnceLock::new();
    KEY.get_or_init(segment::new_owner_key)
}

/// This process's segment files.
fn segment_paths() -> SegmentPaths {
    SegmentPaths::new(&crash_queue_dir(), owner_key())
}

/// This process's crash-queue file path.
#[cfg(test)]
fn crash_queue_path() -> PathBuf {
    segment_paths().queue
}

/// Take this process's owner lock, once, and hold it until the process exits.
/// It is taken before the queue file can exist, so the writer never mistakes a
/// live process's segment for an orphan (`segment` module docs).
fn hold_process_owner_lock() {
    static LOCK: OnceLock<Option<std::fs::File>> = OnceLock::new();
    LOCK.get_or_init(|| match segment::hold_owner_lock(&segment_paths().lock) {
        Ok(file) => Some(file),
        Err(e) => {
            warn!(error = %e, "could not take the segment owner lock; the writer may take this segment in early");
            None
        }
    });
}

/// This process's crash-queue file path in `segment_dir` (for the
/// fresh-injection oracle test to confirm records were buffered to disk before
/// drain).
#[doc(hidden)]
pub fn crash_queue_path_for_test(segment_dir: &std::path::Path) -> PathBuf {
    SegmentPaths::new(segment_dir, owner_key()).queue
}

/// Bounds for one process's crash queue.
#[derive(Clone, Copy, Debug)]
struct QueueLimits {
    /// Most bytes the queue file may hold. Past it, new records are refused.
    max_bytes: u64,
    /// Record bytes per drain request. A request always carries at least one
    /// record, so a record larger than this travels alone.
    chunk_bytes: usize,
    /// Records per drain request.
    chunk_records: usize,
    /// Most bytes the quarantine file may hold. Past it, quarantined records
    /// are counted and logged but not kept.
    quarantine_max_bytes: u64,
}

impl QueueLimits {
    /// 32 MiB holds ~20k records at the average size of the 2026-09-22 wedge
    /// (1.6 KB), far more than a daemon restart produces; a request of 2 MiB or
    /// 256 records stays a fifth of the frame cap and close to the live
    /// forwarder's 200-record batches.
    const DEFAULT: Self = Self {
        max_bytes: 32 << 20,
        chunk_bytes: 2 << 20,
        chunk_records: 256,
        quarantine_max_bytes: 8 << 20,
    };
}

/// Reconnect backoff while the daemon cannot be reached at all: a failed
/// connect costs one syscall, and a restarted daemon should be found quickly.
const RECONNECT_BACKOFF_START: Duration = Duration::from_millis(250);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Backoff after a drain that connected but could not finish: the sink is up
/// and refusing, or dropped the connection mid-drain. Retrying that every few
/// seconds forever is how the 2026-09-22 wedge churned 2.5 TB; each consecutive
/// stall doubles the wait, up to five minutes.
const STALLED_DRAIN_BACKOFF_START: Duration = Duration::from_secs(5);
const STALLED_DRAIN_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// Wait before the next drain attempt after `consecutive` stalled drains (≥ 1).
fn stalled_drain_delay(consecutive: u32) -> Duration {
    let doublings = consecutive.saturating_sub(1).min(16);
    STALLED_DRAIN_BACKOFF_START
        .saturating_mul(1u32 << doublings)
        .min(STALLED_DRAIN_BACKOFF_MAX)
}

/// Quarantined records logged individually per queue; the rest are counted.
const QUARANTINE_LOG_LIMIT: u32 = 8;

/// `system.timeline.records_dropped` stage for drops made by this queue.
const DROP_STAGE: &str = "crash_queue";

/// Synchronously append one `TimelineRecord` as a postcard frame to this
/// process's panic-queue file, fsync'd.
///
/// This is the columnar-free replacement for the old Arrow-IPC panic WAL: a
/// panicking process can't drain its async timeline pipeline before `abort()`,
/// so the record is written straight to disk here, in the panic thread, and
/// recovered by the next client's forwarder via `sweep_stray_panic_queues_on`.
/// Returns the file path on success.
///
/// `wal_dir` is the crash-queue directory; taking it explicitly lets the panic
/// test point at a tempdir instead of `~/.0k/`.
pub fn write_panic_record(
    queue_dir: &std::path::Path,
    record: &TimelineRecord,
) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(queue_dir)?;
    let path = queue_dir.join(format!("{}-panic.queue", owner_key()));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    let mut frame = encode_frame(record).map_err(|e| std::io::Error::other(e.to_string()))?;
    if file.metadata()?.len() == 0 {
        frame.splice(0..0, segment::header_frame());
    }
    file.write_all(&frame)?;
    file.sync_all()?;
    Ok(path)
}

/// Read every record back out of a panic-queue file (test/recovery helper).
#[doc(hidden)]
pub fn read_panic_records(path: &std::path::Path) -> anyhow::Result<Vec<TimelineRecord>> {
    read_segment(path)
}

/// Forward + remove any `*-panic.queue` files left behind by crashed processes.
///
/// The panic hook writes a `{key}-panic.queue` postcard frame in the crashing
/// thread; the process then aborts and never drains it. The next client's
/// forwarder calls this once it has a live daemon connection, so a panic row
/// reaches the parquet timeline (queryable as `system.panic`) just like the old
/// Arrow panic WAL — only the daemon owns the columnar encode now.
///
/// A panic file the sink refuses on its own merits (the sink takes an empty
/// request right after) is set aside as `…-panic.queue.rejected`, so it is kept
/// for inspection instead of being resent on every connect of every client.
pub(crate) async fn sweep_stray_panic_queues_on(conn: &mut ForwarderConn) {
    let dir = crash_queue_dir();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_panic = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("-panic.queue"))
            .unwrap_or(false);
        if !is_panic {
            continue;
        }
        let records = match read_segment(&path) {
            Ok(r) if !r.is_empty() => r,
            Ok(_) => {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            // A panic this build cannot read is still someone's crash record.
            Err(e) => {
                warn!(file = %path.display(), error = %e, "leaving a panic-queue this build cannot read");
                continue;
            }
        };
        let n = records.len();
        let refusal = match forward_records(conn, &records).await {
            ForwardOutcome::Delivered => {
                debug!(file = %path.display(), recovered = n, "recovered panic record(s) to daemon");
                let _ = std::fs::remove_file(&path);
                continue;
            }
            ForwardOutcome::TooLarge { bytes } => {
                format!("{bytes}-byte request exceeds the IPC frame cap")
            }
            ForwardOutcome::Rejected { reason } => {
                if !sink_takes_empty_request(conn).await {
                    debug!(file = %path.display(), "sink refuses every request; leaving panic-queue for retry");
                    continue;
                }
                reason
            }
            _ => {
                // Leave the file for a later sweep rather than lose the panic.
                debug!(file = %path.display(), "panic-queue forward failed; leaving for retry");
                continue;
            }
        };
        let mut aside = path.clone().into_os_string();
        aside.push(".rejected");
        warn!(
            file = %path.display(),
            records = n,
            reason = %refusal,
            "sink refused a stray panic-queue; set aside as .rejected"
        );
        let _ = std::fs::rename(&path, aside);
    }
}

/// Whether the sink answers an empty `IngestTimeline` request with success: the
/// probe that tells a record the sink refuses on its own merits apart from a
/// sink that refuses everything.
async fn sink_takes_empty_request(conn: &mut ForwarderConn) -> bool {
    matches!(forward_records(conn, &[]).await, ForwardOutcome::Delivered)
}

/// The whole frame — length prefix and body — whose body is `body`.
fn frame_bytes<'a>(buf: &'a [u8], body: &Range<usize>) -> &'a [u8] {
    &buf[body.start - 4..body.end]
}


/// Resolve `lazy_data` on activity records, mirroring the forwarder's pre-serialize
/// step so a buffered record is byte-identical to a live-forwarded one.
fn resolve_lazy_data(record: &mut TimelineRecord) {
    if let TimelineRecord::Activity(ref mut activity, _) = record {
        if activity.context.activity_data.is_none() {
            if let Some(ref lazy) = activity.context.lazy_data {
                activity.context.activity_data = lazy.to_json_string().ok();
            }
        }
        activity.context.lazy_data = None;
    }
}

/// What a quarantine log line says about a record: its kind, id and name —
/// never its content.
fn record_shape(record: &TimelineRecord) -> (&'static str, &str, &str) {
    match record {
        TimelineRecord::Activity(activity, _) => (
            "activity",
            &activity.context.activity_id.0,
            &activity.context.activity_type,
        ),
        TimelineRecord::Span(span) => ("span", &span.span_id, &span.span_name),
        TimelineRecord::LogEvent(event) => ("log", &event.id, &event.target),
        TimelineRecord::Conversation(entry) => ("conversation", &entry.id, &entry.entry_type),
    }
}

/// Why a drain stopped before the queue was empty.
#[derive(Debug, Clone, PartialEq)]
enum Stall {
    /// The connection failed mid-drain.
    Transport(String),
    /// The sink refused a lone record and then an empty request too: it is
    /// refusing everything, so nothing was quarantined.
    SinkRefused(String),
    /// The sink took only part of a request (its pipeline closed).
    SinkPartial { accepted: usize, sent: usize },
    /// Reading or rewriting the queue file failed.
    Io(String),
}

impl Stall {
    fn kind(&self) -> &'static str {
        match self {
            Stall::Transport(_) => "transport",
            Stall::SinkRefused(_) => "sink_refused",
            Stall::SinkPartial { .. } => "sink_partial",
            Stall::Io(_) => "io",
        }
    }
}

impl fmt::Display for Stall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stall::Transport(e) => write!(f, "connection failed: {e}"),
            Stall::SinkRefused(r) => write!(f, "sink refuses every request: {r}"),
            Stall::SinkPartial { accepted, sent } => {
                write!(f, "sink took {accepted} of {sent} records")
            }
            Stall::Io(e) => write!(f, "queue file: {e}"),
        }
    }
}

/// The result of one drain pass.
#[derive(Debug, Default)]
struct DrainReport {
    /// Records the sink accepted.
    delivered: usize,
    /// Records quarantined (refused on their own merits, too large for any
    /// frame, or undecodable).
    quarantined: usize,
    /// `IngestTimeline` requests sent, probes included.
    requests: usize,
    /// Records left on disk.
    remaining: usize,
    /// Why the pass stopped early; `None` when the queue was emptied.
    stall: Option<Stall>,
}

/// Counters kept across drain passes for one fallback episode.
#[derive(Debug, Default, Clone, Copy)]
struct QueueStats {
    /// Records refused because the queue was full.
    refused: u64,
    /// Records quarantined.
    quarantined: u64,
    /// Quarantined records logged individually (capped).
    quarantine_logged: u32,
    /// Write calls issued against the queue file: one per append, one per
    /// post-drain rewrite or truncation. The tests read it to pin that a drain
    /// never rewrites the queue record by record.
    queue_writes: u64,
    /// Whether the "queue full" warning has been logged this episode.
    full_warned: bool,
}

/// Handle to this process's crash-queue file, kept open for appends.
struct CrashQueue {
    path: PathBuf,
    quarantine_path: PathBuf,
    file: Option<std::fs::File>,
    /// Bytes in the queue file, tracked so an append can be bounded (and a torn
    /// one rolled back) without a stat.
    len: u64,
    limits: QueueLimits,
    stats: QueueStats,
}

impl CrashQueue {
    fn new() -> Self {
        hold_process_owner_lock();
        let paths = segment_paths();
        Self::at(paths.queue, paths.quarantine, QueueLimits::DEFAULT)
    }

    fn at(path: PathBuf, quarantine_path: PathBuf, limits: QueueLimits) -> Self {
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Self {
            path,
            quarantine_path,
            file: None,
            len,
            limits,
            stats: QueueStats::default(),
        }
    }

    /// Append-handle, lazily opened (and the directory created) on first write.
    fn writer(&mut self) -> anyhow::Result<&mut std::fs::File> {
        let file = match self.file.take() {
            Some(file) => file,
            None => {
                if let Some(dir) = self.path.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?
            }
        };
        Ok(self.file.insert(file))
    }

    /// Buffer one record to disk with a single write. Returns `false` when the
    /// queue is full and the record was refused (and counted as a drop).
    fn buffer(&mut self, record: &TimelineRecord) -> anyhow::Result<bool> {
        let mut frame = encode_frame(record)?;
        if self.len == 0 {
            frame.splice(0..0, segment::header_frame());
        }
        if self.len + frame.len() as u64 > self.limits.max_bytes {
            self.stats.refused += 1;
            record_timeline_drop_for_record(record, DROP_STAGE, "full");
            if !self.stats.full_warned {
                self.stats.full_warned = true;
                warn!(
                    path = %self.path.display(),
                    bytes = self.len,
                    max_bytes = self.limits.max_bytes,
                    "crash-queue full; refusing new records until it drains"
                );
            }
            return Ok(false);
        }
        let len_before = self.len;
        self.stats.queue_writes += 1;
        let file = self.writer()?;
        if let Err(e) = file.write_all(&frame) {
            // Roll a torn frame back so the next append starts on a frame boundary.
            let _ = file.set_len(len_before);
            return Err(e.into());
        }
        self.len += frame.len() as u64;
        Ok(true)
    }

    /// Move one record the sink cannot take out of the way: count it, keep its
    /// frame in the quarantine file while that has room, and log its shape
    /// (the first few individually).
    fn quarantine(&mut self, frame: &[u8], record: Option<&TimelineRecord>, reason: &str) {
        self.stats.quarantined += 1;
        match record {
            Some(record) => record_timeline_drop_for_record(record, DROP_STAGE, "quarantined"),
            None => record_timeline_drop("undecodable", DROP_STAGE, "quarantined", 1),
        }
        let kept = self.keep_quarantined(frame);
        if self.stats.quarantine_logged < QUARANTINE_LOG_LIMIT {
            self.stats.quarantine_logged += 1;
            let (kind, id, name) = record.map(record_shape).unwrap_or(("undecodable", "", ""));
            let kept_in = if kept {
                self.quarantine_path.display().to_string()
            } else {
                "nowhere (quarantine file full)".to_string()
            };
            warn!(
                kind,
                id,
                name,
                frame_bytes = frame.len(),
                reason,
                kept_in = %kept_in,
                "crash-queue quarantined a record the sink cannot take; skipping it"
            );
            if self.stats.quarantine_logged == QUARANTINE_LOG_LIMIT {
                warn!("crash-queue: further quarantined records are counted, not logged");
            }
        }
    }

    /// Append a quarantined frame to the quarantine file if it has room.
    fn keep_quarantined(&self, frame: &[u8]) -> bool {
        let used = std::fs::metadata(&self.quarantine_path)
            .map(|m| m.len())
            .unwrap_or(0);
        if used + frame.len() as u64 > self.limits.quarantine_max_bytes {
            return false;
        }
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.quarantine_path)
            .and_then(|mut f| f.write_all(frame))
            .is_ok()
    }

    /// Drain the on-disk queue to the sink over `conn`, in requests that fit
    /// the IPC frame cap, then remove what was resolved (see the module docs).
    async fn drain(&mut self, conn: &mut ForwarderConn) -> DrainReport {
        let mut report = DrainReport::default();
        let buf = match std::fs::read(&self.path) {
            Ok(buf) => buf,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.len = 0;
                return report;
            }
            Err(e) => {
                report.stall = Some(Stall::Io(e.to_string()));
                return report;
            }
        };
        self.len = buf.len() as u64;
        if buf.is_empty() {
            return report;
        }
        let (frames, intact_end) = index_frames(&buf);
        if intact_end < buf.len() {
            warn!(
                torn_bytes = buf.len() - intact_end,
                "crash-queue tail frame torn; dropping it"
            );
        }

        // Index of the first frame not yet delivered or quarantined.
        let mut next = 0usize;
        'chunks: while next < frames.len() {
            // The next request: a run of decodable frames within the budget. A
            // frame that does not decode is quarantined where it stands.
            let mut start = next;
            let mut records = Vec::new();
            let mut bytes = 0usize;
            let mut end = start;
            while end < frames.len() && records.len() < self.limits.chunk_records {
                let body = &buf[frames[end].clone()];
                if !records.is_empty() && bytes + body.len() > self.limits.chunk_bytes {
                    break;
                }
                match x0k_serialization::serialize::from_bytes_unchecked::<TimelineRecord>(body) {
                    Ok(record) => {
                        bytes += body.len();
                        records.push(record);
                        end += 1;
                    }
                    Err(e) if records.is_empty() => {
                        self.quarantine(
                            frame_bytes(&buf, &frames[end]),
                            None,
                            &format!("frame does not decode: {e}"),
                        );
                        report.quarantined += 1;
                        end += 1;
                        start = end;
                    }
                    Err(_) => break,
                }
            }
            next = start;
            if records.is_empty() {
                continue;
            }

            // Send it; a refused range is halved until the refused record is alone.
            let mut work = VecDeque::from([(0usize, records.len())]);
            while let Some((lo, hi)) = work.pop_front() {
                report.requests += 1;
                match forward_records(conn, &records[lo..hi]).await {
                    ForwardOutcome::Delivered => {
                        report.delivered += hi - lo;
                        next = start + hi;
                    }
                    ForwardOutcome::Rejected { .. } | ForwardOutcome::TooLarge { .. }
                        if hi - lo > 1 =>
                    {
                        let mid = lo + (hi - lo) / 2;
                        work.push_front((mid, hi));
                        work.push_front((lo, mid));
                    }
                    ForwardOutcome::TooLarge { bytes } => {
                        self.quarantine(
                            frame_bytes(&buf, &frames[start + lo]),
                            Some(&records[lo]),
                            &format!("{bytes}-byte request exceeds the IPC frame cap"),
                        );
                        report.quarantined += 1;
                        next = start + hi;
                    }
                    ForwardOutcome::Rejected { reason } => {
                        report.requests += 1;
                        if sink_takes_empty_request(conn).await {
                            self.quarantine(
                                frame_bytes(&buf, &frames[start + lo]),
                                Some(&records[lo]),
                                &format!("sink rejected it: {reason}"),
                            );
                            report.quarantined += 1;
                            next = start + hi;
                        } else {
                            report.stall = Some(Stall::SinkRefused(reason));
                            break 'chunks;
                        }
                    }
                    ForwardOutcome::Partial { accepted } => {
                        report.delivered += accepted;
                        next = start + lo + accepted;
                        report.stall = Some(Stall::SinkPartial {
                            accepted,
                            sent: hi - lo,
                        });
                        break 'chunks;
                    }
                    ForwardOutcome::Transport { error } => {
                        report.stall = Some(Stall::Transport(error));
                        break 'chunks;
                    }
                }
            }
        }

        if let Err(e) = self.remove_resolved(&buf, &frames, next, intact_end) {
            if report.stall.is_none() {
                report.stall = Some(Stall::Io(e.to_string()));
            }
        }
        report.remaining = frames.len().saturating_sub(next);
        report
    }

    /// Drop the resolved prefix `frames[..next]` (and any torn tail) from the
    /// queue file: truncate when nothing is left, otherwise replace the file
    /// with its remainder in one atomic rewrite. Writes nothing when the drain
    /// resolved nothing and there is no torn tail.
    fn remove_resolved(
        &mut self,
        buf: &[u8],
        frames: &[Range<usize>],
        next: usize,
        intact_end: usize,
    ) -> anyhow::Result<()> {
        if next >= frames.len() {
            return self.truncate_to(0);
        }
        if next == 0 {
            if intact_end < buf.len() {
                return self.truncate_to(intact_end as u64);
            }
            return Ok(());
        }
        let keep_from = frames[next].start - 4;
        self.rewrite(&[&segment::header_frame()[..], &buf[keep_from..intact_end]].concat())
    }

    /// Cut the queue file to `len` bytes, keeping the append handle.
    fn truncate_to(&mut self, len: u64) -> anyhow::Result<()> {
        self.stats.queue_writes += 1;
        self.writer()?.set_len(len)?;
        self.len = len;
        Ok(())
    }

    /// Replace the queue file with `remainder`: one write to a sibling, fsync,
    /// rename over the queue. A crash leaves either the old file or the new one.
    fn rewrite(&mut self, remainder: &[u8]) -> anyhow::Result<()> {
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".compact");
        let tmp = PathBuf::from(tmp);
        self.stats.queue_writes += 1;
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(remainder)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        // The append handle still points at the replaced inode.
        self.file = None;
        self.len = remainder.len() as u64;
        Ok(())
    }
}

/// Fallback loop: buffer to disk while the daemon is unreachable, drain on
/// reconnect. Entered with any `pending` records the live forwarder had in hand
/// when its connection died.
///
/// This is the columnar-free replacement for `run_auto_persist_timeline_task`.
/// It runs until the timeline channel closes (process shutdown), at which point
/// any records still on disk stay there for the daemon to take in once this
/// process's owner lock is released.
pub async fn run_crash_queue_fallback(
    mut receiver: mpsc::Receiver<TimelineRecord>,
    mut pending: Vec<TimelineRecord>,
    endpoint: super::TimelineEndpoint,
) {
    adopt_segment_dir(&endpoint.segment_dir);
    let socket_path = endpoint.socket_path;
    let mut queue = CrashQueue::new();

    debug!(
        path = %queue.path.display(),
        bytes = queue.len,
        "timeline crash-queue fallback active"
    );

    let mut reconnect_backoff = RECONNECT_BACKOFF_START;
    let mut stalled_drains = 0u32;
    let mut last_stall: Option<&'static str> = None;

    loop {
        // Persist the failed live batch before attempting another connection.
        // Recovery and forwarding share one loop so repeated failures retain
        // neither old queues nor nested async polling frames.
        for mut record in pending.drain(..) {
            resolve_lazy_data(&mut record);
            if let Err(e) = queue.buffer(&record) {
                warn!(error = %e, "failed to buffer pending timeline record to crash-queue");
            }
        }
        // Try to reconnect and drain, then resume live forwarding.
        let wait = match try_connect_forwarder(&socket_path).await {
            Ok(mut conn) => {
                reconnect_backoff = RECONNECT_BACKOFF_START;
                // Recover any panic rows crashed processes left behind before
                // draining our own buffered records.
                sweep_stray_panic_queues_on(&mut conn).await;
                let report = queue.drain(&mut conn).await;
                match report.stall {
                    None => {
                        debug!(
                            delivered = report.delivered,
                            quarantined = report.quarantined,
                            requests = report.requests,
                            "crash-queue drained; resuming live forwarding"
                        );
                        if queue.stats.refused > 0 {
                            warn!(
                                refused = queue.stats.refused,
                                "crash-queue refused records while full; they were dropped"
                            );
                        }
                        stalled_drains = 0;
                        last_stall = None;
                        match super::auto_persist::run_forwarding_loop_on(conn, receiver).await {
                            Ok(()) => {
                                debug!("timeline forwarder exited cleanly after crash-queue recovery");
                                return;
                            }
                            Err((err, remaining_receiver, undelivered)) => {
                                warn!(error = %err, preserved = undelivered.len(), "forwarding failed again; re-entering crash-queue");
                                receiver = remaining_receiver;
                                pending = undelivered;
                                continue;
                            }
                        }
                    }
                    Some(stall) => {
                        stalled_drains += 1;
                        let delay = stalled_drain_delay(stalled_drains);
                        // Warn when a stall starts or changes character; repeats
                        // go to debug — each warn is itself a record queued here.
                        if last_stall != Some(stall.kind()) {
                            warn!(
                                reason = %stall,
                                delivered = report.delivered,
                                quarantined = report.quarantined,
                                remaining = report.remaining,
                                retry_in_s = delay.as_secs(),
                                "crash-queue drain stalled; keeping the rest on disk and backing off"
                            );
                        } else {
                            debug!(
                                reason = %stall,
                                delivered = report.delivered,
                                remaining = report.remaining,
                                retry_in_s = delay.as_secs(),
                                stalled_drains,
                                "crash-queue drain still stalled"
                            );
                        }
                        last_stall = Some(stall.kind());
                        delay
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, "timeline writer still unreachable; buffering");
                let delay = reconnect_backoff;
                reconnect_backoff = (reconnect_backoff * 2).min(RECONNECT_BACKOFF_MAX);
                delay
            }
        };

        // While disconnected, keep buffering live records for up to `wait`.
        let deadline = tokio::time::sleep(wait);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                maybe = receiver.recv() => match maybe {
                    Some(mut record) => {
                        resolve_lazy_data(&mut record);
                        if let Err(e) = queue.buffer(&record) {
                            warn!(error = %e, "failed to buffer timeline record to crash-queue");
                        }
                    }
                    None => {
                        debug!(
                            bytes = queue.len,
                            "timeline channel closed while disconnected; crash-queue persists for recovery"
                        );
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::net::UnixListener;
    use x0k_ipc_transport::framing;
    use x0k_timeline_record::record::{
        IngestTimeline, IngestTimelineResult, LogEventData, INGEST_TIMELINE_METHOD,
    };

    fn test_log(id: &str) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData {
            id: id.to_string(),
            timestamp_us: 1,
            level: "info".to_string(),
            message: Some(id.to_string()),
            fields_json: None,
            source_file: None,
            source_line: None,
            module: None,
            target: "test".to_string(),
            software_module_id: None,
            workstream_id: None,
            intent_id: None,
            session_id: None,
            pid: None,
            trace_id: None,
            traceparent: None,
            tracestate: None,
            request_id: None,
            parent_request_id: None,
            role: None,
            mode: None,
            origin_system: None,
            origin_subsystem: None,
            agent_profile: None,
            sandbox_id: None,
            invocation_method: None,
            executed_by_json: None,
            authorization_chain_json: None,
            authorization_proof_refs_json: None,
            artifact_id: None,
            native_artifact_id: None,
            document_id: None,
            document_revision_id: None,
            file_path: None,
            event_type: Some("test.log".to_string()),
        })
    }

    /// A log record whose message is `bytes` long.
    fn big_log(id: &str, bytes: usize) -> TimelineRecord {
        let mut record = test_log(id);
        if let TimelineRecord::LogEvent(event) = &mut record {
            event.message = Some("x".repeat(bytes));
        }
        record
    }

    fn id_of(record: &TimelineRecord) -> String {
        record_shape(record).1.to_string()
    }

    fn ids(prefix: &str, range: Range<usize>) -> Vec<String> {
        range.map(|i| format!("{prefix}-{i:02}")).collect()
    }

    fn ids_on_disk(path: &Path) -> Vec<String> {
        read_segment(path).unwrap().iter().map(id_of).collect()
    }

    fn file_len(path: &Path) -> u64 {
        std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
    }

    fn queue_in(dir: &Path, limits: QueueLimits) -> CrashQueue {
        CrashQueue::at(dir.join("q.queue"), dir.join("q.quarantine"), limits)
    }

    /// Wire-compatible envelopes, mirroring auto_persist.rs / x0k-transport.
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

    #[derive(Clone, Copy)]
    enum SinkMode {
        /// Accept every request.
        AcceptAll,
        /// Refuse any request carrying a record whose id contains the marker,
        /// the way the timeline writer refuses a payload it cannot deserialize: the
        /// whole request, with an error reply.
        RejectMarked(&'static str),
        /// Refuse every request, the empty probe included, as a sink with no
        /// timeline pipeline (or one that does not know the method) does.
        RefuseAll,
        /// Accept whole requests until `n` records have arrived, then drop the
        /// connection on the next request without answering.
        HangUpAfter(usize),
        /// Allow recovery to drain, then reject the next live batch.
        RejectSecondRequest,
    }

    /// A timeline sink on a unix socket speaking the real `IngestTimeline` wire,
    /// with the real 10 MiB frame cap (`framing::recv` enforces it).
    struct TestSink {
        _dir: tempfile::TempDir,
        socket: String,
        received: Arc<Mutex<Vec<String>>>,
        requests: Arc<AtomicUsize>,
        largest_payload: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for TestSink {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl TestSink {
        async fn start(mode: SinkMode) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let socket = dir.path().join("sink.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let received = Arc::new(Mutex::new(Vec::<String>::new()));
            let requests = Arc::new(AtomicUsize::new(0));
            let largest_payload = Arc::new(AtomicUsize::new(0));
            let (got, count, largest) =
                (received.clone(), requests.clone(), largest_payload.clone());
            let task = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let (mut reader, mut writer) = stream.into_split();
                    let mut connection_requests = 0;
                    while let Ok(Some(req)) = framing::recv::<RequestEnvelope, _>(&mut reader).await
                    {
                        connection_requests += 1;
                        assert_eq!(req.method, INGEST_TIMELINE_METHOD);
                        count.fetch_add(1, Ordering::SeqCst);
                        largest.fetch_max(req.payload.len(), Ordering::SeqCst);
                        let decoded = x0k_serialization::serialize::from_bytes_unchecked::<
                            IngestTimeline,
                        >(&req.payload);
                        let outcome: Result<Vec<String>, String> = match decoded {
                            Err(e) => Err(format!("Deserialize error: {e}")),
                            Ok(ingest) => {
                                let batch: Vec<String> = ingest.records.iter().map(id_of).collect();
                                match mode {
                                    SinkMode::AcceptAll => Ok(batch),
                                    SinkMode::RejectMarked(marker)
                                        if batch.iter().any(|id| id.contains(marker)) =>
                                    {
                                        Err("Deserialize error: unknown variant".to_string())
                                    }
                                    SinkMode::RejectMarked(_) => Ok(batch),
                                    SinkMode::RefuseAll => {
                                        Err("Timeline sender not configured".to_string())
                                    }
                                    SinkMode::RejectSecondRequest if connection_requests == 2 => {
                                        Err("Transient live-batch refusal".to_string())
                                    }
                                    SinkMode::RejectSecondRequest => Ok(batch),
                                    SinkMode::HangUpAfter(n) => {
                                        if got.lock().unwrap().len() + batch.len() > n {
                                            break;
                                        }
                                        Ok(batch)
                                    }
                                }
                            }
                        };
                        let result = outcome.map(|batch| {
                            let accepted = batch.len();
                            got.lock().unwrap().extend(batch);
                            x0k_serialization::serialize::to_bytes_unchecked(
                                &IngestTimelineResult { accepted, discarded: 0 },
                            )
                            .unwrap()
                        });
                        let resp = ResponseEnvelope {
                            id: req.id,
                            result,
                            traceparent: None,
                        };
                        if framing::send(&mut writer, &resp).await.is_err() {
                            break;
                        }
                    }
                }
            });
            TestSink {
                socket: socket.to_string_lossy().into_owned(),
                _dir: dir,
                received,
                requests,
                largest_payload,
                task,
            }
        }

        async fn connect(&self) -> ForwarderConn {
            try_connect_forwarder(&self.socket).await.unwrap()
        }

        fn received(&self) -> Vec<String> {
            self.received.lock().unwrap().clone()
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn largest_payload(&self) -> usize {
            self.largest_payload.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn repeated_live_failures_recover_without_growing_the_poll_stack() {
        // Profile binding is process-wide. Isolate this replay from other
        // tests, and let the parent report even an aborting stack overflow.
        const CHILD: &str = "X0K_TEST_RECONNECT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let dir = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", concat!(module_path!(), "::repeated_live_failures_recover_without_growing_the_poll_stack").trim_start_matches("x0k_timeline_emit::"), "--nocapture"])
                .env(CHILD, "1")
                .env("X0K_TEST_SEGMENT_DIR", dir.path())
                .output().unwrap();
            assert!(output.status.success(), "recovery child failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        let sink = TestSink::start(SinkMode::RejectSecondRequest).await;
        let (tx, rx) = mpsc::channel(1);
        let endpoint = super::super::TimelineEndpoint {
            socket_path: sink.socket.clone(),
            segment_dir: std::env::var_os("X0K_TEST_SEGMENT_DIR").expect("set by the parent").into(),
        };
        let task = tokio::spawn(run_crash_queue_fallback(
            rx, vec![test_log("cycle-0")], endpoint,
        ));
        // One record at a time: each connection drains the preceding failure,
        // then refuses the next live record. Hundreds of bounces must not
        // retain hundreds of nested forwarding futures.
        tokio::time::timeout(Duration::from_secs(30), async {
            for i in 0..512 {
                while sink.received().len() <= i {
                    assert!(!task.is_finished(), "forwarder stopped at cycle {i}");
                    tokio::task::yield_now().await;
                }
                if i < 511 {
                    tx.send(test_log(&format!("cycle-{}", i + 1))).await.unwrap();
                }
            }
            drop(tx);
            task.await.unwrap();
        }).await.expect("recovery must make progress");
        assert_eq!(sink.received(), (0..512).map(|i| format!("cycle-{i}")).collect::<Vec<_>>());
        assert!(ids_on_disk(&crash_queue_path()).is_empty());
    }

    #[test]
    fn frames_round_trip_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rt.queue");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        for i in 0..5 {
            file.write_all(&encode_frame(&test_log(&format!("rec-{i}"))).unwrap())
                .unwrap();
        }
        file.flush().unwrap();

        let records = read_segment(&path).unwrap();
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            match record {
                TimelineRecord::LogEvent(evt) => assert_eq!(evt.id, format!("rec-{i}")),
                other => panic!("unexpected record: {other:?}"),
            }
        }
    }

    #[test]
    fn truncated_tail_frame_recovers_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trunc.queue");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&encode_frame(&test_log("intact")).unwrap())
            .unwrap();
        // Write a length header claiming more bytes than follow (simulated crash).
        file.write_all(&1000u32.to_le_bytes()).unwrap();
        file.write_all(b"short").unwrap();
        file.flush().unwrap();

        let records = read_segment(&path).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            TimelineRecord::LogEvent(evt) => assert_eq!(evt.id, "intact"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn read_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let records = read_segment(&dir.path().join("nope.queue")).unwrap();
        assert!(records.is_empty());
    }

    /// The 2026-09-22 wedge: a queue past the 10 MiB frame cap was sent as one
    /// request, which `send` refused before writing a byte, forever. It must
    /// drain in requests that fit.
    #[tokio::test]
    async fn queue_larger_than_the_ipc_frame_cap_drains_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = queue_in(dir.path(), QueueLimits::DEFAULT);
        let expected = ids("rec", 0..700);
        for id in &expected {
            assert!(queue.buffer(&big_log(id, 16 * 1024)).unwrap());
        }
        assert!(
            file_len(&queue.path) as usize > framing::MAX_MESSAGE_SIZE,
            "the queue must be past the frame cap to reproduce the wedge"
        );

        let sink = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = sink.connect().await;
        let report = queue.drain(&mut conn).await;

        assert_eq!(report.stall, None);
        assert_eq!((report.delivered, report.quarantined), (700, 0));
        assert_eq!(sink.received(), expected);
        assert!(sink.requests() >= 6, "{} requests", sink.requests());
        assert!(
            sink.largest_payload() <= QueueLimits::DEFAULT.chunk_bytes + 16,
            "largest request {} bytes",
            sink.largest_payload()
        );
        assert_eq!(file_len(&queue.path), 0, "a full drain empties the queue");
    }

    #[tokio::test]
    async fn record_too_large_for_any_frame_is_quarantined_without_sending() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = queue_in(dir.path(), QueueLimits::DEFAULT);
        queue.buffer(&test_log("before")).unwrap();
        queue
            .buffer(&big_log("huge", framing::MAX_MESSAGE_SIZE))
            .unwrap();
        queue.buffer(&test_log("after")).unwrap();

        let sink = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = sink.connect().await;
        let report = queue.drain(&mut conn).await;

        assert_eq!(report.stall, None);
        assert_eq!((report.delivered, report.quarantined), (2, 1));
        assert_eq!(sink.received(), vec!["before", "after"]);
        assert_eq!(sink.requests(), 2, "the oversized record is never sent");
        // It is larger than the 8 MiB quarantine cap too: counted, not kept.
        assert_eq!(queue.stats.quarantined, 1);
        assert!(!queue.quarantine_path.exists());
        assert_eq!(file_len(&queue.path), 0);
    }

    /// A record the sink refuses on its own merits is isolated, quarantined
    /// and skipped; everything around it is delivered, in order.
    #[tokio::test]
    async fn poison_record_is_quarantined_and_the_rest_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = queue_in(dir.path(), QueueLimits::DEFAULT);
        let mut all = ids("rec", 0..40);
        all[17] = "poison-17".to_string();
        for id in &all {
            queue.buffer(&test_log(id)).unwrap();
        }

        let sink = TestSink::start(SinkMode::RejectMarked("poison")).await;
        let mut conn = sink.connect().await;
        let report = queue.drain(&mut conn).await;

        assert_eq!(report.stall, None);
        assert_eq!((report.delivered, report.quarantined), (39, 1));
        let delivered: Vec<String> = all
            .iter()
            .filter(|id| *id != "poison-17")
            .cloned()
            .collect();
        assert_eq!(sink.received(), delivered);
        assert_eq!(ids_on_disk(&queue.quarantine_path), vec!["poison-17"]);
        assert_eq!(queue.stats.quarantined, 1);
        assert_eq!(file_len(&queue.path), 0);

        // New records behind it flow on the next pass; nothing is stuck.
        queue.buffer(&test_log("later")).unwrap();
        let report = queue.drain(&mut conn).await;
        assert_eq!((report.delivered, report.stall), (1, None));
    }

    /// A sink that refuses everything (the empty probe too) is not a reason to
    /// quarantine anything: the queue is kept byte for byte, and not rewritten.
    #[tokio::test]
    async fn sink_refusing_everything_keeps_the_queue_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = queue_in(dir.path(), QueueLimits::DEFAULT);
        for id in ids("rec", 0..30) {
            queue.buffer(&test_log(&id)).unwrap();
        }
        let bytes_before = std::fs::read(&queue.path).unwrap();
        let inode_before = std::fs::metadata(&queue.path).unwrap().ino();
        let writes_before = queue.stats.queue_writes;

        let sink = TestSink::start(SinkMode::RefuseAll).await;
        let mut conn = sink.connect().await;
        let report = queue.drain(&mut conn).await;

        assert_eq!(
            report.stall,
            Some(Stall::SinkRefused("Timeline sender not configured".into()))
        );
        assert_eq!(
            (report.delivered, report.quarantined, report.remaining),
            (0, 0, 30)
        );
        assert!(
            report.requests <= 8,
            "a refusing sink costs a handful of requests, not one per record ({})",
            report.requests
        );
        assert_eq!(std::fs::read(&queue.path).unwrap(), bytes_before);
        assert_eq!(std::fs::metadata(&queue.path).unwrap().ino(), inode_before);
        assert_eq!(queue.stats.queue_writes, writes_before, "no write at all");
        assert!(!queue.quarantine_path.exists());
    }

    /// A drain that stops part-way removes the delivered prefix with one
    /// rewrite of the remainder — never by re-appending it record by record —
    /// and a later reconnect drains the rest.
    #[tokio::test]
    async fn partial_drain_rewrites_the_remainder_once_and_reconnect_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let limits = QueueLimits {
            chunk_records: 10,
            ..QueueLimits::DEFAULT
        };
        let mut queue = queue_in(dir.path(), limits);
        for id in ids("rec", 0..50) {
            queue.buffer(&test_log(&id)).unwrap();
        }
        let bytes_before = std::fs::read(&queue.path).unwrap();
        let (frames, _) = index_frames(&bytes_before);
        let writes_before = queue.stats.queue_writes;

        let flaky = TestSink::start(SinkMode::HangUpAfter(20)).await;
        let mut conn = flaky.connect().await;
        let report = queue.drain(&mut conn).await;

        assert!(
            matches!(report.stall, Some(Stall::Transport(_))),
            "{report:?}"
        );
        assert_eq!((report.delivered, report.remaining), (20, 30));
        assert_eq!(flaky.received(), ids("rec", 0..20));
        assert_eq!(
            queue.stats.queue_writes - writes_before,
            1,
            "30 undelivered records, one rewrite"
        );
        assert_eq!(
            std::fs::read(&queue.path).unwrap(),
            [&segment::header_frame()[..], &bytes_before[frames[20].start - 4..]].concat(),
            "the remainder is the header and the original bytes from record 20 on"
        );

        // Reconnect to a healthy sink: the rest drains, the file is truncated.
        let healthy = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = healthy.connect().await;
        let report = queue.drain(&mut conn).await;
        assert_eq!((report.delivered, report.stall), (30, None));
        assert_eq!(healthy.received(), ids("rec", 20..50));
        assert_eq!(file_len(&queue.path), 0);
        assert_eq!(queue.stats.queue_writes - writes_before, 2);

        // The replaced file takes appends again from zero.
        queue.buffer(&test_log("later")).unwrap();
        assert_eq!(ids_on_disk(&queue.path), vec!["later"]);
    }

    /// The bound refuses new records (keeping the oldest), counts them, and
    /// costs no write once full.
    #[tokio::test]
    async fn full_queue_refuses_new_records_and_counts_them() {
        let dir = tempfile::tempdir().unwrap();
        let frame_len = encode_frame(&test_log("rec-00")).unwrap().len() as u64;
        let full = segment::header_frame().len() as u64 + frame_len * 10;
        let limits = QueueLimits {
            max_bytes: full,
            ..QueueLimits::DEFAULT
        };
        let mut queue = queue_in(dir.path(), limits);

        let stored: Vec<bool> = ids("rec", 0..25)
            .iter()
            .map(|id| queue.buffer(&test_log(id)).unwrap())
            .collect();

        assert_eq!(stored, [vec![true; 10], vec![false; 15]].concat());
        assert_eq!(file_len(&queue.path), full);
        assert_eq!(queue.stats.refused, 15);
        assert_eq!(
            queue.stats.queue_writes, 10,
            "a refused record costs no write"
        );
        assert_eq!(ids_on_disk(&queue.path), ids("rec", 0..10));

        // Once drained, the queue takes records again.
        let sink = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = sink.connect().await;
        assert_eq!(queue.drain(&mut conn).await.delivered, 10);
        assert!(queue.buffer(&test_log("rec-99")).unwrap());
    }

    #[tokio::test]
    async fn undecodable_frame_is_quarantined_and_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.queue");
        let garbage = [4u8, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];
        let mut bytes = encode_frame(&test_log("a")).unwrap();
        bytes.extend_from_slice(&garbage);
        bytes.extend(encode_frame(&test_log("b")).unwrap());
        std::fs::write(&path, &bytes).unwrap();
        let mut queue = queue_in(dir.path(), QueueLimits::DEFAULT);

        let sink = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = sink.connect().await;
        let report = queue.drain(&mut conn).await;

        assert_eq!(report.stall, None);
        assert_eq!((report.delivered, report.quarantined), (2, 1));
        assert_eq!(sink.received(), vec!["a", "b"]);
        assert_eq!(std::fs::read(&queue.quarantine_path).unwrap(), garbage);
        assert_eq!(file_len(&path), 0);
    }

    /// The live forwarder no longer treats a refused batch as delivered: it
    /// hands the batch back, so the crash-queue drain can isolate the refused
    /// record instead of all of them vanishing unseen.
    #[tokio::test]
    async fn live_forward_hands_a_rejected_batch_back() {
        let sink = TestSink::start(SinkMode::RejectMarked("poison")).await;
        let mut conn = sink.connect().await;
        let batch = vec![test_log("ok-1"), test_log("poison-2")];
        let unsent = super::super::auto_persist::forward_batch(&mut conn, batch)
            .await
            .unwrap_err();
        assert_eq!(
            unsent.iter().map(id_of).collect::<Vec<_>>(),
            vec!["ok-1", "poison-2"]
        );
    }

    #[test]
    fn stalled_drains_back_off_to_minutes() {
        let secs = |n| stalled_drain_delay(n).as_secs();
        assert_eq!(
            [1, 2, 3, 4, 5, 6, 7, 1000].map(secs),
            [5, 10, 20, 40, 80, 160, 300, 300]
        );
        assert!(stalled_drain_delay(1) >= RECONNECT_BACKOFF_MAX);
    }

    /// Replay a captured crash-queue file (e.g. a read-only copy of a live one)
    /// through the drain into a test sink.
    ///
    /// `X0K_CRASHQ_REPLAY=<file> [X0K_CRASHQ_REPLAY_REPEAT=<n>] cargo test -p
    /// x0k-env --lib replay_captured_queue_file -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "needs X0K_CRASHQ_REPLAY pointing at a captured queue file"]
    async fn replay_captured_queue_file() {
        let Ok(source) = std::env::var("X0K_CRASHQ_REPLAY") else {
            eprintln!("set X0K_CRASHQ_REPLAY to a captured .queue file");
            return;
        };
        let repeat: usize = std::env::var("X0K_CRASHQ_REPLAY_REPEAT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let captured = std::fs::read(&source).unwrap();
        let (frames, intact_end) = index_frames(&captured);
        let decodable = frames
            .iter()
            .filter(|body| {
                x0k_serialization::serialize::from_bytes_unchecked::<TimelineRecord>(
                    &captured[(*body).clone()],
                )
                .is_ok()
            })
            .count();
        let largest = frames.iter().map(|b| b.len()).max().unwrap_or(0);
        eprintln!(
            "captured: {} bytes, {} intact frames ({} decodable), torn tail {} bytes, largest record {} bytes",
            captured.len(),
            frames.len(),
            decodable,
            captured.len() - intact_end,
            largest
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay.queue");
        std::fs::write(&path, captured[..intact_end].repeat(repeat)).unwrap();
        let total_bytes = intact_end * repeat;
        eprintln!(
            "replaying x{repeat}: {} records, {} bytes; the pre-fix drain's single request would be ~{} bytes against a {}-byte frame cap ({})",
            frames.len() * repeat,
            total_bytes,
            total_bytes + 5,
            framing::MAX_MESSAGE_SIZE,
            if total_bytes + 5 > framing::MAX_MESSAGE_SIZE { "refused by send: the wedge" } else { "fits" }
        );

        let mut queue = CrashQueue::at(
            path.clone(),
            dir.path().join("replay.quarantine"),
            QueueLimits {
                max_bytes: u64::MAX,
                ..QueueLimits::DEFAULT
            },
        );
        let sink = TestSink::start(SinkMode::AcceptAll).await;
        let mut conn = sink.connect().await;
        let started = std::time::Instant::now();
        let report = queue.drain(&mut conn).await;
        eprintln!(
            "drain: {report:?} in {:?}; sink saw {} requests, largest {} bytes; queue writes {}",
            started.elapsed(),
            sink.requests(),
            sink.largest_payload(),
            queue.stats.queue_writes
        );

        assert_eq!(report.stall, None);
        assert_eq!(report.delivered, decodable * repeat);
        assert_eq!(report.quarantined, (frames.len() - decodable) * repeat);
        assert!(sink.largest_payload() < framing::MAX_MESSAGE_SIZE);
        assert_eq!(file_len(&path), 0);
    }
}
