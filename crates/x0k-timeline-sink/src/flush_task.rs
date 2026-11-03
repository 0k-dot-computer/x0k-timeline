//! Unified timeline flush task for daemon processes
//!
//! Accepts `TimelineRecord` (activities, spans, and log events) and writes them
//! to a single ParquetStore via a WAL + memtable architecture.
//!
//! Micro-batches (100 records / 25ms) are converted to Arrow RecordBatches,
//! appended to a WAL for crash safety, and accumulated in a memtable.
//! The memtable flushes to a single large parquet file when it hits 64MB or 60s.
//!
//! # Configuration
//!
//! - `X0K_SPAN_BATCH_SIZE`: Maximum records per micro-batch (default: 100)
//! - `X0K_SPAN_BATCH_TIMEOUT_MS`: Maximum wait for micro-batch (default: 25)
//! - `X0K_FLUSH_SIZE_MB`: Memtable size threshold in MB (default: 64)
//! - `X0K_FLUSH_INTERVAL_SECS`: Max time before parquet flush (default: 60)
//! - `X0K_TIMELINE_INGEST_LAG_WARN_SECS`: Pending-data warning age (default: 300)
//! - `X0K_TIMELINE_INGEST_LAG_WARN_INTERVAL_SECS`: Repeat cadence (default: 300)

use crate::pause::WriterPause;
use crate::config::{FlushConfig, SpanBatchConfig, TieredRetentionConfig};
use crate::memtable::Memtable;
use crate::routing::StorageTier;
use crate::types::TimelineRecord;
use crate::unified_schema;
use crate::unified_schema::{
    ENTRY_KIND_LOG, ENTRY_KIND_SPAN, RETENTION_CLASS_EVENT, RETENTION_CLASS_LOG,
    RETENTION_CLASS_RECORD, RETENTION_CLASS_SPAN,
};
use crate::wal::{self, WalWriter};
use crate::SessionContext;
use anyhow::{anyhow, Context as _, Result};
use arrow::array::{Array as _, RecordBatch, StringArray};
use arrow_select::concat::concat_batches;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};
use x0k_parquet::reader::ParquetReader;
use x0k_parquet::{CompactionOptions, ParquetStore};
use x0k_timeline_record::span::SpanData;

/// Callback invoked after each micro-batch with the batch contents.
/// Used by the SHM publisher to maintain a rolling snapshot.
pub type BatchCallback = Box<dyn FnMut(&[TimelineRecord]) + Send>;

const FLUSH_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Default)]
struct IngestLagState {
    last_warned: Option<Instant>,
    last_error: Option<String>,
}

impl IngestLagState {
    fn clear(&mut self) {
        self.last_warned = None;
        self.last_error = None;
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run the unified timeline flush task
///
/// Accepts both activities and spans via `TimelineRecord`. All records are
/// written to the single `store`; `StorageTier` is preserved as a column value.
///
/// Uses a WAL + memtable architecture: micro-batches are converted to Arrow
/// RecordBatches, appended to a per-process WAL, and accumulated in memory.
/// When the memtable hits the size or time threshold, all batches are
/// concatenated and written as a single large parquet file.
///
/// If `on_batch` is provided, it is called with each micro-batch after conversion.
/// The SHM publisher uses this to maintain a real-time snapshot.
///
/// With `segment_dir`, this writer takes in the local segments of processes
/// that are gone, at startup and every [`SEGMENT_INTAKE_INTERVAL`].
#[allow(clippy::too_many_arguments)]
pub async fn run_timeline_flush_task(
    receiver: mpsc::Receiver<TimelineRecord>,
    shutdown: Arc<Notify>,
    pause: Arc<WriterPause>,
    session_context: SessionContext,
    store: Arc<ParquetStore>,
    on_flush: Option<Arc<dyn Fn() + Send + Sync>>,
    on_batch: Option<BatchCallback>,
    segment_dir: Option<std::path::PathBuf>,
) {
    let flush_config = FlushConfig { segment_dir, ..FlushConfig::from_env() };
    run_timeline_writer_with_config(
        receiver, shutdown, pause, session_context, store, on_flush, on_batch, flush_config,
    ).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_timeline_writer_with_config(
    receiver: mpsc::Receiver<TimelineRecord>,
    shutdown: Arc<Notify>,
    pause: Arc<WriterPause>,
    session_context: SessionContext,
    store: Arc<ParquetStore>,
    on_flush: Option<Arc<dyn Fn() + Send + Sync>>,
    on_batch: Option<BatchCallback>,
    flush_config: FlushConfig,
) {
    // Own cancellation separately from the daemon-wide shutdown notification.
    // Dropping the async waiter must stop this writer, not another service.
    // Regression: cancelled_timeline_waiter_finishes_writer_shutdown.
    struct StopWriterOnDrop(Arc<Notify>);
    impl Drop for StopWriterOnDrop {
        fn drop(&mut self) { self.0.notify_one(); }
    }
    let writer_shutdown = Arc::new(Notify::new());
    let _stop_writer = StopWriterOnDrop(writer_shutdown.clone());
    let worker_shutdown = writer_shutdown.clone();
    let mut worker = tokio::task::spawn_blocking(move || {
        // Conversion, WAL writes/sync and callbacks remain serial on this
        // worker. Their existing micro-batch and durability cadence is unchanged.
        // Detached public callers may tear their runtime down before this
        // worker finishes. Keep timers and file I/O alive through its shutdown.
        // Regression: detached_timeline_writer_finishes_on_runtime_shutdown.
        // block_on drives the serial writer on this blocking thread. A separate
        // runtime worker keeps the lag monitor responsive during synchronous I/O.
        // Regression: public_writer_monitor_observes_synchronous_stall.
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1).enable_all().build() {
            Ok(runtime) => runtime,
            Err(error) => {
                error!(%error, "Failed to start timeline writer runtime");
                return;
            }
        };
        runtime.block_on(run_timeline_flush_task_with_config(
            receiver, worker_shutdown, pause, session_context, store,
            on_flush, on_batch, flush_config,
        ));
    });
    let result = tokio::select! {
        result = &mut worker => result,
        _ = shutdown.notified() => {
            writer_shutdown.notify_one();
            worker.await
        }
    };
    if let Err(error) = result {
        error!(%error, "Timeline writer worker failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_timeline_flush_task_with_config(
    mut receiver: mpsc::Receiver<TimelineRecord>,
    shutdown: Arc<Notify>,
    pause: Arc<WriterPause>,
    session_context: SessionContext,
    store: Arc<ParquetStore>,
    on_flush: Option<Arc<dyn Fn() + Send + Sync>>,
    mut on_batch: Option<BatchCallback>,
    flush_config: FlushConfig,
) {
    let batch_config = SpanBatchConfig::from_env();
    let batch_size = batch_config.batch_size;
    let retention = TieredRetentionConfig::from_config();
    let mut last_reap = Instant::now();
    let mut last_retention_watermark = None;

    let wal_dir = store.dir().join("wal");
    let my_pid = std::process::id();

    // --- Open WAL for this process ---
    let mut wal_writer = if flush_config.wal_enabled {
        match WalWriter::open(&wal_dir, my_pid) {
            Ok(w) => Some(w),
            Err(e) => {
                error!(error = %e, "Failed to open WAL, proceeding without crash safety");
                None
            }
        }
    } else {
        None
    };

    let mut memtable = Memtable::new(flush_config.flush_size_bytes, flush_config.flush_interval);
    let emission_generation = Arc::new(AtomicU64::new(0));
    let folded_generation = Arc::new(AtomicU64::new(0));
    let recovery = wal::recover_wal_files(&wal_dir, my_pid, &unified_schema::timeline_schema());
    let mut pending_recovered_wals = recovery.removable_files;
    if !recovery.batches.is_empty() {
        info!(
            batches = recovery.batches.len(),
            source_wals = pending_recovered_wals.len(),
            "Staging recovered WAL data for parquet"
        );
        for recovered in recovery.batches {
            if let Some(ref mut writer) = wal_writer {
                if let Err(error) = writer.append(&recovered) {
                    error!(%error, "Failed to copy recovered batch into current WAL");
                }
            }
            memtable.push(recovered);
            emission_generation.fetch_add(1, Ordering::Release);
        }
        if let Some(ref mut writer) = wal_writer {
            if let Err(error) = writer.sync() {
                error!(%error, "Failed to sync recovered batches into current WAL");
            }
        }
    }
    for skipped in recovery.skipped {
        process_micro_batch(
            &[wal_skip_record(&skipped)],
            &session_context,
            &mut wal_writer,
            &mut memtable,
            &mut on_batch,
            &emission_generation,
        );
    }
    let mut last_segment_intake = Instant::now();
    if let Some(dir) = &flush_config.segment_dir {
        take_in_orphan_segments(dir, &pause, |records| {
            process_micro_batch(
                records,
                &session_context,
                &mut wal_writer,
                &mut memtable,
                &mut on_batch,
                &emission_generation,
            )
        });
    }
    let _lag_monitor = AbortOnDrop(tokio::spawn(run_ingest_lag_monitor(
        wal_dir.clone(),
        my_pid,
        flush_config.clone(),
        session_context.clone(),
        emission_generation.clone(),
        folded_generation.clone(),
    )));

    info!(
        batch_size,
        batch_timeout_ms = batch_config.batch_timeout_ms,
        flush_size_mb = flush_config.flush_size_bytes / (1024 * 1024),
        flush_interval_secs = flush_config.flush_interval.as_secs(),
        span_retention_h = retention.span_max_age.as_secs() / 3600,
        log_retention_h = retention.log_max_age.as_secs() / 3600,
        event_retention_h = retention.event_max_age.as_secs() / 3600,
        ingest_lag_warn_secs = flush_config.ingest_lag_warn.as_secs(),
        ingest_lag_warn_interval_secs = flush_config.ingest_lag_warn_interval.as_secs(),
        wal_enabled = wal_writer.is_some(),
        "Timeline flush task ready (WAL + memtable)"
    );

    let mut batch: Vec<TimelineRecord> = Vec::with_capacity(batch_size);

    loop {
        if batch.is_empty() {
            // When memtable has data, we can't block indefinitely — we need to
            // wake up when the flush interval expires.
            if let Some(remaining) = memtable.time_until_flush() {
                tokio::select! {
                    _ = shutdown.notified() => {
                        flush_memtable_and_shutdown(
                            &mut memtable,
                            &mut wal_writer,
                            &store,
                            &on_flush,
                            &mut pending_recovered_wals,
                            &emission_generation,
                            &folded_generation,
                        ).await;
                        info!("Timeline flush task shutting down (no pending records)");
                        return;
                    }
                    _ = tokio::time::sleep(remaining) => {
                        // Memtable interval expired with no new records — flush it
                        if memtable.should_flush() {
                            debug!(batch_count = memtable.row_count(), "Flushing memtable on timer (idle)");
                            if let Err(error) = flush_memtable(
                                &mut memtable,
                                &mut wal_writer,
                                &store,
                                &on_flush,
                                &mut pending_recovered_wals,
                                &emission_generation,
                                &folded_generation,
                            ).await {
                                error!(%error, "Timeline flush retry scheduled");
                                tokio::time::sleep(FLUSH_RETRY_DELAY).await;
                            }
                        }
                        continue;
                    }
                    record = receiver.recv() => {
                        match record {
                            Some(r) => batch.push(r),
                            None => {
                                flush_memtable_and_shutdown(
                                    &mut memtable,
                                    &mut wal_writer,
                                    &store,
                                    &on_flush,
                                    &mut pending_recovered_wals,
                                    &emission_generation,
                                    &folded_generation,
                                ).await;
                                debug!("Timeline channel closed");
                                return;
                            }
                        }
                    }
                }
            } else {
                // Memtable is empty — safe to block indefinitely
                tokio::select! {
                    _ = shutdown.notified() => {
                        info!("Timeline flush task shutting down (no pending records)");
                        return;
                    }
                    record = receiver.recv() => {
                        match record {
                            Some(r) => batch.push(r),
                            None => {
                                debug!("Timeline channel closed");
                                return;
                            }
                        }
                    }
                }
            }
        }

        // Drain any additional records available without waiting.
        // Under low load each event forms a single-element batch processed instantly.
        // Under high load the channel naturally backfills during process_micro_batch()
        // execution (~1ms), providing efficient batching without any timer delay.
        loop {
            if batch.len() >= batch_size {
                break;
            }
            match receiver.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }

        batch.retain(|record| pause.keeps(record));

        // Process the micro-batch: convert to RecordBatches, WAL, memtable
        if !batch.is_empty() {
            process_micro_batch(
                &batch,
                &session_context,
                &mut wal_writer,
                &mut memtable,
                &mut on_batch,
                &emission_generation,
            );
            batch.clear();
        }

        // Check if memtable should flush
        if memtable.should_flush() {
            debug!(rows = memtable.row_count(), "Flushing memtable to parquet");
            if let Err(error) = flush_memtable(
                &mut memtable,
                &mut wal_writer,
                &store,
                &on_flush,
                &mut pending_recovered_wals,
                &emission_generation,
                &folded_generation,
            )
            .await
            {
                error!(%error, "Timeline flush retry scheduled");
                tokio::time::sleep(FLUSH_RETRY_DELAY).await;
            }
        }

        // A successful reap pass periodically leaves a permanent watermark,
        // including when it deletes zero files. Processed inline rather than
        // pushed onto `batch`, which is only drained when the next record
        // arrives and may never do so on an idle node.
        if let Some(watermark) = maybe_reap_tiered(
            &store,
            &retention,
            &mut last_reap,
            &mut last_retention_watermark,
        )
        .await
        {
            process_micro_batch(
                &[watermark],
                &session_context,
                &mut wal_writer,
                &mut memtable,
                &mut on_batch,
                &emission_generation,
            );
        }

        if let Some(dir) = &flush_config.segment_dir {
            if last_segment_intake.elapsed() >= SEGMENT_INTAKE_INTERVAL {
                last_segment_intake = Instant::now();
                take_in_orphan_segments(dir, &pause, |records| {
                    process_micro_batch(
                        records,
                        &session_context,
                        &mut wal_writer,
                        &mut memtable,
                        &mut on_batch,
                        &emission_generation,
                    )
                });
            }
        }
    }
}

/// How often the writer looks again for segments whose owner is gone.
pub const SEGMENT_INTAKE_INTERVAL: Duration = Duration::from_secs(300);

/// Records handed to `persist` at a time while taking in a segment.
const SEGMENT_INTAKE_CHUNK: usize = 256;

/// Take in the local segment of every process that is gone
/// (`observability-timeline` §4): read it, hand its records to `persist` (which
/// appends them to the WAL and syncs it), then delete it. A segment whose owner
/// still holds its lock is left to the owner. A pid-named segment with no lock
/// file, written before segments carried owner locks, is taken in once its pid
/// is not alive. Nothing is taken in while the writer is paused: a dead
/// process's segment waits on disk for the resume. Returns the records taken in.
/// Pinned by `segment_intake_tests`.
fn take_in_orphan_segments(
    dir: &std::path::Path,
    pause: &WriterPause,
    mut persist: impl FnMut(&[TimelineRecord]),
) -> usize {
    if pause.is_paused() {
        return 0;
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(e) => {
            warn!(dir = %dir.display(), error = %e, "cannot list the segment directory");
            return 0;
        }
    };
    let mut taken = 0;
    for entry in entries.flatten() {
        let queue = entry.path();
        let Some(key) = crate::segment::queue_owner_key(&queue) else { continue };
        let paths = crate::segment::SegmentPaths::new(dir, key);
        let claim = match crate::segment::claim_if_owner_gone(&paths.lock) {
            Ok(Some(lock)) => Some(lock),
            Ok(None) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if key.parse::<u32>().is_err() || wal::is_pid_alive(key) {
                    continue;
                }
                None
            }
            Err(e) => {
                warn!(file = %paths.lock.display(), error = %e, "cannot check a segment's owner");
                continue;
            }
        };
        let records = match crate::segment::read_segment(&queue) {
            Ok(records) => records,
            Err(e) => {
                warn!(file = %queue.display(), error = %e, "cannot read an orphaned segment; leaving it");
                continue;
            }
        };
        for chunk in records.chunks(SEGMENT_INTAKE_CHUNK) {
            persist(chunk);
        }
        taken += records.len();
        info!(file = %queue.display(), records = records.len(), "took in an orphaned segment");
        if let Err(e) = std::fs::remove_file(&queue) {
            warn!(file = %queue.display(), error = %e, "cannot remove a segment after taking it in");
            continue;
        }
        if claim.is_some() {
            let _ = std::fs::remove_file(&paths.lock);
        }
    }
    taken
}

/// Persist one memtable generation. Cleanup is strictly post-success: a write
/// error restores the same batches with their original age and leaves every
/// WAL copy in place.
async fn flush_memtable(
    memtable: &mut Memtable,
    wal_writer: &mut Option<WalWriter>,
    store: &Arc<ParquetStore>,
    on_flush: &Option<Arc<dyn Fn() + Send + Sync>>,
    pending_recovered_wals: &mut Vec<std::path::PathBuf>,
    emission_generation: &AtomicU64,
    folded_generation: &AtomicU64,
) -> Result<()> {
    if memtable.is_empty() {
        return Ok(());
    }

    let first_insert = memtable
        .first_insert()
        .expect("a non-empty memtable has an insertion time");
    let batches = memtable.take();
    if let Err(error) = flush_batches_to_parquet(&batches, store).await {
        // Incident regression: `failed_parquet_flush_retains_memtable_and_wal`.
        // The pre-fix path discarded this generation and truncated its WAL
        // even when one retention-prefix write failed.
        memtable.restore(batches, first_insert);
        error!(%error, "Timeline parquet flush failed; memtable and WAL retained");
        return Err(error);
    }

    if let Some(writer) = wal_writer {
        if let Err(error) = writer.truncate() {
            error!(%error, "Failed to truncate WAL after successful parquet flush");
        }
    }
    wal::remove_recovered_wal_files(pending_recovered_wals);
    pending_recovered_wals.clear();
    folded_generation.store(
        emission_generation.load(Ordering::Acquire),
        Ordering::Release,
    );
    if let Some(callback) = on_flush {
        callback();
    }
    Ok(())
}

async fn run_ingest_lag_monitor(
    wal_dir: std::path::PathBuf,
    pid: u32,
    config: FlushConfig,
    session_context: SessionContext,
    emission_generation: Arc<AtomicU64>,
    folded_generation: Arc<AtomicU64>,
) {
    let active_wal = wal_dir.join(format!("{pid}.wal"));
    let poll_interval = (config.ingest_lag_warn / 4)
        .min(Duration::from_secs(30))
        .max(Duration::from_millis(10));
    let mut observed_folded = folded_generation.load(Ordering::Acquire);
    let mut pending_since = None;
    let mut state = IngestLagState::default();
    let mut tripwire_wal = None;

    loop {
        tokio::time::sleep(poll_interval).await;
        let now = Instant::now();
        let current_emission = emission_generation.load(Ordering::Acquire);
        let current_folded = folded_generation.load(Ordering::Acquire);
        let has_unfolded_emission = current_emission > current_folded;
        let pending_bytes = std::fs::metadata(&active_wal)
            .map(|metadata| metadata.len())
            .unwrap_or_default();

        if current_folded != observed_folded {
            observed_folded = current_folded;
            pending_since = has_unfolded_emission.then_some(now);
            state.clear();
        } else if !has_unfolded_emission {
            pending_since = None;
            state.clear();
        } else if pending_since.is_none() {
            pending_since = Some(now);
        }

        state.last_error = Some("active WAL has not reached parquet".to_string());
        let Some(record) = ingest_lag_tripwire_if_due(
            pending_since,
            pending_bytes,
            current_emission,
            current_folded,
            &config,
            &mut state,
            now,
            now_micros(),
        ) else {
            continue;
        };

        let age_secs = pending_since
            .map(|started| now.saturating_duration_since(started).as_secs())
            .unwrap_or_default();
        warn!(
            "activity.type" = "obs.tripwire",
            event_type = "obs.tripwire",
            tripwire = "timeline.ingest.lag",
            guard = "timeline_ingest_lag",
            age_secs,
            threshold_secs = config.ingest_lag_warn.as_secs(),
            pending_bytes,
            emission_generation = current_emission,
            folded_generation = current_folded,
            linked_test = "stalled_folder_emits_ingest_lag_tripwire",
            "timeline.ingest.lag"
        );

        if tripwire_wal.is_none() {
            match WalWriter::open_with_stem(&wal_dir, &format!("{pid}-lag")) {
                Ok(writer) => tripwire_wal = Some(writer),
                Err(error) => {
                    error!(%error, "Failed to open independent ingest-lag WAL");
                    state.last_warned = None;
                    continue;
                }
            }
        }
        let batches = convert_timeline_batch(&[record], &session_context);
        if let Some(writer) = tripwire_wal.as_mut() {
            // Incident regression: `stalled_folder_emits_ingest_lag_tripwire`.
            // This WAL is independent of the folder loop being observed.
            for batch in batches {
                if let Err(error) = writer.append(&batch) {
                    error!(%error, "Failed to append ingest-lag tripwire WAL");
                    state.last_warned = None;
                }
            }
            if let Err(error) = writer.sync() {
                error!(%error, "Failed to sync ingest-lag tripwire WAL");
                state.last_warned = None;
            }
        }
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[allow(clippy::too_many_arguments)]
fn ingest_lag_tripwire_if_due(
    first_insert: Option<Instant>,
    pending_bytes: u64,
    emission_generation: u64,
    folded_generation: u64,
    config: &FlushConfig,
    state: &mut IngestLagState,
    now: Instant,
    now_micros: u64,
) -> Option<TimelineRecord> {
    let age = now.saturating_duration_since(first_insert?);
    if age < config.ingest_lag_warn {
        return None;
    }
    let warning_interval = config.ingest_lag_warn_interval.max(Duration::from_secs(1));
    if state
        .last_warned
        .is_some_and(|last| now.saturating_duration_since(last) < warning_interval)
    {
        return None;
    }
    state.last_warned = Some(now);

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tripwire".to_string(), "timeline.ingest.lag".to_string());
    metadata.insert("guard".to_string(), "timeline_ingest_lag".to_string());
    metadata.insert("age_secs".to_string(), age.as_secs().to_string());
    metadata.insert(
        "threshold_secs".to_string(),
        config.ingest_lag_warn.as_secs().to_string(),
    );
    metadata.insert("pending_bytes".to_string(), pending_bytes.to_string());
    metadata.insert(
        "emission_generation".to_string(),
        emission_generation.to_string(),
    );
    metadata.insert(
        "folded_generation".to_string(),
        folded_generation.to_string(),
    );
    metadata.insert(
        "warning_interval_secs".to_string(),
        warning_interval.as_secs().to_string(),
    );
    metadata.insert(
        "linked_test".to_string(),
        "stalled_folder_emits_ingest_lag_tripwire".to_string(),
    );
    if let Some(error) = &state.last_error {
        metadata.insert("error".to_string(), error.clone());
    }

    let context = crate::types::ActivityContext {
        activity_id: x0k_timeline_record::ActivityId::new(),
        activity_type: "obs.tripwire".to_string(),
        started_at: now_micros,
        metadata,
        activity_data: Some("timeline.ingest.lag".to_string()),
        source_module: Some(module_path!().to_string()),
        span_name: Some("timeline_ingest_lag".to_string()),
        ..Default::default()
    };
    Some(TimelineRecord::Activity(
        crate::types::CompletedActivity {
            context,
            ended_at: now_micros,
            duration_ms: 0,
        },
        StorageTier::Record,
    ))
}

fn wal_skip_record(report: &wal::WalSkipReport) -> TimelineRecord {
    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("tripwire".to_string(), "timeline.wal.skipped".to_string());
    metadata.insert("wal.path".to_string(), report.path.display().to_string());
    metadata.insert("wal.reason".to_string(), report.reason.clone());
    metadata.insert(
        "wal.recovered_batches".to_string(),
        report.recovered_batches.to_string(),
    );
    if let Some(path) = &report.quarantined_path {
        metadata.insert(
            "wal.quarantined_path".to_string(),
            path.display().to_string(),
        );
    }
    metadata.insert(
        "linked_test".to_string(),
        "foreign_or_corrupt_wal_is_reported_and_later_good_wal_recovers".to_string(),
    );
    let context = crate::types::ActivityContext {
        activity_id: x0k_timeline_record::ActivityId::new(),
        activity_type: "obs.tripwire".to_string(),
        started_at: now_micros,
        metadata,
        activity_data: Some("timeline.wal.skipped".to_string()),
        source_module: Some(module_path!().to_string()),
        span_name: Some("timeline_wal_skipped".to_string()),
        ..Default::default()
    };
    TimelineRecord::Activity(
        crate::types::CompletedActivity {
            context,
            ended_at: now_micros,
            duration_ms: 0,
        },
        StorageTier::Record,
    )
}

/// Process a micro-batch: convert to RecordBatches, append to WAL, push to memtable.
fn process_micro_batch(
    batch: &[TimelineRecord],
    session_context: &SessionContext,
    wal_writer: &mut Option<WalWriter>,
    memtable: &mut Memtable,
    on_batch: &mut Option<BatchCallback>,
    emission_generation: &AtomicU64,
) {
    let record_batches = convert_timeline_batch(batch, session_context);
    if !record_batches.is_empty() {
        emission_generation.fetch_add(1, Ordering::Release);
    }

    for rb in &record_batches {
        // Append to WAL
        if let Some(ref mut w) = wal_writer {
            if let Err(e) = w.append(rb) {
                error!(error = %e, "WAL append failed");
            }
        }
        // Push to memtable
        memtable.push(rb.clone());
    }

    // Periodic WAL sync (every micro-batch)
    if let Some(ref mut w) = wal_writer {
        if let Err(e) = w.sync() {
            error!(error = %e, "WAL sync failed");
        }
    }

    if let Some(ref mut cb) = on_batch {
        cb(batch);
    }
}

/// Flush remaining memtable data and clean up WAL on shutdown.
async fn flush_memtable_and_shutdown(
    memtable: &mut Memtable,
    wal_writer: &mut Option<WalWriter>,
    store: &Arc<ParquetStore>,
    on_flush: &Option<Arc<dyn Fn() + Send + Sync>>,
    pending_recovered_wals: &mut Vec<std::path::PathBuf>,
    emission_generation: &AtomicU64,
    folded_generation: &AtomicU64,
) {
    if !memtable.is_empty() {
        debug!(rows = memtable.row_count(), "Flushing memtable on shutdown");
        if let Err(error) = flush_memtable(
            memtable,
            wal_writer,
            store,
            on_flush,
            pending_recovered_wals,
            emission_generation,
            folded_generation,
        )
        .await
        {
            error!(%error, "Parquet flush failed on shutdown; WAL retained");
        }
    }
}

/// Convert a micro-batch of TimelineRecords into Arrow RecordBatches.
///
/// Partitions records by kind (spans, central activities, ephemeral activities,
/// log events) and converts each partition to a RecordBatch using the unified
/// schema.
fn convert_timeline_batch(
    batch: &[TimelineRecord],
    session_context: &SessionContext,
) -> Vec<RecordBatch> {
    let mut spans: Vec<&SpanData> = Vec::new();
    let mut central_activities: Vec<crate::types::CompletedActivity> = Vec::new();
    let mut ephemeral_activities: Vec<crate::types::CompletedActivity> = Vec::new();
    let mut record_activities: Vec<crate::types::CompletedActivity> = Vec::new();
    let mut log_events: Vec<&crate::types::LogEventData> = Vec::new();
    let mut conversation_entries: Vec<&crate::types::ConversationMeta> = Vec::new();

    for record in batch {
        match record {
            TimelineRecord::Span(span) => {
                spans.push(span);
            }
            TimelineRecord::Activity(activity, tier) => {
                let mut activity = activity.clone();
                if activity.context.activity_data.is_none() {
                    if let Some(ref lazy) = activity.context.lazy_data {
                        activity.context.activity_data = lazy.to_json_string().ok();
                    }
                }
                activity.context.lazy_data = None;
                if activity.context.correlation_id.is_none() {
                    activity.context.correlation_id =
                        activity.context.metadata.get("correlation_id").cloned();
                }
                if activity.context.span_id.is_none() {
                    activity.context.span_id = activity.context.metadata.get("span_id").cloned();
                }
                if activity.context.parent_span_id.is_none() {
                    activity.context.parent_span_id =
                        activity.context.metadata.get("parent_span_id").cloned();
                }
                match tier {
                    StorageTier::Central => central_activities.push(activity),
                    StorageTier::Ephemeral => ephemeral_activities.push(activity),
                    StorageTier::Record => record_activities.push(activity),
                }
            }
            TimelineRecord::LogEvent(evt) => {
                log_events.push(evt);
            }
            TimelineRecord::Conversation(meta) => {
                conversation_entries.push(meta);
            }
        }
    }

    let mut result = Vec::new();

    if !spans.is_empty() {
        let spans_owned: Vec<SpanData> = spans.into_iter().cloned().collect();
        match unified_schema::span_data_to_batch(&spans_owned) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert spans to RecordBatch"),
        }
    }

    if !central_activities.is_empty() {
        match unified_schema::activity_to_batch(
            &central_activities,
            StorageTier::Central,
            session_context,
        ) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert central activities to RecordBatch"),
        }
    }

    if !ephemeral_activities.is_empty() {
        match unified_schema::activity_to_batch(
            &ephemeral_activities,
            StorageTier::Ephemeral,
            session_context,
        ) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert ephemeral activities to RecordBatch"),
        }
    }

    if !record_activities.is_empty() {
        match unified_schema::activity_to_batch(
            &record_activities,
            StorageTier::Record,
            session_context,
        ) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert record activities to RecordBatch"),
        }
    }

    // Retention deletes by file and a file takes its class from its first row,
    // so a batch holds one class.
    let (kept_as_events, logs): (Vec<_>, Vec<_>) = log_events
        .into_iter()
        .cloned()
        .partition(|event| event.retention_class() == crate::routing::RetentionClass::Event);
    for events in [logs, kept_as_events] {
        if events.is_empty() {
            continue;
        }
        match unified_schema::log_event_to_batch(&events) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert log events to RecordBatch"),
        }
    }

    if !conversation_entries.is_empty() {
        let entries: Vec<crate::types::ConversationMeta> =
            conversation_entries.into_iter().cloned().collect();
        match unified_schema::conversation_to_batch(&entries) {
            Ok(b) => result.push(b),
            Err(e) => error!(error = %e, "Failed to convert conversation entries to RecordBatch"),
        }
    }

    result
}

/// Determine the filename prefix for a RecordBatch based on its retention class.
///
/// Each batch produced by `convert_timeline_batch` is homogeneous — all rows
/// share the same entry_kind. We check the first row.
fn entry_kind_prefix(batch: &RecordBatch) -> &'static str {
    if let Some(col) = batch.column_by_name("retention_class") {
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            if arr.len() > 0 {
                match arr.value(0) {
                    RETENTION_CLASS_SPAN => return "span_",
                    RETENTION_CLASS_LOG => return "log_",
                    RETENTION_CLASS_RECORD => return "record_",
                    RETENTION_CLASS_EVENT => return "event_",
                    _ => {}
                }
            }
        }
    }

    // Legacy fallback for batches produced before the retention_class column
    // existed.
    if let Some(col) = batch.column_by_name("entry_kind") {
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            if arr.len() > 0 {
                let kind = arr.value(0);
                if kind == ENTRY_KIND_SPAN {
                    return "span_";
                } else if kind == ENTRY_KIND_LOG {
                    return "log_";
                }
                return "event_";
            }
        }
    }

    "event_"
}

/// Group RecordBatches by retention prefix and write each group as a single
/// parquet file with the appropriate filename prefix.
async fn flush_batches_to_parquet(
    batches: &[RecordBatch],
    store: &Arc<ParquetStore>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    // Classify batches by prefix
    let mut span_batches: Vec<&RecordBatch> = Vec::new();
    let mut log_batches: Vec<&RecordBatch> = Vec::new();
    let mut event_batches: Vec<&RecordBatch> = Vec::new();
    let mut record_batches: Vec<&RecordBatch> = Vec::new();

    for batch in batches {
        match entry_kind_prefix(batch) {
            "span_" => span_batches.push(batch),
            "log_" => log_batches.push(batch),
            "record_" => record_batches.push(batch),
            _ => event_batches.push(batch),
        }
    }

    let mut failures = Vec::new();
    for (prefix, prefixed) in [
        ("span_", &span_batches),
        ("log_", &log_batches),
        ("event_", &event_batches),
        ("record_", &record_batches),
    ] {
        if let Err(error) = flush_prefixed(prefixed, store, prefix).await {
            failures.push(format!("{prefix}: {error:#}"));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join("; ")))
    }
}

/// Concatenate batches and write with a filename prefix.
async fn flush_prefixed(
    batches: &[&RecordBatch],
    store: &Arc<ParquetStore>,
    prefix: &str,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let owned: Vec<RecordBatch> = batches.iter().map(|b| (*b).clone()).collect();
    let concatenated = concat_batches(&schema, &owned)
        .with_context(|| format!("failed to concatenate {prefix} RecordBatches"))?;

    write_single_batch(&concatenated, store, prefix).await
}

/// Write a single RecordBatch to the store with a filename prefix.
async fn write_single_batch(
    batch: &RecordBatch,
    store: &Arc<ParquetStore>,
    prefix: &str,
) -> Result<()> {
    let ipc_bytes = ParquetReader::to_ipc_bytes(std::slice::from_ref(batch))
        .context("failed to serialize RecordBatch to IPC")?;

    let props = timeline_writer_props();
    let result = store
        .append_batch_with_prefix(&ipc_bytes, props, prefix)
        .await
        .with_context(|| format!("failed to write {prefix} parquet file"))?;
    debug!(
        rows = result.rows_written,
        file = %result.filename,
        "Flushed batches to parquet"
    );
    Ok(())
}

/// Interval between retention reap attempts
const REAP_INTERVAL: Duration = Duration::from_secs(60);

/// Successful no-op passes advance the durable watermark at this cadence.
/// Deletion passes always emit immediately.
const RETENTION_WATERMARK_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Safety margin for orphaned file cleanup — files must be at least this old
/// before they're considered orphaned (avoids racing with in-progress writes).
const ORPHAN_MIN_AGE: Duration = Duration::from_secs(3600);

/// Attempt to reap old Parquet files using tiered retention cutoffs.
///
/// Uses `ParquetStore::retain_tiered` which acquires a cross-process flock
/// with retry-backoff (up to 5s). After successful retention, also cleans up
/// orphaned files not tracked in the manifest.
///
/// Returns a `timeline.retention.watermark` record after every deletion pass
/// and at least hourly after successful no-op passes. Reaping used to be
/// announced with `info!` — a message row reaped on the very clock it was
/// announcing, so an empty historical window could not be distinguished from
/// a window in which nothing happened. The watermark is `Record`-tier: the one
/// row class the reaper never touches.
async fn maybe_reap_tiered(
    store: &ParquetStore,
    config: &TieredRetentionConfig,
    last_reap: &mut Instant,
    last_watermark: &mut Option<Instant>,
) -> Option<TimelineRecord> {
    if last_reap.elapsed() < REAP_INTERVAL {
        return None;
    }
    *last_reap = Instant::now();

    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let cutoffs = config.tier_cutoffs(now_micros);

    let start = Instant::now();
    let mut watermark = None;
    match store.retain_tiered(&cutoffs).await {
        Ok(n) => {
            if n == 0 {
                debug!(
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Retention pass completed (no files reaped)"
                );
            } else {
                info!(
                    deleted = n,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "Reaped old parquet files (tiered)"
                );
            }

            // Incident regression:
            // `retention_watermark_makes_empty_windows_readable`.
            let watermark_due = n > 0
                || last_watermark.is_none_or(|last| {
                    last.elapsed() >= RETENTION_WATERMARK_INTERVAL
                });
            if watermark_due {
                *last_watermark = Some(Instant::now());
                watermark = Some(retention_watermark_record(n, &cutoffs, now_micros));
            }
        }
        Err(e) => {
            warn!(error = %e, elapsed_ms = start.elapsed().as_millis() as u64, "Tiered retention reap failed");
            return None;
        }
    }

    // Clean up orphaned files not tracked in the manifest (e.g. from crashes)
    match store.cleanup_orphaned_files(ORPHAN_MIN_AGE).await {
        Ok(0) => {}
        Ok(n) => info!(deleted = n, "Cleaned up orphaned parquet files"),
        Err(e) => warn!(error = %e, "Orphaned file cleanup failed"),
    }

    match store.compact_once(CompactionOptions::default()).await {
        Ok(result) if result.input_files > 0 => info!(
            input_files = result.input_files,
            output_files = result.output_files,
            rows = result.rows_written,
            "Compacted small timeline parquet files"
        ),
        Ok(_) => {}
        Err(e) => warn!(error = %e, "Timeline parquet compaction failed"),
    }

    watermark
}

/// Build the permanent record of how far a successful pass reaped each tier.
///
/// `SELECT * FROM timeline WHERE event_type =
/// 'timeline.retention.watermark'` answers whether an empty query window is
/// below a tier's retention floor or was observed while still retained.
fn retention_watermark_record(
    deleted: usize,
    cutoffs: &[(String, u64)],
    now_micros: u64,
) -> TimelineRecord {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("retention_files_deleted".to_string(), deleted.to_string());
    metadata.insert("retention_pass_succeeded".to_string(), "true".to_string());
    for (prefix, cutoff) in cutoffs {
        // The legacy no-prefix cutoff has no tier to name.
        if prefix.is_empty() {
            continue;
        }
        let tier = prefix.trim_end_matches('_');
        metadata.insert(
            format!("retention_reaped_through_us_{tier}"),
            cutoff.to_string(),
        );
    }

    let context = crate::types::ActivityContext {
        activity_id: x0k_timeline_record::ActivityId::new(),
        activity_type: "timeline.retention.watermark".to_string(),
        started_at: now_micros,
        metadata,
        activity_data: Some(format!(
            "Retention pass succeeded; reaped {deleted} parquet file(s)"
        )),
        source_module: Some(module_path!().to_string()),
        span_name: Some("timeline_retention_watermark".to_string()),
        ..Default::default()
    };

    TimelineRecord::Activity(
        crate::types::CompletedActivity {
            context,
            ended_at: now_micros,
            duration_ms: 0,
        },
        StorageTier::Record,
    )
}

/// Writer properties for timeline Parquet files
fn timeline_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_column_bloom_filter_enabled(ColumnPath::from("id"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Mutex;
    use std::time::Duration;
    use tracing::field::{Field as TracingField, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{Layer, Registry};

    #[derive(Clone, Debug)]
    struct CapturedTracingEvent {
        level: Level,
        fields: HashMap<String, String>,
    }

    #[derive(Clone)]
    struct TracingCapture {
        events: Arc<Mutex<Vec<CapturedTracingEvent>>>,
        owner: std::thread::ThreadId,
    }

    impl TracingCapture {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                owner: std::thread::current().id(),
            }
        }

        fn snapshot(&self) -> Vec<CapturedTracingEvent> {
            self.events.lock().unwrap().clone()
        }

        async fn wait_for_tripwire(&self, timeout: Duration) -> CapturedTracingEvent {
            tokio::time::timeout(timeout, async {
                loop {
                    if let Some(event) = self.snapshot().into_iter().find(|event| {
                        event.fields.get("tripwire").map(String::as_str)
                            == Some("timeline.ingest.lag")
                    }) {
                        return event;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("stalled folder should emit timeline.ingest.lag through tracing")
        }
    }

    #[derive(Default)]
    struct FieldCapture(HashMap<String, String>);

    impl Visit for FieldCapture {
        fn record_u64(&mut self, field: &TracingField, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_i64(&mut self, field: &TracingField, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_bool(&mut self, field: &TracingField, value: bool) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_str(&mut self, field: &TracingField, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl<S> Layer<S> for TracingCapture
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            if std::thread::current().id() != self.owner {
                return;
            }
            let mut fields = FieldCapture::default();
            event.record(&mut fields);
            self.events.lock().unwrap().push(CapturedTracingEvent {
                level: *event.metadata().level(),
                fields: fields.0,
            });
        }
    }

    fn activity_record(event_type: &str, timestamp_us: u64) -> TimelineRecord {
        let context = crate::types::ActivityContext {
            activity_id: x0k_timeline_record::ActivityId::new(),
            activity_type: event_type.to_string(),
            started_at: timestamp_us,
            activity_data: Some(event_type.to_string()),
            ..Default::default()
        };
        TimelineRecord::Activity(
            crate::types::CompletedActivity {
                context,
                ended_at: timestamp_us,
                duration_ms: 0,
            },
            StorageTier::Central,
        )
    }

    // The controller is outside the single executor worker and always releases
    // and joins it before asserting, including the deliberately failing baseline.
    fn synchronous_writer_phase_fixture(cancel: bool, teardown: bool, monitor: bool) {
        let tmp = tempfile::TempDir::new().unwrap();
        let timeline_dir = tmp.path().join("timeline");
        let store = Arc::new(
            x0k_parquet::ParquetStore::with_table_name(&timeline_dir, "timeline").unwrap(),
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1).enable_all().build().unwrap();
        let (tx, rx) = mpsc::channel(4);
        let shutdown = Arc::new(Notify::new());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        struct WriterExited(std::sync::mpsc::Sender<()>);
        impl Drop for WriterExited {
            fn drop(&mut self) { let _ = self.0.send(()); }
        }
        let (exited_tx, exited_rx) = std::sync::mpsc::channel();
        let exit_witness = WriterExited(exited_tx);
        let callback: BatchCallback = Box::new(move |_| {
            let _keep_until_writer_exit = &exit_witness;
            let _ = entered_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        });
        let observed_store = store.clone();
        let mut config = FlushConfig::from_env();
        config.ingest_lag_warn = Duration::from_secs(1);
        config.ingest_lag_warn_interval = Duration::from_secs(60);
        let task = runtime.spawn(run_timeline_writer_with_config(
            rx, shutdown.clone(), Arc::new(WriterPause::default()),
            SessionContext::from_env(), store, None, Some(callback), config,
        ));
        tx.blocking_send(activity_record("sync.fixture.writer", now_micros())).unwrap();
        let entered = entered_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        let (beat_tx, beat_rx) = std::sync::mpsc::channel();
        let heartbeat = runtime.spawn(async move { let _ = beat_tx.send(()); });
        let responsive = beat_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let lag_wal = timeline_dir.join("wal")
            .join(format!("{}-lag.wal", std::process::id()));
        let observed_lag = !monitor || {
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let has_tripwire = wal::read_wal_file(&lag_wal).ok()
                    .is_some_and(|batches| batches.iter().any(|batch| batch.num_rows() > 0));
                if has_tripwire || Instant::now() >= deadline { break has_tripwire; }
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let (task_cancelled, exited) = if teardown {
            drop(task); // Match callers that detach the public writer handle.
            runtime.block_on(heartbeat).unwrap();
            // Force parent drivers to stop while the writer is still held.
            runtime.shutdown_timeout(Duration::from_millis(100));
            let _ = release_tx.send(());
            let exited = exited_rx.recv_timeout(Duration::from_secs(5)).is_ok();
            (false, exited)
        } else {
            if cancel { task.abort(); } else { shutdown.notify_one(); }
            let _ = release_tx.send(());
            let task_result = runtime.block_on(task);
            runtime.block_on(heartbeat).unwrap();
            let exited = exited_rx.recv_timeout(Duration::from_secs(5)).is_ok();
            drop(tx);
            // Join detached blocking work before assertions or scratch removal.
            drop(runtime);
            (task_result.is_err(), exited)
        };
        if !exited {
            let _preserved_scratch = tmp.keep();
            panic!("writer did not finish during bounded shutdown; scratch preserved");
        }
        let reader_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().unwrap();
        let ipc = reader_runtime.block_on(observed_store.query(
            "SELECT event_type FROM timeline WHERE event_type = 'sync.fixture.writer'",
        ));
        let rows = ipc.and_then(|bytes| ParquetReader::from_ipc_bytes(&bytes))
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>());
        drop(reader_runtime);
        assert!(observed_lag, "lag monitor did not observe blocked synchronous writer");
        assert!(entered, "writer did not enter controlled synchronous phase");
        assert!(responsive, "synchronous timeline work blocked an unrelated executor task");
        assert_eq!(task_cancelled, cancel);
        assert_eq!(rows.unwrap(), 1, "writer did not persist the exact fixture record");
    }

    #[test]
    fn synchronous_timeline_writer_does_not_block_executor() {
        synchronous_writer_phase_fixture(false, false, false);
    }

    #[test]
    fn cancelled_timeline_waiter_finishes_writer_shutdown() {
        synchronous_writer_phase_fixture(true, false, false);
    }

    #[test]
    fn detached_timeline_writer_finishes_on_runtime_shutdown() {
        synchronous_writer_phase_fixture(false, true, false);
    }

    #[test]
    fn public_writer_monitor_observes_synchronous_stall() {
        synchronous_writer_phase_fixture(false, false, true);
    }

    #[tokio::test]
    async fn test_default_db_path() {
        let path = crate::config::SinkConfig::default_db_path().unwrap();
        assert!(path.to_string_lossy().contains("activities"));
    }

    #[tokio::test]
    async fn test_timeline_flush_task_shutdown() {
        let (tx, rx) = mpsc::channel::<TimelineRecord>(10);
        let shutdown = Arc::new(Notify::new());
        let pause = Arc::new(WriterPause::default());
        let session_ctx = SessionContext::from_env();

        let tmp = tempfile::TempDir::new().unwrap();
        let timeline_dir = tmp.path().join("timeline");
        std::fs::create_dir_all(&timeline_dir).unwrap();
        let store = Arc::new(
            x0k_parquet::ParquetStore::with_table_name(&timeline_dir, "timeline").unwrap(),
        );

        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_timeline_flush_task(rx, shutdown_clone, pause, session_ctx, store, None, None, None)
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.notify_one();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("Task should complete")
            .expect("Task should not panic");

        drop(tx);
    }

    #[tokio::test]
    async fn good_wals_fold_while_foreign_and_corrupt_wals_are_reported() {
        let tmp = tempfile::TempDir::new().unwrap();
        let timeline_dir = tmp.path().join("timeline");
        fs::create_dir_all(&timeline_dir).unwrap();
        let store = Arc::new(
            x0k_parquet::ParquetStore::with_table_name(&timeline_dir, "timeline").unwrap(),
        );
        let wal_dir = timeline_dir.join("wal");
        let session = SessionContext::from_env();

        for (stem, event_type, timestamp) in [
            ("01-good", "sync.fixture.first", 1_800_000_000_000_000),
            ("04-good", "sync.fixture.later", 1_800_000_000_000_001),
        ] {
            let batches =
                convert_timeline_batch(&[activity_record(event_type, timestamp)], &session);
            let mut writer = WalWriter::open_with_stem(&wal_dir, stem).unwrap();
            writer.append(&batches[0]).unwrap();
            writer.sync().unwrap();
        }

        let foreign_schema = Arc::new(Schema::new(vec![Field::new(
            "foreign",
            DataType::Utf8,
            false,
        )]));
        let foreign_batch = RecordBatch::try_new(
            foreign_schema,
            vec![Arc::new(StringArray::from(vec!["not-timeline"]))],
        )
        .unwrap();
        let mut foreign = WalWriter::open_with_stem(&wal_dir, "02-foreign").unwrap();
        foreign.append(&foreign_batch).unwrap();
        foreign.sync().unwrap();
        drop(foreign);
        fs::write(wal_dir.join("03-corrupt.wal"), b"not a framed Arrow WAL").unwrap();

        let recovery = wal::recover_wal_files(
            &wal_dir,
            std::process::id(),
            &unified_schema::timeline_schema(),
        );
        assert_eq!(recovery.batches.len(), 2);
        assert_eq!(recovery.skipped.len(), 2);

        flush_batches_to_parquet(&recovery.batches, &store)
            .await
            .unwrap();
        wal::remove_recovered_wal_files(&recovery.removable_files);

        let ipc = store
            .query("SELECT event_type FROM timeline WHERE event_type LIKE 'sync.fixture.%'")
            .await
            .unwrap();
        let folded = ParquetReader::from_ipc_bytes(&ipc).unwrap();
        assert_eq!(folded.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(!wal_dir.join("01-good.wal").exists());
        assert!(!wal_dir.join("04-good.wal").exists());
    }

    #[tokio::test]
    async fn failed_parquet_flush_retains_memtable_and_wal() {
        let tmp = tempfile::TempDir::new().unwrap();
        let timeline_dir = tmp.path().join("timeline");
        fs::create_dir_all(&timeline_dir).unwrap();
        let store = Arc::new(
            x0k_parquet::ParquetStore::with_table_name(&timeline_dir, "timeline").unwrap(),
        );
        let session = SessionContext::from_env();
        let batches = convert_timeline_batch(
            &[activity_record(
                "sync.fixture.pending",
                1_800_000_000_000_000,
            )],
            &session,
        );
        let mut memtable = Memtable::new(1, Duration::ZERO);
        let mut wal_writer = Some(WalWriter::open(&timeline_dir.join("wal"), 424_242).unwrap());
        wal_writer.as_mut().unwrap().append(&batches[0]).unwrap();
        wal_writer.as_mut().unwrap().sync().unwrap();
        let wal_bytes = wal_writer.as_ref().unwrap().bytes_written();
        memtable.push(batches[0].clone());

        let displaced = tmp.path().join("timeline-displaced");
        fs::rename(&timeline_dir, &displaced).unwrap();
        fs::write(&timeline_dir, b"blocks the original store directory").unwrap();

        let mut pending_recovered = Vec::new();
        let emission_generation = AtomicU64::new(1);
        let folded_generation = AtomicU64::new(0);
        let result = flush_memtable(
            &mut memtable,
            &mut wal_writer,
            &store,
            &None,
            &mut pending_recovered,
            &emission_generation,
            &folded_generation,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(memtable.row_count(), 1, "failed generation stays pending");
        assert_eq!(
            wal_writer.as_ref().unwrap().bytes_written(),
            wal_bytes,
            "WAL is not truncated until parquet succeeds"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_folder_emits_ingest_lag_tripwire() {
        let tmp = tempfile::TempDir::new().unwrap();
        let timeline_dir = tmp.path().join("timeline");
        fs::create_dir_all(&timeline_dir).unwrap();
        let store = Arc::new(
            x0k_parquet::ParquetStore::with_table_name(&timeline_dir, "timeline").unwrap(),
        );
        let wal_path = timeline_dir
            .join("wal")
            .join(format!("{}.wal", std::process::id()));
        let session = SessionContext::from_env();
        let config = FlushConfig {
            flush_size_bytes: 1,
            flush_interval: Duration::from_secs(60),
            wal_enabled: true,
            ingest_lag_warn: Duration::from_secs(1),
            ingest_lag_warn_interval: Duration::from_secs(3600),
            segment_dir: None,
        };

        let capture = TracingCapture::new();
        let subscriber = Registry::default().with(capture.clone());
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other x0k-timeline-sink test installs a global tracing subscriber");

        let (tx, rx) = mpsc::channel(1);
        let shutdown = Arc::new(Notify::new());
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(run_timeline_flush_task_with_config(
            rx,
            task_shutdown,
            Arc::new(WriterPause::default()),
            session,
            store,
            None,
            None,
            config,
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if wal_path.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("flush task should open its scratch WAL");

        let displaced = tmp.path().join("timeline-displaced");
        fs::rename(&timeline_dir, &displaced).unwrap();
        fs::write(&timeline_dir, b"blocks the live parquet folder path").unwrap();
        tx.send(activity_record("sync.fixture.pending", now_micros()))
            .await
            .unwrap();

        // The monitor warns about a second after the stall; the claim is the
        // warning and its fields, so the wait leaves room for a loaded host.
        let tripwire = capture.wait_for_tripwire(Duration::from_secs(30)).await;
        assert_eq!(tripwire.level, Level::WARN);
        for (field, expected) in [
            ("activity.type", "obs.tripwire"),
            ("event_type", "obs.tripwire"),
            ("tripwire", "timeline.ingest.lag"),
            ("guard", "timeline_ingest_lag"),
            ("threshold_secs", "1"),
            ("emission_generation", "1"),
            ("folded_generation", "0"),
            (
                "linked_test",
                "stalled_folder_emits_ingest_lag_tripwire",
            ),
        ] {
            assert_eq!(tripwire.fields.get(field).map(String::as_str), Some(expected));
        }
        assert!(
            tripwire
                .fields
                .get("age_secs")
                .and_then(|age| age.parse::<u64>().ok())
                .is_some_and(|age| age >= 1),
            "tripwire must report the observed stall age"
        );
        assert!(
            tripwire.fields.contains_key("pending_bytes"),
            "tripwire must expose pending WAL bytes at the observation surface"
        );

        let observed = capture.snapshot();
        let flush_failure = observed.iter().position(|event| {
            event.level == Level::ERROR
                && event
                    .fields
                    .get("message")
                    .is_some_and(|message| message.contains("Timeline parquet flush failed"))
        });
        let warning = observed.iter().position(|event| {
            event.fields.get("tripwire").map(String::as_str)
                == Some("timeline.ingest.lag")
        });
        assert!(
            flush_failure.zip(warning).is_some_and(|(failure, warning)| failure < warning),
            "the real parquet write failure must precede the lag WARN"
        );

        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("flush task should honor shutdown after the stalled write")
            .expect("flush task should not panic");
    }

    /// Incident regression: `retention_watermark_makes_empty_windows_readable`.
    /// The watermark has to outlive every tier it reports on, including when a
    /// successful pass deleted no files.
    #[tokio::test]
    async fn retention_watermark_makes_empty_windows_readable() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(ParquetStore::with_table_name(tmp.path(), "timeline").unwrap());
        let mut last_reap = Instant::now() - REAP_INTERVAL - Duration::from_millis(1);
        let mut last_watermark = None;

        let watermark = maybe_reap_tiered(
            &store,
            &TieredRetentionConfig::default(),
            &mut last_reap,
            &mut last_watermark,
        )
        .await
        .expect("the first successful no-op pass must emit a watermark");
        let TimelineRecord::Activity(activity, tier) = &watermark
        else {
            panic!("retention watermark should be an activity record");
        };

        assert!(last_watermark.is_some());
        assert_eq!(
            *tier,
            StorageTier::Record,
            "the one tier reaping never sees"
        );
        assert_eq!(
            activity.context.activity_type,
            "timeline.retention.watermark"
        );
        assert_eq!(
            activity
                .context
                .metadata
                .get("retention_files_deleted")
                .map(String::as_str),
            Some("0")
        );
        for tier in ["span", "log", "event"] {
            assert!(
                activity
                    .context
                    .metadata
                    .contains_key(&format!("retention_reaped_through_us_{tier}")),
                "watermark names how far {tier} rows were reaped"
            );
        }
        // The legacy no-prefix cutoff names no tier and must not become a key.
        assert!(!activity
            .context
            .metadata
            .contains_key("retention_reaped_through_us_"));
        assert!(!activity.context.activity_id.0.is_empty());

        let batches = convert_timeline_batch(&[watermark], &SessionContext::from_env());
        flush_batches_to_parquet(&batches, &store).await.unwrap();
        let ipc = store
            .query(
                "SELECT event_type FROM timeline \
                 WHERE event_type = 'timeline.retention.watermark' \
                 AND metadata_json LIKE '%retention_reaped_through_us_log%'",
            )
            .await
            .unwrap();
        let queried = ParquetReader::from_ipc_bytes(&ipc).unwrap();
        assert_eq!(
            queried.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "the durable query surface must expose the retention watermark"
        );
    }
}

#[cfg(test)]
mod segment_intake_tests {
    use super::*;
    use crate::segment::{encode_frame, hold_owner_lock, SegmentPaths};
    use x0k_timeline_record::record::LogEventData;

    /// Retention deletes by file, so a column saying `event` is not enough: an
    /// observation sharing a `log_` file would still go with the logs.
    #[test]
    fn a_process_observation_is_filed_as_an_event_beside_ordinary_logs() {
        let observed = TimelineRecord::LogEvent(crate::types::LogEventData {
            id: "observed".to_string(),
            timestamp_us: 1,
            event_type: Some("process.exit".to_string()),
            ..Default::default()
        });
        let batches =
            convert_timeline_batch(&[log("plain"), observed], &SessionContext::from_env());
        let mut prefixes: Vec<_> = batches.iter().map(entry_kind_prefix).collect();
        prefixes.sort();
        assert_eq!(prefixes, ["event_", "log_"]);
    }

    fn log(id: &str) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData { id: id.into(), ..Default::default() })
    }

    fn write_segment(queue: &std::path::Path, ids: &[&str]) {
        let bytes: Vec<u8> = ids.iter().flat_map(|id| encode_frame(&log(id)).unwrap()).collect();
        std::fs::write(queue, bytes).unwrap();
    }

    fn intake(dir: &std::path::Path, recording: bool) -> Vec<TimelineRecord> {
        let pause = WriterPause::default();
        if !recording {
            pause.begin("test".to_string(), 0);
        }
        let mut persisted = Vec::new();
        take_in_orphan_segments(dir, &pause, |records| {
            persisted.extend_from_slice(records)
        });
        persisted
    }

    /// A pid that was alive and is now gone: a child that has exited and been reaped.
    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    #[test]
    fn a_segment_whose_owner_released_its_lock_is_taken_in_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SegmentPaths::new(dir.path(), "41-00ab");
        drop(hold_owner_lock(&paths.lock).unwrap());
        write_segment(&paths.queue, &["a", "b"]);

        assert_eq!(intake(dir.path(), true).len(), 2);
        assert!(!paths.queue.exists());
        assert!(!paths.lock.exists());
    }

    #[test]
    fn a_segment_whose_owner_holds_its_lock_is_left_to_the_owner() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SegmentPaths::new(dir.path(), "42-00ab");
        let _held = hold_owner_lock(&paths.lock).unwrap();
        write_segment(&paths.queue, &["a"]);

        assert!(intake(dir.path(), true).is_empty());
        assert!(paths.queue.exists());
    }

    #[test]
    fn nothing_is_taken_in_while_recording_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SegmentPaths::new(dir.path(), "43-00ab");
        drop(hold_owner_lock(&paths.lock).unwrap());
        write_segment(&paths.queue, &["a"]);

        assert!(intake(dir.path(), false).is_empty());
        assert!(paths.queue.exists());
    }

    #[test]
    fn a_pid_named_segment_without_a_lock_is_taken_in_once_its_pid_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join(format!("{}.queue", std::process::id()));
        let gone = dir.path().join(format!("{}.queue", dead_pid()));
        write_segment(&live, &["live"]);
        write_segment(&gone, &["gone"]);

        let persisted = intake(dir.path(), true);
        assert_eq!(persisted.len(), 1);
        assert!(live.exists());
        assert!(!gone.exists());
    }
}
