//! Write-ahead log for timeline records
//!
//! WAL format: length-prefixed Arrow IPC frames.
//! ```text
//! [u32 LE: payload length][Arrow IPC bytes]
//! [u32 LE: payload length][Arrow IPC bytes]
//! ...
//! ```
//!
//! Per-process WAL files at `{timeline_dir}/wal/{pid}.wal`.
//! On startup, orphaned WAL files from crashed processes are recovered.

use crate::unified_schema;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};
use x0k_parquet::reader::ParquetReader;

/// WAL writer — appends length-framed Arrow IPC entries to a file.
pub struct WalWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    bytes_written: u64,
}

/// Maximum WAL size before we log errors (256 MB safety cap)
const WAL_SIZE_LIMIT: u64 = 256 * 1024 * 1024;

impl WalWriter {
    /// Open or create a WAL file for this process.
    pub fn open(wal_dir: &Path, pid: u32) -> io::Result<Self> {
        Self::open_with_stem(wal_dir, &pid.to_string())
    }

    /// Open or create a WAL file with an explicit filename stem.
    ///
    /// The resulting file is `{wal_dir}/{stem}.wal`. Use this when the standard
    /// `{pid}.wal` scheme is wrong — for example, the panic hook writes to
    /// `{pid}-panic.wal` so the file is recovered by `recover_wal_files` even
    /// while the panicking PID is briefly still alive (the non-numeric stem
    /// makes `is_pid_alive` short-circuit to "dead").
    pub fn open_with_stem(wal_dir: &Path, file_stem: &str) -> io::Result<Self> {
        fs::create_dir_all(wal_dir)?;
        let path = wal_dir.join(format!("{file_stem}.wal"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes_written = file.metadata()?.len();
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            bytes_written,
        })
    }

    /// Append a RecordBatch to the WAL as a length-framed IPC entry.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let ipc_bytes = ParquetReader::to_ipc_bytes(std::slice::from_ref(batch))
            .map_err(io::Error::other)?;

        let len = ipc_bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&ipc_bytes)?;
        self.bytes_written += 4 + ipc_bytes.len() as u64;

        if self.bytes_written > WAL_SIZE_LIMIT {
            error!(
                bytes = self.bytes_written,
                limit = WAL_SIZE_LIMIT,
                "WAL exceeds size limit — parquet flushes may be failing"
            );
        }

        Ok(())
    }

    /// Flush buffered data and fsync to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()
    }

    /// Truncate the WAL (after a successful parquet flush).
    pub fn truncate(&mut self) -> io::Result<()> {
        // Re-open the file truncated
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.bytes_written = 0;
        Ok(())
    }

    /// Current WAL size in bytes.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Read all RecordBatches from a WAL file.
///
/// An unparseable trailing entry is treated as end-of-log, so batches from the
/// valid prefix are still returned.
pub fn read_wal_file(path: &Path) -> io::Result<Vec<RecordBatch>> {
    Ok(read_wal_file_with_status(path)?.batches)
}

struct WalReadOutcome {
    batches: Vec<RecordBatch>,
    tail_failure: Option<WalTailFailure>,
}

struct WalTailFailure {
    offset: usize,
    reason: String,
}

/// One WAL rejected or only partially recovered without stopping the scan.
#[derive(Debug, Clone)]
pub struct WalSkipReport {
    pub path: PathBuf,
    pub quarantined_path: Option<PathBuf>,
    pub recovered_batches: usize,
    pub reason: String,
}

/// Orphaned WAL data staged for a durable parquet flush.
///
/// `removable_files` are deliberately retained until the caller confirms that
/// every recovered batch reached parquet.
#[derive(Debug, Default)]
pub struct WalRecovery {
    pub batches: Vec<RecordBatch>,
    pub removable_files: Vec<PathBuf>,
    pub skipped: Vec<WalSkipReport>,
}

fn read_wal_file_with_status(path: &Path) -> io::Result<WalReadOutcome> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok(WalReadOutcome {
            batches: Vec::new(),
            tail_failure: None,
        });
    }

    let mut data = Vec::with_capacity(file_len as usize);
    file.read_to_end(&mut data)?;

    let mut batches = Vec::new();
    let mut pos = 0usize;

    let tail_failure = loop {
        if pos == data.len() {
            break None;
        }

        let frame_offset = pos;
        if data.len() - pos < 4 {
            break Some(WalTailFailure {
                offset: frame_offset,
                reason: format!(
                    "truncated length prefix: expected 4 bytes, available {}",
                    data.len() - pos
                ),
            });
        }

        let len =
            u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        let Some(frame_end) = pos.checked_add(len) else {
            break Some(WalTailFailure {
                offset: frame_offset,
                reason: format!("frame length {len} overflows address space"),
            });
        };

        if frame_end > data.len() {
            break Some(WalTailFailure {
                offset: frame_offset,
                reason: format!(
                    "truncated frame: expected {len} payload bytes, available {}",
                    data.len() - pos
                ),
            });
        }

        let ipc_bytes = &data[pos..frame_end];
        // Arrow panics on corrupt buffer bounds instead of returning an error.
        // `wal_tail_garbage_recovers_prefix_and_quarantines` pins this boundary.
        match catch_unwind(AssertUnwindSafe(|| {
            ParquetReader::from_ipc_bytes(ipc_bytes)
        })) {
            Ok(Ok(decoded)) => batches.extend(decoded),
            Ok(Err(e)) => {
                break Some(WalTailFailure {
                    offset: frame_offset,
                    reason: format!("invalid Arrow IPC payload: {e}"),
                });
            }
            Err(_) => {
                break Some(WalTailFailure {
                    offset: frame_offset,
                    reason: "Arrow IPC decoder panicked".to_string(),
                });
            }
        }
        pos = frame_end;
    };

    if let Some(failure) = &tail_failure {
        warn!(
            path = %path.display(),
            offset = failure.offset,
            reason = %failure.reason,
            "WAL has unparseable tail; treating it as end of log"
        );
    }

    Ok(WalReadOutcome {
        batches,
        tail_failure,
    })
}

/// Read all WAL files in the directory, returning their RecordBatches.
///
/// Unlike [`recover_wal_files`], this:
/// - Reads ALL `.wal` files (including the current process's active WAL)
/// - Does NOT delete any files
/// - Returns an empty vec if the directory doesn't exist
pub fn read_all_wal_files(wal_dir: &Path) -> Vec<RecordBatch> {
    let entries = match fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };

    let mut batches = Vec::new();

    let expected_schema = unified_schema::timeline_schema();
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "wal" {
            continue;
        }

        match read_wal_file(&path) {
            Ok(file_batches) => {
                for batch in file_batches {
                    if batch.schema().as_ref() == &expected_schema {
                        batches.push(batch);
                    } else {
                        // Incident regression:
                        // `foreign_or_corrupt_wal_is_reported_and_later_good_wal_recovers`.
                        warn!(
                            "activity.type" = "obs.tripwire",
                            event_type = "obs.tripwire",
                            tripwire = "timeline.wal.foreign_schema",
                            path = %path.display(),
                            linked_test = "foreign_or_corrupt_wal_is_reported_and_later_good_wal_recovers",
                            "Ignoring foreign-schema batch while reading shared timeline WALs"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Failed to read WAL file");
            }
        }
    }

    batches
}

/// Recover orphaned WAL files from crashed processes.
///
/// Reads all WAL files in `wal_dir` except the one belonging to `current_pid`,
/// returns their RecordBatches, and reports the files that may be removed only
/// after the caller has durably flushed those batches.
pub fn recover_wal_files(
    wal_dir: &Path,
    current_pid: u32,
    expected_schema: &Schema,
) -> WalRecovery {
    let entries = match fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(_) => return WalRecovery::default(),
    };

    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect();
    paths.sort();
    let mut recovery = WalRecovery::default();

    for path in paths {
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "wal" {
            continue;
        }

        // Skip our own WAL
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if wal_owner_pid(stem) == Some(current_pid) {
            continue;
        }

        // Check if the owning process is still alive
        if is_pid_alive(stem) {
            debug!(path = %path.display(), "WAL owner still alive, skipping");
            continue;
        }

        info!(path = %path.display(), "Recovering orphaned WAL file");
        match read_wal_file_with_status(&path) {
            Ok(WalReadOutcome {
                batches,
                tail_failure,
            }) => {
                let count = batches.len();

                if batches
                    .iter()
                    .any(|batch| batch.schema().as_ref() != expected_schema)
                {
                    let reason = "record batch schema does not match the unified timeline schema";
                    let quarantined_path = quarantine_wal(&path, 0, reason);
                    warn!(
                        "activity.type" = "obs.tripwire",
                        event_type = "obs.tripwire",
                        tripwire = "timeline.wal.foreign_schema",
                        path = %path.display(),
                        linked_test = "foreign_or_corrupt_wal_is_reported_and_later_good_wal_recovers",
                        reason,
                        "Skipped foreign timeline WAL and continued recovery"
                    );
                    recovery.skipped.push(WalSkipReport {
                        path,
                        quarantined_path,
                        recovered_batches: 0,
                        reason: reason.to_string(),
                    });
                    continue;
                }

                if let Some(failure) = tail_failure {
                    recovery.batches.extend(batches);
                    let quarantined_path = quarantine_wal(&path, count, &failure.reason);
                    recovery.skipped.push(WalSkipReport {
                        path,
                        quarantined_path,
                        recovered_batches: count,
                        reason: failure.reason,
                    });
                } else if count == 0 {
                    if let Err(e) = fs::remove_file(&path) {
                        warn!(path = %path.display(), error = %e, "Failed to remove empty recovered WAL");
                    }
                } else {
                    recovery.batches.extend(batches);
                    recovery.removable_files.push(path.clone());
                    info!(path = %path.display(), batches = count, "Recovered WAL pending parquet flush");
                }
            }
            Err(e) => {
                error!(path = %path.display(), error = %e, "Failed to read orphaned WAL");
                let reason = e.to_string();
                let quarantined_path = quarantine_wal(&path, 0, &reason);
                recovery.skipped.push(WalSkipReport {
                    path,
                    quarantined_path,
                    recovered_batches: 0,
                    reason,
                });
            }
        }
    }

    recovery
}

/// Remove fully decoded WALs after their recovered batches reached parquet.
pub fn remove_recovered_wal_files(paths: &[PathBuf]) {
    for path in paths {
        if let Err(e) = fs::remove_file(path) {
            warn!(path = %path.display(), error = %e, "Failed to remove recovered WAL after parquet flush");
        } else {
            info!(path = %path.display(), "Removed recovered WAL after parquet flush");
        }
    }
}

fn quarantine_wal(path: &Path, recovered_batches: usize, failure: &str) -> Option<PathBuf> {
    let mut corrupt_path = path.as_os_str().to_os_string();
    corrupt_path.push(".corrupt");
    let corrupt_path = PathBuf::from(corrupt_path);

    match fs::rename(path, &corrupt_path) {
        Ok(()) => {
            warn!(
                path = %path.display(),
                quarantined_path = %corrupt_path.display(),
                batches = recovered_batches,
                failure,
                "Quarantined WAL after recovery stopped at an unreadable record"
            );
            Some(corrupt_path)
        }
        Err(e) => {
            error!(
                path = %path.display(),
                quarantined_path = %corrupt_path.display(),
                error = %e,
                failure,
                "Failed to quarantine unreadable WAL"
            );
            None
        }
    }
}

/// Check if a PID (given as a string) is still alive.
#[cfg(unix)]
pub(crate) fn is_pid_alive(pid_str: &str) -> bool {
    let Some(pid) = wal_owner_pid(pid_str).and_then(|pid| i32::try_from(pid).ok()) else {
        return false;
    };
    // kill(pid, 0) checks existence without sending a signal
    unsafe { libc::kill(pid, 0) == 0 }
}

fn wal_owner_pid(stem: &str) -> Option<u32> {
    stem.parse().ok().or_else(|| {
        stem.strip_suffix("-lag")
            .and_then(|owner| owner.parse().ok())
    })
}

#[cfg(not(unix))]
pub(crate) fn is_pid_alive(_pid_str: &str) -> bool {
    // Cannot check process liveness portably; assume dead to trigger WAL recovery
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch(id: &str) -> RecordBatch {
        let schema = Arc::new(test_schema());
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![id]))]).unwrap()
    }

    fn test_schema() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Utf8, false)])
    }

    fn batch_id(batch: &RecordBatch) -> &str {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
    }

    fn ipc_message_metadata(ipc: &[u8], offset: usize) -> (std::ops::Range<usize>, usize) {
        assert_eq!(&ipc[offset..offset + 4], &[0xff; 4]);
        let metadata_len =
            u32::from_le_bytes(ipc[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let metadata = offset + 8..offset + 8 + metadata_len;
        let message = arrow::ipc::root_as_message(&ipc[metadata.clone()]).unwrap();
        let next_message = metadata.end + message.bodyLength() as usize;
        (metadata, next_message)
    }

    fn ipc_with_impossible_buffer_bounds(batch: &RecordBatch) -> Vec<u8> {
        let mut ipc = ParquetReader::to_ipc_bytes(std::slice::from_ref(batch)).unwrap();
        let (_, record_offset) = ipc_message_metadata(&ipc, 0);
        let (record_metadata, _) = ipc_message_metadata(&ipc, record_offset);
        let descriptor_offset = {
            let message = arrow::ipc::root_as_message(&ipc[record_metadata]).unwrap();
            let record_batch = message.header_as_record_batch().unwrap();
            let buffers = record_batch.buffers().unwrap();
            let descriptor = buffers.get(buffers.len() - 1);
            descriptor as *const arrow::ipc::Buffer as usize - ipc.as_ptr() as usize
        };
        let corrupt = arrow::ipc::Buffer::new(141_733_920_362, 12_884_901_888);
        ipc[descriptor_offset..descriptor_offset + corrupt.0.len()].copy_from_slice(&corrupt.0);
        ipc
    }

    #[test]
    fn test_wal_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 12345).unwrap();
        writer.append(&test_batch("row1")).unwrap();
        writer.append(&test_batch("row2")).unwrap();
        writer.sync().unwrap();

        let wal_path = wal_dir.join("12345.wal");
        let batches = read_wal_file(&wal_path).unwrap();
        assert_eq!(batches.len(), 2);

        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "row1");
    }

    #[test]
    fn test_wal_truncate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 99).unwrap();
        writer.append(&test_batch("before")).unwrap();
        writer.sync().unwrap();
        writer.truncate().unwrap();
        writer.append(&test_batch("after")).unwrap();
        writer.sync().unwrap();

        let batches = read_wal_file(&wal_dir.join("99.wal")).unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "after");
    }

    #[test]
    fn test_wal_truncated_entry_tolerance() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let mut writer = WalWriter::open(&wal_dir, 1).unwrap();
        writer.append(&test_batch("good")).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Append a truncated frame: valid length header but incomplete data
        let wal_path = wal_dir.join("1.wal");
        let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(&1000u32.to_le_bytes()).unwrap(); // claims 1000 bytes
        f.write_all(b"short").unwrap(); // only 5 bytes
        drop(f);

        let batches = read_wal_file(&wal_path).unwrap();
        assert_eq!(batches.len(), 1); // only the "good" entry
    }

    #[test]
    fn test_wal_empty_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("empty.wal");
        File::create(&path).unwrap();

        let batches = read_wal_file(&path).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_recover_skips_current_pid() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let my_pid = std::process::id();

        // Create a WAL for "our" pid
        let mut writer = WalWriter::open(&wal_dir, my_pid).unwrap();
        writer.append(&test_batch("mine")).unwrap();
        writer.sync().unwrap();
        drop(writer);
        let mut lag_writer = WalWriter::open_with_stem(&wal_dir, &format!("{my_pid}-lag")).unwrap();
        lag_writer.append(&test_batch("my-tripwire")).unwrap();
        lag_writer.sync().unwrap();
        drop(lag_writer);

        // Recovery should skip it
        let recovered = recover_wal_files(&wal_dir, my_pid, &test_schema());
        assert!(recovered.batches.is_empty());
        // File should still exist
        assert!(wal_dir.join(format!("{my_pid}.wal")).exists());
        assert!(wal_dir.join(format!("{my_pid}-lag.wal")).exists());
    }

    #[test]
    fn wal_tail_truncated_mid_record_recovers_prefix_and_quarantines() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        let wal_path = wal_dir.join("torn-short.wal");
        let corrupt_path = wal_dir.join("torn-short.wal.corrupt");

        let mut writer = WalWriter::open_with_stem(&wal_dir, "torn-short").unwrap();
        writer.append(&test_batch("prefix")).unwrap();
        writer.sync().unwrap();
        let prefix_len = writer.bytes_written();
        writer.append(&test_batch("torn")).unwrap();
        writer.sync().unwrap();
        let full_len = writer.bytes_written();
        drop(writer);

        let torn_len = prefix_len + (full_len - prefix_len) / 2;
        OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap()
            .set_len(torn_len)
            .unwrap();
        let forensic_bytes = fs::read(&wal_path).unwrap();

        let recovered = recover_wal_files(&wal_dir, std::process::id(), &test_schema());

        assert_eq!(recovered.batches.len(), 1);
        assert_eq!(batch_id(&recovered.batches[0]), "prefix");
        assert_eq!(recovered.skipped.len(), 1);
        assert!(!wal_path.exists());
        assert!(corrupt_path.exists());
        assert_eq!(fs::read(corrupt_path).unwrap(), forensic_bytes);
    }

    #[test]
    fn wal_tail_garbage_recovers_prefix_and_quarantines() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        let wal_path = wal_dir.join("garbage-tail.wal");
        let corrupt_path = wal_dir.join("garbage-tail.wal.corrupt");

        let mut writer = WalWriter::open_with_stem(&wal_dir, "garbage-tail").unwrap();
        writer.append(&test_batch("prefix")).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let garbage = ipc_with_impossible_buffer_bounds(&test_batch("corrupt"));
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&(garbage.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&garbage).unwrap();
        file.sync_data().unwrap();
        drop(file);
        let forensic_bytes = fs::read(&wal_path).unwrap();

        let outcome = read_wal_file_with_status(&wal_path).unwrap();
        assert_eq!(outcome.batches.len(), 1);
        assert_eq!(
            outcome.tail_failure.unwrap().reason,
            "Arrow IPC decoder panicked"
        );

        let recovered = recover_wal_files(&wal_dir, std::process::id(), &test_schema());

        assert_eq!(recovered.batches.len(), 1);
        assert_eq!(batch_id(&recovered.batches[0]), "prefix");
        assert_eq!(recovered.skipped.len(), 1);
        assert!(!wal_path.exists());
        assert!(corrupt_path.exists());
        assert_eq!(fs::read(corrupt_path).unwrap(), forensic_bytes);
    }

    #[test]
    fn wal_tail_valid_recovery_is_unchanged() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        let wal_path = wal_dir.join("valid-tail.wal");
        let corrupt_path = wal_dir.join("valid-tail.wal.corrupt");

        let mut writer = WalWriter::open_with_stem(&wal_dir, "valid-tail").unwrap();
        writer.append(&test_batch("first")).unwrap();
        writer.append(&test_batch("second")).unwrap();
        writer.sync().unwrap();
        drop(writer);

        let recovered = recover_wal_files(&wal_dir, std::process::id(), &test_schema());

        assert_eq!(recovered.batches.len(), 2);
        assert_eq!(batch_id(&recovered.batches[0]), "first");
        assert_eq!(batch_id(&recovered.batches[1]), "second");
        assert!(
            wal_path.exists(),
            "source WAL remains until parquet succeeds"
        );
        assert!(!corrupt_path.exists());

        remove_recovered_wal_files(&recovered.removable_files);
        assert!(!wal_path.exists());
    }

    #[test]
    fn foreign_or_corrupt_wal_is_reported_and_later_good_wal_recovers() {
        let tmp = tempfile::TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");

        for (stem, id) in [("01-good", "first"), ("04-good", "later")] {
            let mut writer = WalWriter::open_with_stem(&wal_dir, stem).unwrap();
            writer.append(&test_batch(id)).unwrap();
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

        let recovery = recover_wal_files(&wal_dir, std::process::id(), &test_schema());

        let ids: Vec<&str> = recovery.batches.iter().map(batch_id).collect();
        assert_eq!(ids, vec!["first", "later"]);
        assert_eq!(recovery.skipped.len(), 2);
        assert!(recovery.skipped.iter().all(|report| report
            .quarantined_path
            .as_ref()
            .is_some_and(|path| path.exists())));
        assert_eq!(recovery.removable_files.len(), 2);

        remove_recovered_wal_files(&recovery.removable_files);
        assert!(!wal_dir.join("01-good.wal").exists());
        assert!(!wal_dir.join("04-good.wal").exists());
    }
}
