//! Parquet file manifest for tracking content-addressed files
//!
//! The manifest is an append-only TSV file that records each Parquet file written.
//! Format: `{timestamp_micros}\t{blake3_hex}\t{row_count}\t{filename}\n`
//!
//! This is the future reconciliation index for Negentropy-based sync.
//! Each entry is a content-addressed Parquet file; the manifest itself
//! is the ordered log that peers can diff efficiently.

use fs2::FileExt;
use std::collections::HashSet;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Information about a single Parquet file in the store
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    /// Timestamp when the file was written (microseconds since epoch)
    pub timestamp_micros: u64,
    /// Blake3 hash of the file contents
    pub blake3_hash: [u8; 32],
    /// Number of rows in the file
    pub row_count: usize,
    /// Filename (relative to store directory)
    pub filename: String,
}

impl ParquetFileInfo {
    /// Get the blake3 hash as a hex string
    pub fn blake3_hex(&self) -> String {
        hex_encode(&self.blake3_hash)
    }
}

/// Manages the manifest file for a Parquet store
///
/// All mutations are serialized via an internal Mutex so that concurrent
/// `write_batch` calls don't interleave manifest lines.
pub(crate) struct Manifest {
    path: PathBuf,
    /// Serializes append_entry and retain_since so concurrent writers
    /// don't produce garbled manifest lines.
    lock: Mutex<()>,
}

impl Manifest {
    pub fn new(dir: &Path) -> Self {
        Self {
            path: dir.join("manifest.tsv"),
            lock: Mutex::new(()),
        }
    }

    /// Open (or create) the `.manifest.lock` file for cross-process flock.
    fn lock_file(&self) -> anyhow::Result<std::fs::File> {
        let lock_path = self.path.with_extension("lock");
        Ok(std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?)
    }

    /// Acquire exclusive flock with exponential backoff up to `timeout`.
    ///
    /// Returns the locked file handle, or an error if the lock could not be
    /// acquired within the deadline.
    fn lock_exclusive_with_timeout(&self, timeout: Duration) -> anyhow::Result<std::fs::File> {
        let flock = self.lock_file()?;
        let deadline = Instant::now() + timeout;
        let mut backoff = Duration::from_millis(1);
        loop {
            match flock.try_lock_exclusive() {
                Ok(()) => return Ok(flock),
                Err(_) if Instant::now() >= deadline => {
                    anyhow::bail!("manifest lock busy for {:?}", timeout);
                }
                Err(_) => {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_millis(50));
                }
            }
        }
    }

    /// Acquire shared flock with exponential backoff up to `timeout`.
    ///
    /// Query paths hold this for the lifetime of the physical file snapshot
    /// they hand to DataFusion. That prevents retention from deleting a file
    /// after the query has listed it but before the scanner opens it.
    pub(crate) fn lock_shared_with_timeout(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<std::fs::File> {
        let flock = self.lock_file()?;
        let deadline = Instant::now() + timeout;
        let mut backoff = Duration::from_millis(1);
        loop {
            match flock.try_lock_shared() {
                Ok(()) => return Ok(flock),
                Err(_) if Instant::now() >= deadline => {
                    anyhow::bail!("manifest shared lock busy for {:?}", timeout);
                }
                Err(_) => {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_millis(50));
                }
            }
        }
    }

    /// Append an entry to the manifest
    pub fn append_entry(&self, info: &ParquetFileInfo) -> anyhow::Result<()> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());

        // Exclusive flock — serializes appends across processes for crash safety
        let flock = self.lock_file()?;
        flock.lock_exclusive()?;

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        writeln!(
            file,
            "{}\t{}\t{}\t{}",
            info.timestamp_micros,
            info.blake3_hex(),
            info.row_count,
            info.filename,
        )?;
        file.sync_data()?;

        Ok(())
    }

    /// Read all entries from the manifest
    pub fn read_all(&self) -> anyhow::Result<Vec<ParquetFileInfo>> {
        let _flock = self.lock_shared_with_timeout(Duration::from_secs(5))?;
        self.read_all_unlocked()
    }

    /// Rewrite manifest keeping only entries since cutoff
    ///
    /// Acquires an exclusive cross-process flock. If another process holds the
    /// lock (reaping or appending), returns `Ok(vec![])` — the caller should
    /// retry next cycle.
    pub fn retain_since(&self, cutoff_micros: u64) -> anyhow::Result<Vec<ParquetFileInfo>> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());

        // Exclusive flock — retry with backoff so retention isn't starved by appenders
        let _flock = match self.lock_exclusive_with_timeout(Duration::from_secs(5)) {
            Ok(f) => f,
            Err(_) => {
                tracing::warn!("Skipping retention: manifest lock contention");
                return Ok(Vec::new());
            }
        };

        let all = self.read_all_unlocked()?;
        let (keep, remove): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|e| e.timestamp_micros >= cutoff_micros);

        self.write_entries_unlocked(&keep)?;

        Ok(remove)
    }

    /// Rewrite manifest using per-prefix cutoffs for tiered retention.
    ///
    /// Each entry in `tier_cutoffs` is `(prefix, cutoff_micros)`. For each
    /// manifest entry, the filename is matched against each prefix; the first
    /// match determines the cutoff. If `timestamp < cutoff`, the entry is
    /// removed. An empty-string prefix acts as the default/fallback.
    ///
    /// Acquires an exclusive cross-process flock. If another process holds the
    /// lock, returns `Ok(vec![])`.
    pub fn retain_tiered(
        &self,
        tier_cutoffs: &[(String, u64)],
    ) -> anyhow::Result<Vec<ParquetFileInfo>> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());

        // Exclusive flock — retry with backoff so retention isn't starved by appenders
        let _flock = match self.lock_exclusive_with_timeout(Duration::from_secs(5)) {
            Ok(f) => f,
            Err(_) => {
                tracing::warn!("Skipping retention: manifest lock contention");
                return Ok(Vec::new());
            }
        };

        let all = self.read_all_unlocked()?;
        let (keep, remove): (Vec<_>, Vec<_>) = all.into_iter().partition(|entry| {
            // Find the first matching prefix
            for (prefix, cutoff) in tier_cutoffs {
                if prefix.is_empty() || entry.filename.starts_with(prefix) {
                    return entry.timestamp_micros >= *cutoff;
                }
            }
            // No prefix matched at all — keep the file
            true
        });

        self.write_entries_unlocked(&keep)?;

        Ok(remove)
    }

    /// Atomically replace a set of manifest entries with new compacted entries.
    ///
    /// Returns `Ok(None)` if any entry to remove is no longer present. That means
    /// another retention/compaction pass won the race and the caller should delete
    /// any newly-written untracked files rather than resurrecting old rows.
    pub(crate) fn replace_entries_if_all_present(
        &self,
        remove_filenames: &HashSet<String>,
        add_entries: &[ParquetFileInfo],
    ) -> anyhow::Result<Option<Vec<ParquetFileInfo>>> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let _flock = match self.lock_exclusive_with_timeout(Duration::from_secs(5)) {
            Ok(f) => f,
            Err(_) => {
                tracing::warn!("Skipping compaction manifest replace: manifest lock contention");
                return Ok(None);
            }
        };

        let all = self.read_all_unlocked()?;
        let present: HashSet<&str> = all.iter().map(|entry| entry.filename.as_str()).collect();
        if !remove_filenames
            .iter()
            .all(|filename| present.contains(filename.as_str()))
        {
            return Ok(None);
        }

        let (keep, remove): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|entry| !remove_filenames.contains(&entry.filename));
        let mut rewritten = keep;
        rewritten.extend(add_entries.iter().cloned());
        rewritten.sort_by_key(|entry| entry.timestamp_micros);
        self.write_entries_unlocked(&rewritten)?;

        Ok(Some(remove))
    }

    /// Read all entries without taking the lock (caller must hold it)
    pub(crate) fn read_all_unlocked(&self) -> anyhow::Result<Vec<ParquetFileInfo>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);
        let mut entries = Vec::new();
        let mut malformed_count: u32 = 0;
        let mut first_malformed: Option<String> = None;

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() != 4 {
                malformed_count += 1;
                if first_malformed.is_none() {
                    first_malformed = Some(line.to_string());
                }
                continue;
            }

            let timestamp_micros: u64 = parts[0].parse()?;
            let blake3_hash = hex_decode(parts[1])?;
            let row_count: usize = parts[2].parse()?;
            let filename = parts[3].to_string();

            entries.push(ParquetFileInfo {
                timestamp_micros,
                blake3_hash,
                row_count,
                filename,
            });
        }

        if malformed_count > 0 {
            tracing::warn!(
                count = malformed_count,
                first = ?first_malformed,
                "Skipped malformed manifest lines"
            );
        }

        Ok(entries)
    }

    fn write_entries_unlocked(&self, entries: &[ParquetFileInfo]) -> anyhow::Result<()> {
        let tmp_path = self.path.with_extension("tsv.tmp");
        let mut file = std::fs::File::create(&tmp_path)?;
        for entry in entries {
            writeln!(
                file,
                "{}\t{}\t{}\t{}",
                entry.timestamp_micros,
                entry.blake3_hex(),
                entry.row_count,
                entry.filename,
            )?;
        }
        file.sync_data()?;
        std::fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    /// Repair the manifest by reading all valid entries and rewriting atomically.
    ///
    /// Malformed lines are silently dropped. Returns the count of valid entries preserved.
    #[allow(dead_code)]
    pub fn repair(&self) -> anyhow::Result<usize> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());

        let flock = self.lock_file()?;
        flock.lock_exclusive()?;

        let entries = self.read_all_unlocked()?;
        let count = entries.len();

        let tmp_path = self.path.with_extension("tsv.tmp");
        let mut file = std::fs::File::create(&tmp_path)?;
        for entry in &entries {
            writeln!(
                file,
                "{}\t{}\t{}\t{}",
                entry.timestamp_micros,
                entry.blake3_hex(),
                entry.row_count,
                entry.filename,
            )?;
        }
        file.sync_data()?;
        std::fs::rename(&tmp_path, &self.path)?;

        Ok(count)
    }
}

fn hex_encode(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> anyhow::Result<[u8; 32]> {
    if s.len() != 64 {
        anyhow::bail!("Invalid hex string length: expected 64, got {}", s.len());
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn info(filename: &str, timestamp_micros: u64) -> ParquetFileInfo {
        ParquetFileInfo {
            timestamp_micros,
            blake3_hash: [7; 32],
            row_count: 1,
            filename: filename.to_string(),
        }
    }

    #[test]
    fn shared_lock_blocks_retention_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Arc::new(Manifest::new(dir.path()));
        manifest.append_entry(&info("old.parquet", 10)).unwrap();
        manifest.append_entry(&info("new.parquet", 20)).unwrap();

        let shared = manifest
            .lock_shared_with_timeout(Duration::from_secs(1))
            .unwrap();
        let other = manifest.clone();
        let handle = std::thread::spawn(move || other.retain_since(15).unwrap());

        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !handle.is_finished(),
            "retention should wait while a shared query lock is held"
        );

        drop(shared);
        let removed = handle.join().unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].filename, "old.parquet");
    }
}
