//! Parquet file writer with content-addressing and atomic writes

use crate::families::Families;
use crate::manifest::{Manifest, ParquetFileInfo};
use arrow::record_batch::RecordBatch;
use arrow_select::concat::concat_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Writes RecordBatches as content-addressed Parquet files
pub struct ParquetWriter {
    dir: PathBuf,
    manifest: Manifest,
}

/// Result of one bounded compaction pass.
#[derive(Debug, Clone, Default)]
pub struct CompactionResult {
    /// True when a compaction group was selected and read/rewrite was attempted.
    ///
    /// `input_files == 0` with `attempted == true` means a transient race
    /// (missing input file or manifest replacement conflict) prevented progress;
    /// `attempted == false` means there was no eligible group.
    pub attempted: bool,
    pub input_files: usize,
    pub output_files: usize,
    pub rows_written: usize,
}

impl CompactionResult {
    fn attempted_no_progress() -> Self {
        Self {
            attempted: true,
            ..Self::default()
        }
    }
}

impl ParquetWriter {
    /// Create a new writer, creating the directory if needed
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            manifest: Manifest::new(dir),
        })
    }

    /// Write a RecordBatch as a content-addressed Parquet file
    ///
    /// Returns info about the written file including its blake3 hash.
    pub fn write_batch(
        &self,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
    ) -> anyhow::Result<ParquetFileInfo> {
        self.write_batch_with_prefix(batch, writer_properties, "")
    }

    /// Write a RecordBatch with a filename prefix for tiered retention.
    ///
    /// When `prefix` is non-empty, the filename becomes `{prefix}{timestamp}_{hash}.parquet`.
    /// The reaper uses these prefixes to apply different retention cutoffs.
    pub fn write_batch_with_prefix(
        &self,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        prefix: &str,
    ) -> anyhow::Result<ParquetFileInfo> {
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        self.write_batch_with_prefix_at(batch, writer_properties, prefix, now_micros)
    }

    /// Write a RecordBatch with a caller-supplied logical timestamp.
    pub fn write_batch_with_prefix_at(
        &self,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        prefix: &str,
        timestamp_micros: u64,
    ) -> anyhow::Result<ParquetFileInfo> {
        let info = self.write_batch_file_with_prefix_at(
            batch,
            writer_properties,
            prefix,
            timestamp_micros,
        )?;
        self.manifest.append_entry(&info)?;
        Ok(info)
    }

    fn write_batch_file_with_prefix_at(
        &self,
        batch: &RecordBatch,
        writer_properties: Option<WriterProperties>,
        prefix: &str,
        timestamp_micros: u64,
    ) -> anyhow::Result<ParquetFileInfo> {
        let mut buf = Vec::new();
        let props = writer_properties.unwrap_or_else(|| {
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build()
        });
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        writer.write(batch)?;
        if let Some(families) = Families::footer_of(batch) {
            writer.append_key_value_metadata(families);
        }
        writer.close()?;

        // Content-address with blake3
        let hash = blake3::hash(&buf);
        let hash_bytes: [u8; 32] = *hash.as_bytes();
        let hash_hex = hash.to_hex();
        let first_16 = &hash_hex[..16];

        let filename = format!("{}{}_{}.parquet", prefix, timestamp_micros, first_16);

        // Atomic write: write to .tmp then rename
        let tmp_path = self.dir.join(format!("{}.tmp", filename));
        let final_path = self.dir.join(&filename);
        std::fs::write(&tmp_path, &buf)?;
        std::fs::rename(&tmp_path, &final_path)?;

        let info = ParquetFileInfo {
            timestamp_micros,
            blake3_hash: hash_bytes,
            row_count: batch.num_rows(),
            filename,
        };

        tracing::debug!(
            filename = %info.filename,
            rows = info.row_count,
            bytes = buf.len(),
            "Wrote Parquet file"
        );

        Ok(info)
    }

    /// Compact a selected group of manifest entries into one larger Parquet file.
    ///
    /// The caller chooses a bounded group; this method writes the compacted file
    /// off-manifest, then atomically replaces the selected manifest entries. If
    /// another retention/compaction pass changed the manifest first, the new file
    /// is deleted and no rows are resurrected.
    pub fn compact_entries(
        &self,
        entries: &[ParquetFileInfo],
        writer_properties: Option<WriterProperties>,
        prefix: &str,
    ) -> anyhow::Result<CompactionResult> {
        if entries.len() < 2 {
            return Ok(CompactionResult::default());
        }

        let mut batches = Vec::new();
        let mut missing_inputs = 0usize;
        for entry in entries {
            let file = match std::fs::File::open(self.dir.join(&entry.filename)) {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(
                        filename = %entry.filename,
                        "Removing stale manifest entry during compaction"
                    );
                    missing_inputs += 1;
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            for batch in reader {
                batches.push(batch?);
            }
        }
        if missing_inputs > 0 {
            tracing::debug!(
                missing_inputs,
                selected_files = entries.len(),
                "Compaction selected stale manifest entries"
            );
        }

        let new_entry = if batches.is_empty() {
            None
        } else {
            let schema = batches[0].schema();
            let compacted = concat_batches(&schema, &batches)?;
            let timestamp_micros = entries
                .iter()
                .map(|entry| entry.timestamp_micros)
                .max()
                .unwrap_or_default();
            Some(self.write_batch_file_with_prefix_at(
                &compacted,
                writer_properties,
                prefix,
                timestamp_micros,
            )?)
        };

        let remove_filenames: HashSet<String> =
            entries.iter().map(|entry| entry.filename.clone()).collect();
        let removed = match self.manifest.replace_entries_if_all_present(
            &remove_filenames,
            new_entry.as_slice(),
        )? {
            Some(removed) => removed,
            None => {
                if let Some(new_entry) = &new_entry {
                    let _ = std::fs::remove_file(self.dir.join(&new_entry.filename));
                }
                return Ok(CompactionResult::attempted_no_progress());
            }
        };

        for entry in &removed {
            let path = self.dir.join(&entry.filename);
            if let Err(err) = std::fs::remove_file(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        filename = %entry.filename,
                        error = %err,
                        "Failed to delete compacted input file"
                    );
                }
            }
        }

        Ok(CompactionResult {
            attempted: true,
            input_files: removed.len(),
            output_files: usize::from(new_entry.is_some()),
            rows_written: batches.iter().map(|batch| batch.num_rows()).sum(),
        })
    }

    /// Delete files older than cutoff and rewrite manifest
    ///
    /// Returns the number of files deleted.
    pub fn retain_since(&self, cutoff_micros: u64) -> anyhow::Result<usize> {
        let removed = self.manifest.retain_since(cutoff_micros)?;
        let count = removed.len();

        for entry in removed {
            let path = self.dir.join(&entry.filename);
            if path.exists() {
                std::fs::remove_file(&path)?;
                tracing::debug!(filename = %entry.filename, "Deleted old Parquet file");
            }
        }

        Ok(count)
    }

    /// Delete files using per-prefix cutoffs for tiered retention.
    ///
    /// Each entry in `tier_cutoffs` is `(prefix, cutoff_micros)`. An entry's
    /// filename is matched against each prefix in order; the first match wins.
    /// If no prefix matches, the entry is kept.
    ///
    /// Returns the number of files deleted.
    pub fn retain_tiered(&self, tier_cutoffs: &[(String, u64)]) -> anyhow::Result<usize> {
        let removed = self.manifest.retain_tiered(tier_cutoffs)?;
        let count = removed.len();

        for entry in removed {
            let path = self.dir.join(&entry.filename);
            if path.exists() {
                std::fs::remove_file(&path)?;
                tracing::debug!(filename = %entry.filename, "Deleted old Parquet file (tiered)");
            }
        }

        Ok(count)
    }

    /// Delete `.parquet` files on disk that are not tracked in the manifest.
    ///
    /// Only removes files whose mtime is older than `min_age` to avoid racing
    /// with in-progress writes (the file is created before the manifest entry).
    /// Returns the number of files deleted.
    pub fn cleanup_orphaned_files(&self, min_age: Duration) -> anyhow::Result<usize> {
        let manifest_files: HashSet<String> = self
            .manifest
            .read_all()?
            .into_iter()
            .map(|e| e.filename)
            .collect();

        let now = std::time::SystemTime::now();
        let mut deleted = 0;

        let entries = std::fs::read_dir(&self.dir)?;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            let Some(ext) = path.extension() else {
                continue;
            };
            if ext != "parquet" {
                continue;
            }
            let Some(fname) = path.file_name().and_then(|f| f.to_str()) else {
                continue;
            };

            if manifest_files.contains(fname) {
                continue;
            }

            // Check mtime to avoid deleting files still being written
            let mtime = match entry.metadata().and_then(|m| m.modified()) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let age = now.duration_since(mtime).unwrap_or_default();
            if age < min_age {
                continue;
            }

            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(file = fname, error = %e, "Failed to delete orphaned parquet file");
            } else {
                tracing::info!(
                    file = fname,
                    age_secs = age.as_secs(),
                    "Deleted orphaned parquet file"
                );
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// List all files tracked by the manifest
    pub fn list_files(&self) -> anyhow::Result<Vec<ParquetFileInfo>> {
        self.manifest.read_all()
    }

    /// Hold a shared manifest lock while a query uses a manifest-derived
    /// Parquet file snapshot.
    pub(crate) fn lock_manifest_shared_for_query(&self) -> anyhow::Result<std::fs::File> {
        self.manifest
            .lock_shared_with_timeout(Duration::from_secs(5))
    }

    /// List files while the caller already holds an appropriate manifest lock.
    pub(crate) fn list_files_unlocked(&self) -> anyhow::Result<Vec<ParquetFileInfo>> {
        self.manifest.read_all_unlocked()
    }
}
