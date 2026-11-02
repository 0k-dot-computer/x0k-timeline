//! The timeline's Parquet store
//!
//! Provides append-only columnar data storage backed by content-addressed
//! Parquet files. Designed for time-series, traces, and logs where the
//! pattern is "write batches of rows, query with filters/aggregations."
//!
//! # Architecture
//!
//! - **Writer**: Serializes Arrow RecordBatches to Snappy-compressed Parquet files,
//!   content-addressed with blake3. Atomic writes via tmp+rename.
//! - **Reader**: SQL queries via DataFusion against the directory of Parquet files.
//! - **Store**: Thread-safe wrapper combining writer and reader.
//! - **Manifest**: Append-only TSV tracking all files — future Negentropy sync index.

pub mod families;
pub mod manifest;
pub mod reader;
pub mod store;
pub mod udfs;
pub mod writer;

pub use store::{CompactionOptions, ParquetStore};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use reader::ParquetReader;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("message", DataType::Utf8, false),
        ]))
    }

    fn make_batch(timestamps: &[u64], messages: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(UInt64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(
                    messages.to_vec(),
                )),
            ],
        )
        .unwrap()
    }

    fn timeline_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("message", DataType::Utf8, false),
        ]))
    }

    fn make_timeline_batch(timestamps: &[i64], messages: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            timeline_schema(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(
                    messages.to_vec(),
                )),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        // Write 3 batches
        for i in 0..3 {
            let batch = make_batch(
                &[i * 100 + 1, i * 100 + 2, i * 100 + 3],
                &["hello", "world", "test"],
            );
            let ipc = ParquetReader::to_ipc_bytes(&[batch]).unwrap();
            let result = store.append_batch(&ipc).await.unwrap();
            assert_eq!(result.rows_written, 3);
        }

        // Verify manifest has 3 entries
        let manifest = manifest::Manifest::new(dir.path());
        let entries = manifest.read_all().unwrap();
        assert_eq!(entries.len(), 3);

        // Query all rows
        let result_bytes = store.query("SELECT * FROM data").await.unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 9);

        // Query with filter
        let result_bytes = store
            .query("SELECT * FROM data WHERE timestamp > 100 AND timestamp < 200")
            .await
            .unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(filtered_rows, 3); // 101, 102, 103
    }

    #[test]
    fn test_content_addressing() {
        let dir = tempfile::tempdir().unwrap();
        let writer = writer::ParquetWriter::new(dir.path()).unwrap();

        let batch = make_batch(&[1, 2, 3], &["a", "b", "c"]);

        let info1 = writer.write_batch(&batch, None).unwrap();
        let info2 = writer.write_batch(&batch, None).unwrap();

        // Same data should produce the same blake3 hash
        assert_eq!(info1.blake3_hash, info2.blake3_hash);
        // But different filenames (different timestamps)
        assert_ne!(info1.filename, info2.filename);
    }

    #[tokio::test]
    async fn test_retain_since() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        // Write a batch
        let batch = make_batch(&[1, 2], &["old", "data"]);
        let ipc = ParquetReader::to_ipc_bytes(&[batch]).unwrap();
        store.append_batch(&ipc).await.unwrap();

        // Get the manifest entry to find the timestamp
        let manifest = manifest::Manifest::new(dir.path());
        let entries = manifest.read_all().unwrap();
        let first_ts = entries[0].timestamp_micros;

        // Write another batch
        let batch = make_batch(&[3, 4], &["new", "data"]);
        let ipc = ParquetReader::to_ipc_bytes(&[batch]).unwrap();
        store.append_batch(&ipc).await.unwrap();

        // Retain only entries after the first one
        let deleted = store.retain_since(first_ts + 1).await.unwrap();
        assert_eq!(deleted, 1);

        // Verify only 1 entry remains in manifest
        let entries = manifest.read_all().unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_query_recovers_from_stale_cached_listing() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        let first = store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_batch(&[1, 2], &["old", "rows"])]).unwrap(),
            )
            .await
            .unwrap();
        store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_batch(&[3, 4], &["new", "rows"])]).unwrap(),
            )
            .await
            .unwrap();

        // Populate the cached DataFusion session, then mimic another process
        // removing a file via retention while this store still has the old
        // listing cached.
        store.query("SELECT * FROM data").await.unwrap();
        std::fs::remove_file(dir.path().join(first.filename)).unwrap();

        let result_bytes = store
            .query("SELECT * FROM data WHERE timestamp >= 3")
            .await
            .unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_query_with_pending_skips_missing_manifest_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        let first = store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_batch(&[1, 2], &["old", "rows"])]).unwrap(),
            )
            .await
            .unwrap();
        store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_batch(&[3, 4], &["new", "rows"])]).unwrap(),
            )
            .await
            .unwrap();

        // The manifest can lag physical retention performed by another process.
        // query_with_pending should use only files still present on disk.
        std::fs::remove_file(dir.path().join(first.filename)).unwrap();

        let pending = make_batch(&[5], &["pending"]);
        let result_bytes = store
            .query_with_pending("SELECT * FROM data WHERE timestamp >= 3", &[pending])
            .await
            .unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_query_with_pending_prunes_timestamp_lower_bound_manifest_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::with_table_name(dir.path(), "timeline").unwrap();

        let old = store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_timeline_batch(&[1], &["old"])]).unwrap(),
            )
            .await
            .unwrap();
        let manifest = manifest::Manifest::new(dir.path());
        let old_manifest_ts = manifest.read_all().unwrap()[0].timestamp_micros;

        std::fs::write(dir.path().join(old.filename), b"not a parquet file").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        let lower_bound = old_manifest_ts + 1;
        store
            .append_batch(
                &ParquetReader::to_ipc_bytes(&[make_timeline_batch(
                    &[lower_bound as i64 + 1],
                    &["new"],
                )])
                .unwrap(),
            )
            .await
            .unwrap();

        let pending = make_timeline_batch(&[], &[]);
        let result_bytes = store
            .query_with_pending(
                &format!(
                    "SELECT timestamp_us, message FROM timeline \
                     WHERE timestamp_us >= {lower_bound} \
                     ORDER BY timestamp_us DESC LIMIT 1"
                ),
                &[pending],
            )
            .await
            .unwrap();

        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    fn family_batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
        let text = |column: Vec<Option<&str>>| Arc::new(StringArray::from(column)) as _;
        let names: Vec<Option<&str>> = rows.iter().map(|row| Some(row.0)).collect();
        let fields: Vec<Option<&str>> = rows.iter().map(|row| Some(row.2)).collect();
        let schema = Schema::new(vec![
            Field::new("entry_kind", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, true),
            Field::new("span_name", DataType::Utf8, true),
            Field::new("fields_json", DataType::Utf8, true),
            Field::new("timestamp_us", DataType::Int64, false),
            Field::new("module", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                text(vec![Some("activity"); rows.len()]),
                text(names),
                text(vec![None; rows.len()]),
                text(fields),
                Arc::new(Int64Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())),
                text(vec![Some("checkout::pay"); rows.len()]),
            ],
        )
        .unwrap()
    }

    fn footer(dir: &std::path::Path) -> families::Families {
        let entries = manifest::Manifest::new(dir).read_all().unwrap();
        assert_eq!(entries.len(), 1, "one file");
        families::Families::read_file(&dir.join(&entries[0].filename)).unwrap().expect("a footer")
    }

    #[tokio::test]
    async fn a_file_describes_its_families_after_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let batch = family_batch(&[("pay", 7, r#"{"amount":1.5}"#), ("pay", 3, "{}")]);
        let bytes = ParquetReader::to_ipc_bytes(&[batch]).unwrap();
        ParquetStore::new(dir.path()).unwrap().append_batch(&bytes).await.unwrap();

        let reopened = footer(dir.path());
        let pay = &reopened.families["pay"];
        assert_eq!((pay.rows, pay.first_seen_us, pay.last_seen_us), (2, 3, 7));
        assert!(pay.fields.contains_key("amount"));
        let site = pay.call_sites.iter().next().expect("a call site");
        assert_eq!(site.module.as_deref(), Some("checkout::pay"));
    }

    #[tokio::test]
    async fn compaction_keeps_what_its_inputs_described() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();
        for (at, fields) in [(1, r#"{"a":1}"#), (2, r#"{"b":"x"}"#), (3, r#"{"a":-1}"#)] {
            let batch = family_batch(&[("pay", at, fields)]);
            store.append_batch(&ParquetReader::to_ipc_bytes(&[batch]).unwrap()).await.unwrap();
        }
        store
            .compact_once(CompactionOptions {
                min_input_files: 2,
                max_input_files: 8,
                target_rows: 100,
                min_file_age: std::time::Duration::ZERO,
            })
            .await
            .unwrap();

        let compacted = footer(dir.path());
        let pay = &compacted.families["pay"];
        assert_eq!((pay.rows, pay.first_seen_us, pay.last_seen_us), (3, 1, 3));
        assert_eq!(families::widened(&pay.fields["a"]), Some(families::FieldKind::Signed));
        assert_eq!(families::widened(&pay.fields["b"]), Some(families::FieldKind::Text));
    }

    #[tokio::test]
    async fn a_table_of_other_rows_writes_no_families() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();
        let bytes = ParquetReader::to_ipc_bytes(&[make_batch(&[1], &["row"])]).unwrap();
        store.append_batch(&bytes).await.unwrap();
        let entries = manifest::Manifest::new(dir.path()).read_all().unwrap();
        let path = dir.path().join(&entries[0].filename);
        assert_eq!(families::Families::read_file(&path).unwrap(), None);
    }

    #[tokio::test]
    async fn test_compact_once_replaces_small_manifest_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        for i in 0..5 {
            store
                .append_batch(&ParquetReader::to_ipc_bytes(&[make_batch(&[i], &["row"])]).unwrap())
                .await
                .unwrap();
        }

        let result = store
            .compact_once(CompactionOptions {
                min_input_files: 4,
                max_input_files: 8,
                target_rows: 100,
                min_file_age: std::time::Duration::ZERO,
            })
            .await
            .unwrap();

        assert_eq!(result.input_files, 5);
        assert_eq!(result.output_files, 1);
        assert_eq!(result.rows_written, 5);

        let manifest = manifest::Manifest::new(dir.path());
        let entries = manifest.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_count, 5);

        let result_bytes = store.query("SELECT * FROM data").await.unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_compact_once_removes_missing_manifest_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let store = ParquetStore::new(dir.path()).unwrap();

        let mut written = Vec::new();
        for i in 0..5 {
            written.push(
                store
                    .append_batch(
                        &ParquetReader::to_ipc_bytes(&[make_batch(&[i], &["row"])]).unwrap(),
                    )
                    .await
                    .unwrap(),
            );
        }
        std::fs::remove_file(dir.path().join(&written[0].filename)).unwrap();

        let result = store
            .compact_once(CompactionOptions {
                min_input_files: 4,
                max_input_files: 8,
                target_rows: 100,
                min_file_age: std::time::Duration::ZERO,
            })
            .await
            .unwrap();

        assert!(result.attempted);
        assert_eq!(result.input_files, 5);
        assert_eq!(result.output_files, 1);
        assert_eq!(result.rows_written, 4);

        let manifest = manifest::Manifest::new(dir.path());
        let entries = manifest.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_count, 4);

        let result_bytes = store.query("SELECT * FROM data").await.unwrap();
        let batches = ParquetReader::from_ipc_bytes(&result_bytes).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_compact_once_does_not_starve_after_near_target_files() {
        let dir = tempfile::tempdir().unwrap();
        let writer = writer::ParquetWriter::new(dir.path()).unwrap();

        for (idx, rows) in [60usize, 50, 1, 1, 1].into_iter().enumerate() {
            let timestamps: Vec<u64> = (0..rows as u64).collect();
            let messages = vec!["row"; rows];
            writer
                .write_batch_with_prefix_at(
                    &make_batch(&timestamps, &messages),
                    None,
                    "",
                    idx as u64 + 1,
                )
                .unwrap();
        }

        let store = ParquetStore::new(dir.path()).unwrap();
        let result = store
            .compact_once(CompactionOptions {
                min_input_files: 4,
                max_input_files: 8,
                target_rows: 100,
                min_file_age: std::time::Duration::ZERO,
            })
            .await
            .unwrap();

        assert_eq!(result.input_files, 4);
        assert_eq!(result.output_files, 1);
        assert_eq!(result.rows_written, 112);

        let manifest = manifest::Manifest::new(dir.path());
        let entries = manifest.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| entry.row_count == 112));
    }

    #[test]
    fn test_ipc_roundtrip() {
        let batch = make_batch(&[1, 2, 3], &["a", "b", "c"]);
        let bytes = ParquetReader::to_ipc_bytes(std::slice::from_ref(&batch)).unwrap();
        let decoded = ParquetReader::from_ipc_bytes(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].schema(), batch.schema());
    }
}
