//! The timeline query engine — the columnar core of the Timeline Query Service.
//!
//! This is the code path that was extracted out of `x0k-dev-daemon`'s MCP server
//! (`query_timeline_sql` / `describe_timeline`) so that the columnar stack
//! (arrow + parquet + datafusion + the `json_extract` UDF) lives in ONE focused
//! service instead of being re-linked by every client (DRIVER-1). The engine reads the SAME parquet store and the SAME
//! shared filesystem WAL (`~/.0k/activities/timeline/wal/{pid}.wal`) the
//! single-writer daemon produces, so recent rows stay INSTANTLY queryable.
//!
//! Parity is non-negotiable: every behaviour here is the byte-for-byte
//! equivalent of the dev-daemon implementation the corpus
//! (`timeline-parity-corpus.json`) pins. The 58-column unified schema rides
//! unchanged; `parent_id` stays `parent_id`. An `entry_kind='conversation'` row
//! is returned as stored: its description is NULL, because parquet never held
//! the text and nothing joins it at query time.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use arrow::array::RecordBatch;
use x0k_parquet::families::Families;

/// Resolve the timeline parquet directory: `$X0K_ACTIVITIES_BASE_PATH/timeline`,
/// by default `activities/timeline` under [`crate::config::state_root`].
///
/// The writer ([`crate::writer`]) and the query engine both resolve the store
/// here, so they cannot disagree about it; the dev daemon's readers
/// (`x0k_dev_daemon::ephemeral::EphemeralActivitiesConfig::default()`) open the
/// same default.
pub fn timeline_dir() -> PathBuf {
    let base = std::env::var("X0K_ACTIVITIES_BASE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| crate::config::state_root().join("activities"));
    base.join("timeline")
}

/// A long-lived handle to the timeline parquet store. Cached so each query does
/// not re-scan the directory (parity with the dev-daemon's `get_timeline_store`
/// `OnceCell`).
pub struct TimelineEngine {
    store: OnceLock<Option<x0k_parquet::ParquetStore>>,
    footers: Mutex<HashMap<PathBuf, Option<Arc<Families>>>>,
}

/// Why a store's families could not be read.
#[derive(Debug, thiserror::Error)]
pub enum FamiliesError {
    #[error("{0}")]
    Store(String),
    #[error("listing the store's files: {0}")]
    List(anyhow::Error),
    #[error("reading the store's files: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// The families of a store, and how much of it spoke for itself.
#[derive(Debug, Default)]
pub struct EventFamilies {
    pub families: Families,
    pub files: usize,
    /// Files written before footers held families.
    pub files_without_schema: usize,
}

impl Default for TimelineEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineEngine {
    pub fn new() -> Self {
        Self {
            store: OnceLock::new(),
            footers: Mutex::default(),
        }
    }

    /// Get or lazily open the cached [`x0k_parquet::ParquetStore`].
    ///
    /// The store internally owns the DataFusion `SessionContext` and registers
    /// the `json_extract` UDF via `x0k_parquet::udfs::register_timeline_udfs`
    /// — that registration is the load-bearing piece the
    /// `json_extract_node_name_slicing_W1` parity query guards.
    pub fn get_store(&self) -> Result<&x0k_parquet::ParquetStore, String> {
        self.store
            .get_or_init(|| {
                let dir = timeline_dir();
                if !dir.exists() {
                    if let Err(e) = std::fs::create_dir_all(&dir) {
                        tracing::warn!(error = %e, "Failed to create timeline dir");
                        return None;
                    }
                }
                match x0k_parquet::ParquetStore::with_table_name(&dir, "timeline") {
                    Ok(s) => Some(s),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to open timeline store");
                        None
                    }
                }
            })
            .as_ref()
            .ok_or_else(|| "Timeline store not available".to_string())
    }

    /// The event families of the store and of the rows not flushed yet.
    ///
    /// A file's name is its content's hash, so a footer read once stands.
    pub fn event_families(&self) -> Result<EventFamilies, FamiliesError> {
        let store = self.get_store().map_err(FamiliesError::Store)?;
        let paths = store.file_paths().map_err(FamiliesError::List)?;
        let mut answer = EventFamilies { files: paths.len(), ..EventFamilies::default() };
        let mut footers = self.footers.lock().unwrap_or_else(PoisonError::into_inner);
        for path in &paths {
            let footer = match footers.get(path) {
                Some(footer) => footer.clone(),
                None => match Families::read_file(path) {
                    Ok(footer) => {
                        let footer = footer.map(Arc::new);
                        footers.insert(path.clone(), footer.clone());
                        footer
                    }
                    // Retention or compaction took it after the listing.
                    Err(error) => {
                        tracing::debug!(%error, path = %path.display(), "unreadable footer");
                        None
                    }
                },
            };
            match footer {
                Some(footer) => answer.families.absorb(&footer),
                None => answer.files_without_schema += 1,
            }
        }
        let standing: HashSet<&PathBuf> = paths.iter().collect();
        footers.retain(|path, _| standing.contains(path));
        let wal_dir = store.dir().join("wal");
        for batch in x0k_activity_sink::wal::read_all_wal_files(&wal_dir) {
            if let Some(families) = Families::of_batch(&batch) {
                answer.families.absorb(&families);
            }
        }
        Ok(answer)
    }

    /// Execute a (validated) SELECT/WITH query against the timeline.
    ///
    /// Reads recent unflushed WAL batches via
    /// `x0k_activity_sink::wal::read_all_wal_files` so very recent rows are
    /// visible before parquet flush (the recent-data-instantly-queryable
    /// guarantee).
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let store = self.get_store()?;

        // Read unflushed WAL data so recent traces are immediately queryable.
        let wal_dir = store.dir().join("wal");
        let pending = x0k_activity_sink::wal::read_all_wal_files(&wal_dir);

        let bytes = store
            .query_with_pending(sql, &pending)
            .await
            .map_err(|e| format!("SQL query failed: {}", e))?;

        if bytes.is_empty() {
            return Ok(vec![]);
        }

        x0k_parquet::reader::ParquetReader::from_ipc_bytes(&bytes)
            .map_err(|e| format!("Failed to parse query results: {}", e))
    }
}

/// SQL safety gate: only `SELECT`/`WITH`; reject write keywords outside string
/// literals. Byte-identical to the dev-daemon `query_timeline_sql` guard so the
/// allow/deny surface does not drift in the move.
pub fn validate_select_only(sql: &str) -> Result<(), String> {
    let sql_upper = sql.trim().to_uppercase();
    if !sql_upper.starts_with("SELECT") && !sql_upper.starts_with("WITH") {
        return Err("Only SELECT and WITH queries are allowed".to_string());
    }
    for kw in &[
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "COPY",
    ] {
        let mut in_quote = false;
        for (i, ch) in sql_upper.char_indices() {
            if ch == '\'' {
                in_quote = !in_quote;
            } else if !in_quote && sql_upper[i..].starts_with(kw) {
                let before_ok = i == 0 || !sql_upper.as_bytes()[i - 1].is_ascii_alphanumeric();
                let after_idx = i + kw.len();
                let after_ok = after_idx >= sql_upper.len()
                    || !sql_upper.as_bytes()[after_idx].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return Err(format!("Write operations ({}) are not allowed", kw));
                }
            }
        }
    }
    Ok(())
}

/// Inject `LIMIT <max_rows>` when the query does not already carry one. Matches
/// the dev-daemon behaviour (case-insensitive `LIMIT` presence test).
pub fn inject_limit(sql: &str, max_rows: usize) -> String {
    if sql.to_uppercase().contains("LIMIT") {
        sql.to_string()
    } else {
        format!("{} LIMIT {}", sql.trim().trim_end_matches(';'), max_rows)
    }
}

/// Convert an Arrow column value at a given row to a `serde_json::Value`.
///
/// Handles the three physical types in the timeline schema (Utf8, Int64,
/// UInt32) and parses `*_json` columns into nested JSON. Moved verbatim from
/// the dev-daemon so row rendering is byte-identical.
pub fn arrow_col_to_json(
    col: &dyn arrow::array::Array,
    row: usize,
    field_name: &str,
) -> serde_json::Value {
    use arrow::array::{Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        let s = a.value(row);
        if field_name.ends_with("_json") {
            if let Ok(parsed) = serde_json::from_str(s) {
                return parsed;
            }
        }
        serde_json::Value::String(s.to_string())
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        serde_json::json!(a.value(row))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        serde_json::json!(a.value(row))
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        // DataFusion scalar functions like length()/abs() can yield Int32;
        // render as a JSON number so e.g. `length(description)` is `200`, not a
        // debug-formatted array string.
        serde_json::json!(a.value(row))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        serde_json::json!(a.value(row))
    } else {
        serde_json::Value::String(format!("{:?}", col))
    }
}

/// Render a vector of record batches into row objects + column names.
pub fn batches_to_rows(
    batches: &[RecordBatch],
) -> (Vec<serde_json::Map<String, serde_json::Value>>, Vec<String>) {
    let columns: Vec<String> = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        vec![]
    };

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = arrow_col_to_json(col, row_idx, field.name());
                obj.insert(field.name().clone(), val);
            }
            rows.push(obj);
        }
    }
    (rows, columns)
}
