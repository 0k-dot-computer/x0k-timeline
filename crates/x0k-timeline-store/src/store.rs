//! High-level Parquet store combining writer and reader

use crate::reader::ParquetReader;
use crate::udfs::register_timeline_udfs;
use crate::writer::{CompactionResult, ParquetWriter};
use anyhow::Context;
use arrow::array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use parquet::file::properties::WriterProperties;
use sqlparser::ast::{
    BinaryOperator, Expr, LimitClause, OrderByKind, Select, SelectItem, Statement, TableFactor,
    Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const QUERY_TARGET_PARTITIONS: usize = 4;
const QUERY_META_FETCH_CONCURRENCY: usize = 4;

/// Result of appending a batch
pub struct AppendResult {
    /// Number of rows written
    pub rows_written: usize,
    /// Filename of the Parquet file created
    pub filename: String,
}

/// Parameters for one bounded Parquet compaction pass.
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// Do not compact unless at least this many small files share a tier prefix.
    pub min_input_files: usize,
    /// Maximum number of files to replace in one pass.
    pub max_input_files: usize,
    /// Stop adding files to a pass once their manifest row counts reach this.
    pub target_rows: usize,
    /// Avoid compacting very recent files that may still be in active debug use.
    pub min_file_age: Duration,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            min_input_files: 32,
            max_input_files: 1024,
            target_rows: 100_000,
            min_file_age: Duration::from_secs(5 * 60),
        }
    }
}

/// Thread-safe Parquet store combining write and query operations
///
/// The writer needs no lock — each `write_batch` produces a uniquely-named file
/// and the manifest has its own internal Mutex for append serialization.
/// The session context is lazily created on first query and reused; ListingTable
/// auto-discovers new Parquet files in the directory.
///
/// Statistics collection is disabled on the listing table to avoid expensive
/// per-row-group column metadata parsing on every query. The incremental cursor
/// approach used by callers makes row-group pruning unnecessary.
pub struct ParquetStore {
    writer: ParquetWriter,
    /// Schema is set by the first write and cached for session construction
    schema: RwLock<Option<SchemaRef>>,
    /// Cached DataFusion session with the directory registered as a listing table.
    /// Created lazily on first query. ListingTable auto-discovers new files.
    session: RwLock<Option<SessionContext>>,
    dir: PathBuf,
    table_name: String,
    /// Monotonically increasing counter bumped on each write.
    /// Callers can use `generation()` + `has_new_data_since()` to skip
    /// queries when no new files have been written.
    write_generation: AtomicU64,
}

impl ParquetStore {
    /// Create a new Parquet store at the given directory
    ///
    /// The table name defaults to "data" for SQL queries.
    pub fn new(dir: &Path) -> anyhow::Result<Self> {
        Self::with_table_name(dir, "data")
    }

    /// Create a new Parquet store with a custom table name
    pub fn with_table_name(dir: &Path, table_name: &str) -> anyhow::Result<Self> {
        let writer = ParquetWriter::new(dir)?;
        Ok(Self {
            writer,
            schema: RwLock::new(None),
            session: RwLock::new(None),
            dir: dir.to_path_buf(),
            table_name: table_name.to_string(),
            write_generation: AtomicU64::new(0),
        })
    }

    /// Append a batch from Arrow IPC bytes
    ///
    /// Decodes the IPC bytes into RecordBatches and writes each as a Parquet file.
    /// The schema is captured from the first write for subsequent query operations.
    pub async fn append_batch(&self, ipc_bytes: &[u8]) -> anyhow::Result<AppendResult> {
        self.append_batch_inner(ipc_bytes, None).await
    }

    /// Append a batch with custom writer properties (e.g. bloom filters)
    pub async fn append_batch_with_props(
        &self,
        ipc_bytes: &[u8],
        props: WriterProperties,
    ) -> anyhow::Result<AppendResult> {
        self.append_batch_inner(ipc_bytes, Some(props)).await
    }

    /// Synchronous version of `append_batch` for use from sync contexts (e.g. SpanSink::write_span)
    pub fn append_batch_sync(&self, ipc_bytes: &[u8]) -> anyhow::Result<AppendResult> {
        let batches = ParquetReader::from_ipc_bytes(ipc_bytes)?;
        if batches.is_empty() {
            anyhow::bail!("No record batches in IPC data");
        }

        {
            let mut schema = self.schema.blocking_write();
            if schema.is_none() {
                *schema = Some(batches[0].schema());
            }
        }

        let mut total_rows = 0;
        let mut last_filename = String::new();

        for batch in &batches {
            let info = self.writer.write_batch(batch, None)?;
            total_rows += info.row_count;
            last_filename = info.filename;
        }

        self.write_generation.fetch_add(1, Ordering::Release);

        // Invalidate the cached query session (see `append_batch_inner` for
        // the rationale). Sync writers (SpanSink) interleave with async
        // readers from the same store and need the same invalidation.
        if let Ok(mut cached) = self.session.try_write() {
            *cached = None;
        }

        Ok(AppendResult {
            rows_written: total_rows,
            filename: last_filename,
        })
    }

    /// Current write generation counter.
    ///
    /// Incremented after every successful write. Callers can snapshot this value
    /// and later compare with `has_new_data_since()` to avoid redundant queries
    /// when no new files have been written.
    pub fn generation(&self) -> u64 {
        self.write_generation.load(Ordering::Acquire)
    }

    /// Returns true if new data has been written since the given generation.
    pub fn has_new_data_since(&self, since: u64) -> bool {
        self.generation() > since
    }

    /// Query the store with SQL, returning Arrow IPC bytes
    ///
    /// The `SessionContext` is rebuilt on every call. Caching it across
    /// queries caused `ListingTable` to serve a stale snapshot of the
    /// directory: queries for attributes written after the session was
    /// first cached returned zero rows even when the data was physically
    /// present on disk. Rebuilding is cheap relative to the query itself
    /// and keeps small, read-heavy stores (like the UI-control registry)
    /// consistent with the filesystem.
    pub async fn query(&self, sql: &str) -> anyhow::Result<Vec<u8>> {
        match self.query_once(sql).await {
            Ok(bytes) => Ok(bytes),
            Err(err) if is_missing_parquet_file_error(&err) => {
                tracing::warn!(
                    error = %err,
                    "Refreshing cached Parquet query session after stale file reference"
                );
                self.clear_session().await;
                self.query_once(sql)
                    .await
                    .with_context(|| "query failed after refreshing stale Parquet session")
            }
            Err(err) => Err(err),
        }
    }

    async fn query_once(&self, sql: &str) -> anyhow::Result<Vec<u8>> {
        let ctx = match self.get_session().await? {
            Some(ctx) => ctx,
            None => return Ok(Vec::new()),
        };

        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        ParquetReader::to_ipc_bytes(&batches)
    }

    /// Query the store with parameterized SQL, returning Arrow IPC bytes
    ///
    /// Use `$1`, `$2`, ... as placeholders and pass corresponding params.
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<ScalarValue>,
    ) -> anyhow::Result<Vec<u8>> {
        match self.query_with_params_once(sql, params.clone()).await {
            Ok(bytes) => Ok(bytes),
            Err(err) if is_missing_parquet_file_error(&err) => {
                tracing::warn!(
                    error = %err,
                    "Refreshing cached Parquet query session after stale file reference"
                );
                self.clear_session().await;
                self.query_with_params_once(sql, params)
                    .await
                    .with_context(|| {
                        "parameterized query failed after refreshing stale Parquet session"
                    })
            }
            Err(err) => Err(err),
        }
    }

    async fn query_with_params_once(
        &self,
        sql: &str,
        params: Vec<ScalarValue>,
    ) -> anyhow::Result<Vec<u8>> {
        let ctx = match self.get_session().await? {
            Some(ctx) => ctx,
            None => return Ok(Vec::new()),
        };

        let df = ctx.sql(sql).await?.with_param_values(params)?;
        let batches = df.collect().await?;
        ParquetReader::to_ipc_bytes(&batches)
    }

    /// Query the store with SQL, including pending (unflushed) RecordBatches.
    ///
    /// Creates a temporary session over a manifest snapshot plus any pending data.
    /// Even with no pending batches, this path is used by timeline callers so
    /// they get stale-manifest protection and query pruning instead of scanning
    /// every shard through the cached directory listing.
    pub async fn query_with_pending(
        &self,
        sql: &str,
        pending: &[RecordBatch],
    ) -> anyhow::Result<Vec<u8>> {
        match self.query_with_pending_once(sql, pending).await {
            Ok(bytes) => Ok(bytes),
            Err(err) if is_missing_parquet_file_error(&err) => {
                tracing::warn!(
                    error = %err,
                    "Retrying Parquet query after stale file reference"
                );
                self.clear_session().await;
                self.query_with_pending_once(sql, pending)
                    .await
                    .with_context(|| {
                        "query with pending batches failed after refreshing stale Parquet session"
                    })
            }
            Err(err) => Err(err),
        }
    }

    async fn query_with_pending_once(
        &self,
        sql: &str,
        pending: &[RecordBatch],
    ) -> anyhow::Result<Vec<u8>> {
        let Some(schema) = self.query_schema(pending).await? else {
            return Ok(Vec::new());
        };

        let _manifest_read_lock = self.writer.lock_manifest_shared_for_query()?;
        let manifest_entries = self.writer.list_files_unlocked()?;
        let fallback_to_directory = manifest_entries.is_empty();
        let prune_plan = QueryPrunePlan::analyze(sql, &self.table_name);

        if prune_plan.can_iterate_recent_desc() {
            return self
                .query_recent_desc_snapshot(sql, pending, schema, manifest_entries, &prune_plan)
                .await;
        }

        let manifest_entries = prune_plan.apply_static_filters(manifest_entries);
        self.query_snapshot_with_entries(
            sql,
            pending,
            schema,
            manifest_entries,
            fallback_to_directory,
        )
        .await
    }

    async fn query_schema(&self, pending: &[RecordBatch]) -> anyhow::Result<Option<SchemaRef>> {
        {
            let schema = self.schema.read().await;
            if let Some(schema) = &*schema {
                return Ok(Some(schema.clone()));
            }
        }

        if let Some(batch) = pending.first() {
            let schema = batch.schema();
            let mut cached = self.schema.write().await;
            if cached.is_none() {
                *cached = Some(schema.clone());
            }
            return Ok(Some(schema));
        }

        let Some(schema) = self.infer_schema()? else {
            return Ok(None);
        };
        let mut cached = self.schema.write().await;
        if cached.is_none() {
            *cached = Some(schema.clone());
        }
        Ok(Some(schema))
    }

    async fn query_recent_desc_snapshot(
        &self,
        sql: &str,
        pending: &[RecordBatch],
        schema: SchemaRef,
        manifest_entries: Vec<crate::manifest::ParquetFileInfo>,
        prune_plan: &QueryPrunePlan,
    ) -> anyhow::Result<Vec<u8>> {
        let mut manifest_entries = prune_plan.apply_static_filters(manifest_entries);
        manifest_entries.sort_by_key(|entry| std::cmp::Reverse(entry.timestamp_micros));

        if manifest_entries.is_empty() {
            return self
                .query_snapshot_with_entries(sql, pending, schema, manifest_entries, false)
                .await;
        }

        let mut candidate_count = prune_plan
            .initial_recent_candidate_count()
            .min(manifest_entries.len());

        loop {
            let candidates = manifest_entries[..candidate_count].to_vec();
            let bytes = self
                .query_snapshot_with_entries(sql, pending, schema.clone(), candidates, false)
                .await?;

            if candidate_count >= manifest_entries.len()
                || prune_plan.recent_result_is_complete(
                    &bytes,
                    manifest_entries[candidate_count].timestamp_micros,
                )
            {
                return Ok(bytes);
            }

            let next = (candidate_count * 2).min(manifest_entries.len());
            if next == candidate_count {
                return Ok(bytes);
            }
            candidate_count = next;
        }
    }

    async fn query_snapshot_with_entries(
        &self,
        sql: &str,
        pending: &[RecordBatch],
        schema: SchemaRef,
        manifest_entries: Vec<crate::manifest::ParquetFileInfo>,
        fallback_to_directory_if_manifest_empty: bool,
    ) -> anyhow::Result<Vec<u8>> {
        let table_paths = self.existing_parquet_table_paths_from_entries(
            manifest_entries,
            fallback_to_directory_if_manifest_empty,
        )?;
        self.query_snapshot_with_paths(sql, pending, schema, table_paths)
            .await
    }

    async fn query_snapshot_with_paths(
        &self,
        sql: &str,
        pending: &[RecordBatch],
        schema: SchemaRef,
        table_paths: Vec<ListingTableUrl>,
    ) -> anyhow::Result<Vec<u8>> {
        if table_paths.is_empty() && pending.is_empty() {
            return Ok(Vec::new());
        }

        let ctx = new_query_context();

        match (table_paths.is_empty(), pending.is_empty()) {
            (false, true) => {
                let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet")
                    .with_collect_stat(false);
                let config = ListingTableConfig::new_with_multi_paths(table_paths)
                    .with_listing_options(listing_options)
                    .with_schema(schema);
                let table = ListingTable::try_new(config)?;
                ctx.register_table(&self.table_name, Arc::new(table))?;
            }
            (false, false) => {
                let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet")
                    .with_collect_stat(false);
                let config = ListingTableConfig::new_with_multi_paths(table_paths)
                    .with_listing_options(listing_options)
                    .with_schema(schema);
                let table = ListingTable::try_new(config)?;
                ctx.register_table("__parquet", Arc::new(table))?;

                let mem_table = MemTable::try_new(pending[0].schema(), vec![pending.to_vec()])?;
                ctx.register_table("__pending", Arc::new(mem_table))?;

                ctx.sql(&format!(
                    "CREATE VIEW {} AS SELECT * FROM __parquet UNION SELECT * FROM __pending",
                    self.table_name
                ))
                .await?;
            }
            (true, false) => {
                let mem_table = MemTable::try_new(pending[0].schema(), vec![pending.to_vec()])?;
                ctx.register_table(&self.table_name, Arc::new(mem_table))?;
            }
            (true, true) => unreachable!("empty snapshot returned above"),
        }

        let df = ctx.sql(sql).await?;
        let batches = df.collect().await?;
        ParquetReader::to_ipc_bytes(&batches)
    }

    /// Get the directory this store writes to.
    /// The files a query reads, newest last.
    pub fn file_paths(&self) -> anyhow::Result<Vec<PathBuf>> {
        let _manifest_read_lock = self.writer.lock_manifest_shared_for_query()?;
        let entries = self.writer.list_files_unlocked()?;
        Ok(entries.iter().map(|entry| self.dir.join(&entry.filename)).collect())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append a batch with custom writer properties and a filename prefix.
    ///
    /// The prefix is prepended to the filename for tiered retention (e.g. "span_", "log_").
    pub async fn append_batch_with_prefix(
        &self,
        ipc_bytes: &[u8],
        props: WriterProperties,
        prefix: &str,
    ) -> anyhow::Result<AppendResult> {
        let batches = ParquetReader::from_ipc_bytes(ipc_bytes)?;
        if batches.is_empty() {
            anyhow::bail!("No record batches in IPC data");
        }

        {
            let mut schema = self.schema.write().await;
            if schema.is_none() {
                *schema = Some(batches[0].schema());
            }
        }

        let mut total_rows = 0;
        let mut last_filename = String::new();

        for batch in &batches {
            let info = self
                .writer
                .write_batch_with_prefix(batch, Some(props.clone()), prefix)?;
            total_rows += info.row_count;
            last_filename = info.filename;
        }

        self.write_generation.fetch_add(1, Ordering::Release);

        Ok(AppendResult {
            rows_written: total_rows,
            filename: last_filename,
        })
    }

    /// Delete files older than cutoff, returning number deleted
    pub async fn retain_since(&self, cutoff_micros: u64) -> anyhow::Result<usize> {
        let deleted = self.writer.retain_since(cutoff_micros)?;
        if deleted > 0 {
            self.clear_session().await;
        }
        Ok(deleted)
    }

    /// Delete files using per-prefix cutoffs for tiered retention.
    ///
    /// Each entry in `tier_cutoffs` is `(prefix, cutoff_micros)`.
    /// Returns the number of files deleted.
    pub async fn retain_tiered(&self, tier_cutoffs: &[(String, u64)]) -> anyhow::Result<usize> {
        let deleted = self.writer.retain_tiered(tier_cutoffs)?;
        if deleted > 0 {
            self.clear_session().await;
        }
        Ok(deleted)
    }

    /// Delete `.parquet` files on disk that are not tracked in the manifest.
    ///
    /// Only removes files whose mtime is older than `min_age` to avoid racing
    /// with in-progress writes. Returns the number of files deleted.
    pub async fn cleanup_orphaned_files(&self, min_age: Duration) -> anyhow::Result<usize> {
        let deleted = self.writer.cleanup_orphaned_files(min_age)?;
        if deleted > 0 {
            self.clear_session().await;
        }
        Ok(deleted)
    }

    /// Run one bounded compaction pass over old small files.
    pub async fn compact_once(
        &self,
        options: CompactionOptions,
    ) -> anyhow::Result<CompactionResult> {
        let entries = self.writer.list_files()?;
        let Some(group) = select_compaction_group(&entries, &options) else {
            return Ok(CompactionResult::default());
        };
        let prefix = retention_prefix(&group[0].filename).to_string();
        let result = self.writer.compact_entries(&group, None, &prefix)?;
        if result.input_files > 0 {
            self.write_generation.fetch_add(1, Ordering::Release);
            self.clear_session().await;
        }
        Ok(result)
    }

    // ---- internal helpers ----

    async fn append_batch_inner(
        &self,
        ipc_bytes: &[u8],
        props: Option<WriterProperties>,
    ) -> anyhow::Result<AppendResult> {
        let batches = ParquetReader::from_ipc_bytes(ipc_bytes)?;
        if batches.is_empty() {
            anyhow::bail!("No record batches in IPC data");
        }

        // Cache schema from first batch
        {
            let mut schema = self.schema.write().await;
            if schema.is_none() {
                *schema = Some(batches[0].schema());
            }
        }

        let mut total_rows = 0;
        let mut last_filename = String::new();

        for batch in &batches {
            let info = self.writer.write_batch(batch, props.clone())?;
            total_rows += info.row_count;
            last_filename = info.filename;
        }

        self.write_generation.fetch_add(1, Ordering::Release);

        // Invalidate the cached query session so the next read picks up the
        // new file. Without this, the `ListingTable` snapshot taken when the
        // session was first built keeps serving the older file list, and
        // queries from other clients of the same `ParquetStore` instance
        // (e.g. the daemon-side x0k SDK) miss rows that have just been
        // written. The session is cheap to rebuild — DataFusion 53+ caches
        // parquet metadata natively, so the only real cost is a fresh
        // directory listing — and the alternative (rebuild on every query)
        // is what the `query` doc comment originally claimed but the
        // implementation never delivered.
        self.clear_session().await;

        Ok(AppendResult {
            rows_written: total_rows,
            filename: last_filename,
        })
    }

    /// Get or create a cached SessionContext for querying.
    ///
    /// Returns None if there's no schema yet (no data written/inferred).
    /// The session is created once and reused — ListingTable auto-discovers
    /// new Parquet files in the directory on each query, and datafusion 53+
    /// caches per-file parquet metadata internally.
    async fn get_session(&self) -> anyhow::Result<Option<SessionContext>> {
        // Fast path: session already cached
        {
            let cached = self.session.read().await;
            if cached.is_some() {
                return Ok(cached.clone());
            }
        }

        // Need to create session — get or infer the schema first
        let schema = {
            let s = self.schema.read().await;
            match &*s {
                Some(s) => s.clone(),
                None => {
                    drop(s);
                    let inferred = self.infer_schema()?;
                    match inferred {
                        Some(s) => {
                            let mut schema = self.schema.write().await;
                            *schema = Some(s.clone());
                            s
                        }
                        None => return Ok(None),
                    }
                }
            }
        };

        // Build the session — DataFusion 53+ caches parquet metadata natively
        let ctx = new_query_context();
        let listing_path = self.dir.to_string_lossy().to_string();
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(false);
        let table_path = ListingTableUrl::parse(&listing_path)?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;
        ctx.register_table(&self.table_name, Arc::new(table))?;

        // Cache and return
        let mut cached = self.session.write().await;
        *cached = Some(ctx.clone());
        Ok(Some(ctx))
    }

    async fn clear_session(&self) {
        let mut cached = self.session.write().await;
        *cached = None;
    }

    /// Infer schema from existing Parquet files in the directory
    fn infer_schema(&self) -> anyhow::Result<Option<SchemaRef>> {
        let entries: Vec<_> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
            .collect();

        for entry in entries {
            match std::fs::File::open(entry.path()) {
                Ok(file) => {
                    let builder =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            file,
                        )?;
                    return Ok(Some(builder.schema().clone()));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    continue;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(None)
    }

    fn existing_parquet_table_paths_from_entries(
        &self,
        manifest_entries: Vec<crate::manifest::ParquetFileInfo>,
        fallback_to_directory_if_manifest_empty: bool,
    ) -> anyhow::Result<Vec<ListingTableUrl>> {
        let candidate_paths: Vec<PathBuf> =
            if manifest_entries.is_empty() && fallback_to_directory_if_manifest_empty {
                std::fs::read_dir(&self.dir)?
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                    .collect()
            } else {
                manifest_entries
                    .into_iter()
                    .map(|entry| self.dir.join(entry.filename))
                    .collect()
            };

        let mut skipped_missing = 0usize;
        let mut table_paths = Vec::new();
        for path in candidate_paths {
            match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_file() => {
                    let path = path.to_string_lossy().to_string();
                    table_paths.push(ListingTableUrl::parse(&path)?);
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    skipped_missing += 1;
                }
                Err(err) => return Err(err.into()),
            }
        }

        if skipped_missing > 0 {
            tracing::warn!(
                skipped_missing,
                "Skipped manifest parquet files that no longer exist"
            );
        }

        Ok(table_paths)
    }
}

#[derive(Debug, Clone, Default)]
struct QueryPrunePlan {
    timestamp_lower_bound: Option<u64>,
    recent_desc_limit: Option<usize>,
    projects_timestamp_us: bool,
}

impl QueryPrunePlan {
    fn analyze(sql: &str, table_name: &str) -> Self {
        let dialect = GenericDialect {};
        let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
            return Self::default();
        };
        if statements.len() != 1 {
            return Self::default();
        }

        let Statement::Query(query) = &statements[0] else {
            return Self::default();
        };
        if query.with.is_some() {
            return Self::default();
        }
        let Some(select) = query.body.as_ref().as_select() else {
            return Self::default();
        };
        if !selects_from_table(select, table_name) {
            return Self::default();
        }

        let timestamp_lower_bound = select
            .selection
            .as_ref()
            .and_then(extract_timestamp_lower_bound);
        let recent_desc_limit = if orders_by_timestamp_desc(query.order_by.as_ref()) {
            query.limit_clause.as_ref().and_then(limit_clause_value)
        } else {
            None
        };

        Self {
            timestamp_lower_bound,
            recent_desc_limit,
            projects_timestamp_us: projection_includes_timestamp_us(&select.projection),
        }
    }

    fn can_iterate_recent_desc(&self) -> bool {
        self.recent_desc_limit.is_some() && self.projects_timestamp_us
    }

    fn apply_static_filters(
        &self,
        manifest_entries: Vec<crate::manifest::ParquetFileInfo>,
    ) -> Vec<crate::manifest::ParquetFileInfo> {
        let Some(lower) = self.timestamp_lower_bound else {
            return manifest_entries;
        };
        manifest_entries
            .into_iter()
            .filter(|entry| entry.timestamp_micros >= lower)
            .collect()
    }

    fn initial_recent_candidate_count(&self) -> usize {
        let limit = self.recent_desc_limit.unwrap_or(1).max(1);
        (limit * 16).max(128)
    }

    fn recent_result_is_complete(&self, ipc_bytes: &[u8], next_unscanned_timestamp: u64) -> bool {
        let Some(limit) = self.recent_desc_limit else {
            return false;
        };
        let Ok(batches) = ParquetReader::from_ipc_bytes(ipc_bytes) else {
            return false;
        };

        let mut row_count = 0usize;
        let mut min_result_timestamp: Option<i64> = None;
        for batch in batches {
            row_count += batch.num_rows();
            let Ok(column_idx) = batch.schema().index_of("timestamp_us") else {
                return false;
            };
            let column = batch.column(column_idx);
            if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        let value = values.value(row);
                        min_result_timestamp =
                            Some(min_result_timestamp.map_or(value, |min| min.min(value)));
                    }
                }
            } else if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
                for row in 0..values.len() {
                    if !values.is_null(row) {
                        let value = values.value(row).min(i64::MAX as u64) as i64;
                        min_result_timestamp =
                            Some(min_result_timestamp.map_or(value, |min| min.min(value)));
                    }
                }
            } else {
                return false;
            }
        }

        if row_count < limit {
            return false;
        }
        let Some(min_result_timestamp) = min_result_timestamp else {
            return false;
        };
        (next_unscanned_timestamp.min(i64::MAX as u64) as i64) <= min_result_timestamp
    }
}

fn selects_from_table(select: &Select, table_name: &str) -> bool {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return false;
    }
    match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string().eq_ignore_ascii_case(table_name),
        _ => false,
    }
}

fn projection_includes_timestamp_us(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => true,
        SelectItem::UnnamedExpr(expr) => expr_is_column(expr, "timestamp_us"),
        SelectItem::ExprWithAlias { expr, alias } => {
            expr_is_column(expr, "timestamp_us") && alias.value.eq_ignore_ascii_case("timestamp_us")
        }
    })
}

fn orders_by_timestamp_desc(order_by: Option<&sqlparser::ast::OrderBy>) -> bool {
    let Some(order_by) = order_by else {
        return false;
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    let Some(first) = expressions.first() else {
        return false;
    };
    expr_is_column(&first.expr, "timestamp_us") && first.options.asc == Some(false)
}

fn limit_clause_value(limit_clause: &LimitClause) -> Option<usize> {
    let expr = match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset: _,
            limit_by,
        } if limit_by.is_empty() => limit,
        LimitClause::OffsetCommaLimit { limit, .. } => limit,
        _ => return None,
    };
    numeric_literal(expr)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn extract_timestamp_lower_bound(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Nested(inner) => extract_timestamp_lower_bound(inner),
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            match (
                extract_timestamp_lower_bound(left),
                extract_timestamp_lower_bound(right),
            ) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(bound), None) | (None, Some(bound)) => Some(bound),
                (None, None) => None,
            }
        }
        Expr::BinaryOp { left, op, right } => timestamp_binary_lower_bound(left, op, right),
        Expr::Between {
            expr,
            negated: false,
            low,
            high: _,
        } if expr_is_column(expr, "timestamp_us") => numeric_literal(low),
        _ => None,
    }
}

fn timestamp_binary_lower_bound(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<u64> {
    if expr_is_column(left, "timestamp_us") {
        let value = numeric_literal(right)?;
        match op {
            BinaryOperator::GtEq => Some(value),
            BinaryOperator::Gt => Some(value.saturating_add(1)),
            _ => None,
        }
    } else if expr_is_column(right, "timestamp_us") {
        let value = numeric_literal(left)?;
        match op {
            BinaryOperator::LtEq => Some(value),
            BinaryOperator::Lt => Some(value.saturating_add(1)),
            _ => None,
        }
    } else {
        None
    }
}

fn expr_is_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column_name),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(column_name)),
        Expr::Nested(inner) => expr_is_column(inner, column_name),
        _ => false,
    }
}

fn numeric_literal(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse::<u64>().ok(),
            _ => None,
        },
        Expr::Nested(inner) => numeric_literal(inner),
        _ => None,
    }
}

fn select_compaction_group(
    entries: &[crate::manifest::ParquetFileInfo],
    options: &CompactionOptions,
) -> Option<Vec<crate::manifest::ParquetFileInfo>> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_micros() as u64;
    let newest_eligible = now_micros
        .saturating_sub(options.min_file_age.as_micros().min(u128::from(u64::MAX)) as u64);
    let prefixes = ["span_", "log_", "event_", "record_", ""];
    let mut best: Option<Vec<crate::manifest::ParquetFileInfo>> = None;

    for prefix in prefixes {
        let mut candidates: Vec<_> = entries
            .iter()
            .filter(|entry| retention_prefix(&entry.filename) == prefix)
            .filter(|entry| entry.timestamp_micros <= newest_eligible)
            .filter(|entry| entry.row_count < options.target_rows)
            .cloned()
            .collect();
        candidates.sort_by_key(|entry| entry.timestamp_micros);

        let mut selected = Vec::new();
        let mut rows = 0usize;
        for entry in candidates {
            rows = rows.saturating_add(entry.row_count);
            selected.push(entry);
            if selected.len() >= options.max_input_files
                || (selected.len() >= options.min_input_files && rows >= options.target_rows)
            {
                break;
            }
        }

        if selected.len() >= options.min_input_files {
            let replace = best
                .as_ref()
                .is_none_or(|current| selected.len() > current.len());
            if replace {
                best = Some(selected);
            }
        }
    }

    best
}

fn retention_prefix(filename: &str) -> &str {
    for prefix in ["span_", "log_", "event_", "record_"] {
        if filename.starts_with(prefix) {
            return prefix;
        }
    }
    ""
}

fn new_query_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(QUERY_TARGET_PARTITIONS)
        .set_usize(
            "datafusion.execution.meta_fetch_concurrency",
            QUERY_META_FETCH_CONCURRENCY,
        );
    let ctx = SessionContext::new_with_config(config);
    register_timeline_udfs(&ctx);
    ctx
}

fn is_missing_parquet_file_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let text = cause.to_string();
        let mentions_parquet_file = text.contains("Object at location")
            || text.contains("Failed to fetch metadata for file");
        let missing = text.contains("not found")
            || text.contains("No such file or directory")
            || text.contains("os error 2");
        mentions_parquet_file && missing
    })
}
