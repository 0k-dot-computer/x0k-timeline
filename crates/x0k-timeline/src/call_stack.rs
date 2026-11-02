//! The `query_call_stack` span-tree walk — relocated out of `x0k-dev-daemon`.
//!
//! `query_call_stack` navigates the structural span hierarchy (`parent_span_id`)
//! and the semantic activity-causality edges (`parent_id`) to answer
//! "what led to this error?" (ancestors) and "what did this call?" (descendants).
//! In the dev-daemon it lived in `daemon/handlers/observability.rs`, entangled
//! with the daemon-internal `TimelineEntryJson` / `ActivityRecord` and dispatched
//! through the IPC `bridge`. DRIVER-1 moves it here so it reads the SAME parquet
//! store + shared WAL the rest of the engine reads, with no dev-daemon link — the
//! span-tree view keeps answering when the dev daemon is crash-looping.
//!
//! The types below are the self-contained equivalents of the dev-daemon protocol
//! types (`QueryCallStack`, `QueryCallStackResult`, `CallStackNode`,
//! `TimelineEntryJson`). Field names + the parent-resolution rules
//! (`span` → `parent_span_id`, `log`/`activity` → `span_id`/`parent_id`) ride
//! byte-for-byte so the call-stack JSON is identical to the pre-move surface.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use schemars::JsonSchema;

use crate::engine::TimelineEngine;

/// Default true for `include_logs`.
fn default_true() -> bool {
    true
}

/// A timeline row as seen by the call-stack walk. The byte-for-byte equivalent
/// of the dev-daemon `TimelineEntryJson`, carrying both the structural span
/// parentage (`parent_span_id`) and the semantic activity parentage
/// (`parent_id` / `parent_activity_id`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TimelineEntryJson {
    pub id: String,
    #[serde(default)]
    pub entity_uri: Option<String>,
    pub timestamp: u64,
    #[serde(default)]
    pub duration_micros: Option<u64>,
    pub source: String,
    pub event_type: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub target_type: Option<String>,
    #[serde(default)]
    pub workstream_id: Option<String>,
    #[serde(default)]
    pub module_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
    /// Parent entry ID for generic causal chains. Span rows resolve to
    /// `parent_span_id`; log/activity rows resolve to their containing span.
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub parent_activity_id: Option<String>,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub parent_request_id: Option<String>,
    #[serde(default)]
    pub metadata: Option<String>,
    #[serde(default)]
    pub source_file: Option<String>,
    #[serde(default)]
    pub source_line: Option<u32>,
    #[serde(default)]
    pub source_function: Option<String>,
    #[serde(default)]
    pub source_module: Option<String>,
    #[serde(default)]
    pub span_name: Option<String>,
    #[serde(default)]
    pub cause_hash: Option<String>,
}

/// `query_call_stack` request parameters (self-contained equivalent of the
/// dev-daemon `QueryCallStackWithContext` flattened with its inner
/// `QueryCallStack`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QueryCallStack {
    #[schemars(
        description = "Trace entry ID to focus on. Span rows center the tree directly; activity/log rows center it on their containing span."
    )]
    #[serde(default)]
    pub focus_span_id: Option<String>,
    #[schemars(
        description = "W3C Trace ID to query. Auto-discovered if focus_span_id is provided."
    )]
    #[serde(default)]
    pub trace_id: Option<String>,
    #[schemars(
        description = "Direction: 'ancestors' (backtrace), 'descendants' (default), 'both', or 'full'"
    )]
    #[serde(default)]
    pub direction: Option<String>,
    #[schemars(description = "Optional workstream ID to filter by")]
    #[serde(default)]
    pub workstream_id: Option<String>,
    #[schemars(description = "Maximum depth to traverse. Default: 10")]
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[schemars(description = "Event type prefixes to include (e.g., ['agent.', 'task.'])")]
    #[serde(default)]
    pub event_types: Vec<String>,
    #[schemars(description = "Source location filters (e.g., ['sync.rs', 'daemon.rs:42'])")]
    #[serde(default)]
    pub source_filters: Vec<String>,
    #[schemars(
        description = "Include span-attached non-span entries (logs, activity milestones). Default: true"
    )]
    #[serde(default = "default_true")]
    pub include_logs: bool,
    #[schemars(description = "Include sibling entries at each level. Default: false")]
    #[serde(default)]
    pub include_siblings: bool,
    /// MCP-only: store the result in the context registry for rlm_analyze. The
    /// Query Service does not own that registry, so this is accepted for surface
    /// compatibility but is a no-op here (context_id stays None).
    #[schemars(
        description = "If true, load the result into the context store and return context_id"
    )]
    #[serde(default)]
    pub load_to_context: Option<bool>,
}

/// A node in the call-stack tree (byte-for-byte equivalent of the dev-daemon
/// `CallStackNode`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct CallStackNode {
    pub span_id: String,
    #[serde(default)]
    pub span_name: Option<String>,
    #[serde(default)]
    pub source_location: Option<String>,
    pub start_time: u64,
    #[serde(default)]
    pub end_time: Option<u64>,
    #[serde(default)]
    pub duration_micros: Option<u64>,
    pub event_type: String,
    #[serde(default)]
    pub logs: Vec<TimelineEntryJson>,
    #[serde(default)]
    pub children: Vec<CallStackNode>,
    pub depth: usize,
}

/// `query_call_stack` result. The dev-daemon flattened a `context_id` onto this;
/// we keep the same flattened shape so the MCP JSON is identical.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QueryCallStackResult {
    #[serde(default)]
    pub focus_entry: Option<TimelineEntryJson>,
    #[serde(default)]
    pub roots: Vec<CallStackNode>,
    #[serde(default)]
    pub ancestors: Vec<TimelineEntryJson>,
    pub span_count: usize,
    pub log_count: usize,
    #[serde(default)]
    pub duration_micros: Option<u64>,
    #[serde(default)]
    pub trace_id: Option<String>,
    /// Context ID when load_to_context was true. Always None in the Query Service
    /// (it does not own the dev-daemon context registry).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
}

impl TimelineEngine {
    /// Navigate the span hierarchy. Direct port of the dev-daemon
    /// `ToolHandlers::query_call_stack` over this engine's store.
    pub async fn query_call_stack(
        &self,
        req: &QueryCallStack,
    ) -> Result<QueryCallStackResult, String> {
        let direction = req.direction.as_deref().unwrap_or("descendants");
        let max_depth = req.max_depth.unwrap_or(10);

        // focus_span_id → look up the trace entry and auto-discover trace_id.
        let (focus_entry, trace_id) = if let Some(ref focus_id) = req.focus_span_id {
            let entry = self
                .find_call_stack_entry_by_id(focus_id, req.workstream_id.as_deref())
                .await;
            let trace_id = entry
                .as_ref()
                .and_then(|e| e.trace_id.clone())
                .or_else(|| req.trace_id.clone());
            (entry, trace_id)
        } else {
            (None, req.trace_id.clone())
        };

        // "ancestors"/"both" → walk up the parent chain.
        let ancestors = if direction == "ancestors" || direction == "both" {
            if let Some(ref focus) = focus_entry {
                self.walk_ancestors(focus, req.workstream_id.as_deref(), max_depth)
                    .await
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // "descendants"/"both"/"full" → build the tree.
        let (roots, span_count, log_count, all_entries) =
            if direction == "descendants" || direction == "both" || direction == "full" {
                if let Some(ref tid) = trace_id {
                    let entries = self
                        .query_call_stack_entries(
                            tid,
                            req.workstream_id.as_deref(),
                            &req.event_types,
                            1_000,
                        )
                        .await?;

                    let filtered_entries: Vec<TimelineEntryJson> = if req.source_filters.is_empty()
                    {
                        entries
                    } else {
                        entries
                            .into_iter()
                            .filter(|e| {
                                if let Some(ref src_mod) = e.source_module {
                                    req.source_filters.iter().any(|f| src_mod.contains(f))
                                } else {
                                    true
                                }
                            })
                            .collect()
                    };

                    let root_span_id = if direction == "full" {
                        None
                    } else if let Some(ref focus) = focus_entry {
                        if focus.span_name.is_some() {
                            Some(focus.id.as_str())
                        } else {
                            focus.parent_id.as_deref()
                        }
                    } else {
                        None
                    };

                    let (roots, span_count, log_count) = build_call_stack_tree(
                        &filtered_entries,
                        root_span_id,
                        max_depth,
                        req.include_logs,
                    );

                    (roots, span_count, log_count, filtered_entries)
                } else {
                    (Vec::new(), 0, 0, Vec::new())
                }
            } else {
                (Vec::new(), 0, 0, Vec::new())
            };

        let duration_micros = if !all_entries.is_empty() {
            let first_ts = all_entries.first().map(|e| e.timestamp).unwrap_or(0);
            let last_ts = all_entries.last().map(|e| e.timestamp).unwrap_or(0);
            Some(last_ts.saturating_sub(first_ts))
        } else if !ancestors.is_empty() {
            let first_ts = ancestors.first().map(|e| e.timestamp).unwrap_or(0);
            let last_ts = ancestors.last().map(|e| e.timestamp).unwrap_or(0);
            Some(last_ts.saturating_sub(first_ts))
        } else {
            None
        };

        Ok(QueryCallStackResult {
            focus_entry,
            roots,
            ancestors,
            span_count,
            log_count,
            duration_micros,
            trace_id,
            context_id: None,
        })
    }

    /// Find a single call-stack entry by id.
    async fn find_call_stack_entry_by_id(
        &self,
        entry_id: &str,
        workstream_id: Option<&str>,
    ) -> Option<TimelineEntryJson> {
        let safe_id = entry_id.replace('\'', "''");
        let mut conditions = vec![
            format!("id = '{}'", safe_id),
            "entry_kind IN ('span', 'log', 'activity')".to_string(),
        ];
        if let Some(ws) = workstream_id {
            conditions.push(format!("workstream_id = '{}'", ws.replace('\'', "''")));
        }

        self.query_call_stack_rows(&conditions, 1)
            .await
            .ok()
            .and_then(|mut rows| rows.pop())
    }

    /// Query the trace-local entries that participate in call-stack views.
    async fn query_call_stack_entries(
        &self,
        trace_id: &str,
        workstream_id: Option<&str>,
        event_types: &[String],
        limit: usize,
    ) -> Result<Vec<TimelineEntryJson>, String> {
        let mut conditions = vec![
            format!("trace_id = '{}'", trace_id.replace('\'', "''")),
            "entry_kind IN ('span', 'log', 'activity')".to_string(),
        ];
        if let Some(ws) = workstream_id {
            conditions.push(format!("workstream_id = '{}'", ws.replace('\'', "''")));
        }
        if !event_types.is_empty() {
            let type_conds: Vec<String> = event_types
                .iter()
                .map(|t| format!("event_type LIKE '{}%'", t.replace('\'', "''")))
                .collect();
            conditions.push(format!(
                "(entry_kind = 'span' OR {})",
                type_conds.join(" OR ")
            ));
        }

        self.query_call_stack_rows(&conditions, limit).await
    }

    /// Walk up the parent chain from a trace entry (root-to-focus order).
    async fn walk_ancestors(
        &self,
        start: &TimelineEntryJson,
        workstream_id: Option<&str>,
        max_depth: usize,
    ) -> Vec<TimelineEntryJson> {
        let mut ancestors = Vec::new();
        let mut current_parent_id = start.parent_id.clone();
        let lookup_ws = start.workstream_id.as_deref().or(workstream_id);

        while let Some(parent_id) = current_parent_id {
            if ancestors.len() >= max_depth {
                break;
            }
            if let Some(parent_entry) = self
                .find_call_stack_entry_by_id(&parent_id, lookup_ws)
                .await
            {
                current_parent_id = parent_entry.parent_id.clone();
                ancestors.push(parent_entry);
            } else {
                break;
            }
        }

        ancestors.reverse();
        ancestors
    }

    /// Run the call-stack projection SQL against the store + shared WAL and
    /// decode rows into [`TimelineEntryJson`]. Byte-for-byte port of the
    /// dev-daemon `query_call_stack_rows`.
    async fn query_call_stack_rows(
        &self,
        conditions: &[String],
        limit: usize,
    ) -> Result<Vec<TimelineEntryJson>, String> {
        use arrow::array::{Array, Int64Array, StringArray, UInt32Array};

        let store = match self.get_store() {
            Ok(store) => store,
            // No store yet (fresh node, writer not up) → empty, never error: the
            // decoupling guarantee is that this answers regardless of the writer.
            Err(_) => return Ok(Vec::new()),
        };

        let sql = format!(
            "SELECT id, entry_kind, event_type, timestamp_us, duration_ms, actor, trace_id, \
             span_id, parent_span_id, software_module_id, workstream_id, correlation_id, \
             parent_id, request_id, parent_request_id, description, metadata_json, fields_json, \
             source_file, source_line, module, span_name \
             FROM timeline WHERE {} ORDER BY timestamp_us ASC LIMIT {}",
            conditions.join(" AND "),
            limit,
        );

        let wal_dir = store.dir().join("wal");
        let pending = x0k_activity_sink::wal::read_all_wal_files(&wal_dir);
        let bytes = match store.query_with_pending(&sql, &pending).await {
            Ok(bytes) if !bytes.is_empty() => bytes,
            Ok(_) => return Ok(Vec::new()),
            Err(e) => {
                tracing::debug!(error = %e, "Call-stack Parquet query failed");
                return Ok(Vec::new());
            }
        };

        let batches = x0k_parquet::reader::ParquetReader::from_ipc_bytes(&bytes)
            .map_err(|e| format!("Failed to parse call-stack results: {}", e))?;
        let mut entries = Vec::new();

        for batch in &batches {
            let n = batch.num_rows();
            let col = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            };
            let i64_col = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            };
            let u32_col = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
            };
            let str_val = |c: Option<&StringArray>, i: usize| -> Option<String> {
                c.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_string())
                    }
                })
            };

            let id_c = col("id");
            let kind_c = col("entry_kind");
            let event_type_c = col("event_type");
            let ts_c = i64_col("timestamp_us");
            let dur_c = i64_col("duration_ms");
            let actor_c = col("actor");
            let trace_c = col("trace_id");
            let span_c = col("span_id");
            let parent_span_c = col("parent_span_id");
            let mod_c = col("software_module_id");
            let ws_c = col("workstream_id");
            let corr_c = col("correlation_id");
            let parent_c = col("parent_id");
            let request_c = col("request_id");
            let parent_request_c = col("parent_request_id");
            let desc_c = col("description");
            let metadata_c = col("metadata_json");
            let fields_c = col("fields_json");
            let sf_c = col("source_file");
            let sl_c = u32_col("source_line");
            let sm_c = col("module");
            let span_name_c = col("span_name");

            for i in 0..n {
                let id = match str_val(id_c, i) {
                    Some(id) => id,
                    None => continue,
                };
                let entry_kind = str_val(kind_c, i).unwrap_or_default();
                let span_id = str_val(span_c, i);
                if entry_kind == "activity" && span_id.is_none() {
                    continue;
                }
                let parent_span_id = str_val(parent_span_c, i);
                let parent_activity_id = str_val(parent_c, i);
                let parent_id = match entry_kind.as_str() {
                    "span" => parent_span_id.clone(),
                    "log" => span_id.clone(),
                    "activity" => span_id.clone().or_else(|| parent_activity_id.clone()),
                    _ => None,
                };
                let metadata = match entry_kind.as_str() {
                    "span" | "log" => str_val(fields_c, i),
                    "activity" => str_val(metadata_c, i),
                    _ => str_val(metadata_c, i).or_else(|| str_val(fields_c, i)),
                };

                entries.push(TimelineEntryJson {
                    id,
                    entity_uri: None,
                    timestamp: ts_c
                        .and_then(|c| {
                            if c.is_null(i) {
                                None
                            } else {
                                Some(c.value(i) as u64)
                            }
                        })
                        .unwrap_or(0),
                    duration_micros: dur_c.and_then(|c| {
                        if c.is_null(i) {
                            None
                        } else {
                            Some(c.value(i) as u64 * 1000)
                        }
                    }),
                    source: "timeline".to_string(),
                    event_type: str_val(event_type_c, i)
                        .unwrap_or_else(|| format!("timeline.{}", entry_kind)),
                    description: str_val(desc_c, i),
                    actor: str_val(actor_c, i),
                    target: None,
                    target_type: None,
                    workstream_id: str_val(ws_c, i),
                    module_id: str_val(mod_c, i),
                    correlation_id: str_val(corr_c, i),
                    trace_id: str_val(trace_c, i),
                    parent_id,
                    parent_activity_id,
                    parent_span_id,
                    request_id: str_val(request_c, i),
                    parent_request_id: str_val(parent_request_c, i),
                    metadata,
                    source_file: str_val(sf_c, i),
                    source_line: sl_c
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                    source_function: None,
                    source_module: str_val(sm_c, i),
                    span_name: if entry_kind == "span" {
                        str_val(span_name_c, i)
                            .or_else(|| str_val(event_type_c, i))
                            .or_else(|| span_id.clone())
                    } else {
                        None
                    },
                    cause_hash: None,
                });
            }
        }

        Ok(entries)
    }
}

/// Build a call-stack tree from timeline entries. Verbatim port of the
/// dev-daemon `build_call_stack_tree`.
fn build_call_stack_tree(
    entries: &[TimelineEntryJson],
    root_span_id: Option<&str>,
    max_depth: usize,
    include_logs: bool,
) -> (Vec<CallStackNode>, usize, usize) {
    let mut spans: HashMap<String, &TimelineEntryJson> = HashMap::new();
    let mut logs_by_parent: HashMap<String, Vec<&TimelineEntryJson>> = HashMap::new();
    let mut children_by_parent: HashMap<String, Vec<String>> = HashMap::new();

    for entry in entries {
        if entry.span_name.is_some() {
            spans.insert(entry.id.clone(), entry);
            if let Some(ref parent_id) = entry.parent_id {
                children_by_parent
                    .entry(parent_id.clone())
                    .or_default()
                    .push(entry.id.clone());
            }
        } else if let Some(ref parent_id) = entry.parent_id {
            logs_by_parent
                .entry(parent_id.clone())
                .or_default()
                .push(entry);
        }
    }

    let span_count = spans.len();
    let log_count: usize = logs_by_parent.values().map(|v| v.len()).sum();

    let root_span_ids: Vec<String> = if let Some(root_id) = root_span_id {
        if spans.contains_key(root_id) {
            vec![root_id.to_string()]
        } else {
            Vec::new()
        }
    } else {
        spans
            .keys()
            .filter(|span_id| {
                if let Some(entry) = spans.get(*span_id) {
                    match &entry.parent_id {
                        None => true,
                        Some(parent_id) => !spans.contains_key(parent_id),
                    }
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    };

    let mut roots: Vec<CallStackNode> = root_span_ids
        .iter()
        .filter_map(|span_id| {
            spans.get(span_id).map(|entry| {
                build_call_stack_node(
                    span_id,
                    entry,
                    &spans,
                    &children_by_parent,
                    &logs_by_parent,
                    0,
                    max_depth,
                    include_logs,
                )
            })
        })
        .collect();

    roots.sort_by_key(|n| n.start_time);

    (roots, span_count, log_count)
}

/// Recursively build a [`CallStackNode`]. Verbatim port of the dev-daemon
/// `build_call_stack_node`.
#[allow(clippy::too_many_arguments)]
fn build_call_stack_node(
    span_id: &str,
    entry: &TimelineEntryJson,
    spans: &HashMap<String, &TimelineEntryJson>,
    children_by_parent: &HashMap<String, Vec<String>>,
    logs_by_parent: &HashMap<String, Vec<&TimelineEntryJson>>,
    depth: usize,
    max_depth: usize,
    include_logs: bool,
) -> CallStackNode {
    let logs: Vec<TimelineEntryJson> = if include_logs && depth < max_depth {
        logs_by_parent
            .get(span_id)
            .map(|log_entries| {
                let mut logs: Vec<TimelineEntryJson> =
                    log_entries.iter().map(|e| (*e).clone()).collect();
                logs.sort_by_key(|e| e.timestamp);
                logs
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let children: Vec<CallStackNode> = if depth < max_depth {
        children_by_parent
            .get(span_id)
            .map(|child_ids| {
                child_ids
                    .iter()
                    .filter_map(|child_id| {
                        spans.get(child_id).map(|child_entry| {
                            build_call_stack_node(
                                child_id,
                                child_entry,
                                spans,
                                children_by_parent,
                                logs_by_parent,
                                depth + 1,
                                max_depth,
                                include_logs,
                            )
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let source_location = match (&entry.source_file, &entry.source_line) {
        (Some(file), Some(line)) => Some(format!("{}:{}", file, line)),
        (Some(file), None) => Some(file.clone()),
        _ => None,
    };

    let duration_micros = entry.duration_micros;
    let end_time = entry.duration_micros.map(|d| entry.timestamp + d);

    CallStackNode {
        span_id: span_id.to_string(),
        span_name: entry.span_name.clone(),
        source_location,
        start_time: entry.timestamp,
        end_time,
        duration_micros,
        event_type: entry.event_type.clone(),
        logs,
        children,
        depth,
    }
}
