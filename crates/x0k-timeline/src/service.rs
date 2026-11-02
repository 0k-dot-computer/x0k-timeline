//! The MCP service surface of the Timeline Query Service.
//!
//! Hosts the pure-timeline query tools that the dev-daemon used to own
//! (`query_timeline_sql` + `describe_timeline`), backed by the shared
//! [`TimelineEngine`] (parquet + shared-WAL + json_extract UDF).
//! Decoupled from x0k-dev-daemon: killing the dev daemon does NOT take timeline
//! query down (the DECOUPLING test).
//!
//! `query_call_stack` and the timeline source of `get_recent_errors` read the
//! same engine; their full relocation out of the dev-daemon IPC handler (which
//! still owns build-state aggregation for `get_recent_errors`) is staged behind
//! the same engine seam.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{Content, IntoContents, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_router, Json, RoleServer, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use x0k_timeline_record::pause as wire;

use crate::call_stack::{QueryCallStack, QueryCallStackResult};
use crate::engine::{
    batches_to_rows, inject_limit, validate_select_only, FamiliesError, TimelineEngine,
};
use crate::schema::{self, DescribeTimelineResult, TimelinePauseResult};
use crate::writer_client;

/// Default cap on rows returned by `query_timeline_sql`.
const DEFAULT_MAX_ROWS: usize = 500;

// =============================================================================
// Tool parameter / result types (parity with x0k-dev-daemon protocol::tools)
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QueryTimelineSql {
    #[schemars(
        description = "SQL query (SELECT only). Table: 'timeline'. Key columns: id, entry_kind, \
                       storage_tier, retention_class, timestamp_us, level, event_type, description, software_module_id, \
                       workstream_id, session_id, pid, trace_id, span_name, source_file, source_line, \
                       fields_json, error_message, metadata_json. Recent unflushed WAL rows are included. \
                       Use query_call_stack for ancestor/descendant span hierarchy and describe_timeline for full schema."
    )]
    pub sql: String,
    #[schemars(description = "Safety cap on rows returned. Default: 500")]
    #[serde(default)]
    pub max_rows: Option<usize>,
    #[schemars(
        description = "If true, store result in context registry and return context_id for rlm_analyze"
    )]
    #[serde(default)]
    pub load_to_context: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QueryTimelineSqlResult {
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub row_count: usize,
    pub truncated: bool,
    pub columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeTimeline {
    #[schemars(description = "Include example SQL queries. Default: true")]
    #[serde(default = "default_true")]
    pub include_examples: bool,
    #[schemars(description = "List the event families the store holds. Default: true")]
    #[serde(default = "default_true")]
    pub include_families: bool,
    #[schemars(description = "Only the families whose name starts with this, with their \
                              fields, types and call sites. An empty string names every family.")]
    #[serde(default)]
    pub family: Option<String>,
}

impl Default for DescribeTimeline {
    fn default() -> Self {
        Self {
            include_examples: true,
            include_families: true,
            family: None,
        }
    }
}

/// A tool's failure, reported to the caller as its text.
#[derive(Debug)]
pub struct ToolFailure(String);

impl From<FamiliesError> for ToolFailure {
    fn from(fault: FamiliesError) -> Self {
        Self(fault.to_string())
    }
}

impl IntoContents for ToolFailure {
    fn into_contents(self) -> Vec<Content> {
        vec![Content::text(self.0)]
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct PauseTimeline {
    #[schemars(description = "Why the timeline is being paused. Kept on the pause's row.")]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ResumeTimeline {}

// =============================================================================
// Handler
// =============================================================================

/// The Timeline Query Service MCP handler. Cheaply clonable (the engine is
/// `Arc`-shared) so it can be cloned per HTTP session.
#[derive(Clone)]
pub struct TimelineQueryHandler {
    engine: Arc<TimelineEngine>,
    tool_router: ToolRouter<Self>,
}

impl Default for TimelineQueryHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl TimelineQueryHandler {
    pub fn new() -> Self {
        Self {
            engine: Arc::new(TimelineEngine::new()),
            tool_router: Self::tool_router(),
        }
    }

    pub fn engine(&self) -> &Arc<TimelineEngine> {
        &self.engine
    }
}

#[tool_router]
impl TimelineQueryHandler {
    /// Execute SQL against the unified Parquet timeline store.
    #[tool(
        description = "Execute SQL against the unified Parquet timeline store. Table: 'timeline'. \
                       Key columns: id, entry_kind ('activity'/'span'/'log'/'conversation'), timestamp_us (μs since epoch), \
                       storage_tier, retention_class, level (TRACE/DEBUG/INFO/WARN/ERROR), event_type, description, software_module_id, \
                       workstream_id, session_id, pid, trace_id, span_name, source_file, source_line, fields_json, \
                       error_message, metadata_json. Conversation rows have a NULL description. \
                       Recent unflushed WAL rows are included. Use query_call_stack for ancestor/descendant \
                       span hierarchy and describe_timeline for full schema. \
                       Read-only (SELECT only). Set load_to_context=true for rlm_analyze.",
        annotations(read_only_hint = true)
    )]
    pub async fn query_timeline_sql(
        &self,
        params: Parameters<QueryTimelineSql>,
    ) -> Result<Json<QueryTimelineSqlResult>, ToolFailure> {
        let req = params.0;
        let max_rows = req.max_rows.unwrap_or(DEFAULT_MAX_ROWS);

        validate_select_only(&req.sql).map_err(ToolFailure)?;
        let sql = inject_limit(req.sql.trim(), max_rows);

        let batches = match self.engine.query(&sql).await {
            Ok(b) => b,
            Err(e) if e == "Timeline store not available" => {
                return Ok(Json(QueryTimelineSqlResult::default()));
            }
            Err(e) => return Err(ToolFailure(e)),
        };

        if batches.is_empty() {
            return Ok(Json(QueryTimelineSqlResult::default()));
        }

        let (rows, columns) = batches_to_rows(&batches);
        let row_count = rows.len();
        let truncated = row_count >= max_rows;

        // The Query Service does not own the dev-daemon's context registry;
        // load_to_context is a no-op here (context_id stays None). Callers that
        // need rlm_analyze context still route through the dev-daemon.
        Ok(Json(QueryTimelineSqlResult {
            rows,
            row_count,
            truncated,
            columns,
            context_id: None,
        }))
    }

    /// Get timeline schema and example queries.
    #[tool(
        description = "Get schema information and example SQL queries for the timeline Parquet store. \
                       Returns column names, types, descriptions, ready-to-use SQL examples, and the \
                       event families the store holds. Name a family to get its fields_json fields, \
                       their types and where they were emitted from.",
        annotations(read_only_hint = true)
    )]
    pub async fn describe_timeline(
        &self,
        params: Parameters<DescribeTimeline>,
    ) -> Result<Json<DescribeTimelineResult>, ToolFailure> {
        let examples = if params.0.include_examples {
            schema::examples()
        } else {
            vec![]
        };
        let families = if params.0.include_families {
            let engine = Arc::clone(&self.engine);
            let found = tokio::task::spawn_blocking(move || engine.event_families())
                .await
                .map_err(FamiliesError::from)??;
            Some(schema::describe_families(&found, params.0.family.as_deref()))
        } else {
            None
        };
        let pause: Option<wire::PauseState> = writer_client::call_writer(
            wire::TIMELINE_PAUSE_STATE_METHOD,
            &wire::ReadPauseState::default(),
        )
        .await
        .ok();
        Ok(Json(DescribeTimelineResult {
            columns: schema::timeline_columns(),
            examples,
            entry_kinds: schema::entry_kinds(),
            pause: pause.map(TimelinePauseResult::from),
            families,
        }))
    }

    /// Stop the timeline keeping records until it is resumed.
    #[tool(
        description = "Pause the timeline writer. Until it resumes or restarts, the writer discards \
                       span, log and event rows and counts them, and keeps record-class rows. The \
                       pause and the resume are rows, so a query can tell the gap from a quiet \
                       time. Pausing a paused writer changes nothing.",
        annotations(idempotent_hint = true, destructive_hint = false)
    )]
    pub async fn pause_timeline(
        &self,
        params: Parameters<PauseTimeline>,
    ) -> Result<Json<TimelinePauseResult>, ToolFailure> {
        let op = wire::PauseTimeline {
            reason: params.0.reason,
        };
        let state: wire::PauseState = writer_client::call_writer(wire::PAUSE_TIMELINE_METHOD, &op)
            .await
            .map_err(|e| ToolFailure(format!("{e:#}")))?;
        Ok(Json(state.into()))
    }

    /// Go back to keeping records.
    #[tool(
        description = "Resume the timeline writer. Answers the pause it ended, with what it \
                       discarded. Resuming a writer that is not paused changes nothing.",
        annotations(idempotent_hint = true, destructive_hint = false)
    )]
    pub async fn resume_timeline(
        &self,
        _params: Parameters<ResumeTimeline>,
    ) -> Result<Json<TimelinePauseResult>, ToolFailure> {
        let state: wire::PauseState =
            writer_client::call_writer(wire::RESUME_TIMELINE_METHOD, &wire::ResumeTimeline::default())
                .await
                .map_err(|e| ToolFailure(format!("{e:#}")))?;
        Ok(Json(state.into()))
    }

    /// Navigate the span hierarchy to understand execution flow.
    #[tool(
        description = "Navigate the span hierarchy to understand execution flow. Use for: \
                       (1) Error backtracing: focus_span_id + direction='ancestors' walks up \
                       the parent chain to show what led to an error. \
                       (2) Call tree exploration: trace_id + direction='descendants' shows \
                       what functions were called. \
                       (3) Full context: direction='both' or 'full' for complete picture. \
                       Returns tree structure with spans and attached logs. \
                       Set load_to_context=true to store result in context registry for further analysis.",
        annotations(read_only_hint = true)
    )]
    pub async fn query_call_stack(
        &self,
        params: Parameters<QueryCallStack>,
    ) -> Result<Json<QueryCallStackResult>, ToolFailure> {
        // load_to_context is accepted for surface compatibility but is a no-op
        // here — the Query Service does not own the dev-daemon context registry
        // (parity with query_timeline_sql). context_id stays None.
        let result = self
            .engine
            .query_call_stack(&params.0)
            .await
            .map_err(ToolFailure)?;
        Ok(Json(result))
    }
}

impl ServerHandler for TimelineQueryHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "x0k timeline service: the machine's one timeline writer and its query \
                 surface. Owns the DataFusion engine, json_extract UDF, and the parquet + \
                 shared-WAL read behind the timeline query tools. Stands without \
                 x0k-dev-daemon, so the timeline is written and queried even when the dev \
                 daemon is crash-looping, rebuilding or stopped."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParam>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::ListToolsResult, rmcp::ErrorData>>
           + Send
           + '_ {
        let tools = self.tool_router.list_all();
        std::future::ready(Ok(rmcp::model::ListToolsResult {
            tools,
            ..Default::default()
        }))
    }

    fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParam,
        context: rmcp::service::RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::CallToolResult, rmcp::ErrorData>>
           + Send
           + '_ {
        let call_context = ToolCallContext::new(self, request, context);
        async move { self.tool_router.call(call_context).await }
    }
}
