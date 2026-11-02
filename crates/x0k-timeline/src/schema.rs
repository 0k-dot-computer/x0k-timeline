//! The describe_timeline schema surface — the contract agents read before
//! writing any SQL. The 58-column unified schema and the four entry_kinds ride
//! UNCHANGED from the dev-daemon implementation (the
//! `schema_describe_column_count` parity row asserts exactly 58 columns +
//! `["activity","span","log","conversation"]`).

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use x0k_parquet::families::{widened, Family, FieldKind};

use crate::engine::EventFamilies;

/// Column description for the timeline schema.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ColumnDescription {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub description: String,
    pub entry_kinds: Vec<String>,
}

/// Example SQL query for common tasks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqlExample {
    pub description: String,
    pub sql: String,
}

/// Description of an entry_kind value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EntryKindDescription {
    pub name: String,
    pub description: String,
}

/// Response from describe_timeline.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeTimelineResult {
    pub columns: Vec<ColumnDescription>,
    #[serde(default)]
    pub examples: Vec<SqlExample>,
    pub entry_kinds: Vec<EntryKindDescription>,
    /// The writer's pause state, or nothing when the writer did not answer.
    #[serde(default)]
    pub pause: Option<TimelinePauseResult>,
    /// What the store holds, family by family, from the rows themselves.
    #[serde(default)]
    pub families: Option<FamiliesDescription>,
}

/// The event families of the store.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FamiliesDescription {
    pub families: Vec<FamilyDescription>,
    pub files: u64,
    /// Files written before footers held families, whose rows are not counted here.
    pub files_without_schema: u64,
    /// A limit dropped something, so this is not all of what was seen.
    pub truncated: bool,
}

/// One event family: the rows sharing an `event_type`, or a `span_name`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FamilyDescription {
    pub name: String,
    pub entry_kinds: Vec<String>,
    pub rows: u64,
    pub first_seen_us: i64,
    pub last_seen_us: i64,
    /// Empty unless the call named this family's prefix.
    #[serde(default)]
    pub fields: Vec<FieldDescription>,
    #[serde(default)]
    pub call_sites: Vec<CallSiteDescription>,
}

/// One field of `fields_json`, as its observations describe it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FieldDescription {
    pub name: String,
    /// The narrowest type covering every observation: `boolean`, `unsigned`,
    /// `signed`, `float`, `text` or `structured`. Nothing when it was only ever
    /// null, or when the observations conflict.
    #[serde(default)]
    pub data_type: Option<String>,
    /// The types seen, when no one type covers them.
    #[serde(default)]
    pub conflict: Vec<String>,
}

/// Where a family's rows were emitted from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CallSiteDescription {
    #[serde(default)]
    pub module: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub line: Option<u32>,
}

fn kind_name(kind: FieldKind) -> String {
    match kind {
        FieldKind::Boolean => "boolean",
        FieldKind::Unsigned => "unsigned",
        FieldKind::Signed => "signed",
        FieldKind::Float => "float",
        FieldKind::Text => "text",
        FieldKind::Structured => "structured",
    }
    .to_string()
}

/// `answer`, each family with its fields and call sites when its name starts
/// with `detail`, and as a name and counts otherwise.
pub fn describe_families(answer: &EventFamilies, detail: Option<&str>) -> FamiliesDescription {
    let described = |(name, family): (&String, &Family)| {
        let mut described = FamilyDescription {
            name: name.clone(),
            entry_kinds: family.entry_kinds.iter().cloned().collect(),
            rows: family.rows,
            first_seen_us: family.first_seen_us,
            last_seen_us: family.last_seen_us,
            fields: Vec::new(),
            call_sites: Vec::new(),
        };
        if detail.is_some_and(|prefix| name.starts_with(prefix)) {
            described.fields = family.fields.iter().map(describe_field).collect();
            described.call_sites = family
                .call_sites
                .iter()
                .map(|site| CallSiteDescription {
                    module: site.module.clone(),
                    file: site.file.clone(),
                    line: site.line,
                })
                .collect();
        }
        described
    };
    let selected = |(name, _): &(&String, &Family)| detail.is_none_or(|prefix| name.starts_with(prefix));
    FamiliesDescription {
        families: answer.families.families.iter().filter(selected).map(described).collect(),
        files: answer.files as u64,
        files_without_schema: answer.files_without_schema as u64,
        truncated: answer.families.truncated,
    }
}

fn describe_field((name, kinds): (&String, &BTreeSet<FieldKind>)) -> FieldDescription {
    let data_type = widened(kinds);
    let conflict = if data_type.is_none() {
        kinds.iter().copied().map(kind_name).collect()
    } else {
        Vec::new()
    };
    FieldDescription { name: name.clone(), data_type: data_type.map(kind_name), conflict }
}

/// Whether the writer is paused, since when, why, and what it has discarded.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TimelinePauseResult {
    pub paused: bool,
    /// Microseconds since the Unix epoch.
    pub since_us: Option<u64>,
    pub reason: Option<String>,
    pub discarded_spans: u64,
    pub discarded_logs: u64,
    pub discarded_events: u64,
}

impl From<x0k_timeline_record::pause::PauseState> for TimelinePauseResult {
    fn from(state: x0k_timeline_record::pause::PauseState) -> Self {
        Self {
            paused: state.paused,
            since_us: state.since_us,
            reason: state.reason,
            discarded_spans: state.discarded.span,
            discarded_logs: state.discarded.log,
            discarded_events: state.discarded.event,
        }
    }
}

fn col(
    name: &str,
    data_type: &str,
    nullable: bool,
    description: &str,
    entry_kinds: &[&str],
) -> ColumnDescription {
    ColumnDescription {
        name: name.into(),
        data_type: data_type.into(),
        nullable,
        description: description.into(),
        entry_kinds: entry_kinds.iter().map(|s| s.to_string()).collect(),
    }
}

/// The full 58-column schema description (parity with dev-daemon describe_timeline).
pub fn timeline_columns() -> Vec<ColumnDescription> {
    let a = ["activity"];
    let al = ["activity", "log"];
    let asp = ["activity", "span"];
    let asl = ["activity", "span", "log"];
    let aslc = ["activity", "span", "log", "conversation"];
    let alc = ["activity", "log", "conversation"];
    let slc = ["span", "log", "conversation"];
    let sl = ["span", "log"];
    let asc = ["activity", "span", "conversation"];

    vec![
        col("id", "Utf8", false, "Unique identifier (activity_id, span_id, or session_id:seq)", &aslc),
        col("entry_kind", "Utf8", false, "Type discriminator: 'activity', 'span', 'log', or 'conversation'", &aslc),
        col("storage_tier", "Utf8", false, "Query/routing class. Activities use 'central', 'ephemeral', or 'record'; spans/logs/conversations use the local debug class.", &aslc),
        col("retention_class", "Utf8", false, "Physical retention class: 'span', 'log', 'event', or 'record'. This determines Parquet retention policy.", &aslc),
        col("timestamp_us", "Int64", false, "Timestamp in microseconds since epoch", &aslc),
        col("duration_ms", "Int64", true, "Duration in milliseconds", &asc),
        col("trace_id", "Utf8", true, "W3C trace ID (32-char hex) for correlating rows across activities, spans, logs, and conversation entries.", &aslc),
        col("traceparent", "Utf8", true, "W3C traceparent when canonical request provenance is attached.", &al),
        col("tracestate", "Utf8", true, "W3C tracestate when canonical request provenance is attached.", &al),
        col("span_id", "Utf8", true, "Structural span identifier. Present on spans and on activities that deliberately attach to span structure.", &asp),
        col("parent_span_id", "Utf8", true, "Structural parent span identifier. Use with query_call_stack for hierarchy traversal.", &asp),
        col("span_name", "Utf8", true, "Span name or activity-attached span label.", &asp),
        col("level", "Utf8", true, "Severity/classification level. Spans/logs use tracing levels; activities/conversations use derived INFO/WARN/ERROR style levels.", &aslc),
        col("event_type", "Utf8", true, "Structured event type. Activities use activity_type, logs can carry explicit event_type, conversations use conversation.*.", &alc),
        col("workstream_id", "Utf8", true, "Workstream ID for scoping", &aslc),
        col("intent_id", "Utf8", true, "Grove intent ID", &aslc),
        col("request_id", "Utf8", true, "Canonical request ID when present.", &al),
        col("parent_request_id", "Utf8", true, "Canonical parent request ID when present.", &al),
        col("role", "Utf8", true, "Logical execution role when canonical provenance is attached.", &al),
        col("mode", "Utf8", true, "Execution mode when canonical provenance is attached.", &al),
        col("origin_system", "Utf8", true, "Origin system when canonical provenance is attached.", &al),
        col("origin_subsystem", "Utf8", true, "Origin subsystem when canonical provenance is attached.", &al),
        col("agent_profile", "Utf8", true, "Agent profile when canonical provenance is attached.", &al),
        col("sandbox_id", "Utf8", true, "Sandbox identifier when canonical provenance is attached.", &al),
        col("invocation_method", "Utf8", true, "Invocation method when canonical provenance is attached.", &al),
        col("executed_by_json", "Utf8", true, "Direct executor serialized as JSON when canonical provenance is attached.", &al),
        col("authorization_chain_json", "Utf8", true, "Authority chain serialized as JSON when canonical provenance is attached.", &al),
        col("authorization_proof_refs_json", "Utf8", true, "Authorization proof refs serialized as JSON when canonical provenance is attached.", &al),
        col("artifact_id", "Utf8", true, "Canonical artifact identifier when present.", &al),
        col("native_artifact_id", "Utf8", true, "Backend-native artifact identifier when present.", &al),
        col("document_id", "Utf8", true, "Canonical document identifier when present.", &al),
        col("document_revision_id", "Utf8", true, "Canonical document revision identifier when present.", &al),
        col("file_path", "Utf8", true, "Canonical file path when present.", &al),
        col("source_code_snapshot_id", "Utf8", true, "Recorded source-code snapshot identifier when present.", &al),
        col("source_code_json", "Utf8", true, "Source-code provenance serialized as JSON when present.", &al),
        col("code_snapshot_json", "Utf8", true, "Code-snapshot record serialized as JSON when present.", &al),
        col("approval_id", "Utf8", true, "Approval identifier when present.", &al),
        col("approval_record_json", "Utf8", true, "Approval record serialized as JSON when present.", &al),
        col("software_module_id", "Utf8", true, "Software module that produced this entry", &asl),
        col("function", "Utf8", true, "Function name when captured at emission time.", &asp),
        col("description", "Utf8", true, "Human-readable description or log message. NULL for conversation entries: the store never held their text.", &alc),
        col("source_file", "Utf8", true, "Source file path", &asl),
        col("source_line", "UInt32", true, "Source line number", &asl),
        col("module", "Utf8", true, "Rust module path (e.g., 'x0k_core::sync')", &asl),
        col("target", "Utf8", true, "Tracing target", &sl),
        col("fields_json", "Utf8", true, "Structured fields as JSON. Spans: span fields. Logs: log fields. Conversation: {role, seq, query_id}.", &slc),
        col("events_json", "Utf8", true, "Span events as JSON array", &["span"]),
        col("actor", "Utf8", true, "Actor entity URI", &a),
        col("target_type", "Utf8", true, "Activity-specific target classification when present.", &a),
        col("session_id", "Utf8", true, "Agent session ID.", &aslc),
        col("pid", "UInt32", true, "OS process ID of the emitting process", &asl),
        col("correlation_id", "Utf8", true, "Correlation ID for grouping", &a),
        col("parent_id", "Utf8", true, "Parent activity ID for semantic activity causality edges.", &a),
        col("error_message", "Utf8", true, "Error message if applicable", &a),
        col("old_value", "Utf8", true, "Activity-specific old value payload when present.", &a),
        col("new_value", "Utf8", true, "Activity-specific new value payload when present.", &a),
        col("ended_at_us", "Int64", true, "Activity end timestamp in microseconds since epoch.", &a),
        col("metadata_json", "Utf8", true, "Additional metadata as JSON", &a),
    ]
}

/// The four entry_kind value descriptions.
pub fn entry_kinds() -> Vec<EntryKindDescription> {
    vec![
        EntryKindDescription { name: "activity".into(), description: "Structured activities from the activity system (agent actions, task events, module events)".into() },
        EntryKindDescription { name: "span".into(), description: "Tracing spans from #[instrument] and manual spans (function calls, durations)".into() },
        EntryKindDescription { name: "log".into(), description: "Log events from tracing macros (info!, warn!, error!, debug!)".into() },
        EntryKindDescription { name: "conversation".into(), description: "Agent conversation entries. The description is NULL: the store holds only each entry's metadata. Use event_type for filtering (conversation.message, conversation.query_complete, etc.). Role/seq/query_id in fields_json.".into() },
    ]
}

/// The example SQL queries surfaced by describe_timeline when requested.
pub fn examples() -> Vec<SqlExample> {
    let ex = |description: &str, sql: &str| SqlExample {
        description: description.into(),
        sql: sql.into(),
    };
    vec![
        ex("Recent errors", "SELECT timestamp_us, level, description, source_file, source_line FROM timeline WHERE level = 'ERROR' ORDER BY timestamp_us DESC LIMIT 20"),
        ex("Logs from a module", "SELECT timestamp_us, level, description, span_name FROM timeline WHERE software_module_id = 'grove-service' AND entry_kind IN ('log', 'span') ORDER BY timestamp_us DESC LIMIT 50"),
        ex("Activity history", "SELECT timestamp_us, event_type, description, software_module_id FROM timeline WHERE entry_kind = 'activity' ORDER BY timestamp_us DESC LIMIT 50"),
        ex("Dropped-record summaries", "SELECT timestamp_us, event_type, description, metadata_json FROM timeline WHERE event_type = 'system.timeline.records_dropped' ORDER BY timestamp_us DESC LIMIT 20"),
        ex("Trace correlation", "SELECT timestamp_us, entry_kind, level, span_name, description FROM timeline WHERE trace_id = '<id>' ORDER BY timestamp_us"),
        ex("Launch manifest inspection", "SELECT timestamp_us, entry_kind, event_type, description, fields_json FROM timeline WHERE session_id = '<session-id>' AND event_type IN ('provider.launch.configured', 'provider.opencode.configured', 'sandbox.launch.configured') ORDER BY timestamp_us"),
        ex("Query/routing and retention classes", "SELECT retention_class, storage_tier, COUNT(*) AS rows FROM timeline GROUP BY retention_class, storage_tier ORDER BY retention_class, storage_tier"),
        ex("Error distribution by module", "SELECT software_module_id, COUNT(*) as cnt FROM timeline WHERE level = 'ERROR' GROUP BY 1 ORDER BY 2 DESC"),
        ex("Session logs", "SELECT timestamp_us, level, description, source_file FROM timeline WHERE session_id = '<id>' AND entry_kind = 'log' ORDER BY timestamp_us DESC LIMIT 100"),
        ex("Interleaved session timeline", "SELECT timestamp_us, entry_kind, event_type, description FROM timeline WHERE session_id = '<id>' ORDER BY timestamp_us LIMIT 100"),
        ex("Recent conversation messages", "SELECT timestamp_us, session_id, event_type, description, fields_json FROM timeline WHERE entry_kind = 'conversation' AND event_type = 'conversation.message' ORDER BY timestamp_us DESC LIMIT 20"),
    ]
}
