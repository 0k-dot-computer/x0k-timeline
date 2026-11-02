//! Unified Arrow schema for timeline records (activities + spans)
//!
//! A single flat table covering both activity and span data. Discriminated by
//! `entry_kind` ("activity" or "span"). Activity-only columns are null in span
//! rows and vice versa — Parquet encodes null runs to near-zero bytes.
//!
//! Replaces the former span-only `span_schema.rs`.

use crate::routing::{RetentionClass, StorageTier};
use crate::types::CompletedActivity;
use crate::SessionContext;
use arrow::array::{Int64Array, RecordBatch, StringArray, StringBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use x0k_timeline_record::span::SpanData;

/// Entry kind discriminator values
pub const ENTRY_KIND_ACTIVITY: &str = "activity";
pub const ENTRY_KIND_SPAN: &str = "span";
pub const ENTRY_KIND_LOG: &str = "log";
pub const ENTRY_KIND_CONVERSATION: &str = "conversation";

/// Storage tier column values
pub const STORAGE_TIER_CENTRAL: &str = "central";
pub const STORAGE_TIER_EPHEMERAL: &str = "ephemeral";
pub const STORAGE_TIER_RECORD: &str = "record";

/// Retention class column values
pub const RETENTION_CLASS_SPAN: &str = "span";
pub const RETENTION_CLASS_LOG: &str = "log";
pub const RETENTION_CLASS_EVENT: &str = "event";
pub const RETENTION_CLASS_RECORD: &str = "record";

/// Arrow schema for the unified timeline table
///
/// ~30 columns total. Columns shared by both entry kinds come first,
/// followed by span-specific columns, then activity-specific columns.
pub fn timeline_schema() -> Schema {
    Schema::new(vec![
        // === Primary key and discriminator ===
        Field::new("id", DataType::Utf8, false), // activity_id or span_id
        Field::new("entry_kind", DataType::Utf8, false), // "activity" or "span"
        Field::new("storage_tier", DataType::Utf8, false), // query/routing class
        Field::new("retention_class", DataType::Utf8, false), // physical retention class
        // === Shared timing ===
        Field::new("timestamp_us", DataType::Int64, false), // microseconds since epoch
        Field::new("duration_ms", DataType::Int64, true),
        // === Shared identity ===
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("traceparent", DataType::Utf8, true),
        Field::new("tracestate", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true), // span's own ID or activity's span_id
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, true),
        // === Shared classification ===
        Field::new("level", DataType::Utf8, true), // log level (span) or derived severity (activity)
        Field::new("event_type", DataType::Utf8, true), // activity_type for activities, null for spans
        // === Shared context ===
        Field::new("workstream_id", DataType::Utf8, true),
        Field::new("intent_id", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new("parent_request_id", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, true),
        Field::new("mode", DataType::Utf8, true),
        Field::new("origin_system", DataType::Utf8, true),
        Field::new("origin_subsystem", DataType::Utf8, true),
        Field::new("agent_profile", DataType::Utf8, true),
        Field::new("sandbox_id", DataType::Utf8, true),
        Field::new("invocation_method", DataType::Utf8, true),
        Field::new("executed_by_json", DataType::Utf8, true),
        Field::new("authorization_chain_json", DataType::Utf8, true),
        Field::new("authorization_proof_refs_json", DataType::Utf8, true),
        Field::new("artifact_id", DataType::Utf8, true),
        Field::new("native_artifact_id", DataType::Utf8, true),
        Field::new("document_id", DataType::Utf8, true),
        Field::new("document_revision_id", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("source_code_snapshot_id", DataType::Utf8, true),
        Field::new("source_code_json", DataType::Utf8, true),
        Field::new("code_snapshot_json", DataType::Utf8, true),
        Field::new("approval_id", DataType::Utf8, true),
        Field::new("approval_record_json", DataType::Utf8, true),
        Field::new("software_module_id", DataType::Utf8, true),
        // === Shared source location ===
        Field::new("function", DataType::Utf8, true),
        Field::new("module", DataType::Utf8, true),
        Field::new("source_file", DataType::Utf8, true),
        Field::new("source_line", DataType::UInt32, true),
        Field::new("target", DataType::Utf8, true),
        // === Span-specific ===
        Field::new("fields_json", DataType::Utf8, true),
        Field::new("events_json", DataType::Utf8, true),
        // === Activity-specific ===
        Field::new("actor", DataType::Utf8, true),
        Field::new("target_type", DataType::Utf8, true),
        Field::new("correlation_id", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true), // parent activity ID
        Field::new("session_id", DataType::Utf8, true),
        Field::new("pid", DataType::UInt32, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("old_value", DataType::Utf8, true),
        Field::new("new_value", DataType::Utf8, true),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("ended_at_us", DataType::Int64, true), // activity end timestamp in μs
        Field::new("metadata_json", DataType::Utf8, true), // remaining metadata as JSON
    ])
}

/// Number of columns in the timeline schema
pub const TIMELINE_COLUMN_COUNT: usize = 58;

fn retention_class_str(class: RetentionClass) -> &'static str {
    match class {
        RetentionClass::Span => RETENTION_CLASS_SPAN,
        RetentionClass::Log => RETENTION_CLASS_LOG,
        RetentionClass::Event => RETENTION_CLASS_EVENT,
        RetentionClass::Record => RETENTION_CLASS_RECORD,
    }
}

fn activity_retention_class(tier: StorageTier) -> RetentionClass {
    match tier {
        StorageTier::Record => RetentionClass::Record,
        StorageTier::Central | StorageTier::Ephemeral => RetentionClass::Event,
    }
}

/// Convert a slice of SpanData into a RecordBatch using the unified schema
pub fn span_data_to_batch(spans: &[SpanData]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(timeline_schema());
    let len = spans.len();

    // === Primary key and discriminator ===
    let ids: Vec<&str> = spans.iter().map(|s| s.span_id.as_str()).collect();
    let entry_kinds: Vec<&str> = vec![ENTRY_KIND_SPAN; len];
    let storage_tiers: Vec<&str> = vec![STORAGE_TIER_EPHEMERAL; len];
    let retention_classes: Vec<&str> = vec![retention_class_str(RetentionClass::Span); len];

    // === Shared timing ===
    let timestamps: Vec<i64> = spans.iter().map(|s| s.start_time as i64).collect();
    let mut durations: Vec<Option<i64>> = Vec::with_capacity(len);

    // === Shared identity ===
    let mut trace_ids = StringBuilder::new();
    let mut traceparents = StringBuilder::new();
    let mut tracestates = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut parent_span_ids = StringBuilder::new();
    let mut span_names = StringBuilder::new();

    // === Shared classification ===
    let mut levels = StringBuilder::new();
    let mut event_types = StringBuilder::new(); // null for spans

    // === Shared context ===
    let mut workstream_ids = StringBuilder::new();
    let mut intent_ids = StringBuilder::new();
    let mut request_ids = StringBuilder::new();
    let mut parent_request_ids = StringBuilder::new();
    let mut roles = StringBuilder::new();
    let mut modes = StringBuilder::new();
    let mut origin_systems = StringBuilder::new();
    let mut origin_subsystems = StringBuilder::new();
    let mut agent_profiles = StringBuilder::new();
    let mut sandbox_ids = StringBuilder::new();
    let mut invocation_methods = StringBuilder::new();
    let mut executed_by_jsons = StringBuilder::new();
    let mut authorization_chain_jsons = StringBuilder::new();
    let mut authorization_proof_refs_jsons = StringBuilder::new();
    let mut artifact_ids = StringBuilder::new();
    let mut native_artifact_ids = StringBuilder::new();
    let mut document_ids = StringBuilder::new();
    let mut document_revision_ids = StringBuilder::new();
    let mut file_paths = StringBuilder::new();
    let mut source_code_snapshot_ids = StringBuilder::new();
    let mut source_code_jsons = StringBuilder::new();
    let mut code_snapshot_jsons = StringBuilder::new();
    let mut approval_ids = StringBuilder::new();
    let mut approval_record_jsons = StringBuilder::new();
    let mut software_module_ids = StringBuilder::new();

    // === Shared source location ===
    let mut functions = StringBuilder::new();
    let mut modules = StringBuilder::new();
    let mut source_files = StringBuilder::new();
    let mut source_lines = UInt32Builder::new();
    let mut targets = StringBuilder::new();

    // === Span-specific ===
    let mut fields_jsons = StringBuilder::new();
    let mut events_jsons = StringBuilder::new();

    // === Activity-specific (all null for spans, except session_id and pid) ===
    let mut actors = StringBuilder::new();
    let mut target_types = StringBuilder::new();
    let mut correlation_ids = StringBuilder::new();
    let mut parent_ids = StringBuilder::new();
    let mut session_ids_col = StringBuilder::new();
    let mut pid_col = UInt32Builder::new();
    let mut descriptions = StringBuilder::new();
    let mut old_values = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut error_messages = StringBuilder::new();
    let mut ended_at_us: Vec<Option<i64>> = Vec::with_capacity(len);
    let mut metadata_jsons = StringBuilder::new();

    for span in spans {
        durations.push(span.duration_ms.map(|d| d as i64));

        // identity
        trace_ids.append_value(&span.trace_id);
        traceparents.append_null();
        tracestates.append_null();
        span_ids.append_value(&span.span_id);
        append_opt(&mut parent_span_ids, span.parent_span_id.as_deref());
        span_names.append_value(&span.span_name);

        // classification
        levels.append_value(&span.level);
        event_types.append_null(); // spans have no event_type

        // context
        append_opt(&mut workstream_ids, span.workstream_id.as_deref());
        append_opt(&mut intent_ids, span.intent_id.as_deref());
        request_ids.append_null();
        parent_request_ids.append_null();
        roles.append_null();
        modes.append_null();
        origin_systems.append_null();
        origin_subsystems.append_null();
        agent_profiles.append_null();
        sandbox_ids.append_null();
        invocation_methods.append_null();
        executed_by_jsons.append_null();
        authorization_chain_jsons.append_null();
        authorization_proof_refs_jsons.append_null();
        artifact_ids.append_null();
        native_artifact_ids.append_null();
        document_ids.append_null();
        document_revision_ids.append_null();
        file_paths.append_null();
        source_code_snapshot_ids.append_null();
        source_code_jsons.append_null();
        code_snapshot_jsons.append_null();
        approval_ids.append_null();
        approval_record_jsons.append_null();
        append_opt(&mut software_module_ids, span.software_module_id.as_deref());

        // source location
        append_opt(&mut functions, span.function.as_deref());
        append_opt(&mut modules, span.module.as_deref());
        append_opt(&mut source_files, span.source_file.as_deref());
        match span.source_line {
            Some(l) => source_lines.append_value(l),
            None => source_lines.append_null(),
        }
        targets.append_value(&span.target);

        // span-specific: fields_json
        if span.fields.is_empty() {
            fields_jsons.append_null();
        } else {
            let fields_map: serde_json::Map<String, serde_json::Value> = span
                .fields
                .iter()
                .filter_map(|(k, v)| v.to_value().ok().map(|val| (k.clone(), val)))
                .collect();
            match serde_json::to_string(&fields_map) {
                Ok(s) => fields_jsons.append_value(&s),
                Err(_) => fields_jsons.append_null(),
            }
        }

        // span-specific: events_json
        if span.events.is_empty() {
            events_jsons.append_null();
        } else {
            let events: Vec<serde_json::Value> = span
                .events
                .iter()
                .map(|e| {
                    let event_fields: serde_json::Map<String, serde_json::Value> = e
                        .fields
                        .iter()
                        .filter_map(|(k, v)| v.to_value().ok().map(|val| (k.clone(), val)))
                        .collect();
                    serde_json::json!({
                        "timestamp": e.timestamp,
                        "level": e.level,
                        "message": e.message,
                        "fields": event_fields,
                        "source_file": e.source_file,
                        "source_line": e.source_line,
                    })
                })
                .collect();
            match serde_json::to_string(&events) {
                Ok(s) => events_jsons.append_value(&s),
                Err(_) => events_jsons.append_null(),
            }
        }

        // activity-specific: all null for spans except session_id and pid
        actors.append_null();
        target_types.append_null();
        correlation_ids.append_null();
        parent_ids.append_null();
        append_opt(&mut session_ids_col, span.session_id.as_deref());
        match span.pid {
            Some(p) => pid_col.append_value(p),
            None => pid_col.append_null(),
        }
        descriptions.append_null();
        old_values.append_null();
        new_values.append_null();
        error_messages.append_null();
        ended_at_us.push(None);
        metadata_jsons.append_null();
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(entry_kinds)),
            Arc::new(StringArray::from(storage_tiers)),
            Arc::new(StringArray::from(retention_classes)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(durations)),
            Arc::new(trace_ids.finish()),
            Arc::new(traceparents.finish()),
            Arc::new(tracestates.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(parent_span_ids.finish()),
            Arc::new(span_names.finish()),
            Arc::new(levels.finish()),
            Arc::new(event_types.finish()),
            Arc::new(workstream_ids.finish()),
            Arc::new(intent_ids.finish()),
            Arc::new(request_ids.finish()),
            Arc::new(parent_request_ids.finish()),
            Arc::new(roles.finish()),
            Arc::new(modes.finish()),
            Arc::new(origin_systems.finish()),
            Arc::new(origin_subsystems.finish()),
            Arc::new(agent_profiles.finish()),
            Arc::new(sandbox_ids.finish()),
            Arc::new(invocation_methods.finish()),
            Arc::new(executed_by_jsons.finish()),
            Arc::new(authorization_chain_jsons.finish()),
            Arc::new(authorization_proof_refs_jsons.finish()),
            Arc::new(artifact_ids.finish()),
            Arc::new(native_artifact_ids.finish()),
            Arc::new(document_ids.finish()),
            Arc::new(document_revision_ids.finish()),
            Arc::new(file_paths.finish()),
            Arc::new(source_code_snapshot_ids.finish()),
            Arc::new(source_code_jsons.finish()),
            Arc::new(code_snapshot_jsons.finish()),
            Arc::new(approval_ids.finish()),
            Arc::new(approval_record_jsons.finish()),
            Arc::new(software_module_ids.finish()),
            Arc::new(functions.finish()),
            Arc::new(modules.finish()),
            Arc::new(source_files.finish()),
            Arc::new(source_lines.finish()),
            Arc::new(targets.finish()),
            Arc::new(fields_jsons.finish()),
            Arc::new(events_jsons.finish()),
            Arc::new(actors.finish()),
            Arc::new(target_types.finish()),
            Arc::new(correlation_ids.finish()),
            Arc::new(parent_ids.finish()),
            Arc::new(session_ids_col.finish()),
            Arc::new(pid_col.finish()),
            Arc::new(descriptions.finish()),
            Arc::new(old_values.finish()),
            Arc::new(new_values.finish()),
            Arc::new(error_messages.finish()),
            Arc::new(Int64Array::from(ended_at_us)),
            Arc::new(metadata_jsons.finish()),
        ],
    )?;

    Ok(batch)
}

/// Convert a slice of CompletedActivity into a RecordBatch using the unified schema
pub fn activity_to_batch(
    activities: &[CompletedActivity],
    tier: StorageTier,
    session_context: &SessionContext,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(timeline_schema());
    let len = activities.len();

    let tier_str = match tier {
        StorageTier::Central => STORAGE_TIER_CENTRAL,
        StorageTier::Ephemeral => STORAGE_TIER_EPHEMERAL,
        StorageTier::Record => STORAGE_TIER_RECORD,
    };

    // === Primary key and discriminator ===
    let ids: Vec<&str> = activities
        .iter()
        .map(|a| a.context.activity_id.0.as_str())
        .collect();
    let entry_kinds: Vec<&str> = vec![ENTRY_KIND_ACTIVITY; len];
    let storage_tiers: Vec<&str> = vec![tier_str; len];
    let retention_classes: Vec<&str> =
        vec![retention_class_str(activity_retention_class(tier)); len];

    // === Shared timing ===
    // Activity timestamps are microseconds by contract.
    let timestamps: Vec<i64> = activities
        .iter()
        .map(|a| a.context.started_at as i64)
        .collect();
    let durations: Vec<Option<i64>> = activities
        .iter()
        .map(|a| Some(a.duration_ms as i64))
        .collect();

    // === Shared identity ===
    let mut trace_ids = StringBuilder::new();
    let mut traceparents = StringBuilder::new();
    let mut tracestates = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut parent_span_ids = StringBuilder::new();
    let mut span_names = StringBuilder::new();

    // === Shared classification ===
    let mut levels = StringBuilder::new();
    let event_types: Vec<&str> = activities
        .iter()
        .map(|a| a.context.activity_type.as_str())
        .collect();

    // === Shared context ===
    let mut workstream_ids = StringBuilder::new();
    let mut intent_ids = StringBuilder::new();
    let mut request_ids = StringBuilder::new();
    let mut parent_request_ids = StringBuilder::new();
    let mut roles = StringBuilder::new();
    let mut modes = StringBuilder::new();
    let mut origin_systems = StringBuilder::new();
    let mut origin_subsystems = StringBuilder::new();
    let mut agent_profiles = StringBuilder::new();
    let mut sandbox_ids = StringBuilder::new();
    let mut invocation_methods = StringBuilder::new();
    let mut executed_by_jsons = StringBuilder::new();
    let mut authorization_chain_jsons = StringBuilder::new();
    let mut authorization_proof_refs_jsons = StringBuilder::new();
    let mut artifact_ids = StringBuilder::new();
    let mut native_artifact_ids = StringBuilder::new();
    let mut document_ids = StringBuilder::new();
    let mut document_revision_ids = StringBuilder::new();
    let mut file_paths = StringBuilder::new();
    let mut source_code_snapshot_ids = StringBuilder::new();
    let mut source_code_jsons = StringBuilder::new();
    let mut code_snapshot_jsons = StringBuilder::new();
    let mut approval_ids = StringBuilder::new();
    let mut approval_record_jsons = StringBuilder::new();
    let mut software_module_ids = StringBuilder::new();

    // === Shared source location ===
    let mut functions = StringBuilder::new();
    let mut modules = StringBuilder::new();
    let mut source_files = StringBuilder::new();
    let mut source_lines = UInt32Builder::new();
    let mut targets = StringBuilder::new();

    // === Span-specific (all null for activities) ===
    let mut fields_jsons = StringBuilder::new();
    let mut events_jsons = StringBuilder::new();

    // === Activity-specific ===
    let mut actors = StringBuilder::new();
    let mut target_types = StringBuilder::new();
    let mut correlation_ids = StringBuilder::new();
    let mut parent_ids = StringBuilder::new();
    let mut session_ids_col = StringBuilder::new();
    let mut pid_col = UInt32Builder::new();
    let mut descriptions = StringBuilder::new();
    let mut old_values = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut error_messages = StringBuilder::new();
    let mut ended_at_us: Vec<Option<i64>> = Vec::with_capacity(len);
    let mut metadata_jsons = StringBuilder::new();

    for activity in activities {
        let ctx = &activity.context;

        // identity — use session context trace_id as fallback
        let effective_trace_id = ctx
            .trace_id
            .as_deref()
            .or(session_context.trace_id.as_deref());
        append_opt(&mut trace_ids, effective_trace_id);
        append_opt(
            &mut traceparents,
            ctx.metadata.get("traceparent").map(|s| s.as_str()),
        );
        append_opt(
            &mut tracestates,
            ctx.metadata.get("tracestate").map(|s| s.as_str()),
        );
        append_opt(&mut span_ids, ctx.span_id.as_deref());
        append_opt(&mut parent_span_ids, ctx.parent_span_id.as_deref());
        append_opt(&mut span_names, ctx.span_name.as_deref());

        // classification — an explicit `activity.level` (set when a warn!/error!
        // event is promoted into this tier) is the real severity; fall back to
        // sniffing the type string only when nothing said so.
        let level = ctx
            .metadata
            .get("activity.level")
            .map(String::as_str)
            .filter(|l| matches!(*l, "error" | "warn" | "info" | "debug" | "trace"))
            .unwrap_or_else(|| derive_level(&ctx.activity_type));
        levels.append_value(level);

        // context — workstream from session context
        append_opt(
            &mut workstream_ids,
            session_context.workstream_id.as_deref(),
        );
        append_opt(&mut intent_ids, session_context.intent_id.as_deref());
        append_opt(
            &mut request_ids,
            ctx.metadata.get("request_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut parent_request_ids,
            ctx.metadata.get("parent_request_id").map(|s| s.as_str()),
        );
        append_opt(&mut roles, ctx.metadata.get("role").map(|s| s.as_str()));
        append_opt(&mut modes, ctx.metadata.get("mode").map(|s| s.as_str()));
        append_opt(
            &mut origin_systems,
            ctx.metadata.get("origin_system").map(|s| s.as_str()),
        );
        append_opt(
            &mut origin_subsystems,
            ctx.metadata.get("origin_subsystem").map(|s| s.as_str()),
        );
        append_opt(
            &mut agent_profiles,
            ctx.metadata.get("agent_profile").map(|s| s.as_str()),
        );
        append_opt(
            &mut sandbox_ids,
            ctx.metadata.get("sandbox_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut invocation_methods,
            ctx.metadata.get("invocation_method").map(|s| s.as_str()),
        );
        append_opt(
            &mut executed_by_jsons,
            ctx.metadata.get("executed_by_json").map(|s| s.as_str()),
        );
        append_opt(
            &mut authorization_chain_jsons,
            ctx.metadata
                .get("authorization_chain_json")
                .map(|s| s.as_str()),
        );
        append_opt(
            &mut authorization_proof_refs_jsons,
            ctx.metadata
                .get("authorization_proof_refs_json")
                .map(|s| s.as_str()),
        );
        append_opt(
            &mut artifact_ids,
            ctx.metadata.get("artifact_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut native_artifact_ids,
            ctx.metadata.get("native_artifact_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut document_ids,
            ctx.metadata.get("document_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut document_revision_ids,
            ctx.metadata
                .get("document_revision_id")
                .or_else(|| ctx.metadata.get("revision_id"))
                .map(|s| s.as_str()),
        );
        append_opt(
            &mut file_paths,
            ctx.metadata
                .get("file_path")
                .or_else(|| ctx.metadata.get("path"))
                .map(|s| s.as_str()),
        );
        append_opt(
            &mut source_code_snapshot_ids,
            ctx.metadata
                .get("source_code_snapshot_id")
                .map(|s| s.as_str()),
        );
        append_opt(
            &mut source_code_jsons,
            ctx.metadata.get("source_code_json").map(|s| s.as_str()),
        );
        append_opt(
            &mut code_snapshot_jsons,
            ctx.metadata.get("code_snapshot_json").map(|s| s.as_str()),
        );
        append_opt(
            &mut approval_ids,
            ctx.metadata.get("approval_id").map(|s| s.as_str()),
        );
        append_opt(
            &mut approval_record_jsons,
            ctx.metadata.get("approval_record_json").map(|s| s.as_str()),
        );

        // Extract software_module from metadata
        let sw_module = ctx
            .metadata
            .get("activity.software_module")
            .or_else(|| ctx.metadata.get("software_module"));
        append_opt(&mut software_module_ids, sw_module.map(|s| s.as_str()));

        // source location
        append_opt(&mut functions, ctx.source_function.as_deref());
        append_opt(&mut modules, ctx.source_module.as_deref());
        append_opt(&mut source_files, ctx.source_file.as_deref());
        match ctx.source_line {
            Some(l) => source_lines.append_value(l),
            None => source_lines.append_null(),
        }

        // target — extract from metadata
        let target = ctx
            .metadata
            .get("activity.target")
            .or_else(|| ctx.metadata.get("target"));
        append_opt(&mut targets, target.map(|s| s.as_str()));

        // span-specific: null for activities
        fields_jsons.append_null();
        events_jsons.append_null();

        // activity-specific
        match &ctx.actor {
            Some(actor) => actors.append_value(actor),
            None => actors.append_null(),
        }

        let target_type = ctx
            .metadata
            .get("activity.target_type")
            .or_else(|| ctx.metadata.get("target_type"));
        append_opt(&mut target_types, target_type.map(|s| s.as_str()));

        append_opt(&mut correlation_ids, ctx.correlation_id.as_deref());
        match &ctx.parent_id {
            Some(pid) => parent_ids.append_value(pid.0.as_str()),
            None => parent_ids.append_null(),
        }

        let session_id = ctx
            .metadata
            .get("activity.session_id")
            .or_else(|| ctx.metadata.get("session_id"));
        let session_id_str = session_id.map(|s| s.as_str());
        append_opt(
            &mut session_ids_col,
            session_id_str.or(session_context.session_id.as_deref()),
        );

        // pid: extract from metadata with fallback to session_context
        let pid_val = ctx
            .metadata
            .get("pid")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(session_context.pid);
        pid_col.append_value(pid_val);

        append_opt(&mut descriptions, ctx.activity_data.as_deref());

        let old_val = ctx
            .metadata
            .get("activity.old_value")
            .or_else(|| ctx.metadata.get("old_value"));
        append_opt(&mut old_values, old_val.map(|s| s.as_str()));

        let new_val = ctx
            .metadata
            .get("activity.new_value")
            .or_else(|| ctx.metadata.get("new_value"));
        append_opt(&mut new_values, new_val.map(|s| s.as_str()));

        let error = ctx
            .metadata
            .get("activity.error")
            .or_else(|| ctx.metadata.get("error"))
            .or_else(|| ctx.metadata.get("error_message"));
        append_opt(&mut error_messages, error.map(|s| s.as_str()));

        ended_at_us.push(Some(activity.ended_at as i64));

        // Remaining metadata as JSON (exclude special fields already extracted).
        // A value the emitter recorded as a number or bool is written as one.
        let remaining: std::collections::HashMap<&str, serde_json::Value> = ctx
            .metadata
            .iter()
            .filter(|(k, _)| !is_extracted_metadata_key(k))
            .map(|(k, v)| (k.as_str(), metadata_json_value(ctx, k, v)))
            .collect();
        if remaining.is_empty() {
            metadata_jsons.append_null();
        } else {
            match serde_json::to_string(&remaining) {
                Ok(s) => metadata_jsons.append_value(&s),
                Err(_) => metadata_jsons.append_null(),
            }
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(entry_kinds)),
            Arc::new(StringArray::from(storage_tiers)),
            Arc::new(StringArray::from(retention_classes)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(durations)),
            Arc::new(trace_ids.finish()),
            Arc::new(traceparents.finish()),
            Arc::new(tracestates.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(parent_span_ids.finish()),
            Arc::new(span_names.finish()),
            Arc::new(levels.finish()),
            Arc::new(StringArray::from(event_types)),
            Arc::new(workstream_ids.finish()),
            Arc::new(intent_ids.finish()),
            Arc::new(request_ids.finish()),
            Arc::new(parent_request_ids.finish()),
            Arc::new(roles.finish()),
            Arc::new(modes.finish()),
            Arc::new(origin_systems.finish()),
            Arc::new(origin_subsystems.finish()),
            Arc::new(agent_profiles.finish()),
            Arc::new(sandbox_ids.finish()),
            Arc::new(invocation_methods.finish()),
            Arc::new(executed_by_jsons.finish()),
            Arc::new(authorization_chain_jsons.finish()),
            Arc::new(authorization_proof_refs_jsons.finish()),
            Arc::new(artifact_ids.finish()),
            Arc::new(native_artifact_ids.finish()),
            Arc::new(document_ids.finish()),
            Arc::new(document_revision_ids.finish()),
            Arc::new(file_paths.finish()),
            Arc::new(source_code_snapshot_ids.finish()),
            Arc::new(source_code_jsons.finish()),
            Arc::new(code_snapshot_jsons.finish()),
            Arc::new(approval_ids.finish()),
            Arc::new(approval_record_jsons.finish()),
            Arc::new(software_module_ids.finish()),
            Arc::new(functions.finish()),
            Arc::new(modules.finish()),
            Arc::new(source_files.finish()),
            Arc::new(source_lines.finish()),
            Arc::new(targets.finish()),
            Arc::new(fields_jsons.finish()),
            Arc::new(events_jsons.finish()),
            Arc::new(actors.finish()),
            Arc::new(target_types.finish()),
            Arc::new(correlation_ids.finish()),
            Arc::new(parent_ids.finish()),
            Arc::new(session_ids_col.finish()),
            Arc::new(pid_col.finish()),
            Arc::new(descriptions.finish()),
            Arc::new(old_values.finish()),
            Arc::new(new_values.finish()),
            Arc::new(error_messages.finish()),
            Arc::new(Int64Array::from(ended_at_us)),
            Arc::new(metadata_jsons.finish()),
        ],
    )?;

    Ok(batch)
}

/// Convert a slice of LogEventData into a RecordBatch using the unified schema
pub fn log_event_to_batch(events: &[crate::types::LogEventData]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(timeline_schema());
    let len = events.len();

    let ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
    let entry_kinds: Vec<&str> = vec![ENTRY_KIND_LOG; len];
    let storage_tiers: Vec<&str> = vec![STORAGE_TIER_EPHEMERAL; len];
    let retention_classes: Vec<&str> = vec![retention_class_str(RetentionClass::Log); len];

    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_us as i64).collect();
    let durations: Vec<Option<i64>> = vec![None; len];

    let mut trace_ids = StringBuilder::new();
    let mut traceparents = StringBuilder::new();
    let mut tracestates = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut parent_span_ids = StringBuilder::new();
    let mut span_names = StringBuilder::new();

    let mut levels = StringBuilder::new();
    let mut event_types = StringBuilder::new();

    let mut workstream_ids = StringBuilder::new();
    let mut intent_ids = StringBuilder::new();
    let mut request_ids = StringBuilder::new();
    let mut parent_request_ids = StringBuilder::new();
    let mut roles = StringBuilder::new();
    let mut modes = StringBuilder::new();
    let mut origin_systems = StringBuilder::new();
    let mut origin_subsystems = StringBuilder::new();
    let mut agent_profiles = StringBuilder::new();
    let mut sandbox_ids = StringBuilder::new();
    let mut invocation_methods = StringBuilder::new();
    let mut executed_by_jsons = StringBuilder::new();
    let mut authorization_chain_jsons = StringBuilder::new();
    let mut authorization_proof_refs_jsons = StringBuilder::new();
    let mut artifact_ids = StringBuilder::new();
    let mut native_artifact_ids = StringBuilder::new();
    let mut document_ids = StringBuilder::new();
    let mut document_revision_ids = StringBuilder::new();
    let mut file_paths = StringBuilder::new();
    let mut source_code_snapshot_ids = StringBuilder::new();
    let mut source_code_jsons = StringBuilder::new();
    let mut code_snapshot_jsons = StringBuilder::new();
    let mut approval_ids = StringBuilder::new();
    let mut approval_record_jsons = StringBuilder::new();
    let mut software_module_ids = StringBuilder::new();

    let mut functions = StringBuilder::new();
    let mut modules = StringBuilder::new();
    let mut source_files = StringBuilder::new();
    let mut source_lines = UInt32Builder::new();
    let mut targets = StringBuilder::new();

    let mut fields_jsons = StringBuilder::new();
    let mut events_jsons = StringBuilder::new();

    let mut actors = StringBuilder::new();
    let mut target_types = StringBuilder::new();
    let mut correlation_ids = StringBuilder::new();
    let mut parent_ids = StringBuilder::new();
    let mut session_ids_col = StringBuilder::new();
    let mut pid_col = UInt32Builder::new();
    let mut descriptions = StringBuilder::new();
    let mut old_values = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut error_messages = StringBuilder::new();
    let mut ended_at_us: Vec<Option<i64>> = Vec::with_capacity(len);
    let mut metadata_jsons = StringBuilder::new();

    for evt in events {
        append_opt(&mut trace_ids, evt.trace_id.as_deref());
        append_opt(&mut traceparents, evt.traceparent.as_deref());
        append_opt(&mut tracestates, evt.tracestate.as_deref());
        span_ids.append_null();
        parent_span_ids.append_null();
        span_names.append_null();

        levels.append_value(&evt.level);
        append_opt(&mut event_types, evt.event_type.as_deref());

        append_opt(&mut workstream_ids, evt.workstream_id.as_deref());
        append_opt(&mut intent_ids, evt.intent_id.as_deref());
        append_opt(&mut request_ids, evt.request_id.as_deref());
        append_opt(&mut parent_request_ids, evt.parent_request_id.as_deref());
        append_opt(&mut roles, evt.role.as_deref());
        append_opt(&mut modes, evt.mode.as_deref());
        append_opt(&mut origin_systems, evt.origin_system.as_deref());
        append_opt(&mut origin_subsystems, evt.origin_subsystem.as_deref());
        append_opt(&mut agent_profiles, evt.agent_profile.as_deref());
        append_opt(&mut sandbox_ids, evt.sandbox_id.as_deref());
        append_opt(&mut invocation_methods, evt.invocation_method.as_deref());
        append_opt(&mut executed_by_jsons, evt.executed_by_json.as_deref());
        append_opt(
            &mut authorization_chain_jsons,
            evt.authorization_chain_json.as_deref(),
        );
        append_opt(
            &mut authorization_proof_refs_jsons,
            evt.authorization_proof_refs_json.as_deref(),
        );
        append_opt(&mut artifact_ids, evt.artifact_id.as_deref());
        append_opt(&mut native_artifact_ids, evt.native_artifact_id.as_deref());
        append_opt(&mut document_ids, evt.document_id.as_deref());
        append_opt(
            &mut document_revision_ids,
            evt.document_revision_id.as_deref(),
        );
        append_opt(&mut file_paths, evt.file_path.as_deref());
        source_code_snapshot_ids.append_null();
        source_code_jsons.append_null();
        code_snapshot_jsons.append_null();
        approval_ids.append_null();
        approval_record_jsons.append_null();
        append_opt(&mut software_module_ids, evt.software_module_id.as_deref());

        functions.append_null();
        append_opt(&mut modules, evt.module.as_deref());
        append_opt(&mut source_files, evt.source_file.as_deref());
        match evt.source_line {
            Some(l) => source_lines.append_value(l),
            None => source_lines.append_null(),
        }
        targets.append_value(&evt.target);

        // Use fields_json for structured log fields, message goes in description
        append_opt(&mut fields_jsons, evt.fields_json.as_deref());
        events_jsons.append_null();

        actors.append_null();
        target_types.append_null();
        correlation_ids.append_null();
        parent_ids.append_null();
        append_opt(&mut session_ids_col, evt.session_id.as_deref());
        match evt.pid {
            Some(p) => pid_col.append_value(p),
            None => pid_col.append_null(),
        }
        append_opt(&mut descriptions, evt.message.as_deref());
        old_values.append_null();
        new_values.append_null();
        error_messages.append_null();
        ended_at_us.push(None);
        metadata_jsons.append_null();
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(entry_kinds)),
            Arc::new(StringArray::from(storage_tiers)),
            Arc::new(StringArray::from(retention_classes)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(durations)),
            Arc::new(trace_ids.finish()),
            Arc::new(traceparents.finish()),
            Arc::new(tracestates.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(parent_span_ids.finish()),
            Arc::new(span_names.finish()),
            Arc::new(levels.finish()),
            Arc::new(event_types.finish()),
            Arc::new(workstream_ids.finish()),
            Arc::new(intent_ids.finish()),
            Arc::new(request_ids.finish()),
            Arc::new(parent_request_ids.finish()),
            Arc::new(roles.finish()),
            Arc::new(modes.finish()),
            Arc::new(origin_systems.finish()),
            Arc::new(origin_subsystems.finish()),
            Arc::new(agent_profiles.finish()),
            Arc::new(sandbox_ids.finish()),
            Arc::new(invocation_methods.finish()),
            Arc::new(executed_by_jsons.finish()),
            Arc::new(authorization_chain_jsons.finish()),
            Arc::new(authorization_proof_refs_jsons.finish()),
            Arc::new(artifact_ids.finish()),
            Arc::new(native_artifact_ids.finish()),
            Arc::new(document_ids.finish()),
            Arc::new(document_revision_ids.finish()),
            Arc::new(file_paths.finish()),
            Arc::new(source_code_snapshot_ids.finish()),
            Arc::new(source_code_jsons.finish()),
            Arc::new(code_snapshot_jsons.finish()),
            Arc::new(approval_ids.finish()),
            Arc::new(approval_record_jsons.finish()),
            Arc::new(software_module_ids.finish()),
            Arc::new(functions.finish()),
            Arc::new(modules.finish()),
            Arc::new(source_files.finish()),
            Arc::new(source_lines.finish()),
            Arc::new(targets.finish()),
            Arc::new(fields_jsons.finish()),
            Arc::new(events_jsons.finish()),
            Arc::new(actors.finish()),
            Arc::new(target_types.finish()),
            Arc::new(correlation_ids.finish()),
            Arc::new(parent_ids.finish()),
            Arc::new(session_ids_col.finish()),
            Arc::new(pid_col.finish()),
            Arc::new(descriptions.finish()),
            Arc::new(old_values.finish()),
            Arc::new(new_values.finish()),
            Arc::new(error_messages.finish()),
            Arc::new(Int64Array::from(ended_at_us)),
            Arc::new(metadata_jsons.finish()),
        ],
    )?;

    Ok(batch)
}

/// Convert a slice of ConversationMeta into a RecordBatch using the unified schema
pub fn conversation_to_batch(
    entries: &[crate::types::ConversationMeta],
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(timeline_schema());
    let len = entries.len();

    let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
    let entry_kinds: Vec<&str> = vec![ENTRY_KIND_CONVERSATION; len];
    let storage_tiers: Vec<&str> = vec![STORAGE_TIER_EPHEMERAL; len];
    let retention_classes: Vec<&str> = vec![retention_class_str(RetentionClass::Event); len];

    let timestamps: Vec<i64> = entries.iter().map(|e| e.timestamp_us as i64).collect();
    let durations: Vec<Option<i64>> = entries
        .iter()
        .map(|e| e.duration_ms.map(|d| d as i64))
        .collect();

    let mut trace_ids = StringBuilder::new();
    let mut traceparents = StringBuilder::new();
    let mut tracestates = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut parent_span_ids = StringBuilder::new();
    let mut span_names = StringBuilder::new();

    let mut levels = StringBuilder::new();
    let mut event_types = StringBuilder::new();

    let mut workstream_ids = StringBuilder::new();
    let mut intent_ids = StringBuilder::new();
    let mut request_ids = StringBuilder::new();
    let mut parent_request_ids = StringBuilder::new();
    let mut roles = StringBuilder::new();
    let mut modes = StringBuilder::new();
    let mut origin_systems = StringBuilder::new();
    let mut origin_subsystems = StringBuilder::new();
    let mut agent_profiles = StringBuilder::new();
    let mut sandbox_ids = StringBuilder::new();
    let mut invocation_methods = StringBuilder::new();
    let mut executed_by_jsons = StringBuilder::new();
    let mut authorization_chain_jsons = StringBuilder::new();
    let mut authorization_proof_refs_jsons = StringBuilder::new();
    let mut artifact_ids = StringBuilder::new();
    let mut native_artifact_ids = StringBuilder::new();
    let mut document_ids = StringBuilder::new();
    let mut document_revision_ids = StringBuilder::new();
    let mut file_paths = StringBuilder::new();
    let mut source_code_snapshot_ids = StringBuilder::new();
    let mut source_code_jsons = StringBuilder::new();
    let mut code_snapshot_jsons = StringBuilder::new();
    let mut approval_ids = StringBuilder::new();
    let mut approval_record_jsons = StringBuilder::new();
    let mut software_module_ids = StringBuilder::new();

    let mut functions = StringBuilder::new();
    let mut modules = StringBuilder::new();
    let mut source_files = StringBuilder::new();
    let mut source_lines = UInt32Builder::new();
    let mut targets = StringBuilder::new();

    let mut fields_jsons = StringBuilder::new();
    let mut events_jsons = StringBuilder::new();

    let mut actors = StringBuilder::new();
    let mut target_types = StringBuilder::new();
    let mut correlation_ids = StringBuilder::new();
    let mut parent_ids = StringBuilder::new();
    let mut session_ids_col = StringBuilder::new();
    let mut pid_col = UInt32Builder::new();
    let mut descriptions = StringBuilder::new();
    let mut old_values = StringBuilder::new();
    let mut new_values = StringBuilder::new();
    let mut error_messages = StringBuilder::new();
    let mut ended_at_us: Vec<Option<i64>> = Vec::with_capacity(len);
    let mut metadata_jsons = StringBuilder::new();

    for entry in entries {
        append_opt(&mut trace_ids, entry.trace_id.as_deref());
        traceparents.append_null();
        tracestates.append_null();
        span_ids.append_null();
        parent_span_ids.append_null();
        span_names.append_null();

        levels.append_value("INFO");
        event_types.append_value(format!("conversation.{}", entry.entry_type));

        append_opt(&mut workstream_ids, entry.workstream_id.as_deref());
        append_opt(&mut intent_ids, entry.intent_id.as_deref());
        request_ids.append_null();
        parent_request_ids.append_null();
        roles.append_null();
        modes.append_null();
        origin_systems.append_null();
        origin_subsystems.append_null();
        agent_profiles.append_null();
        sandbox_ids.append_null();
        invocation_methods.append_null();
        executed_by_jsons.append_null();
        authorization_chain_jsons.append_null();
        authorization_proof_refs_jsons.append_null();
        artifact_ids.append_null();
        native_artifact_ids.append_null();
        document_ids.append_null();
        document_revision_ids.append_null();
        file_paths.append_null();
        source_code_snapshot_ids.append_null();
        source_code_jsons.append_null();
        code_snapshot_jsons.append_null();
        approval_ids.append_null();
        approval_record_jsons.append_null();
        software_module_ids.append_value("x0k-agent");

        functions.append_null();
        modules.append_null();
        source_files.append_null();
        source_lines.append_null();
        targets.append_null();

        // fields_json: role, seq, query_id
        let mut fields = serde_json::Map::new();
        if let Some(ref role) = entry.role {
            fields.insert("role".into(), serde_json::Value::String(role.clone()));
        }
        fields.insert("seq".into(), serde_json::json!(entry.seq));
        if let Some(ref qid) = entry.query_id {
            fields.insert("query_id".into(), serde_json::Value::String(qid.clone()));
        }
        match serde_json::to_string(&fields) {
            Ok(s) => fields_jsons.append_value(&s),
            Err(_) => fields_jsons.append_null(),
        }
        events_jsons.append_null();

        actors.append_null();
        target_types.append_null();
        correlation_ids.append_null();
        parent_ids.append_null();
        session_ids_col.append_value(&entry.session_id);
        pid_col.append_null();
        match &entry.description {
            Some(d) => descriptions.append_value(d),
            None => descriptions.append_null(),
        }
        old_values.append_null();
        new_values.append_null();
        error_messages.append_null();
        ended_at_us.push(None);
        metadata_jsons.append_null();
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(entry_kinds)),
            Arc::new(StringArray::from(storage_tiers)),
            Arc::new(StringArray::from(retention_classes)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Int64Array::from(durations)),
            Arc::new(trace_ids.finish()),
            Arc::new(traceparents.finish()),
            Arc::new(tracestates.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(parent_span_ids.finish()),
            Arc::new(span_names.finish()),
            Arc::new(levels.finish()),
            Arc::new(event_types.finish()),
            Arc::new(workstream_ids.finish()),
            Arc::new(intent_ids.finish()),
            Arc::new(request_ids.finish()),
            Arc::new(parent_request_ids.finish()),
            Arc::new(roles.finish()),
            Arc::new(modes.finish()),
            Arc::new(origin_systems.finish()),
            Arc::new(origin_subsystems.finish()),
            Arc::new(agent_profiles.finish()),
            Arc::new(sandbox_ids.finish()),
            Arc::new(invocation_methods.finish()),
            Arc::new(executed_by_jsons.finish()),
            Arc::new(authorization_chain_jsons.finish()),
            Arc::new(authorization_proof_refs_jsons.finish()),
            Arc::new(artifact_ids.finish()),
            Arc::new(native_artifact_ids.finish()),
            Arc::new(document_ids.finish()),
            Arc::new(document_revision_ids.finish()),
            Arc::new(file_paths.finish()),
            Arc::new(source_code_snapshot_ids.finish()),
            Arc::new(source_code_jsons.finish()),
            Arc::new(code_snapshot_jsons.finish()),
            Arc::new(approval_ids.finish()),
            Arc::new(approval_record_jsons.finish()),
            Arc::new(software_module_ids.finish()),
            Arc::new(functions.finish()),
            Arc::new(modules.finish()),
            Arc::new(source_files.finish()),
            Arc::new(source_lines.finish()),
            Arc::new(targets.finish()),
            Arc::new(fields_jsons.finish()),
            Arc::new(events_jsons.finish()),
            Arc::new(actors.finish()),
            Arc::new(target_types.finish()),
            Arc::new(correlation_ids.finish()),
            Arc::new(parent_ids.finish()),
            Arc::new(session_ids_col.finish()),
            Arc::new(pid_col.finish()),
            Arc::new(descriptions.finish()),
            Arc::new(old_values.finish()),
            Arc::new(new_values.finish()),
            Arc::new(error_messages.finish()),
            Arc::new(Int64Array::from(ended_at_us)),
            Arc::new(metadata_jsons.finish()),
        ],
    )?;

    Ok(batch)
}

/// Append an optional string value to a StringBuilder
fn append_opt(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Derive a log-like level from an activity type
fn derive_level(activity_type: &str) -> &'static str {
    let lower = activity_type.to_lowercase();
    if lower.contains("error") || lower.contains("failed") || lower.contains("panic") {
        "error"
    } else if lower.contains("warn") {
        "warn"
    } else if lower.contains("debug") {
        "debug"
    } else {
        "info"
    }
}

/// Check if a metadata key is extracted into a dedicated column
/// The `metadata_json` value for one activity metadata entry.
///
/// The activity context carries metadata as text, with
/// `typed_metadata_keys` naming the entries the emitter recorded as a number
/// or bool. Those go back into the row as that scalar — a `u64` stays a number
/// from emitter to column (observability-timeline §5). Every other entry is a
/// string, however numeric it looks: `"007"` recorded as text stays text. A
/// typed key whose text no longer parses as a scalar (something overwrote it
/// with a string) falls back to the string rather than dropping the value.
fn metadata_json_value(
    ctx: &crate::types::ActivityContext,
    key: &str,
    value: &str,
) -> serde_json::Value {
    if ctx.typed_metadata_keys.contains(key) {
        if let Ok(scalar @ (serde_json::Value::Number(_) | serde_json::Value::Bool(_))) = serde_json::from_str::<serde_json::Value>(value) {
            return scalar
        }
    }
    serde_json::Value::String(value.to_string())
}

fn is_extracted_metadata_key(key: &str) -> bool {
    matches!(
        key,
        "activity.software_module"
            | "software_module"
            | "activity.target"
            | "target"
            | "activity.target_type"
            | "target_type"
            | "activity.session_id"
            | "session_id"
            | "activity.error"
            | "error"
            | "error_message"
            | "activity.old_value"
            | "old_value"
            | "activity.new_value"
            | "new_value"
            | "activity.id"
            | "activity.type"
            | "activity.parent_id"
            | "activity.durability"
            | "activity.description"
            | "activity.data"
            | "pid"
            | "request_id"
            | "parent_request_id"
            | "role"
            | "mode"
            | "origin_system"
            | "origin_subsystem"
            | "agent_profile"
            | "sandbox_id"
            | "invocation_method"
            | "traceparent"
            | "tracestate"
            | "executed_by_json"
            | "authorization_chain_json"
            | "authorization_proof_refs_json"
            | "artifact_id"
            | "native_artifact_id"
            | "document_id"
            | "document_revision_id"
            | "revision_id"
            | "file_path"
            | "source_code_snapshot_id"
            | "source_code_json"
            | "code_snapshot_json"
            | "approval_id"
            | "approval_record_json"
            | "path"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use std::collections::HashMap;

    fn test_span(id: &str) -> SpanData {
        SpanData {
            span_id: id.to_string(),
            trace_id: "trace123".to_string(),
            parent_span_id: None,
            start_time: 1704067200000000,
            duration_ms: Some(42),
            level: "info".to_string(),
            function: Some("test_fn".to_string()),
            module: Some("test_mod".to_string()),
            source_file: Some("src/main.rs".to_string()),
            source_line: Some(10),
            span_name: "test_span".to_string(),
            target: "test".to_string(),
            fields: HashMap::new(),
            events: Vec::new(),
            workstream_id: Some("ws-1".to_string()),
            intent_id: None,
            software_module_id: Some("my-daemon".to_string()),
            session_id: None,
            pid: Some(12345),
        }
    }

    fn test_activity(id: &str, activity_type: &str) -> CompletedActivity {
        use crate::types::ActivityContext;
        use x0k_timeline_record::ActivityId;

        CompletedActivity {
            context: ActivityContext {
                activity_id: ActivityId::from(id),
                activity_type: activity_type.to_string(),
                parent_id: None,
                actor: None,
                started_at: 1704067200000000,
                metadata: HashMap::new(),
                typed_metadata_keys: Default::default(),
                correlation_id: Some("corr-123".to_string()),
                trace_id: Some("trace456".to_string()),
                span_id: None,
                parent_span_id: None,
                activity_data: Some("test description".to_string()),
                lazy_data: None,
                source_file: None,
                source_line: None,
                source_function: None,
                source_module: None,
                span_name: None,
            },
            ended_at: 1704067201000000,
            duration_ms: 1000,
        }
    }

    fn test_log(id: &str, event_type: &str) -> crate::types::LogEventData {
        crate::types::LogEventData {
            id: id.to_string(),
            timestamp_us: 1704067200000000,
            level: "info".to_string(),
            message: Some("test log".to_string()),
            fields_json: Some(
                serde_json::json!({
                    "provider_id": "provider-1",
                    "path": "/tmp/from-fields.json",
                })
                .to_string(),
            ),
            source_file: Some("src/main.rs".to_string()),
            source_line: Some(22),
            module: Some("test_mod".to_string()),
            target: "test".to_string(),
            software_module_id: Some("my-daemon".to_string()),
            workstream_id: Some("ws-1".to_string()),
            intent_id: Some("intent-1".to_string()),
            session_id: Some("session-1".to_string()),
            pid: Some(12345),
            trace_id: Some("trace789".to_string()),
            traceparent: Some(
                "00-11111111111111111111111111111111-2222222222222222-01".to_string(),
            ),
            tracestate: Some("x0k=session-1".to_string()),
            request_id: Some("req-123".to_string()),
            parent_request_id: Some("req-122".to_string()),
            role: Some("agent".to_string()),
            mode: Some("foreground".to_string()),
            origin_system: Some("x0k-agent".to_string()),
            origin_subsystem: Some("provider.launch".to_string()),
            agent_profile: Some("coder".to_string()),
            sandbox_id: Some("sandbox-1".to_string()),
            invocation_method: Some("Triggered".to_string()),
            executed_by_json: Some("{\"id\":\"software:test\"}".to_string()),
            authorization_chain_json: Some("[{\"id\":\"authority:test\"}]".to_string()),
            authorization_proof_refs_json: Some("[\"proof-1\"]".to_string()),
            artifact_id: Some("artifact-1".to_string()),
            native_artifact_id: Some("native-1".to_string()),
            document_id: Some("document-1".to_string()),
            document_revision_id: Some("revision-1".to_string()),
            file_path: Some("/tmp/output.txt".to_string()),
            event_type: Some(event_type.to_string()),
        }
    }

    #[test]
    fn test_schema_field_count() {
        let schema = timeline_schema();
        assert_eq!(schema.fields().len(), TIMELINE_COLUMN_COUNT);
    }

    #[test]
    fn test_single_span_conversion() {
        let spans = vec![test_span("span1")];
        let batch = span_data_to_batch(&spans).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), TIMELINE_COLUMN_COUNT);

        // Verify entry_kind = "span"
        let kind_col = batch
            .column_by_name("entry_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kind_col.value(0), ENTRY_KIND_SPAN);

        // Verify storage_tier = "ephemeral"
        let tier_col = batch
            .column_by_name("storage_tier")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tier_col.value(0), STORAGE_TIER_EPHEMERAL);

        let retention_col = batch
            .column_by_name("retention_class")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(retention_col.value(0), RETENTION_CLASS_SPAN);
    }

    #[test]
    fn test_multiple_spans_conversion() {
        let spans: Vec<SpanData> = (0..5).map(|i| test_span(&format!("span_{}", i))).collect();
        let batch = span_data_to_batch(&spans).unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn test_timestamp_microsecond_conversion() {
        let spans = vec![test_span("s1")];
        let batch = span_data_to_batch(&spans).unwrap();
        let ts_col = batch
            .column_by_name("timestamp_us")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1704067200000000);
    }

    #[test]
    fn an_activitys_actor_id_fills_its_actor_column() {
        use arrow::array::Array;

        let mut named = test_activity("act1", "agent.spawned");
        named.context.actor = Some("actor-7".to_string());
        let unnamed = test_activity("act2", "agent.spawned");
        let session = SessionContext::new(None, None, None);
        let batch = activity_to_batch(&[named, unnamed], StorageTier::Central, &session).unwrap();
        let actors = batch
            .column_by_name("actor")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(actors.value(0), "actor-7");
        assert!(actors.is_null(1));
    }

    #[test]
    fn test_single_activity_conversion() {
        let activities = vec![test_activity("act1", "agent.spawned")];
        let session = SessionContext::new(None, None, Some("ws-1".to_string()));
        let batch = activity_to_batch(&activities, StorageTier::Central, &session).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), TIMELINE_COLUMN_COUNT);

        // Verify entry_kind = "activity"
        let kind_col = batch
            .column_by_name("entry_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kind_col.value(0), ENTRY_KIND_ACTIVITY);

        // Verify storage_tier = "central"
        let tier_col = batch
            .column_by_name("storage_tier")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tier_col.value(0), STORAGE_TIER_CENTRAL);

        let retention_col = batch
            .column_by_name("retention_class")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(retention_col.value(0), RETENTION_CLASS_EVENT);

        // Verify event_type
        let et_col = batch
            .column_by_name("event_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(et_col.value(0), "agent.spawned");
    }

    /// observability-timeline §5: a recorded type survives to the row. The
    /// `sync.frontier.advanced` activity wrote `entry_count` as `"5"` while its
    /// sibling log row wrote `5`, so one field held two types in one class.
    #[test]
    fn typed_metadata_keeps_its_json_type_in_metadata_json() {
        let mut activity = test_activity("act1", "sync.frontier.advanced");
        let ctx = &mut activity.context;
        for (key, text) in [
            ("entry_count", "5"),
            ("ratio", "0.25"),
            ("converged", "true"),
            ("zip_code", "007"),
            ("overwritten", "not-a-number"),
        ] {
            ctx.metadata.insert(key.to_string(), text.to_string());
        }
        for key in ["entry_count", "ratio", "converged", "overwritten"] {
            ctx.typed_metadata_keys.insert(key.to_string());
        }

        let session = SessionContext::default();
        let batch = activity_to_batch(&[activity], StorageTier::Central, &session).unwrap();
        let metadata_json = batch
            .column_by_name("metadata_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let row: serde_json::Value = serde_json::from_str(metadata_json.value(0)).unwrap();

        assert_eq!(row["entry_count"], serde_json::json!(5));
        assert_eq!(row["ratio"], serde_json::json!(0.25));
        assert_eq!(row["converged"], serde_json::json!(true));
        // Text stays text, however numeric it looks.
        assert_eq!(row["zip_code"], serde_json::json!("007"));
        // A typed key overwritten by a string keeps the string, not a null.
        assert_eq!(row["overwritten"], serde_json::json!("not-a-number"));
    }

    #[test]
    fn test_activity_timestamp_conversion() {
        let activities = vec![test_activity("act1", "agent.spawned")];
        let session = SessionContext::default();
        let batch = activity_to_batch(&activities, StorageTier::Central, &session).unwrap();

        let ts_col = batch
            .column_by_name("timestamp_us")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1704067200000000);

        let ended_col = batch
            .column_by_name("ended_at_us")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ended_col.value(0), 1704067201000000);
    }

    #[test]
    fn test_activity_description_column() {
        let activities = vec![test_activity("act1", "agent.spawned")];
        let session = SessionContext::default();
        let batch = activity_to_batch(&activities, StorageTier::Central, &session).unwrap();

        let desc_col = batch
            .column_by_name("description")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(desc_col.value(0), "test description");
    }

    #[test]
    fn test_activity_execution_columns_extract_from_metadata() {
        let mut activity = test_activity("act1", "agent.spawned");
        activity
            .context
            .metadata
            .insert("request_id".to_string(), "req-123".to_string());
        activity
            .context
            .metadata
            .insert("parent_request_id".to_string(), "req-122".to_string());
        activity.context.metadata.insert(
            "executed_by_json".to_string(),
            "{\"id\":\"software:test\"}".to_string(),
        );
        activity.context.metadata.insert(
            "traceparent".to_string(),
            "00-11111111111111111111111111111111-2222222222222222-01".to_string(),
        );
        activity
            .context
            .metadata
            .insert("tracestate".to_string(), "x0k=session-1".to_string());
        activity
            .context
            .metadata
            .insert("invocation_method".to_string(), "Triggered".to_string());
        activity
            .context
            .metadata
            .insert("artifact_id".to_string(), "artifact-1".to_string());
        activity
            .context
            .metadata
            .insert("document_id".to_string(), "document-1".to_string());
        activity
            .context
            .metadata
            .insert("document_revision_id".to_string(), "revision-1".to_string());
        activity
            .context
            .metadata
            .insert("file_path".to_string(), "/tmp/output.txt".to_string());
        activity.context.metadata.insert(
            "source_code_snapshot_id".to_string(),
            "x0k:code-snapshot/demo".to_string(),
        );
        activity.context.metadata.insert(
            "source_code_json".to_string(),
            "{\"inputs\":[],\"snapshot\":{\"snapshot_id\":\"x0k:code-snapshot/demo\"}}".to_string(),
        );
        activity.context.metadata.insert(
            "code_snapshot_json".to_string(),
            "{\"snapshot_id\":\"x0k:code-snapshot/demo\",\"inputs\":[],\"entries\":[]}".to_string(),
        );
        activity
            .context
            .metadata
            .insert("approval_id".to_string(), "x0k:approval/demo".to_string());
        activity.context.metadata.insert(
            "approval_record_json".to_string(),
            "{\"approval_id\":\"x0k:approval/demo\",\"scope\":\"workspace_merge\",\"policy_kind\":\"protected_paths\",\"policy_source\":\"config.merge.protected_paths\",\"decision\":\"approved\",\"approver_id\":\"agent:lane\",\"approver_name\":\"Lane\",\"matched_paths\":[\"adr/ADR-001.md\"],\"artifact_ids\":[],\"file_paths\":[],\"metadata\":{}}".to_string(),
        );
        let session = SessionContext::default();
        let batch = activity_to_batch(&[activity], StorageTier::Central, &session).unwrap();

        let request_id = batch
            .column_by_name("request_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_request_id = batch
            .column_by_name("parent_request_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let executed_by_json = batch
            .column_by_name("executed_by_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let traceparent = batch
            .column_by_name("traceparent")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tracestate = batch
            .column_by_name("tracestate")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let invocation_method = batch
            .column_by_name("invocation_method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let artifact_id = batch
            .column_by_name("artifact_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let document_id = batch
            .column_by_name("document_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let document_revision_id = batch
            .column_by_name("document_revision_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let file_path = batch
            .column_by_name("file_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let source_code_snapshot_id = batch
            .column_by_name("source_code_snapshot_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let source_code_json = batch
            .column_by_name("source_code_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let code_snapshot_json = batch
            .column_by_name("code_snapshot_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let approval_id = batch
            .column_by_name("approval_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let approval_record_json = batch
            .column_by_name("approval_record_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let metadata_json = batch
            .column_by_name("metadata_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(request_id.value(0), "req-123");
        assert_eq!(parent_request_id.value(0), "req-122");
        assert_eq!(executed_by_json.value(0), "{\"id\":\"software:test\"}");
        assert_eq!(
            traceparent.value(0),
            "00-11111111111111111111111111111111-2222222222222222-01"
        );
        assert_eq!(tracestate.value(0), "x0k=session-1");
        assert_eq!(invocation_method.value(0), "Triggered");
        assert_eq!(artifact_id.value(0), "artifact-1");
        assert_eq!(document_id.value(0), "document-1");
        assert_eq!(document_revision_id.value(0), "revision-1");
        assert_eq!(file_path.value(0), "/tmp/output.txt");
        assert_eq!(source_code_snapshot_id.value(0), "x0k:code-snapshot/demo");
        assert_eq!(
            source_code_json.value(0),
            "{\"inputs\":[],\"snapshot\":{\"snapshot_id\":\"x0k:code-snapshot/demo\"}}"
        );
        assert_eq!(
            code_snapshot_json.value(0),
            "{\"snapshot_id\":\"x0k:code-snapshot/demo\",\"inputs\":[],\"entries\":[]}"
        );
        assert_eq!(approval_id.value(0), "x0k:approval/demo");
        assert_eq!(
            approval_record_json.value(0),
            "{\"approval_id\":\"x0k:approval/demo\",\"scope\":\"workspace_merge\",\"policy_kind\":\"protected_paths\",\"policy_source\":\"config.merge.protected_paths\",\"decision\":\"approved\",\"approver_id\":\"agent:lane\",\"approver_name\":\"Lane\",\"matched_paths\":[\"adr/ADR-001.md\"],\"artifact_ids\":[],\"file_paths\":[],\"metadata\":{}}"
        );
        assert!(metadata_json.is_null(0));
    }

    #[test]
    fn test_log_execution_columns_use_direct_fields() {
        let batch = log_event_to_batch(&[test_log("log1", "provider.launch.configured")]).unwrap();

        let request_id = batch
            .column_by_name("request_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let parent_request_id = batch
            .column_by_name("parent_request_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let traceparent = batch
            .column_by_name("traceparent")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tracestate = batch
            .column_by_name("tracestate")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let executed_by_json = batch
            .column_by_name("executed_by_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let artifact_id = batch
            .column_by_name("artifact_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let native_artifact_id = batch
            .column_by_name("native_artifact_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let document_id = batch
            .column_by_name("document_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let document_revision_id = batch
            .column_by_name("document_revision_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let file_path = batch
            .column_by_name("file_path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let event_type = batch
            .column_by_name("event_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let metadata_json = batch
            .column_by_name("metadata_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(request_id.value(0), "req-123");
        assert_eq!(parent_request_id.value(0), "req-122");
        assert_eq!(
            traceparent.value(0),
            "00-11111111111111111111111111111111-2222222222222222-01"
        );
        assert_eq!(tracestate.value(0), "x0k=session-1");
        assert_eq!(executed_by_json.value(0), "{\"id\":\"software:test\"}");
        assert_eq!(artifact_id.value(0), "artifact-1");
        assert_eq!(native_artifact_id.value(0), "native-1");
        assert_eq!(document_id.value(0), "document-1");
        assert_eq!(document_revision_id.value(0), "revision-1");
        assert_eq!(file_path.value(0), "/tmp/output.txt");
        assert_eq!(event_type.value(0), "provider.launch.configured");
        assert!(metadata_json.is_null(0));
    }

    #[test]
    fn test_nullable_span_fields() {
        let mut span = test_span("s1");
        span.parent_span_id = None;
        span.duration_ms = None;
        span.function = None;
        span.module = None;
        span.source_file = None;
        span.source_line = None;
        span.workstream_id = None;
        span.intent_id = None;
        span.software_module_id = None;

        let batch = span_data_to_batch(&[span]).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_derive_level() {
        assert_eq!(derive_level("agent.spawned"), "info");
        assert_eq!(derive_level("agent.failed"), "error");
        assert_eq!(derive_level("system.error"), "error");
        assert_eq!(derive_level("debug.trace"), "debug");
    }

    /// A promoted `warn!` carries its real severity in `activity.level`, and
    /// that beats sniffing the type string. Without this, a warn! promoted
    /// under a type like `grove.fold_cache.unusable` would land as `info` and
    /// `get_recent_errors` (which filters `UPPER(level) IN ('ERROR','WARN')`)
    /// would never surface it.
    #[test]
    fn explicit_activity_level_overrides_type_sniffing() {
        let mut activity = test_activity("promoted-1", "grove.fold_cache.unusable");
        activity
            .context
            .metadata
            .insert("activity.level".to_string(), "warn".to_string());

        let batch = activity_to_batch(
            &[activity],
            StorageTier::Central,
            &crate::SessionContext::default(),
        )
        .expect("batch");

        let levels = batch
            .column_by_name("level")
            .expect("level column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(
            levels.value(0),
            "warn",
            "the type string says nothing about severity; the explicit level does"
        );

        // Nothing explicit → unchanged behaviour.
        let plain = test_activity("plain-1", "agent.failed");
        let batch = activity_to_batch(
            &[plain],
            StorageTier::Central,
            &crate::SessionContext::default(),
        )
        .expect("batch");
        let levels = batch
            .column_by_name("level")
            .expect("level column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(levels.value(0), "error");
    }
}
