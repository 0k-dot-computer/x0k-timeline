//! OTLP over HTTP on localhost: how a process that cannot link the emit role
//! hands the writer its records (`timeline-public-surface` §2).
//!
//! The receiver takes OTLP/HTTP JSON (`application/json`) at `/v1/traces`,
//! `/v1/logs` and `/v1/metrics` on the loopback interface only, at the OTLP port plus the
//! profile's offset. Each span becomes a span record and each log record (an
//! event included) a log record, through `span_record` and `log_record`,
//! the one place the OTLP mapping lives, and the records go to the writer's
//! own ingest. A record that cannot be a record is refused by name in OTLP's
//! partial-success answer while the rest of its batch is kept. A body in any
//! other encoding is refused with 415 and writes nothing.
//!
//! The mapping, which the surface promises:
//!
//! | OTLP | Record today | §1 record field |
//! |---|---|---|
//! | `traceId`, `spanId`, `parentSpanId`, lowercased | `trace_id`, `span_id`, `parent_span_id` | the same |
//! | `traceState` | `tracestate` (log records) | `tracestate` |
//! | resource `service.name` | `software_module_id` | `process` |
//! | resource `process.pid` | `pid` | `process` |
//! | scope name | `target` | `target` |
//! | span `name` | `span_name` | `event_type` |
//! | start, end | `start_time`, `duration_ms` | `started_at_us`, `ended_at_us` |
//! | log `timeUnixNano` (else observed) | `timestamp_us` | `timestamp_us` |
//! | severity text (else number) | `level` | `level` |
//! | log body | `message` | `description` |
//! | `eventName`, else attribute `event.name` | `event_type` | `event_type` |
//! | attribute `session.id` | `session_id` | `session_id` |
//! | every other attribute, resource and record | `fields` / `fields_json`, under its own key | `attributes` |
//! | metric name | `event_type` | `event_type` |
//! | a sum or gauge data point's value, unit, kind, temporality and monotonicity | `fields_json`: `value`, `unit`, `kind`, `temporality`, `monotonic` | `fields` |
//!
//! Each data point of a sum or a gauge is one row (a log record today), so
//! Claude Code's token and cost usage land keyed by session (`session.id`)
//! and account (`user.account_uuid`, under its own key). The point's own
//! fields are written after its attributes and win a clash of names. A
//! histogram, exponential histogram or summary has no row shape yet, and each
//! of its points is refused by name.
//!
//! A log record named as a process observation (`process.exit` and the rest,
//! `timeline-public-surface` §8) has to be one: its attributes are checked and
//! a misfit refused by name, and it is kept as an event rather than a log.
//!
//! An attribute keyed `x0k.` is never taken: the prefix is x0k's own
//! namespace, and no client of this receiver writes into it.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::sync::Notify;
use x0k_timeline_record::process;
use x0k_timeline_record::record::{IngestTimeline, LogEventData, TimelineRecord};
use x0k_timeline_record::span::SpanData;
use x0k_timeline_record::JsonValue;

use crate::TimelineWriter;

/// The OTLP/HTTP port every OpenTelemetry exporter defaults to.
pub const OTLP_HTTP_PORT: u16 = 4318;

/// The prefix reserved to x0k: never taken from OTLP input.
const RESERVED_PREFIX: &str = "x0k.";

/// The address the receiver listens on at `port`: loopback, always.
pub fn listen_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// The receiver's routes, handing what they take to `writer`.
pub fn receiver_routes(writer: TimelineWriter) -> Router {
    Router::new()
        .route("/v1/traces", post(traces))
        .route("/v1/logs", post(logs))
        .route("/v1/metrics", post(metrics))
        .with_state(Arc::new(writer))
}

/// Serve the receiver on `listener` until `shutdown` is notified.
pub async fn serve(listener: tokio::net::TcpListener, writer: TimelineWriter, shutdown: Arc<Notify>) {
    let served = axum::serve(listener, receiver_routes(writer))
        .with_graceful_shutdown(async move { shutdown.notified().await })
        .await;
    if let Err(error) = served {
        tracing::error!(event_type = "timeline.otlp.serve_failed", %error, "the OTLP receiver stopped");
    }
}

async fn traces(State(writer): State<Arc<TimelineWriter>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(refused) = refuse_encoding(&headers) {
        return refused;
    }
    let request: TracesRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => return bad_request(format!("not an OTLP traces request: {error}")),
    };
    let mut records = Vec::new();
    let mut refused = Refused::default();
    for resource in &request.resource_spans {
        let process = Process::of(resource.resource.as_ref());
        for scope in &resource.scope_spans {
            let target = scope.scope.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            for span in &scope.spans {
                match span_record(&process, &target, span) {
                    Ok(record) => records.push(record),
                    Err(why) => refused.note(why.to_string()),
                }
            }
        }
    }
    hand_over(&writer, records, refused, "rejectedSpans").await
}

async fn logs(State(writer): State<Arc<TimelineWriter>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(refused) = refuse_encoding(&headers) {
        return refused;
    }
    let request: LogsRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => return bad_request(format!("not an OTLP logs request: {error}")),
    };
    let mut records = Vec::new();
    let mut refused = Refused::default();
    for resource in &request.resource_logs {
        let process = Process::of(resource.resource.as_ref());
        for scope in &resource.scope_logs {
            let target = scope.scope.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            for log in &scope.log_records {
                match log_record(&process, &target, log) {
                    Ok(record) => records.push(record),
                    Err(why) => refused.note(why.to_string()),
                }
            }
        }
    }
    hand_over(&writer, records, refused, "rejectedLogRecords").await
}

async fn metrics(State(writer): State<Arc<TimelineWriter>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(refused) = refuse_encoding(&headers) {
        return refused;
    }
    let request: MetricsRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => return bad_request(format!("not an OTLP metrics request: {error}")),
    };
    let mut records = Vec::new();
    let mut refused = Refused::default();
    for resource in &request.resource_metrics {
        let process = Process::of(resource.resource.as_ref());
        for scope in &resource.scope_metrics {
            let target = scope.scope.as_ref().map(|s| s.name.clone()).unwrap_or_default();
            for metric in &scope.metrics {
                for row in metric_rows(&process, &target, metric) {
                    match row {
                        Ok(record) => records.push(record),
                        Err(why) => refused.note(why.to_string()),
                    }
                }
            }
        }
    }
    hand_over(&writer, records, refused, "rejectedDataPoints").await
}

/// A body this receiver cannot read is refused before anything is written.
fn refuse_encoding(headers: &HeaderMap) -> Option<Response> {
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    if content_type.split(';').next().is_some_and(|t| t.trim().eq_ignore_ascii_case("application/json")) {
        return None;
    }
    Some(
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("this receiver takes OTLP/HTTP JSON (Content-Type: application/json), not {content_type:?}"),
        )
            .into_response(),
    )
}

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, message).into_response()
}

/// Records refused from a batch: how many, and the first reason.
#[derive(Default)]
struct Refused {
    count: u64,
    first: Option<String>,
}

impl Refused {
    fn note(&mut self, why: String) {
        self.count += 1;
        self.first.get_or_insert(why);
    }
}

/// Hand `records` to the writer and answer in OTLP's shape: `{}` when all of
/// the batch was taken, a partial success naming what was not.
async fn hand_over(writer: &TimelineWriter, records: Vec<TimelineRecord>, mut refused: Refused, field: &str) -> Response {
    let offered = records.len();
    let taken = match writer.ingest_timeline(&IngestTimeline { records }).await {
        Ok(result) => result.accepted,
        Err(error) => {
            return (StatusCode::SERVICE_UNAVAILABLE, format!("the writer took nothing: {error}")).into_response()
        }
    };
    if taken < offered {
        refused.count += (offered - taken) as u64;
        refused.first.get_or_insert_with(|| "the writer is shutting down".to_string());
    }
    if refused.count > 0 {
        tracing::warn!(event_type = "timeline.otlp.refused", refused = refused.count,
            reason = refused.first.as_deref().unwrap_or(""), "OTLP records refused");
    }
    let answer = match refused.first {
        None => json!({}),
        Some(reason) => {
            let mut partial = Map::new();
            partial.insert(field.to_string(), json!(refused.count));
            partial.insert("errorMessage".to_string(), Value::String(reason));
            json!({ "partialSuccess": partial })
        }
    };
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], answer.to_string()).into_response()
}

/// What a resource says about the process that sent it.
struct Process {
    service: Option<String>,
    pid: Option<u32>,
    attributes: Map<String, Value>,
}

impl Process {
    fn of(resource: Option<&Resource>) -> Self {
        let mut attributes = attribute_map(resource.map(|r| r.attributes.as_slice()).unwrap_or(&[]));
        let service = attributes.remove("service.name").and_then(|v| v.as_str().map(str::to_string));
        let pid = attributes.remove("process.pid").and_then(|v| v.as_u64()).and_then(|p| u32::try_from(p).ok());
        Process { service, pid, attributes }
    }
}

/// A span as a span record, or why it cannot be one.
fn span_record(process: &Process, target: &str, span: &Span) -> Result<TimelineRecord, Misfit> {
    let trace_id = w3c_id(&span.trace_id, 32).ok_or_else(|| Misfit::TraceId(span.name.clone()))?;
    let span_id = w3c_id(&span.span_id, 16).ok_or_else(|| Misfit::SpanId(span.name.clone()))?;
    let start = span.start_time_unix_nano.0 / 1_000;
    if start == 0 {
        return Err(Misfit::NoStart(span.name.clone()));
    }
    let end = span.end_time_unix_nano.0 / 1_000;
    let mut fields = process.attributes.clone();
    fields.extend(attribute_map(&span.attributes));
    let session_id = take_session(&mut fields);
    Ok(TimelineRecord::Span(SpanData {
        span_id,
        trace_id,
        parent_span_id: w3c_id(&span.parent_span_id, 16),
        start_time: start,
        duration_ms: (end >= start && end > 0).then(|| (end - start) / 1_000),
        level: "INFO".to_string(),
        span_name: span.name.clone(),
        target: target.to_string(),
        fields: fields.into_iter().map(|(key, value)| (key, JsonValue::from(value))).collect::<HashMap<_, _>>(),
        software_module_id: process.service.clone(),
        session_id,
        pid: process.pid,
        ..SpanData::default()
    }))
}

/// A log record or event as a log record, or why it cannot be one.
fn log_record(process: &Process, target: &str, log: &LogRecord) -> Result<TimelineRecord, Misfit> {
    let nanos = if log.time_unix_nano.0 != 0 { log.time_unix_nano.0 } else { log.observed_time_unix_nano.0 };
    if nanos == 0 {
        return Err(Misfit::NoTime);
    }
    let event_type = log
        .event_name
        .clone()
        .filter(|name| !name.is_empty())
        .or_else(|| attribute_text(&log.attributes, "event.name").map(str::to_string));
    let attributes = attribute_map(&log.attributes);
    if let Some(event_type) = &event_type {
        process::check_observation(event_type, &attributes).map_err(Misfit::Observation)?;
    }
    let mut fields = process.attributes.clone();
    fields.extend(attributes);
    let session_id = take_session(&mut fields);
    let trace_id = w3c_id(&log.trace_id, 32);
    let span_id = w3c_id(&log.span_id, 16);
    Ok(TimelineRecord::LogEvent(LogEventData {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp_us: nanos / 1_000,
        level: severity(log),
        message: log.body.as_ref().map(|body| match any_value(body) {
            Value::String(text) => text,
            other => other.to_string(),
        }),
        fields_json: (!fields.is_empty()).then(|| Value::Object(fields).to_string()),
        target: target.to_string(),
        software_module_id: process.service.clone(),
        session_id,
        pid: process.pid,
        traceparent: trace_id.as_ref().zip(span_id.as_ref()).map(|(t, s)| format!("00-{t}-{s}-01")),
        trace_id,
        tracestate: log.trace_state.clone().filter(|s| !s.is_empty()),
        event_type,
        ..LogEventData::default()
    }))
}

/// Each data point of a sum or a gauge as a row, or why it cannot be one. A
/// histogram, exponential histogram or summary has no row shape yet: each of
/// its points is refused, by name.
fn metric_rows(process: &Process, target: &str, metric: &Metric) -> Vec<Result<TimelineRecord, Misfit>> {
    let refuse_all = |kind: &'static str, points: usize| {
        (0..points).map(|_| Err(Misfit::UnsupportedMetric(metric.name.clone(), kind))).collect()
    };
    if let Some(sum) = &metric.sum {
        let temporality = match sum.aggregation_temporality {
            1 => "delta",
            2 => "cumulative",
            _ => "unspecified",
        };
        return sum.data_points.iter()
            .map(|point| point_row(process, target, metric, point, "sum", Some((temporality, sum.is_monotonic))))
            .collect();
    }
    if let Some(gauge) = &metric.gauge {
        return gauge.data_points.iter().map(|point| point_row(process, target, metric, point, "gauge", None)).collect();
    }
    if let Some(histogram) = &metric.histogram {
        return refuse_all("histogram", histogram.data_points.len());
    }
    if let Some(histogram) = &metric.exponential_histogram {
        return refuse_all("exponential histogram", histogram.data_points.len());
    }
    if let Some(summary) = &metric.summary {
        return refuse_all("summary", summary.data_points.len());
    }
    Vec::new()
}

/// One number data point as a row: the metric's name its event type, the
/// point's attributes under their own keys, and its value, unit and kind
/// (and, for a sum, what it counts over) beside them.
fn point_row(
    process: &Process,
    target: &str,
    metric: &Metric,
    point: &NumberDataPoint,
    kind: &str,
    sum: Option<(&str, bool)>,
) -> Result<TimelineRecord, Misfit> {
    let nanos = if point.time_unix_nano.0 != 0 { point.time_unix_nano.0 } else { point.start_time_unix_nano.0 };
    if nanos == 0 {
        return Err(Misfit::NoPointTime(metric.name.clone()));
    }
    let value = match (point.as_double, &point.as_int) {
        (Some(number), _) => json!(number),
        (None, Some(number)) => Value::from(number.0),
        (None, None) => return Err(Misfit::NoValue(metric.name.clone())),
    };
    let mut fields = process.attributes.clone();
    fields.extend(attribute_map(&point.attributes));
    let session_id = take_session(&mut fields);
    fields.insert("value".into(), value);
    fields.insert("unit".into(), Value::String(metric.unit.clone()));
    fields.insert("kind".into(), Value::String(kind.into()));
    if let Some((temporality, monotonic)) = sum {
        fields.insert("temporality".into(), Value::String(temporality.into()));
        fields.insert("monotonic".into(), Value::Bool(monotonic));
    }
    if point.start_time_unix_nano.0 != 0 {
        fields.insert("start_time_us".into(), json!(point.start_time_unix_nano.0 / 1_000));
    }
    Ok(TimelineRecord::LogEvent(LogEventData {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp_us: nanos / 1_000,
        level: "INFO".to_string(),
        fields_json: Some(Value::Object(fields).to_string()),
        target: target.to_string(),
        software_module_id: process.service.clone(),
        session_id,
        pid: process.pid,
        event_type: Some(metric.name.clone()),
        ..LogEventData::default()
    }))
}

/// Why a span or log record cannot be a record.
#[derive(Debug)]
enum Misfit {
    TraceId(String),
    SpanId(String),
    NoStart(String),
    NoTime,
    NoPointTime(String),
    NoValue(String),
    UnsupportedMetric(String, &'static str),
    Observation(process::Misfit),
}

impl std::fmt::Display for Misfit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Misfit::TraceId(span) => write!(f, "span {span:?}: traceId is not 16 bytes of hex"),
            Misfit::SpanId(span) => write!(f, "span {span:?}: spanId is not 8 bytes of hex"),
            Misfit::NoStart(span) => write!(f, "span {span:?}: no startTimeUnixNano"),
            Misfit::Observation(misfit) => write!(f, "{misfit}"),
            Misfit::NoTime => write!(f, "a log record with neither timeUnixNano nor observedTimeUnixNano"),
            Misfit::NoPointTime(metric) => write!(f, "a data point of {metric:?} with neither timeUnixNano nor startTimeUnixNano"),
            Misfit::NoValue(metric) => write!(f, "a data point of {metric:?} with neither asDouble nor asInt"),
            Misfit::UnsupportedMetric(metric, kind) => write!(f, "{metric:?} is a {kind}, which this receiver does not take; send sums and gauges"),
        }
    }
}

/// The text value of the attribute `key`, read from the request as sent.
fn attribute_text<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
    attributes.iter().find(|a| a.key == key)?.value.as_ref()?.string_value.as_deref()
}

/// The session a record ran in, taken out of its attributes.
fn take_session(fields: &mut Map<String, Value>) -> Option<String> {
    fields.remove("session.id").and_then(|v| v.as_str().map(str::to_string))
}

/// A W3C id of `hex_len` hex digits, lowercased; `None` when absent or all zero.
fn w3c_id(raw: &str, hex_len: usize) -> Option<String> {
    let id = raw.trim().to_ascii_lowercase();
    (id.len() == hex_len && id.bytes().all(|b| b.is_ascii_hexdigit()) && id.bytes().any(|b| b != b'0')).then_some(id)
}

/// OTLP's severity as a tracing level: its text when given, else its number.
fn severity(log: &LogRecord) -> String {
    let named = match log.severity_number {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=24 => "ERROR",
        _ => "INFO",
    };
    match log.severity_text.as_deref().map(str::to_ascii_uppercase).as_deref() {
        Some("TRACE") => "TRACE",
        Some("DEBUG") => "DEBUG",
        Some("INFO" | "INFORMATION") => "INFO",
        Some("WARN" | "WARNING") => "WARN",
        Some("ERROR" | "FATAL") => "ERROR",
        _ => named,
    }
    .to_string()
}

/// Attributes by key, every `x0k.` key dropped.
fn attribute_map(attributes: &[KeyValue]) -> Map<String, Value> {
    let mut map = Map::new();
    for attribute in attributes {
        if attribute.key.starts_with(RESERVED_PREFIX) {
            tracing::debug!(event_type = "timeline.otlp.reserved_attribute", key = %attribute.key,
                "an OTLP attribute in x0k's namespace was dropped");
            continue;
        }
        map.insert(attribute.key.clone(), attribute.value.as_ref().map(any_value).unwrap_or(Value::Null));
    }
    map
}

/// An OTLP `AnyValue` as JSON.
fn any_value(value: &AnyValue) -> Value {
    if let Some(text) = &value.string_value {
        Value::String(text.clone())
    } else if let Some(flag) = value.bool_value {
        Value::Bool(flag)
    } else if let Some(number) = &value.int_value {
        Value::from(number.0)
    } else if let Some(number) = value.double_value {
        json!(number)
    } else if let Some(array) = &value.array_value {
        Value::Array(array.values.iter().map(any_value).collect())
    } else if let Some(list) = &value.kvlist_value {
        Value::Object(attribute_map(&list.values))
    } else if let Some(bytes) = &value.bytes_value {
        Value::String(bytes.clone())
    } else {
        Value::Null
    }
}

// The OTLP/HTTP JSON request shapes (opentelemetry-proto's JSON mapping:
// lowerCamelCase names, 64-bit integers as strings or numbers, ids as hex).

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TracesRequest {
    #[serde(default)]
    resource_spans: Vec<ResourceSpans>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceSpans {
    resource: Option<Resource>,
    #[serde(default)]
    scope_spans: Vec<ScopeSpans>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeSpans {
    scope: Option<Scope>,
    #[serde(default)]
    spans: Vec<Span>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Span {
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    parent_span_id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    start_time_unix_nano: U64,
    #[serde(default)]
    end_time_unix_nano: U64,
    #[serde(default)]
    attributes: Vec<KeyValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogsRequest {
    #[serde(default)]
    resource_logs: Vec<ResourceLogs>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceLogs {
    resource: Option<Resource>,
    #[serde(default)]
    scope_logs: Vec<ScopeLogs>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeLogs {
    scope: Option<Scope>,
    #[serde(default)]
    log_records: Vec<LogRecord>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogRecord {
    #[serde(default)]
    time_unix_nano: U64,
    #[serde(default)]
    observed_time_unix_nano: U64,
    #[serde(default)]
    severity_number: i32,
    severity_text: Option<String>,
    body: Option<AnyValue>,
    #[serde(default)]
    attributes: Vec<KeyValue>,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    trace_state: Option<String>,
    event_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetricsRequest {
    #[serde(default)]
    resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceMetrics {
    resource: Option<Resource>,
    #[serde(default)]
    scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeMetrics {
    scope: Option<Scope>,
    #[serde(default)]
    metrics: Vec<Metric>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metric {
    #[serde(default)]
    name: String,
    #[serde(default)]
    unit: String,
    sum: Option<Sum>,
    gauge: Option<Gauge>,
    histogram: Option<Points>,
    exponential_histogram: Option<Points>,
    summary: Option<Points>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Sum {
    #[serde(default)]
    data_points: Vec<NumberDataPoint>,
    #[serde(default)]
    aggregation_temporality: i32,
    #[serde(default)]
    is_monotonic: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Gauge {
    #[serde(default)]
    data_points: Vec<NumberDataPoint>,
}

/// The points of a kind this receiver refuses: only how many there are.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Points {
    #[serde(default)]
    data_points: Vec<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NumberDataPoint {
    #[serde(default)]
    attributes: Vec<KeyValue>,
    #[serde(default)]
    start_time_unix_nano: U64,
    #[serde(default)]
    time_unix_nano: U64,
    as_double: Option<f64>,
    as_int: Option<I64>,
}

#[derive(Deserialize)]
struct Resource {
    #[serde(default)]
    attributes: Vec<KeyValue>,
}

#[derive(Deserialize)]
struct Scope {
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: Option<AnyValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnyValue {
    string_value: Option<String>,
    bool_value: Option<bool>,
    int_value: Option<I64>,
    double_value: Option<f64>,
    array_value: Option<ArrayValue>,
    kvlist_value: Option<KeyValueList>,
    bytes_value: Option<String>,
}

#[derive(Deserialize)]
struct ArrayValue {
    #[serde(default)]
    values: Vec<AnyValue>,
}

#[derive(Deserialize)]
struct KeyValueList {
    #[serde(default)]
    values: Vec<KeyValue>,
}

/// A 64-bit unsigned integer, as JSON writes it: a string or a number.
#[derive(Default)]
struct U64(u64);

impl<'de> Deserialize<'de> for U64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Number(n) => n.as_u64().map(U64).ok_or_else(|| serde::de::Error::custom("not a u64")),
            Value::String(s) => s.parse().map(U64).map_err(serde::de::Error::custom),
            Value::Null => Ok(U64(0)),
            other => Err(serde::de::Error::custom(format!("not a u64: {other}"))),
        }
    }
}

/// A 64-bit signed integer, as JSON writes it: a string or a number.
struct I64(i64);

impl<'de> Deserialize<'de> for I64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Number(n) => n.as_i64().map(I64).ok_or_else(|| serde::de::Error::custom("not an i64")),
            Value::String(s) => s.parse().map(I64).map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::custom(format!("not an i64: {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rows of the mapping table that name a §1 field, so retargeting to
    /// the published record (a265f9fe) is a change of the right-hand column:
    /// each OTLP input here lands in today's field named beside it.
    #[test]
    fn the_mapping_lands_each_input_where_the_table_says() {
        let process = Process::of(Some(&Resource {
            attributes: vec![
                KeyValue { key: "service.name".into(), value: Some(text("svc")) },
                KeyValue { key: "process.pid".into(), value: Some(int(42)) },
                KeyValue { key: "host.arch".into(), value: Some(text("amd64")) },
            ],
        }));
        let log = LogRecord {
            time_unix_nano: U64(2_000_000_000),
            observed_time_unix_nano: U64(0),
            severity_number: 13,
            severity_text: None,
            body: Some(text("said")),
            attributes: vec![
                KeyValue { key: "session.id".into(), value: Some(text("s-1")) },
                KeyValue { key: "event.name".into(), value: Some(text("tool_result")) },
                KeyValue { key: "x0k.intent_id".into(), value: Some(text("claimed")) },
            ],
            trace_id: "0AF7651916CD43DD8448EB211C80319C".into(),
            span_id: "B7AD6B7169203331".into(),
            trace_state: Some("congo=t61rcWkgMzE".into()),
            event_name: None,
        };
        let TimelineRecord::LogEvent(row) = log_record(&process, "scope", &log).unwrap() else { unreachable!() };
        assert_eq!(row.software_module_id.as_deref(), Some("svc"), "service.name → process");
        assert_eq!(row.pid, Some(42), "process.pid → process");
        assert_eq!(row.target, "scope", "scope → target");
        assert_eq!(row.timestamp_us, 2_000_000, "timeUnixNano → timestamp_us");
        assert_eq!(row.level, "WARN", "severity → level");
        assert_eq!(row.message.as_deref(), Some("said"), "body → description");
        assert_eq!(row.event_type.as_deref(), Some("tool_result"), "event.name → event_type");
        assert_eq!(row.session_id.as_deref(), Some("s-1"), "session.id → session_id");
        assert_eq!(row.trace_id.as_deref(), Some("0af7651916cd43dd8448eb211c80319c"), "traceId → trace_id");
        assert_eq!(row.tracestate.as_deref(), Some("congo=t61rcWkgMzE"), "traceState → tracestate");
        let fields: Value = serde_json::from_str(row.fields_json.as_deref().unwrap()).unwrap();
        assert_eq!(fields["host.arch"], "amd64", "other attributes → attributes, under their own keys");
        assert!(fields.get("x0k.intent_id").is_none(), "x0k. is never taken");
    }

    fn text(value: &str) -> AnyValue {
        AnyValue { string_value: Some(value.into()), ..empty() }
    }

    fn int(value: i64) -> AnyValue {
        AnyValue { int_value: Some(I64(value)), ..empty() }
    }

    fn empty() -> AnyValue {
        AnyValue {
            string_value: None,
            bool_value: None,
            int_value: None,
            double_value: None,
            array_value: None,
            kvlist_value: None,
            bytes_value: None,
        }
    }
}
