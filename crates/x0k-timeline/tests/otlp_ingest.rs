//! Rung-1 oracle for the timeline writer's OTLP receiver
//! (`timeline-public-surface` §2).
//!
//! The claim, stated so it can fail: **a process that cannot link the emit
//! role sends OTLP/HTTP JSON to the writer on localhost, and each span and
//! log record becomes one timeline record: the W3C identifiers in the trace
//! fields, the resource's service in the software module, the scope in the
//! target, and every other attribute under its own key; an `x0k.` attribute
//! is never taken; a record that cannot be a record is refused by name while
//! the rest of its batch is kept; a body in another encoding is refused and
//! writes nothing.**
//!
//! The bodies are real client output: Claude Code's own OTLP/HTTP JSON
//! export (its account identity replaced with synthetic values), and the
//! opentelemetry-proto repository's example requests. Each request goes over
//! a real loopback socket to the receiver, which hands its records to the
//! writer's ingest; the records are read off the writer's channel.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use x0k_timeline_query::{otlp, TimelineWriter};
use x0k_timeline_record::record::TimelineRecord;

const CLAUDE_CODE_LOGS: &str = include_str!("fixtures/otlp/claude-code-logs.json");
const UPSTREAM_TRACE: &str = include_str!("fixtures/otlp/upstream-trace.json");
const UPSTREAM_LOGS: &str = include_str!("fixtures/otlp/upstream-logs.json");
const CLAUDE_CODE_METRICS: &str = include_str!("fixtures/otlp/claude-code-metrics.json");
const UPSTREAM_METRICS: &str = include_str!("fixtures/otlp/upstream-metrics.json");

/// A receiver on a fresh loopback port, and the channel its writer feeds.
async fn receiver() -> (std::net::SocketAddr, mpsc::Receiver<TimelineRecord>, Arc<Notify>) {
    let (tx, rx) = mpsc::channel(1024);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Arc::new(Notify::new());
    tokio::spawn(otlp::serve(listener, TimelineWriter::new(tx), shutdown.clone()));
    (addr, rx, shutdown)
}

/// POST `body` to `path` as `content_type`; the status and the response body.
async fn post(addr: std::net::SocketAddr, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default().to_string();
    (status, body)
}

/// Every record the writer has been handed so far.
async fn drain(rx: &mut mpsc::Receiver<TimelineRecord>) -> Vec<TimelineRecord> {
    let mut records = Vec::new();
    while let Ok(Some(record)) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        records.push(record);
    }
    records
}

#[tokio::test]
async fn a_span_becomes_a_span_record_with_its_trace_and_attributes() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let (status, body) = post(addr, "/v1/traces", "application/json", UPSTREAM_TRACE.as_bytes()).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body.contains("rejected"), "nothing refused: {body}");

    let records = drain(&mut rx).await;
    let [TimelineRecord::Span(span)] = records.as_slice() else { panic!("one span record: {records:?}") };
    assert_eq!(span.trace_id, "5b8efff798038103d269b633813fc60c", "W3C ids, lowercase");
    assert_eq!(span.span_id, "eee19b7ec3c1b174");
    assert_eq!(span.parent_span_id.as_deref(), Some("eee19b7ec3c1b173"));
    assert_eq!(span.span_name, "I'm a server span");
    assert_eq!(span.start_time, 1_544_712_660_000_000, "microseconds");
    assert_eq!(span.duration_ms, Some(1_000));
    assert_eq!(span.software_module_id.as_deref(), Some("my.service"), "the resource's service");
    assert_eq!(span.target, "my.library", "the scope");
    assert_eq!(span.fields["my.span.attr"].to_value().unwrap(), serde_json::json!("some value"), "an attribute under its own key");
}

#[tokio::test]
async fn claude_codes_events_become_log_records_keyed_by_their_session() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let (status, body) = post(addr, "/v1/logs", "application/json", CLAUDE_CODE_LOGS.as_bytes()).await;
    assert_eq!(status, 200, "{body}");

    let records = drain(&mut rx).await;
    assert_eq!(records.len(), 10, "one record per log record Claude Code sent");
    for record in &records {
        let TimelineRecord::LogEvent(log) = record else { panic!("a log record: {record:?}") };
        assert_eq!(log.session_id.as_deref(), Some("00000000-0000-4000-8000-0000000005e5"), "the session");
        assert_eq!(log.software_module_id.as_deref(), Some("claude-code"), "the resource's service");
        assert_eq!(log.target, "com.anthropic.claude_code.events", "the scope");
        assert!(log.timestamp_us > 1_700_000_000_000_000, "microseconds: {}", log.timestamp_us);
        assert!(log.event_type.is_some(), "the event's name: {log:?}");
        assert!(log.message.as_deref().is_some_and(|m| m.starts_with("claude_code.")), "its body: {log:?}");
        let fields: serde_json::Value = serde_json::from_str(log.fields_json.as_deref().unwrap()).unwrap();
        assert_eq!(fields["user.account_uuid"], "00000000-0000-4000-8000-000000000001", "attributes under their own keys");
    }
    let names: Vec<_> = records.iter().filter_map(|r| match r {
        TimelineRecord::LogEvent(log) => log.event_type.clone(),
        _ => None,
    }).collect();
    assert!(names.contains(&"hook_execution_start".to_string()), "{names:?}");
}

#[tokio::test]
async fn an_x0k_attribute_is_never_taken_from_otlp() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let claimed = UPSTREAM_LOGS.replacen("\"string.attribute\"", "\"x0k.intent_id\"", 1);
    let (status, body) = post(addr, "/v1/logs", "application/json", claimed.as_bytes()).await;
    assert_eq!(status, 200, "{body}");

    let records = drain(&mut rx).await;
    assert!(!records.is_empty());
    for record in &records {
        let TimelineRecord::LogEvent(log) = record else { panic!("a log record") };
        let fields = log.fields_json.as_deref().unwrap_or("{}");
        assert!(!fields.contains("x0k."), "the reserved prefix is dropped: {fields}");
        assert_eq!(log.intent_id, None, "and it sets nothing of x0k's");
    }
}

#[tokio::test]
async fn a_span_with_no_trace_is_refused_by_name_and_the_rest_kept() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let mut request: serde_json::Value = serde_json::from_str(UPSTREAM_TRACE).unwrap();
    let spans = request["resourceSpans"][0]["scopeSpans"][0]["spans"].as_array_mut().unwrap();
    let mut broken = spans[0].clone();
    broken["traceId"] = serde_json::json!("");
    spans.push(broken);
    let (status, body) = post(addr, "/v1/traces", "application/json", request.to_string().as_bytes()).await;
    assert_eq!(status, 200, "a partial success is still a success: {body}");
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["partialSuccess"]["rejectedSpans"], 1, "{body}");
    assert!(answer["partialSuccess"]["errorMessage"].as_str().unwrap().contains("traceId"), "named: {body}");
    assert_eq!(drain(&mut rx).await.len(), 1, "the good span is kept");
}

#[tokio::test]
async fn a_protobuf_body_is_refused_and_writes_nothing() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let (status, body) = post(addr, "/v1/traces", "application/x-protobuf", b"\x0a\x00").await;
    assert_eq!(status, 415, "{body}");
    assert!(body.contains("application/json"), "says what it takes: {body}");
    assert!(drain(&mut rx).await.is_empty());
}

#[test]
fn the_listener_is_loopback_at_the_port_it_is_given() {
    assert_eq!(otlp::listen_addr(otlp::OTLP_HTTP_PORT).to_string(), "127.0.0.1:4318");
    assert_eq!(otlp::listen_addr(5000).to_string(), "127.0.0.1:5000");
}

/// A metric row's fields, parsed.
fn fields_of(record: &TimelineRecord) -> serde_json::Value {
    let TimelineRecord::LogEvent(log) = record else { panic!("a log record: {record:?}") };
    serde_json::from_str(log.fields_json.as_deref().unwrap()).unwrap()
}

#[tokio::test]
async fn claude_codes_token_and_cost_usage_become_rows_keyed_by_session_and_account() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let (status, body) = post(addr, "/v1/metrics", "application/json", CLAUDE_CODE_METRICS.as_bytes()).await;
    assert_eq!(status, 200, "{body}");
    assert!(!body.contains("rejected"), "nothing refused: {body}");

    let records = drain(&mut rx).await;
    assert_eq!(records.len(), 5, "one row per data point: one cost, four token kinds");
    let mut tokens = std::collections::BTreeMap::new();
    for record in &records {
        let TimelineRecord::LogEvent(row) = record else { panic!("a row: {record:?}") };
        assert_eq!(row.session_id.as_deref(), Some("00000000-0000-4000-8000-0000000005e5"), "keyed by session");
        assert_eq!(row.timestamp_us, 1_793_218_982_843_000, "the point's time, in microseconds");
        let fields = fields_of(record);
        assert_eq!(fields["user.account_uuid"], "00000000-0000-4000-8000-000000000001", "and by account");
        assert_eq!(fields["model"], "claude-opus-5-5");
        assert_eq!(fields["temporality"], "delta", "each point says what it counts");
        match row.event_type.as_deref() {
            Some("claude_code.cost.usage") => {
                assert_eq!(fields["value"], 0.0872746);
                assert_eq!(fields["unit"], "USD");
            }
            Some("claude_code.token.usage") => {
                assert_eq!(fields["unit"], "tokens");
                tokens.insert(fields["type"].as_str().unwrap().to_string(), fields["value"].as_f64().unwrap());
            }
            other => panic!("a usage metric: {other:?}"),
        }
    }
    assert_eq!(
        tokens.into_iter().collect::<Vec<_>>(),
        vec![("cacheCreation".into(), 10_615.0), ("cacheRead".into(), 11_333.0), ("input".into(), 2.0), ("output".into(), 4.0)],
        "every token kind, with its count"
    );
}

#[tokio::test]
async fn a_histogram_is_refused_by_name_while_sums_and_gauges_are_kept() {
    let (addr, mut rx, _shutdown) = receiver().await;
    let (status, body) = post(addr, "/v1/metrics", "application/json", UPSTREAM_METRICS.as_bytes()).await;
    assert_eq!(status, 200, "a partial success is still a success: {body}");
    let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(answer["partialSuccess"]["rejectedDataPoints"], 2, "the histogram and the exponential histogram: {body}");
    assert!(answer["partialSuccess"]["errorMessage"].as_str().unwrap().contains("histogram"), "named: {body}");

    let records = drain(&mut rx).await;
    let values: Vec<_> = records.iter().map(|r| {
        let TimelineRecord::LogEvent(row) = r else { panic!() };
        (row.event_type.clone().unwrap(), fields_of(r)["value"].as_f64().unwrap())
    }).collect();
    assert_eq!(values, vec![("my.counter".to_string(), 5.0), ("my.gauge".to_string(), 10.0)]);
}
