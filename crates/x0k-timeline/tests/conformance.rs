//! The timeline conformance suite (`timeline-public-surface`, intent
//! 56421f3b), run against x0k's implementation.
//!
//! The suite is the vector files in `vectors/`: each holds the vectors for one
//! check of one clause, written in the published record format (§1), so every
//! implementation runs the same files (`vectors/README.md`). This file is
//! x0k's binding. It turns a vector's records into x0k's records, runs x0k's
//! code, and turns what comes back into the published format to compare.
//!
//! The claims, stated so they can fail: **every vector of a check x0k
//! implements holds on x0k's code; a check or vector x0k does not implement
//! yet is reported NOT RUN, naming the intent that implements it, and one with
//! neither fails the run; and every vector that runs fails once its
//! expectation is flipped, so no vector holds whatever the code does.**
//!
//! An expectation matches when every field it names matches: an object by
//! its named keys, an array element by element, `null` when the field is
//! absent or null, and `{"$contains": "text"}` a string containing the text.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use x0k_activity_sink::segment;
use x0k_parquet::families::{Families, Row};
use x0k_timeline_query::schema::describe_families;
use x0k_timeline_query::{otlp, EventFamilies, TimelineWriter};
use x0k_timeline_record::JsonValue;
use x0k_timeline_record::pause::PauseTimeline;
use x0k_timeline_record::record::{
    ActivityContext, CompletedActivity, IngestTimeline, LogEventData, StorageTier, TimelineRecord,
};
use x0k_timeline_record::span::SpanData;

/// Checks x0k does not implement yet, and the intent that implements each.
const NOT_YET: &[(&str, &str)] = &[
    ("record.fit", "a265f9fe: the record becomes a shape"),
    (
        "ingest.open",
        "a265f9fe: the wire becomes canonical CBOR, opened by shape fingerprints",
    ),
    ("reader.filter", "669b6715: the base reader"),
    ("state.fold", "0bbdea50: the state-history row"),
];

/// Vectors of a check x0k runs whose claim x0k does not hold yet.
const NOT_YET_VECTORS: &[(&str, &str)] = &[
    (
        "a misfit record is refused by name and the rest of the batch kept",
        "a265f9fe: ingest checks each record against the record shape",
    ),
    (
        "account identifiers never reach the published attributes",
        "a265f9fe: the OTLP mapping writes account identifiers to the writer's x0k namespace",
    ),
];

fn vectors_dir() -> PathBuf {
    PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"))
        .join("vectors")
}

/// Every vector file, by name, in name order.
fn vector_files() -> Vec<(String, Value)> {
    let mut files: Vec<_> = std::fs::read_dir(vectors_dir())
        .expect("the vectors directory")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| {
            let text = std::fs::read_to_string(&p).unwrap();
            let value =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
            (p.file_name().unwrap().to_string_lossy().into_owned(), value)
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

#[test]
fn every_vector_file_names_its_clause_check_and_vectors() {
    let files = vector_files();
    assert!(
        !files.is_empty(),
        "no vector files in {}",
        vectors_dir().display()
    );
    let mut names = std::collections::BTreeSet::new();
    for (file, doc) in &files {
        assert!(
            doc["clause"]
                .as_str()
                .is_some_and(|c| c.starts_with("timeline-public-surface §")),
            "{file}: clause"
        );
        assert!(
            doc["check"].as_str().is_some_and(|c| !c.is_empty()),
            "{file}: check"
        );
        assert!(
            doc["about"].as_str().is_some_and(|c| !c.is_empty()),
            "{file}: about"
        );
        let vectors = doc["vectors"]
            .as_array()
            .unwrap_or_else(|| panic!("{file}: vectors"));
        assert!(!vectors.is_empty(), "{file}: no vectors");
        for v in vectors {
            let name = v["name"]
                .as_str()
                .unwrap_or_else(|| panic!("{file}: a vector without a name"));
            assert!(
                v.get("input").is_some() && v.get("expect").is_some(),
                "{file}: {name}: input and expect"
            );
            assert!(
                first_leaf(&v["expect"]).is_some(),
                "{file}: {name}: an expectation with nothing to flip"
            );
            assert!(
                names.insert(name.to_string()),
                "{file}: {name}: a name used twice"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_vector_holds_on_x0ks_implementation() {
    let (mut ran, mut not_run, mut failures) = (0, 0, Vec::new());
    for (file, doc) in vector_files() {
        let check = doc["check"].as_str().unwrap();
        for v in doc["vectors"].as_array().unwrap() {
            let name = v["name"].as_str().unwrap();
            let pending = NOT_YET
                .iter()
                .find(|(c, _)| *c == check)
                .or_else(|| NOT_YET_VECTORS.iter().find(|(n, _)| *n == name));
            if let Some((_, intent)) = pending {
                println!("NOT RUN {file}: {name} (intent {intent})");
                not_run += 1;
                continue;
            }
            let answer = match check {
                "segment.read" => segment_read(&v["input"]),
                "ingest.batch" => ingest_batch(&v["input"]).await,
                "otlp.map" => otlp_map(&v["input"]).await,
                "writer.pause" => writer_pause(&v["input"]).await,
                "families.fold" => families_fold(&v["input"]),
                other => {
                    failures.push(format!(
                        "{file}: {name}: check {other} has no implementation and no intent"
                    ));
                    continue;
                }
            };
            ran += 1;
            if let Err(why) = matches(&v["expect"], &answer, "") {
                failures.push(format!("{file}: {name}: {why}\n  answer: {answer}"));
                continue;
            }
            let flipped = flip(&v["expect"]);
            if matches(&flipped, &answer, "").is_ok() {
                failures.push(format!(
                    "{file}: {name}: still holds with its expectation flipped to {flipped}"
                ));
                continue;
            }
            println!("ok      {file}: {name}");
        }
    }
    println!(
        "conformance: {ran} ran, {not_run} NOT RUN, {} failed",
        failures.len()
    );
    assert!(ran > 0, "no vector ran");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Whether `actual` matches `expected` (the module doc); the first mismatch, at `at`.
fn matches(expected: &Value, actual: &Value, at: &str) -> Result<(), String> {
    match expected {
        Value::Null if actual.is_null() => Ok(()),
        Value::Object(want) if want.len() == 1 && want.contains_key("$contains") => {
            let needle = want["$contains"].as_str().unwrap_or_default();
            match actual.as_str() {
                Some(text) if text.contains(needle) => Ok(()),
                _ => Err(format!("{at}: {actual} does not contain {needle:?}")),
            }
        }
        Value::Object(want) => {
            let have = actual
                .as_object()
                .ok_or_else(|| format!("{at}: {actual} is not an object"))?;
            for (key, value) in want {
                matches(
                    value,
                    have.get(key).unwrap_or(&Value::Null),
                    &format!("{at}.{key}"),
                )?;
            }
            Ok(())
        }
        Value::Array(want) => {
            let have = actual
                .as_array()
                .ok_or_else(|| format!("{at}: {actual} is not an array"))?;
            if want.len() != have.len() {
                return Err(format!(
                    "{at}: {} element(s), not {}",
                    have.len(),
                    want.len()
                ));
            }
            want.iter()
                .zip(have)
                .enumerate()
                .try_for_each(|(i, (w, h))| matches(w, h, &format!("{at}[{i}]")))
        }
        scalar if scalar == actual => Ok(()),
        scalar => Err(format!("{at}: {actual}, not {scalar}")),
    }
}

/// The path to the first value `flip` changes: a scalar, an empty array or
/// object, or a `$contains` matcher.
fn first_leaf(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::Object(m) if m.len() == 1 && m.contains_key("$contains") => Some(vec![]),
        Value::Object(m) if !m.is_empty() => m.iter().find_map(|(k, x)| {
            first_leaf(x).map(|mut p| {
                p.insert(0, json!(k));
                p
            })
        }),
        Value::Array(a) if !a.is_empty() => a.iter().enumerate().find_map(|(i, x)| {
            first_leaf(x).map(|mut p| {
                p.insert(0, json!(i));
                p
            })
        }),
        _ => Some(vec![]),
    }
}

/// `expected` with its first leaf changed, so it asks for something else.
fn flip(expected: &Value) -> Value {
    let mut out = expected.clone();
    let path = first_leaf(expected).expect("a leaf to flip");
    let mut at = &mut out;
    for step in &path {
        at = match step {
            Value::String(k) => &mut at[k.as_str()],
            index => &mut at[index.as_u64().unwrap() as usize],
        };
    }
    *at = match at.take() {
        Value::Null => json!("flipped"),
        Value::Bool(b) => json!(!b),
        Value::Number(n) => json!(n.as_f64().unwrap() + 1.0),
        Value::String(s) => json!(format!("{s}-flipped")),
        Value::Array(_) => json!(["flipped"]),
        Value::Object(m) if m.contains_key("$contains") => {
            json!({"$contains": format!("{}-flipped", m["$contains"].as_str().unwrap_or_default())})
        }
        Value::Object(_) => json!({"flipped": true}),
    };
    out
}

// --- x0k's binding: the published record format and x0k's records today ---

fn text(r: &Value, key: &str) -> Option<String> {
    r[key].as_str().map(str::to_string)
}

/// A published record as x0k's record today. Fields and attributes both
/// land in x0k's `fields`, as the OTLP mapping table in `otlp.rs` says.
fn to_x0k(r: &Value) -> TimelineRecord {
    let mut fields = r["attributes"].as_object().cloned().unwrap_or_default();
    fields.extend(r["fields"].as_object().cloned().unwrap_or_default());
    match r["entry_kind"].as_str() {
        Some("span") => {
            let started = r["started_at_us"].as_u64().unwrap_or(0);
            TimelineRecord::Span(SpanData {
                span_id: text(r, "span_id")
                    .or_else(|| text(r, "id"))
                    .unwrap_or_default(),
                trace_id: text(r, "trace_id").unwrap_or_default(),
                parent_span_id: text(r, "parent_span_id"),
                start_time: started,
                duration_ms: r["ended_at_us"]
                    .as_u64()
                    .map(|end| end.saturating_sub(started) / 1000),
                level: text(r, "level").unwrap_or_default(),
                module: text(r, "module"),
                span_name: text(r, "event_type").unwrap_or_default(),
                target: text(r, "target").unwrap_or_default(),
                fields: fields
                    .into_iter()
                    .map(|(k, v)| (k, JsonValue::from_value(v).unwrap()))
                    .collect(),
                ..Default::default()
            })
        }
        Some("activity") => {
            let context = ActivityContext {
                activity_id: x0k_timeline_record::ActivityId::from(text(r, "id").unwrap_or_default()),
                activity_type: text(r, "event_type").unwrap_or_default(),
                started_at: r["timestamp_us"].as_u64().unwrap_or(0),
                ..Default::default()
            };
            let tier = match r["retention_class"].as_str() {
                Some("record") => StorageTier::Record,
                _ => StorageTier::Central,
            };
            let activity = CompletedActivity {
                context,
                ended_at: 0,
                duration_ms: 0,
            };
            TimelineRecord::Activity(activity, tier)
        }
        _ => TimelineRecord::LogEvent(LogEventData {
            id: text(r, "id").unwrap_or_default(),
            timestamp_us: r["timestamp_us"].as_u64().unwrap_or(0),
            level: text(r, "level").unwrap_or_default(),
            message: text(r, "description"),
            fields_json: (!fields.is_empty()).then(|| Value::Object(fields).to_string()),
            module: text(r, "module"),
            target: text(r, "target").unwrap_or_default(),
            software_module_id: text(&r["process"], "software_module"),
            session_id: text(r, "session_id"),
            pid: r["process"]["pid"].as_u64().map(|p| p as u32),
            trace_id: text(r, "trace_id"),
            tracestate: text(r, "tracestate"),
            event_type: text(r, "event_type"),
            ..Default::default()
        }),
    }
}

/// x0k's record today in the published format, with only the fields it has.
fn published(record: &TimelineRecord) -> Value {
    let mut out = Map::new();
    let mut put = |key: &str, value: Value| {
        if !value.is_null() {
            out.insert(key.to_string(), value);
        }
    };
    match record {
        TimelineRecord::Span(s) => {
            put("entry_kind", json!("span"));
            put("id", json!(s.span_id));
            put("trace_id", json!(s.trace_id));
            put("span_id", json!(s.span_id));
            put("parent_span_id", json!(s.parent_span_id));
            put("event_type", json!(s.span_name));
            put("level", json!(s.level));
            put("target", json!(s.target));
            put("module", json!(s.module));
            put("started_at_us", json!(s.start_time));
            put(
                "ended_at_us",
                json!(s.duration_ms.map(|d| s.start_time + d * 1000)),
            );
            put("process", json!({"software_module": s.software_module_id}));
            let attributes: Map<String, Value> = s
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), v.to_value().unwrap_or(Value::Null)))
                .collect();
            put("attributes", Value::Object(attributes));
        }
        TimelineRecord::LogEvent(l) => {
            put("entry_kind", json!("log"));
            put("id", json!(l.id));
            put("timestamp_us", json!(l.timestamp_us));
            put("level", json!(l.level));
            put("target", json!(l.target));
            put("module", json!(l.module));
            put("description", json!(l.message));
            put("event_type", json!(l.event_type));
            put("session_id", json!(l.session_id));
            put("trace_id", json!(l.trace_id));
            put("tracestate", json!(l.tracestate));
            put(
                "process",
                json!({"software_module": l.software_module_id, "pid": l.pid}),
            );
            let attributes = l
                .fields_json
                .as_deref()
                .and_then(|f| serde_json::from_str(f).ok())
                .unwrap_or_else(|| json!({}));
            put("attributes", attributes);
        }
        TimelineRecord::Activity(a, _) => {
            put("entry_kind", json!("activity"));
            put("id", json!(a.context.activity_id.0));
            put("event_type", json!(a.context.activity_type));
            put("description", json!(a.context.activity_data));
            put("retention_class", json!(record.retention_class().as_str()));
            put("attributes", json!(a.context.metadata));
        }
        other => put(
            "entry_kind",
            json!(format!("{other:?}")
                .split('(')
                .next()
                .unwrap_or_default()
                .to_lowercase()),
        ),
    }
    Value::Object(out)
}

fn id_of(record: &TimelineRecord) -> Value {
    published(record)["id"].clone()
}

fn records_of(input: &Value) -> Vec<TimelineRecord> {
    input["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(to_x0k)
        .collect()
}

/// `segment.read`: write the records to one segment, damage its end, read it.
fn segment_read(input: &Value) -> Value {
    let frames: Vec<Vec<u8>> = records_of(input)
        .iter()
        .map(|r| segment::encode_frame(r).unwrap())
        .collect();
    let header = match input["header"].as_str() {
        None => segment::header_frame(),
        Some("none") => Vec::new(),
        Some(format) => segment::header_frame_naming(format),
    };
    let mut bytes: Vec<u8> = [header, frames.concat()].concat();
    if let Some(last) = frames.last() {
        let start = bytes.len() - last.len();
        match input["tail"].as_str() {
            Some("whole") => {}
            Some("torn_inside_last_record") => bytes.truncate(start + 4 + (last.len() - 4) / 2),
            Some("torn_inside_last_length") => bytes.truncate(start + 2),
            other => panic!("segment.read: tail {other:?}"),
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("conformance.queue");
    std::fs::write(&path, &bytes).unwrap();
    match segment::read_segment(&path) {
        Ok(read) => json!({"ids": read.iter().map(id_of).collect::<Vec<_>>()}),
        Err(fault) => {
            let refused = fault.downcast_ref::<segment::UnknownSegmentFormat>().unwrap();
            json!({"refused": refused.0, "kept": path.exists()})
        }
    }
}

/// `ingest.batch`: hand the batch to the writer's ingest; what it accepted and
/// what reached its store, which is the writer's channel.
async fn ingest_batch(input: &Value) -> Value {
    let (tx, mut rx) = mpsc::channel(1024);
    let writer = TimelineWriter::new(tx);
    let result = writer
        .ingest_timeline(&IngestTimeline {
            records: records_of(input),
        })
        .await
        .unwrap();
    let mut received = Vec::new();
    while let Ok(record) = rx.try_recv() {
        received.push(id_of(&record));
    }
    json!({"accepted": result.accepted, "received_ids": received})
}

/// `writer.pause`: a writer taken through the vector's steps; what each answered
/// and the records that reached the pipeline.
async fn writer_pause(input: &Value) -> Value {
    let (tx, rx) = mpsc::channel(1024);
    let mut rx = Some(rx);
    let mut kept = Vec::new();
    let mut writer = TimelineWriter::new(tx.clone());
    let mut steps = Vec::new();
    for step in input["steps"].as_array().unwrap() {
        let state = |s: x0k_timeline_record::pause::PauseState| {
            json!({"paused": s.paused, "reason": s.reason, "discarded": {
                "span": s.discarded.span, "log": s.discarded.log, "event": s.discarded.event,
            }})
        };
        let answer = match step["op"].as_str().unwrap() {
            "pause" => {
                let reason = step["reason"].as_str().unwrap().to_string();
                match writer.pause(&PauseTimeline { reason }).await {
                    Ok(s) => state(s),
                    Err(_) => json!({"refused": true}),
                }
            }
            "resume" => state(writer.resume().await.unwrap()),
            "state" => state(writer.pause_state()),
            "ingest" => {
                let records = records_of(step);
                let r = writer.ingest_timeline(&IngestTimeline { records }).await.unwrap();
                json!({"accepted": r.accepted, "discarded": r.discarded})
            }
            "restart" => {
                writer = TimelineWriter::new(tx.clone());
                json!({"restarted": true})
            }
            "close_pipeline" => {
                rx = None;
                json!({"closed": true})
            }
            other => panic!("no step {other}"),
        };
        steps.push(answer);
        if let Some(rx) = rx.as_mut() {
            while let Ok(record) = rx.try_recv() {
                kept.push(published(&record));
            }
        }
    }
    json!({"steps": steps, "kept": kept})
}

/// `otlp.map`: one request to the writer's OTLP receiver on a loopback port;
/// its status, its answer and the records it wrote.
async fn otlp_map(input: &Value) -> Value {
    let body: Vec<u8> = if let Some(file) = input["body_file"].as_str() {
        std::fs::read(vectors_dir().join(file)).unwrap()
    } else if let Some(raw) = input["body_text"].as_str() {
        raw.as_bytes().to_vec()
    } else {
        input["body"].to_string().into_bytes()
    };
    let content_type = input["content_type"].as_str().unwrap_or("application/json");
    let (tx, mut rx) = mpsc::channel(1024);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = Arc::new(Notify::new());
    tokio::spawn(otlp::serve(
        listener,
        TimelineWriter::new(tx),
        shutdown.clone(),
    ));

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        input["path"].as_str().unwrap(),
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    shutdown.notify_waiters();
    let status: u16 = response.split_whitespace().nth(1).unwrap().parse().unwrap();
    let text = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    let answer = serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({"text": text}));

    let mut records = Vec::new();
    while let Ok(Some(record)) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        records.push(published(&record));
    }
    json!({"status": status, "response": answer, "records": records})
}

/// What `describe_timeline` says of the rows in `input`, as `{name: fields}`.
fn families_fold(input: &Value) -> Value {
    let mut families = Families::default();
    for row in input["rows"].as_array().expect("rows") {
        let fields = row["fields"].to_string();
        families.observe(&Row {
            name: row["family"].as_str().expect("family"),
            entry_kind: "span",
            timestamp_us: row["timestamp_us"].as_i64().expect("timestamp_us"),
            fields_json: Some(&fields),
            call_site: None,
        });
    }
    let answer = EventFamilies { families, files: 0, files_without_schema: 0 };
    let described = describe_families(&answer, Some(""));
    let by_name = described.families.iter().map(|family| {
        let fields = family.fields.iter().map(|field| {
            (field.name.clone(), json!({ "type": field.data_type, "conflict": field.conflict }))
        });
        let fields: serde_json::Map<String, Value> = fields.collect();
        (
            family.name.clone(),
            json!({
                "rows": family.rows,
                "first_seen_us": family.first_seen_us,
                "last_seen_us": family.last_seen_us,
                "fields": fields,
            }),
        )
    });
    Value::Object(by_name.collect())
}
