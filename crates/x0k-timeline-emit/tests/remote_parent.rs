//! A call heard from another process is answered under the caller's trace:
//! the caller names its span as a W3C `traceparent`, and a span opened with
//! that value in a `traceparent` field joins the caller's trace under the
//! caller's span, though no span here is its ancestor.
//!
//! Its own test binary, because the layer's context registry is the first
//! layer built in a process, and `current_traceparent` reads through it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use tracing::{info_span, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;
use x0k_timeline_emit::observability::{current_traceparent, ObservabilityLayer};
use x0k_timeline_emit::span_tracing::{ModuleRule, SpanContext, SpanFilterState, SpanSink};

/// Every span the layer writes, by name.
#[derive(Default)]
struct Written(Mutex<HashMap<String, SpanContext>>);

impl SpanSink for Written {
    fn write_span(&self, span: &SpanContext) {
        self.0.lock().unwrap().insert(span.span_name.clone(), span.clone());
    }
    fn get_latest_cause(&self, _entity: &str, _attribute: &str) -> Option<String> {
        None
    }
    fn flush(&self) {}
}

fn every_span() -> Arc<ArcSwap<SpanFilterState>> {
    Arc::new(ArcSwap::from_pointee(SpanFilterState {
        min_level: Level::TRACE,
        rules: vec![ModuleRule { prefix: String::new(), enabled: true, level: Level::TRACE }],
        exclude_modules: vec![],
        capture_events: true,
    }))
}

#[test]
fn a_span_naming_a_remote_parent_joins_its_trace_under_that_span() {
    let written = Arc::new(Written::default());
    let layer = ObservabilityLayer::builder().with_span_sink(written.clone()).with_filter_state(every_span()).build();
    let mut sent = None;
    tracing::subscriber::with_default(Registry::default().with(layer), || {
        {
            let caller = info_span!("caller");
            let _entered = caller.enter();
            sent = current_traceparent();
        }
        let sent = sent.clone().expect("the entered span names itself as a traceparent");
        let heard = info_span!("heard", traceparent = sent.as_str());
        heard.in_scope(|| drop(info_span!("heard_child")));
        drop(heard);
        drop(info_span!("unrelated", traceparent = "not a traceparent"));
    });
    let written = written.0.lock().unwrap();
    let (caller, heard, child, unrelated) =
        (&written["caller"], &written["heard"], &written["heard_child"], &written["unrelated"]);
    assert_eq!(sent.as_deref(), Some(format!("00-{}-{}-01", caller.trace_id, caller.span_id).as_str()));
    assert_eq!(heard.trace_id, caller.trace_id, "the heard span joins the caller's trace");
    assert_eq!(heard.parent_span_id.as_deref(), Some(caller.span_id.as_str()), "under the caller's span");
    assert_eq!(child.trace_id, caller.trace_id, "what runs under the heard span is in the caller's trace too");
    assert_eq!(child.parent_span_id.as_deref(), Some(heard.span_id.as_str()));
    assert_ne!(unrelated.trace_id, caller.trace_id, "a value that does not parse names no parent");
    assert_eq!(unrelated.parent_span_id, None);
    assert_eq!(unrelated.fields.get("traceparent_unread"), Some(&serde_json::Value::Bool(true)), "and its row says so");
    assert_eq!(heard.fields.get("traceparent_unread"), None, "a parent that parses is read");
}
