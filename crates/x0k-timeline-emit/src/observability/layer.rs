//! Unified observability layer implementation
//!
//! Single `Layer<S>` that handles both structured spans and activities.

use crate::activity::ActorResolver;
use crate::span_tracing::{SpanContext, SpanEvent, SpanFilterState, SpanSink, SpanTracingConfig};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::{field::Visit, span, Event, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};
use x0k_timeline_record::ActivityId;
use x0k_timeline_record::record::{
    route_activity_storage_tier, ActivityContext, ActivitySink, CompletedActivity,
    LazySerializable, StorageTier, TimelineRecord, LEGACY_DURABILITY_KEY,
    STORAGE_TIER_CENTRAL_VALUE, STORAGE_TIER_EPHEMERAL_VALUE, STORAGE_TIER_KEY,
    STORAGE_TIER_RECORD_VALUE,
};

/// A freestanding event that earned a durable activity row, and how it got its
/// name.
///
/// The name's *provenance* is what the tier decision needs: a class the emitter
/// named carries a prefix the routing table can match, while a class we
/// synthesised from severity (`log.warn`) carries nothing to route on.
struct PromotedEvent {
    activity_type: String,
    /// The emitter supplied this class via `activity.type` or `event_type`.
    emitter_named: bool,
}

impl PromotedEvent {
    fn named(activity_type: String) -> Self {
        Self {
            activity_type,
            emitter_named: true,
        }
    }

    fn synthetic(activity_type: &str) -> Self {
        Self {
            activity_type: activity_type.to_string(),
            emitter_named: false,
        }
    }
}

/// In-flight context — either a span or an activity
enum InFlightContext {
    Span(SpanContext),
    Activity(ActivityContext),
}

/// Global handle to the active layer's in-flight context map, registered on
/// [`ObservabilityLayerBuilder::build`]. This is the read-side seam that lets
/// callers correlate the *current* tracing span back to the **canonical**
/// `span_id` / `trace_id` the parquet timeline sink wrote (16-/32-char hex),
/// rather than the opaque `tracing::span::Id` (`Id(N)`). Only the first layer
/// built wins (one process installs one subscriber).
static CONTEXT_REGISTRY: OnceLock<Arc<DashMap<span::Id, InFlightContext>>> = OnceLock::new();

/// Canonical span identity matching the parquet `span_id` / `trace_id` columns.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpanCorrelation {
    /// 16-char hex span id (the value written to the timeline `span_id` column).
    pub span_id: String,
    /// 32-char hex trace id (the value written to the timeline `trace_id`
    /// column). Empty when the span has no tracked trace context.
    pub trace_id: String,
}

/// Resolve the canonical correlation ids for the **currently entered** tracing
/// span, by looking it up in the active [`ObservabilityLayer`]'s context map.
///
/// Returns `None` when there is no current span, no layer has been installed
/// (e.g. a unit test with no subscriber), or the current span is not tracked
/// (filtered out, or an activity rather than a trace span). The returned
/// `span_id` / `trace_id` are the *same strings the parquet sink emitted*, so a
/// downstream `WHERE span_id IN (...)` query will match.
pub fn current_span_correlation() -> Option<SpanCorrelation> {
    let registry = CONTEXT_REGISTRY.get()?;
    let id = tracing::Span::current().id()?;
    let entry = registry.get(&id)?;
    match &*entry {
        InFlightContext::Span(ctx) => Some(SpanCorrelation {
            span_id: ctx.span_id.clone(),
            trace_id: ctx.trace_id.clone(),
        }),
        InFlightContext::Activity(_) => None,
    }
}

/// The currently entered span as a W3C `traceparent`
/// (`00-{trace id}-{span id}-01`), for a call this process makes to another:
/// the span the callee opens with it in a `traceparent` field joins this
/// trace under this span. `None` where [`current_span_correlation`] is, and
/// for a span with no tracked trace.
pub fn current_traceparent() -> Option<String> {
    let current = current_span_correlation()?;
    (!current.trace_id.is_empty()).then(|| format!("00-{}-{}-01", current.trace_id, current.span_id))
}

/// The trace a span joins when it names a remote parent in a `traceparent`
/// field, as the span a call heard from another process is answered under
/// does: that parent's trace, under that parent's span, ahead of any
/// ancestor here. The field is read when the span opens, so a value set
/// later with `record` is never seen, and it is read as recorded, so it is
/// written plain or with `%`: `?` quotes it. A value that does not parse
/// names no parent, and the span's row says so in `traceparent_unread`
/// (an event here would re-enter this layer).
fn remote_parent(fields: &mut std::collections::HashMap<String, serde_json::Value>) -> Option<(String, Option<String>)> {
    let parent = match fields.get("traceparent") {
        None => return None,
        Some(serde_json::Value::String(named)) => crate::trace_context::remote_parent(named),
        Some(_) => None,
    };
    if parent.is_none() {
        fields.insert("traceparent_unread".to_string(), serde_json::Value::Bool(true));
    }
    parent
}

impl InFlightContext {
    fn start_time(&self) -> u64 {
        match self {
            InFlightContext::Span(s) => s.start_time,
            InFlightContext::Activity(a) => a.started_at,
        }
    }
}

fn duration_ms_between(start_us: u64, end_us: u64) -> u64 {
    end_us.saturating_sub(start_us) / 1_000
}

/// Field visitor that extracts typed values (serde_json::Value)
struct RichFieldExtractor {
    fields: std::collections::HashMap<String, serde_json::Value>,
    message: Option<String>,
}

impl RichFieldExtractor {
    fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
            message: None,
        }
    }

    /// Convert fields to String-valued map (for activity metadata)
    fn as_string_map(&self) -> std::collections::HashMap<String, String> {
        self.fields
            .iter()
            .map(|(k, v)| {
                let s = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (k.clone(), s)
            })
            .collect()
    }

    /// Keys whose recorded value is a JSON scalar (number or bool), not a
    /// string. `as_string_map` flattens these to their JSON text; this set is
    /// what lets the sink restore the type when it writes `metadata_json`.
    fn typed_keys(&self) -> std::collections::BTreeSet<String> {
        self.fields
            .iter()
            .filter(|(_, v)| is_json_scalar(v))
            .map(|(k, _)| k.clone())
            .collect()
    }
}

fn is_json_scalar(value: &serde_json::Value) -> bool {
    matches!(
        value,
        serde_json::Value::Number(_) | serde_json::Value::Bool(_)
    )
}

impl Visit for RichFieldExtractor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value_str = format!("{:?}", value);
        if field.name() == "message" {
            self.message = Some(value_str.trim_matches('"').to_string());
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(value_str),
            );
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_string(), serde_json::Value::Number(n));
        }
    }
}

// ---------------------------------------------------------------------------
// ObservabilityLayer
// ---------------------------------------------------------------------------

/// Unified observability layer that captures both spans and activities
pub struct ObservabilityLayer {
    /// Single map for all in-flight contexts
    contexts: Arc<DashMap<span::Id, InFlightContext>>,

    // -- Span sinks --
    span_sink: Option<Arc<dyn SpanSink>>,

    // -- Activity sinks --
    activity_sink: Option<Arc<dyn ActivitySink>>,
    actor_resolver: Option<Arc<dyn ActorResolver>>,

    // -- Shared config --
    filter_state: Arc<ArcSwap<SpanFilterState>>,
    max_contexts: usize,
    buffer_events: bool,
    workstream_id: Option<String>,
    intent_id: Option<String>,
    software_module_id: Option<String>,
    session_id: Option<String>,
    pid: u32,

    evicted_count: AtomicU64,
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct ObservabilityLayerBuilder {
    span_sink: Option<Arc<dyn SpanSink>>,
    activity_sink: Option<Arc<dyn ActivitySink>>,
    actor_resolver: Option<Arc<dyn ActorResolver>>,
    config: Option<SpanTracingConfig>,
    filter_state: Option<Arc<ArcSwap<SpanFilterState>>>,
    software_module_id: Option<String>,
}

impl ObservabilityLayerBuilder {
    pub fn with_span_sink(mut self, sink: Arc<dyn SpanSink>) -> Self {
        self.span_sink = Some(sink);
        self
    }

    pub fn with_activity_sink(mut self, sink: Arc<dyn ActivitySink>) -> Self {
        self.activity_sink = Some(sink);
        self
    }

    pub fn with_actor_resolver(mut self, resolver: Arc<dyn ActorResolver>) -> Self {
        self.actor_resolver = Some(resolver);
        self
    }

    pub fn with_config(mut self, config: SpanTracingConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_filter_state(mut self, state: Arc<ArcSwap<SpanFilterState>>) -> Self {
        self.filter_state = Some(state);
        self
    }

    pub fn with_software_module_id(mut self, module_id: impl Into<String>) -> Self {
        self.software_module_id = Some(module_id.into());
        self
    }

    pub fn build(self) -> ObservabilityLayer {
        let config = self.config.unwrap_or_default();
        let filter_state = self.filter_state.unwrap_or_else(|| {
            Arc::new(ArcSwap::from_pointee(SpanFilterState::from_env()))
        });

        let workstream_id = std::env::var("X0K_WORKSTREAM_ID").ok();
        let intent_id = std::env::var("X0K_INTENT_ID").ok();
        let session_id = std::env::var("X0K_SESSION_ID").ok();
        let pid = std::process::id();

        let contexts = Arc::new(DashMap::new());
        // Publish the context map so `current_span_correlation()` can resolve
        // the canonical span_id/trace_id of the current tracing span. First
        // layer built wins (one subscriber per process).
        let _ = CONTEXT_REGISTRY.set(Arc::clone(&contexts));

        ObservabilityLayer {
            contexts,
            span_sink: self.span_sink,
            activity_sink: self.activity_sink,
            actor_resolver: self.actor_resolver,
            filter_state,
            max_contexts: config.max_spans,
            buffer_events: config.buffer_events,
            workstream_id,
            intent_id,
            software_module_id: self.software_module_id,
            session_id,
            pid,
            evicted_count: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

impl ObservabilityLayer {
    pub fn builder() -> ObservabilityLayerBuilder {
        ObservabilityLayerBuilder::default()
    }

    fn generate_span_id() -> String {
        use rand::Rng;
        let bytes: [u8; 8] = rand::thread_rng().gen();
        hex::encode(bytes)
    }

    /// Walk the ancestor chain looking for any tracked Span context whose
    /// `fields` map contains a `node_name` entry. Returns the first match.
    ///
    /// This lets every log/span/activity captured by the layer inherit the
    /// node_name set on the nearest ancestor `x0k_session` span, so that
    /// multi-session tests running in a single process can be disambiguated
    /// in the parquet timeline without having to grep description text.
    fn get_node_name<S>(&self, id: &span::Id, ctx: &Context<'_, S>) -> Option<String>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        // Check the span itself first, then walk parents
        let span = ctx.span(id)?;
        let mut cursor = Some(span);
        while let Some(current) = cursor {
            if let Some(entry) = self.contexts.get(&current.id()) {
                if let InFlightContext::Span(ref span_ctx) = *entry {
                    if let Some(value) = span_ctx.fields.get("node_name") {
                        if let serde_json::Value::String(s) = value {
                            return Some(s.clone());
                        }
                        // Fallback: stringify any other JSON shape
                        return Some(value.to_string());
                    }
                }
            }
            cursor = current.parent();
        }
        None
    }

    fn get_trace_context<S>(&self, id: &span::Id, ctx: &Context<'_, S>) -> (String, Option<String>)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        // Walk the full ancestor chain looking for any tracked context (Span or Activity).
        // This ensures child spans under an Activity parent (e.g., test activity) inherit
        // the activity's trace_id instead of generating a random one.
        if let Some(span) = ctx.span(id) {
            let mut current = span.parent();
            while let Some(ancestor) = current {
                if let Some(entry) = self.contexts.get(&ancestor.id()) {
                    match &*entry {
                        InFlightContext::Span(ref parent_ctx) => {
                            return (
                                parent_ctx.trace_id.clone(),
                                Some(parent_ctx.span_id.clone()),
                            );
                        }
                        InFlightContext::Activity(ref act_ctx) => {
                            if let Some(ref tid) = act_ctx.trace_id {
                                return (tid.clone(), act_ctx.span_id.clone());
                            }
                        }
                    }
                }
                current = ancestor.parent();
            }
        }

        if let Ok(traceparent) = std::env::var("TRACEPARENT") {
            let parts: Vec<&str> = traceparent.split('-').collect();
            if parts.len() >= 3 {
                return (parts[1].to_string(), Some(parts[2].to_string()));
            }
        }

        let trace_id = {
            use rand::Rng;
            let bytes: [u8; 16] = rand::thread_rng().gen();
            hex::encode(bytes)
        };
        (trace_id, None)
    }

    fn evict_if_needed(&self) {
        let current_len = self.contexts.len();
        if current_len < self.max_contexts {
            return;
        }

        let evict_count = std::cmp::max(1, self.max_contexts / 10);

        let mut entries: Vec<(span::Id, u64)> = self
            .contexts
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().start_time()))
            .collect();

        entries.sort_by_key(|(_, t)| *t);

        let mut evicted: u64 = 0;
        for (id, _) in entries.into_iter().take(evict_count) {
            if self.contexts.remove(&id).is_some() {
                evicted += 1;
            }
        }

        if evicted > 0 {
            self.evicted_count.fetch_add(evicted, Ordering::Relaxed);
            // Use eprintln instead of tracing::warn! to avoid re-entering
            // ObservabilityLayer::on_event while we hold the eviction lock.
            eprintln!(
                "WARN: Evicted {} stale contexts (total_evicted={}, max={})",
                evicted,
                self.evicted_count.load(Ordering::Relaxed),
                self.max_contexts,
            );
        }
    }

    /// Extract lazy serializable data from activity fields
    fn extract_lazy_data(
        fields: &std::collections::HashMap<String, String>,
    ) -> Option<LazySerializable> {
        if let (Some(event_type), Some(dedup_key), Some(context)) = (
            fields.get("lazy.event_type"),
            fields.get("lazy.dedup_key"),
            fields.get("lazy.context"),
        ) {
            let properties = if let Some(props_json) = fields.get("lazy.properties") {
                serde_json::from_str::<std::collections::HashMap<String, String>>(props_json)
                    .unwrap_or_default()
            } else {
                fields
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix("lazy.prop.")
                            .map(|prop_key| (prop_key.to_string(), value.clone()))
                    })
                    .collect()
            };

            return Some(LazySerializable::from_event_data(
                event_type.clone(),
                dedup_key.clone(),
                context.clone(),
                properties,
            ));
        }

        if let Some(json_str) = fields.get("lazy.json") {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return Some(LazySerializable::from_json(json_value));
            }
        }

        let lazy_props: std::collections::HashMap<String, String> = fields
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("lazy.prop.")
                    .map(|prop_key| (prop_key.to_string(), value.clone()))
            })
            .collect();

        if !lazy_props.is_empty() {
            return Some(LazySerializable::from_properties(lazy_props));
        }

        None
    }

    fn is_activity_span(metadata: &tracing::Metadata<'_>) -> bool {
        let name = metadata.name();
        name == "activity" || name == "test_activity"
    }

    /// The activity type a freestanding event is promoted under, or `None` to
    /// leave it a message row.
    ///
    /// An explicit `activity.type` opts into the durable semantic tier at any
    /// severity. Otherwise severity decides: WARN/ERROR events are durable and
    /// retain an older emitter's `event_type` when present, falling back to a
    /// synthetic severity name only for untyped events. INFO `event_type`
    /// events remain named log rows on the short retention clock.
    /// Incident regression:
    /// `typed_warns_keep_their_semantic_event_name_without_promoting_info`.
    ///
    /// This answers only "is this news?" — a question about severity and
    /// structure. Where the promoted row *lives* is a separate, later question
    /// answered by [`Self::promoted_storage_tier`] from volume and retention.
    fn promoted_activity_type(
        level: &tracing::Level,
        extractor: &RichFieldExtractor,
    ) -> Option<PromotedEvent> {
        if let Some(serde_json::Value::String(explicit)) = extractor.fields.get("activity.type") {
            if !explicit.is_empty() {
                return Some(PromotedEvent::named(explicit.clone()));
            }
        }

        match *level {
            tracing::Level::ERROR => Some(
                Self::event_type_field(extractor)
                    .map(PromotedEvent::named)
                    .unwrap_or_else(|| PromotedEvent::synthetic("log.error")),
            ),
            tracing::Level::WARN => Some(
                Self::event_type_field(extractor)
                    .map(PromotedEvent::named)
                    .unwrap_or_else(|| PromotedEvent::synthetic("log.warn")),
            ),
            _ => None,
        }
    }

    /// The storage tier a promoted event's row is written under.
    ///
    /// Promotion (above) and placement (here) are two decisions, and this path
    /// used to collapse them: every promoted row was hard-coded
    /// `StorageTier::Central`, which silently bypassed both placement
    /// mechanisms — the prefix table *and* the `activity.storage_tier`
    /// metadata override. One `sync.*` warn then wrote 35.7% of every row on
    /// the node under the operator-facing label. Incident regression:
    /// `promoted_events_route_their_storage_tier_2026_09_03`.
    ///
    /// Precedence, matching `route_activity_storage_tier`'s own:
    ///
    /// 1. An explicit `activity.storage_tier` (legacy `activity.durability`)
    ///    field on the event wins outright, whatever named the class.
    /// 2. A class the *emitter* named — `activity.type` or `event_type` — has a
    ///    real prefix, so the routing table places it.
    /// 3. A class we synthesised from severity alone (`log.warn`, `log.error`)
    ///    has no prefix the table knows; routing it would only read back the
    ///    table's catch-all. An untyped promoted warn stays Central, exactly as
    ///    before this path learned to route.
    ///
    /// `retention_class` is untouched by any of this: the sink derives it from
    /// `entry_kind`, and Central and Ephemeral activities alike are written as
    /// `event` rows on the same 7-day clock. This decides the query label, not
    /// how long the evidence survives.
    fn promoted_storage_tier(
        promoted: &PromotedEvent,
        fields: &std::collections::HashMap<String, String>,
    ) -> StorageTier {
        if let Some(explicit) = Self::explicit_storage_tier(fields) {
            return explicit;
        }

        if promoted.emitter_named {
            route_activity_storage_tier(&promoted.activity_type, fields)
        } else {
            StorageTier::Central
        }
    }

    /// The `activity.storage_tier` override carried on an event's fields, if
    /// any. An unrecognised value is no override, so it falls through to
    /// routing — the same way `route_activity_storage_tier` treats it.
    fn explicit_storage_tier(
        fields: &std::collections::HashMap<String, String>,
    ) -> Option<StorageTier> {
        let raw = fields
            .get(STORAGE_TIER_KEY)
            .or_else(|| fields.get(LEGACY_DURABILITY_KEY))?;
        match raw.as_str() {
            STORAGE_TIER_CENTRAL_VALUE => Some(StorageTier::Central),
            STORAGE_TIER_EPHEMERAL_VALUE => Some(StorageTier::Ephemeral),
            STORAGE_TIER_RECORD_VALUE => Some(StorageTier::Record),
            _ => None,
        }
    }

    fn event_type_field(extractor: &RichFieldExtractor) -> Option<String> {
        match extractor.fields.get("event_type") {
            Some(serde_json::Value::String(event_type)) if !event_type.is_empty() => {
                Some(event_type.clone())
            }
            _ => None,
        }
    }

    /// Build the durable activity record for a promoted event.
    ///
    /// The event's message becomes `activity_data`, which the sink writes to
    /// the `description` column — so a promoted `warn!` is found by the same
    /// `WHERE description = …` a message row answered to, just in a tier that
    /// outlives the incident. `activity.level` carries the real severity; the
    /// sink prefers it over sniffing the type string, so `get_recent_errors`
    /// still sees `level = 'warn'`.
    fn event_activity_record(
        &self,
        metadata: &tracing::Metadata<'_>,
        extractor: &RichFieldExtractor,
        promoted: PromotedEvent,
        trace_id: Option<String>,
    ) -> TimelineRecord {
        let mut fields = extractor.as_string_map();
        fields.insert(
            "activity.level".to_string(),
            metadata.level().to_string().to_lowercase(),
        );
        fields.insert("activity.target".to_string(), metadata.target().to_string());
        if let Some(ref module_id) = self.software_module_id {
            fields
                .entry("activity.software_module".to_string())
                .or_insert_with(|| module_id.clone());
        }
        if let Some(ref sid) = self.session_id {
            fields
                .entry("activity.session_id".to_string())
                .or_insert_with(|| sid.clone());
        }
        fields
            .entry("pid".to_string())
            .or_insert_with(|| self.pid.to_string());

        let correlation_id = fields.get("correlation_id").cloned();
        let now = x0k_timeline_record::now_micros();

        // Placement, decided from the class and the emitter's override — never
        // from the fact of promotion.
        let storage_tier = Self::promoted_storage_tier(&promoted, &fields);

        let context = ActivityContext {
            activity_id: ActivityId::new(),
            activity_type: promoted.activity_type,
            parent_id: None,
            actor: None,
            started_at: now,
            metadata: fields,
            typed_metadata_keys: extractor.typed_keys(),
            correlation_id,
            trace_id,
            span_id: Some(Self::generate_span_id()),
            parent_span_id: None,
            activity_data: extractor.message.clone(),
            lazy_data: None,
            source_file: metadata.file().map(|s| s.to_string()),
            source_line: metadata.line(),
            source_function: metadata.module_path().map(|s| s.to_string()),
            source_module: metadata.module_path().map(|s| s.to_string()),
            span_name: Some(metadata.name().to_string()),
        };

        TimelineRecord::Activity(
            CompletedActivity {
                context,
                ended_at: now,
                duration_ms: 0,
            },
            storage_tier,
        )
    }
}

// ---------------------------------------------------------------------------
// Layer impl
// ---------------------------------------------------------------------------

impl<S> Layer<S> for ObservabilityLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();

        if Self::is_activity_span(metadata) {
            self.on_new_activity(attrs, id, &ctx);
        } else {
            self.on_new_trace_span(attrs, id, &ctx);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let filter = self.filter_state.load();
        if !filter.capture_events {
            return;
        }

        let metadata = event.metadata();

        if !filter.should_track(metadata) {
            return;
        }

        let mut extractor = RichFieldExtractor::new();
        event.record(&mut extractor);

        // Inherit node_name from the nearest ancestor session span (if any).
        // We resolve this from the *event's* current span — events have no id of
        // their own, so we use the current span as the lookup root.
        // Done up-front so the value flows into both the buffered SpanEvent path
        // and the freestanding LogEvent path.
        let inherited_node_name = ctx
            .current_span()
            .id()
            .and_then(|id| self.get_node_name(id, &ctx));
        if let Some(node_name) = inherited_node_name.as_ref() {
            extractor
                .fields
                .entry("node_name".to_string())
                .or_insert_with(|| serde_json::Value::String(node_name.clone()));
        }

        // `activity.type` is an explicit durable-semantic opt-in. WARN/ERROR
        // events are durable evidence too, retaining their real `event_type`
        // when one was supplied. INFO `event_type` events deliberately remain
        // named log rows: the field makes them queryable, not permanent.
        if let Some(promoted) = Self::promoted_activity_type(metadata.level(), &extractor) {
            let trace_id = ctx.current_span().id().and_then(|id| {
                self.contexts.get(id).and_then(|entry| match *entry {
                    InFlightContext::Span(ref span_ctx) => Some(span_ctx.trace_id.clone()),
                    InFlightContext::Activity(ref act_ctx) => act_ctx.trace_id.clone(),
                })
            });
            let record = self.event_activity_record(metadata, &extractor, promoted, trace_id);
            let _ = crate::timeline_channel::try_send_global(record, "event_activity");
            return;
        }

        // Try to attach to a tracked parent span, extracting trace_id for correlation
        let mut parent_trace_id: Option<String> = None;
        let attached = if let Some(current) = ctx.current_span().id() {
            if let Some(mut entry) = self.contexts.get_mut(current) {
                if let InFlightContext::Span(ref mut span_ctx) = *entry {
                    parent_trace_id = Some(span_ctx.trace_id.clone());
                    if self.buffer_events {
                        span_ctx.events.push(SpanEvent {
                            timestamp: x0k_timeline_record::now_micros(),
                            level: metadata.level().to_string().to_lowercase(),
                            message: extractor.message.clone(),
                            fields: extractor.fields.clone(),
                            source_file: metadata.file().map(|s| s.to_string()),
                            source_line: metadata.line(),
                        });
                        true
                    } else {
                        false // not buffering — emit as freestanding with trace_id
                    }
                } else {
                    false // parent is an activity — treat as freestanding
                }
            } else {
                false // parent not tracked
            }
        } else {
            false // no parent span at all
        };

        // Freestanding event → emit as LogEvent to the timeline channel
        if !attached {
            let event_type = Self::event_type_field(&extractor);
            let fields_json = if extractor.fields.is_empty() {
                None
            } else {
                serde_json::to_string(&extractor.fields).ok()
            };

            let log_event = x0k_timeline_record::record::LogEventData {
                id: Self::generate_span_id(),
                timestamp_us: x0k_timeline_record::now_micros(),
                level: metadata.level().to_string().to_lowercase(),
                message: extractor.message,
                fields_json,
                source_file: metadata.file().map(|s| s.to_string()),
                source_line: metadata.line(),
                module: metadata.module_path().map(|s| s.to_string()),
                target: metadata.target().to_string(),
                software_module_id: self.software_module_id.clone(),
                workstream_id: self.workstream_id.clone(),
                intent_id: self.intent_id.clone(),
                session_id: self.session_id.clone(),
                pid: Some(self.pid),
                trace_id: parent_trace_id,
                traceparent: None,
                tracestate: None,
                request_id: None,
                parent_request_id: None,
                role: None,
                mode: None,
                origin_system: None,
                origin_subsystem: None,
                agent_profile: None,
                sandbox_id: None,
                invocation_method: None,
                executed_by_json: None,
                authorization_chain_json: None,
                authorization_proof_refs_json: None,
                artifact_id: None,
                native_artifact_id: None,
                document_id: None,
                document_revision_id: None,
                file_path: None,
                event_type,
            };

            let _ = crate::timeline_channel::try_send_global(
                TimelineRecord::LogEvent(log_event),
                "log_event",
            );
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
        if let Some(mut entry) = self.contexts.get_mut(id) {
            let mut extractor = RichFieldExtractor::new();
            values.record(&mut extractor);

            match *entry {
                InFlightContext::Span(ref mut span_ctx) => {
                    for (key, value) in extractor.fields {
                        span_ctx.fields.insert(key, value);
                    }
                }
                InFlightContext::Activity(ref mut act_ctx) => {
                    let string_fields = extractor.as_string_map();
                    for (key, value) in string_fields.clone() {
                        act_ctx.metadata.insert(key, value);
                    }
                    // A re-recorded field takes its new type: a later string
                    // must not be written back as the number it replaced.
                    for (key, value) in &extractor.fields {
                        if is_json_scalar(value) {
                            act_ctx.typed_metadata_keys.insert(key.clone());
                        } else {
                            act_ctx.typed_metadata_keys.remove(key);
                        }
                    }

                    // Test completion detection
                    if act_ctx.activity_type == "test" && string_fields.contains_key("test.status")
                    {
                        let context = act_ctx.clone();
                        drop(entry);

                        let ended_at = x0k_timeline_record::now_micros();
                        let duration_ms = duration_ms_between(context.started_at, ended_at);

                        let completed = CompletedActivity {
                            context,
                            ended_at,
                            duration_ms,
                        };

                        if let Some(ref sink) = self.activity_sink {
                            sink.update_activity(completed);
                        }

                        self.contexts.remove(id);
                    }
                }
            }
        }
    }

    fn on_close(&self, id: span::Id, _ctx: Context<'_, S>) {
        if let Some((_, context)) = self.contexts.remove(&id) {
            match context {
                InFlightContext::Span(mut span_ctx) => {
                    let ended_at = x0k_timeline_record::now_micros();
                    span_ctx.duration_ms = Some(duration_ms_between(span_ctx.start_time, ended_at));

                    if let Some(ref sink) = self.span_sink {
                        sink.write_span(&span_ctx);
                    } else if let Some(sink) = crate::span_tracing::get_span_sink() {
                        sink.write_span(&span_ctx);
                    }
                }
                InFlightContext::Activity(mut act_ctx) => {
                    let ended_at = x0k_timeline_record::now_micros();
                    let duration_ms = duration_ms_between(act_ctx.started_at, ended_at);

                    // For tests, set default status if not already set
                    if act_ctx.activity_type == "test" {
                        if act_ctx.metadata.get("test.status") == Some(&"started".to_string()) {
                            act_ctx
                                .metadata
                                .insert("test.status".to_string(), "completed".to_string());
                        }

                        let completed = CompletedActivity {
                            context: act_ctx,
                            ended_at,
                            duration_ms,
                        };

                        if let Some(ref sink) = self.activity_sink {
                            sink.update_activity(completed);
                        }
                    } else {
                        let completed = CompletedActivity {
                            context: act_ctx,
                            ended_at,
                            duration_ms,
                        };

                        if let Some(ref sink) = self.activity_sink {
                            sink.record_activity(completed);
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Private: span vs activity creation
// ---------------------------------------------------------------------------

impl ObservabilityLayer {
    fn on_new_trace_span<S>(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: &Context<'_, S>,
    ) where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let metadata = attrs.metadata();

        let filter = self.filter_state.load();
        if !filter.should_track(metadata) {
            return;
        }

        let mut extractor = RichFieldExtractor::new();
        attrs.record(&mut extractor);

        // Inherit node_name from the nearest ancestor session span unless this
        // span set it explicitly. This makes every span row in the timeline
        // queryable by node_name without grepping description text.
        if !extractor.fields.contains_key("node_name") {
            if let Some(node_name) = self.get_node_name(id, ctx) {
                extractor.fields.insert(
                    "node_name".to_string(),
                    serde_json::Value::String(node_name),
                );
            }
        }

        let (trace_id, parent_span_id) =
            remote_parent(&mut extractor.fields).unwrap_or_else(|| self.get_trace_context(id, ctx));
        let span_id = Self::generate_span_id();

        let source_file = metadata.file().map(|s| s.to_string());
        let source_line = metadata.line();
        let module = metadata.module_path().map(|s| s.to_string());
        let span_name = metadata.name().to_string();
        let function = Some(span_name.clone());

        // Promote session_id from span fields to the top-level column.
        // When a function uses #[instrument(fields(session_id = ...))] the value
        // lands in fields_json but the timeline column comes from X0K_SESSION_ID
        // env var. If the process isn't the agent sandbox (e.g., daemon), the
        // column would be NULL despite fields having the value.
        let session_id = extractor
            .fields
            .get("session_id")
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .or_else(|| self.session_id.clone());

        let span_ctx = SpanContext {
            span_id,
            trace_id,
            parent_span_id,
            start_time: x0k_timeline_record::now_micros(),
            duration_ms: None,
            level: metadata.level().to_string().to_lowercase(),
            function,
            module,
            source_file,
            source_line,
            span_name,
            target: metadata.target().to_string(),
            fields: extractor.fields,
            events: Vec::new(),
            workstream_id: self.workstream_id.clone(),
            intent_id: self.intent_id.clone(),
            software_module_id: self.software_module_id.clone(),
            session_id,
            pid: Some(self.pid),
        };

        self.evict_if_needed();
        self.contexts
            .insert(id.clone(), InFlightContext::Span(span_ctx));
    }

    fn on_new_activity<S>(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: &Context<'_, S>)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let metadata = attrs.metadata();

        let mut extractor = RichFieldExtractor::new();
        attrs.record(&mut extractor);
        let string_fields = extractor.as_string_map();

        let activity_id = string_fields
            .get("activity.id")
            .map(|s| ActivityId(s.to_string()))
            .unwrap_or_default();

        let activity_type = string_fields
            .get("activity.type")
            .map(|s| s.to_string())
            .or_else(|| {
                if metadata.name() == "test_activity" {
                    Some("test".to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "unknown".to_string());

        let parent_id = string_fields
            .get("activity.parent_id")
            .map(|s| ActivityId(s.to_string()))
            .or_else(|| {
                ctx.span(id)
                    .and_then(|span| span.parent())
                    .and_then(|parent| self.contexts.get(&parent.id()))
                    .and_then(|entry| {
                        if let InFlightContext::Activity(ref act) = *entry {
                            Some(act.activity_id.clone())
                        } else {
                            None
                        }
                    })
            });

        let (fallback_trace_id, fallback_parent_span_id) = self.get_trace_context(id, ctx);
        let trace_id = string_fields
            .get("event.dedup_key")
            .or_else(|| string_fields.get("trace.id"))
            .map(|s| s.to_string())
            .or(Some(fallback_trace_id));

        let activity_data = string_fields
            .get("activity.description")
            .or_else(|| string_fields.get("activity.data"))
            .map(|s| s.to_string());
        let lazy_data = Self::extract_lazy_data(&string_fields);

        let actor: Option<String> = self
            .actor_resolver
            .as_ref()
            .and_then(|resolver| resolver.resolve_actor(attrs));

        let source_file = metadata.file().map(|s| s.to_string());
        let source_line = metadata.line();
        let source_module = metadata.module_path().map(|s| s.to_string());
        let source_function = if metadata.name() == "activity" || metadata.name() == "test_activity"
        {
            string_fields
                .get("activity.function")
                .map(|s| s.to_string())
        } else {
            Some(metadata.name().to_string())
        };
        let span_name = Some(metadata.name().to_string());
        let correlation_id = string_fields.get("correlation_id").cloned();

        let mut act_ctx = ActivityContext {
            activity_id: activity_id.clone(),
            activity_type: activity_type.clone(),
            parent_id,
            actor,
            started_at: x0k_timeline_record::now_micros(),
            metadata: string_fields,
            typed_metadata_keys: extractor.typed_keys(),
            correlation_id,
            trace_id,
            span_id: Some(Self::generate_span_id()),
            parent_span_id: fallback_parent_span_id,
            activity_data,
            lazy_data,
            source_file,
            source_line,
            source_function,
            source_module,
            span_name,
        };

        if let Some(ref module_id) = self.software_module_id {
            act_ctx
                .metadata
                .entry("activity.software_module".to_string())
                .or_insert_with(|| module_id.clone());
        }

        if let Some(ref sid) = self.session_id {
            act_ctx
                .metadata
                .entry("activity.session_id".to_string())
                .or_insert_with(|| sid.clone());
        }

        act_ctx
            .metadata
            .entry("pid".to_string())
            .or_insert_with(|| self.pid.to_string());

        if act_ctx.activity_type == "test" {
            act_ctx
                .metadata
                .insert("test.status".to_string(), "started".to_string());
        }

        self.evict_if_needed();
        self.contexts
            .insert(id.clone(), InFlightContext::Activity(act_ctx.clone()));

        // Immediately record test activities so panicking tests are still captured
        if act_ctx.activity_type == "test" {
            if let Some(ref sink) = self.activity_sink {
                let started = CompletedActivity {
                    context: act_ctx,
                    ended_at: x0k_timeline_record::now_micros(),
                    duration_ms: 0,
                };
                sink.record_activity(started);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span_tracing::{ModuleRule, SpanFilterState};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tracing::{info_span, trace, Level};
    use tracing_subscriber::{layer::SubscriberExt, Registry};

    struct CountingSpanSink {
        count: AtomicUsize,
    }

    impl SpanSink for CountingSpanSink {
        fn write_span(&self, _span: &SpanContext) {
            self.count.fetch_add(1, AtomicOrdering::SeqCst);
        }

        fn get_latest_cause(&self, _entity: &str, _attribute: &str) -> Option<String> {
            None
        }

        fn flush(&self) {}
    }

    struct TestActivitySink {
        activities: Arc<DashMap<String, CompletedActivity>>,
    }

    impl ActivitySink for TestActivitySink {
        fn record_activity(&self, activity: CompletedActivity) {
            self.activities
                .insert(activity.context.activity_id.0.clone(), activity);
        }

        fn update_activity(&self, activity: CompletedActivity) {
            self.activities
                .insert(activity.context.activity_id.0.clone(), activity);
        }
    }

    fn accept_all_filter() -> Arc<ArcSwap<SpanFilterState>> {
        Arc::new(ArcSwap::from_pointee(SpanFilterState {
            min_level: Level::TRACE,
            rules: vec![ModuleRule {
                prefix: String::new(),
                enabled: true,
                level: Level::TRACE,
            }],
            exclude_modules: vec![],
            capture_events: true,
        }))
    }

    fn reject_all_filter() -> Arc<ArcSwap<SpanFilterState>> {
        Arc::new(ArcSwap::from_pointee(SpanFilterState {
            min_level: Level::TRACE,
            rules: vec![ModuleRule {
                prefix: String::new(),
                enabled: false,
                level: Level::TRACE,
            }],
            exclude_modules: vec![],
            capture_events: true,
        }))
    }

    #[test]
    fn test_span_tracking() {
        let span_sink = Arc::new(CountingSpanSink {
            count: AtomicUsize::new(0),
        });

        let layer = ObservabilityLayer::builder()
            .with_span_sink(span_sink.clone())
            .with_filter_state(accept_all_filter())
            .build();

        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("test_operation", task_id = "task-123");
            let _guard = span.enter();
            trace!("Doing some work");
        });

        assert_eq!(span_sink.count.load(AtomicOrdering::SeqCst), 1);
    }

    /// Sink that captures the full `SpanContext` of each written span so a test
    /// can compare the canonical span_id/trace_id against `current_span_correlation`.
    struct CapturingSpanSink {
        spans: Arc<DashMap<String, SpanContext>>,
    }

    impl SpanSink for CapturingSpanSink {
        fn write_span(&self, span: &SpanContext) {
            self.spans.insert(span.span_name.clone(), span.clone());
        }
        fn get_latest_cause(&self, _entity: &str, _attribute: &str) -> Option<String> {
            None
        }
        fn flush(&self) {}
    }

    #[test]
    fn current_span_correlation_matches_sink_ids() {
        // The global CONTEXT_REGISTRY is first-write-wins per process. Build a
        // layer and make it the global default so the current span resolves
        // through whichever map the OnceLock holds. We assert the *format* of
        // the resolved ids (16-/32-char hex) — the canonical sink columns —
        // rather than the opaque `Id(N)` debug form the old impl emitted.
        let spans = Arc::new(DashMap::new());
        let span_sink = Arc::new(CapturingSpanSink {
            spans: spans.clone(),
        });
        let layer = ObservabilityLayer::builder()
            .with_span_sink(span_sink.clone())
            .with_filter_state(accept_all_filter())
            .build();
        let subscriber = Registry::default().with(layer);

        let mut resolved: Option<SpanCorrelation> = None;
        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("correlation_probe");
            let _guard = span.enter();
            resolved = current_span_correlation();
        });

        // The registry may point at an earlier test's layer (OnceLock), so a
        // None here is tolerated; but when Some, the ids must be canonical hex,
        // never the `Id(N)` debug form.
        if let Some(corr) = resolved {
            assert_eq!(
                corr.span_id.len(),
                16,
                "span_id must be 16-char hex (canonical sink form), got {:?}",
                corr.span_id
            );
            assert!(
                corr.span_id.chars().all(|c| c.is_ascii_hexdigit()),
                "span_id must be hex, got {:?}",
                corr.span_id
            );
            assert!(
                !corr.span_id.starts_with("Id("),
                "must not be the opaque tracing Id debug form"
            );
            if !corr.trace_id.is_empty() {
                assert_eq!(corr.trace_id.len(), 32, "trace_id must be 32-char hex");
            }
        }
    }

    #[test]
    fn test_span_filtering() {
        let span_sink = Arc::new(CountingSpanSink {
            count: AtomicUsize::new(0),
        });

        let layer = ObservabilityLayer::builder()
            .with_span_sink(span_sink.clone())
            .with_filter_state(reject_all_filter())
            .build();

        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("test_operation");
            let _guard = span.enter();
        });

        assert_eq!(span_sink.count.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn test_activity_tracking() {
        let activity_sink = Arc::new(TestActivitySink {
            activities: Arc::new(DashMap::new()),
        });

        let layer = ObservabilityLayer::builder()
            .with_activity_sink(activity_sink.clone())
            .with_filter_state(accept_all_filter())
            .build();

        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!(
                "activity",
                activity.id = "test-123",
                activity.r#type = "test_operation"
            );
            let _guard = span.enter();
            trace!("Doing some work");
        });

        assert_eq!(activity_sink.activities.len(), 1);
        assert!(activity_sink.activities.contains_key("test-123"));
    }

    #[test]
    fn test_activity_test_status_update() {
        let activity_sink = Arc::new(TestActivitySink {
            activities: Arc::new(DashMap::new()),
        });

        let layer = ObservabilityLayer::builder()
            .with_activity_sink(activity_sink.clone())
            .with_filter_state(accept_all_filter())
            .build();

        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!(
                "activity",
                activity.id = "test-456",
                activity.r#type = "test",
                test.name = "my_test",
                test.status = tracing::field::Empty,
            );
            let _guard = span.enter();
            span.record("test.status", "passed");
            trace!("Test completed");
        });

        assert_eq!(activity_sink.activities.len(), 1);
        let activity = activity_sink.activities.get("test-456").unwrap();
        assert_eq!(
            activity.context.metadata.get("test.status").unwrap(),
            "passed"
        );
    }

    /// observability-timeline §5: a `u64` or `bool` recorded through `tracing`
    /// reaches the activity record marked as a JSON scalar, so the sink writes
    /// it into `metadata_json` as a number or bool rather than a string. A
    /// field displayed with `%` is text and stays unmarked; a later `record`
    /// retypes the field in either direction.
    #[test]
    fn tracing_scalars_reach_the_activity_record_typed() {
        let activity_sink = Arc::new(TestActivitySink {
            activities: Arc::new(DashMap::new()),
        });
        let layer = ObservabilityLayer::builder()
            .with_activity_sink(activity_sink.clone())
            .with_filter_state(accept_all_filter())
            .build();
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!(
                "activity",
                activity.id = "typed-1",
                "activity.type" = "sync.frontier.advanced",
                entry_count = 5u64,
                delta = -3i64,
                converged = true,
                peer = %"42",
                late = tracing::field::Empty,
                demoted = 7u64,
            );
            span.record("late", 9u64);
            span.record("demoted", "seven");
            let _guard = span.enter();
        });

        let activity = activity_sink.activities.get("typed-1").unwrap();
        let ctx = &activity.context;
        let typed: Vec<&str> = ctx.typed_metadata_keys.iter().map(String::as_str).collect();
        assert_eq!(typed, ["converged", "delta", "entry_count", "late"]);
        assert_eq!(ctx.metadata["entry_count"], "5");
        assert_eq!(ctx.metadata["delta"], "-3");
        assert_eq!(ctx.metadata["converged"], "true");
        assert_eq!(ctx.metadata["late"], "9");
        assert_eq!(ctx.metadata["peer"], "42");
        assert_eq!(ctx.metadata["demoted"], "seven");
    }

    /// Which freestanding events earn the durable tier. The linked incident is
    /// `typed_warns_keep_their_semantic_event_name_without_promoting_info`.
    #[test]
    fn typed_warns_keep_their_semantic_event_name_without_promoting_info() {
        fn extractor(fields: &[(&str, &str)]) -> RichFieldExtractor {
            let mut extractor = RichFieldExtractor::new();
            for (k, v) in fields {
                extractor.fields.insert(
                    (*k).to_string(),
                    serde_json::Value::String((*v).to_string()),
                );
            }
            extractor
        }

        fn decide(level: Level, fields: &[(&str, &str)]) -> Option<String> {
            ObservabilityLayer::promoted_activity_type(&level, &extractor(fields))
                .map(|promoted| promoted.activity_type)
        }

        // Severity alone is enough — this is the P0's warn!.
        assert_eq!(decide(Level::WARN, &[]).as_deref(), Some("log.warn"));
        assert_eq!(decide(Level::ERROR, &[]).as_deref(), Some("log.error"));

        // Structure alone is enough, and names the event's own type.
        assert_eq!(
            decide(Level::INFO, &[("activity.type", "grove.fold_cache.hit")]).as_deref(),
            Some("grove.fold_cache.hit")
        );

        // `event_type` names a log row but does not opt INFO into durable
        // retention. The same name on WARN is retained through promotion.
        assert_eq!(
            decide(Level::INFO, &[("event_type", "sync.lane.converged")]),
            None
        );
        assert_eq!(
            ObservabilityLayer::event_type_field(&extractor(&[(
                "event_type",
                "sync.lane.converged",
            )]))
            .as_deref(),
            Some("sync.lane.converged")
        );
        assert_eq!(
            decide(Level::WARN, &[("event_type", "sync.peer.quiet")]).as_deref(),
            Some("sync.peer.quiet")
        );

        // An explicit type beats the synthetic severity name.
        assert_eq!(
            decide(
                Level::WARN,
                &[("activity.type", "grove.fold_cache.unusable")]
            )
            .as_deref(),
            Some("grove.fold_cache.unusable")
        );

        // Plain chatter stays a log-class message row. Policy, not oversight.
        assert_eq!(decide(Level::INFO, &[]), None);
        assert_eq!(decide(Level::INFO, &[("path", "/tmp/x")]), None);
        // An empty activity.type is not structure.
        assert_eq!(decide(Level::INFO, &[("activity.type", "")]), None);
    }

    /// 2026-09-03: promoted WARN/ERROR rows were written with a hard-coded
    /// `StorageTier::Central`, which bypassed **both** placement mechanisms —
    /// the `route_activity_storage_tier` prefix table and the documented
    /// `activity.storage_tier` metadata override. `sync.payload.starved` rode
    /// that bypass to 534k central-labelled rows, 35.7% of the node's timeline.
    ///
    /// Placement is a separate decision from promotion, and this pins both
    /// mechanisms plus the negative row: a promoted event the table has no
    /// prefix for still lands Central. Nothing here touches `retention_class` —
    /// the sink derives that from `entry_kind`, and Central and Ephemeral
    /// activities alike are `event` rows on the same clock.
    #[test]
    fn promoted_events_route_their_storage_tier_2026_09_03() {
        fn tier(level: Level, fields: &[(&str, &str)]) -> StorageTier {
            let mut extractor = RichFieldExtractor::new();
            for (k, v) in fields {
                extractor
                    .fields
                    .insert((*k).to_string(), serde_json::Value::String((*v).to_string()));
            }
            let promoted = ObservabilityLayer::promoted_activity_type(&level, &extractor)
                .expect("fixture should promote");
            ObservabilityLayer::promoted_storage_tier(&promoted, &extractor.as_string_map())
        }

        // Mechanism 1, the prefix route. `sync.*` is not an operator-facing
        // prefix, so the firehose class no longer claims the central label.
        assert_eq!(
            tier(Level::WARN, &[("event_type", "sync.payload.starved")]),
            StorageTier::Ephemeral
        );
        // The table's own central and record prefixes still reach it.
        assert_eq!(
            tier(Level::ERROR, &[("event_type", "system.panic")]),
            StorageTier::Central
        );
        assert_eq!(
            tier(Level::WARN, &[("activity.type", "usage.tokens")]),
            StorageTier::Record
        );

        // Mechanism 2, the metadata override — in both directions, and via the
        // legacy alias.
        assert_eq!(
            tier(
                Level::ERROR,
                &[
                    ("event_type", "system.panic"),
                    ("activity.storage_tier", "ephemeral"),
                ],
            ),
            StorageTier::Ephemeral
        );
        assert_eq!(
            tier(
                Level::WARN,
                &[
                    ("event_type", "sync.lanes.shadow_divergence"),
                    ("activity.storage_tier", "central"),
                ],
            ),
            StorageTier::Central
        );
        assert_eq!(
            tier(
                Level::WARN,
                &[
                    ("event_type", "sync.payload.starved"),
                    ("activity.durability", "record"),
                ],
            ),
            StorageTier::Record
        );
        // An unrecognised override is not an override; routing still decides.
        assert_eq!(
            tier(
                Level::WARN,
                &[
                    ("event_type", "sync.payload.starved"),
                    ("activity.storage_tier", "bogus"),
                ],
            ),
            StorageTier::Ephemeral
        );

        // The negative row. A promoted event with no class of its own has no
        // prefix for the table to match; it stays Central rather than falling
        // into the table's catch-all.
        assert_eq!(tier(Level::WARN, &[]), StorageTier::Central);
        assert_eq!(tier(Level::ERROR, &[]), StorageTier::Central);
        // ...and an override still moves it.
        assert_eq!(
            tier(Level::WARN, &[("activity.storage_tier", "ephemeral")]),
            StorageTier::Ephemeral
        );

        // Our override parse must not drift from the canonical one.
        for value in ["central", "ephemeral", "record", "bogus"] {
            let mut fields = std::collections::HashMap::new();
            fields.insert(STORAGE_TIER_KEY.to_string(), value.to_string());
            let promoted = PromotedEvent::named("sync.payload.starved".to_string());
            assert_eq!(
                ObservabilityLayer::promoted_storage_tier(&promoted, &fields),
                route_activity_storage_tier("sync.payload.starved", &fields),
                "override {value} disagrees with route_activity_storage_tier"
            );
        }
    }
}
