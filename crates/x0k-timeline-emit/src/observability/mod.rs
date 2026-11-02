//! Unified observability layer for spans, activities, and log events
//!
//! Single `Layer<S>` implementation that captures all tracing data through one
//! DashMap, one channel, and one set of eviction logic.
//!
//! # Discrimination
//!
//! - Spans named `"activity"` or `"test_activity"` → tracked as activities
//! - All other spans → filtered by `SpanFilterState` and tracked as spans

mod layer;

pub use layer::{
    current_span_correlation, current_traceparent, ObservabilityLayer, ObservabilityLayerBuilder,
    SpanCorrelation,
};
