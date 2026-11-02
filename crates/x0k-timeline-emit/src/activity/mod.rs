//! Activity tracking and RDF store for unified system observability

pub mod async_sink;
pub mod channel_sink;
pub mod context;
pub mod emission;
pub mod layer;

pub use async_sink::{AsyncActivitySink, AsyncActivitySinkBuilder, AsyncSinkConfig};
pub use channel_sink::{take_activity_receiver, ChannelActivitySink};
pub use context::{
    activity_span, child_activity_span, current_activity_id, test_activity_span, ActivityGuard,
};
pub use emission::{build_activity_record, emit_activity_record, ActivityRecordSpec};
pub use layer::ActorResolver;

// Re-export canonical types from x0k-types for backwards compatibility
pub use x0k_timeline_record::record::{
    ActivityContext, ActivityOperation, ActivitySink, CompletedActivity, LazySerializable,
    SerializableData,
};
