//! Async activity sink implementation with channel-based processing

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, warn};

use x0k_timeline_record::record::{ActivitySink, CompletedActivity};

/// Configuration for async activity processing
#[derive(Debug, Clone)]
pub struct AsyncSinkConfig {
    /// Size of the channel buffer
    pub buffer_size: usize,
    /// Maximum batch size for processing
    pub batch_size: usize,
    /// Whether to block on channel full (vs drop activities)
    pub block_on_full: bool,
}

impl Default for AsyncSinkConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
            batch_size: 100,
            block_on_full: false,
        }
    }
}

/// Operation type for async processing
enum ActivityOperation {
    Record(CompletedActivity),
    Update(CompletedActivity),
}

/// Async wrapper for any ActivitySink that processes activities in background
pub struct AsyncActivitySink {
    tx: mpsc::Sender<ActivityOperation>,
    _handle: JoinHandle<()>,
}

impl AsyncActivitySink {
    /// Serialize lazy data in a CompletedActivity (called in background task)
    fn serialize_lazy_data(activity: &mut CompletedActivity) {
        // Only process if we have lazy data and no existing activity_data
        if let Some(lazy_data) = &activity.context.lazy_data {
            if activity.context.activity_data.is_none() {
                // Perform the serialization that was deferred from the hot path
                match lazy_data.to_json_string() {
                    Ok(json_string) => {
                        activity.context.activity_data = Some(json_string);
                    }
                    Err(e) => {
                        // Log error but don't fail the entire operation
                        tracing::warn!(
                            activity_id = %activity.context.activity_id.0,
                            error = %e,
                            "Failed to serialize lazy activity data"
                        );
                        activity.context.activity_data = Some("{}".to_string());
                    }
                }
            }
        }
    }

    /// Create a new async sink wrapping an underlying sink
    pub fn new(inner: Arc<dyn ActivitySink>, config: AsyncSinkConfig) -> Self {
        let (tx, mut rx) = mpsc::channel(config.buffer_size);

        let handle = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(config.batch_size);

            while let Some(operation) = rx.recv().await {
                batch.push(operation);

                // Try to batch more operations if available
                while batch.len() < config.batch_size {
                    match rx.try_recv() {
                        Ok(operation) => batch.push(operation),
                        Err(_) => break,
                    }
                }

                // Process the batch with lazy serialization
                for operation in batch.drain(..) {
                    match operation {
                        ActivityOperation::Record(mut activity) => {
                            // Perform lazy serialization here in the background task
                            Self::serialize_lazy_data(&mut activity);
                            inner.record_activity(activity);
                        }
                        ActivityOperation::Update(mut activity) => {
                            // Perform lazy serialization here in the background task
                            Self::serialize_lazy_data(&mut activity);
                            inner.update_activity(activity);
                        }
                    }
                }
            }

            // Process any remaining operations on shutdown
            if !batch.is_empty() {
                for operation in batch {
                    match operation {
                        ActivityOperation::Record(mut activity) => {
                            Self::serialize_lazy_data(&mut activity);
                            inner.record_activity(activity);
                        }
                        ActivityOperation::Update(mut activity) => {
                            Self::serialize_lazy_data(&mut activity);
                            inner.update_activity(activity);
                        }
                    }
                }
            }
        });

        Self {
            tx,
            _handle: handle,
        }
    }

    /// Create with default configuration
    pub fn with_default_config(inner: Arc<dyn ActivitySink>) -> Self {
        Self::new(inner, AsyncSinkConfig::default())
    }
}

impl ActivitySink for AsyncActivitySink {
    fn record_activity(&self, activity: CompletedActivity) {
        // Try to send without blocking
        match self.tx.try_send(ActivityOperation::Record(activity)) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(op)) => {
                // Channel is full - log and drop (or could implement blocking)
                if let ActivityOperation::Record(activity) = op {
                    warn!(
                        activity_id = %activity.context.activity_id.0,
                        "Activity channel full, dropping activity"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("Activity processing channel closed");
            }
        }
    }

    fn update_activity(&self, activity: CompletedActivity) {
        // Send as an update operation
        match self.tx.try_send(ActivityOperation::Update(activity)) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(op)) => {
                // Channel is full - log and drop
                if let ActivityOperation::Update(activity) = op {
                    warn!(
                        activity_id = %activity.context.activity_id.0,
                        "Activity channel full, dropping update"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("Activity processing channel closed");
            }
        }
    }
}

/// Builder for AsyncActivitySink with fluent API
pub struct AsyncActivitySinkBuilder {
    config: AsyncSinkConfig,
}

impl Default for AsyncActivitySinkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncActivitySinkBuilder {
    pub fn new() -> Self {
        Self {
            config: AsyncSinkConfig::default(),
        }
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    pub fn block_on_full(mut self, block: bool) -> Self {
        self.config.block_on_full = block;
        self
    }

    pub fn build(self, inner: Arc<dyn ActivitySink>) -> AsyncActivitySink {
        AsyncActivitySink::new(inner, self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use x0k_timeline_record::record::ActivityContext;

    struct CountingSink {
        count: Arc<AtomicUsize>,
        delay_ms: u64,
    }

    impl ActivitySink for CountingSink {
        fn record_activity(&self, _activity: CompletedActivity) {
            if self.delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(self.delay_ms));
            }
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        fn update_activity(&self, _activity: CompletedActivity) {
            // For testing, just treat update same as record
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_async_batching() {
        let count = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingSink {
            count: count.clone(),
            delay_ms: 0,
        });

        let async_sink = AsyncActivitySink::with_default_config(inner);

        // Send multiple activities
        for i in 0..100 {
            let activity = CompletedActivity {
                context: ActivityContext {
                    activity_id: x0k_timeline_record::ActivityId(format!("test-{}", i)),
                    activity_type: "test".to_string(),
                    parent_id: None,
                    actor: None,
                    started_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    metadata: Default::default(),
                    typed_metadata_keys: Default::default(),
                    correlation_id: None,
                    trace_id: None,
                    span_id: None,
                    parent_span_id: None,
                    activity_data: None,
                    lazy_data: None,
                    source_file: None,
                    source_line: None,
                    source_function: None,
                    source_module: None,
                    span_name: None,
                },
                ended_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                duration_ms: 100,
            };
            async_sink.record_activity(activity);
        }

        // Give some time for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // All activities should be processed
        assert_eq!(count.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn test_async_non_blocking() {
        let count = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(CountingSink {
            count: count.clone(),
            delay_ms: 10, // Simulate slow sink
        });

        let async_sink = AsyncActivitySink::with_default_config(inner);

        let start = Instant::now();

        // Send activities - should not block
        for i in 0..10 {
            let activity = CompletedActivity {
                context: ActivityContext {
                    activity_id: x0k_timeline_record::ActivityId(format!("test-{}", i)),
                    activity_type: "test".to_string(),
                    parent_id: None,
                    actor: None,
                    started_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    metadata: Default::default(),
                    typed_metadata_keys: Default::default(),
                    correlation_id: None,
                    trace_id: None,
                    span_id: None,
                    parent_span_id: None,
                    activity_data: None,
                    lazy_data: None,
                    source_file: None,
                    source_line: None,
                    source_function: None,
                    source_module: None,
                    span_name: None,
                },
                ended_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                duration_ms: 100,
            };
            async_sink.record_activity(activity);
        }

        let send_duration = start.elapsed();

        // Sending should be fast (not 10 * 10ms = 100ms)
        assert!(send_duration < Duration::from_millis(50));

        // Wait for processing to complete
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(count.load(Ordering::Relaxed), 10);
    }
}
