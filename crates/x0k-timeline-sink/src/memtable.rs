//! In-memory accumulator for RecordBatches
//!
//! Collects RecordBatches from micro-batch conversions and triggers a flush
//! to parquet when a size or time threshold is reached.

use arrow::array::RecordBatch;
use std::time::{Duration, Instant};

/// In-memory accumulator that triggers parquet flushes based on size or time.
pub struct Memtable {
    batches: Vec<RecordBatch>,
    estimated_bytes: usize,
    row_count: usize,
    first_insert: Option<Instant>,
    flush_size_bytes: usize,
    flush_interval: Duration,
}

impl Memtable {
    /// Create a new memtable with the given flush thresholds.
    pub fn new(flush_size_bytes: usize, flush_interval: Duration) -> Self {
        Self {
            batches: Vec::new(),
            estimated_bytes: 0,
            row_count: 0,
            first_insert: None,
            flush_size_bytes,
            flush_interval,
        }
    }

    /// Push a RecordBatch into the memtable.
    pub fn push(&mut self, batch: RecordBatch) {
        if self.first_insert.is_none() {
            self.first_insert = Some(Instant::now());
        }
        self.row_count += batch.num_rows();
        self.estimated_bytes += batch.get_array_memory_size();
        self.batches.push(batch);
    }

    /// Whether the memtable should be flushed to parquet.
    ///
    /// Returns true if:
    /// - Accumulated bytes >= flush_size_bytes, OR
    /// - Time since first insert >= flush_interval
    pub fn should_flush(&self) -> bool {
        if self.batches.is_empty() {
            return false;
        }
        if self.estimated_bytes >= self.flush_size_bytes {
            return true;
        }
        if let Some(first) = self.first_insert {
            if first.elapsed() >= self.flush_interval {
                return true;
            }
        }
        false
    }

    /// Take all accumulated batches, resetting the memtable.
    pub fn take(&mut self) -> Vec<RecordBatch> {
        self.estimated_bytes = 0;
        self.row_count = 0;
        self.first_insert = None;
        std::mem::take(&mut self.batches)
    }

    /// Restore batches after a parquet flush failed.
    ///
    /// The original insertion time is retained so ingest lag measures the
    /// whole stall rather than resetting on each retry.
    pub fn restore(&mut self, batches: Vec<RecordBatch>, first_insert: Instant) {
        for batch in batches {
            self.row_count += batch.num_rows();
            self.estimated_bytes += batch.get_array_memory_size();
            self.batches.push(batch);
        }
        self.first_insert = Some(
            self.first_insert
                .map_or(first_insert, |current| current.min(first_insert)),
        );
    }

    /// When the oldest currently pending batch entered the memtable.
    pub fn first_insert(&self) -> Option<Instant> {
        self.first_insert
    }

    /// Time remaining until the flush interval fires.
    ///
    /// Returns `None` if the memtable is empty or already past the interval.
    pub fn time_until_flush(&self) -> Option<Duration> {
        let first = self.first_insert?;
        let elapsed = first.elapsed();
        if elapsed >= self.flush_interval {
            Some(Duration::ZERO)
        } else {
            Some(self.flush_interval - elapsed)
        }
    }

    /// Whether the memtable has no batches.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Number of rows accumulated.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Estimated bytes accumulated.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let ids: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        let ids: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(ids))]).unwrap()
    }

    #[test]
    fn test_empty_memtable() {
        let mt = Memtable::new(1024, Duration::from_secs(60));
        assert!(mt.is_empty());
        assert!(!mt.should_flush());
        assert!(mt.time_until_flush().is_none());
    }

    #[test]
    fn test_push_and_take() {
        let mut mt = Memtable::new(1024 * 1024, Duration::from_secs(60));
        mt.push(test_batch(5));
        mt.push(test_batch(3));

        assert!(!mt.is_empty());
        assert_eq!(mt.row_count(), 8);

        let batches = mt.take();
        assert_eq!(batches.len(), 2);
        assert!(mt.is_empty());
        assert_eq!(mt.row_count(), 0);
    }

    #[test]
    fn test_flush_by_size() {
        // Tiny threshold so a single batch triggers it
        let mut mt = Memtable::new(1, Duration::from_secs(3600));
        mt.push(test_batch(10));
        assert!(mt.should_flush());
    }

    #[test]
    fn test_flush_by_time() {
        let mut mt = Memtable::new(1024 * 1024 * 1024, Duration::from_millis(0));
        mt.push(test_batch(1));
        // Duration::from_millis(0) means immediately
        assert!(mt.should_flush());
    }

    #[test]
    fn test_time_until_flush() {
        let mut mt = Memtable::new(1024 * 1024, Duration::from_secs(60));
        assert!(mt.time_until_flush().is_none());

        mt.push(test_batch(1));
        let remaining = mt.time_until_flush().unwrap();
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::from_secs(58)); // some slack for test execution
    }
}
