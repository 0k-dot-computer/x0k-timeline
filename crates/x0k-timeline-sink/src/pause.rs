//! Whether the writer is paused, and what it has discarded while it was
//! (`timeline-public-surface` §7). One value, shared by the two places records
//! enter the pipeline: the writer's ingest and the flush task's channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use x0k_timeline_record::pause::{DiscardedRows, PauseState};
use x0k_timeline_record::record::RetentionClass;

use crate::types::TimelineRecord;

#[derive(Debug, Default)]
pub struct WriterPause {
    paused: AtomicBool,
    state: Mutex<PauseState>,
}

impl WriterPause {
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Whether the pipeline keeps `record`: always, unless the writer is paused
    /// and the record is not of the record class. A discard is counted here.
    pub fn keeps(&self, record: &TimelineRecord) -> bool {
        if !self.is_paused() {
            return true;
        }
        let class = record.retention_class();
        if class == RetentionClass::Record {
            return true;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        add_discard(&mut state.discarded, class);
        false
    }

    /// Enter the paused state.
    pub fn begin(&self, reason: String, since_us: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        *state = PauseState {
            paused: true,
            since_us: Some(since_us),
            reason: Some(reason),
            discarded: DiscardedRows::default(),
        };
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Leave the paused state, answering what it held.
    pub fn end(&self) -> PauseState {
        self.paused.store(false, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *state)
    }

    pub fn state(&self) -> PauseState {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

fn add_discard(discarded: &mut DiscardedRows, class: RetentionClass) {
    match class {
        RetentionClass::Span => discarded.span += 1,
        RetentionClass::Log => discarded.log += 1,
        RetentionClass::Event | RetentionClass::Record => discarded.event += 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActivityContext, CompletedActivity};
    use x0k_timeline_record::record::{LogEventData, StorageTier};

    fn log() -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData::default())
    }

    fn activity(tier: StorageTier) -> TimelineRecord {
        let activity = CompletedActivity {
            context: ActivityContext::default(),
            ended_at: 0,
            duration_ms: 0,
        };
        TimelineRecord::Activity(activity, tier)
    }

    #[test]
    fn a_running_writer_keeps_everything_and_counts_nothing() {
        let pause = WriterPause::default();
        assert!(pause.keeps(&log()));
        assert_eq!(pause.state().discarded, DiscardedRows::default());
    }

    #[test]
    fn a_paused_writer_keeps_the_record_class_and_counts_the_rest_by_class() {
        let pause = WriterPause::default();
        pause.begin("test".to_string(), 7);
        assert!(!pause.keeps(&log()));
        assert!(!pause.keeps(&activity(StorageTier::Central)));
        assert!(pause.keeps(&activity(StorageTier::Record)));
        let discarded = pause.state().discarded;
        assert_eq!((discarded.span, discarded.log, discarded.event), (0, 1, 1));
    }

    #[test]
    fn ending_a_pause_answers_what_it_held_and_clears_it() {
        let pause = WriterPause::default();
        pause.begin("test".to_string(), 7);
        assert!(!pause.keeps(&log()));
        let ended = pause.end();
        assert_eq!(ended.discarded.log, 1);
        assert_eq!(ended.since_us, Some(7));
        assert!(!pause.is_paused());
        assert_eq!(pause.state(), PauseState::default());
    }
}
