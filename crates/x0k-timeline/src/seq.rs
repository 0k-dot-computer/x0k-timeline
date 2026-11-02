//! The writer's sequence numbers: the `seq` of a record's `(node, seq)`
//! identity (timeline-public-surface §1), which also
//! keys what an upstream writer acknowledges (§2, observability-timeline §15).
//!
//! A sequence number is never issued twice by one writer, across restarts and
//! crashes included. Rows leave the node (to an upstream writer) before their
//! segment is written, so a counter persisted with the segment could hand a
//! restarted writer a number that a row already sent carries. The writer
//! therefore reserves ahead: it durably records a high-water mark, the first
//! number not yet reserved, before it issues any number below it, and a
//! restart starts at the recorded mark. Numbers reserved and never issued
//! leave gaps, and a gap means nothing: loss is found by what was not
//! acknowledged, never by a missing number.
//!
//! The mark is one decimal line in its own file, replaced atomically (write,
//! sync, rename, sync the directory). A mark that cannot be read is refused by
//! name, never taken as zero: starting over would reissue numbers.

use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// How many numbers one durable write reserves.
pub const RESERVE: u64 = 1024;

/// The first number a writer with no mark yet issues.
const FIRST: u64 = 1;

/// Issues one writer's sequence numbers, each above every number it issued
/// before, in this run or any earlier one.
#[derive(Debug)]
pub struct Sequence {
    mark: PathBuf,
    next: u64,
    reserved: u64,
}

impl Sequence {
    /// The sequence whose high-water mark is the file `mark`: it continues at
    /// the recorded mark, or at 1 when there is none yet.
    pub fn open(mark: impl Into<PathBuf>) -> Result<Self, SeqError> {
        let mark = mark.into();
        let next = match fs::read_to_string(&mark) {
            Ok(text) => text
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|&n| n >= FIRST)
                .ok_or_else(|| SeqError::Unreadable { mark: mark.clone(), why: format!("not a sequence number: {text:?}") })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => FIRST,
            Err(e) => return Err(SeqError::Unreadable { mark, why: e.to_string() }),
        };
        Ok(Sequence { mark, next, reserved: next })
    }

    /// The next number, reserving more first when the reservation is spent.
    pub fn issue(&mut self) -> Result<u64, SeqError> {
        if self.next >= self.reserved {
            let reserved = self.next.checked_add(RESERVE).ok_or(SeqError::Exhausted)?;
            write_mark(&self.mark, reserved).map_err(|e| SeqError::Unwritable { mark: self.mark.clone(), why: e.to_string() })?;
            self.reserved = reserved;
        }
        let issued = self.next;
        self.next += 1;
        Ok(issued)
    }
}

/// Replace the mark atomically and durably.
fn write_mark(mark: &Path, value: u64) -> io::Result<()> {
    let tmp = mark.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    writeln!(file, "{value}")?;
    file.sync_all()?;
    fs::rename(&tmp, mark)?;
    if let Some(dir) = mark.parent().filter(|d| !d.as_os_str().is_empty()) {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Why a sequence cannot go on.
#[derive(Debug)]
pub enum SeqError {
    /// The mark exists and cannot be read as a number: starting over would reissue numbers.
    Unreadable { mark: PathBuf, why: String },
    /// The reservation could not be made durable, so nothing is issued.
    Unwritable { mark: PathBuf, why: String },
    /// Every 64-bit number has been reserved.
    Exhausted,
}

impl fmt::Display for SeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeqError::Unreadable { mark, why } => write!(f, "the sequence mark {} cannot be read ({why}); refusing to start over", mark.display()),
            SeqError::Unwritable { mark, why } => write!(f, "the sequence mark {} could not be made durable ({why}); nothing issued", mark.display()),
            SeqError::Exhausted => write!(f, "the sequence is exhausted"),
        }
    }
}

impl std::error::Error for SeqError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark_in(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().join("timeline.seq")
    }

    #[test]
    fn numbers_rise_and_start_at_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut seq = Sequence::open(mark_in(&dir)).unwrap();
        let issued: Vec<u64> = (0..3000).map(|_| seq.issue().unwrap()).collect();
        assert_eq!(issued[0], 1);
        assert!(issued.windows(2).all(|w| w[1] > w[0]), "strictly rising, across reservations");
    }

    #[test]
    fn a_number_is_durable_ahead_of_its_issue() {
        let dir = tempfile::tempdir().unwrap();
        let mut seq = Sequence::open(mark_in(&dir)).unwrap();
        for _ in 0..2500 {
            let issued = seq.issue().unwrap();
            let mark: u64 = fs::read_to_string(mark_in(&dir)).unwrap().trim().parse().unwrap();
            assert!(mark > issued, "the mark ({mark}) is above every number issued ({issued}) when it is issued");
        }
    }

    #[test]
    fn a_writer_that_dies_mid_reservation_restarts_above_everything_it_issued() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = Sequence::open(mark_in(&dir)).unwrap();
        let before: Vec<u64> = (0..1500).map(|_| first.issue().unwrap()).collect();
        // Dropped with no close: a crash, mid-reservation.
        drop(first);
        let mut again = Sequence::open(mark_in(&dir)).unwrap();
        let after = again.issue().unwrap();
        assert!(after > *before.iter().max().unwrap(), "{after} is above every earlier number");
    }

    #[test]
    fn an_unreadable_mark_is_refused_never_taken_as_zero() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(mark_in(&dir), "garbage\n").unwrap();
        let refused = Sequence::open(mark_in(&dir)).unwrap_err();
        assert!(matches!(refused, SeqError::Unreadable { .. }), "{refused}");
        assert!(refused.to_string().contains("refusing to start over"), "{refused}");
    }
}
