//! A process's local timeline segment (`observability-timeline` §4).
//!
//! While a process cannot reach the local writer, it appends its
//! `TimelineRecord`s to its own segment, `{dir}/{key}.queue`: a header frame
//! naming the format, then `[u32 LE length][postcard TimelineRecord]` frames.
//! Only `postcard` is needed to read one, so this module is in the crate's base
//! build. A segment with no header is the same format, from before the header.
//!
//! A segment's owner holds an exclusive lock on `{dir}/{key}.lock` for its
//! whole life, taken before its queue file exists. The lock, not a pid, says
//! whether the owner is alive: sandboxed processes run in their own pid
//! namespace but write into the host's segment directory, so a pid seen from
//! the writer can name another process or none. A lock is held on the inode
//! and is released by the kernel when the owner exits, however it exits.
//! `{key}` is the owner's pid and a random nonce, unique across namespaces.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Read;
use std::ops::Range;
use std::path::{Path, PathBuf};

use tracing::warn;
use x0k_timeline_record::record::TimelineRecord;

/// The segment directory's name under a profile directory.
pub const SEGMENT_DIR_NAME: &str = "timeline-crash-queue";

/// The segment directory of the profile at `profile_dir`.
pub fn segment_dir(profile_dir: &Path) -> PathBuf {
    profile_dir.join(SEGMENT_DIR_NAME)
}

/// A fresh owner key: this process's pid and a random nonce.
pub fn new_owner_key() -> String {
    format!("{}-{:016x}", std::process::id(), rand_nonce())
}

fn rand_nonce() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    // `RandomState` is seeded from the OS per process; mixing in the time keeps
    // two keys minted by one process distinct.
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    );
    hasher.finish()
}

/// The files of the segment owned by `key` in `dir`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPaths {
    /// The record frames.
    pub queue: PathBuf,
    /// Frames the writer refused on their own merits, kept for inspection.
    pub quarantine: PathBuf,
    /// The owner's lock.
    pub lock: PathBuf,
}

impl SegmentPaths {
    pub fn new(dir: &Path, key: &str) -> Self {
        Self {
            queue: dir.join(format!("{key}.queue")),
            quarantine: dir.join(format!("{key}.quarantine")),
            lock: dir.join(format!("{key}.lock")),
        }
    }
}

/// Take the owner lock at `lock_path`, creating it. Hold the returned file for
/// as long as the segment may be written; the lock is released when it drops.
pub fn hold_owner_lock(lock_path: &Path) -> std::io::Result<File> {
    if let Some(dir) = lock_path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lock_path)?;
    file.lock()?;
    Ok(file)
}

/// Whether a segment's owner is gone: `Some(lock)` when the owner no longer
/// holds `lock_path`, with the lock now held by the caller until it drops;
/// `None` while the owner holds it.
pub fn claim_if_owner_gone(lock_path: &Path) -> std::io::Result<Option<File>> {
    let file = OpenOptions::new().write(true).open(lock_path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// The owner key of a segment queue file: its name without `.queue`. `None` for
/// any other file in the directory, panic queues included (their owner sweeps
/// them through its forwarder).
pub fn queue_owner_key(path: &Path) -> Option<&str> {
    let name = path.file_name()?.to_str()?;
    let key = name.strip_suffix(".queue")?;
    (!key.ends_with("-panic")).then_some(key)
}

/// What a header frame's body starts with. No record's encoding starts this way.
const HEADER_TAG: &[u8] = b"x0k.timeline.segment ";

/// The format this build writes and reads.
pub const SEGMENT_FORMAT: &str = "postcard-1";

/// A segment in a format this build cannot read. The file is left where it is.
#[derive(Debug, thiserror::Error)]
#[error("segment format {0} is not one this build reads")]
pub struct UnknownSegmentFormat(pub String);

/// The frame a segment starts with, naming `format`.
pub fn header_frame_naming(format: &str) -> Vec<u8> {
    let body = [HEADER_TAG, format.as_bytes()].concat();
    let mut frame = (body.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// The frame a segment of this build starts with.
pub fn header_frame() -> Vec<u8> {
    header_frame_naming(SEGMENT_FORMAT)
}

/// The format `buf`'s header names, if it starts with one.
fn named_format(buf: &[u8]) -> Option<String> {
    let body = buf.get(4..)?.strip_prefix(HEADER_TAG)?;
    let len = u32::from_le_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let name = body.get(..len.checked_sub(HEADER_TAG.len())?)?;
    Some(String::from_utf8_lossy(name).into_owned())
}

/// Encode one record as a complete length-prefixed frame, ready for one write.
///
/// An `Activity` carrying `lazy_data` must have it resolved first: the field is
/// `#[serde(skip)]`.
pub fn encode_frame(record: &TimelineRecord) -> anyhow::Result<Vec<u8>> {
    let body = x0k_serialization::serialize::to_bytes_unchecked(record)
        .map_err(|e| anyhow::anyhow!("postcard encode failed: {e}"))?;
    let len = u32::try_from(body.len())
        .map_err(|_| anyhow::anyhow!("timeline record too large for a segment frame"))?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Locate the intact record frames in `buf`: the body range of each, and the
/// offset where the intact prefix ends. Bytes past that offset are a torn tail — a
/// frame whose write never completed. A header frame is not a record.
pub fn index_frames(buf: &[u8]) -> (Vec<Range<usize>>, usize) {
    let mut frames = Vec::new();
    let mut offset = named_format(buf).map_or(0, |format| {
        4 + HEADER_TAG.len() + format.len()
    });
    while offset + 4 <= buf.len() {
        let len = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        let body = offset + 4;
        if body + len > buf.len() {
            break;
        }
        frames.push(body..body + len);
        offset = body + len;
    }
    (frames, offset)
}

/// Decode one frame body.
pub fn decode_frame(body: &[u8]) -> anyhow::Result<TimelineRecord> {
    x0k_serialization::serialize::from_bytes_unchecked::<TimelineRecord>(body)
        .map_err(|e| anyhow::anyhow!("postcard decode failed: {e}"))
}

/// Every record in the segment file at `path`, in order. A torn trailing frame
/// is dropped and a frame that does not decode is skipped, so a crash mid-write
/// loses only that frame. A missing file reads as empty.
pub fn read_segment(path: &Path) -> anyhow::Result<Vec<TimelineRecord>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if let Some(format) = named_format(&buf).filter(|format| format != SEGMENT_FORMAT) {
        return Err(UnknownSegmentFormat(format).into());
    }
    let (frames, intact_end) = index_frames(&buf);
    let mut records = Vec::with_capacity(frames.len());
    for body in frames {
        match decode_frame(&buf[body]) {
            Ok(record) => records.push(record),
            Err(e) => warn!(file = %path.display(), error = %e, "skipping corrupt segment frame"),
        }
    }
    if intact_end < buf.len() {
        warn!(
            file = %path.display(),
            torn_bytes = buf.len() - intact_end,
            "segment has a torn trailing frame; dropped"
        );
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use x0k_timeline_record::record::LogEventData;

    fn log(id: &str) -> TimelineRecord {
        TimelineRecord::LogEvent(LogEventData {
            id: id.into(),
            ..Default::default()
        })
    }

    #[test]
    fn a_segment_reads_back_its_frames_and_drops_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1-0.queue");
        let mut bytes = encode_frame(&log("a")).unwrap();
        bytes.extend(encode_frame(&log("b")).unwrap());
        let torn = encode_frame(&log("c")).unwrap();
        bytes.extend(&torn[..torn.len() - 1]);
        std::fs::write(&path, bytes).unwrap();

        let ids: Vec<String> = read_segment(&path)
            .unwrap()
            .into_iter()
            .map(|r| match r {
                TimelineRecord::LogEvent(l) => l.id,
                other => panic!("unexpected record {other:?}"),
            })
            .collect();
        assert_eq!(ids, ["a", "b"]);
    }

    #[test]
    fn a_header_is_not_a_record_and_a_segment_without_one_still_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1-0.queue");
        let frame = encode_frame(&log("a")).unwrap();

        std::fs::write(&path, [header_frame(), frame.clone()].concat()).unwrap();
        assert_eq!(read_segment(&path).unwrap().len(), 1);

        std::fs::write(&path, &frame).unwrap();
        assert_eq!(read_segment(&path).unwrap().len(), 1);
    }

    #[test]
    fn a_format_this_build_cannot_read_is_refused_by_name_and_left_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1-0.queue");
        let frame = encode_frame(&log("a")).unwrap();
        std::fs::write(&path, [header_frame_naming("cbor-9"), frame].concat()).unwrap();

        let refused = read_segment(&path).unwrap_err();
        let format = refused.downcast_ref::<UnknownSegmentFormat>().expect("a named refusal");
        assert_eq!(format.0, "cbor-9");
        assert!(path.exists());
    }

    #[test]
    fn a_torn_header_reads_as_nothing() {
        let header = header_frame();
        let (frames, _) = index_frames(&header[..header.len() - 3]);
        assert!(frames.is_empty());
    }

    #[test]
    fn a_held_owner_lock_cannot_be_claimed_and_a_released_one_can() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SegmentPaths::new(dir.path(), &new_owner_key());
        let held = hold_owner_lock(&paths.lock).unwrap();
        assert!(claim_if_owner_gone(&paths.lock).unwrap().is_none());
        drop(held);
        assert!(claim_if_owner_gone(&paths.lock).unwrap().is_some());
    }

    #[test]
    fn only_owned_queue_files_name_an_owner() {
        assert_eq!(queue_owner_key(Path::new("/d/7-ab.queue")), Some("7-ab"));
        assert_eq!(queue_owner_key(Path::new("/d/7-panic.queue")), None);
        assert_eq!(queue_owner_key(Path::new("/d/7-ab.queue.compact")), None);
        assert_eq!(queue_owner_key(Path::new("/d/7-ab.lock")), None);
    }
}
