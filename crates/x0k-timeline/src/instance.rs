//! One service per state directory, held by an advisory lock on a file.
//!
//! The kernel owns the lock, so a crashed holder leaves nothing to clean up:
//! the lock goes with the process. Rust opens files close-on-exec, so a child
//! the service starts cannot keep it alive. The file's contents are only a
//! note of who holds it, which a refusal quotes.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How often a waiter retries while the holder drains.
const RETRY: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub enum InstanceLockError {
    #[error("cannot open lock file {path}: {source}")]
    Open {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot take lock file {path}: {source}")]
    Lock {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} is held by {holder}")]
    Held { path: PathBuf, holder: String },
}

/// Held for the life of the service.
#[derive(Debug)]
pub struct InstanceLock {
    _file: File,
}

impl InstanceLock {
    /// Take the lock at `path`, waiting up to `wait` for a holder that is
    /// draining, and creating the file and its directory if they are missing.
    pub fn acquire(path: &Path, wait: Duration) -> Result<Self, InstanceLockError> {
        let open_error = |source| InstanceLockError::Open {
            path: path.to_path_buf(),
            source,
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(open_error)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(open_error)?;
        let deadline = Instant::now() + wait;
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(RETRY);
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(InstanceLockError::Held {
                        path: path.to_path_buf(),
                        holder: holder_note(&mut file),
                    });
                }
                Err(TryLockError::Error(source)) => {
                    return Err(InstanceLockError::Lock {
                        path: path.to_path_buf(),
                        source,
                    });
                }
            }
        }
        // A note the service could not write costs a refusal its detail, not
        // the service its start.
        if let Err(e) = write_holder_note(&mut file) {
            tracing::warn!(path = %path.display(), error = %e, "could not note the lock's holder");
        }
        Ok(Self { _file: file })
    }
}

fn holder_note(file: &mut File) -> String {
    let mut note = String::new();
    let read = file
        .seek(SeekFrom::Start(0))
        .and_then(|_| file.read_to_string(&mut note));
    match read {
        Ok(_) if !note.trim().is_empty() => note.trim().to_string(),
        _ => "an unnamed process".to_string(),
    }
}

fn write_holder_note(file: &mut File) -> std::io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    write!(file, "pid {}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_holder_is_refused_with_the_first_named() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state/service.lock");
        let _first = InstanceLock::acquire(&path, Duration::ZERO).unwrap();
        let refused = InstanceLock::acquire(&path, Duration::ZERO).unwrap_err();
        let expected = format!("pid {}", std::process::id());
        assert!(
            matches!(&refused, InstanceLockError::Held { holder, .. } if *holder == expected),
            "{refused}"
        );
    }

    #[test]
    fn dropping_the_lock_lets_the_next_holder_in() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.lock");
        drop(InstanceLock::acquire(&path, Duration::ZERO).unwrap());
        InstanceLock::acquire(&path, Duration::ZERO).unwrap();
    }

    #[test]
    fn a_waiter_gets_the_lock_when_the_holder_drains() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.lock");
        let first = InstanceLock::acquire(&path, Duration::ZERO).unwrap();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(first);
        });
        InstanceLock::acquire(&path, Duration::from_secs(5)).unwrap();
        releaser.join().unwrap();
    }
}
