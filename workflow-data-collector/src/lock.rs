//! Single-writer lock (§3.3).
//!
//! `$data_dir/.lock` holds `pid` + `started_at` (the kernel starttime, field
//! 22 of `/proc/<pid>/stat`). A lock is **stale** — and taken over — when the
//! pid is not running, or `/proc/<pid>/stat` starttime does not match the
//! recorded value (an OS-recycled pid is not the collector). The collector
//! exits 3 only when pid **and** identity both match a live process.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const LOCK_FILENAME: &str = ".lock";

#[derive(Debug)]
pub enum LockError {
    /// A live collector owns this data_dir → exit 3.
    Busy,
    Io(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Busy => write!(
                f,
                "another wfdc instance is already running on this data_dir"
            ),
            LockError::Io(m) => write!(f, "io error: {m}"),
        }
    }
}

impl std::error::Error for LockError {}

#[derive(Debug, Serialize, Deserialize)]
struct LockEntry {
    pid: u32,
    started_at: u64,
}

/// Acquire the single-writer lock, taking over a stale lock. The returned
/// guard removes the lock file on drop (clean exit).
pub fn acquire(data_dir: &Path) -> Result<LockGuard, LockError> {
    let path = data_dir.join(LOCK_FILENAME);
    if path.exists() {
        match read_entry(&path) {
            Some(entry) => {
                if process_is_live(entry.pid, entry.started_at) {
                    return Err(LockError::Busy);
                }
                log::warn!(
                    "stale lock at {} (pid {} not a live collector) — taking over",
                    path.display(),
                    entry.pid
                );
            }
            None => {
                log::warn!("corrupt lock at {} — taking over", path.display());
            }
        }
    }
    let pid = std::process::id();
    let started_at = get_starttime(pid).unwrap_or(0);
    let entry = LockEntry { pid, started_at };
    let json =
        serde_json::to_vec(&entry).map_err(|e| LockError::Io(format!("serialize lock: {e}")))?;
    write_private(&path, &json)?;
    Ok(LockGuard { path })
}

/// Guard that removes the lock file when dropped (clean-stop path).
#[derive(Debug)]
pub struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn read_entry(path: &Path) -> Option<LockEntry> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str::<LockEntry>(&text).ok()
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), LockError> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| LockError::Io(format!("create {}: {e}", path.display())))?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| LockError::Io(format!("chmod {}: {e}", path.display())))?;
    f.write_all(bytes)
        .map_err(|e| LockError::Io(format!("write {}: {e}", path.display())))?;
    f.sync_all()
        .map_err(|e| LockError::Io(format!("fsync {}: {e}", path.display())))
}

/// Kernel starttime (clock ticks since boot) of `pid` — field 22 of
/// `/proc/<pid>/stat`. `None` when the pid is not running (or unreadable).
pub fn get_starttime(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm (field 2) may contain spaces/parens — split after the last ')'.
    let after_comm = stat.rsplit_once(')')?.1;
    let field22 = after_comm.split_whitespace().nth(19)?; // fields 3..22 → index 19
    field22.parse::<u64>().ok()
}

/// pid **and** identity (starttime) both match a live process (§3.3).
pub fn process_is_live(pid: u32, started_at: u64) -> bool {
    get_starttime(pid) == Some(started_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn write_entry(path: &Path, pid: u32, started_at: u64) {
        let e = LockEntry { pid, started_at };
        std::fs::write(path, serde_json::to_vec(&e).unwrap()).unwrap();
    }

    #[test]
    fn acquire_creates_lock_with_our_pid_and_0600() {
        let dir = tempfile::tempdir().unwrap();
        let guard = acquire(dir.path()).unwrap();
        let text = std::fs::read_to_string(dir.path().join(LOCK_FILENAME)).unwrap();
        let entry: LockEntry = serde_json::from_str(&text).unwrap();
        assert_eq!(entry.pid, std::process::id());
        assert!(entry.started_at > 0);
        let mode = std::fs::metadata(dir.path().join(LOCK_FILENAME))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, ".lock must be 0600");
        drop(guard);
        assert!(
            !dir.path().join(LOCK_FILENAME).exists(),
            "guard drop removes the lock"
        );
    }

    #[test]
    fn second_acquire_while_live_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let _g1 = acquire(dir.path()).unwrap();
        match acquire(dir.path()) {
            Err(LockError::Busy) => {}
            other => panic!("expected Busy, got {other:?}"),
        }
    }

    #[test]
    fn acquire_after_drop_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        {
            let _g1 = acquire(dir.path()).unwrap();
        }
        let _g2 = acquire(dir.path()).unwrap();
    }

    #[test]
    fn dead_pid_is_stale_and_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        // spawn a process that exits immediately; its pid is dead afterwards
        let child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        drop(child);
        // wait until it is reaped
        for _ in 0..50 {
            if std::fs::read_to_string(format!("/proc/{dead_pid}/stat")).is_err() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        write_entry(&dir.path().join(LOCK_FILENAME), dead_pid, 12345);
        let _g = acquire(dir.path()).expect("dead pid lock is stale → takeover");
    }

    #[test]
    fn live_pid_with_wrong_starttime_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let our_pid = std::process::id();
        write_entry(
            &dir.path().join(LOCK_FILENAME),
            our_pid,
            get_starttime(our_pid).unwrap() + 1,
        );
        let _g = acquire(dir.path()).expect("identity mismatch → stale → takeover");
    }

    #[test]
    fn live_pid_with_matching_starttime_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let our_pid = std::process::id();
        write_entry(
            &dir.path().join(LOCK_FILENAME),
            our_pid,
            get_starttime(our_pid).unwrap(),
        );
        assert!(matches!(acquire(dir.path()), Err(LockError::Busy)));
    }

    #[test]
    fn corrupt_lock_is_taken_over() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LOCK_FILENAME), "not json at all").unwrap();
        let _g = acquire(dir.path()).expect("corrupt lock → takeover");
    }

    #[test]
    fn starttime_of_self_is_positive_and_stable() {
        let a = get_starttime(std::process::id()).unwrap();
        let b = get_starttime(std::process::id()).unwrap();
        assert!(a > 0);
        assert_eq!(a, b);
    }

    #[test]
    fn starttime_of_missing_pid_is_none() {
        assert_eq!(get_starttime(u32::MAX), None);
    }
}
