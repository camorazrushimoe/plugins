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

/// Acquire the single-writer lock (§3.3).
///
/// Acquisition is **atomic** (`create_new`/O_EXCL): two processes racing to
/// take over a stale lock cannot both win — one `create_new` succeeds, the
/// other sees `AlreadyExists`, re-reads the winner's entry and exits 3 when
/// it is live. A stale or corrupt lock is removed and the create retried.
/// The returned guard removes the lock on drop **only if it still holds our
/// pid + starttime**, so a loser can never delete the winner's lock.
pub fn acquire(data_dir: &Path) -> Result<LockGuard, LockError> {
    let path = data_dir.join(LOCK_FILENAME);
    let pid = std::process::id();
    let started_at = get_starttime(pid).unwrap_or(0);
    let entry = LockEntry { pid, started_at };
    let json =
        serde_json::to_vec(&entry).map_err(|e| LockError::Io(format!("serialize lock: {e}")))?;

    const MAX_RETRIES: u32 = 10;
    for _ in 0..MAX_RETRIES {
        match create_private(&path) {
            Ok(mut f) => {
                use std::io::Write;
                f.write_all(&json)
                    .map_err(|e| LockError::Io(format!("write {}: {e}", path.display())))?;
                f.sync_all()
                    .map_err(|e| LockError::Io(format!("fsync {}: {e}", path.display())))?;
                return Ok(LockGuard {
                    path,
                    pid,
                    started_at,
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => match read_entry(&path) {
                Some(existing) if process_is_live(existing.pid, existing.started_at) => {
                    return Err(LockError::Busy);
                }
                _ => {
                    log::warn!("stale/corrupt lock at {} — taking over", path.display());
                    std::fs::remove_file(&path)
                        .map_err(|e| LockError::Io(format!("remove {}: {e}", path.display())))?;
                }
            },
            Err(e) => return Err(LockError::Io(format!("create {}: {e}", path.display()))),
        }
    }
    Err(LockError::Io(format!(
        "could not acquire lock at {} after {MAX_RETRIES} attempts",
        path.display()
    )))
}

/// Guard that removes the lock file on drop **only when it is still ours**
/// (clean-stop path; a concurrent winner's lock is never touched).
#[derive(Debug)]
pub struct LockGuard {
    path: PathBuf,
    pid: u32,
    started_at: u64,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(entry) = read_entry(&self.path) {
            if entry.pid == self.pid && entry.started_at == self.started_at {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

fn read_entry(path: &Path) -> Option<LockEntry> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str::<LockEntry>(&text).ok()
}

/// Atomic exclusive create with mode 0600 regardless of umask.
fn create_private(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    Ok(f)
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
    fn drop_never_removes_someone_elses_lock() {
        let dir = tempfile::tempdir().unwrap();
        let guard = acquire(dir.path()).unwrap();
        // another process wins the lock while we are alive (e.g. after our
        // pid+starttime record was overwritten by a takeover race)
        let foreign = LockEntry {
            pid: std::process::id(),
            started_at: get_starttime(std::process::id()).unwrap() + 1,
        };
        std::fs::write(
            dir.path().join(LOCK_FILENAME),
            serde_json::to_vec(&foreign).unwrap(),
        )
        .unwrap();
        drop(guard);
        assert!(
            dir.path().join(LOCK_FILENAME).exists(),
            "a loser's drop must not delete the winner's lock"
        );
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
