//! Single-writer lock (§3.3): `$data_dir/.lock` holds pid + started_at
//! (process starttime in clock ticks, from `/proc/<pid>/stat`).
//!
//! A lock is **stale** — and taken over — when the pid is not running, or
//! `/proc/<pid>/stat` starttime does not match the recorded `started_at` (an
//! OS-recycled pid is not the collector). Exit code 3 only when pid **and**
//! identity both match a live process.

use std::path::{Path, PathBuf};

use crate::Error;

pub const LOCK_FILE: &str = ".lock";

#[derive(Debug)]
pub struct LockGuard {
    path: PathBuf,
}

/// Read this process's starttime (field 22) from /proc/self/stat.
fn self_starttime() -> Result<u64, Error> {
    let stat = std::fs::read_to_string("/proc/self/stat")
        .map_err(|e| Error::Fatal(format!("cannot read /proc/self/stat: {e}")))?;
    starttime_of(&stat).ok_or_else(|| Error::Fatal("cannot parse /proc/self/stat starttime".into()))
}

/// Extract starttime (field 22) from a `/proc/<pid>/stat` string.
fn starttime_of(stat: &str) -> Option<u64> {
    // comm is field 2 and may contain spaces; split after the last ')'.
    let rest = stat.rsplit_once(')')?.1;
    // `rest` starts at field 3; starttime is field 22 → index 22 - 3 = 19.
    rest.split_whitespace().nth(19)?.parse().ok()
}

fn pid_running(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Try to read the live starttime of a pid (None if not running).
fn pid_starttime(pid: u32) -> Option<u64> {
    if !pid_running(pid) {
        return None;
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    starttime_of(&stat)
}

/// Acquire the data_dir lock. Returns `Err(Error::LockConflict)` when a live
/// process (matching pid AND identity) holds it — the only exit-3 path.
pub fn acquire(data_dir: &Path) -> Result<LockGuard, Error> {
    let path = data_dir.join(LOCK_FILE);

    if path.exists() {
        let text = std::fs::read_to_string(&path)
            .map_err(|e| Error::Fatal(format!("lock {} unreadable: {e}", path.display())))?;
        let mut it = text.split_whitespace();
        let pid: Option<u32> = it.next().and_then(|p| p.parse().ok());
        let recorded: Option<u64> = it.next().and_then(|p| p.parse().ok());

        if let (Some(pid), Some(recorded)) = (pid, recorded) {
            if let Some(live_starttime) = pid_starttime(pid) {
                if live_starttime == recorded {
                    // pid AND identity both match a live process → exit 3.
                    return Err(Error::LockConflict);
                }
                // pid recycled (identity differs) → stale, take over.
            }
            // pid not running → stale, take over.
        }
        // Unparsable lock file → stale, take over.
    }

    // Take over / create the lock.
    let pid = std::process::id();
    let started_at = self_starttime()?;
    let content = format!("{pid} {started_at}\n");
    let tmp = path.with_extension("lock.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        set_0600(&f)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    // fsync the dir so the lock survives a crash.
    if let Ok(d) = std::fs::File::open(data_dir) {
        let _ = d.sync_all();
    }

    Ok(LockGuard { path })
}

fn set_0600(f: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-lock-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn acquire_twice_in_same_process_conflicts() {
        let d = tmpdir("conflict");
        let _g1 = acquire(&d).unwrap();
        let err = acquire(&d).unwrap_err();
        assert!(matches!(err, Error::LockConflict));
        drop(_g1);
        let _g2 = acquire(&d).unwrap(); // released → acquirable again
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn stale_lock_with_dead_pid_is_taken_over() {
        let d = tmpdir("stale");
        // pid 99999999 is almost certainly not running
        std::fs::write(d.join(LOCK_FILE), "99999999 12345\n").unwrap();
        let g = acquire(&d).unwrap();
        assert!(g.path.exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn stale_lock_with_recycled_pid_is_taken_over() {
        let d = tmpdir("recycled");
        // our own pid, but a starttime that cannot match → identity mismatch
        std::fs::write(d.join(LOCK_FILE), format!("{} 1\n", std::process::id())).unwrap();
        let g = acquire(&d).unwrap();
        assert!(g.path.exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn unparsable_lock_is_stale() {
        let d = tmpdir("garbage");
        std::fs::write(d.join(LOCK_FILE), "not-a-lock\n").unwrap();
        let _g = acquire(&d).unwrap();
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn lock_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("perm");
        let _g = acquire(&d).unwrap();
        let mode = std::fs::metadata(d.join(LOCK_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&d);
    }
}
