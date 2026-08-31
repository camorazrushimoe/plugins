//! CHECKPOINT (§3.1): stores the last flushed stream id, written atomically
//! (`CHECKPOINT.tmp` + rename + fsync). Never moved backward, never advanced
//! for un-flushed data.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::Error;

pub const CHECKPOINT_FILE: &str = "CHECKPOINT";

/// Read the checkpoint (trimmed). Missing file → `None`.
pub fn read(data_dir: &Path) -> Result<Option<String>, Error> {
    let p = data_dir.join(CHECKPOINT_FILE);
    if !p.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&p)?;
    let id = text.trim();
    if id.is_empty() {
        Ok(None)
    } else {
        Ok(Some(id.to_string()))
    }
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Atomically write the checkpoint: tmp + fsync + rename + fsync dir.
pub fn write(data_dir: &Path, id: &str) -> Result<(), Error> {
    let tmp = data_dir.join(format!("{CHECKPOINT_FILE}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        set_0600(&f)?;
        f.write_all(id.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, data_dir.join(CHECKPOINT_FILE))?;
    fsync_dir(data_dir)?;
    Ok(())
}

fn set_0600(f: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
}

/// Parse a Redis stream id into a sortable `(ms, seq)` tuple.
pub fn parse_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// Dedupe rule (§3.1): an entry is a duplicate when its stream_id is `<=` the
/// last flushed checkpoint. Stream ids are monotonic per stream.
pub fn is_duplicate(entry_id: &str, checkpoint: Option<&str>) -> bool {
    match checkpoint {
        None => false,
        Some(cp) => match (parse_id(entry_id), parse_id(cp)) {
            (Some(a), Some(b)) => a <= b,
            // Unparsable ids never skip — be conservative and keep the event.
            _ => false,
        },
    }
}

/// Increment a stream id by one sequence step ("<ms>-<seq>" → "<ms>-<seq+1>").
pub fn next_id(id: &str) -> String {
    match parse_id(id) {
        Some((ms, seq)) => format!("{ms}-{}", seq + 1),
        None => id.to_string(),
    }
}

/// §3.1 resume point: never earlier than the durable CHECKPOINT, but at least
/// the highest stream id already written to JSONL. A crash between appending
/// a batch and writing CHECKPOINT leaves rows on disk whose ids the CHECKPOINT
/// file has not caught up to; resuming from `max(durable, written)` makes the
/// at-least-once re-read duplicate-free (§3.1: "cannot duplicate rows").
pub fn resume_start(durable: &str, max_written: Option<&str>) -> String {
    match max_written {
        Some(mw) => {
            // The fresh-start sentinel "0" (bare ms, no sequence) means the
            // very beginning of the stream — equivalent to (0, 0).
            let durable_parsed = parse_id(durable).or_else(|| (durable == "0").then_some((0, 0)));
            match (durable_parsed, parse_id(mw)) {
                (Some(d), Some(m)) if m > d => mw.to_string(),
                _ => durable.to_string(),
            }
        }
        None => durable.to_string(),
    }
}

/// The path of the checkpoint file (exposed for tests).
pub fn path(data_dir: &Path) -> PathBuf {
    data_dir.join(CHECKPOINT_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-cp-{tag}-{}-{}",
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
    fn read_missing_is_none() {
        let d = tmpdir("missing");
        assert_eq!(read(&d).unwrap(), None);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn write_then_read_roundtrip_atomic() {
        let d = tmpdir("rt");
        write(&d, "1725062400000-42").unwrap();
        assert_eq!(read(&d).unwrap().as_deref(), Some("1725062400000-42"));
        // no tmp left behind
        assert!(!d.join("CHECKPOINT.tmp").exists());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn checkpoint_file_is_0600() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("perm");
        write(&d, "1-0").unwrap();
        let mode = std::fs::metadata(d.join(CHECKPOINT_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn dedupe_boundary_is_inclusive_at_or_before() {
        assert!(!is_duplicate("1725062400000-5", None));
        assert!(is_duplicate("1725062400000-5", Some("1725062400000-5")));
        assert!(is_duplicate("1725062400000-5", Some("1725062400000-9")));
        assert!(!is_duplicate("1725062400000-5", Some("1725062400000-4")));
        // sequence ordering
        assert!(!is_duplicate("1725062400000-1", Some("1725062400000-0")));
        assert!(is_duplicate("1725062400000-0", Some("1725062400000-0")));
    }

    #[test]
    fn next_id_increments_sequence() {
        assert_eq!(next_id("1725062400000-0"), "1725062400000-1");
        assert_eq!(next_id("1725062400000-42"), "1725062400000-43");
    }

    // --- §3.1 resume point ------------------------------------------------------

    #[test]
    fn resume_start_prefers_max_written_when_ahead() {
        assert_eq!(resume_start("0", Some("5-0")), "5-0");
        assert_eq!(
            resume_start("1725062400000-2", Some("1725062400000-4")),
            "1725062400000-4"
        );
    }

    #[test]
    fn resume_start_never_rewinds_below_durable() {
        assert_eq!(resume_start("5-0", Some("4-0")), "5-0");
        assert_eq!(resume_start("5-0", Some("5-0")), "5-0");
        assert_eq!(resume_start("5-0", None), "5-0");
        assert_eq!(resume_start("0", None), "0");
        assert_eq!(
            resume_start("0", Some("not-an-id")),
            "0",
            "unparsable written id falls back"
        );
    }
}
