//! CHECKPOINT (§3.1): stores the last flushed stream id.
//!
//! Written **atomically** (`CHECKPOINT.tmp` + rename) and fsynced (file and
//! parent directory) so a crash can never leave a torn checkpoint. The
//! checkpoint is only advanced after the batch's JSONL files are written and
//! fsynced; it never moves backward and never advances for un-flushed data.

use std::path::Path;

use crate::Error;

pub const CHECKPOINT_FILENAME: &str = "CHECKPOINT";
pub const CHECKPOINT_TMP_FILENAME: &str = "CHECKPOINT.tmp";

/// Read the last flushed stream id. A missing or empty file means "start from
/// the beginning of the stream" (`0`). Anything that is not a valid stream id
/// is a fatal IO error — silently restarting from `0` would duplicate rows.
pub fn read(data_dir: &Path) -> Result<String, Error> {
    let path = data_dir.join(CHECKPOINT_FILENAME);
    if !path.exists() {
        return Ok("0".to_string());
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| Error::Io(format!("cannot read {}: {e}", path.display())))?;
    let id = text.trim();
    if id.is_empty() {
        return Ok("0".to_string());
    }
    if !is_valid_stream_id(id) {
        return Err(Error::Io(format!(
            "corrupt CHECKPOINT at {}: {id:?} is not a stream id",
            path.display()
        )));
    }
    Ok(id.to_string())
}

/// A stream id is `<ms>-<seq>` (both u64) or the special `0`.
pub fn is_valid_stream_id(id: &str) -> bool {
    crate::streamid::is_valid(id)
}

/// Atomically persist the last flushed stream id: write `CHECKPOINT.tmp`,
/// fsync it, rename over `CHECKPOINT`, fsync the directory.
pub fn write(data_dir: &Path, id: &str) -> Result<(), Error> {
    if !is_valid_stream_id(id) {
        return Err(Error::Io(format!(
            "refusing to checkpoint invalid stream id {id:?}"
        )));
    }
    use std::io::Write;
    let tmp = data_dir.join(CHECKPOINT_TMP_FILENAME);
    let final_path = data_dir.join(CHECKPOINT_FILENAME);

    let result = (|| -> Result<(), Error> {
        let mut f = crate::fsutil::open_private(&tmp, false)?;
        f.write_all(id.as_bytes())
            .map_err(|e| Error::Io(format!("write {}: {e}", tmp.display())))?;
        f.write_all(b"\n")
            .map_err(|e| Error::Io(format!("write {}: {e}", tmp.display())))?;
        f.sync_all()
            .map_err(|e| Error::Io(format!("fsync {}: {e}", tmp.display())))?;
        drop(f);
        std::fs::rename(&tmp, &final_path).map_err(|e| {
            Error::Io(format!(
                "rename {} → {}: {e}",
                tmp.display(),
                final_path.display()
            ))
        })?;
        fsync_dir(data_dir)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// fsync a directory so a completed rename is durable.
fn fsync_dir(dir: &Path) -> Result<(), Error> {
    let d = std::fs::File::open(dir)
        .map_err(|e| Error::Io(format!("open dir {}: {e}", dir.display())))?;
    d.sync_all()
        .map_err(|e| Error::Io(format!("fsync dir {}: {e}", dir.display())))
}

/// §3.1 resume point: never earlier than the durable CHECKPOINT, but at least
/// the highest stream id already written to JSONL. A crash between appending
/// a batch and writing CHECKPOINT leaves rows on disk whose ids the CHECKPOINT
/// file has not caught up to; resuming from `max(durable, written)` makes the
/// at-least-once re-read duplicate-free (§3.1: "cannot duplicate rows").
pub fn resume_start(durable: &str, max_written: Option<&str>) -> String {
    match max_written {
        Some(mw) => match (
            crate::streamid::StreamId::parse(durable),
            crate::streamid::StreamId::parse(mw),
        ) {
            (Some(d), Some(m)) if m > d => mw.to_string(),
            _ => durable.to_string(),
        },
        None => durable.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn missing_checkpoint_reads_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(dir.path()).unwrap(), "0");
    }

    #[test]
    fn write_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "1725062400000-0").unwrap();
        assert_eq!(read(dir.path()).unwrap(), "1725062400000-0");
    }

    #[test]
    fn empty_file_reads_zero() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CHECKPOINT_FILENAME), "").unwrap();
        assert_eq!(read(dir.path()).unwrap(), "0");
    }

    #[test]
    fn garbage_checkpoint_is_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CHECKPOINT_FILENAME), "not-an-id\n").unwrap();
        assert!(read(dir.path()).is_err());
        std::fs::write(dir.path().join(CHECKPOINT_FILENAME), "123\n").unwrap();
        assert!(read(dir.path()).is_err(), "bare ms without seq is invalid");
    }

    #[test]
    fn atomic_write_leaves_no_tmp_and_sets_0600() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "1725062400000-5").unwrap();
        assert!(!dir.path().join(CHECKPOINT_TMP_FILENAME).exists());
        let mode = std::fs::metadata(dir.path().join(CHECKPOINT_FILENAME))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "CHECKPOINT must be 0600");
        let content = std::fs::read_to_string(dir.path().join(CHECKPOINT_FILENAME)).unwrap();
        assert_eq!(content, "1725062400000-5\n");
    }

    #[test]
    fn overwrite_advances_forward() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "1-0").unwrap();
        write(dir.path(), "2-0").unwrap();
        assert_eq!(read(dir.path()).unwrap(), "2-0");
    }

    #[test]
    fn stale_tmp_from_crash_is_ignored_and_replaced() {
        let dir = tempfile::tempdir().unwrap();
        // crash artifact: tmp with new content, CHECKPOINT with old content
        std::fs::write(dir.path().join(CHECKPOINT_TMP_FILENAME), "999-0\n").unwrap();
        write(dir.path(), "5-0").unwrap();
        assert_eq!(read(dir.path()).unwrap(), "5-0");
        assert!(!dir.path().join(CHECKPOINT_TMP_FILENAME).exists());
    }

    #[test]
    fn refuses_invalid_id() {
        let dir = tempfile::tempdir().unwrap();
        assert!(write(dir.path(), "garbage").is_err());
        assert!(!dir.path().join(CHECKPOINT_FILENAME).exists());
    }

    #[test]
    fn valid_ids() {
        assert!(is_valid_stream_id("0"));
        assert!(is_valid_stream_id("1725062400000-0"));
        assert!(is_valid_stream_id("1-42"));
        assert!(!is_valid_stream_id(""));
        assert!(!is_valid_stream_id("1725062400000"));
        assert!(!is_valid_stream_id("abc-0"));
        assert!(!is_valid_stream_id("1-2-3"));
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
