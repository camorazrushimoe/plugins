//! Raw dataset on disk (§5.2) + startup repair (§3.2) + permission
//! enforcement (§2).
//!
//! Layout: `raw/dt=YYYY-MM-DD/events.jsonl` (office-wide) and
//! `teams/<team_safe>/raw/dt=YYYY-MM-DD/events.jsonl` (split by team). Files
//! are created 0600, directories 0700. One JSON object per line, append-only
//! within a partition. Each batch is flushed with a single fsync per file.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::decode::RawLine;
use crate::Error;

const RAW_DIR: &str = "raw";
const TEAMS_DIR: &str = "teams";

/// One decoded event to persist, already partitioned.
pub struct WriteEntry {
    pub line: RawLine,
    pub team_safe: String,
    pub dt: String,
}

/// A startup repair (§3.2): a partial line was truncated at EOF.
#[derive(Debug, Clone, PartialEq)]
pub struct Repair {
    pub path: PathBuf,
    pub bytes_dropped: u64,
}

/// The on-disk store for one `data_dir`.
pub struct Store {
    data_dir: PathBuf,
}

impl Store {
    /// Create `data_dir` (0700) and the `raw/` + `teams/` roots (0700).
    /// Existing `data_dir` is tightened to 0700 (permission enforcement §2).
    pub fn open(data_dir: &Path) -> Result<Store, Error> {
        ensure_dir_0700(data_dir)?;
        ensure_dir_0700(&data_dir.join(RAW_DIR))?;
        ensure_dir_0700(&data_dir.join(TEAMS_DIR))?;
        Ok(Store {
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// §3.2 startup repair: for every `*.jsonl` under `data_dir`, if the last
    /// line does not end with `\n`, truncate the partial line. The caller
    /// logs each repair (file + bytes dropped).
    pub fn repair_partial_lines(&self) -> Result<Vec<Repair>, Error> {
        let mut out = Vec::new();
        let mut stack = vec![self.data_dir.clone()];
        while let Some(dir) = stack.pop() {
            let rd = std::fs::read_dir(&dir)
                .map_err(|e| Error::Io(format!("read_dir {}: {e}", dir.display())))?;
            for entry in rd {
                let entry =
                    entry.map_err(|e| Error::Io(format!("read_dir {}: {e}", dir.display())))?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    if let Some(r) = repair_file(&path)? {
                        out.push(r);
                    }
                }
            }
        }
        Ok(out)
    }

    /// §3.1 at-least-once safety net: the highest `stream_id` present in the
    /// JSONL dataset. A crash between appending a batch and writing
    /// CHECKPOINT leaves rows on disk whose ids the CHECKPOINT file has not
    /// caught up to; the caller resumes from `max(durable CHECKPOINT, this)`
    /// so the re-read after such a crash cannot duplicate rows (§3.1 "cannot
    /// duplicate rows in raw/ (or anywhere else)"). Lines without a parsable
    /// `stream_id` (e.g. future `sessions/` rows) are skipped.
    pub fn max_written_stream_id(&self) -> Result<Option<String>, Error> {
        let mut best: Option<(crate::streamid::StreamId, String)> = None;
        let mut stack = vec![self.data_dir.clone()];
        while let Some(dir) = stack.pop() {
            let rd = std::fs::read_dir(&dir)
                .map_err(|e| Error::Io(format!("read_dir {}: {e}", dir.display())))?;
            for entry in rd {
                let entry =
                    entry.map_err(|e| Error::Io(format!("read_dir {}: {e}", dir.display())))?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    let text = std::fs::read_to_string(&path)
                        .map_err(|e| Error::Io(format!("read {}: {e}", path.display())))?;
                    for line in text.lines() {
                        let raw = match stream_id_of_line(line) {
                            Some(id) => id,
                            None => continue,
                        };
                        let parsed = match crate::streamid::StreamId::parse(&raw) {
                            Some(id) => id,
                            None => continue,
                        };
                        let better = best.as_ref().is_none_or(|(cur, _)| parsed > *cur);
                        if better {
                            best = Some((parsed, raw));
                        }
                    }
                }
            }
        }
        Ok(best.map(|(_, raw)| raw))
    }

    /// Append a whole XREAD batch (one flush per batch): group lines by
    /// destination file, write each file once, fsync each file. Returns the
    /// number of files written.
    pub fn write_batch(&mut self, entries: &[WriteEntry]) -> Result<usize, Error> {
        // Group serialized lines by destination path.
        let mut by_path: BTreeMap<PathBuf, Vec<String>> = BTreeMap::new();
        for e in entries {
            let line = serde_json::to_string(&e.line)
                .map_err(|err| Error::Fatal(format!("serialize raw line: {err}")))?;
            for path in self.paths_for(e) {
                by_path.entry(path).or_default().push(line.clone());
            }
        }
        let n = by_path.len();
        for (path, lines) in by_path {
            append_lines(&path, &lines)?;
        }
        Ok(n)
    }

    fn paths_for(&self, e: &WriteEntry) -> [PathBuf; 2] {
        let partition = format!("dt={}", e.dt);
        let office = self
            .data_dir
            .join(RAW_DIR)
            .join(&partition)
            .join("events.jsonl");
        let team = self
            .data_dir
            .join(TEAMS_DIR)
            .join(&e.team_safe)
            .join(RAW_DIR)
            .join(partition)
            .join("events.jsonl");
        [office, team]
    }
}

/// Create a directory with mode 0700 (tightening an existing directory too).
fn ensure_dir_0700(dir: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir)
        .map_err(|e| Error::Io(format!("create dir {}: {e}", dir.display())))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| Error::Io(format!("chmod {}: {e}", dir.display())))?;
    Ok(())
}

/// Append lines to a JSONL file: create parent dirs (0700), open/append with
/// mode 0600, write all lines, fsync.
fn append_lines(path: &Path, lines: &[String]) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        ensure_dir_0700(parent)?;
    }
    let mut f = crate::fsutil::open_private(path, true)?;
    let mut buf = String::new();
    for l in lines {
        buf.push_str(l);
        buf.push('\n');
    }
    f.write_all(buf.as_bytes())
        .map_err(|e| Error::Io(format!("write {}: {e}", path.display())))?;
    f.sync_all()
        .map_err(|e| Error::Io(format!("fsync {}: {e}", path.display())))?;
    Ok(())
}

/// §3.2: if the file's last line does not end with `\n`, truncate the partial
/// line (drop it). Returns the repair when one happened.
fn repair_file(path: &Path) -> Result<Option<Repair>, Error> {
    use std::io::{Read, Seek, SeekFrom};
    let meta =
        std::fs::metadata(path).map_err(|e| Error::Io(format!("stat {}: {e}", path.display())))?;
    let len = meta.len();
    if len == 0 {
        return Ok(None);
    }
    let mut f = std::fs::File::open(path)
        .map_err(|e| Error::Io(format!("open {}: {e}", path.display())))?;
    f.seek(SeekFrom::End(-1))
        .map_err(|e| Error::Io(format!("seek {}: {e}", path.display())))?;
    let mut last = [0u8; 1];
    f.read_exact(&mut last)
        .map_err(|e| Error::Io(format!("read {}: {e}", path.display())))?;
    if last[0] == b'\n' {
        return Ok(None);
    }
    // Scan backwards for the last newline; if none, truncate to 0.
    let truncate_at = find_last_newline(&mut f, len)?;
    drop(f);
    let w = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| Error::Io(format!("open {}: {e}", path.display())))?;
    w.set_len(truncate_at)
        .map_err(|e| Error::Io(format!("truncate {}: {e}", path.display())))?;
    w.sync_all()
        .map_err(|e| Error::Io(format!("fsync {}: {e}", path.display())))?;
    Ok(Some(Repair {
        path: path.to_path_buf(),
        bytes_dropped: len - truncate_at,
    }))
}

/// Position just after the last `\n` in the file, or 0 if there is none.
fn find_last_newline(f: &mut std::fs::File, len: u64) -> Result<u64, Error> {
    use std::io::{Read, Seek, SeekFrom};
    const CHUNK: u64 = 8192;
    let mut pos = len;
    let mut buf = vec![0u8; CHUNK as usize];
    while pos > 0 {
        let start = pos.saturating_sub(CHUNK);
        let n = (pos - start) as usize;
        f.seek(SeekFrom::Start(start))
            .map_err(|e| Error::Io(format!("seek: {e}")))?;
        f.read_exact(&mut buf[..n])
            .map_err(|e| Error::Io(format!("read: {e}")))?;
        if let Some(rel) = buf[..n].iter().rposition(|&b| b == b'\n') {
            return Ok(start + rel as u64 + 1);
        }
        pos = start;
    }
    Ok(0)
}

/// Extract the `stream_id` field from one raw JSONL line, if present.
fn stream_id_of_line(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    v.get("stream_id")?.as_str().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{decode, RawLine};
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;

    fn line(id: &str) -> RawLine {
        let flat: BTreeMap<String, String> = [
            ("action".to_string(), "task.started".to_string()),
            ("actor".to_string(), "dev".to_string()),
            ("team".to_string(), "dev-1".to_string()),
        ]
        .into();
        decode(id, &flat).line
    }

    fn entry(id: &str, team_safe: &str, dt: &str) -> WriteEntry {
        WriteEntry {
            line: line(id),
            team_safe: team_safe.to_string(),
            dt: dt.to_string(),
        }
    }

    fn read_lines(p: &Path) -> Vec<String> {
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn open_creates_roots_with_0700() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        for p in [
            dir.path(),
            &dir.path().join(RAW_DIR),
            &dir.path().join(TEAMS_DIR),
        ] {
            assert!(p.is_dir());
            let mode = std::fs::metadata(p).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "{p:?} must be 0700");
        }
        assert_eq!(store.data_dir(), dir.path());
    }

    #[test]
    fn existing_data_dir_is_tightened_to_0700() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        Store::open(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn batch_writes_office_and_team_views() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let n = store
            .write_batch(&[entry("1725062400000-0", "dev-1", "2024-08-31")])
            .unwrap();
        assert_eq!(n, 2, "office + team view = 2 files");

        let office = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        let team = dir
            .path()
            .join("teams/dev-1/raw/dt=2024-08-31/events.jsonl");
        assert_eq!(read_lines(&office), read_lines(&team));
        let rows: Vec<serde_json::Value> = read_lines(&office)
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["stream_id"], "1725062400000-0");
        assert_eq!(rows[0]["team"], "dev-1", "original team kept on the row");
        // perms: files 0600
        for p in [&office, &team] {
            let mode = std::fs::metadata(p).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "{p:?} must be 0600");
        }
    }

    #[test]
    fn batch_appends_in_order_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store
            .write_batch(&[entry("1-0", "dev-1", "2024-08-31")])
            .unwrap();
        store
            .write_batch(&[entry("2-0", "dev-1", "2024-08-31")])
            .unwrap();
        let office = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        let lines = read_lines(&office);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"stream_id\":\"1-0\""));
        assert!(lines[1].contains("\"stream_id\":\"2-0\""));
    }

    #[test]
    fn multiple_teams_and_dts_split_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store
            .write_batch(&[
                entry("1-0", "dev-1", "2024-08-31"),
                entry("2-0", "dev-2", "2024-08-31"),
                entry("3-0", "dev-1", "2024-09-01"),
            ])
            .unwrap();
        let dev1 = dir
            .path()
            .join("teams/dev-1/raw/dt=2024-08-31/events.jsonl");
        let dev2 = dir
            .path()
            .join("teams/dev-2/raw/dt=2024-08-31/events.jsonl");
        let dev1_next = dir
            .path()
            .join("teams/dev-1/raw/dt=2024-09-01/events.jsonl");
        assert_eq!(read_lines(&dev1).len(), 1);
        assert_eq!(read_lines(&dev2).len(), 1);
        assert_eq!(read_lines(&dev1_next).len(), 1);
        // office raw has all three
        assert_eq!(
            read_lines(&dir.path().join("raw/dt=2024-08-31/events.jsonl")).len(),
            2
        );
        assert_eq!(
            read_lines(&dir.path().join("raw/dt=2024-09-01/events.jsonl")).len(),
            1
        );
    }

    #[test]
    fn unknown_team_goes_to_unknown_folder() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store
            .write_batch(&[entry("1-0", "_unknown", "2024-08-31")])
            .unwrap();
        assert!(dir
            .path()
            .join("teams/_unknown/raw/dt=2024-08-31/events.jsonl")
            .is_file());
    }

    // --- §3.2 startup repair ---------------------------------------------------

    fn write_file(p: &Path, content: &str) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }

    #[test]
    fn repair_truncates_trailing_partial_line() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let f = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        write_file(&f, "{\"a\":1}\n{\"a\":2}\n{\"a\":3");
        let repairs = store.repair_partial_lines().unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].path, f);
        assert_eq!(repairs[0].bytes_dropped, 6, "partial line is 6 bytes");
        assert_eq!(
            std::fs::read_to_string(&f).unwrap(),
            "{\"a\":1}\n{\"a\":2}\n"
        );
    }

    #[test]
    fn repair_leaves_complete_files_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let f = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        write_file(&f, "{\"a\":1}\n{\"a\":2}\n");
        assert!(store.repair_partial_lines().unwrap().is_empty());
        assert_eq!(
            std::fs::read_to_string(&f).unwrap(),
            "{\"a\":1}\n{\"a\":2}\n"
        );
    }

    #[test]
    fn repair_truncates_file_with_no_newline_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let f = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        write_file(&f, "{\"a\":1}");
        let repairs = store.repair_partial_lines().unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].bytes_dropped, 7);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), "");
    }

    #[test]
    fn repair_skips_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let f = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        write_file(&f, "");
        assert!(store.repair_partial_lines().unwrap().is_empty());
    }

    #[test]
    fn repair_walks_all_views_and_skips_non_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        write_file(&dir.path().join("raw/dt=2024-08-31/events.jsonl"), "ok\n");
        write_file(
            &dir.path()
                .join("teams/dev-1/raw/dt=2024-08-31/events.jsonl"),
            "partial",
        );
        write_file(&dir.path().join("CHECKPOINT"), "123-0\n"); // not jsonl → untouched
        write_file(&dir.path().join(".lock"), "{}");
        let repairs = store.repair_partial_lines().unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(
            repairs[0].path,
            dir.path()
                .join("teams/dev-1/raw/dt=2024-08-31/events.jsonl")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("CHECKPOINT")).unwrap(),
            "123-0\n"
        );
    }

    // --- §3.1 crash-window watermark scan --------------------------------------

    #[test]
    fn max_written_returns_highest_stream_id_across_views() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        store
            .write_batch(&[entry("1-0", "dev-1", "2024-08-31")])
            .unwrap();
        store
            .write_batch(&[entry("2-0", "dev-1", "2024-08-31")])
            .unwrap();
        store
            .write_batch(&[entry("3-0", "dev-2", "2024-09-01")])
            .unwrap();
        assert_eq!(
            store.max_written_stream_id().unwrap().as_deref(),
            Some("3-0")
        );
    }

    #[test]
    fn max_written_empty_dir_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.max_written_stream_id().unwrap(), None);
    }

    #[test]
    fn max_written_ignores_unparsable_lines_and_non_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let f = dir.path().join("raw/dt=2024-08-31/events.jsonl");
        write_file(
            &f,
            "{\"stream_id\":\"1-0\"}\nnot-json\n{\"stream_id\":\"garbage\"}\n{\"stream_id\":\"2-0\"}\n",
        );
        write_file(&dir.path().join("CHECKPOINT"), "9-0\n"); // not jsonl → ignored
        assert_eq!(
            store.max_written_stream_id().unwrap().as_deref(),
            Some("2-0")
        );
    }
}
