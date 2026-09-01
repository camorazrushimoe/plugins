//! Session rows (§5.3 table) and the `sessions.jsonl` store.
//!
//! A session is one agent turn: one `task.started` paired with a later
//! `task.finished`. Rows live under
//! `teams/<team_safe>/sessions/dt=YYYY-MM-DD/sessions.jsonl`, one JSON object
//! per line, rewritten via `*.tmp` + atomic rename on every upsert. Open rows
//! live on the `dt=` of `started_at` and are upserted when they close, even
//! after midnight.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Error;

/// Session lifecycle state (§5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Completed,
    Open,
    Interrupted,
    OrphanFinish,
    Expired,
}

/// One session row, serialized with stable key order (§5.3 table order).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRow {
    pub session_pk: String,
    pub team: String,
    pub actor: String,
    pub session_id: Option<String>,
    pub start_stream_id: Option<String>,
    pub finish_stream_id: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub state: State,
    pub snippet_in: Option<String>,
    pub snippet_out: Option<String>,
    pub issues: Option<Vec<String>>,
    pub prs: Option<Vec<String>>,
    pub linear: Option<Vec<String>>,
    pub handoff: Option<String>,
    pub project: Option<String>,
}

impl SessionRow {
    /// Where this row lives on disk: `(team_folder, dt)` per §5.3. Normal rows
    /// live on the `dt=` of `started_at` in the start's team folder; an
    /// `orphan_finish` lives on its own finish timestamp's `dt=` in the
    /// finish's team folder.
    pub fn location(&self) -> (String, String) {
        let folder = crate::team::team_safe(Some(&self.team), Some(&self.actor));
        let ts = self
            .started_at
            .as_deref()
            .or(self.finished_at.as_deref())
            .and_then(parse_rfc3339)
            .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
        (folder, ts.format("%Y-%m-%d").to_string())
    }

    /// Deterministic file order: by the start stream id when present, else
    /// the finish stream id (`StreamId` is numeric `(ms, seq)` order); `None`
    /// sorts first; `session_pk` breaks ties (it is unique per row).
    fn sort_key(&self) -> (Option<crate::streamid::StreamId>, &str) {
        let sid = self
            .start_stream_id
            .as_deref()
            .or(self.finish_stream_id.as_deref())
            .unwrap_or("");
        (
            crate::streamid::StreamId::parse(sid),
            self.session_pk.as_str(),
        )
    }
}

fn parse_rfc3339(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

/// A whole-file rewrite for one `sessions.jsonl`: the full row set for one
/// `(team_folder, dt)` partition (§5.3 upsert semantics).
#[derive(Debug, Clone, PartialEq)]
pub struct SessionWrite {
    pub team_folder: String,
    pub dt: String,
    pub rows: Vec<SessionRow>,
}

/// The on-disk sessions store for one `data_dir`.
///
/// Reads and rewrites `teams/*/sessions/dt=*/sessions.jsonl`. Every file is
/// created 0600, every directory 0700 (§2). Rewrites go through a `*.tmp`
/// file and an atomic rename, so a crash never leaves a partial session line
/// (and the startup repair in §3.2 never sees one).
pub struct SessionStore {
    data_dir: PathBuf,
}

impl SessionStore {
    pub fn new(data_dir: &Path) -> Self {
        SessionStore {
            data_dir: data_dir.to_path_buf(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Path of one day's session file for a team folder.
    pub fn path_for(&self, team_folder: &str, dt: &str) -> PathBuf {
        self.data_dir
            .join("teams")
            .join(team_folder)
            .join("sessions")
            .join(format!("dt={dt}"))
            .join("sessions.jsonl")
    }

    /// Atomic upsert of one or more whole-partition rewrites. Rows are
    /// deduped by `session_pk` (last wins) and written in deterministic
    /// order. Returns the number of files written.
    pub fn upsert(&self, writes: &[SessionWrite]) -> Result<usize, Error> {
        let mut by_path: BTreeMap<PathBuf, Vec<SessionRow>> = BTreeMap::new();
        for w in writes {
            by_path
                .entry(self.path_for(&w.team_folder, &w.dt))
                .or_default()
                .extend(w.rows.iter().cloned());
        }
        let n = by_path.len();
        for (path, rows) in by_path {
            let mut deduped: BTreeMap<String, SessionRow> = BTreeMap::new();
            for r in rows {
                deduped.insert(r.session_pk.clone(), r);
            }
            let mut rows: Vec<SessionRow> = deduped.into_values().collect();
            rows.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
            write_file_atomic(&path, &rows)?;
        }
        Ok(n)
    }

    /// Load every session row currently on disk (all teams, all `dt=`
    /// partitions). Used at startup to rebuild the pairing pool (§5.3 — an
    /// `open` / `interrupted` row survives a restart and a later finish still
    /// pairs with it). Malformed lines are skipped and logged, never fatal.
    pub fn load_all(&self) -> Result<Vec<SessionRow>, Error> {
        let mut out = Vec::new();
        let teams = self.data_dir.join("teams");
        if !teams.is_dir() {
            return Ok(out);
        }
        let team_entries = std::fs::read_dir(&teams)
            .map_err(|e| Error::Io(format!("read_dir {}: {e}", teams.display())))?;
        for team_entry in team_entries {
            let team_entry =
                team_entry.map_err(|e| Error::Io(format!("read_dir {}: {e}", teams.display())))?;
            let sessions = team_entry.path().join("sessions");
            if !sessions.is_dir() {
                continue;
            }
            let dt_entries = std::fs::read_dir(&sessions)
                .map_err(|e| Error::Io(format!("read_dir {}: {e}", sessions.display())))?;
            for dt_entry in dt_entries {
                let dt_entry = dt_entry
                    .map_err(|e| Error::Io(format!("read_dir {}: {e}", sessions.display())))?;
                let file = dt_entry.path().join("sessions.jsonl");
                if !file.is_file() {
                    continue;
                }
                let content = std::fs::read_to_string(&file)
                    .map_err(|e| Error::Io(format!("read {}: {e}", file.display())))?;
                for (i, line) in content.lines().enumerate() {
                    match serde_json::from_str::<SessionRow>(line) {
                        Ok(row) => out.push(row),
                        Err(e) => log::warn!(
                            "sessions.jsonl: skipping malformed line {} in {}: {e}",
                            i + 1,
                            file.display()
                        ),
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Rewrite one `sessions.jsonl` via `*.tmp` + atomic rename. Directories are
/// created 0700, the file 0600 (§2); the file is fsynced before the rename so
/// a crash between rename and next fsync never yields an empty/partial file.
fn write_file_atomic(path: &Path, rows: &[SessionRow]) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        ensure_dir_0700(parent)?;
    }
    let tmp = path.with_extension("jsonl.tmp");
    let mut f = crate::fsutil::open_private(&tmp, false)?;
    let mut buf = String::new();
    for r in rows {
        let line = serde_json::to_string(r)
            .map_err(|err| Error::Fatal(format!("serialize session row: {err}")))?;
        buf.push_str(&line);
        buf.push('\n');
    }
    f.write_all(buf.as_bytes())
        .map_err(|e| Error::Io(format!("write {}: {e}", tmp.display())))?;
    f.sync_all()
        .map_err(|e| Error::Io(format!("fsync {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        Error::Io(format!(
            "rename {} → {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pk: &str, start: Option<&str>, finish: Option<&str>, state: State) -> SessionRow {
        SessionRow {
            session_pk: pk.to_string(),
            team: "dev-1".to_string(),
            actor: "developer".to_string(),
            session_id: None,
            start_stream_id: start.map(|s| s.to_string()),
            finish_stream_id: finish.map(|s| s.to_string()),
            started_at: Some("2026-08-30T21:00:00Z".to_string()),
            finished_at: None,
            duration_ms: None,
            state,
            snippet_in: None,
            snippet_out: None,
            issues: None,
            prs: None,
            linear: None,
            handoff: None,
            project: None,
        }
    }

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn location_uses_started_at_dt_and_team_folder() {
        let r = row("pk1", Some("1725062400000-0"), None, State::Open);
        assert_eq!(
            r.location(),
            ("dev-1".to_string(), "2026-08-30".to_string())
        );
    }

    #[test]
    fn location_of_orphan_uses_finished_at() {
        let mut r = row("pk1", None, Some("1725062400000-0"), State::OrphanFinish);
        r.started_at = None;
        r.finished_at = Some("2026-08-31T00:01:00Z".to_string());
        assert_eq!(
            r.location(),
            ("dev-1".to_string(), "2026-08-31".to_string())
        );
    }

    #[test]
    fn upsert_creates_file_with_0600_and_dirs_0700() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        let w = SessionWrite {
            team_folder: "dev-1".to_string(),
            dt: "2026-08-30".to_string(),
            rows: vec![row("pk1", Some("1-0"), None, State::Open)],
        };
        store.upsert(&[w]).unwrap();
        let path = store.path_for("dev-1", "2026-08-30");
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(
            meta.permissions().mode() & 0o777,
            0o600,
            "file must be 0600"
        );
        let dirmeta =
            std::fs::metadata(dir.path().join("teams/dev-1/sessions/dt=2026-08-30")).unwrap();
        assert_eq!(
            dirmeta.permissions().mode() & 0o777,
            0o700,
            "dirs must be 0700"
        );
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.ends_with('\n'));
        assert!(content.contains("\"session_pk\":\"pk1\""));
        // no leftover tmp file
        assert!(!path.with_extension("jsonl.tmp").exists());
    }

    #[test]
    fn upsert_is_atomic_rewrite_not_append() {
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        store
            .upsert(&[SessionWrite {
                team_folder: "dev-1".to_string(),
                dt: "2026-08-30".to_string(),
                rows: vec![row("pk1", Some("1-0"), None, State::Open)],
            }])
            .unwrap();
        // Rewrite the same file with a second row; pk1 must still be there
        // (whole-partition upsert) and pk2 appended in deterministic order.
        store
            .upsert(&[SessionWrite {
                team_folder: "dev-1".to_string(),
                dt: "2026-08-30".to_string(),
                rows: vec![
                    row("pk2", Some("2-0"), None, State::Open),
                    row("pk1", Some("1-0"), None, State::Open),
                ],
            }])
            .unwrap();
        let content = std::fs::read_to_string(store.path_for("dev-1", "2026-08-30")).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "pk1 must survive the rewrite");
        assert!(
            lines[0].contains("\"session_pk\":\"pk1\""),
            "sorted by start id: {} ",
            lines[0]
        );
        assert!(lines[1].contains("\"session_pk\":\"pk2\""));
    }

    #[test]
    fn upsert_dedupes_by_session_pk_last_wins() {
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        let mut closed = row("pk1", Some("1-0"), Some("2-0"), State::Completed);
        closed.finished_at = Some("2026-08-30T22:00:00Z".to_string());
        let open = row("pk1", Some("1-0"), None, State::Open);
        store
            .upsert(&[SessionWrite {
                team_folder: "dev-1".to_string(),
                dt: "2026-08-30".to_string(),
                rows: vec![closed.clone(), open],
            }])
            .unwrap();
        let content = std::fs::read_to_string(store.path_for("dev-1", "2026-08-30")).unwrap();
        assert_eq!(content.lines().count(), 1, "duplicate pk must collapse");
        assert!(content.contains("\"state\":\"open\""));
        assert!(!content.contains("\"state\":\"completed\""));
    }

    #[test]
    fn load_all_reads_rows_across_teams_and_partitions() {
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        store
            .upsert(&[
                SessionWrite {
                    team_folder: "dev-1".to_string(),
                    dt: "2026-08-30".to_string(),
                    rows: vec![row("pk1", Some("1-0"), None, State::Open)],
                },
                SessionWrite {
                    team_folder: "_unknown".to_string(),
                    dt: "2026-08-31".to_string(),
                    rows: vec![row("pk2", Some("3-0"), None, State::Open)],
                },
            ])
            .unwrap();
        let rows = store.load_all().unwrap();
        assert_eq!(rows.len(), 2);
        let pks: Vec<&str> = rows.iter().map(|r| r.session_pk.as_str()).collect();
        assert!(pks.contains(&"pk1") && pks.contains(&"pk2"));
    }

    #[test]
    fn load_all_skips_malformed_lines() {
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        store
            .upsert(&[SessionWrite {
                team_folder: "dev-1".to_string(),
                dt: "2026-08-30".to_string(),
                rows: vec![row("pk1", Some("1-0"), None, State::Open)],
            }])
            .unwrap();
        let path = store.path_for("dev-1", "2026-08-30");
        let mut content = std::fs::read_to_string(&path).unwrap();
        content.push_str("{not-json}\n");
        std::fs::write(&path, content).unwrap();
        let rows = store.load_all().unwrap();
        assert_eq!(rows.len(), 1, "malformed line must be skipped, not fatal");
        assert_eq!(rows[0].session_pk, "pk1");
    }

    #[test]
    fn load_all_empty_when_nothing_written() {
        let dir = tempdir();
        let store = SessionStore::new(dir.path());
        assert!(store.load_all().unwrap().is_empty());
    }

    #[test]
    fn row_json_key_order_follows_spec_table() {
        let r = row("pk1", Some("1-0"), None, State::Open);
        let s = serde_json::to_string(&r).unwrap();
        let keys: [&str; 17] = [
            "session_pk",
            "team",
            "actor",
            "session_id",
            "start_stream_id",
            "finish_stream_id",
            "started_at",
            "finished_at",
            "duration_ms",
            "state",
            "snippet_in",
            "snippet_out",
            "issues",
            "prs",
            "linear",
            "handoff",
            "project",
        ];
        // exact §5.3 column order: first key must be session_pk, etc.
        assert!(
            s.starts_with(&format!("{{\"{}\":", keys[0])),
            "first key must be session_pk: {s}"
        );
        let mut prev_idx = 0usize;
        for k in keys {
            let idx = s
                .find(&format!("\"{k}\":"))
                .unwrap_or_else(|| panic!("missing key {k}: {s}"));
            assert!(idx > prev_idx, "key {k} out of order: {s}");
            prev_idx = idx;
        }
    }
}
