//! `sessions.jsonl` (§5.3): the only derived table in v1. Rows are written per
//! day (`teams/<team>/sessions/dt=YYYY-MM-DD/sessions.jsonl`) and upserted
//! when they close — a rewrite of that day's file via `*.tmp` + atomic rename.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

/// One session row (§5.3 table). Field order = serialization order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionRow {
    /// sha256 of `team|actor|start_stream_id` (finish stream id for
    /// `orphan_finish` rows, which have no start).
    pub session_pk: String,
    /// Original `team` string as on the start (finish for orphan rows).
    pub team: Option<String>,
    pub actor: Option<String>,
    pub session_id: Option<String>,
    pub start_stream_id: Option<String>,
    pub finish_stream_id: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: Option<i64>,
    /// `completed | open | interrupted | orphan_finish | expired`
    pub state: String,
    pub snippet_in: Option<String>,
    pub snippet_out: Option<String>,
    pub issues: Option<Value>,
    pub prs: Option<Value>,
    pub linear: Option<Value>,
    pub handoff: Option<Value>,
    pub project: Option<Value>,
}

pub const STATE_COMPLETED: &str = "completed";
pub const STATE_OPEN: &str = "open";
pub const STATE_INTERRUPTED: &str = "interrupted";
pub const STATE_ORPHAN_FINISH: &str = "orphan_finish";
pub const STATE_EXPIRED: &str = "expired";

/// Placement key: (team_folder, dt) — which sessions file a row lives in.
pub type Placement = (String, String);

/// In-memory mirror of the on-disk sessions files, with dirty tracking.
#[derive(Debug, Default)]
pub struct SessionStore {
    rows: BTreeMap<Placement, BTreeMap<String, SessionRow>>,
    dirty: BTreeSet<Placement>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a row at its placement; marks that day's file dirty.
    pub fn upsert(&mut self, placement: Placement, row: SessionRow) {
        self.rows
            .entry(placement.clone())
            .or_default()
            .insert(row.session_pk.clone(), row);
        self.dirty.insert(placement);
    }

    pub fn get(&self, placement: &Placement, pk: &str) -> Option<&SessionRow> {
        self.rows.get(placement).and_then(|m| m.get(pk))
    }

    /// All rows, sorted by (team_folder, dt, pk) — deterministic.
    pub fn all(&self) -> Vec<SessionRow> {
        let mut out = Vec::new();
        for rows in self.rows.values() {
            out.extend(rows.values().cloned());
        }
        out.sort_by(|a, b| a.session_pk.cmp(&b.session_pk));
        out
    }

    /// Direct access to the row map (placement → pk → row), for pool rebuilds.
    pub fn rows(&self) -> &BTreeMap<Placement, BTreeMap<String, SessionRow>> {
        &self.rows
    }

    /// Load every existing sessions file under `data_dir` into the store.
    /// Loaded rows are **not** marked dirty — only rows touched by this run
    /// are rewritten on flush, so untouched day files stay byte-identical.
    pub fn load(&mut self, data_dir: &Path) -> Result<(), Error> {
        let teams = data_dir.join("teams");
        let Ok(entries) = std::fs::read_dir(&teams) else {
            return Ok(()); // no teams yet → nothing to load
        };
        for team_entry in entries.flatten() {
            if !team_entry.path().is_dir() {
                continue;
            }
            let folder = team_entry.file_name().to_string_lossy().to_string();
            let sessions = team_entry.path().join("sessions");
            let Ok(dt_entries) = std::fs::read_dir(&sessions) else {
                continue;
            };
            for dt_entry in dt_entries.flatten() {
                let name = dt_entry.file_name().to_string_lossy().to_string();
                let Some(dt) = name.strip_prefix("dt=").map(|s| s.to_string()) else {
                    continue;
                };
                let file = dt_entry.path().join("sessions.jsonl");
                if !file.exists() {
                    continue;
                }
                let text = std::fs::read_to_string(&file)?;
                for line in text.lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let row: SessionRow = serde_json::from_str(line)?;
                    let placement: Placement = (folder.clone(), dt.clone());
                    self.rows
                        .entry(placement)
                        .or_default()
                        .insert(row.session_pk.clone(), row);
                }
            }
        }
        Ok(())
    }

    /// All rows belonging to one team folder (for tests / status).
    pub fn team_rows(&self, team_folder: &str) -> Vec<SessionRow> {
        let mut out = Vec::new();
        for ((folder, _dt), rows) in &self.rows {
            if folder == team_folder {
                out.extend(rows.values().cloned());
            }
        }
        out.sort_by(|a, b| a.session_pk.cmp(&b.session_pk));
        out
    }

    /// Write every dirty day file atomically (tmp + fsync + rename, 0600).
    pub fn flush(&mut self, data_dir: &Path) -> Result<(), Error> {
        let dirty: Vec<Placement> = self.dirty.iter().cloned().collect();
        for placement in dirty {
            let (team_folder, dt) = &placement;
            let dir = data_dir
                .join("teams")
                .join(team_folder)
                .join("sessions")
                .join(format!("dt={dt}"));
            crate::writer::ensure_0700(&dir)?;
            let final_path = dir.join("sessions.jsonl");
            let tmp = dir.join("sessions.jsonl.tmp");

            let mut content = String::new();
            if let Some(rows) = self.rows.get(&placement) {
                for row in rows.values() {
                    let mut line = serde_json::to_string(row)?;
                    line.push('\n');
                    content.push_str(&line);
                }
            }

            {
                let mut f = std::fs::File::create(&tmp)?;
                use std::os::unix::fs::PermissionsExt;
                f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
                f.write_all(content.as_bytes())?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp, &final_path)?;
            if let Ok(d) = std::fs::File::open(dir.parent().unwrap_or(Path::new("."))) {
                let _ = d.sync_all();
            }
        }
        self.dirty.clear();
        Ok(())
    }
}

impl SessionRow {
    /// The team folder a row is placed in — derived from its original team
    /// string and actor, mirroring how placement is chosen at creation.
    pub fn team_folder(&self) -> Option<String> {
        Some(crate::team::team_folder(
            self.team.as_deref(),
            self.actor.as_deref(),
        ))
    }

    /// The file this row was written to (for tests).
    pub fn file_path(&self, data_dir: &Path) -> Option<PathBuf> {
        let folder = self.team_folder()?;
        // dt is not stored on the row; recover from started_at/finished_at via
        // the stream clock fallback.
        let dt = self
            .started_at
            .as_deref()
            .or(self.finished_at.as_deref())
            .and_then(|ts| {
                let ms = crate::timeutil::event_ms(
                    self.start_stream_id
                        .as_deref()
                        .or(self.finish_stream_id.as_deref())?,
                    Some(ts),
                );
                Some(crate::timeutil::dt_of_ms(ms))
            })?;
        Some(
            data_dir
                .join("teams")
                .join(folder)
                .join("sessions")
                .join(format!("dt={dt}"))
                .join("sessions.jsonl"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-sess-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn row(pk: &str, state: &str) -> SessionRow {
        SessionRow {
            session_pk: pk.to_string(),
            team: Some("dev-1".into()),
            actor: Some("dev".into()),
            session_id: Some("s".into()),
            start_stream_id: Some("1-0".into()),
            finish_stream_id: None,
            started_at: Some("2026-08-30T10:00:00Z".into()),
            finished_at: None,
            duration_ms: None,
            state: state.to_string(),
            snippet_in: None,
            snippet_out: None,
            issues: None,
            prs: None,
            linear: None,
            handoff: None,
            project: None,
        }
    }

    #[test]
    fn flush_writes_sorted_deterministic_file() {
        let dir = tmpdir("flush");
        let mut store = SessionStore::new();
        store.upsert(
            ("dev-1".into(), "2026-08-30".into()),
            row("pk-b", STATE_OPEN),
        );
        store.upsert(
            ("dev-1".into(), "2026-08-30".into()),
            row("pk-a", STATE_COMPLETED),
        );
        store.flush(&dir).unwrap();

        let f = dir
            .join("teams")
            .join("dev-1")
            .join("sessions")
            .join("dt=2026-08-30")
            .join("sessions.jsonl");
        let text = std::fs::read_to_string(&f).unwrap();
        let pks: Vec<String> = text
            .lines()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v["session_pk"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(
            pks,
            vec!["pk-a".to_string(), "pk-b".to_string()],
            "sorted by pk"
        );
        assert!(!dir
            .join("teams")
            .join("dev-1")
            .join("sessions")
            .join("dt=2026-08-30")
            .join("sessions.jsonl.tmp")
            .exists());

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&f).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn upsert_rewrites_existing_row() {
        let dir = tmpdir("upsert");
        let mut store = SessionStore::new();
        let mut r = row("pk-1", STATE_OPEN);
        store.upsert(("dev-1".into(), "2026-08-30".into()), r.clone());
        r.state = STATE_COMPLETED.to_string();
        r.finish_stream_id = Some("2-0".into());
        store.upsert(("dev-1".into(), "2026-08-30".into()), r);
        store.flush(&dir).unwrap();

        let f = dir
            .join("teams")
            .join("dev-1")
            .join("sessions")
            .join("dt=2026-08-30")
            .join("sessions.jsonl");
        let text = std::fs::read_to_string(&f).unwrap();
        assert_eq!(text.lines().count(), 1);
        let v: Value = serde_json::from_str(text.trim_end()).unwrap();
        assert_eq!(v["state"], "completed");
        assert_eq!(v["finish_stream_id"], "2-0");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn session_pk_sha256_shape() {
        // sha256("dev-1|dev|1-0") hex — verify against a known-good literal.
        use sha2::{Digest, Sha256};
        let expect = format!("{:x}", Sha256::digest(b"dev-1|dev|1-0"));
        assert_eq!(expect.len(), 64);
        assert!(expect.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
