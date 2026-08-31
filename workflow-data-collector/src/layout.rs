//! On-disk layout scanning (spec §5.1–§5.3, §5.5).
//!
//! The scanner walks `data_dir` and classifies every `*.jsonl` file into
//! the three views the trim operates on:
//!
//! - office raw:    `raw/dt=<date>/events.jsonl`
//! - team raw:      `teams/<team_safe>/raw/dt=<date>/events.jsonl`
//! - team sessions: `teams/<team_safe>/sessions/dt=<date>/sessions.jsonl`
//!
//! Any other `*.jsonl` is counted toward the cap but is never trimmed
//! (it is not a view). `MANIFEST.json`, `CHECKPOINT` and `.lock` are not
//! `*.jsonl` and are neither counted nor touched.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub const RAW_DIR: &str = "raw";
pub const SESSIONS_DIR: &str = "sessions";
pub const TEAMS_DIR: &str = "teams";
pub const RAW_FILE: &str = "events.jsonl";
pub const SESSIONS_FILE: &str = "sessions.jsonl";

/// Which of the three spec views a raw file belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewKind {
    /// `raw/dt=…/events.jsonl` — the office-wide dataset.
    OfficeRaw,
    /// `teams/<team>/raw/dt=…/events.jsonl`.
    TeamRaw,
}

/// One JSONL line in a raw events view.
#[derive(Debug, Clone)]
pub struct EventLine {
    /// `stream_id` of the event, when the line parsed. A line without a
    /// parsable `stream_id` is kept and never trimmed.
    pub stream_id: Option<String>,
    /// Exact bytes of the line (including its trailing `\n` when present).
    pub bytes: Vec<u8>,
}

impl EventLine {
    pub fn byte_len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// One JSONL line in a sessions view (§5.3).
#[derive(Debug, Clone)]
pub struct SessionRow {
    /// `state`: completed | open | interrupted | orphan_finish | expired.
    pub state: String,
    pub start_stream_id: Option<String>,
    pub finish_stream_id: Option<String>,
    /// Exact bytes of the row (including its trailing `\n` when present).
    pub bytes: Vec<u8>,
}

impl SessionRow {
    pub fn byte_len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// A raw events view file (`OfficeRaw` or `TeamRaw`) for one `dt=` date.
#[derive(Debug, Clone)]
pub struct RawView {
    pub kind: ViewKind,
    /// Team folder name for `TeamRaw`; `None` for the office view.
    pub team: Option<String>,
    pub date: String,
    pub path: PathBuf,
    /// File size in bytes at scan time.
    pub bytes: u64,
    pub lines: Vec<EventLine>,
}

/// A sessions view file for one `dt=` date.
#[derive(Debug, Clone)]
pub struct SessionsView {
    pub team: String,
    pub date: String,
    pub path: PathBuf,
    /// File size in bytes at scan time.
    pub bytes: u64,
    pub rows: Vec<SessionRow>,
}

/// Everything the cap/trim logic needs to know about `data_dir`.
#[derive(Debug, Clone, Default)]
pub struct DataDir {
    pub raw_views: Vec<RawView>,
    pub sessions_views: Vec<SessionsView>,
    /// `*.jsonl` files outside the three views — counted, never trimmed.
    pub other_jsonl: Vec<PathBuf>,
}

impl DataDir {
    /// Total bytes of every `*.jsonl` under `data_dir` (the cap, §5.5).
    pub fn jsonl_bytes(&self) -> u64 {
        let views: u64 = self
            .raw_views
            .iter()
            .map(|v| v.bytes)
            .chain(self.sessions_views.iter().map(|v| v.bytes))
            .sum();
        let other: u64 = self
            .other_jsonl
            .iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        views + other
    }

    /// Distinct `dt=` dates across all views, ascending.
    pub fn dates(&self) -> BTreeSet<String> {
        self.raw_views
            .iter()
            .map(|v| v.date.clone())
            .chain(self.sessions_views.iter().map(|v| v.date.clone()))
            .collect()
    }

    /// The oldest `dt=` date across all views, if any.
    pub fn oldest_date(&self) -> Option<String> {
        self.dates().into_iter().next()
    }
}

/// Scan `data_dir` and classify every file into the view model.
///
/// A missing directory yields an empty [`DataDir`] (nothing to trim).
pub fn scan(data_dir: &Path) -> io::Result<DataDir> {
    let mut dd = DataDir::default();
    if !data_dir.is_dir() {
        return Ok(dd);
    }
    walk(data_dir, data_dir, &mut dd)?;
    Ok(dd)
}

fn walk(root: &Path, dir: &Path, dd: &mut DataDir) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            walk(root, &path, dd)?;
        } else if ft.is_file() {
            classify(root, &path, dd)?;
        }
    }
    Ok(())
}

fn classify(root: &Path, path: &Path, dd: &mut DataDir) -> io::Result<()> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| io::Error::other("path escaped data_dir"))?;
    let comps: Vec<&str> = rel
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();

    match comps.as_slice() {
        // raw/dt=<date>/events.jsonl
        [RAW_DIR, dt, RAW_FILE] => {
            if let Some(date) = dt.strip_prefix("dt=") {
                let bytes = fs::metadata(path)?.len();
                let lines = parse_raw_file(path)?;
                dd.raw_views.push(RawView {
                    kind: ViewKind::OfficeRaw,
                    team: None,
                    date: date.to_string(),
                    path: path.to_path_buf(),
                    bytes,
                    lines,
                });
                return Ok(());
            }
        }
        // teams/<team>/raw/dt=<date>/events.jsonl
        [TEAMS_DIR, team, RAW_DIR, dt, RAW_FILE] => {
            if let Some(date) = dt.strip_prefix("dt=") {
                let bytes = fs::metadata(path)?.len();
                let lines = parse_raw_file(path)?;
                dd.raw_views.push(RawView {
                    kind: ViewKind::TeamRaw,
                    team: Some((*team).to_string()),
                    date: date.to_string(),
                    path: path.to_path_buf(),
                    bytes,
                    lines,
                });
                return Ok(());
            }
        }
        // teams/<team>/sessions/dt=<date>/sessions.jsonl
        [TEAMS_DIR, team, SESSIONS_DIR, dt, SESSIONS_FILE] => {
            if let Some(date) = dt.strip_prefix("dt=") {
                let bytes = fs::metadata(path)?.len();
                let rows = parse_sessions_file(path)?;
                dd.sessions_views.push(SessionsView {
                    team: (*team).to_string(),
                    date: date.to_string(),
                    path: path.to_path_buf(),
                    bytes,
                    rows,
                });
                return Ok(());
            }
        }
        _ => {}
    }

    // Any other *.jsonl counts toward the cap but is not a view.
    if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
        dd.other_jsonl.push(path.to_path_buf());
    }
    Ok(())
}

fn parse_raw_file(path: &Path) -> io::Result<Vec<EventLine>> {
    let data = fs::read(path)?;
    let mut lines = Vec::new();
    let mut start = 0usize;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            lines.push(parse_raw_segment(&data[start..=i]));
            start = i + 1;
        }
    }
    if start < data.len() {
        // Trailing partial line (startup repair normally prevents this);
        // keep it as-is, byte-exact, never trimmed (no stream_id may parse).
        lines.push(parse_raw_segment(&data[start..]));
    }
    Ok(lines)
}

fn parse_raw_segment(bytes: &[u8]) -> EventLine {
    let trimmed = strip_eol(bytes);
    let stream_id = serde_json::from_slice::<serde_json::Value>(trimmed)
        .ok()
        .and_then(|v| {
            v.get("stream_id")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        });
    EventLine {
        stream_id,
        bytes: bytes.to_vec(),
    }
}

fn parse_sessions_file(path: &Path) -> io::Result<Vec<SessionRow>> {
    let data = fs::read(path)?;
    let mut rows = Vec::new();
    let mut start = 0usize;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            rows.push(parse_session_segment(&data[start..=i]));
            start = i + 1;
        }
    }
    if start < data.len() {
        rows.push(parse_session_segment(&data[start..]));
    }
    Ok(rows)
}

fn parse_session_segment(bytes: &[u8]) -> SessionRow {
    let trimmed = strip_eol(bytes);
    let mut state = String::new();
    let mut start_stream_id = None;
    let mut finish_stream_id = None;
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(trimmed) {
        state = v
            .get("state")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        start_stream_id = v
            .get("start_stream_id")
            .and_then(|s| s.as_str())
            .map(str::to_string);
        finish_stream_id = v
            .get("finish_stream_id")
            .and_then(|s| s.as_str())
            .map(str::to_string);
    }
    SessionRow {
        state,
        start_stream_id,
        finish_stream_id,
        bytes: bytes.to_vec(),
    }
}

fn strip_eol(mut b: &[u8]) -> &[u8] {
    if b.last() == Some(&b'\n') {
        b = &b[..b.len() - 1];
    }
    if b.last() == Some(&b'\r') {
        b = &b[..b.len() - 1];
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_bytes_excludes_non_jsonl_files() {
        let dir = std::env::temp_dir().join(format!("wfdc-layout-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("raw/dt=2026-08-31")).unwrap();
        fs::write(
            dir.join("raw/dt=2026-08-31/events.jsonl"),
            "{\"stream_id\":\"1-0\"}\n",
        )
        .unwrap();
        fs::write(dir.join("MANIFEST.json"), "{\"bytes_used\": 999999999}").unwrap();
        fs::write(dir.join("CHECKPOINT"), "1725062400000-0").unwrap();
        fs::write(dir.join(".lock"), "pid 1234").unwrap();
        let dd = scan(&dir).unwrap();
        assert_eq!(dd.jsonl_bytes(), 20);
        assert_eq!(dd.raw_views.len(), 1);
        assert_eq!(dd.raw_views[0].date, "2026-08-31");
        let _ = fs::remove_dir_all(&dir);
    }
}
