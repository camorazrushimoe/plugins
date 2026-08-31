//! Shared helpers for the integration tests: temp trees, fixture lines,
//! and size math. Expected values in tests are derived from the exact
//! content strings written here — an independent source from the
//! implementation, which reads real file bytes.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new(name: &str) -> Self {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("wfdc-bon69-{name}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    pub fn join(&self, rel: &str) -> PathBuf {
        self.path.join(rel)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Write `content` at `rel` under `dir`, creating parents.
pub fn write(dir: &Path, rel: &str, content: &str) -> PathBuf {
    let p = dir.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(&p, content).unwrap();
    p
}

/// A raw event line with a stream id (spec §5.2 shape, trimmed to what
/// the collector's views need).
pub fn raw_line(stream_id: &str) -> String {
    format!(
        "{{\"stream_id\":\"{stream_id}\",\"action\":\"task.started\",\"ts\":\"2026-08-31T00:00:00Z\",\"team\":\"dev-1\"}}\n"
    )
}

/// A session row line (spec §5.3 shape).
pub fn session_line(state: &str, start: Option<&str>, finish: Option<&str>) -> String {
    let mut fields = vec![
        format!("\"session_pk\":\"pk-{state}\""),
        format!("\"state\":\"{state}\""),
    ];
    if let Some(s) = start {
        fields.push(format!("\"start_stream_id\":\"{s}\""));
    }
    if let Some(f) = finish {
        fields.push(format!("\"finish_stream_id\":\"{f}\""));
    }
    format!("{{{}}}\n", fields.join(","))
}

pub fn bytes(s: &str) -> u64 {
    s.len() as u64
}

/// Build one concatenated file content from lines and return it.
pub fn file(lines: &[String]) -> String {
    lines.concat()
}

/// True when a path exists on disk.
pub fn exists(p: &Path) -> bool {
    p.exists()
}

/// Read a file's content.
pub fn read(p: &Path) -> String {
    fs::read_to_string(p).unwrap()
}

/// List every `*.tmp` path under `dir`.
pub fn tmp_leftovers(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(d: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(rd) = fs::read_dir(d) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .map(|n| n.to_string_lossy().ends_with(".tmp"))
                    .unwrap_or(false)
                {
                    out.push(p);
                }
            }
        }
    }
    walk(dir, &mut out);
    out
}
