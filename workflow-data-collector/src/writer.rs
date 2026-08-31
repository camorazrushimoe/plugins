//! Raw JSONL writer (§5.2): office `raw/` plus per-team `teams/<team>/raw/`.
//! Append-only within a partition, fsync per flush, files 0600, dirs 0700.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use crate::Error;
use serde_json::{Map, Value};

/// One prepared raw row: the serialized line plus its partitioning and team.
#[derive(Debug, Clone)]
pub struct RawRow {
    pub dt: String,
    pub team_folder: String,
    pub json: String,
}

/// Build the raw JSONL line per §5.2 (stable field order).
pub fn raw_line(stream_id: &str, decoded: &crate::decoder::Decoded) -> String {
    let mut m = Map::new();
    m.insert("stream_id".into(), Value::String(stream_id.to_string()));
    m.insert(
        "envelope_id".into(),
        decoded
            .envelope_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "ts".into(),
        decoded.ts.clone().map(Value::String).unwrap_or(Value::Null),
    );
    m.insert(
        "actor".into(),
        decoded
            .actor
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "action".into(),
        decoded
            .action
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "target".into(),
        decoded
            .target
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "team".into(),
        decoded
            .team
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "project".into(),
        decoded.project.clone().unwrap_or(Value::Null),
    );
    m.insert("payload".into(), decoded.payload.clone());
    m.insert("fields".into(), Value::Object(decoded.fields.clone()));
    m.insert("decode_ok".into(), Value::Bool(decoded.decode_ok));

    let mut line = serde_json::to_string(&Value::Object(m)).expect("serialize raw row");
    line.push('\n');
    line
}

fn set_0600(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Ensure a directory exists with 0700 perms (§2).
pub fn ensure_0700(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    if !path.exists() {
        std::fs::create_dir_all(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    } else {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Ensure the data_dir layout exists (0700).
pub fn ensure_data_dir(data_dir: &Path) -> Result<(), Error> {
    ensure_0700(data_dir)?;
    ensure_0700(&data_dir.join("raw"))?;
    Ok(())
}

/// Append a batch of raw rows and fsync. Rows are grouped per file
/// (`raw/dt=…/events.jsonl` and `teams/<team>/raw/dt=…/events.jsonl`).
/// One flush per XREAD batch in follow; once per backfill run.
pub fn append_batch(data_dir: &Path, rows: &[RawRow]) -> Result<(), Error> {
    if rows.is_empty() {
        return Ok(());
    }
    // Group by target file to keep the same open handle across rows of a file.
    let mut files: BTreeMap<std::path::PathBuf, Vec<&str>> = BTreeMap::new();
    for row in rows {
        let office = data_dir
            .join("raw")
            .join(format!("dt={}", row.dt))
            .join("events.jsonl");
        files.entry(office).or_default().push(&row.json);
        let team = data_dir
            .join("teams")
            .join(&row.team_folder)
            .join("raw")
            .join(format!("dt={}", row.dt))
            .join("events.jsonl");
        files.entry(team).or_default().push(&row.json);
    }

    for (path, lines) in files {
        if let Some(parent) = path.parent() {
            ensure_0700(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        for line in lines {
            f.write_all(line.as_bytes())?;
        }
        f.sync_all()?;
        set_0600(&path)?;
    }
    Ok(())
}

/// §3.1 at-least-once safety net: the highest `stream_id` present in the
/// JSONL dataset. A crash between appending a batch and writing CHECKPOINT
/// leaves rows on disk whose ids the CHECKPOINT file has not caught up to;
/// the caller resumes from `max(durable CHECKPOINT, this)` so the re-read
/// after such a crash cannot duplicate rows (§3.1 "cannot duplicate rows in
/// raw/ (or anywhere else)"). Lines without a parsable `stream_id` (e.g.
/// future `sessions/` rows) are skipped.
pub fn max_written_stream_id(data_dir: &Path) -> Result<Option<String>, Error> {
    let mut best: Option<((u64, u64), String)> = None;
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = std::fs::read_dir(&dir)?;
        for entry in rd {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                let text = std::fs::read_to_string(&path)?;
                for line in text.lines() {
                    let raw = match stream_id_of_line(line) {
                        Some(id) => id,
                        None => continue,
                    };
                    let parsed = match crate::checkpoint::parse_id(&raw) {
                        Some(id) => id,
                        None => continue,
                    };
                    let better = best.as_ref().map(|(cur, _)| parsed > *cur).unwrap_or(true);
                    if better {
                        best = Some((parsed, raw));
                    }
                }
            }
        }
    }
    Ok(best.map(|(_, raw)| raw))
}

/// Extract the `stream_id` field from one raw JSONL line, if present.
fn stream_id_of_line(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    v.get("stream_id")?.as_str().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-writer-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn decoded(team: &str, action: &str) -> crate::decoder::Decoded {
        let mut flat = HashMap::new();
        flat.insert("action".into(), action.to_string());
        flat.insert("team".into(), team.to_string());
        flat.insert("actor".into(), "dev".to_string());
        crate::decoder::decode("1-0", &flat)
    }

    #[test]
    fn raw_line_shape_matches_spec() {
        let d = decoded("dev-1", "task.started");
        let line = raw_line("1725062400000-0", &d);
        let v: Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(v["stream_id"], "1725062400000-0");
        assert_eq!(v["actor"], "dev");
        assert_eq!(v["action"], "task.started");
        assert_eq!(v["team"], "dev-1");
        assert_eq!(v["decode_ok"], true);
        assert!(v["fields"]["actor"].is_string());
        // field order per §5.2 (preserve_order)
        let order: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        assert_eq!(
            order,
            vec![
                "stream_id",
                "envelope_id",
                "ts",
                "actor",
                "action",
                "target",
                "team",
                "project",
                "payload",
                "fields",
                "decode_ok"
            ]
        );
    }

    #[test]
    fn append_writes_office_and_team_views() {
        let dir = tmpdir("views");
        ensure_data_dir(&dir).unwrap();
        let row = RawRow {
            dt: "2026-08-30".into(),
            team_folder: "dev-1".into(),
            json: "{\"stream_id\":\"1-0\"}\n".into(),
        };
        append_batch(&dir, &[row]).unwrap();

        let office = dir.join("raw").join("dt=2026-08-30").join("events.jsonl");
        let team = dir
            .join("teams")
            .join("dev-1")
            .join("raw")
            .join("dt=2026-08-30")
            .join("events.jsonl");
        assert_eq!(
            std::fs::read_to_string(&office).unwrap(),
            "{\"stream_id\":\"1-0\"}\n"
        );
        assert_eq!(
            std::fs::read_to_string(&team).unwrap(),
            "{\"stream_id\":\"1-0\"}\n"
        );

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&office).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "files 0600");
        let dmode = std::fs::metadata(dir.join("raw"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(dmode & 0o777, 0o700, "dirs 0700");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_is_append_only() {
        let dir = tmpdir("append");
        ensure_data_dir(&dir).unwrap();
        let mk = |id: &str| RawRow {
            dt: "2026-08-30".into(),
            team_folder: "_unknown".into(),
            json: format!("{{\"stream_id\":\"{id}\"}}\n"),
        };
        append_batch(&dir, &[mk("1-0"), mk("1-1")]).unwrap();
        append_batch(&dir, &[mk("1-2")]).unwrap();
        let office = dir.join("raw").join("dt=2026-08-30").join("events.jsonl");
        let text = std::fs::read_to_string(&office).unwrap();
        assert_eq!(text.lines().count(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- §3.1 crash-window watermark scan --------------------------------------

    #[test]
    fn max_written_returns_highest_stream_id_across_views() {
        let dir = tmpdir("maxw-across");
        ensure_data_dir(&dir).unwrap();
        let mk = |id: &str, team: &str, dt: &str| RawRow {
            dt: dt.into(),
            team_folder: team.into(),
            json: format!("{{\"stream_id\":\"{id}\"}}\n"),
        };
        append_batch(&dir, &[mk("1-0", "dev-1", "2024-08-31")]).unwrap();
        append_batch(&dir, &[mk("2-0", "dev-1", "2024-08-31")]).unwrap();
        append_batch(&dir, &[mk("3-0", "dev-2", "2024-09-01")]).unwrap();
        assert_eq!(max_written_stream_id(&dir).unwrap().as_deref(), Some("3-0"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_written_empty_dir_is_none() {
        let dir = tmpdir("maxw-empty");
        ensure_data_dir(&dir).unwrap();
        assert_eq!(max_written_stream_id(&dir).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_written_ignores_unparsable_lines_and_non_jsonl() {
        let dir = tmpdir("maxw-ignore");
        ensure_data_dir(&dir).unwrap();
        let f = dir.join("raw/dt=2024-08-31/events.jsonl");
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(
            &f,
            "{\"stream_id\":\"1-0\"}\nnot-json\n{\"stream_id\":\"garbage\"}\n{\"stream_id\":\"2-0\"}\n",
        )
        .unwrap();
        std::fs::write(dir.join("CHECKPOINT"), "9-0\n").unwrap(); // not jsonl → ignored
        assert_eq!(max_written_stream_id(&dir).unwrap().as_deref(), Some("2-0"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
