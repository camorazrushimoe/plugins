//! MANIFEST.json (§5.4) + `wfdc status` / `wfdc status --json`.
//!
//! The manifest is the machine-readable observability document: rewritten
//! after every successful flush (follow and backfill), and printed verbatim
//! by `wfdc status --json`. It never contains the Redis password — the URL
//! is stored with userinfo stripped (§5.4).
//!
//! Stable key order is a contract: `wfdc status --json` prints one JSON
//! document whose keys appear in the same order every time, and `wfdc status`
//! prints the same fields as `key: value` lines, so staging checks and tests
//! can assert on either.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::checkpoint;
use crate::config::Config;
use crate::sessions::SessionStore;
use crate::writer;
use crate::Error;

pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// One drop-log entry (§5.4). The trim (BON-69, spec §5.5) appends these;
/// the manifest exposes the last 100. Until the trim is wired, the array is
/// present and empty — the shape is the contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropLogEntry {
    /// Wall-clock UTC instant the entry was recorded (RFC 3339).
    pub when: String,
    /// `date` → a whole `dt=` partition was dropped; `today` → one event trimmed.
    pub scope: String,
    /// Dropped `dt=` partition (scope `date` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Trimmed event's stream id (scope `today` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    /// Bytes freed by this drop (sum over every view touched).
    pub bytes_freed: u64,
}

/// The observability document (§5.4). Field order = stable serialization order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Crate version (`Cargo.toml`), the same string `wfdc --version` prints.
    pub plugin_version: String,
    /// `redis_url` with userinfo stripped — never the password (§5.4).
    pub redis_url: String,
    /// Stream name being collected.
    pub stream: String,
    /// Last flushed CHECKPOINT (§3.1); `null` before the first flush.
    pub checkpoint: Option<String>,
    /// Highest stream id actually written to JSONL — `max(durable CHECKPOINT,
    /// highest id on disk)`, so a crash between append and checkpoint write is
    /// visible here rather than hidden.
    pub last_flush_stream_id: Option<String>,
    /// Total events in the office `raw/` dataset (the canonical event set;
    /// per-team raws mirror it, so they are not double-counted).
    pub event_count: u64,
    /// Total session rows across all teams.
    pub session_count: u64,
    /// Per-`dt=` byte counts of office `raw/dt=…/events.jsonl`.
    pub per_dt_bytes: BTreeMap<String, u64>,
    /// Per-state session counts; all five keys always present.
    pub session_states: BTreeMap<String, u64>,
    /// Recent drop-log entries (last 100). Populated by the trim (§5.5, BON-69).
    pub drop_log: Vec<DropLogEntry>,
    /// Discovered original team strings from the bus (unsanitized, sorted).
    pub discovered_teams: Vec<String>,
    /// Total JSONL bytes under `data_dir` (the `max_mb` cap denominator,
    /// §5.5 step 1). `MANIFEST.json`, `CHECKPOINT` and `.lock` are not JSONL
    /// and never count.
    pub bytes_used: u64,
    /// The effective cap in MB (normalized per §2).
    pub max_mb: u64,
}

impl Default for Manifest {
    fn default() -> Self {
        let mut session_states = BTreeMap::new();
        for s in [
            crate::sessions::STATE_COMPLETED,
            crate::sessions::STATE_EXPIRED,
            crate::sessions::STATE_INTERRUPTED,
            crate::sessions::STATE_OPEN,
            crate::sessions::STATE_ORPHAN_FINISH,
        ] {
            session_states.insert(s.to_string(), 0u64);
        }
        Manifest {
            plugin_version: env!("CARGO_PKG_VERSION").to_string(),
            redis_url: String::new(),
            stream: String::new(),
            checkpoint: None,
            last_flush_stream_id: None,
            event_count: 0,
            session_count: 0,
            per_dt_bytes: BTreeMap::new(),
            session_states,
            drop_log: Vec::new(),
            discovered_teams: Vec::new(),
            bytes_used: 0,
            max_mb: 0,
        }
    }
}

/// Strip userinfo from a `redis://` URL (§5.4: never store the password).
///
/// `redis://:s3cr3t@127.0.0.1:6380` → `redis://127.0.0.1:6380`.
/// Anything after the last `@` in the authority is kept; a URL with no
/// userinfo is returned unchanged; an unparsable URL is returned unchanged
/// (never panics).
pub fn redact_redis_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after = &url[scheme_end + 3..];
    let authority_end = after.find(['/', '?', '#']).unwrap_or(after.len());
    let authority = &after[..authority_end];
    match authority.rfind('@') {
        Some(at) => {
            let host = &authority[at + 1..];
            format!(
                "{}{}{}",
                &url[..scheme_end + 3],
                host,
                &after[authority_end..]
            )
        }
        None => url.to_string(),
    }
}

/// One office `raw/dt=…/events.jsonl` file's measured state.
struct OfficeRaw {
    date: String,
    bytes: u64,
    lines: u64,
}

/// Walk `data_dir/raw/dt=…/events.jsonl` (office view only).
fn office_raws(data_dir: &Path) -> Vec<OfficeRaw> {
    let raw = data_dir.join("raw");
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(&raw) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(dt) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("dt="))
            .map(|s| s.to_string())
        else {
            continue;
        };
        let file = path.join("events.jsonl");
        let Ok(meta) = std::fs::metadata(&file) else {
            continue;
        };
        let lines = std::fs::read_to_string(&file)
            .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count() as u64)
            .unwrap_or(0);
        out.push(OfficeRaw {
            date: dt,
            bytes: meta.len(),
            lines,
        });
    }
    out.sort_by(|a, b| a.date.cmp(&b.date));
    out
}

/// Sum of every `*.jsonl` byte under `data_dir` (§5.5 step 1 denominator).
/// `MANIFEST.json`, `CHECKPOINT` and `.lock` are not `*.jsonl` and never count.
fn jsonl_bytes(data_dir: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
                total += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// Collect the manifest from on-disk state (never touches Redis, never takes
/// the single-writer lock — `wfdc status` must work while a collector runs).
pub fn collect(cfg: &Config) -> Result<Manifest, Error> {
    let mut m = Manifest {
        redis_url: redact_redis_url(&cfg.redis_url),
        stream: cfg.stream.clone(),
        max_mb: cfg.max_mb,
        ..Manifest::default()
    };

    if !cfg.data_dir.is_dir() {
        return Ok(m);
    }

    m.checkpoint = checkpoint::read(&cfg.data_dir)?;

    // last-flush = max(durable CHECKPOINT, highest id actually on disk), so
    // the crash-window gap (rows written ahead of CHECKPOINT) is visible.
    let max_written = writer::max_written_stream_id(&cfg.data_dir)?;
    m.last_flush_stream_id = match (&m.checkpoint, &max_written) {
        (Some(cp), Some(w)) => {
            let cp_t = checkpoint::parse_id(cp);
            let w_t = checkpoint::parse_id(w);
            match (cp_t, w_t) {
                (Some(c), Some(wt)) if wt > c => Some(w.clone()),
                _ => Some(cp.clone()),
            }
        }
        (Some(cp), None) => Some(cp.clone()),
        (None, Some(w)) => Some(w.clone()),
        (None, None) => None,
    };

    // Office raw: event count + per-dt= bytes (MAN-4).
    let mut teams = std::collections::BTreeSet::new();
    for r in office_raws(&cfg.data_dir) {
        m.event_count += r.lines;
        m.per_dt_bytes.insert(r.date.clone(), r.bytes);
    }

    // Sessions: per-state counts + session_count + original team strings.
    let mut store = SessionStore::new();
    store.load(&cfg.data_dir)?;
    for row in store.all() {
        m.session_count += 1;
        if let Some(n) = m.session_states.get_mut(&row.state) {
            *n += 1;
        } else {
            m.session_states.insert(row.state.clone(), 1);
        }
        if let Some(t) = row.team.as_deref() {
            if !t.trim().is_empty() {
                teams.insert(t.trim().to_string());
            }
        }
    }

    // Discovered original team strings also from the raw lines themselves
    // (events without a session row still carry the bus `team` string).
    for line in raw_team_strings(&cfg.data_dir) {
        teams.insert(line);
    }
    m.discovered_teams = teams.into_iter().collect();

    m.bytes_used = jsonl_bytes(&cfg.data_dir);
    Ok(m)
}

/// Unique non-empty `team` strings found in office raw lines (unsanitized).
fn raw_team_strings(data_dir: &Path) -> Vec<String> {
    let mut out = std::collections::BTreeSet::new();
    let raw = data_dir.join("raw");
    let Ok(entries) = std::fs::read_dir(&raw) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let file = path.join("events.jsonl");
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in text.lines() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(t) = v.get("team").and_then(|t| t.as_str()) {
                    if !t.trim().is_empty() {
                        out.insert(t.trim().to_string());
                    }
                }
            }
        }
    }
    out.into_iter().collect()
}

/// Atomic write of `MANIFEST.json`: `*.tmp` + fsync + rename, mode 0600 (§2).
fn atomic_write(data_dir: &Path, content: &[u8]) -> Result<(), Error> {
    writer::ensure_0700(data_dir)?;
    let final_path = data_dir.join(MANIFEST_FILE);
    let tmp = data_dir.join(format!("{MANIFEST_FILE}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    Ok(())
}

/// Collect and rewrite `MANIFEST.json`. Called after every successful flush
/// (follow per batch, backfill once per run) — §5.4 "Rewritten each flush".
pub fn write(cfg: &Config) -> Result<(), Error> {
    let m = collect(cfg)?;
    let json = serde_json::to_string(&m)?;
    atomic_write(&cfg.data_dir, json.as_bytes())
}

/// `wfdc status` (human lines) / `wfdc status --json` (single JSON document
/// with stable key order, no trailing prose). Read-only: never acquires the
/// single-writer lock, so it works while a collector is running.
pub fn status(cfg: &Config, json: bool) -> Result<(), Error> {
    let m = collect(cfg)?;
    if json {
        println!("{}", serde_json::to_string(&m)?);
    } else {
        for line in human_lines(&m) {
            println!("{line}");
        }
    }
    Ok(())
}

/// Human `status`: the same fields as `key: value` lines (STJ-2).
pub fn human_lines(m: &Manifest) -> Vec<String> {
    let mut out = Vec::new();
    out.push(format!("plugin_version: {}", m.plugin_version));
    out.push(format!("redis_url: {}", m.redis_url));
    out.push(format!("stream: {}", m.stream));
    out.push(format!(
        "checkpoint: {}",
        m.checkpoint.as_deref().unwrap_or("(none)")
    ));
    out.push(format!(
        "last_flush_stream_id: {}",
        m.last_flush_stream_id.as_deref().unwrap_or("(none)")
    ));
    out.push(format!("event_count: {}", m.event_count));
    out.push(format!("session_count: {}", m.session_count));
    for (date, bytes) in &m.per_dt_bytes {
        out.push(format!("per_dt_bytes.{date}: {bytes}"));
    }
    for (state, count) in &m.session_states {
        out.push(format!("session_states.{state}: {count}"));
    }
    for (i, e) in m.drop_log.iter().enumerate() {
        out.push(format!("drop_log.{i}.when: {}", e.when));
        out.push(format!("drop_log.{i}.scope: {}", e.scope));
        if let Some(d) = &e.date {
            out.push(format!("drop_log.{i}.date: {d}"));
        }
        if let Some(s) = &e.stream_id {
            out.push(format!("drop_log.{i}.stream_id: {s}"));
        }
        out.push(format!("drop_log.{i}.bytes_freed: {}", e.bytes_freed));
    }
    for (i, team) in m.discovered_teams.iter().enumerate() {
        out.push(format!("discovered_teams.{i}: {team}"));
    }
    out.push(format!("bytes_used: {}", m.bytes_used));
    out.push(format!("max_mb: {}", m.max_mb));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-manifest-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    // --- redaction (MAN-2 adversarial) ------------------------------------

    #[test]
    fn redact_strips_password_userinfo() {
        assert_eq!(
            redact_redis_url("redis://:s3cr3t@127.0.0.1:6380"),
            "redis://127.0.0.1:6380"
        );
        assert_eq!(
            redact_redis_url("redis://user:***@host:port"),
            "redis://host:port"
        );
        assert_eq!(
            redact_redis_url("redis://user:pass@host:6379/0"),
            "redis://host:6379/0"
        );
    }

    #[test]
    fn redact_leaves_no_userinfo_urls_unchanged() {
        assert_eq!(
            redact_redis_url("redis://127.0.0.1:6380"),
            "redis://127.0.0.1:6380"
        );
        assert_eq!(
            redact_redis_url("redis://127.0.0.1:6380/2"),
            "redis://127.0.0.1:6380/2"
        );
        assert_eq!(redact_redis_url("redis://[::1]:6380"), "redis://[::1]:6380");
    }

    #[test]
    fn redact_never_returns_password() {
        for url in [
            "redis://:hunter2@127.0.0.1:6380",
            "redis://u:hunter2@h:1",
            "redis://hunter2@h:1",
        ] {
            let out = redact_redis_url(url);
            assert!(!out.contains("hunter2"), "{url} → {out}");
        }
    }

    #[test]
    fn redact_unparsable_url_is_unchanged() {
        assert_eq!(redact_redis_url("not a url"), "not a url");
        assert_eq!(redact_redis_url(""), "");
    }

    // --- serialization ------------------------------------------------------

    #[test]
    fn serializes_with_stable_key_order() {
        let m = Manifest::default();
        let s = serde_json::to_string(&m).unwrap();
        // Exact document: stable key order, no trailing prose, five states.
        assert_eq!(
            s,
            r#"{"plugin_version":"0.3.0","redis_url":"","stream":"","checkpoint":null,"last_flush_stream_id":null,"event_count":0,"session_count":0,"per_dt_bytes":{},"session_states":{"completed":0,"expired":0,"interrupted":0,"open":0,"orphan_finish":0},"drop_log":[],"discovered_teams":[],"bytes_used":0,"max_mb":0}"#
        );
    }

    #[test]
    fn drop_log_entry_omits_irrelevant_field() {
        let e = DropLogEntry {
            when: "2026-08-31T07:00:00Z".into(),
            scope: "today".into(),
            date: None,
            stream_id: Some("1725062400000-0".into()),
            bytes_freed: 88,
        };
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"when":"2026-08-31T07:00:00Z","scope":"today","stream_id":"1725062400000-0","bytes_freed":88}"#
        );
    }

    // --- write --------------------------------------------------------------

    #[test]
    fn write_creates_0600_manifest_atomically() {
        let dir = tmpdir("write");
        let cfg = Config {
            redis_url: "redis://:pw@127.0.0.1:6380".into(),
            stream: "office:events".into(),
            data_dir: dir.clone(),
            max_mb: 500,
            expire_hours: 6,
        };
        write(&cfg).unwrap();
        let p = dir.join(MANIFEST_FILE);
        assert!(p.exists());
        assert!(!dir.join(format!("{MANIFEST_FILE}.tmp")).exists());
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "MANIFEST.json 0600 (MAN-6)");
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert_eq!(v["redis_url"], "redis://127.0.0.1:6380", "redacted in file");
        assert_eq!(v["max_mb"], 500);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- collect on a hand-built tree ---------------------------------------

    #[test]
    fn collect_counts_bytes_excluding_manifest_checkpoint_lock() {
        let dir = tmpdir("collect");
        writer::ensure_data_dir(&dir).unwrap();
        std::fs::create_dir_all(dir.join("raw/dt=2026-08-31")).unwrap();
        std::fs::write(
            dir.join("raw/dt=2026-08-31/events.jsonl"),
            "{\"stream_id\":\"1-0\",\"team\":\"dev-1\"}\n",
        )
        .unwrap();
        // Non-JSONL files must never count toward bytes_used (§5.5).
        std::fs::write(dir.join("MANIFEST.json"), "{\"bytes_used\":999999999}").unwrap();
        std::fs::write(dir.join("CHECKPOINT"), "1-0\n").unwrap();
        std::fs::write(dir.join(".lock"), "1 1\n").unwrap();

        let cfg = Config {
            redis_url: "redis://127.0.0.1:6380".into(),
            stream: "office:events".into(),
            data_dir: dir.clone(),
            max_mb: 500,
            expire_hours: 6,
        };
        let m = collect(&cfg).unwrap();
        let f_size = std::fs::metadata(dir.join("raw/dt=2026-08-31/events.jsonl"))
            .unwrap()
            .len();
        assert_eq!(m.bytes_used, f_size);
        assert_eq!(m.event_count, 1);
        assert_eq!(m.last_flush_stream_id.as_deref(), Some("1-0"));
        assert_eq!(m.per_dt_bytes.get("2026-08-31"), Some(&f_size));
        assert!(m.discovered_teams.contains(&"dev-1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn human_lines_cover_every_field() {
        let m = Manifest {
            checkpoint: Some("5-0".into()),
            per_dt_bytes: BTreeMap::from([("2026-08-31".to_string(), 28)]),
            session_states: BTreeMap::from([("completed".to_string(), 1)]),
            drop_log: vec![DropLogEntry {
                when: "2026-08-31T07:00:00Z".into(),
                scope: "today".into(),
                date: None,
                stream_id: Some("1-0".into()),
                bytes_freed: 10,
            }],
            ..Manifest::default()
        };
        let lines = human_lines(&m);
        let joined = lines.join("\n");
        assert!(joined.contains("plugin_version: "));
        assert!(joined.contains("checkpoint: 5-0"));
        assert!(joined.contains("per_dt_bytes.2026-08-31: 28"));
        assert!(joined.contains("session_states.completed: 1"));
        assert!(joined.contains("drop_log.0.stream_id: 1-0"));
        assert!(joined.contains("max_mb: 0"));
    }
}
