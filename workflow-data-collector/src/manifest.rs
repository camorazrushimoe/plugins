//! MANIFEST.json (§5.4) + `wfdc status` / `wfdc status --json`.
//!
//! The manifest is the machine-readable observability document: rewritten
//! after every successful flush (follow per batch + at start; backfill once
//! per run), and printed verbatim by `wfdc status --json`. It never contains
//! the Redis password — the URL is stored with userinfo stripped (§5.4).
//!
//! Stable key order is a contract: `wfdc status --json` prints one JSON
//! document whose keys appear in the same order every time, and `wfdc status`
//! prints the same fields as `key: value` lines, so staging checks and tests
//! can assert on either surface.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::checkpoint;
use crate::config::Config;
use crate::drop_log;
use crate::layout;
use crate::sessions;
use crate::Error;

pub const MANIFEST_FILE: &str = "MANIFEST.json";

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
    /// Recent drop-log entries (last 100), from the persisted ring
    /// `data_dir/DROP_LOG.json` (empty array when the file is absent).
    pub drop_log: Vec<drop_log::DropLogEntry>,
    /// Discovered original team strings from the bus (unsanitized, sorted).
    pub discovered_teams: Vec<String>,
    /// Total JSONL bytes under `data_dir` (the `max_mb` cap denominator,
    /// §5.5 step 1). `MANIFEST.json`, `CHECKPOINT`, `.lock` and
    /// `DROP_LOG.json` are not JSONL and never count.
    pub bytes_used: u64,
    /// The effective cap in MB (normalized per §2).
    pub max_mb: u64,
}

/// All five session states, always present as keys (0 default) in stable
/// (alphabetical via BTreeMap) order: completed, expired, interrupted, open,
/// orphan_finish.
const ALL_STATES: [sessions::State; 5] = [
    sessions::State::Completed,
    sessions::State::Expired,
    sessions::State::Interrupted,
    sessions::State::Open,
    sessions::State::OrphanFinish,
];

/// `sessions::State` → manifest key string (serde snake_case, §5.3).
fn state_key(s: sessions::State) -> &'static str {
    match s {
        sessions::State::Completed => "completed",
        sessions::State::Open => "open",
        sessions::State::Interrupted => "interrupted",
        sessions::State::OrphanFinish => "orphan_finish",
        sessions::State::Expired => "expired",
    }
}

/// `drop_log::Scope` → manifest `scope` string (§5.4: `date` | `today`).
fn scope_str(s: drop_log::Scope) -> &'static str {
    match s {
        drop_log::Scope::Date => "date",
        drop_log::Scope::Today => "today",
    }
}

impl Default for Manifest {
    fn default() -> Self {
        let mut session_states = BTreeMap::new();
        for s in ALL_STATES {
            session_states.insert(state_key(s).to_string(), 0u64);
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
/// Anything after the last `@` inside the authority is kept; a URL with no
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

/// The crash-window gap: the highest `stream_id` actually written to the raw
/// JSONL dataset. Mirrors `raw::Store::max_written_stream_id` (lines without
/// a valid `stream_id` are skipped; a strictly-greater id replaces the best)
/// but computed read-only from the scan's raw views — office + team. It never
/// calls `Store::open`, whose `ensure_dir_0700` would create `raw/`+`teams/`
/// and chmod an existing `data_dir`: `wfdc status` must not mutate the tree.
fn max_written_stream_id(tree: &layout::DataDir) -> Option<String> {
    let mut best: Option<(crate::streamid::StreamId, String)> = None;
    for view in &tree.raw_views {
        for line in &view.lines {
            let Some(raw) = line.stream_id.as_deref() else {
                continue;
            };
            let Some(parsed) = crate::streamid::StreamId::parse(raw) else {
                continue;
            };
            let better = best.as_ref().is_none_or(|(cur, _)| parsed > *cur);
            if better {
                best = Some((parsed, raw.to_string()));
            }
        }
    }
    best.map(|(_, raw)| raw)
}

/// Map the durable CHECKPOINT ("0" = fresh) + highest written id onto the
/// manifest's `checkpoint` / `last_flush_stream_id` fields.
///
/// The crash-window rule is identical to `checkpoint::resume_start`:
/// last_flush = max(durable CHECKPOINT, highest id written to JSONL), with
/// the "0" sentinel mapped to `None` (a fresh/missing CHECKPOINT is null).
fn checkpoint_fields(
    data_dir: &Path,
    tree: &layout::DataDir,
) -> Result<(Option<String>, Option<String>), Error> {
    let durable = checkpoint::read(data_dir)?;
    let cp = if durable == "0" {
        None
    } else {
        Some(durable.clone())
    };
    // Read-only: the highest written id comes from the scan, never from
    // `Store::open` (which creates dirs / chmods — see `max_written_stream_id`).
    let max_written = max_written_stream_id(tree);
    let last = checkpoint::resume_start(&durable, max_written.as_deref());
    let last_flush = if last == "0" { None } else { Some(last) };
    Ok((cp, last_flush))
}

/// Collect the manifest from on-disk state. Never touches Redis, never takes
/// the single-writer lock, never runs startup repair, never writes the tree —
/// `wfdc status` must work while a collector holds the lock, and a missing
/// `data_dir` yields the default manifest without creating the directory
/// (or `raw/`/`teams/` inside an existing one) (§5.4).
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

    // Disk scan first (read-only): office raw event counts + per-dt= bytes,
    // the §5.5 bytes_used denominator (every *.jsonl under data_dir), and the
    // crash-window gap (highest written stream id across the raw views).
    let tree = layout::scan(&cfg.data_dir).map_err(|e| Error::Io(format!("scan data_dir: {e}")))?;
    let (cp, last_flush) = checkpoint_fields(&cfg.data_dir, &tree)?;
    m.checkpoint = cp;
    m.last_flush_stream_id = last_flush;

    m.bytes_used = tree.jsonl_bytes();

    let mut teams = BTreeSet::new();
    for view in &tree.raw_views {
        if view.kind != layout::ViewKind::OfficeRaw {
            continue;
        }
        m.event_count += view.lines.len() as u64;
        m.per_dt_bytes.insert(view.date.clone(), view.bytes);
        // Discovered original team strings from the raw lines themselves
        // (events without a session row still carry the bus `team` string).
        for line in &view.lines {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&line.bytes) {
                if let Some(t) = v.get("team").and_then(|t| t.as_str()) {
                    if !t.trim().is_empty() {
                        teams.insert(t.trim().to_string());
                    }
                }
            }
        }
    }

    // Sessions: per-state counts + session_count + original team strings.
    let rows = sessions::SessionStore::new(&cfg.data_dir).load_all()?;
    for row in &rows {
        m.session_count += 1;
        let key = state_key(row.state);
        *m.session_states.entry(key.to_string()).or_insert(0) += 1;
        if !row.team.trim().is_empty() {
            teams.insert(row.team.trim().to_string());
        }
    }

    // Drop log: the persisted ring (last 100; empty array when no file).
    m.drop_log = drop_log::load(&cfg.data_dir)?
        .entries()
        .iter()
        .cloned()
        .collect();

    m.discovered_teams = teams.into_iter().collect();
    Ok(m)
}

/// Atomic write of `MANIFEST.json`: `*.tmp` + fsync + rename, mode 0600 (§2).
fn atomic_write(data_dir: &Path, content: &[u8]) -> Result<(), Error> {
    let final_path = data_dir.join(MANIFEST_FILE);
    let tmp = data_dir.join(format!("{MANIFEST_FILE}.tmp"));
    let result = (|| -> Result<(), Error> {
        let mut f = crate::fsutil::open_private(&tmp, false)?;
        f.write_all(content)
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
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Collect and rewrite `MANIFEST.json`. Called after every successful flush
/// (follow per batch + at start; backfill once per run) — §5.4
/// "Rewritten each flush".
pub fn write(cfg: &Config) -> Result<(), Error> {
    let m = collect(cfg)?;
    let json =
        serde_json::to_string(&m).map_err(|e| Error::Fatal(format!("serialize manifest: {e}")))?;
    atomic_write(&cfg.data_dir, json.as_bytes())
}

/// `wfdc status` (human lines) / `wfdc status --json` (single JSON document
/// with stable key order, no trailing prose). Read-only: never acquires the
/// single-writer lock, so it works while a collector is running.
pub fn status(cfg: &Config, json: bool) -> Result<(), Error> {
    let m = collect(cfg)?;
    if json {
        let s = serde_json::to_string(&m)
            .map_err(|e| Error::Fatal(format!("serialize manifest: {e}")))?;
        println!("{s}");
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
        out.push(format!("drop_log.{i}.scope: {}", scope_str(e.scope)));
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
    use crate::sessions::{SessionRow, State};
    use std::os::unix::fs::PermissionsExt;

    fn cfg_for(dir: &std::path::Path, max_mb: u64) -> Config {
        Config {
            redis_url: "redis://127.0.0.1:6380".into(),
            stream: "office:events".into(),
            data_dir: dir.to_path_buf(),
            max_mb,
            expire_hours: 6,
        }
    }

    // --- redaction (MAN-2 adversarial) --------------------------------------

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
            redact_redis_url("redis://user:***@host:6379/0"),
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
            "redis://u:***@h:1",
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

    // --- serialization -------------------------------------------------------

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
    fn default_has_all_five_session_state_keys() {
        let m = Manifest::default();
        assert_eq!(m.session_states.len(), 5);
        for k in [
            "completed",
            "expired",
            "interrupted",
            "open",
            "orphan_finish",
        ] {
            assert_eq!(m.session_states.get(k), Some(&0), "key {k} present as 0");
        }
    }

    // --- write ---------------------------------------------------------------

    #[test]
    fn write_creates_0600_manifest_atomically_and_redacts() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            redis_url: "redis://:pw@127.0.0.1:6380".into(),
            stream: "office:events".into(),
            data_dir: dir.path().to_path_buf(),
            max_mb: 500,
            expire_hours: 6,
        };
        write(&cfg).unwrap();
        let p = dir.path().join(MANIFEST_FILE);
        assert!(p.exists());
        assert!(!dir.path().join(format!("{MANIFEST_FILE}.tmp")).exists());
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "MANIFEST.json must be 0600 (§2)");
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert_eq!(v["redis_url"], "redis://127.0.0.1:6380", "redacted in file");
        assert_eq!(v["max_mb"], 500);
    }

    #[test]
    fn write_roundtrips_through_collect() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 500);
        write(&cfg).unwrap();
        let again = collect(&cfg).unwrap();
        assert_eq!(again.plugin_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(again.redis_url, "redis://127.0.0.1:6380");
        assert_eq!(again.stream, "office:events");
        assert_eq!(again.max_mb, 500);
        // nothing on disk → all counts zero, no sessions/teams/drop log
        assert_eq!(again.event_count, 0);
        assert_eq!(again.session_count, 0);
        assert!(again.per_dt_bytes.is_empty());
        assert!(again.drop_log.is_empty());
        assert!(again.discovered_teams.is_empty());
        assert_eq!(again.bytes_used, 0);
    }

    // --- collect on a hand-built tree ----------------------------------------

    #[test]
    fn collect_on_missing_data_dir_is_default() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::remove_dir(dir.path()).unwrap();
        let cfg = cfg_for(dir.path(), 500);
        let m = collect(&cfg).unwrap();
        assert_eq!(m.event_count, 0);
        assert_eq!(m.redis_url, "redis://127.0.0.1:6380");
        assert_eq!(m.stream, "office:events");
        assert_eq!(m.max_mb, 500);
        assert!(m.checkpoint.is_none(), "no dir → no checkpoint");
        assert!(m.last_flush_stream_id.is_none());
        // and it must NOT have created the dir (read-only contract)
        assert!(!dir.path().exists(), "collect must never create data_dir");
    }

    #[test]
    fn collect_on_pristine_existing_dir_creates_no_subdirs() {
        // An existing-but-empty data_dir must stay untouched: the read-only
        // contract covers raw/ + teams/ too (Store::open would create and
        // chmod them — collect must not call it).
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 500);
        let m = collect(&cfg).unwrap();
        assert_eq!(m.event_count, 0);
        assert!(m.checkpoint.is_none());
        assert!(m.last_flush_stream_id.is_none());
        assert!(!dir.path().join("raw").exists(), "must not create raw/");
        assert!(!dir.path().join("teams").exists(), "must not create teams/");
        assert!(
            !dir.path().join(MANIFEST_FILE).exists(),
            "collect never writes"
        );
    }

    #[test]
    fn collect_counts_events_dt_bytes_and_teams_excluding_non_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        crate::raw::Store::open(dir.path()).unwrap();
        std::fs::create_dir_all(dir.path().join("raw/dt=2026-08-31")).unwrap();
        std::fs::write(
            dir.path().join("raw/dt=2026-08-31/events.jsonl"),
            "{\"stream_id\":\"1-0\",\"team\":\"dev-1\"}\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("raw/dt=2026-09-01")).unwrap();
        std::fs::write(
            dir.path().join("raw/dt=2026-09-01/events.jsonl"),
            "{\"stream_id\":\"2-0\",\"team\":\"Dev Team/1\"}\n",
        )
        .unwrap();
        // Non-JSONL files must never count toward bytes_used (§5.5).
        std::fs::write(
            dir.path().join("MANIFEST.json"),
            "{\"bytes_used\":999999999}",
        )
        .unwrap();
        std::fs::write(dir.path().join("CHECKPOINT"), "2-0\n").unwrap();
        std::fs::write(dir.path().join(".lock"), "1 1\n").unwrap();
        std::fs::write(
            dir.path().join("DROP_LOG.json"),
            "[{\"when\":\"w\",\"scope\":\"today\",\"stream_id\":\"9-9\",\"bytes_freed\":1}]",
        )
        .unwrap();

        let cfg = cfg_for(dir.path(), 500);
        let m = collect(&cfg).unwrap();
        let f31 = std::fs::metadata(dir.path().join("raw/dt=2026-08-31/events.jsonl"))
            .unwrap()
            .len();
        let f01 = std::fs::metadata(dir.path().join("raw/dt=2026-09-01/events.jsonl"))
            .unwrap()
            .len();
        assert_eq!(m.event_count, 2);
        assert_eq!(m.bytes_used, f31 + f01, "only *.jsonl counts");
        assert_eq!(m.per_dt_bytes.get("2026-08-31"), Some(&f31));
        assert_eq!(m.per_dt_bytes.get("2026-09-01"), Some(&f01));
        assert_eq!(m.checkpoint.as_deref(), Some("2-0"));
        assert_eq!(m.last_flush_stream_id.as_deref(), Some("2-0"));
        // discovered teams from raw lines, unsanitized + sorted
        assert_eq!(
            m.discovered_teams,
            vec!["Dev Team/1".to_string(), "dev-1".to_string()]
        );
        // drop_log loaded from the persisted ring (unsanitized shape)
        assert_eq!(m.drop_log.len(), 1);
        assert_eq!(m.drop_log[0].stream_id.as_deref(), Some("9-9"));
    }

    fn sess_row(pk: &str, team: &str, state: State) -> SessionRow {
        SessionRow {
            session_pk: pk.into(),
            team: team.into(),
            actor: "dev".into(),
            session_id: None,
            start_stream_id: Some("1-0".into()),
            finish_stream_id: None,
            started_at: Some("2026-08-30T10:00:00Z".into()),
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

    #[test]
    fn collect_counts_all_five_states_from_hand_built_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("raw/dt=2026-08-30")).unwrap();
        std::fs::write(
            dir.path().join("raw/dt=2026-08-30/events.jsonl"),
            "{\"stream_id\":\"1-0\",\"team\":\"dev-1\"}\n",
        )
        .unwrap();
        let rows = [
            sess_row("pk-c", "dev-1", State::Completed),
            sess_row("pk-e", "dev-1", State::Expired),
            sess_row("pk-i", "dev-1", State::Interrupted),
            sess_row("pk-o", "dev-1", State::Open),
            sess_row("pk-f", "dev-1", State::OrphanFinish),
        ];
        let store = crate::sessions::SessionStore::new(dir.path());
        let writes: Vec<_> = rows
            .iter()
            .map(|r| {
                let (folder, dt) = r.location();
                crate::sessions::SessionWrite {
                    team_folder: folder,
                    dt,
                    rows: vec![r.clone()],
                }
            })
            .collect();
        store.upsert(&writes).unwrap();
        let cfg = cfg_for(dir.path(), 500);
        let m = collect(&cfg).unwrap();
        assert_eq!(m.session_count, 5);
        for (k, want) in [
            ("completed", 1u64),
            ("expired", 1),
            ("interrupted", 1),
            ("open", 1),
            ("orphan_finish", 1),
        ] {
            assert_eq!(m.session_states.get(k), Some(&want), "state {k}");
        }
        assert!(m.discovered_teams.contains(&"dev-1".to_string()));
    }

    // --- human_lines ----------------------------------------------------------

    #[test]
    fn human_lines_cover_every_field() {
        let m = Manifest {
            checkpoint: Some("5-0".into()),
            per_dt_bytes: BTreeMap::from([("2026-08-31".to_string(), 28)]),
            session_states: BTreeMap::from([("completed".to_string(), 1)]),
            drop_log: vec![crate::drop_log::DropLogEntry::event(
                "2026-08-31T07:00:00Z",
                "1-0",
                10,
            )],
            ..Manifest::default()
        };
        let joined = human_lines(&m).join("\n");
        assert!(joined.contains("plugin_version: "));
        assert!(joined.contains("checkpoint: 5-0"));
        assert!(joined.contains("last_flush_stream_id: (none)"));
        assert!(joined.contains("per_dt_bytes.2026-08-31: 28"));
        assert!(joined.contains("session_states.completed: 1"));
        assert!(joined.contains("drop_log.0.scope: today"));
        assert!(joined.contains("drop_log.0.stream_id: 1-0"));
        assert!(joined.contains("bytes_used: 0"));
        assert!(joined.contains("max_mb: 0"));
    }
}
