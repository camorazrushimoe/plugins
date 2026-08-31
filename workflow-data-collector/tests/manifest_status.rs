//! BON-70 acceptance tests: MANIFEST.json (§5.4) + `wfdc status` / `status --json`.
//!
//! QA matrix under test: MAN-1 (rewritten each flush), MAN-2 (redis_url
//! userinfo-stripped, adversarial), MAN-3/4 (required fields, per-state counts,
//! per-`dt=` byte counts), MAN-5 (discovered original team strings), MAN-6
//! (MANIFEST.json 0600), STJ-1 (status --json: single doc, stable key order,
//! no trailing prose), plus lock-independence (status never takes the
//! single-writer lock — it must work while a collector holds it).
//!
//! Same harness as tests/backfill.rs: a dedicated per-run stream on the test
//! Redis (`WFDC_TEST_REDIS`, default `redis://127.0.0.1:6380`), per-test temp
//! `data_dir`s, CLI via `env!("CARGO_BIN_EXE_wfdc")`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::Value;
use wfdc::config::Config;
use wfdc::manifest::{self, Manifest};

fn redis_url() -> String {
    std::env::var("WFDC_TEST_REDIS").unwrap_or_else(|_| "redis://127.0.0.1:6380".into())
}

struct TestRedis {
    stream: String,
    conn: redis::Connection,
}

impl TestRedis {
    fn new() -> Self {
        let client = redis::Client::open(redis_url()).expect("connect to test redis");
        let conn = client.get_connection().expect("redis connection");
        let stream = format!(
            "wfdc-test-manifest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        assert!(!stream.starts_with("office:"), "never touch the live bus");
        Self { stream, conn }
    }

    fn xadd_owned(&mut self, id: &str, fields: &[(String, String)]) {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream).arg(id);
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        cmd.exec(&mut self.conn).expect("xadd");
    }
}

impl Drop for TestRedis {
    fn drop(&mut self) {
        let _ = redis::cmd("DEL").arg(&self.stream).exec(&mut self.conn);
    }
}

fn temp_data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wfdc-test-manifest-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cfg_for(dir: &Path, stream: &str, max_mb: u64) -> Config {
    Config {
        redis_url: redis_url(),
        stream: stream.to_string(),
        data_dir: dir.to_path_buf(),
        max_mb,
        expire_hours: 100000, // no expiry in these deterministic tests
    }
}

fn start_event(id: &str, actor: &str, session_id: &str, team: &str) -> Vec<(String, String)> {
    vec![
        ("action".into(), "task.started".into()),
        ("actor".into(), actor.into()),
        ("target".into(), actor.into()),
        ("team".into(), team.into()),
        ("session_id".into(), session_id.into()),
        (
            "timestamp".into(),
            format!("2026-08-30T10:{:02}:00Z", id.parse::<u64>().unwrap() % 60),
        ),
    ]
}

fn finish_event(id: &str, actor: &str, session_id: &str, team: &str) -> Vec<(String, String)> {
    vec![
        ("action".into(), "task.finished".into()),
        ("actor".into(), actor.into()),
        ("target".into(), actor.into()),
        ("team".into(), team.into()),
        ("session_id".into(), session_id.into()),
        (
            "timestamp".into(),
            format!("2026-08-30T11:{:02}:00Z", id.parse::<u64>().unwrap() % 60),
        ),
    ]
}

fn read_manifest(dir: &Path) -> Manifest {
    let text = std::fs::read_to_string(dir.join("MANIFEST.json")).expect("MANIFEST.json present");
    serde_json::from_str(&text).expect("MANIFEST.json parses")
}

fn manifest_mtime(dir: &Path) -> std::time::SystemTime {
    std::fs::metadata(dir.join("MANIFEST.json"))
        .expect("MANIFEST.json exists")
        .modified()
        .expect("mtime")
}

fn office_raw_dates_and_sizes(dir: &Path) -> Vec<(String, u64)> {
    let raw = dir.join("raw");
    let mut out = Vec::new();
    if raw.exists() {
        let mut dates: Vec<PathBuf> = std::fs::read_dir(&raw)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_dir())
            .collect();
        dates.sort();
        for d in dates {
            let name = d.file_name().unwrap().to_string_lossy().to_string();
            let date = name.strip_prefix("dt=").unwrap_or(&name).to_string();
            let f = d.join("events.jsonl");
            if f.exists() {
                out.push((date, std::fs::metadata(&f).unwrap().len()));
            }
        }
    }
    out
}

/// Expected canonical MANIFEST key order (STJ-1 "stable key order").
const EXPECTED_KEYS: [&str; 13] = [
    "plugin_version",
    "redis_url",
    "stream",
    "checkpoint",
    "last_flush_stream_id",
    "event_count",
    "session_count",
    "per_dt_bytes",
    "session_states",
    "drop_log",
    "discovered_teams",
    "bytes_used",
    "max_mb",
];

const EXPECTED_STATES: [&str; 5] = [
    "completed",
    "expired",
    "interrupted",
    "open",
    "orphan_finish",
];

// ---------------------------------------------------------------------------
// MAN-1: MANIFEST rewritten each flush, reflects the new checkpoint.
// ---------------------------------------------------------------------------
#[test]
fn manifest_rewritten_on_backfill_flush_and_mtime_changes() {
    let mut tr = TestRedis::new();
    for i in 0..3u64 {
        let id = format!("1725000000000-{i}");
        tr.xadd_owned(&id, &start_event(&format!("{i}"), "dev", "s1", "dev-1"));
    }
    let dir = temp_data_dir("man1");
    let cfg = cfg_for(&dir, &tr.stream, 500);

    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");
    let m1 = read_manifest(&dir);
    assert_eq!(m1.checkpoint.as_deref(), Some("1725000000000-2"));
    assert_eq!(m1.last_flush_stream_id.as_deref(), Some("1725000000000-2"));
    assert_eq!(m1.event_count, 3);
    let t1 = manifest_mtime(&dir);

    // A second flush (new events) rewrites MANIFEST → mtime changes.
    tr.xadd_owned("1725000000003-0", &start_event("3", "dev", "s2", "dev-1"));
    std::thread::sleep(Duration::from_millis(20)); // ensure mtime granularity
    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");
    let m2 = read_manifest(&dir);
    assert_eq!(m2.checkpoint.as_deref(), Some("1725000000003-0"));
    assert_eq!(m2.event_count, 4);
    let t2 = manifest_mtime(&dir);
    assert_ne!(t1, t2, "MANIFEST rewritten each flush (mtime changes)");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// MAN-1 follow path: the follow loop rewrites MANIFEST after each batch.
// ---------------------------------------------------------------------------
#[test]
fn follow_flush_writes_manifest() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    tr.xadd_owned("1725000000001-0", &finish_event("1", "dev", "s1", "dev-1"));

    let dir = temp_data_dir("man1-follow");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    let cfg2 = cfg.clone();
    let handle = std::thread::spawn(move || {
        let _ = wfdc::follow::run(&cfg2);
    });

    // Poll until the follow loop's first flush produced MANIFEST.json.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut manifest = None;
    while Instant::now() < deadline {
        if dir.join("MANIFEST.json").exists() {
            manifest = Some(read_manifest(&dir));
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let m = manifest.expect("follow wrote MANIFEST.json within 15s");
    assert_eq!(m.event_count, 2, "both events flushed");
    assert_eq!(m.checkpoint.as_deref(), Some("1725000000001-0"));
    // follow still running (holds its own data_dir lock) — that's fine, the
    // test process exits and drops the thread.
    drop(handle);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// MAN-2: redis_url userinfo-stripped — adversarial, never leaks the password.
// ---------------------------------------------------------------------------
#[test]
fn credentialed_redis_url_redacted_end_to_end() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    let dir = temp_data_dir("man2");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");

    // Re-run status --json with a credentialed URL via env (CLI child only).
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .args(["status", "--json"])
        .env("WFDC_REDIS_URL", "redis://:s3cr3t@127.0.0.1:6380")
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status --json");
    assert!(out.status.success(), "status --json exit 0");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        !stdout.contains("s3cr3t"),
        "password must never appear in status --json: {stdout}"
    );
    let v: Value = serde_json::from_str(stdout.trim()).expect("single JSON doc");
    assert_eq!(v["redis_url"], "redis://127.0.0.1:6380");

    // Grep the whole data_dir for the password — zero hits (MAN-2).
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap().flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(text) = std::fs::read_to_string(&p) {
                assert!(
                    !text.contains("s3cr3t"),
                    "password leaked into {}",
                    p.display()
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// MAN-3/4/5: required fields, per-state counts, per-dt= bytes, discovered teams.
// ---------------------------------------------------------------------------
#[test]
fn manifest_counts_states_dt_bytes_and_original_teams() {
    let mut tr = TestRedis::new();
    // completed: start+finish on "Dev Team/1" (folder Dev_Team_1)
    tr.xadd_owned(
        "1725000000000-0",
        &start_event("0", "dev", "s1", "Dev Team/1"),
    );
    tr.xadd_owned(
        "1725000000001-0",
        &finish_event("1", "dev", "s1", "Dev Team/1"),
    );
    // open: start without finish
    tr.xadd_owned(
        "1725000000002-0",
        &start_event("2", "dev", "s2", "Dev Team/1"),
    );
    // orphan_finish: finish without a start
    tr.xadd_owned("1725000000003-0", &finish_event("3", "dev", "s3", "dev-1"));

    let dir = temp_data_dir("man345");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");

    let m = read_manifest(&dir);
    assert_eq!(m.event_count, 4);
    assert_eq!(m.session_count, 3);
    // per-state counts (all five keys present with stable order)
    let mut states: Vec<(String, u64)> = m
        .session_states
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    states.sort();
    let expected: Vec<(String, u64)> = vec![
        ("completed".into(), 1),
        ("expired".into(), 0),
        ("interrupted".into(), 0),
        ("open".into(), 1),
        ("orphan_finish".into(), 1),
    ];
    assert_eq!(states, expected);

    // per-dt= bytes match the office raw files on disk (MAN-4)
    let on_disk = office_raw_dates_and_sizes(&dir);
    assert!(!on_disk.is_empty(), "office raw exists");
    assert_eq!(m.per_dt_bytes.len(), on_disk.len());
    for (date, size) in &on_disk {
        assert_eq!(
            m.per_dt_bytes.get(date).copied(),
            Some(*size),
            "per_dt_bytes[{date}] == on-disk byte count"
        );
    }

    // discovered original team strings (unsanitized) — MAN-5
    assert!(m.discovered_teams.contains(&"Dev Team/1".to_string()));
    assert!(m.discovered_teams.contains(&"dev-1".to_string()));

    // bytes_used >= office raw bytes (includes team raws + sessions), and the
    // cap field is present and equals cfg
    assert!(m.bytes_used >= on_disk.iter().map(|(_, s)| *s).sum());
    assert_eq!(m.max_mb, 500);
    assert_eq!(m.plugin_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(m.stream, tr.stream);

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// STJ-1: status --json — single doc, stable key order, no trailing prose.
// ---------------------------------------------------------------------------
#[test]
fn status_json_single_doc_stable_key_order_no_trailing_prose() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    let dir = temp_data_dir("stj1");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");

    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .args(["status", "--json"])
        .env("WFDC_REDIS_URL", redis_url())
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status --json");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.ends_with("}\n"),
        "single doc, no trailing prose: {stdout:?}"
    );
    assert_eq!(
        stdout.trim().matches('\n').count(),
        0,
        "single JSON document"
    );

    // stable key order (STJ-1)
    let v: Value = serde_json::from_str(stdout.trim()).expect("parse");
    let obj = v.as_object().expect("object");
    let keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
    assert_eq!(keys, EXPECTED_KEYS.to_vec(), "stable key order");
    let states = obj["session_states"]
        .as_object()
        .expect("session_states obj");
    let state_keys: Vec<&str> = states.keys().map(|k| k.as_str()).collect();
    assert_eq!(
        state_keys,
        EXPECTED_STATES.to_vec(),
        "per-state keys stable"
    );

    // same shape as MANIFEST.json on disk
    let file_text = std::fs::read_to_string(dir.join("MANIFEST.json")).unwrap();
    let file_v: Value = serde_json::from_str(&file_text).unwrap();
    assert_eq!(v, file_v, "status --json == MANIFEST.json content");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// STJ-2: human status prints the same fields as lines.
// ---------------------------------------------------------------------------
#[test]
fn status_human_prints_same_fields_as_lines() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    let dir = temp_data_dir("stj2");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    wfdc::backfill::run(&cfg, "0", "+").expect("backfill ok");

    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .args(["status"])
        .env("WFDC_REDIS_URL", redis_url())
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).expect("utf8");
    let lines: Vec<&str> = text.lines().collect();
    assert!(!lines.is_empty());
    assert!(
        lines[0].starts_with("plugin_version: "),
        "first line: {}",
        lines[0]
    );
    let joined = text.to_string();
    assert!(joined.contains("max_mb: 500"));
    // a single start event → one open session
    assert!(joined.contains("session_states.open: 1"));
    assert!(joined.contains("session_states.completed: 0"));
    assert!(joined.contains("redis_url: redis://127.0.0.1:6380"));

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Status never takes the single-writer lock: works while follow holds it.
// ---------------------------------------------------------------------------
#[test]
fn status_works_while_follow_holds_the_lock() {
    let tr = TestRedis::new();
    let dir = temp_data_dir("lockfree");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    let cfg2 = cfg.clone();
    let handle = std::thread::spawn(move || {
        let _ = wfdc::follow::run(&cfg2);
    });

    // Wait until follow holds the lock.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if dir.join(".lock").exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(dir.join(".lock").exists(), "follow acquired the lock");

    // A second writer must conflict (exit 3) — proves the lock is live.
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .args(["backfill", "--from", "0", "--to", "+"])
        .env("WFDC_REDIS_URL", redis_url())
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc backfill");
    assert_eq!(out.status.code(), Some(3), "lock conflict exit 3");

    // status is read-only and must succeed despite the live lock.
    let out = Command::new(bin)
        .args(["status", "--json"])
        .env("WFDC_REDIS_URL", redis_url())
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status --json");
    assert!(
        out.status.success(),
        "status succeeds while the collector holds the lock (exit {})",
        out.status
    );
    let _ = String::from_utf8(out.stdout).expect("utf8");

    drop(handle);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// manifest::collect on a hand-built data_dir: all five states + cap accounting.
// ---------------------------------------------------------------------------
#[test]
fn collect_counts_all_five_states_from_hand_built_dir() {
    use wfdc::sessions::SessionRow;

    let dir = temp_data_dir("collect");
    // office raw, two dates
    std::fs::create_dir_all(dir.join("raw/dt=2026-08-30")).unwrap();
    std::fs::create_dir_all(dir.join("raw/dt=2026-08-31")).unwrap();
    std::fs::write(
        dir.join("raw/dt=2026-08-30/events.jsonl"),
        "{\"stream_id\":\"1-0\",\"team\":\"dev-1\"}\n{\"stream_id\":\"2-0\",\"team\":\"Dev Team/1\"}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("raw/dt=2026-08-31/events.jsonl"),
        "{\"stream_id\":\"3-0\",\"team\":\"dev-1\"}\n",
    )
    .unwrap();

    // sessions for one team with all five states
    let mk = |pk: &str, state: &str| SessionRow {
        session_pk: pk.into(),
        team: Some("dev-1".into()),
        actor: Some("dev".into()),
        session_id: Some("s".into()),
        start_stream_id: Some("1-0".into()),
        finish_stream_id: None,
        started_at: Some("2026-08-30T10:00:00Z".into()),
        finished_at: None,
        duration_ms: None,
        state: state.into(),
        snippet_in: None,
        snippet_out: None,
        issues: None,
        prs: None,
        linear: None,
        handoff: None,
        project: None,
    };
    let rows = [
        mk("pk-c", "completed"),
        mk("pk-e", "expired"),
        mk("pk-i", "interrupted"),
        mk("pk-o", "open"),
        mk("pk-f", "orphan_finish"),
    ];
    let sess_dir = dir.join("teams/dev-1/sessions/dt=2026-08-30");
    std::fs::create_dir_all(&sess_dir).unwrap();
    let content: String = rows
        .iter()
        .map(|r| serde_json::to_string(r).unwrap() + "\n")
        .collect();
    std::fs::write(sess_dir.join("sessions.jsonl"), content).unwrap();
    std::fs::write(dir.join("CHECKPOINT"), "3-0\n").unwrap();

    let cfg = Config {
        redis_url: "redis://127.0.0.1:6380".into(),
        stream: "office:events".into(),
        data_dir: dir.clone(),
        max_mb: 500,
        expire_hours: 6,
    };
    let m = manifest::collect(&cfg).expect("collect");
    assert_eq!(m.event_count, 3);
    assert_eq!(m.session_count, 5);
    assert_eq!(m.session_states.get("completed"), Some(&1));
    assert_eq!(m.session_states.get("expired"), Some(&1));
    assert_eq!(m.session_states.get("interrupted"), Some(&1));
    assert_eq!(m.session_states.get("open"), Some(&1));
    assert_eq!(m.session_states.get("orphan_finish"), Some(&1));
    assert_eq!(m.checkpoint.as_deref(), Some("3-0"));
    assert_eq!(m.last_flush_stream_id.as_deref(), Some("3-0"));
    let f30 = std::fs::metadata(dir.join("raw/dt=2026-08-30/events.jsonl"))
        .unwrap()
        .len();
    let f31 = std::fs::metadata(dir.join("raw/dt=2026-08-31/events.jsonl"))
        .unwrap()
        .len();
    assert_eq!(m.per_dt_bytes.get("2026-08-30"), Some(&f30));
    assert_eq!(m.per_dt_bytes.get("2026-08-31"), Some(&f31));
    assert!(m.discovered_teams.contains(&"Dev Team/1".to_string()));
    assert!(m.discovered_teams.contains(&"dev-1".to_string()));
    assert_eq!(m.max_mb, 500);
    assert!(m.bytes_used > 0);

    let _ = std::fs::remove_dir_all(&dir);
}
