//! BON-70 acceptance tests: MANIFEST.json (§5.4) + `wfdc status` / `status --json`.
//!
//! QA matrix under test:
//! - MAN-1: MANIFEST rewritten each flush (follow per batch + at start; backfill
//!   once per run), mtime changes.
//! - MAN-2: redis_url userinfo-stripped — adversarial, zero password hits under
//!   data_dir even when the manifest is written from a credentialed config.
//! - MAN-3/4/5: required fields, per-state session counts (all five keys always
//!   present), per-`dt=` byte counts, discovered original team strings.
//! - STJ-1: `status --json` = one JSON document, stable key order, no trailing
//!   prose, same shape as MANIFEST.json.
//! - STJ-2: human `status` prints the same fields as `key: value` lines.
//! - STJ-3: status never takes the single-writer lock — it works while a
//!   collector holds it (and a second writer exits 3).
//! - collect on a hand-built dir: all five states counted.
//!
//! Same harness as tests/backfill.rs: a dedicated per-run stream on the test
//! Redis (`WFDC_TEST_REDIS`, default `redis://127.0.0.1:6380`), per-test temp
//! `data_dir`s, CLI via `env!("CARGO_BIN_EXE_wfdc")`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use wfdc::backfill;
use wfdc::checkpoint;
use wfdc::config::Config;
use wfdc::follow::{self, FollowOptions};
use wfdc::manifest::{self, Manifest};
use wfdc::pairing::Pairer;
use wfdc::raw::Store;
use wfdc::sessions::{SessionRow, SessionStore, State};
use wfdc::stream::RedisStream;

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

/// Library backfill run mirroring main.rs: store → startup repair → resume
/// point (`max(durable CHECKPOINT, highest id written to JSONL)`) → run.
fn run_backfill(
    cfg: &Config,
    from: &str,
    to: &str,
) -> Result<backfill::BackfillOutcome, wfdc::Error> {
    let mut store = Store::open(&cfg.data_dir)?;
    store.repair_partial_lines()?;
    let durable = checkpoint::read(&cfg.data_dir)?;
    let max_written = store.max_written_stream_id()?;
    let resume = checkpoint::resume_start(&durable, max_written.as_deref());
    let mut redis = RedisStream::new(&cfg.redis_url)?;
    let session_store = SessionStore::new(&cfg.data_dir);
    let mut now = chrono::Utc::now;
    backfill::run(
        cfg,
        &mut redis,
        &cfg.stream,
        &mut store,
        &session_store,
        &resume,
        cfg.expire_hours,
        cfg.max_mb,
        from,
        to,
        &mut now,
    )
}

/// Spawn a follow loop (store + lock + run) on `cfg` in a background thread,
/// mirroring main.rs run_follow. Returns the join handle and the stop flag.
fn spawn_follow(cfg: &Config) -> (std::thread::JoinHandle<()>, Arc<AtomicBool>) {
    let cfg = cfg.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        let mut store = match Store::open(&cfg.data_dir) {
            Ok(s) => s,
            Err(_) => return,
        };
        let _lock = match wfdc::lock::acquire(&cfg.data_dir) {
            Ok(l) => l,
            Err(_) => return,
        };
        let mut redis = match RedisStream::new(&cfg.redis_url) {
            Ok(r) => r,
            Err(_) => return,
        };
        let session_store = SessionStore::new(&cfg.data_dir);
        let mut pairer = Pairer::new(cfg.expire_hours);
        match session_store.load_all() {
            Ok(rows) => pairer.rebuild(rows),
            Err(_) => return,
        }
        let durable = match checkpoint::read(&cfg.data_dir) {
            Ok(d) => d,
            Err(_) => return,
        };
        let max_written = match store.max_written_stream_id() {
            Ok(m) => m,
            Err(_) => return,
        };
        let start = checkpoint::resume_start(&durable, max_written.as_deref());
        let opts = FollowOptions::default();
        let mut sleeper = |d: Duration| std::thread::sleep(d);
        let mut now = chrono::Utc::now;
        let mut time = follow::LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        let _ = follow::run(
            &cfg,
            &mut redis,
            &cfg.stream,
            &mut store,
            &start,
            &opts,
            cfg.max_mb,
            &stop_thread,
            &mut time,
            &mut pairer,
            &session_store,
        );
    });
    (handle, stop)
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

/// Poll-tolerant read: `None` while MANIFEST.json is missing or stale (the
/// startup manifest write precedes the first flush — tests poll for the
/// flushed state, not mere file existence).
fn read_manifest_if(dir: &Path, pred: impl Fn(&Manifest) -> bool) -> Option<Manifest> {
    let text = std::fs::read_to_string(dir.join("MANIFEST.json")).ok()?;
    let m: Manifest = serde_json::from_str(&text).ok()?;
    pred(&m).then_some(m)
}

fn manifest_mtime(dir: &Path) -> std::time::SystemTime {
    std::fs::metadata(dir.join("MANIFEST.json"))
        .expect("MANIFEST.json exists")
        .modified()
        .expect("mtime")
}

fn office_raw_dates_and_sizes(dir: &Path) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let raw = dir.join("raw");
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
// MAN-1: MANIFEST rewritten each flush (backfill once per run), mtime changes.
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

    run_backfill(&cfg, "0", "+").expect("backfill ok");
    let m1 = read_manifest(&dir);
    assert_eq!(m1.checkpoint.as_deref(), Some("1725000000000-2"));
    assert_eq!(m1.last_flush_stream_id.as_deref(), Some("1725000000000-2"));
    assert_eq!(m1.event_count, 3);
    let t1 = manifest_mtime(&dir);

    // A second flush (new events) rewrites MANIFEST → mtime changes.
    tr.xadd_owned("1725000000003-0", &start_event("3", "dev", "s2", "dev-1"));
    std::thread::sleep(Duration::from_millis(20)); // ensure mtime granularity
    run_backfill(&cfg, "0", "+").expect("backfill ok");
    let m2 = read_manifest(&dir);
    assert_eq!(m2.checkpoint.as_deref(), Some("1725000000003-0"));
    assert_eq!(m2.event_count, 4);
    let t2 = manifest_mtime(&dir);
    assert_ne!(t1, t2, "MANIFEST rewritten each flush (mtime changes)");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// MAN-1 follow path: the follow loop rewrites MANIFEST after each flush.
// ---------------------------------------------------------------------------
#[test]
fn follow_flush_writes_manifest() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    tr.xadd_owned("1725000000001-0", &finish_event("1", "dev", "s1", "dev-1"));

    let dir = temp_data_dir("man1-follow");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    let (handle, stop) = spawn_follow(&cfg);

    // Poll until the follow loop's flush produced MANIFEST.json with both
    // events (the startup manifest write is event_count 0 — poll for the
    // flushed state, not mere file existence).
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut manifest = None;
    while Instant::now() < deadline {
        if let Some(m) = read_manifest_if(&dir, |m| m.event_count == 2) {
            manifest = Some(m);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let m = manifest.expect("follow flushed MANIFEST.json with 2 events within 15s");
    assert_eq!(m.checkpoint.as_deref(), Some("1725000000001-0"));

    // Clean stop so the thread releases the lock before we remove the dir.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
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
    run_backfill(&cfg, "0", "+").expect("backfill ok");

    // Rewrite MANIFEST from a credentialed config (what a collector running
    // with a password-bearing URL would flush) — manifest::write is disk-only,
    // so no Redis auth is required to prove the redaction surface.
    let credentialed = Config {
        redis_url: "redis://:s3cr3t@127.0.0.1:6380".into(),
        ..cfg.clone()
    };
    manifest::write(&credentialed).expect("write manifest from credentialed cfg");
    let file_text = std::fs::read_to_string(dir.join("MANIFEST.json")).unwrap();
    assert!(
        !file_text.contains("s3cr3t"),
        "MANIFEST.json must never contain the password: {file_text}"
    );

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

    // `wfdc status --json` with a credentialed URL never prints the password.
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

    // Human status too.
    let out = Command::new(bin)
        .args(["status"])
        .env("WFDC_REDIS_URL", "redis://:s3cr3t@127.0.0.1:6380")
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).expect("utf8");
    assert!(!text.contains("s3cr3t"), "human status leaks: {text}");
    assert!(text.contains("redis_url: redis://127.0.0.1:6380"));

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// MAN-3/4/5: required fields, per-state counts, per-dt= bytes, discovered
// teams (all five session-state keys always present).
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
    run_backfill(&cfg, "0", "+").expect("backfill ok");

    let m = read_manifest(&dir);
    assert_eq!(m.event_count, 4);
    assert_eq!(m.session_count, 3);
    // per-state counts (all five keys present with stable order)
    let states: Vec<(String, u64)> = m
        .session_states
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
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
    assert_eq!(
        m.redis_url,
        manifest::redact_redis_url(&cfg.redis_url),
        "redis_url stored redacted (userinfo stripped)"
    );

    // MAN-6: MANIFEST.json is 0600
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(dir.join("MANIFEST.json"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "MANIFEST.json must be 0600 (§2)");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// STJ-1: status --json — single doc, stable key order, no trailing prose,
// same shape as MANIFEST.json.
// ---------------------------------------------------------------------------
#[test]
fn status_json_single_doc_stable_key_order_no_trailing_prose() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    let dir = temp_data_dir("stj1");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    run_backfill(&cfg, "0", "+").expect("backfill ok");

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

    // stable key order (STJ-1) — assert on the raw document (serde_json::Value
    // alphabetizes map keys, so order can only be checked on the text).
    let v: Value = serde_json::from_str(stdout.trim()).expect("parse");
    let mut prev = 0usize;
    for k in EXPECTED_KEYS {
        let idx = stdout
            .find(&format!("\"{k}\":"))
            .unwrap_or_else(|| panic!("missing key {k} in doc: {stdout}"));
        assert!(idx > prev, "key {k} out of stable order: {stdout}");
        prev = idx;
    }
    // per-state keys stable (alphabetical BTreeMap order, all five present)
    let states = obj_states(stdout.trim());
    assert_eq!(states, EXPECTED_STATES.to_vec(), "per-state keys stable");
    // the parsed document has exactly the expected top-level keys
    assert_eq!(v.as_object().expect("object").len(), EXPECTED_KEYS.len());

    // same shape as MANIFEST.json on disk
    let file_text = std::fs::read_to_string(dir.join("MANIFEST.json")).unwrap();
    let file_v: Value = serde_json::from_str(&file_text).unwrap();
    assert_eq!(v, file_v, "status --json == MANIFEST.json content");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Read the `session_states` object's keys in document order from the raw
/// JSON text (serde_json::Value would alphabetize them).
fn obj_states(doc: &str) -> Vec<String> {
    let marker = "\"session_states\":{";
    let start = doc.find(marker).expect("session_states") + marker.len();
    let end = doc[start..]
        .find('}')
        .map(|i| start + i)
        .expect("closing brace");
    let inner = &doc[start..end];
    let mut out = Vec::new();
    let mut rest = inner;
    while let Some(q) = rest.find('"') {
        let key_start = q + 1;
        let key_end = rest[key_start..]
            .find('"')
            .map(|i| key_start + i)
            .expect("key end");
        out.push(rest[key_start..key_end].to_string());
        rest = &rest[key_end + 1..];
    }
    out
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
    run_backfill(&cfg, "0", "+").expect("backfill ok");

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
    assert!(joined.contains(&format!(
        "redis_url: {}",
        manifest::redact_redis_url(&redis_url())
    )));
    assert!(joined.contains("stream: "));

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// STJ-3: status never takes the single-writer lock: works while follow holds it.
// ---------------------------------------------------------------------------
#[test]
fn status_works_while_follow_holds_the_lock() {
    let mut tr = TestRedis::new();
    tr.xadd_owned("1725000000000-0", &start_event("0", "dev", "s1", "dev-1"));
    let dir = temp_data_dir("lockfree");
    let cfg = cfg_for(&dir, &tr.stream, 500);
    let (handle, stop) = spawn_follow(&cfg);

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
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let v: Value = serde_json::from_str(stdout.trim()).expect("single JSON doc");
    assert_eq!(
        v["stream"], tr.stream,
        "status reflects the running collector's stream"
    );

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = handle.join();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Status on a missing data_dir: default manifest, exit 0, no dir created.
// ---------------------------------------------------------------------------
#[test]
fn status_on_missing_data_dir_is_default_and_creates_nothing() {
    let tr = TestRedis::new();
    let dir = temp_data_dir("missing");
    std::fs::remove_dir(&dir).unwrap();

    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .args(["status", "--json"])
        .env("WFDC_REDIS_URL", redis_url())
        .env("WFDC_DATA_DIR", &dir)
        .env("WFDC_STREAM", &tr.stream)
        .output()
        .expect("run wfdc status --json");
    assert!(out.status.success(), "status on missing dir exits 0");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let v: Value = serde_json::from_str(stdout.trim()).expect("single JSON doc");
    assert_eq!(v["event_count"], 0);
    assert_eq!(v["checkpoint"], Value::Null);
    assert!(!dir.exists(), "status must never create data_dir");
}

// ---------------------------------------------------------------------------
// §5.4 drop-log feed: cap::enforce persists the trim to DROP_LOG.json (0600,
// `.json` not `.jsonl`), and manifest::collect surfaces it (last 100).
// ---------------------------------------------------------------------------
#[test]
fn cap_enforce_persists_drop_log_visible_in_manifest() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::SystemTime;

    let dir = temp_data_dir("droplog");
    // Two office-raw dates, together over 1 MiB (the minimum enforceable cap
    // at max_mb=1): enforcement deletes the oldest date.
    let line = "{\"stream_id\":\"1725062400000-0\",\"team\":\"dev-1\"}\n";
    let big: String = line.repeat(16_000); // ~ 752 KB per file, > 1 MiB together
    let f1 = dir.join("raw/dt=2026-08-29/events.jsonl");
    let f2 = dir.join("raw/dt=2026-08-30/events.jsonl");
    std::fs::create_dir_all(f1.parent().unwrap()).unwrap();
    std::fs::create_dir_all(f2.parent().unwrap()).unwrap();
    std::fs::write(&f1, &big).unwrap();
    std::fs::write(&f2, &big).unwrap();

    // max_mb=1 → 1 MiB cap (Config values are raw here; cap::enforce receives
    // the raw number exactly as follow/backfill pass the normalized value).
    let cfg = Config {
        redis_url: "redis://127.0.0.1:6380".into(),
        stream: "office:events".into(),
        data_dir: dir.clone(),
        max_mb: 1,
        expire_hours: 6,
    };
    wfdc::cap::enforce(&dir, cfg.max_mb, SystemTime::UNIX_EPOCH);

    // The persisted ring exists, is 0600, and is not JSONL (never counts).
    let ring = dir.join("DROP_LOG.json");
    assert!(ring.is_file(), "DROP_LOG.json persisted after a trim");
    let mode = std::fs::metadata(&ring).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "DROP_LOG.json must be 0600 (§2)");
    let ring_entries: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(&ring).unwrap()).unwrap();
    assert!(!ring_entries.is_empty(), "ring holds the trim entries");
    assert_eq!(ring_entries[0]["scope"], "date");
    assert_eq!(ring_entries[0]["date"], "2026-08-29", "oldest date dropped");

    // The manifest surfaces the ring, and the oldest date is gone.
    let m = manifest::collect(&cfg).expect("collect");
    assert_eq!(m.drop_log.len(), ring_entries.len());
    assert_eq!(m.drop_log[0].date.as_deref(), Some("2026-08-29"));
    assert!(!f1.exists(), "oldest date deleted by the trim");
    assert!(f2.exists(), "newer date survives");
    // bytes_used excludes DROP_LOG.json (it is not *.jsonl) and reflects the
    // post-trim office raw.
    let f2_size = std::fs::metadata(&f2).unwrap().len();
    assert_eq!(m.bytes_used, f2_size);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn collect_counts_all_five_states_from_hand_built_dir() {
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

    // sessions for one team with all five states (canonical SessionRow shape)
    let mk = |pk: &str, state: State| SessionRow {
        session_pk: pk.into(),
        team: "dev-1".into(),
        actor: "dev".into(),
        session_id: Some("s".into()),
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
    };
    let rows = [
        mk("pk-c", State::Completed),
        mk("pk-e", State::Expired),
        mk("pk-i", State::Interrupted),
        mk("pk-o", State::Open),
        mk("pk-f", State::OrphanFinish),
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
    assert!(
        m.drop_log.is_empty(),
        "no DROP_LOG.json → empty array (shape is the contract)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
