//! BON-72 acceptance tests: `wfdc backfill` against a real Redis stream.
//!
//! These tests use a dedicated, per-run stream name on the shared office Redis
//! (`redis://127.0.0.1:6380` by default; override with `WFDC_TEST_REDIS`) so
//! they never touch the live `office:events` bus. Every stream is DELeted on
//! drop. All `data_dir`s are per-test temp dirs.
//!
//! Semantics under test (pinned in SPEC.md §3 / QA plan BKF-1..13):
//! - the chosen range `[from, to]` is inclusive (XRANGE semantics);
//! - dedupe applies: entries with `stream_id <=` the last flushed CHECKPOINT
//!   are skipped — re-running a range never duplicates rows (BKF-5/6);
//! - CHECKPOINT advances forward only, never backward (BKF-7);
//! - the pairing pool is rebuilt from session rows already on disk, so a
//!   finish in range pairs with a start flushed in an earlier run;
//! - an inverted range (`--from` > `--to`) writes nothing and exits 0 (BKF-3).

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use wfdc::backfill;
use wfdc::config::Config;
use wfdc::sessions::SessionRow;

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
            "wfdc-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        // Namespace isolation check: never run against the real bus stream.
        assert!(
            !stream.starts_with("office:"),
            "test stream must not be office:events"
        );
        Self { stream, conn }
    }

    /// XADD with an explicit, increasing stream id.
    fn xadd(&mut self, id: &str, fields: &[(&str, &str)]) {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.stream).arg(id);
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        cmd.exec(&mut self.conn).expect("xadd");
    }

    /// XADD with owned string pairs (for helper-built event payloads).
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
        "wfdc-test-{}-{}-{}",
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

fn cfg_for(dir: &Path, stream: &str) -> Config {
    Config {
        redis_url: redis_url(),
        stream: stream.to_string(),
        data_dir: dir.to_path_buf(),
        max_mb: 0,
        expire_hours: 100000, // effectively no expiry for determinism tests
    }
}

fn read_raw_lines(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let raw = dir.join("raw");
    if raw.exists() {
        for dt in sorted_dirs(&raw) {
            for f in sorted_files(&dt) {
                if f.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    for line in std::fs::read_to_string(&f).unwrap().lines() {
                        if !line.is_empty() {
                            out.push(serde_json::from_str(line).unwrap());
                        }
                    }
                }
            }
        }
    }
    out
}

fn read_session_rows(dir: &Path) -> Vec<SessionRow> {
    let mut out = Vec::new();
    let teams = dir.join("teams");
    if teams.exists() {
        for team in sorted_dirs(&teams) {
            let sdir = team.join("sessions");
            if sdir.exists() {
                for dt in sorted_dirs(&sdir) {
                    for f in sorted_files(&dt) {
                        if f.extension().map(|e| e == "jsonl").unwrap_or(false) {
                            for line in std::fs::read_to_string(&f).unwrap().lines() {
                                if !line.is_empty() {
                                    out.push(serde_json::from_str(line).unwrap());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

fn sorted_dirs(p: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(p)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|x| x.is_dir())
        .collect();
    v.sort();
    v
}

fn sorted_files(p: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(p)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|x| x.is_file())
        .collect();
    v.sort();
    v
}

fn checkpoint_of(dir: &Path) -> Option<String> {
    let p = dir.join("CHECKPOINT");
    if p.exists() {
        Some(std::fs::read_to_string(&p).unwrap().trim().to_string())
    } else {
        None
    }
}

fn write_checkpoint(dir: &Path, id: &str) {
    std::fs::write(dir.join("CHECKPOINT"), id).unwrap();
}

fn start_event(id: &str, actor: &str, session_id: &str, team: &str) -> Vec<(&'static str, String)> {
    vec![
        ("action", "task.started".to_string()),
        ("actor", actor.to_string()),
        ("target", actor.to_string()),
        ("team", team.to_string()),
        ("session_id", session_id.to_string()),
        (
            "timestamp",
            format!("2026-08-30T10:{:02}:00Z", id.parse::<u64>().unwrap() % 60),
        ),
    ]
}

fn finish_event(
    id: &str,
    actor: &str,
    session_id: &str,
    team: &str,
) -> Vec<(&'static str, String)> {
    vec![
        ("action", "task.finished".to_string()),
        ("actor", actor.to_string()),
        ("target", actor.to_string()),
        ("team", team.to_string()),
        ("session_id", session_id.to_string()),
        (
            "timestamp",
            format!("2026-08-30T11:{:02}:00Z", id.parse::<u64>().unwrap() % 60),
        ),
    ]
}

fn as_pairs(events: Vec<(&'static str, String)>) -> Vec<(String, String)> {
    events
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

fn start_fields(
    actor: &'static str,
    session_id: &'static str,
) -> Vec<(&'static str, &'static str)> {
    vec![
        ("action", "task.started"),
        ("actor", actor),
        ("team", "dev-1"),
        ("session_id", session_id),
    ]
}

// ---------------------------------------------------------------------------
// 1. Range selection: only the chosen [from, to] window is written (BKF-1).
// ---------------------------------------------------------------------------
#[test]
fn backfill_writes_only_the_chosen_range() {
    let mut tr = TestRedis::new();
    // 10 events, ids 100..110
    for i in 0..10u64 {
        let id = format!("{}-0", 1725000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        );
    }
    let dir = temp_data_dir("range");
    let cfg = cfg_for(&dir, &tr.stream);
    let from = "1725000000002-0";
    let to = "1725000000005-0";
    let out = backfill::run(&cfg, from, to).expect("backfill ok");

    let rows = read_raw_lines(&dir);
    assert_eq!(rows.len(), 4, "only 4 rows in range");
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r["stream_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![
            "1725000000002-0",
            "1725000000003-0",
            "1725000000004-0",
            "1725000000005-0"
        ]
    );
    assert_eq!(out.raw_lines, 4);
    // checkpoint advanced to the range end (forward-only)
    assert_eq!(checkpoint_of(&dir).as_deref(), Some(to));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 2. Determinism: same range → byte-identical raw rows and same session_pk set
//    (BKF-12).
// ---------------------------------------------------------------------------
#[test]
fn backfill_is_deterministic() {
    let mut tr = TestRedis::new();
    let mut n: u64 = 0;
    for i in 0..5u64 {
        let id = format!("{}-0", 1726000000000 + n);
        tr.xadd_owned(
            &id,
            &as_pairs(start_event(&format!("{i}"), "dev", "sess-a", "dev-1")),
        );
        n += 1;
    }
    for i in 0..5u64 {
        let id = format!("{}-0", 1726000000000 + n);
        tr.xadd_owned(
            &id,
            &as_pairs(finish_event(&format!("{i}"), "dev", "sess-a", "dev-1")),
        );
        n += 1;
    }

    let d1 = temp_data_dir("det1");
    let d2 = temp_data_dir("det2");
    backfill::run(
        &cfg_for(&d1, &tr.stream),
        "1726000000000-0",
        "1726000000009-0",
    )
    .unwrap();
    backfill::run(
        &cfg_for(&d2, &tr.stream),
        "1726000000000-0",
        "1726000000009-0",
    )
    .unwrap();

    let raw1 = read_raw_lines(&d1);
    let raw2 = read_raw_lines(&d2);
    assert_eq!(raw1.len(), 10);
    assert_eq!(raw1, raw2, "raw rows byte-identical across runs");

    let s1 = read_session_rows(&d1);
    let s2 = read_session_rows(&d2);
    assert_eq!(s1.len(), 5, "five completed sessions");
    let pk1: Vec<&str> = s1.iter().map(|r| r.session_pk.as_str()).collect();
    let pk2: Vec<&str> = s2.iter().map(|r| r.session_pk.as_str()).collect();
    assert_eq!(pk1, pk2, "same session_pk values across runs");
    assert!(s1.iter().all(|r| r.state == "completed"));
    let _ = std::fs::remove_dir_all(&d1);
    let _ = std::fs::remove_dir_all(&d2);
}

// ---------------------------------------------------------------------------
// 3. Dedupe/checkpoint (§3.1): backfill applies the same dedupe as follow —
//    entries with stream_id <= the last flushed CHECKPOINT are skipped, and
//    CHECKPOINT is only ever advanced forward (BKF-5/6/7).
// ---------------------------------------------------------------------------
#[test]
fn backfill_above_checkpoint_advances_forward_and_does_not_duplicate() {
    let mut tr = TestRedis::new();
    for i in 0..10u64 {
        let id = format!("{}-0", 1727000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        );
    }
    let dir = temp_data_dir("dedupe");
    // Simulate follow-mode checkpoint at id 4: ids 0..=4 already flushed.
    write_checkpoint(&dir, "1727000000004-0");

    let out = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1727000000005-0",
        "1727000000009-0",
    )
    .expect("backfill ok");
    assert_eq!(out.raw_lines, 5);
    let rows = read_raw_lines(&dir);
    assert_eq!(
        rows.len(),
        5,
        "only the backfilled range is on disk — no dupes"
    );
    // checkpoint advanced forward to the range end
    assert_eq!(checkpoint_of(&dir).as_deref(), Some("1727000000009-0"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backfill_rerun_same_range_does_not_duplicate() {
    let mut tr = TestRedis::new();
    for i in 0..5u64 {
        let id = format!("{}-0", 1727500000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        );
    }
    let dir = temp_data_dir("rerun");
    let cfg = cfg_for(&dir, &tr.stream);

    let out1 = backfill::run(&cfg, "1727500000000-0", "1727500000004-0").expect("first backfill");
    assert_eq!(out1.raw_lines, 5);
    assert_eq!(read_raw_lines(&dir).len(), 5);
    assert_eq!(checkpoint_of(&dir).as_deref(), Some("1727500000004-0"));

    // BKF-5: same range again → 5 lines, not 10 (stream_id <= checkpoint skipped).
    let out2 = backfill::run(&cfg, "1727500000000-0", "1727500000004-0").expect("second backfill");
    assert_eq!(out2.raw_lines, 0, "all entries <= checkpoint → skipped");
    assert_eq!(read_raw_lines(&dir).len(), 5, "no duplicates on re-run");
    assert_eq!(
        checkpoint_of(&dir).as_deref(),
        Some("1727500000004-0"),
        "checkpoint unchanged"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backfill_range_below_checkpoint_writes_nothing_and_keeps_checkpoint() {
    let mut tr = TestRedis::new();
    for i in 0..10u64 {
        let id = format!("{}-0", 1728000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        );
    }
    let dir = temp_data_dir("hole");
    write_checkpoint(&dir, "1728000000007-0");

    // Recovered/older range entirely at or below the checkpoint → nothing
    // written (dedupe), checkpoint never moved backward (BKF-7).
    let out = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1728000000002-0",
        "1728000000005-0",
    )
    .expect("backfill ok");
    assert_eq!(out.raw_lines, 0, "all entries <= checkpoint are skipped");
    assert_eq!(read_raw_lines(&dir).len(), 0);
    assert_eq!(
        checkpoint_of(&dir).as_deref(),
        Some("1728000000007-0"),
        "checkpoint never moved backward"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backfill_partial_overlap_skips_below_and_writes_above_checkpoint() {
    let mut tr = TestRedis::new();
    for i in 0..10u64 {
        let id = format!("{}-0", 1728100000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        );
    }
    let dir = temp_data_dir("overlap");
    write_checkpoint(&dir, "1728100000007-0");

    // Range straddles the checkpoint: ids 5,6,7 (<= 7) skipped; 8,9 written
    // exactly once; checkpoint advances to the range end (BKF-6).
    let out = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1728100000005-0",
        "1728100000009-0",
    )
    .expect("backfill ok");
    assert_eq!(out.raw_lines, 2);
    let raw = read_raw_lines(&dir);
    let ids: Vec<&str> = raw
        .iter()
        .map(|r| r["stream_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["1728100000008-0", "1728100000009-0"]);
    assert_eq!(checkpoint_of(&dir).as_deref(), Some("1728100000009-0"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4. Sessions written by backfill respect pairing rules (§5.3) including
//    orphan_finish for finishes whose start is outside the range.
// ---------------------------------------------------------------------------
#[test]
fn backfill_pairs_sessions_and_marks_orphan_finish() {
    let mut tr = TestRedis::new();
    // a start OUTSIDE the range (before --from); its finish INSIDE the range
    // cannot pair (start not read) → orphan_finish
    tr.xadd(
        "1729000000000-0",
        &[
            ("action", "task.started"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "x"),
        ],
    );
    tr.xadd(
        "1729000000001-0",
        &[
            ("action", "task.finished"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "x"),
        ],
    );
    // a complete pair inside the range
    tr.xadd(
        "1729000000002-0",
        &[
            ("action", "task.started"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "y"),
        ],
    );
    tr.xadd(
        "1729000000003-0",
        &[
            ("action", "task.finished"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "y"),
        ],
    );
    // a start inside the range whose finish is outside (beyond --to) → open
    tr.xadd(
        "1729000000004-0",
        &[
            ("action", "task.started"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "z"),
        ],
    );

    let dir = temp_data_dir("orphan");
    let out = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1729000000001-0",
        "1729000000004-0",
    )
    .expect("backfill ok");
    assert_eq!(out.raw_lines, 4);
    let sessions = read_session_rows(&dir);
    let states: Vec<&str> = sessions.iter().map(|r| r.state.as_str()).collect();
    assert!(
        states.contains(&"completed"),
        "y pair completed: {states:?}"
    );
    assert!(
        states.contains(&"orphan_finish"),
        "finish without in-range start is orphan: {states:?}"
    );
    assert!(
        states.contains(&"open"),
        "start without in-range finish is open: {states:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 5. Same pairing rules as follow across runs: a finish in range pairs with a
//    start that was flushed earlier (<= checkpoint) via the on-disk pool.
// ---------------------------------------------------------------------------
#[test]
fn backfill_finish_pairs_with_start_flushed_in_earlier_run() {
    let mut tr = TestRedis::new();
    tr.xadd("1732000000000-0", &start_fields("dev", "s1"));
    tr.xadd(
        "1732000000001-0",
        &[
            ("action", "task.finished"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "s1"),
        ],
    );

    let dir = temp_data_dir("cross");
    let cfg = cfg_for(&dir, &tr.stream);

    // Run 1: only the start → open row on disk, checkpoint = start id.
    let out1 = backfill::run(&cfg, "1732000000000-0", "1732000000000-0").expect("run1");
    assert_eq!(out1.raw_lines, 1);
    let s1 = read_session_rows(&dir);
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0].state, "open");

    // Run 2: only the finish. The start is <= checkpoint → dedupe-skipped from
    // raw, but the finish must still pair with the open row rebuilt from disk.
    let out2 = backfill::run(&cfg, "1732000000001-0", "1732000000001-0").expect("run2");
    assert_eq!(out2.raw_lines, 1);
    let s2 = read_session_rows(&dir);
    assert_eq!(s2.len(), 1, "still one session row — completed, not orphan");
    assert_eq!(s2[0].state, "completed");
    assert_eq!(
        s2[0].session_pk, s1[0].session_pk,
        "same session_pk across runs"
    );
    assert_eq!(checkpoint_of(&dir).as_deref(), Some("1732000000001-0"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 6. End-to-end through the real binary: `wfdc backfill --from --to` CLI.
// ---------------------------------------------------------------------------
#[test]
fn cli_backfill_end_to_end() {
    let mut tr = TestRedis::new();
    for i in 0..6u64 {
        let id = format!("{}-0", 1730000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s"),
            ],
        );
    }
    let dir = temp_data_dir("cli");
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let out = Command::new(bin)
        .env("WFDC_DATA_DIR", &dir)
        .args(["--redis", &redis_url(), "--stream", &tr.stream])
        .args([
            "backfill",
            "--from",
            "1730000000001-0",
            "--to",
            "1730000000004-0",
        ])
        .output()
        .expect("run wfdc backfill");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(read_raw_lines(&dir).len(), 4);
    assert_eq!(checkpoint_of(&dir).as_deref(), Some("1730000000004-0"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 6b. Follow-after-backfill must NOT clobber session rows it did not create:
//     follow rebuilds its pool from the on-disk sessions (same as backfill),
//     so a finish consumed by follow pairs with a start backfilled earlier.
// ---------------------------------------------------------------------------
#[test]
fn follow_preserves_session_rows_from_earlier_backfill() {
    let mut tr = TestRedis::new();
    tr.xadd("1735000000000-0", &start_fields("dev", "s1"));
    tr.xadd(
        "1735000000001-0",
        &[
            ("action", "task.finished"),
            ("actor", "dev"),
            ("team", "dev-1"),
            ("session_id", "s1"),
        ],
    );

    let dir = temp_data_dir("followkeep");
    let cfg = cfg_for(&dir, &tr.stream);

    // Backfill only the start → one open row on disk, checkpoint = start id.
    let out = backfill::run(&cfg, "1735000000000-0", "1735000000000-0").expect("backfill start");
    assert_eq!(out.raw_lines, 1);
    let rows = read_session_rows(&dir);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "open");
    let pk = rows[0].session_pk.clone();

    // Follow from the checkpoint: it must consume the finish, pair it with the
    // on-disk open row, and rewrite the file with exactly that one completed
    // row (never drop the pre-existing row, never orphan the finish).
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let mut child = Command::new(bin)
        .env("WFDC_DATA_DIR", &dir)
        .args(["--redis", &redis_url(), "--stream", &tr.stream])
        .spawn()
        .expect("spawn follow");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if checkpoint_of(&dir).as_deref() == Some("1735000000001-0") {
            break;
        }
        if std::time::Instant::now() > deadline {
            child.kill().ok();
            panic!("follow did not advance checkpoint to the finish id");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    child.kill().ok();
    let _ = child.wait();

    let rows2 = read_session_rows(&dir);
    assert_eq!(
        rows2.len(),
        1,
        "exactly one row — the backfilled start, completed"
    );
    assert_eq!(rows2[0].state, "completed");
    assert_eq!(
        rows2[0].session_pk, pk,
        "same session_pk across backfill+follow"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 7. No partial output on failure / empty ranges:
//    a range with no events is a clean no-op; an inverted range writes
//    nothing and exits 0 (BKF-3).
// ---------------------------------------------------------------------------
#[test]
fn backfill_empty_range_is_clean_noop() {
    let mut tr = TestRedis::new();
    for i in 0..3u64 {
        let id = format!("{}-0", 1731000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
            ],
        );
    }
    let dir = temp_data_dir("empty");
    let out = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1731000000001-0",
        "1731000000002-0",
    )
    .expect("ok");
    assert_eq!(out.raw_lines, 2);
    // empty gap beyond the stream: nothing written, checkpoint unchanged
    let out2 = backfill::run(
        &cfg_for(&dir, &tr.stream),
        "1731000000005-0",
        "1731000000009-0",
    )
    .expect("ok");
    assert_eq!(out2.raw_lines, 0);
    assert_eq!(
        checkpoint_of(&dir).as_deref(),
        Some("1731000000002-0"),
        "checkpoint not advanced for empty range"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn backfill_inverted_range_is_clean_noop_and_exits_zero() {
    let mut tr = TestRedis::new();
    for i in 0..3u64 {
        let id = format!("{}-0", 1733000000000 + i);
        tr.xadd(
            &id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
            ],
        );
    }
    let dir = temp_data_dir("invert");
    let from = "1733000000002-0";
    let to = "1733000000001-0";

    // Library path: --from after --to → no rows, no checkpoint, Ok.
    let out = backfill::run(&cfg_for(&dir, &tr.stream), from, to)
        .expect("inverted range is not an error");
    assert_eq!(out.raw_lines, 0);
    assert_eq!(read_raw_lines(&dir).len(), 0);
    assert_eq!(checkpoint_of(&dir), None, "no checkpoint written");

    // CLI path: exit code 0.
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let r = Command::new(bin)
        .env("WFDC_DATA_DIR", &dir)
        .args(["--redis", &redis_url(), "--stream", &tr.stream])
        .args(["backfill", "--from", from, "--to", to])
        .output()
        .expect("run wfdc backfill inverted");
    assert!(
        r.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&r.stderr)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 8. Backfill is a writer: it respects the single-writer lock (§3.3) — a live
//    holder makes backfill exit 3; a stale lock is taken over (BKF-10).
// ---------------------------------------------------------------------------
#[test]
fn backfill_respects_the_single_writer_lock() {
    use std::os::unix::fs::PermissionsExt;

    let mut tr = TestRedis::new();
    tr.xadd("1734000000000-0", &start_fields("dev", "s1"));

    // A live lock (our own pid + matching starttime) → exit 3.
    let dir = temp_data_dir("locklive");
    let bin = env!("CARGO_BIN_EXE_wfdc");
    let _lock = wfdc::lock::acquire(&dir).expect("hold the lock in-process");
    let out = Command::new(bin)
        .env("WFDC_DATA_DIR", &dir)
        .args(["--redis", &redis_url(), "--stream", &tr.stream])
        .args([
            "backfill",
            "--from",
            "1734000000000-0",
            "--to",
            "1734000000000-0",
        ])
        .output()
        .expect("run backfill under lock");
    assert_eq!(
        out.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    drop(_lock);
    let _ = std::fs::remove_dir_all(&dir);

    // A stale lock (dead pid) → taken over, runs, exit 0.
    let dir2 = temp_data_dir("lockstale");
    std::fs::write(dir2.join(".lock"), "99999999 12345\n").unwrap();
    let out2 = Command::new(bin)
        .env("WFDC_DATA_DIR", &dir2)
        .args(["--redis", &redis_url(), "--stream", &tr.stream])
        .args([
            "backfill",
            "--from",
            "1734000000000-0",
            "--to",
            "1734000000000-0",
        ])
        .output()
        .expect("run backfill with stale lock");
    assert!(
        out2.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    assert_eq!(read_raw_lines(&dir2).len(), 1);
    // data files 0600, dirs 0700 (§2)
    let mode = std::fs::metadata(dir2.join("CHECKPOINT"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
    let _ = std::fs::remove_dir_all(&dir2);
}
