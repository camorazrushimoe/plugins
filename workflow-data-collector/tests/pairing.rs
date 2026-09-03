//! End-to-end pairing tests through the real follow loop (§5.3).
//!
//! A scripted [`StreamSource`] drives `follow::run` with a frozen clock, and
//! the assertions read the `sessions.jsonl` files the loop actually wrote —
//! deterministic pairing, midnight upsert, expiry on empty rounds, orphan
//! placement, and pool persistence across a restart.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

use wfdc::follow::{self, FollowOptions};
use wfdc::pairing::Pairer;
use wfdc::raw::Store;
use wfdc::sessions::{SessionRow, SessionStore, State};
use wfdc::stream::{StreamEntry, StreamError, StreamSource};

/// Scripted stream source: plays queued batches, then a few empty rounds
/// (quiet-stream idles), then sets `stop` so the loop exits cleanly.
struct Scripted {
    batches: VecDeque<Vec<StreamEntry>>,
    idle_rounds: usize,
    stop: Arc<AtomicBool>,
}

impl Scripted {
    fn new(stop: Arc<AtomicBool>) -> Self {
        Scripted {
            batches: VecDeque::new(),
            idle_rounds: 0,
            stop,
        }
    }

    fn batch(&mut self, entries: Vec<StreamEntry>) {
        self.batches.push_back(entries);
    }

    fn idles(&mut self, n: usize) {
        self.idle_rounds = n;
    }

    fn entry(id: &str, pairs: &[(&str, &str)]) -> StreamEntry {
        StreamEntry {
            id: id.to_string(),
            fields: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn start(id: &str, ts: &str) -> StreamEntry {
        Self::entry(
            id,
            &[
                ("action", "task.started"),
                ("actor", "developer"),
                ("team", "dev-1"),
                ("timestamp", ts),
            ],
        )
    }

    fn finish(id: &str, ts: &str) -> StreamEntry {
        Self::entry(
            id,
            &[
                ("action", "task.finished"),
                ("actor", "developer"),
                ("team", "dev-1"),
                ("timestamp", ts),
            ],
        )
    }
}

impl StreamSource for Scripted {
    fn xread(
        &mut self,
        _stream: &str,
        _from: &str,
        _block_ms: u64,
        _count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        if let Some(batch) = self.batches.pop_front() {
            return Ok(batch);
        }
        if self.idle_rounds > 0 {
            self.idle_rounds -= 1;
            return Ok(vec![]); // quiet stream: BLOCK timeout
        }
        self.stop.store(true, Ordering::Relaxed);
        Ok(vec![])
    }

    fn xrange(
        &mut self,
        _stream: &str,
        _from: &str,
        _to: &str,
        _count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        // xrange is backfill-only; the follow-loop tests never call it.
        unreachable!("xrange is not used by the follow loop")
    }

    fn stream_exists(&mut self, _stream: &str) -> Result<bool, StreamError> {
        Ok(true)
    }
}

fn t(iso: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(iso)
        .unwrap()
        .with_timezone(&Utc)
}

struct Harness {
    dir: tempfile::TempDir,
    stop: Arc<AtomicBool>,
    clock: DateTime<Utc>,
}

impl Harness {
    fn new(epoch: DateTime<Utc>) -> Self {
        Harness {
            dir: tempfile::tempdir().unwrap(),
            stop: Arc::new(AtomicBool::new(false)),
            clock: epoch,
        }
    }

    /// Run the follow loop to completion on a scripted source. `clock_step`
    /// advances the wall clock by this much per read iteration.
    fn run_script(&mut self, script: impl FnOnce(&mut Scripted), clock_step: Duration) {
        let mut store = Store::open(self.dir.path()).unwrap();
        let session_store = SessionStore::new(self.dir.path());
        let mut pairer = Pairer::new(6); // default expiry window
        let mut src = Scripted::new(Arc::clone(&self.stop));
        script(&mut src);
        let mut sleeper = |_: std::time::Duration| {}; // fake never errors → no backoff
        let mut now = || {
            self.clock += clock_step;
            self.clock
        };
        let mut time = follow::LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        follow::run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &FollowOptions {
                jitter: false,
                ..Default::default()
            },
            &self.stop,
            &mut time,
            &mut pairer,
            &session_store,
        )
        .unwrap();
        self.stop.store(false, Ordering::Relaxed);
    }

    /// Run with a fresh pairer rebuilt from the sessions already on disk
    /// (simulates a restart, §5.3 pool persistence).
    fn run_restarted(&mut self, script: impl FnOnce(&mut Scripted), clock_step: Duration) {
        let mut store = Store::open(self.dir.path()).unwrap();
        let session_store = SessionStore::new(self.dir.path());
        let mut pairer = Pairer::new(6);
        pairer.rebuild(session_store.load_all().unwrap());
        let mut src = Scripted::new(Arc::clone(&self.stop));
        script(&mut src);
        let mut sleeper = |_: std::time::Duration| {};
        let mut now = || {
            self.clock += clock_step;
            self.clock
        };
        let mut time = follow::LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        follow::run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &FollowOptions {
                jitter: false,
                ..Default::default()
            },
            &self.stop,
            &mut time,
            &mut pairer,
            &session_store,
        )
        .unwrap();
        self.stop.store(false, Ordering::Relaxed);
    }

    fn session_path(&self, team_folder: &str, dt: &str) -> std::path::PathBuf {
        self.dir
            .path()
            .join("teams")
            .join(team_folder)
            .join("sessions")
            .join(format!("dt={dt}"))
            .join("sessions.jsonl")
    }

    fn session_rows(&self, team_folder: &str, dt: &str) -> Vec<SessionRow> {
        let content = std::fs::read_to_string(self.session_path(team_folder, dt)).unwrap();
        content
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

/// The follow loop writes the day's `sessions.jsonl` under the start's team
/// folder, with a completed row carrying start + finish fields.
#[test]
fn loop_pairs_start_finish_into_sessions_file() {
    let mut h = Harness::new(t("2026-08-30T21:00:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
            s.batch(vec![Scripted::finish("200-0", "2026-08-30T21:00:10Z")]);
        },
        Duration::seconds(1),
    );
    let rows = h.session_rows("dev-1", "2026-08-30");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.state, State::Completed);
    assert_eq!(r.start_stream_id.as_deref(), Some("100-0"));
    assert_eq!(r.finish_stream_id.as_deref(), Some("200-0"));
    assert_eq!(r.duration_ms, Some(10_000));
    assert!(
        !r.session_pk.starts_with("dev-1|developer|100-0"),
        "session_pk is a hash, not a raw concatenation"
    );
    assert_eq!(r.session_pk.len(), 64, "sha256 hex");
}

/// Identical stream input through the loop → byte-identical sessions files
/// (determinism §7) and identical session_pk values.
#[test]
fn identical_stream_produces_identical_files() {
    let mut a = Harness::new(t("2026-08-30T21:00:00Z"));
    a.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
            s.batch(vec![Scripted::start("101-0", "2026-08-30T21:00:01Z")]);
            s.batch(vec![Scripted::finish("102-0", "2026-08-30T21:00:02Z")]);
            s.batch(vec![Scripted::finish("103-0", "2026-08-30T21:00:03Z")]);
            s.batch(vec![Scripted::finish("104-0", "2026-08-30T21:00:04Z")]);
        },
        Duration::seconds(1),
    );
    let mut b = Harness::new(t("2026-08-30T21:00:00Z"));
    b.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
            s.batch(vec![Scripted::start("101-0", "2026-08-30T21:00:01Z")]);
            s.batch(vec![Scripted::finish("102-0", "2026-08-30T21:00:02Z")]);
            s.batch(vec![Scripted::finish("103-0", "2026-08-30T21:00:03Z")]);
            s.batch(vec![Scripted::finish("104-0", "2026-08-30T21:00:04Z")]);
        },
        Duration::seconds(1),
    );
    let fa = std::fs::read(a.session_path("dev-1", "2026-08-30")).unwrap();
    let fb = std::fs::read(b.session_path("dev-1", "2026-08-30")).unwrap();
    assert_eq!(fa, fb, "identical stream → identical sessions.jsonl bytes");
    // FIFO: the first finish pairs the OLDEST start (100-0).
    let rows = a.session_rows("dev-1", "2026-08-30");
    let completed: Vec<&SessionRow> = rows
        .iter()
        .filter(|r| r.state == State::Completed)
        .collect();
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].start_stream_id.as_deref(), Some("100-0"));
    assert_eq!(completed[0].finish_stream_id.as_deref(), Some("102-0"));
    assert_eq!(completed[1].start_stream_id.as_deref(), Some("101-0"));
    assert_eq!(completed[1].finish_stream_id.as_deref(), Some("103-0"));
    // the third finish (no unmatched start left) is an orphan, kept
    assert!(rows.iter().any(|r| r.state == State::OrphanFinish));
    assert_eq!(rows.len(), 3, "2 completed + 1 orphan_finish");
}

/// A session that spans midnight lives on the `dt=` of `started_at` and is
/// upserted there even after midnight (§5.3).
#[test]
fn midnight_session_is_upserted_into_start_day_file() {
    let mut h = Harness::new(t("2026-08-30T23:59:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T23:59:00Z")]);
            s.batch(vec![Scripted::finish("200-0", "2026-08-31T00:01:00Z")]);
        },
        Duration::seconds(1),
    );
    let rows = h.session_rows("dev-1", "2026-08-30");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, State::Completed);
    assert_eq!(rows[0].finished_at.as_deref(), Some("2026-08-31T00:01:00Z"));
    assert!(
        !h.session_path("dev-1", "2026-08-31").exists(),
        "the finish's own day must not receive the row"
    );
}

/// Expiry runs on every read iteration, including empty rounds: an open row
/// ages out while the stream is quiet and lands on disk as `expired`.
#[test]
fn empty_rounds_expire_open_rows() {
    let t0 = t("2026-08-30T21:00:00Z");
    let mut h = Harness::new(t0);
    // window = 6 h; the clock advances 7 h per iteration → the first empty
    // round (iteration 2) already sees started_at older than the window.
    h.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
            s.idles(2);
        },
        Duration::hours(7),
    );
    let rows = h.session_rows("dev-1", "2026-08-30");
    assert_eq!(rows.len(), 1, "the open row expired during quiet rounds");
    assert_eq!(rows[0].state, State::Expired);
    assert_eq!(rows[0].start_stream_id.as_deref(), Some("100-0"));
}

/// An orphan finish (no compatible start) is placed on the `dt=` of its own
/// finish timestamp in the finish's own team folder, and is kept (§5.3).
#[test]
fn orphan_finish_is_kept_in_finish_team_folder() {
    let mut h = Harness::new(t("2026-08-30T21:00:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::entry(
                "50-0",
                &[
                    ("action", "task.finished"),
                    ("actor", "researcher"),
                    ("team", "lab-1"),
                    ("timestamp", "2026-08-30T21:00:00Z"),
                ],
            )]);
        },
        Duration::seconds(1),
    );
    let rows = h.session_rows("lab-1", "2026-08-30");
    assert_eq!(rows.len(), 1, "orphan finishes are kept, not dropped");
    assert_eq!(rows[0].state, State::OrphanFinish);
    assert_eq!(rows[0].finish_stream_id.as_deref(), Some("50-0"));
    assert_eq!(rows[0].start_stream_id, None);
    assert_eq!(rows[0].session_id, None);
}

/// Only `task.started` / `task.finished` pair (§4.2): an unrelated action
/// produces no session row, and a finish without a start is an orphan.
#[test]
fn unrelated_actions_do_not_pair() {
    let mut h = Harness::new(t("2026-08-30T21:00:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::entry(
                "10-0",
                &[
                    ("action", "agent.started"),
                    ("actor", "developer"),
                    ("team", "dev-1"),
                    ("timestamp", "2026-08-30T21:00:00Z"),
                ],
            )]);
            s.batch(vec![Scripted::finish("20-0", "2026-08-30T21:00:05Z")]);
        },
        Duration::seconds(1),
    );
    let rows = h.session_rows("dev-1", "2026-08-30");
    assert_eq!(rows.len(), 1, "agent.started must not open a session");
    assert_eq!(rows[0].state, State::OrphanFinish);
}

/// The pairing pool is rebuilt from the sessions already on disk at startup,
/// so a finish arriving after a restart still pairs with a pre-restart start
/// (cross-batch pool persistence, §5.3).
#[test]
fn restart_rebuilds_pool_from_disk_rows() {
    let mut h = Harness::new(t("2026-08-30T21:00:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
        },
        Duration::seconds(1),
    );
    assert_eq!(h.session_rows("dev-1", "2026-08-30")[0].state, State::Open);

    // restart: fresh pairer rebuilt from disk, finish arrives
    h.run_restarted(
        |s| {
            s.batch(vec![Scripted::finish("200-0", "2026-08-30T21:00:10Z")]);
        },
        Duration::seconds(1),
    );
    let rows = h.session_rows("dev-1", "2026-08-30");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].state,
        State::Completed,
        "post-restart finish pairs with the reloaded start"
    );
    assert_eq!(rows[0].finish_stream_id.as_deref(), Some("200-0"));
    assert_eq!(rows[0].duration_ms, Some(10_000));
}

/// Raw and session views coexist under one data_dir; session files are 0600
/// and their directories 0700 (§2).
#[test]
fn session_files_have_0600_perms_and_coexist_with_raw() {
    use std::os::unix::fs::PermissionsExt;
    let mut h = Harness::new(t("2026-08-30T21:00:00Z"));
    h.run_script(
        |s| {
            s.batch(vec![Scripted::start("100-0", "2026-08-30T21:00:00Z")]);
            s.batch(vec![Scripted::finish("200-0", "2026-08-30T21:00:10Z")]);
        },
        Duration::seconds(1),
    );
    let path = h.session_path("dev-1", "2026-08-30");
    let meta = std::fs::metadata(&path).unwrap();
    assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    // the raw office view for the same day exists too
    let raw_office = h.dir.path().join("raw/dt=2026-08-30/events.jsonl");
    assert!(
        raw_office.is_file(),
        "raw dataset is written alongside sessions"
    );
    // no leftover tmp file
    assert!(!path.with_extension("jsonl.tmp").exists());
    // every session line round-trips
    for row in h.session_rows("dev-1", "2026-08-30") {
        assert!(!row.session_pk.is_empty());
    }
}
