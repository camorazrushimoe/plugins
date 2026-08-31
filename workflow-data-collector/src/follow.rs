//! Follow loop (§3): blocking XREAD with backoff, one flush per batch,
//! at-least-once dedupe, and the mandated write order
//! (write batch → fsync JSONL → atomic CHECKPOINT → advance watermark).
//!
//! Redis being down is **not** an exit: reads fail → log, back off
//! (1 s → 60 s, capped, with jitter; reset on success) and retry forever.
//! CHECKPOINT never advances while disconnected — it only moves after a
//! successful flush, and never for un-flushed data.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::decode;
use crate::raw::{Store, WriteEntry};
use crate::stream::StreamSource;
use crate::team;
use crate::Error;

pub const BLOCK_MS: u64 = 1000;
pub const COUNT: usize = 16;
pub const BACKOFF_START_MS: u64 = 1000;
pub const BACKOFF_MAX_MS: u64 = 60_000;

/// Exponential backoff 1 s → 60 s, capped, with jitter (disabled for tests).
pub struct Backoff {
    current_ms: u64,
    jitter: bool,
    rng: u64,
}

impl Backoff {
    pub fn new(jitter: bool) -> Self {
        Backoff {
            current_ms: BACKOFF_START_MS,
            jitter,
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    pub fn reset(&mut self) {
        self.current_ms = BACKOFF_START_MS;
    }

    /// Next wait: the current base (doubling up to 60 s) plus ±25% jitter,
    /// clamped to [1 s, 60 s]. `reset()` after any successful read.
    pub fn next_wait(&mut self) -> Duration {
        let base = self.current_ms;
        self.current_ms = (self.current_ms * 2).min(BACKOFF_MAX_MS);
        let jitter = if self.jitter {
            let quarter = (base / 4).max(1);
            let r = self.next_rand() % (2 * quarter + 1);
            (r as i64) - (quarter as i64)
        } else {
            0
        };
        let wait = (base as i64 + jitter).clamp(BACKOFF_START_MS as i64, BACKOFF_MAX_MS as i64);
        Duration::from_millis(wait as u64)
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// A Redis stream id, compared numerically (`<ms>-<seq>`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamId {
    ms: u64,
    seq: u64,
}

fn parse_stream_id(id: &str) -> Option<StreamId> {
    if id == "0" {
        return Some(StreamId { ms: 0, seq: 0 });
    }
    let mut parts = id.split('-');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(ms), Some(seq), None) => Some(StreamId {
            ms: ms.parse().ok()?,
            seq: seq.parse().ok()?,
        }),
        _ => None,
    }
}

/// §3.1 dedupe: skip any entry whose stream_id is `<=` the last flushed
/// checkpoint. Stream ids are monotonic per stream.
fn should_skip(id: &str, checkpoint: &str) -> Result<bool, Error> {
    let id = parse_stream_id(id)
        .ok_or_else(|| Error::Fatal(format!("unparsable stream id from Redis: {id:?}")))?;
    let cp = parse_stream_id(checkpoint)
        .ok_or_else(|| Error::Fatal(format!("unparsable checkpoint: {checkpoint:?}")))?;
    Ok(id <= cp)
}

/// Options for one follow run (defaults per §3).
#[derive(Debug, Clone)]
pub struct FollowOptions {
    pub block_ms: u64,
    pub count: usize,
    pub jitter: bool,
}

impl Default for FollowOptions {
    fn default() -> Self {
        FollowOptions {
            block_ms: BLOCK_MS,
            count: COUNT,
            jitter: true,
        }
    }
}

/// Outcome of one loop iteration.
#[derive(Debug, PartialEq)]
pub enum Step {
    /// Empty read (BLOCK timeout on a quiet stream) — nothing to flush.
    Idle,
    /// Flushed `n` events and advanced the checkpoint.
    Flushed(usize),
    /// Read failed (Redis down); the caller should wait `Duration` and retry.
    BackingOff(Duration),
}

/// One iteration of the follow loop. `checkpoint` (the in-memory watermark)
/// advances only inside `Flushed`, after the batch is durable (§3.1 order:
/// write → fsync JSONL → atomic CHECKPOINT → advance).
pub fn step<S: StreamSource>(
    source: &mut S,
    stream: &str,
    checkpoint: &mut String,
    store: &mut Store,
    opts: &FollowOptions,
    backoff: &mut Backoff,
) -> Result<Step, Error> {
    match source.xread(stream, checkpoint, opts.block_ms, opts.count) {
        Ok(entries) => {
            backoff.reset();
            if entries.is_empty() {
                return Ok(Step::Idle);
            }
            let mut fresh: Vec<_> = Vec::with_capacity(entries.len());
            for e in entries {
                if !should_skip(&e.id, checkpoint)? {
                    fresh.push(e);
                }
            }
            if fresh.is_empty() {
                // everything at/below the watermark — nothing to flush
                return Ok(Step::Idle);
            }
            let last_id = fresh.last().expect("non-empty").id.clone();
            let mut write_entries: Vec<WriteEntry> = Vec::with_capacity(fresh.len());
            for e in &fresh {
                let decoded = decode::decode(&e.id, &e.fields);
                if let Some(reason) = &decoded.decode_error {
                    log::warn!(
                        "decode failure — stream_id={} reason={} (line kept with decode_ok=false)",
                        e.id,
                        reason
                    );
                }
                write_entries.push(WriteEntry {
                    team_safe: team::team_safe(
                        decoded.line.team.as_deref(),
                        decoded.line.actor.as_deref(),
                    ),
                    dt: crate::dt::dt_for(&e.id, decoded.line.ts.as_deref()),
                    line: decoded.line,
                });
            }
            store.write_batch(&write_entries)?;
            crate::checkpoint::write(store.data_dir(), &last_id)?;
            *checkpoint = last_id;
            log::info!(
                "flushed {} events (checkpoint={})",
                write_entries.len(),
                *checkpoint
            );
            Ok(Step::Flushed(write_entries.len()))
        }
        Err(e) => {
            // CHECKPOINT never advances while disconnected: we only reach
            // here with the watermark unchanged.
            let wait = backoff.next_wait();
            log::warn!(
                "stream read failed: {e} — retrying in {} ms (checkpoint unchanged at {checkpoint})",
                wait.as_millis()
            );
            Ok(Step::BackingOff(wait))
        }
    }
}

/// Run the follow loop until `stop` is set. `sleep` is injected so tests can
/// record waits without sleeping; the production closure parks in short
/// slices so a signal interrupts promptly.
pub fn run<S: StreamSource>(
    source: &mut S,
    stream: &str,
    store: &mut Store,
    initial_checkpoint: &str,
    opts: &FollowOptions,
    stop: &AtomicBool,
    sleep: &mut dyn FnMut(Duration),
) -> Result<(), Error> {
    let mut checkpoint = initial_checkpoint.to_string();
    let mut backoff = Backoff::new(opts.jitter);
    loop {
        if stop.load(Ordering::Relaxed) {
            log::info!("stop requested — clean exit at checkpoint {checkpoint}");
            return Ok(());
        }
        match step(source, stream, &mut checkpoint, store, opts, &mut backoff)? {
            Step::Idle | Step::Flushed(_) => {}
            Step::BackingOff(wait) => sleep(wait),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint;
    use crate::raw::Store;
    use crate::stream::{FakeSource, StreamError};

    fn entry(id: &str) -> crate::stream::StreamEntry {
        FakeSource::entry(
            id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
            ],
        )
    }

    fn lines_in(dir: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        let raw = dir.join("raw");
        if !raw.exists() {
            return out;
        }
        for dt in std::fs::read_dir(raw).unwrap() {
            let dt = dt.unwrap().path();
            for f in std::fs::read_dir(dt).unwrap() {
                let f = f.unwrap().path();
                out.extend(
                    std::fs::read_to_string(&f)
                        .unwrap()
                        .lines()
                        .map(|s| s.to_string()),
                );
            }
        }
        out
    }

    /// Drive `step` until the fake's script is exhausted.
    fn drive(
        src: &mut FakeSource,
        store: &mut Store,
        initial_cp: &str,
        waits: &mut Vec<u64>,
    ) -> Result<Vec<Step>, Error> {
        let opts = FollowOptions {
            jitter: false,
            ..Default::default()
        };
        let mut cp = initial_cp.to_string();
        let mut backoff = Backoff::new(false);
        let mut steps = Vec::new();
        loop {
            let s = step(src, "office:events", &mut cp, store, &opts, &mut backoff)?;
            if let Step::BackingOff(w) = &s {
                waits.push(w.as_millis() as u64)
            }
            steps.push(s);
            if src.drained() {
                return Ok(steps);
            }
        }
    }

    #[test]
    fn backoff_sequence_and_reset() {
        let mut b = Backoff::new(false);
        let mut seq = Vec::new();
        for _ in 0..10 {
            seq.push(b.next_wait().as_millis() as u64);
        }
        assert_eq!(
            seq,
            vec![1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000, 60000]
        );
        b.reset();
        assert_eq!(b.next_wait().as_millis() as u64, 1000);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let mut b = Backoff::new(true);
        for _ in 0..200 {
            let w = b.next_wait().as_millis() as i64;
            assert!((1000..=60000).contains(&w), "wait {w} out of bounds");
        }
    }

    #[test]
    fn dedupe_rule() {
        assert!(should_skip("5-0", "5-0").unwrap(), "equal is skipped");
        assert!(should_skip("4-0", "5-0").unwrap());
        assert!(
            should_skip("5-0", "5-1").unwrap(),
            "same ms, lower seq skipped"
        );
        assert!(!should_skip("6-0", "5-0").unwrap());
        assert!(!should_skip("5-1", "5-0").unwrap());
        assert!(!should_skip("1-0", "0").unwrap());
        assert!(
            should_skip("garbage", "0").is_err(),
            "unparsable id is fatal"
        );
    }

    #[test]
    fn flushes_batch_and_advances_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("1-0"), entry("2-0"), entry("3-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert_eq!(steps, vec![Step::Flushed(3)]);
        assert!(waits.is_empty());
        let lines = lines_in(dir.path());
        assert_eq!(lines.len(), 3, "one flush per batch — all 3 lines at once");
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "3-0");
    }

    #[test]
    fn writes_office_and_team_views() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("1-0")]));
        let mut waits = Vec::new();
        drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert!(dir.path().join("raw/dt=1970-01-01/events.jsonl").is_file());
        assert!(dir
            .path()
            .join("teams/dev-1/raw/dt=1970-01-01/events.jsonl")
            .is_file());
    }

    #[test]
    fn redis_down_never_advances_checkpoint_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Err(StreamError::Unavailable("down".into())));
        src.push(Err(StreamError::Unavailable("down".into())));
        src.push(Err(StreamError::Unavailable("down".into())));
        src.push(Ok(vec![entry("1-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert_eq!(steps[0], Step::BackingOff(Duration::from_millis(1000)));
        assert_eq!(steps[1], Step::BackingOff(Duration::from_millis(2000)));
        assert_eq!(steps[2], Step::BackingOff(Duration::from_millis(4000)));
        assert_eq!(steps[3], Step::Flushed(1));
        assert_eq!(waits, vec![1000, 2000, 4000], "1s→2s→4s backoff");
        assert_eq!(
            checkpoint::read(dir.path()).unwrap(),
            "1-0",
            "checkpoint advanced only after recovery"
        );
        assert_eq!(lines_in(dir.path()).len(), 1);
    }

    #[test]
    fn checkpoint_unchanged_while_disconnected() {
        let dir = tempfile::tempdir().unwrap();
        // pre-existing checkpoint from an earlier run
        checkpoint::write(dir.path(), "7-0").unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Err(StreamError::Unavailable("down".into())));
        let mut waits = Vec::new();
        drive(&mut src, &mut store, "7-0", &mut waits).unwrap();
        assert_eq!(
            checkpoint::read(dir.path()).unwrap(),
            "7-0",
            "never moved while down"
        );
        assert!(lines_in(dir.path()).is_empty());
    }

    #[test]
    fn empty_batch_is_noop_and_resets_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Err(StreamError::Unavailable("down".into())));
        src.push(Ok(vec![])); // empty read = success → backoff reset
        src.push(Ok(vec![entry("1-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert_eq!(steps[0], Step::BackingOff(Duration::from_millis(1000)));
        assert_eq!(steps[1], Step::Idle);
        assert_eq!(steps[2], Step::Flushed(1));
        assert_eq!(waits, vec![1000], "only the error waited");
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
    }

    #[test]
    fn dedupe_against_existing_checkpoint_skips_overlap() {
        let dir = tempfile::tempdir().unwrap();
        // simulate a re-read where the batch overlaps the flushed watermark
        checkpoint::write(dir.path(), "5-0").unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("4-0"), entry("5-0"), entry("6-0")]));
        let mut waits = Vec::new();
        drive(&mut src, &mut store, "5-0", &mut waits).unwrap();
        let lines = lines_in(dir.path());
        assert_eq!(lines.len(), 1, "only 6-0 written; 4-0/5-0 skipped");
        assert!(lines[0].contains("\"stream_id\":\"6-0\""));
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "6-0");
    }

    #[test]
    fn all_deduped_batch_is_idle_and_keeps_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        checkpoint::write(dir.path(), "9-0").unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("8-0"), entry("9-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, "9-0", &mut waits).unwrap();
        assert_eq!(steps, vec![Step::Idle], "nothing new → idle, no flush");
        assert!(lines_in(dir.path()).is_empty());
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "9-0");
    }

    #[test]
    fn decode_failure_keeps_event_with_decode_ok_false() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![FakeSource::entry(
            "1-0",
            &[("json", "{broken"), ("action", "task.started")],
        )]));
        let mut waits = Vec::new();
        drive(&mut src, &mut store, "0", &mut waits).unwrap();
        let lines = lines_in(dir.path());
        assert_eq!(lines.len(), 1, "decode failure must not drop the event");
        let row: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(row["decode_ok"], false);
        assert_eq!(row["stream_id"], "1-0");
    }

    #[test]
    fn run_loop_exits_on_stop_flag() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("1-0")]));
        src.push(Ok(vec![])); // would spin forever without stop
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let stopper = {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(80));
                stop.store(true, Ordering::Relaxed);
            })
        };
        let mut sleeper = |_: Duration| panic!("no errors → sleeper must not run");
        run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &FollowOptions {
                jitter: false,
                ..Default::default()
            },
            &stop,
            &mut sleeper,
        )
        .unwrap();
        stopper.join().unwrap();
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
    }

    #[test]
    fn run_loop_exits_on_stop_flag_after_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push(Err(StreamError::Unavailable("down".into())));
        let stop = AtomicBool::new(false);
        let mut sleeper = |_: Duration| stop.store(true, Ordering::Relaxed);
        run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &FollowOptions {
                jitter: false,
                ..Default::default()
            },
            &stop,
            &mut sleeper,
        )
        .unwrap();
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "0");
    }

    #[test]
    fn stop_before_any_read_exits_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        let stop = AtomicBool::new(true);
        let mut sleeper = |_: Duration| panic!("must not sleep when stopping");
        run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &FollowOptions {
                jitter: false,
                ..Default::default()
            },
            &stop,
            &mut sleeper,
        )
        .unwrap();
        assert!(lines_in(dir.path()).is_empty());
    }
}
