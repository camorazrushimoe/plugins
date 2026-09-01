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
use crate::pairing::Pairer;
use crate::raw::{Store, WriteEntry};
use crate::sessions::SessionStore;
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

/// §3.1 dedupe: skip any entry whose stream_id is `<=` the last flushed
/// checkpoint. Stream ids are monotonic per stream.
fn should_skip(id: &str, checkpoint: &str) -> Result<bool, Error> {
    let id = crate::streamid::StreamId::parse(id)
        .ok_or_else(|| Error::Fatal(format!("unparsable stream id from Redis: {id:?}")))?;
    let cp = crate::streamid::StreamId::parse(checkpoint)
        .ok_or_else(|| Error::Fatal(format!("unparsable checkpoint: {checkpoint:?}")))?;
    Ok(id <= cp)
}

/// Options for one follow run (defaults per §3).
#[derive(Debug, Clone)]
pub struct FollowOptions {
    pub block_ms: u64,
    pub count: usize,
    pub jitter: bool,
    /// `--max-reads N` / `--once` (§3.4): stop cleanly after N XREAD batches
    /// (empty batches count — each read iteration is one batch).
    pub max_reads: Option<usize>,
    /// `--max-idle-ms MS` (§3.4): stop cleanly when no event arrives for MS,
    /// checked after each read iteration. 0 = immediate stop.
    pub max_idle_ms: Option<u64>,
}

impl Default for FollowOptions {
    fn default() -> Self {
        FollowOptions {
            block_ms: BLOCK_MS,
            count: COUNT,
            jitter: true,
            max_reads: None,
            max_idle_ms: None,
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
/// write → fsync JSONL → atomic CHECKPOINT → advance). Every decoded
/// `task.started` / `task.finished` is fed to `pairer` (§5.3); the session
/// upserts and expiry aging run in [`run`] on every iteration, including
/// empty rounds.
pub fn step<S: StreamSource>(
    source: &mut S,
    stream: &str,
    checkpoint: &mut String,
    store: &mut Store,
    opts: &FollowOptions,
    backoff: &mut Backoff,
    pairer: &mut Pairer,
) -> Result<Step, Error> {
    match source.xread(stream, checkpoint, opts.block_ms, opts.count) {
        Ok(entries) => {
            if entries.is_empty() {
                // §3: distinguish a quiet existing stream (idle, backoff
                // reset) from a missing stream key (log + backoff, like Redis
                // being down — never a silent idle, never an exit).
                match source.stream_exists(stream) {
                    Ok(true) => {
                        backoff.reset();
                        return Ok(Step::Idle);
                    }
                    Ok(false) => {
                        let wait = backoff.next_wait();
                        log::warn!(
                            "stream key {stream:?} is missing — retrying in {} ms (checkpoint unchanged at {checkpoint})",
                            wait.as_millis()
                        );
                        return Ok(Step::BackingOff(wait));
                    }
                    Err(e) => {
                        let wait = backoff.next_wait();
                        log::warn!(
                            "stream check failed: {e} — retrying in {} ms",
                            wait.as_millis()
                        );
                        return Ok(Step::BackingOff(wait));
                    }
                }
            }
            backoff.reset();
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
                // §5.3: only task.started / task.finished pair; every other
                // action is ignored by the assembler.
                pairer.ingest(&decoded)?;
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

/// Injected time primitives for the follow loop: `sleep` waits during
/// backoff and `now` reads the clock. `now` is a **wall clock**
/// (`chrono::DateTime<Utc>`) because it serves two masters — §5.3 expiry
/// aging (wall-clock elapsed since `started_at`) and the §3.4
/// `--max-idle-ms` silence timer. Production passes `chrono::Utc::now`;
/// tests inject a controllable stand-in so expiry and stop-contract
/// scenarios are deterministic.
pub struct LoopTime<'a> {
    pub sleep: &'a mut dyn FnMut(Duration),
    pub now: &'a mut dyn FnMut() -> chrono::DateTime<chrono::Utc>,
}

/// Run the follow loop until a stop condition fires. All stop triggers
/// (§3.4: signal, `--once`, `--max-reads`, `--max-idle-ms`) exit through the
/// same clean path: the in-flight batch has already been flushed +
/// CHECKPOINTed inside `step`, and `run` returns `Ok(())` → exit 0.
///
/// §5.3 pairing is fully wired in: every decoded event is fed to `pairer`
/// inside `step`, and after **every** read iteration — including empty rounds
/// and the final iteration before a stop trigger exits — the pool is aged
/// and the touched `sessions.jsonl` partitions are upserted. A clean stop
/// never skips the pairing upsert/expiry (BUG-WFDC-007 seam).
#[allow(clippy::too_many_arguments)] // the follow loop's collaborators are all distinct seams (§2/§3/§5.3)
pub fn run<S: StreamSource>(
    source: &mut S,
    stream: &str,
    store: &mut Store,
    initial_checkpoint: &str,
    opts: &FollowOptions,
    stop: &AtomicBool,
    time: &mut LoopTime,
    pairer: &mut Pairer,
    session_store: &SessionStore,
) -> Result<(), Error> {
    let mut checkpoint = initial_checkpoint.to_string();
    let mut backoff = Backoff::new(opts.jitter);
    let mut reads: usize = 0;
    let mut last_event = (time.now)();
    loop {
        if stop.load(Ordering::Relaxed) {
            log::info!("stop requested — clean exit at checkpoint {checkpoint}");
            return Ok(());
        }
        // §3.4: every read iteration (Idle or Flushed) is one XREAD batch and
        // is checked against the stop conditions; BackingOff is not a read.
        match step(
            source,
            stream,
            &mut checkpoint,
            store,
            opts,
            &mut backoff,
            pairer,
        )? {
            Step::Idle => reads += 1,
            Step::Flushed(_) => {
                reads += 1;
                // Events arrived: reset the idle baseline so the timer
                // measures silence since the last event.
                last_event = (time.now)();
            }
            Step::BackingOff(wait) => {
                (time.sleep)(wait);
                continue;
            }
        }
        // §5.3: expiry aging on every read iteration, incl. empty rounds; the
        // upsert rewrites each touched day's sessions.jsonl atomically. This
        // runs on the final iteration too — a stop trigger below can never
        // skip the pairing upsert/expiry (BUG-WFDC-007 seam).
        pairer.age((time.now)());
        let writes = pairer.take_writes();
        session_store.upsert(&writes)?;
        // Common stop-contract tail — identical for every trigger (§3.4:
        // one clean path → flush+CHECKPOINT already done in `step` → exit 0).
        if max_reads_reached(opts, reads, &checkpoint) {
            return Ok(());
        }
        if idle_exceeded(opts, (time.now)(), last_event, &checkpoint) {
            return Ok(());
        }
    }
}

/// §3.4 `--once` / `--max-reads N`: stop cleanly after N XREAD batches.
fn max_reads_reached(opts: &FollowOptions, reads: usize, checkpoint: &str) -> bool {
    if let Some(n) = opts.max_reads {
        if reads >= n {
            log::info!("max-reads reached ({reads}) — clean stop at checkpoint {checkpoint}");
            return true;
        }
    }
    false
}

/// §3.4 `--max-idle-ms MS`: stop cleanly when no event arrives for MS
/// (checked after each read iteration; events reset the timer).
fn idle_exceeded(
    opts: &FollowOptions,
    now: chrono::DateTime<chrono::Utc>,
    last_event: chrono::DateTime<chrono::Utc>,
    checkpoint: &str,
) -> bool {
    if let Some(ms) = opts.max_idle_ms {
        let elapsed_ms = now.signed_duration_since(last_event).num_milliseconds() as u64;
        if elapsed_ms >= ms {
            log::info!(
                "no event for {ms} ms (idle {elapsed_ms} ms) — clean stop at checkpoint {checkpoint}"
            );
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint;
    use crate::raw::Store;
    use crate::stream::{FakeSource, StreamEntry, StreamError};

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
        let mut pairer = Pairer::new(6);
        let mut steps = Vec::new();
        loop {
            let s = step(
                src,
                "office:events",
                &mut cp,
                store,
                &opts,
                &mut backoff,
                &mut pairer,
            )?;
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
        src.push(Ok(vec![])); // empty read on an existing stream = success → backoff reset
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
    fn missing_stream_key_logs_and_backs_off_never_idles() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push_exists(false);
        src.push(Ok(vec![])); // empty read, key missing
        src.push_exists(false);
        src.push(Ok(vec![])); // still missing
        src.push_exists(false);
        src.push(Ok(vec![])); // still missing
        src.push_exists(true);
        src.push(Ok(vec![])); // key appears → idle
        src.push(Ok(vec![entry("1-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert_eq!(
            steps[0..3],
            [
                Step::BackingOff(Duration::from_millis(1000)),
                Step::BackingOff(Duration::from_millis(2000)),
                Step::BackingOff(Duration::from_millis(4000)),
            ],
            "missing key → 1s→2s→4s backoff"
        );
        assert_eq!(steps[3], Step::Idle, "key appeared → quiet idle");
        assert_eq!(steps[4], Step::Flushed(1));
        assert_eq!(waits, vec![1000, 2000, 4000]);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
    }

    #[test]
    fn missing_stream_key_never_advances_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let mut src = FakeSource::new();
        src.push_exists(false);
        src.push(Ok(vec![]));
        let mut waits = Vec::new();
        drive(&mut src, &mut store, "0", &mut waits).unwrap();
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "0");
        assert!(lines_in(dir.path()).is_empty());
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

    /// The ticket's at-least-once integration scenario, at the unit seam:
    /// a crash between appending a batch and writing CHECKPOINT leaves rows on
    /// disk ahead of the durable checkpoint; resuming from
    /// `max(durable, max written)` must not re-write them.
    #[test]
    fn crash_window_resume_skips_rows_already_written() {
        fn wentry(id: &str) -> crate::raw::WriteEntry {
            let flat: std::collections::BTreeMap<String, String> = [
                ("action".to_string(), "task.started".to_string()),
                ("actor".to_string(), "dev".to_string()),
                ("team".to_string(), "dev-1".to_string()),
            ]
            .into();
            let d = crate::decode::decode(id, &flat);
            crate::raw::WriteEntry {
                team_safe: crate::team::team_safe(d.line.team.as_deref(), d.line.actor.as_deref()),
                dt: crate::dt::dt_for(id, d.line.ts.as_deref()),
                line: d.line,
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        // pre-crash state: rows 1-0..4-0 on disk, CHECKPOINT lagging at 2-0
        // (batch 1 [1-0,2-0] was flushed+checkpointed; batch 2 [3-0,4-0] was
        // appended and fsynced, then the process died before the checkpoint).
        store.write_batch(&[wentry("1-0"), wentry("2-0")]).unwrap();
        checkpoint::write(dir.path(), "2-0").unwrap();
        store.write_batch(&[wentry("3-0"), wentry("4-0")]).unwrap();
        // resume point = max(durable CHECKPOINT, highest id written to JSONL)
        let durable = checkpoint::read(dir.path()).unwrap();
        let max_written = store.max_written_stream_id().unwrap();
        let start = crate::checkpoint::resume_start(&durable, max_written.as_deref());
        assert_eq!(start, "4-0", "resume past the crash-written rows");
        // the stream re-read returns the crash batch plus one new event
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("3-0"), entry("4-0"), entry("5-0")]));
        let mut waits = Vec::new();
        let steps = drive(&mut src, &mut store, &start, &mut waits).unwrap();
        assert_eq!(steps, vec![Step::Flushed(1)], "only 5-0 is fresh");
        let lines = lines_in(dir.path());
        assert_eq!(lines.len(), 5, "1-0..5-0 exactly once — no duplicates");
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "5-0");
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
        let mut pairer = Pairer::new(6);
        let session_store = SessionStore::new(dir.path());
        let mut now = || chrono::Utc::now();
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
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
            &mut time,
            &mut pairer,
            &session_store,
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
        let mut pairer = Pairer::new(6);
        let session_store = SessionStore::new(dir.path());
        let mut now = || chrono::Utc::now();
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
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
            &mut time,
            &mut pairer,
            &session_store,
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
        let mut pairer = Pairer::new(6);
        let session_store = SessionStore::new(dir.path());
        let mut now = || chrono::Utc::now();
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
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
            &mut time,
            &mut pairer,
            &session_store,
        )
        .unwrap();
        assert!(lines_in(dir.path()).is_empty());
    }

    // --- BON-71 §3.4: deterministic stop contract --------------------------

    /// FakeSource wrapper that simulates the XREAD BLOCK timeout on a quiet
    /// stream: each **empty** read advances the shared clock by `block_ms`
    /// (a non-empty read returns immediately, so the clock does not move).
    /// This makes `--max-idle-ms` tests deterministic without real sleeping.
    struct BlockingClockSource {
        src: FakeSource,
        // One injected wall clock (DateTime<Utc>) shared with the follow loop:
        // each empty read advances it by `block_ms`, so `--max-idle-ms` and
        // §5.3 expiry scenarios are deterministic without real sleeping.
        clock: std::sync::Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
        block_ms: u64,
    }

    impl BlockingClockSource {
        fn new(
            block_ms: u64,
        ) -> (
            Self,
            std::sync::Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
        ) {
            let clock = std::sync::Arc::new(std::sync::Mutex::new(chrono::Utc::now()));
            (
                BlockingClockSource {
                    src: FakeSource::new(),
                    clock: std::sync::Arc::clone(&clock),
                    block_ms,
                },
                clock,
            )
        }
    }

    impl StreamSource for BlockingClockSource {
        fn xread(
            &mut self,
            stream: &str,
            from: &str,
            block_ms: u64,
            count: usize,
        ) -> Result<Vec<StreamEntry>, StreamError> {
            let out = self.src.xread(stream, from, block_ms, count);
            if matches!(&out, Ok(v) if v.is_empty()) {
                *self.clock.lock().unwrap() += chrono::Duration::milliseconds(self.block_ms as i64);
            }
            out
        }

        fn stream_exists(&mut self, stream: &str) -> Result<bool, StreamError> {
            self.src.stream_exists(stream)
        }

        fn xrange(
            &mut self,
            stream: &str,
            from: &str,
            to: &str,
            count: usize,
        ) -> Result<Vec<StreamEntry>, StreamError> {
            // xrange is backfill-only; the stop-contract tests never call it.
            self.src.xrange(stream, from, to, count)
        }
    }

    /// Run `follow::run` with a stop contract and a controllable clock. The
    /// pairer/session store are internal here (no rows are seeded in these
    /// scenarios); the §5.3 pairing+expiry wiring has its own tests below.
    fn run_contract(
        src: &mut impl StreamSource,
        store: &mut Store,
        initial_cp: &str,
        opts: &FollowOptions,
        clock: &std::sync::Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
    ) -> Result<(), Error> {
        let stop = AtomicBool::new(false);
        let mut sleeper = |_: Duration| { /* no backoff in these scripts */ };
        let mut now = {
            let clock = std::sync::Arc::clone(clock);
            move || *clock.lock().unwrap()
        };
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        let mut pairer = Pairer::new(6);
        let session_store = SessionStore::new(store.data_dir());
        run(
            src,
            "office:events",
            store,
            initial_cp,
            opts,
            &stop,
            &mut time,
            &mut pairer,
            &session_store,
        )
    }

    fn opts_with(max_reads: Option<usize>, max_idle_ms: Option<u64>) -> FollowOptions {
        FollowOptions {
            jitter: false,
            max_reads,
            max_idle_ms,
            ..Default::default()
        }
    }

    #[test]
    fn once_is_one_batch_then_clean_stop() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, _clock) = BlockingClockSource::new(1000);
        src.src
            .push(Ok(vec![entry("1-0"), entry("2-0"), entry("3-0")]));
        // --once ≡ --max-reads 1: exactly one XREAD batch, then exit 0.
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(Some(1), None),
            &std::sync::Arc::new(std::sync::Mutex::new(chrono::Utc::now())),
        )
        .unwrap();
        let lines = lines_in(dir.path());
        assert_eq!(lines.len(), 3, "one batch flushed");
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "3-0");
    }

    #[test]
    fn once_on_empty_stream_is_one_empty_batch_and_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // --once on an empty stream: one (empty) batch, exit 0, no CHECKPOINT.
        run_contract(&mut src, &mut store, "0", &opts_with(Some(1), None), &clock).unwrap();
        assert!(lines_in(dir.path()).is_empty());
        assert!(
            !dir.path().join("CHECKPOINT").exists(),
            "no flush → no CHECKPOINT file"
        );
    }

    #[test]
    fn max_reads_counts_empty_rounds() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, _clock) = BlockingClockSource::new(1000);
        // MAX-1(a): 1 event pending, --max-reads 2 → round 1 flushes E1,
        // round 2 blocks ~1s and returns empty → exit 0, CHECKPOINT = E1.
        src.src.push(Ok(vec![entry("1-0")]));
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(Some(2), None),
            &std::sync::Arc::new(std::sync::Mutex::new(chrono::Utc::now())),
        )
        .unwrap();
        assert_eq!(lines_in(dir.path()).len(), 1);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
    }

    #[test]
    fn max_reads_three_empty_rounds_stops_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, _clock) = BlockingClockSource::new(1000);
        // MAX-1(b): --max-reads 3 on an empty stream → 3 empty rounds, exit 0.
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(Some(3), None),
            &std::sync::Arc::new(std::sync::Mutex::new(chrono::Utc::now())),
        )
        .unwrap();
        assert!(lines_in(dir.path()).is_empty());
        assert!(!dir.path().join("CHECKPOINT").exists());
    }

    #[test]
    fn max_reads_does_not_count_backoff_rounds() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, _clock) = BlockingClockSource::new(1000);
        // Redis down once (BackingOff — NOT a batch), then one real batch.
        src.src.push(Err(StreamError::Unavailable("down".into())));
        src.src.push(Ok(vec![entry("1-0")]));
        let mut waits = Vec::new();
        let stop = AtomicBool::new(false);
        let mut sleeper = |d: Duration| waits.push(d.as_millis() as u64);
        let mut now = || chrono::Utc::now();
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        let mut pairer = Pairer::new(6);
        let session_store = SessionStore::new(dir.path());
        run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &opts_with(Some(1), None),
            &stop,
            &mut time,
            &mut pairer,
            &session_store,
        )
        .unwrap();
        assert_eq!(waits, vec![1000], "backoff ran once");
        assert_eq!(
            lines_in(dir.path()).len(),
            1,
            "the batch was the 1 counted read"
        );
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
    }

    #[test]
    fn max_idle_stops_after_idle_window() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // IDL-1: empty stream, --max-idle-ms 2000 → 2 empty rounds (each
        // simulating a 1000ms BLOCK) then clean stop; no busy-loop.
        let base = *clock.lock().unwrap();
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(None, Some(2000)),
            &clock,
        )
        .unwrap();
        assert!(lines_in(dir.path()).is_empty());
        assert!(!dir.path().join("CHECKPOINT").exists());
        // The clock advanced exactly two block windows → ~2000ms elapsed.
        let elapsed = clock
            .lock()
            .unwrap()
            .signed_duration_since(base)
            .num_milliseconds();
        assert!((1900..=2100).contains(&elapsed), "elapsed {elapsed}ms");
    }

    #[test]
    fn max_idle_events_reset_the_timer() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // IDL-3: events keep the run alive past the window; the timer resets
        // on every flush. Script: event at t=0 (flush), then two empty rounds
        // (t=1000, t=2000). max_idle=1500 → stop after the round at t=2000.
        let base = *clock.lock().unwrap();
        src.src.push(Ok(vec![entry("1-0")]));
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(None, Some(1500)),
            &clock,
        )
        .unwrap();
        assert_eq!(lines_in(dir.path()).len(), 1, "the event was flushed");
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
        let elapsed = clock
            .lock()
            .unwrap()
            .signed_duration_since(base)
            .num_milliseconds();
        assert!(
            (1900..=2100).contains(&elapsed),
            "stopped ~2s after start: {elapsed}ms"
        );
    }

    #[test]
    fn max_idle_zero_is_immediate_stop() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // IDL-4 pin: --max-idle-ms 0 = zero idle tolerance → stop after the
        // first read iteration (a flush), before any further XREAD.
        src.src.push(Ok(vec![entry("1-0")]));
        let base = *clock.lock().unwrap();
        run_contract(&mut src, &mut store, "0", &opts_with(None, Some(0)), &clock).unwrap();
        assert_eq!(lines_in(dir.path()).len(), 1);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
        let elapsed = clock
            .lock()
            .unwrap()
            .signed_duration_since(base)
            .num_milliseconds();
        assert!(elapsed < 1900, "no extra idle rounds: elapsed {elapsed}ms");
    }

    #[test]
    fn max_reads_and_max_idle_both_apply_first_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // max_reads=1 wins before the 3000ms idle window elapses.
        src.src.push(Ok(vec![entry("1-0")]));
        let base = *clock.lock().unwrap();
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(Some(1), Some(3000)),
            &clock,
        )
        .unwrap();
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1-0");
        let elapsed = clock
            .lock()
            .unwrap()
            .signed_duration_since(base)
            .num_milliseconds();
        assert!(elapsed < 1900, "max_reads stopped first: {elapsed}ms");
    }

    // --- BUG-WFDC-007 seam regression: the §3.4 clean-stop path must not ----
    // --- skip the §5.3 pairing upsert / expiry aging.                    ----

    /// The stop path must not skip the pairing upsert: `--max-reads 1`
    /// (≡ `--once`) flushes a `task.started`; the open session row must be
    /// written to `sessions.jsonl` before the clean stop exits.
    #[test]
    fn max_reads_stop_path_flushes_pairing_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let session_store = SessionStore::new(dir.path());
        let (mut src, _clock) = BlockingClockSource::new(1000);
        // A task.started with a fresh RFC 3339 timestamp: the 6h expiry
        // window must NOT age this row mid-run — the assertion is on the
        // pairing upsert landing before the clean stop.
        let ts = chrono::Utc::now().to_rfc3339();
        src.src.push(Ok(vec![FakeSource::entry(
            "1-0",
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("timestamp", ts.as_str()),
            ],
        )]));
        run_contract(
            &mut src,
            &mut store,
            "0",
            &opts_with(Some(1), None),
            &std::sync::Arc::new(std::sync::Mutex::new(chrono::Utc::now())),
        )
        .unwrap();
        let rows = session_store.load_all().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the open start row was upserted before the clean stop"
        );
        assert_eq!(rows[0].state, crate::sessions::State::Open);
        assert_eq!(rows[0].start_stream_id.as_deref(), Some("1-0"));
    }

    /// Expiry must run on a quiet stream even when `--max-idle-ms` triggers
    /// the clean stop: the stop happens after a read iteration, and the §5.3
    /// aging + upsert run on that iteration (the BUG-WFDC-007 seam gap named
    /// in QA: "expiry not running on a quiet stream with --max-idle-ms").
    #[test]
    fn idle_stop_still_ages_expiry_on_quiet_stream() {
        let dir = tempfile::tempdir().unwrap();
        let session_store = SessionStore::new(dir.path());
        // Seed an `open` start whose started_at is 2h in the past — with a
        // 1h expiry window it must be aged to `expired` during the run.
        let started_at = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
        let open_row = crate::sessions::SessionRow {
            session_pk: "pk-seed-open".into(),
            team: "dev-1".into(),
            actor: "dev".into(),
            session_id: None,
            start_stream_id: Some("1-0".into()),
            finish_stream_id: None,
            started_at: Some(started_at.clone()),
            finished_at: None,
            duration_ms: None,
            state: crate::sessions::State::Open,
            snippet_in: None,
            snippet_out: None,
            issues: None,
            prs: None,
            linear: None,
            handoff: None,
            project: None,
        };
        let loc = open_row.location();
        session_store
            .upsert(&[crate::sessions::SessionWrite {
                team_folder: loc.0.clone(),
                dt: loc.1.clone(),
                rows: vec![open_row.clone()],
            }])
            .unwrap();
        let mut store = Store::open(dir.path()).unwrap();
        let (mut src, clock) = BlockingClockSource::new(1000);
        // Quiet stream: only empty rounds. --max-idle-ms 0 stops after the
        // first read iteration — the aging/upsert on that iteration must
        // still land (expiry runs before the clean stop exits).
        let stop = AtomicBool::new(false);
        let mut sleeper = |_: Duration| { /* no backoff in this script */ };
        let mut now = {
            let clock = std::sync::Arc::clone(&clock);
            move || *clock.lock().unwrap()
        };
        let mut time = LoopTime {
            sleep: &mut sleeper,
            now: &mut now,
        };
        let mut pairer = Pairer::new(1); // 1h expiry window
        pairer.rebuild(session_store.load_all().unwrap());
        run(
            &mut src,
            "office:events",
            &mut store,
            "0",
            &opts_with(None, Some(0)),
            &stop,
            &mut time,
            &mut pairer,
            &session_store,
        )
        .unwrap();
        let rows = session_store.load_all().unwrap();
        assert_eq!(rows.len(), 1, "the seeded open row is still on disk");
        assert_eq!(
            rows[0].state,
            crate::sessions::State::Expired,
            "expiry aging ran on the quiet idle round before the clean stop"
        );
    }
}
