//! `wfdc backfill [--from STREAM_ID] [--to STREAM_ID]` (§3.5, §9 item 7).
//!
//! Replays a **chosen range** of the stream (first install, rebuilt derived
//! tables, a trimmed stream recovered elsewhere) — automatic catch-up is the
//! checkpoint's job. It writes raw + session rows with the **same writer,
//! decoder and pairing rules as follow** (§4.1, §5.1, §5.3).
//!
//! Semantics (pinned in SPEC.md §3.5):
//! - The range `[from, to]` is inclusive on both ends (Redis XRANGE
//!   semantics). Defaults: `--from 0` (stream start), `--to +` (stream end).
//! - **Dedupe (§3.1) applies.** An entry whose `stream_id` is `<=` the
//!   resume point — `max(durable CHECKPOINT, highest id already written to
//!   JSONL)` — is skipped, exactly like follow. A crash between appending a
//!   batch and writing CHECKPOINT leaves rows on disk whose ids the
//!   CHECKPOINT file has not caught up to; skipping everything at/below the
//!   written watermark makes a re-run after such a crash duplicate-free.
//!   Re-running a range never duplicates rows, and a range that sits
//!   entirely at/below the resume point writes nothing.
//! - The pairing pool is **rebuilt from the session rows already on disk**
//!   before the range is processed, so a finish inside the range still pairs
//!   with a start that was flushed earlier (`<=` checkpoint) — the same
//!   cross-batch pool persistence follow has in memory. Skipped entries are
//!   never fed to the pool a second time.
//! - **CHECKPOINT is never moved backward.** It advances forward to
//!   `max(current, last backfilled stream id)` only after everything is on
//!   disk (§3.1 ordering), so follow-mode checkpoint semantics are
//!   undisturbed and follow will not re-read the backfilled range. A run
//!   that wrote nothing leaves CHECKPOINT untouched.
//! - **Expiry** (§5.3) is evaluated once against wall clock at the end of
//!   the range — backfill has no read iterations, so this reproduces the
//!   session state follow would have produced for the same events.
//! - **No partial output**: everything is staged in memory and flushed once
//!   (raw batch, then session partitions); on any error the command exits 1
//!   and the data_dir is left unchanged (no checkpoint advance).
//! - An empty or inverted range (`--from` after `--to`) writes nothing and
//!   exits 0. An invalid `--from`/`--to` is a config error → exit 1.

use chrono::{DateTime, Utc};

use crate::config::Config;
use crate::decode;
use crate::pairing::Pairer;
use crate::raw::{Store, WriteEntry};
use crate::sessions::SessionStore;
use crate::stream::StreamSource;
use crate::team;
use crate::Error;

/// XRANGE page size (memory-bounded pagination through the range).
pub const XRANGE_COUNT: usize = 512;

/// Result of one backfill run (informational; logged by the caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillOutcome {
    pub from: String,
    pub to: String,
    /// Raw rows written by this run (already-deduplicated entries skipped).
    pub raw_lines: u64,
    /// Total session rows in the pairing store after the run (on-disk pool
    /// rebuilt at startup, plus anything this run opened/closed).
    pub session_rows: u64,
    /// The CHECKPOINT after the run (unchanged when nothing was written).
    pub checkpoint: Option<String>,
}

/// Validate a `--from`/`--to` stream id: `0`, `-`, `+` or `<ms>-<seq>`.
fn validate_bound(s: &str, what: &str) -> Result<(), Error> {
    if s == "+" || s == "-" || s == "0" {
        return Ok(());
    }
    if crate::streamid::StreamId::parse(s).is_none() {
        return Err(Error::Config(format!(
            "{what} is not a valid stream id: {s:?} (expected <ms>-<seq>, 0, -, or +)"
        )));
    }
    Ok(())
}

/// §3.1 dedupe: an entry whose `stream_id` is `<=` the resume point (max of
/// the durable CHECKPOINT and the highest id already written to JSONL) was
/// already written — skip it. Stream ids are monotonic per stream.
fn at_or_below(id: &str, resume: &str) -> Result<bool, Error> {
    let id = crate::streamid::StreamId::parse(id)
        .ok_or_else(|| Error::Fatal(format!("unparsable stream id from Redis: {id:?}")))?;
    let r = crate::streamid::StreamId::parse(resume)
        .ok_or_else(|| Error::Fatal(format!("unparsable resume point: {resume:?}")))?;
    Ok(id <= r)
}

/// XRANGE cursor just after `id` (Redis exclusive-interval `(id` form), so
/// the next page starts at the first entry strictly after it.
fn after(id: &str) -> String {
    format!("({id}")
}

/// Run one backfill over `[from, to]` (inclusive, Redis XRANGE semantics).
///
/// `resume` is the dedupe watermark — `checkpoint::resume_start(durable,
/// max_written)` — computed by the caller (main, or the test harness) so the
/// caller owns the startup scan (§3.1 crash-window recovery) exactly as the
/// follow path does. `now` is the wall clock for §5.3 expiry, injected for
/// tests. `max_mb` is the §5.5 disk cap (already normalized, spec §2); it is
/// enforced after the run's single flush, exactly like follow enforces after
/// every flush. `cfg` feeds the §5.4 MANIFEST hook: the manifest is rewritten
/// once per run, after the final flush + cap enforcement.
#[allow(clippy::too_many_arguments)] // the backfill run's collaborators are distinct seams (§2/§3/§5.3)
pub fn run<S: StreamSource>(
    cfg: &Config,
    source: &mut S,
    stream: &str,
    store: &mut Store,
    session_store: &SessionStore,
    resume: &str,
    expire_hours: u64,
    max_mb: u64,
    from: &str,
    to: &str,
    now: &mut dyn FnMut() -> DateTime<Utc>,
) -> Result<BackfillOutcome, Error> {
    validate_bound(from, "--from")?;
    validate_bound(to, "--to")?;
    // An inverted range (--from after --to) is an empty range: XRANGE returns
    // nothing, no rows are written, exit 0 (§3.5).

    // §5.3: rebuild the pairing pool from the session rows already on disk —
    // a finish in this range must pair with a start flushed earlier
    // (<= checkpoint), exactly as follow's in-memory pool would.
    let mut pairer = Pairer::new(expire_hours);
    pairer.rebuild(session_store.load_all()?);

    let mut write_entries: Vec<WriteEntry> = Vec::new();
    // last_written drives the checkpoint (only actually written rows);
    // the XRANGE cursor advances past skipped entries via the page's last id.
    let mut last_written: Option<String> = None;
    let mut cursor = from.to_string();

    loop {
        let page = source
            .xrange(stream, &cursor, to, XRANGE_COUNT)
            .map_err(|e| Error::Fatal(e.to_string()))?;
        if page.is_empty() {
            break;
        }
        for e in &page {
            // Dedupe at write time (§3.1): skip stream_id <= the resume point.
            if at_or_below(&e.id, resume)? {
                continue;
            }
            let decoded = decode::decode(&e.id, &e.fields);
            if let Some(reason) = &decoded.decode_error {
                log::warn!(
                    "decode failure — stream_id={} reason={} (line kept with decode_ok=false)",
                    e.id,
                    reason
                );
            }
            pairer.ingest(&decoded)?;
            write_entries.push(WriteEntry {
                team_safe: team::team_safe(
                    decoded.line.team.as_deref(),
                    decoded.line.actor.as_deref(),
                ),
                dt: crate::dt::dt_for(&e.id, decoded.line.ts.as_deref()),
                line: decoded.line,
            });
            last_written = Some(e.id.clone());
        }
        if page.len() < XRANGE_COUNT {
            break;
        }
        cursor = after(page.last().expect("non-empty page").id.as_str());
    }

    // A run with nothing in range (empty/inverted range, or everything
    // at/below the resume point) is a byte-true no-op (§3.5 "writes
    // nothing"): expiry of pre-existing rows is follow's job, not a no-op
    // backfill's.
    let has_entries = !write_entries.is_empty();

    // §5.3 expiry: evaluated once at the end of the range (wall clock) so a
    // backfill of old data reproduces what follow would have produced.
    if has_entries {
        pairer.age(now());

        // Everything is staged in memory — flush once (raw batch, then
        // session partitions), then move the checkpoint forward-only (§3.1
        // ordering).
        store.write_batch(&write_entries)?;
        session_store.upsert(&pairer.take_writes())?;
    }

    // Nothing flushed → never touch the checkpoint (§3.5: untouched when
    // nothing is written). Otherwise any written id is > resume >= durable,
    // so advancing to last_written can never move the checkpoint backward.
    let new_cp = last_written.clone();
    if let Some(cp) = &new_cp {
        crate::checkpoint::write(store.data_dir(), cp)?;
    }

    // §5.5: after every successful flush, enforce the cap. A no-op backfill
    // wrote nothing, so there is nothing to enforce.
    if has_entries {
        crate::cap::enforce(store.data_dir(), max_mb, now().into());
        // §5.4: MANIFEST once per run, after the final flush + cap
        // enforcement (the drop-log ring entries from this trim are persisted
        // and visible). A no-op run flushed nothing → no manifest rewrite.
        crate::manifest::write(cfg)?;
    }

    let checkpoint = new_cp
        .clone()
        .or_else(|| crate::checkpoint::read(store.data_dir()).ok());
    let outcome = BackfillOutcome {
        from: from.to_string(),
        to: to.to_string(),
        raw_lines: write_entries.len() as u64,
        session_rows: pairer.row_count() as u64,
        checkpoint,
    };

    log::info!(
        "backfill {}..{}: {} raw rows, {} session rows, checkpoint {}",
        from,
        to,
        outcome.raw_lines,
        outcome.session_rows,
        outcome.checkpoint.as_deref().unwrap_or("-")
    );
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint;
    use crate::stream::{FakeSource, StreamEntry};
    use std::path::Path;

    fn entry(id: &str) -> StreamEntry {
        FakeSource::entry(
            id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
            ],
        )
    }

    fn start_sid(id: &str) -> StreamEntry {
        FakeSource::entry(
            id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
                ("timestamp", "2026-08-30T10:00:00Z"),
            ],
        )
    }

    fn finish(id: &str) -> StreamEntry {
        FakeSource::entry(
            id,
            &[
                ("action", "task.finished"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("session_id", "s1"),
            ],
        )
    }

    fn frozen_clock() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-30T21:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// Test Config for a temp store dir (stream matches the fixture stream).
    fn test_cfg(dir: &Path) -> Config {
        Config {
            redis_url: "redis://127.0.0.1:6380".into(),
            stream: "office:events".into(),
            data_dir: dir.to_path_buf(),
            max_mb: 100_000,
            expire_hours: 100_000,
        }
    }

    /// Drive `run` with a fresh store; returns the outcome.
    fn run_with(
        src: &mut FakeSource,
        dir: &Path,
        resume: &str,
        from: &str,
        to: &str,
    ) -> Result<BackfillOutcome, Error> {
        let mut store = Store::open(dir).unwrap();
        let session_store = SessionStore::new(dir);
        let mut now = || frozen_clock();
        run(
            &test_cfg(dir),
            src,
            "office:events",
            &mut store,
            &session_store,
            resume,
            100_000, // effectively no expiry for determinism tests
            100_000, // max_mb: no cap in these tests
            from,
            to,
            &mut now,
        )
    }

    fn raw_stream_ids(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let raw = dir.join("raw");
        if !raw.exists() {
            return out;
        }
        for dt in std::fs::read_dir(raw).unwrap() {
            let dt = dt.unwrap().path();
            for f in std::fs::read_dir(dt).unwrap() {
                let f = f.unwrap().path();
                if f.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    for line in std::fs::read_to_string(&f).unwrap().lines() {
                        if !line.is_empty() {
                            let v: serde_json::Value = serde_json::from_str(line).unwrap();
                            out.push(v["stream_id"].as_str().unwrap().to_string());
                        }
                    }
                }
            }
        }
        out
    }

    fn raw_lines(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let raw = dir.join("raw");
        if !raw.exists() {
            return out;
        }
        for dt in std::fs::read_dir(raw).unwrap() {
            let dt = dt.unwrap().path();
            for f in std::fs::read_dir(dt).unwrap() {
                let f = f.unwrap().path();
                if f.extension().map(|e| e == "jsonl").unwrap_or(false) {
                    out.extend(
                        std::fs::read_to_string(&f)
                            .unwrap()
                            .lines()
                            .map(|s| s.to_string()),
                    );
                }
            }
        }
        out
    }

    // --- validate_bound -------------------------------------------------------

    #[test]
    fn validate_bounds() {
        assert!(validate_bound("0", "--from").is_ok());
        assert!(validate_bound("+", "--to").is_ok());
        assert!(validate_bound("-", "--from").is_ok());
        assert!(validate_bound("1725062400000-0", "--from").is_ok());
        assert!(validate_bound("garbage", "--from").is_err());
        assert!(validate_bound("1725062400000", "--from").is_err()); // missing -seq
    }

    // --- range selection ------------------------------------------------------

    #[test]
    fn writes_only_the_chosen_range_and_advances_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        // Real XRANGE already filters to [from, to]; the fake scripts the
        // same page XRANGE would return for --from 2-0 --to 3-0.
        src.push(Ok(vec![entry("2-0"), entry("3-0")]));
        let out = run_with(&mut src, dir.path(), "0", "2-0", "3-0").unwrap();
        assert_eq!(out.raw_lines, 2);
        assert_eq!(
            raw_stream_ids(dir.path()),
            vec!["2-0".to_string(), "3-0".to_string()]
        );
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "3-0");
    }

    #[test]
    fn pages_until_short_page() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        // first page is exactly XRANGE_COUNT → loop asks for the next page
        let page1: Vec<StreamEntry> = (1..=XRANGE_COUNT)
            .map(|i| entry(&format!("{i}-0")))
            .collect();
        src.push(Ok(page1));
        src.push(Ok(vec![entry("513-0"), entry("514-0")]));
        let out = run_with(&mut src, dir.path(), "0", "1-0", "+").unwrap();
        assert_eq!(out.raw_lines, (XRANGE_COUNT + 2) as u64);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "514-0");
    }

    // --- dedupe / checkpoint (§3.1) -------------------------------------------

    #[test]
    fn entries_at_or_below_resume_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        // 7 entries; resume at 5-0 (ids 0..=5 already written).
        let page: Vec<StreamEntry> = (0..7).map(|i| entry(&format!("{i}-0"))).collect();
        src.push(Ok(page));
        let out = run_with(&mut src, dir.path(), "5-0", "0-0", "6-0").unwrap();
        assert_eq!(out.raw_lines, 1, "only 6-0 is above the resume point");
        assert_eq!(raw_stream_ids(dir.path()), vec!["6-0".to_string()]);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "6-0");
    }

    #[test]
    fn rerun_same_range_does_not_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        let page: Vec<StreamEntry> = (1000..1005).map(|i| entry(&format!("{i}-0"))).collect();
        src.push(Ok(page));
        let out1 = run_with(&mut src, dir.path(), "0", "0", "1004-0").unwrap();
        assert_eq!(out1.raw_lines, 5);
        assert_eq!(raw_stream_ids(dir.path()).len(), 5);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1004-0");

        // second run: resume = checkpoint (1004-0) → everything skipped
        let mut src2 = FakeSource::new();
        let page2: Vec<StreamEntry> = (1000..1005).map(|i| entry(&format!("{i}-0"))).collect();
        src2.push(Ok(page2));
        let out2 = run_with(&mut src2, dir.path(), "1004-0", "0", "1004-0").unwrap();
        assert_eq!(out2.raw_lines, 0, "all entries <= resume are skipped");
        assert_eq!(
            raw_stream_ids(dir.path()).len(),
            5,
            "no duplicates on re-run"
        );
        assert_eq!(
            checkpoint::read(dir.path()).unwrap(),
            "1004-0",
            "checkpoint unchanged"
        );
    }

    #[test]
    fn range_below_resume_writes_nothing_and_keeps_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        let page: Vec<StreamEntry> = (0..5).map(|i| entry(&format!("{i}-0"))).collect();
        src.push(Ok(page));
        let out = run_with(&mut src, dir.path(), "7-0", "0-0", "4-0").unwrap();
        assert_eq!(out.raw_lines, 0);
        assert!(raw_stream_ids(dir.path()).is_empty());
        assert!(
            !dir.path().join("CHECKPOINT").exists(),
            "no checkpoint written"
        );
    }

    // --- BUG-WFDC-002: JSONL-watermark resume at backfill startup -------------

    #[test]
    fn crash_window_rerun_after_manual_checkpoint_regress_does_not_duplicate() {
        // The DED repro, unit form: seed 1000-0..1006-0, backfill, then the
        // durable CHECKPOINT is set *behind* the highest written id (a crash
        // between appending the batch and writing CHECKPOINT). The resume
        // point must be max(durable, highest written id on disk) — a plain
        // skip-<=-checkpoint would re-write 1003-0..1006-0.
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<String> = (0..7u64).map(|i| format!("{}-0", 1000 + i)).collect();

        // Run 1: full backfill, everything written, checkpoint = 1006-0.
        let mut src = FakeSource::new();
        let page: Vec<StreamEntry> = ids.iter().map(|id| entry(id)).collect();
        src.push(Ok(page));
        let out1 = run_with(&mut src, dir.path(), "0", "0", "+").unwrap();
        assert_eq!(out1.raw_lines, 7);
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1006-0");

        // Crash window: the durable CHECKPOINT is behind the written rows.
        checkpoint::write(dir.path(), "1002-0").unwrap();
        let max_written = Store::open(dir.path())
            .unwrap()
            .max_written_stream_id()
            .unwrap();
        assert_eq!(max_written.as_deref(), Some("1006-0"));
        let resume = checkpoint::resume_start("1002-0", max_written.as_deref());
        assert_eq!(
            resume, "1006-0",
            "resume = max(durable CHECKPOINT, highest written id)"
        );

        // Run 2: everything at/below the written watermark is skipped.
        let mut src2 = FakeSource::new();
        let page2: Vec<StreamEntry> = ids.iter().map(|id| entry(id)).collect();
        src2.push(Ok(page2));
        let out2 = run_with(&mut src2, dir.path(), &resume, "0", "+").unwrap();
        assert_eq!(
            out2.raw_lines, 0,
            "no rows re-written after the crash window"
        );
        let all = raw_stream_ids(dir.path());
        assert_eq!(all.len(), 7, "7 unique rows");
        let unique: std::collections::BTreeSet<&String> = all.iter().collect();
        assert_eq!(unique.len(), 7, "0 duplicates");
    }

    // --- pairing pool rebuilt from disk (§5.3) --------------------------------

    #[test]
    fn finish_in_range_pairs_with_start_flushed_in_earlier_run() {
        let dir = tempfile::tempdir().unwrap();
        // Run 1: only the start (id 1000-0, session_id s1) → open row on disk.
        let mut src = FakeSource::new();
        src.push(Ok(vec![start_sid("1000-0")]));
        run_with(&mut src, dir.path(), "0", "0", "1000-0").unwrap();
        assert_eq!(checkpoint::read(dir.path()).unwrap(), "1000-0");

        // Run 2: only the finish. The start is <= resume → dedupe-skipped
        // from raw, but the finish must still pair with the open row rebuilt
        // from disk (same session_pk, completed — not orphan).
        let mut src2 = FakeSource::new();
        src2.push(Ok(vec![finish("1001-0")]));
        let session_store = SessionStore::new(dir.path());
        let mut store = Store::open(dir.path()).unwrap();
        let mut now = || frozen_clock();
        let out = run(
            &test_cfg(dir.path()),
            &mut src2,
            "office:events",
            &mut store,
            &session_store,
            "1000-0",
            100_000,
            100_000, // max_mb: no cap in these tests
            "0",
            "1001-0",
            &mut now,
        )
        .unwrap();
        assert_eq!(out.raw_lines, 1);

        use crate::sessions::State;
        let rows = session_store.load_all().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "still one session row — completed, not orphan"
        );
        assert_eq!(rows[0].state, State::Completed);
        assert_eq!(rows[0].start_stream_id.as_deref(), Some("1000-0"));
        assert_eq!(rows[0].finish_stream_id.as_deref(), Some("1001-0"));
    }

    // --- §5.3 expiry: evaluated once against wall clock at end of range ------

    #[test]
    fn expiry_is_evaluated_once_at_end_of_range() {
        // §3.5: backfill has no read iterations, so it ages the pairing pool
        // once against wall clock at the end of the range. An `open` row
        // whose started_at is past the window must land on disk as `expired`
        // — exactly the session state follow would have produced.
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        // started_at = 2026-08-30T10:00:00Z; frozen clock = 2026-08-30T21:00:00Z
        // → 11 h elapsed; window = 1 h → strictly longer → expired.
        src.push(Ok(vec![start_sid("1000-0")]));
        let session_store = SessionStore::new(dir.path());
        let mut store = Store::open(dir.path()).unwrap();
        let mut now = || frozen_clock();
        let out = run(
            &test_cfg(dir.path()),
            &mut src,
            "office:events",
            &mut store,
            &session_store,
            "0",      // resume
            1,        // expire_hours: 1 h window
            100_000,  // max_mb: no cap in these tests
            "0",      // from
            "1000-0", // to
            &mut now,
        )
        .unwrap();
        assert_eq!(out.raw_lines, 1);
        use crate::sessions::State;
        let rows = session_store.load_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            State::Expired,
            "open row past the window expires at end of range"
        );
        assert_eq!(rows[0].start_stream_id.as_deref(), Some("1000-0"));
    }

    #[test]
    fn expiry_boundary_exactly_at_window_keeps_row_open() {
        // §5.3: strictly longer than the window is required to expire;
        // exactly at the window the row stays open and leaves the pool.
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![start_sid("1000-0")]));
        let session_store = SessionStore::new(dir.path());
        let mut store = Store::open(dir.path()).unwrap();
        let mut now = || frozen_clock();
        let out = run(
            &test_cfg(dir.path()),
            &mut src,
            "office:events",
            &mut store,
            &session_store,
            "0",
            11,      // window == elapsed (11 h) → not strictly longer
            100_000, // max_mb: no cap in these tests
            "0",
            "1000-0",
            &mut now,
        )
        .unwrap();
        assert_eq!(out.raw_lines, 1);
        use crate::sessions::State;
        let rows = session_store.load_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            State::Open,
            "exactly at the window is not expired"
        );
    }

    #[test]
    fn fully_skipped_range_leaves_session_view_untouched() {
        // §3.5: "a range that sits entirely at/below the resume point writes
        // nothing" — byte-true for the session view too. A no-op run must not
        // run the once-at-end expiry and rewrite on-disk rows (open → expired);
        // that is follow's job, not a no-op backfill's.
        let dir = tempfile::tempdir().unwrap();
        // Run 1: an open start. Elapsed = exactly 11 h, window = 11 h → not
        // strictly longer → stays Open on disk.
        let mut src = FakeSource::new();
        src.push(Ok(vec![start_sid("1000-0")]));
        let session_store = SessionStore::new(dir.path());
        let mut store = Store::open(dir.path()).unwrap();
        let mut now = || frozen_clock();
        run(
            &test_cfg(dir.path()),
            &mut src,
            "office:events",
            &mut store,
            &session_store,
            "0",
            11,      // window == elapsed → Open
            100_000, // max_mb: no cap in these tests
            "0",
            "1000-0",
            &mut now,
        )
        .unwrap();

        // Run 2: the same range, now entirely at/below the resume point. With
        // a 1 h window the row is far past expiry — but nothing is written,
        // so it must stay Open.
        let mut src2 = FakeSource::new();
        src2.push(Ok(vec![start_sid("1000-0")]));
        let mut store2 = Store::open(dir.path()).unwrap();
        let mut now2 = || frozen_clock();
        let out = run(
            &test_cfg(dir.path()),
            &mut src2,
            "office:events",
            &mut store2,
            &session_store,
            "1000-0",
            1,
            100_000, // max_mb: no cap in these tests
            "0",
            "1000-0",
            &mut now2,
        )
        .unwrap();
        assert_eq!(out.raw_lines, 0, "everything at/below resume is skipped");
        use crate::sessions::State;
        let rows = session_store.load_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].state,
            State::Open,
            "no-op run must not expire pre-existing rows"
        );
    }

    // --- BUG-WFDC-004: deterministic key order + byte-exact JSON --------------

    #[test]
    fn raw_line_is_byte_exact_with_stable_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        src.push(Ok(vec![entry("1-0")]));
        run_with(&mut src, dir.path(), "0", "0", "1-0").unwrap();
        let lines = raw_lines(dir.path());
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            r#"{"stream_id":"1-0","envelope_id":null,"ts":null,"actor":"dev","action":"task.started","target":null,"team":"dev-1","project":null,"payload":{},"fields":{"action":"task.started","actor":"dev","team":"dev-1"},"decode_ok":true}"#
        );
    }

    #[test]
    fn same_range_in_two_fresh_dirs_is_byte_identical() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let mut src = FakeSource::new();
        for i in 0..4 {
            src.push(Ok(vec![entry(&format!("{i}-0"))]));
        }
        run_with(&mut src, d1.path(), "0", "0", "3-0").unwrap();
        let mut src2 = FakeSource::new();
        for i in 0..4 {
            src2.push(Ok(vec![entry(&format!("{i}-0"))]));
        }
        run_with(&mut src2, d2.path(), "0", "0", "3-0").unwrap();
        assert_eq!(
            raw_lines(d1.path()),
            raw_lines(d2.path()),
            "byte-identical raw rows"
        );
    }

    // --- §5.5 wiring: backfill enforces the cap after its flush ------------

    /// Drive `run` with a fresh store and an explicit cap; returns the outcome.
    fn run_with_mb(
        src: &mut FakeSource,
        dir: &Path,
        resume: &str,
        from: &str,
        to: &str,
        max_mb: u64,
    ) -> Result<BackfillOutcome, Error> {
        let mut store = Store::open(dir).unwrap();
        let session_store = SessionStore::new(dir);
        let mut now = || frozen_clock();
        run(
            &test_cfg(dir),
            src,
            "office:events",
            &mut store,
            &session_store,
            resume,
            100_000, // effectively no expiry for determinism tests
            max_mb,
            from,
            to,
            &mut now,
        )
    }

    /// Write `raw/dt=<date>/events.jsonl` with at least `target_bytes` of
    /// stream-id'd lines (the collector's raw shape) — a fixture big enough
    /// to push a `max_mb=1` (1 MiB) cap over its limit.
    fn seed_big_date(dir: &Path, date: &str, target_bytes: usize) {
        let p = dir.join(format!("raw/dt={date}/events.jsonl"));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        let line = "{\"stream_id\":\"1725062400000-0\",\"action\":\"task.started\",\"ts\":\"2026-08-30T00:00:00Z\",\"team\":\"dev-1\"}\n";
        let mut content = String::with_capacity(target_bytes + 64);
        while content.len() < target_bytes {
            content.push_str(line);
        }
        std::fs::write(&p, content).unwrap();
    }

    /// A `task.started` entry with an explicit RFC 3339 timestamp, so the
    /// decoded line lands in the `dt=` partition of that date.
    fn entry_ts(id: &str, ts: &str) -> StreamEntry {
        FakeSource::entry(
            id,
            &[
                ("action", "task.started"),
                ("actor", "dev"),
                ("team", "dev-1"),
                ("timestamp", ts),
            ],
        )
    }

    /// §5.5: backfill writes with the same flush path as follow, so the cap
    /// is enforced after its single flush. The fixture is *under* the cap at
    /// startup; the backfilled batch pushes it over → the oldest date is
    /// deleted.
    #[test]
    fn run_enforces_cap_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        seed_big_date(dir.path(), "2026-08-30", 400_000);
        seed_big_date(dir.path(), "2026-08-31", 400_000);
        let mut src = FakeSource::new();
        let page: Vec<StreamEntry> = (0..1500u64)
            .map(|i| entry_ts(&format!("1725062400000-{i}"), "2026-08-31T10:00:00Z"))
            .collect();
        src.push(Ok(page));
        let out = run_with_mb(&mut src, dir.path(), "0", "0", "+", 1).unwrap();
        assert_eq!(out.raw_lines, 1500);
        assert!(
            !dir.path().join("raw/dt=2026-08-30/events.jsonl").exists(),
            "post-flush enforcement deleted the oldest date"
        );
        assert!(
            raw_stream_ids(dir.path()).contains(&"1725062400000-1499".to_string()),
            "the backfilled batch survives in the newer date"
        );
    }
}
