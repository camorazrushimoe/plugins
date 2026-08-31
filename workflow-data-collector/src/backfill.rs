//! `wfdc backfill [--from STREAM_ID] [--to STREAM_ID]` (§3, §9.7).
//!
//! Reads a chosen stream range and writes raw + session rows deterministically
//! using the **same writer, decoder and pairing rules as follow**. Backfill is
//! for a chosen range only (first install, rebuilt derived tables, a trimmed
//! stream recovered elsewhere) — automatic catch-up is the checkpoint's job.
//!
//! Semantics (pinned in SPEC.md §3):
//! - The range `[from, to]` is inclusive on both ends (Redis XRANGE
//!   semantics). Defaults: `--from 0` (stream start), `--to +` (stream end).
//! - **Dedupe (§3.1) applies**: an entry whose `stream_id` is `<=` the last
//!   flushed CHECKPOINT is skipped, exactly like follow — re-running a range
//!   never duplicates rows, and a range that sits at/below the checkpoint
//!   writes nothing.
//! - The pairing pool is **rebuilt from the session rows already on disk**
//!   before the range is processed, so a finish inside the range still pairs
//!   with a start that was flushed earlier (<= checkpoint) — the same
//!   cross-batch pool persistence follow has in memory.
//! - CHECKPOINT is never moved backward and never advanced for un-flushed
//!   data: it advances forward to `max(current, last flushed id in range)`
//!   only after everything is on disk, so follow-mode checkpoint semantics
//!   are undisturbed and follow will not re-read the backfilled range.
//! - An empty/inverted range (`--from` after `--to`) writes nothing and
//!   exits 0.
//! - No partial output: everything is staged in memory and flushed once; on
//!   any error the command exits 1 and the data_dir is left unchanged.

use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::Commands;

use crate::checkpoint;
use crate::config::Config;
use crate::decoder::decode;
use crate::pairing::Pairer;
use crate::repair;
use crate::team::team_folder;
use crate::timeutil::dt_and_ms;
use crate::writer::{self, raw_line, RawRow};
use crate::Error;

/// XRANGE page size (memory-bounded pagination through the range).
const XRANGE_COUNT: usize = 512;

#[derive(Debug, Clone)]
pub struct BackfillOutcome {
    pub from: String,
    pub to: String,
    pub raw_lines: u64,
    pub session_rows: u64,
    pub checkpoint: Option<String>,
}

/// Validate a --from/--to stream id. Accepts "+", "-", "0" and "<ms>-<seq>".
pub fn validate_bound(s: &str, what: &str) -> Result<(), Error> {
    if s == "+" || s == "-" || s == "0" {
        return Ok(());
    }
    if checkpoint::parse_id(s).is_none() {
        return Err(Error::Fatal(format!(
            "{what} is not a valid stream id: {s:?} (expected <ms>-<seq>, 0, -, or +)"
        )));
    }
    Ok(())
}

/// Run a backfill over `[from, to]` (inclusive, Redis XRANGE semantics).
pub fn run(cfg: &Config, from: &str, to: &str) -> Result<BackfillOutcome, Error> {
    validate_bound(from, "--from")?;
    validate_bound(to, "--to")?;
    // An inverted range (--from after --to) is an empty range: XRANGE returns
    // nothing, no rows are written, exit 0 (§3 / BKF-3).

    // Single writer per data_dir (§3.3) — exit 3 when a live collector holds it.
    writer::ensure_data_dir(&cfg.data_dir)?;
    let _lock = crate::lock::acquire(&cfg.data_dir)?;

    // Startup repair (§3.2) — same as follow.
    for log in repair::repair(&cfg.data_dir)? {
        eprintln!("{log}");
    }

    let existing = checkpoint::read(&cfg.data_dir)?;

    let client = redis::Client::open(cfg.redis_url.as_str())?;
    let mut con = client.get_connection()?;

    // Rebuild the pairing pool from the session rows already on disk (§5.3):
    // a finish in this range must pair with a start that was flushed earlier
    // (<= checkpoint) exactly as follow's in-memory pool would.
    let mut pairer = Pairer::new();
    pairer.store_mut().load(&cfg.data_dir)?;
    pairer.rebuild_pool();

    let mut raw_rows: Vec<RawRow> = Vec::new();
    // last_seen drives the XRANGE cursor (must advance past skipped entries);
    // last_written drives the checkpoint (only actually written rows).
    let mut last_seen: Option<String> = None;
    let mut last_written: Option<String> = None;
    let mut cursor = from.to_string();

    loop {
        let reply: redis::streams::StreamRangeReply =
            con.xrange_count(&cfg.stream, &cursor, to, XRANGE_COUNT)?;
        if reply.ids.is_empty() {
            break;
        }
        for entry in &reply.ids {
            let id = &entry.id;
            last_seen = Some(id.clone());
            // Dedupe at write time (§3.1): skip stream_id <= the last flushed
            // checkpoint. Re-running a range never duplicates rows.
            if checkpoint::is_duplicate(id, existing.as_deref()) {
                continue;
            }
            let fields: HashMap<String, String> = entry
                .map
                .iter()
                .map(|(k, v)| (k.clone(), crate::decoder::field_to_string(v)))
                .collect();
            let d = decode(id, &fields);
            if let Some(w) = &d.warning {
                eprintln!("{w}");
            }
            let (dt, _ms) = dt_and_ms(id, d.ts.as_deref());
            let folder = team_folder(d.team.as_deref(), d.actor.as_deref());
            raw_rows.push(RawRow {
                dt,
                team_folder: folder,
                json: raw_line(id, &d),
            });
            pairer.on_event(&d, id)?;
            last_written = Some(id.clone());
        }
        if reply.ids.len() < XRANGE_COUNT {
            break;
        }
        cursor = checkpoint::next_id(last_seen.as_deref().unwrap_or(from));
    }

    // Expiry, evaluated once at the end of the range (wall clock, §5.3) so a
    // backfill of old data reproduces what follow would have produced.
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    pairer.apply_expiry(now_ms, (cfg.expire_hours * 3600 * 1000) as i64);

    // Write everything, then advance the checkpoint forward-only.
    writer::append_batch(&cfg.data_dir, &raw_rows)?;
    pairer.store_mut().flush(&cfg.data_dir)?;

    // Nothing flushed → never touch the checkpoint (§3.1: never advanced for
    // un-flushed data). Otherwise advance forward only.
    let new_cp = match (&existing, &last_written) {
        (Some(old), Some(new)) => {
            let old_t = checkpoint::parse_id(old);
            let new_t = checkpoint::parse_id(new);
            match (old_t, new_t) {
                (Some(o), Some(n)) if n > o => Some(new.clone()),
                _ => None, // never moved backward
            }
        }
        (None, Some(new)) => Some(new.clone()),
        (_, None) => None,
    };

    if let Some(cp) = &new_cp {
        checkpoint::write(&cfg.data_dir, cp)?;
    }

    // MANIFEST is rewritten after every successful flush (§5.4) — backfill
    // flushes once per run.
    crate::manifest::write(cfg)?;

    let checkpoint = new_cp.or(existing);
    let outcome = BackfillOutcome {
        from: from.to_string(),
        to: to.to_string(),
        raw_lines: raw_rows.len() as u64,
        session_rows: pairer.store().all().len() as u64,
        checkpoint: checkpoint.clone(),
    };

    eprintln!(
        "backfill {}..{}: {} raw rows, {} session rows, checkpoint {}",
        from,
        to,
        outcome.raw_lines,
        outcome.session_rows,
        outcome.checkpoint.as_deref().unwrap_or("-")
    );
    Ok(outcome)
}

/// Load an existing on-disk checkpoint for a data_dir (public for tests).
pub fn read_checkpoint(data_dir: &Path) -> Result<Option<String>, Error> {
    checkpoint::read(data_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_bounds() {
        assert!(validate_bound("0", "--from").is_ok());
        assert!(validate_bound("+", "--to").is_ok());
        assert!(validate_bound("-", "--from").is_ok());
        assert!(validate_bound("1725062400000-0", "--from").is_ok());
        assert!(validate_bound("garbage", "--from").is_err());
        assert!(validate_bound("1725062400000", "--from").is_err()); // missing -seq
    }
}
