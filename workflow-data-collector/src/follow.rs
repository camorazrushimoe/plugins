//! Follow mode (§3): blocking `XREAD` on the stream, one flush per batch,
//! at-least-once dedupe, Redis-down backoff (1 s → 60 s, capped, with jitter;
//! never exits; CHECKPOINT never advances while disconnected).
//!
//! This is the minimal follow loop shared with `backfill` (same writer,
//! decoder and pairing rules). The deterministic stop contract (`--once`,
//! `--max-reads`, `--max-idle-ms`, signals) is BON-71's scope.

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::streams::StreamReadOptions;
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

const COUNT: usize = 32;
const BLOCK_MS: usize = 1000;
const BACKOFF_MIN_MS: u64 = 1000;
const BACKOFF_MAX_MS: u64 = 60_000;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn jitter_ms() -> u64 {
    (now_ms() as u64) % 250
}

/// Follow the stream until the process is killed.
pub fn run(cfg: &Config) -> Result<(), Error> {
    writer::ensure_data_dir(&cfg.data_dir)?;
    let _lock = crate::lock::acquire(&cfg.data_dir)?;

    for log in repair::repair(&cfg.data_dir)? {
        eprintln!("{log}");
    }

    let client = redis::Client::open(cfg.redis_url.as_str())?;
    let mut con = client.get_connection()?;

    let durable = checkpoint::read(&cfg.data_dir)?;
    let mut checkpoint = durable.clone();
    // §3.1 crash window: a crash between appending a batch and writing
    // CHECKPOINT leaves rows on disk ahead of the durable checkpoint; resume
    // from the highest id actually written to JSONL so the at-least-once
    // re-read cannot duplicate rows (§3.1: "cannot duplicate rows").
    let max_written = writer::max_written_stream_id(&cfg.data_dir)?;
    let base = checkpoint.as_deref().unwrap_or("0");
    let mut start = checkpoint::resume_start(base, max_written.as_deref());
    let mut backoff_ms = BACKOFF_MIN_MS;
    let window_ms = (cfg.expire_hours * 3600 * 1000) as i64;
    let opts = StreamReadOptions::default().count(COUNT).block(BLOCK_MS);
    // One pairer per run: the unmatched-start pool persists across batches.
    // Load existing session rows first so (a) a restart or a follow-after-
    // backfill never clobbers rows it did not create (flush rewrites a day
    // file from the store) and (b) a finish whose start was flushed earlier
    // (<= checkpoint) still pairs with it — same as backfill's on-disk pool.
    let mut pairer = Pairer::new();
    pairer.store_mut().load(&cfg.data_dir)?;
    pairer.rebuild_pool();

    // Log a distinct line when the written-watermark scan recovered the
    // resume point past the durable CHECKPOINT (crash-window recovery).
    if start != base {
        eprintln!(
            "startup scan: JSONL rows ahead of CHECKPOINT (checkpoint={}, highest written={}); resuming from {start}",
            checkpoint.as_deref().unwrap_or("(none)"),
            max_written.as_deref().unwrap_or("")
        );
    }

    eprintln!(
        "wfdc follow: stream={} data_dir={} checkpoint={}",
        cfg.stream,
        cfg.data_dir.display(),
        checkpoint.as_deref().unwrap_or("(none, from start)")
    );

    loop {
        let reply = con.xread_options(&[cfg.stream.as_str()], &[start.as_str()], &opts);
        let reply: redis::streams::StreamReadReply = match reply {
            Ok(r) => r,
            Err(e) => {
                // Redis down: log, wait, retry with backoff (1 s → 60 s,
                // capped, with jitter). Never exit; CHECKPOINT never advances
                // while disconnected (§3).
                eprintln!("redis unavailable: {e}; retrying in {backoff_ms} ms");
                thread::sleep(Duration::from_millis(backoff_ms + jitter_ms()));
                backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
                continue;
            }
        };
        if reply.keys.is_empty() {
            // BLOCK timed out with no event: expiry still runs on every read
            // iteration, including empty ones (§5.3).
            pairer.apply_expiry(now_ms(), window_ms);
            pairer.store_mut().flush(&cfg.data_dir)?;
            continue;
        }

        let mut rows: Vec<RawRow> = Vec::new();
        let mut last_id: Option<String> = None;
        for key in &reply.keys {
            for entry in &key.ids {
                let id = &entry.id;
                let fields: std::collections::HashMap<String, String> = entry
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), crate::decoder::field_to_string(v)))
                    .collect();
                // Dedupe at write time (§3.1): skip stream_id <= the last
                // flushed checkpoint.
                if checkpoint::is_duplicate(id, checkpoint.as_deref()) {
                    continue;
                }
                let d = decode(id, &fields);
                if let Some(w) = &d.warning {
                    eprintln!("{w}");
                }
                let (dt, _ms) = dt_and_ms(id, d.ts.as_deref());
                let folder = team_folder(d.team.as_deref(), d.actor.as_deref());
                rows.push(RawRow {
                    dt,
                    team_folder: folder,
                    json: raw_line(id, &d),
                });
                pairer.on_event(&d, id)?;
                last_id = Some(id.to_string());
            }
        }
        if let Some(last) = last_id {
            // Order: write batch → fsync JSONL → atomic CHECKPOINT → advance
            // the in-memory watermark → rewrite MANIFEST (§5.4: each flush).
            writer::append_batch(&cfg.data_dir, &rows)?;
            pairer.store_mut().flush(&cfg.data_dir)?;
            checkpoint::write(&cfg.data_dir, &last)?;
            crate::manifest::write(cfg)?;
            checkpoint = Some(last.clone());
            start = last;
        }
        backoff_ms = BACKOFF_MIN_MS;
    }
}
