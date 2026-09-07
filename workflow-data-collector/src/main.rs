//! wfdc binary entry point.
//!
//! Wires config (§2) → permissions → single-writer lock (§3.3) → startup
//! repair (§3.2) → checkpoint resume (§3.1) → follow loop (§3) with
//! SIGTERM/SIGINT graceful stop (§3.4 signal path). Exit codes: 0 clean stop,
//! 1 fatal config/IO, 3 lock conflict.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use wfdc::backfill;
use wfdc::cli;
use wfdc::config::{Config, Sources};
use wfdc::follow::{self, FollowOptions};
use wfdc::lock::{self, LockError};
use wfdc::pairing::Pairer;
use wfdc::raw::Store;
use wfdc::sessions::SessionStore;
use wfdc::stream::RedisStream;
use wfdc::Error;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let code = match real_main() {
        Ok(()) => 0,
        Err(e) => {
            log::error!("{e}");
            wfdc::exit_code(&e)
        }
    };
    std::process::exit(code);
}

fn real_main() -> Result<(), Error> {
    // --- CLI / env / config ---------------------------------------------------
    let args: Vec<String> = std::env::args().collect();
    let cli = cli::parse(args.iter().cloned()).map_err(Error::Config)?;
    if cli.help {
        print!("{}", cli::USAGE);
        return Ok(());
    }
    if cli.version {
        println!("{}", cli::version_string());
        return Ok(());
    }
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let binary_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));
    let cwd = std::env::current_dir().unwrap_or_else(|_| binary_dir.clone());
    let cfg = Config::load(&Sources {
        cli: &cli,
        env: &env,
        binary_dir: &binary_dir,
        cwd: &cwd,
    })
    .map_err(|e| Error::Config(e.0))?;

    log::info!(
        "wfdc starting: stream={} data_dir={} redis_url={} max_mb={} expire_hours={}",
        cfg.stream,
        cfg.data_dir.display(),
        cfg.redis_url,
        cfg.max_mb,
        cfg.expire_hours
    );

    // --- store, perms (§2: data_dir 0700, files 0600), single-writer lock ----
    let store = Store::open(&cfg.data_dir)?;
    let _lock = lock::acquire(&cfg.data_dir).map_err(|e| match e {
        LockError::Busy => Error::LockBusy,
        LockError::Io(m) => Error::Io(m),
    })?;

    // §3.2 startup repair: drop partial lines at EOF before reading anything.
    let repairs = store.repair_partial_lines()?;
    for r in &repairs {
        log::warn!(
            "startup repair: truncated partial line at {} ({} bytes dropped)",
            r.path.display(),
            r.bytes_dropped
        );
    }

    // §3.1 resume from CHECKPOINT; a fresh data_dir starts at "0" so the full
    // retained stream history is caught up (never silently start at "$").
    let durable = wfdc::checkpoint::read(&cfg.data_dir)?;
    // A crash between appending a batch and writing CHECKPOINT leaves rows on
    // disk ahead of the durable checkpoint; resume from the max written id so
    // the at-least-once re-read cannot duplicate them (§3.1).
    let max_written = store.max_written_stream_id()?;
    let start = wfdc::checkpoint::resume_start(&durable, max_written.as_deref());
    let recovered = max_written.as_deref().is_some_and(|mw| {
        matches!(
            (
                wfdc::streamid::StreamId::parse(mw),
                wfdc::streamid::StreamId::parse(&durable),
            ),
            (Some(m), Some(d)) if m > d
        )
    });
    if recovered {
        log::info!(
            "startup scan: JSONL rows ahead of CHECKPOINT (checkpoint={durable}, highest written={}); resuming from {start}",
            max_written.as_deref().unwrap_or("")
        );
    } else {
        log::info!("resuming from checkpoint {start}");
    }

    match cli.command {
        cli::Command::Follow => run_follow(
            cfg,
            store,
            &start,
            cli.follow_max_reads(),
            cli.max_idle_ms.map(|n| n as u64),
        ),
        cli::Command::Backfill { from, to } => run_backfill(cfg, store, &start, &from, &to),
    }
}

/// Follow: install signal handlers, then block on the follow loop until a
/// clean stop (1st SIGTERM/SIGINT → flush + exit 0; 2nd → exit 1).
fn run_follow(
    cfg: Config,
    store: Store,
    start: &str,
    max_reads: Option<usize>,
    max_idle_ms: Option<u64>,
) -> Result<(), Error> {
    // --- signals: 1st SIGTERM/SIGINT → clean stop, 2nd → immediate exit 1 ---
    let stop = Arc::new(AtomicBool::new(false));
    let signal_stop = Arc::clone(&stop);
    let main_thread = std::thread::current();
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
    ])
    .map_err(|e| Error::Io(format!("cannot install signal handlers: {e}")))?;
    std::thread::spawn(move || {
        let mut count = 0usize;
        for _ in signals.forever() {
            count += 1;
            if count == 1 {
                log::info!(
                    "signal received — finishing the in-flight batch, then clean stop \
                     (a second signal exits immediately)"
                );
                signal_stop.store(true, Ordering::Relaxed);
                main_thread.unpark();
            } else {
                log::warn!("second signal — exiting immediately with 1");
                std::process::exit(1);
            }
        }
    });

    let mut store = store;
    // --- follow ----------------------------------------------------------------
    let mut redis = RedisStream::new(&cfg.redis_url)?;
    // §3.4 stop contract: `--once` ≡ `--max-reads 1` (single source of truth:
    // `CliArgs::follow_max_reads`); `--max-idle-ms` sets the idle window. The
    // clean-stop path (flush → CHECKPOINT → exit 0) is identical for every
    // trigger because `follow::run` exits through one code path; `--max-mb`
    // enforcement itself lands with BON-69 (§5.5).
    let opts = FollowOptions {
        max_reads,
        max_idle_ms,
        ..Default::default()
    };
    log::info!(
        "stop contract: max_reads={:?} max_idle_ms={:?}",
        opts.max_reads,
        opts.max_idle_ms
    );
    // §5.3: the pairing pool is rebuilt from the session rows already on disk,
    // so an `open` / `interrupted` start from a previous run is still pairable
    // by a finish arriving after this restart (cross-batch pool persistence).
    let session_store = SessionStore::new(&cfg.data_dir);
    let mut pairer = Pairer::new(cfg.expire_hours);
    pairer.rebuild(session_store.load_all()?);
    let mut sleep = |dur: Duration| {
        // park in slices so the signal handler can unpark us promptly; never
        // sleep past a graceful-stop request.
        let deadline = std::time::Instant::now() + dur;
        while !stop.load(Ordering::Relaxed) {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            std::thread::park_timeout(deadline - now);
        }
    };
    let mut now = chrono::Utc::now;
    let mut time = follow::LoopTime {
        sleep: &mut sleep,
        now: &mut now,
    };
    follow::run(
        &mut redis,
        &cfg.stream,
        &mut store,
        start,
        &opts,
        cfg.max_mb,
        &stop,
        &mut time,
        &mut pairer,
        &session_store,
    )?;

    log::info!("clean stop (checkpoint durable)");
    Ok(())
}

/// Backfill: one-shot chosen-range replay with the same writer/decoder/pairing
/// rules as follow (§3.5). Dedupe and the resume point were already computed
/// above (max of the durable CHECKPOINT and the highest id written to JSONL).
fn run_backfill(cfg: Config, store: Store, start: &str, from: &str, to: &str) -> Result<(), Error> {
    let mut store = store;
    let mut redis = RedisStream::new(&cfg.redis_url)?;
    let session_store = SessionStore::new(&cfg.data_dir);
    let mut now = chrono::Utc::now;
    backfill::run(
        &mut redis,
        &cfg.stream,
        &mut store,
        &session_store,
        start,
        cfg.expire_hours,
        cfg.max_mb,
        from,
        to,
        &mut now,
    )?;
    Ok(())
}
