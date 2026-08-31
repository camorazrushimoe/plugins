//! `wfdc` — Workflow Data Collector (spec v0.3.0).
//!
//! CLI per §2: `wfdc` / `wfdc follow` / `wfdc backfill [--from] [--to]`.
//! Config precedence: CLI > env > file > defaults.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use wfdc::backfill;
use wfdc::config::{self, CliOverrides};
use wfdc::exit_code;

#[derive(Parser, Debug)]
#[command(
    name = "wfdc",
    version,
    about = "Workflow Data Collector — reads the factory Redis stream, writes raw JSONL + paired agent sessions (spec v0.3.0)"
)]
struct Cli {
    /// Path to wfdc.toml (search order: --config, $WFDC_CONFIG,
    /// <binary-dir>/wfdc.toml, ./wfdc.toml).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Redis URL (prefer WFDC_REDIS_URL for credentialed URLs — argv is
    /// visible in /proc/<pid>/cmdline).
    #[arg(long, global = true)]
    redis: Option<String>,
    /// Stream name.
    #[arg(long, global = true)]
    stream: Option<String>,
    /// Disk cap in MB (default 500; 1–15 → 16).
    #[arg(long, global = true)]
    max_mb: Option<u64>,
    /// Expiry window in hours (default 6; test knob).
    #[arg(long, global = true)]
    expire_after: Option<u64>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Follow the stream (default command).
    Follow,
    /// Backfill a chosen stream range.
    Backfill {
        /// First stream id of the range (inclusive). Default: 0 (start).
        #[arg(long, default_value = "0")]
        from: String,
        /// Last stream id of the range (inclusive). Default: + (latest).
        #[arg(long, default_value = "+")]
        to: String,
    },
    /// Print the MANIFEST observability document (§5.4).
    Status {
        /// Machine-readable output: one JSON document, stable key order.
        #[arg(long)]
        json: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let overrides = CliOverrides {
        config: cli.config.clone(),
        redis_url: cli.redis.clone(),
        stream: cli.stream.clone(),
        max_mb: cli.max_mb,
        expire_hours: cli.expire_after,
    };

    let cfg = match config::resolve(&overrides) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("wfdc: {e}");
            return ExitCode::from(exit_code(&e) as u8);
        }
    };

    let result = match &cli.command {
        Some(Command::Backfill { from, to }) => backfill::run(&cfg, from, to).map(|_| ()),
        Some(Command::Status { json }) => wfdc::manifest::status(&cfg, *json),
        Some(Command::Follow) | None => wfdc::follow::run(&cfg),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wfdc: {e}");
            ExitCode::from(exit_code(&e) as u8)
        }
    }
}
