//! Workflow Data Collector (wfdc) — spec v0.3.0.
//!
//! Follows the factory Redis stream (`office:events`) in near-real-time and
//! writes a raw, minimally structured JSONL dataset to `data_dir`, without
//! duplicates or data loss. This crate implements §2 (config + perms),
//! §3.1–3.3 (flush/checkpoint/dedupe, startup repair, single-writer lock) and
//! §9.1–9.2 (core pipeline) of `SPEC.md`.

pub mod checkpoint;
pub mod cli;
pub mod config;
pub mod decode;
pub mod dt;
pub mod follow;
pub mod fsutil;
pub mod lock;
pub mod raw;
pub mod stream;
pub mod streamid;
pub mod team;

use std::fmt;

/// Fatal error kinds. Maps to the §3.4 exit codes: 0 clean stop, 1 fatal
/// config/IO, 3 lock conflict.
#[derive(Debug)]
pub enum Error {
    /// Fatal configuration problem (bad TOML, malformed Redis URL, perms…).
    Config(String),
    /// Fatal IO problem (data_dir not writable, corrupt CHECKPOINT, …).
    Io(String),
    /// Protocol-level corruption (e.g. an unparsable stream id from Redis).
    Fatal(String),
    /// Another live collector owns this data_dir (§3.3) → exit 3.
    LockBusy,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(m) => write!(f, "config error: {m}"),
            Error::Io(m) => write!(f, "io error: {m}"),
            Error::Fatal(m) => write!(f, "fatal: {m}"),
            Error::LockBusy => write!(
                f,
                "another wfdc instance is already running on this data_dir"
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

/// Exit code for an [`Error`] per §3.4.
pub fn exit_code(e: &Error) -> i32 {
    match e {
        Error::LockBusy => 3,
        _ => 1,
    }
}
