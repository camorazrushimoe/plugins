//! Workflow Data Collector — reads the factory Redis stream and writes a raw
//! JSONL dataset plus paired agent sessions (spec v0.3.0).
//!
//! Module layout follows the spec sections:
//! - [`config`]    §2  config precedence, path resolution, permissions
//! - [`decoder`]   §4.1 wire decoder
//! - [`team`]      §5.1 team folder naming
//! - [`timeutil`]  §4.2 timestamp contract
//! - [`checkpoint`] §3.1 checkpoint + dedupe
//! - [`lock`]      §3.3 single-writer lock
//! - [`repair`]    §3.2 startup partial-line repair
//! - [`writer`]    §5.2 raw JSONL writer
//! - [`pairing`], [`sessions`] §5.3 session pairing + sessions.jsonl
//! - [`manifest`] §5.4 MANIFEST.json + `status`/`status --json`
//! - [`backfill`]  §9.7 backfill command
//! - [`follow`]    §3   follow loop

pub mod backfill;
pub mod checkpoint;
pub mod config;
pub mod decoder;
pub mod follow;
pub mod lock;
pub mod manifest;
pub mod pairing;
pub mod repair;
pub mod sessions;
pub mod team;
pub mod timeutil;
pub mod writer;

use std::fmt;

/// Process-level error: fatal (exit 1) vs lock conflict (exit 3), per §3.4.
#[derive(Debug)]
pub enum Error {
    /// Fatal config or IO error → exit 1.
    Fatal(String),
    /// Lock held by a live process → exit 3.
    LockConflict,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Fatal(m) => write!(f, "{m}"),
            Error::LockConflict => write!(f, "another collector holds the data_dir lock"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Fatal(e.to_string())
    }
}

impl From<redis::RedisError> for Error {
    fn from(e: redis::RedisError) -> Self {
        Error::Fatal(format!("redis: {e}"))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Fatal(format!("json: {e}"))
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Fatal(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Fatal(s.to_string())
    }
}

/// Exit code for a given error per §3.4.
pub fn exit_code(e: &Error) -> i32 {
    match e {
        Error::LockConflict => 3,
        Error::Fatal(_) => 1,
    }
}
