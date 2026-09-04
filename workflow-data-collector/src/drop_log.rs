//! Drop log (spec §5.4 feed, produced by §5.5).
//!
//! One entry per dropped `dt=` partition (scope `date`) or per trimmed
//! event (scope `today`). The manifest (§5.4) exposes the last
//! [`DROP_LOG_CAP`] entries; this module is the ring that feeds it.

use std::collections::VecDeque;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The manifest keeps at most this many recent drop-log entries (§5.4).
pub const DROP_LOG_CAP: usize = 100;

/// Persisted ring file name (§5.4 feed). `.json` — deliberately **not**
/// `.jsonl` — so the file is excluded from the §5.5 max_mb cap and from
/// every JSONL scan (`layout`, startup repair, crash-window watermark).
pub const DROP_LOG_FILE: &str = "DROP_LOG.json";

/// What a drop-log entry refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// An entire `dt=` partition was deleted (oldest-date deletion, §5.5 step 2).
    Date,
    /// A single event was trimmed from today's views (§5.5 step 3).
    Today,
}

/// One drop-log entry.
///
/// Field order is stable (`when`, `scope`, `date`, `stream_id`,
/// `bytes_freed`) so the manifest / `status --json` can serialize it
/// verbatim with a stable key order. Exactly one of `date` / `stream_id`
/// is set, matching `scope`; the other is omitted when serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropLogEntry {
    /// Wall-clock UTC instant the entry was recorded (RFC 3339).
    pub when: String,
    /// `date` → a whole partition was dropped; `today` → one event trimmed.
    pub scope: Scope,
    /// Dropped `dt=` partition (scope `date` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Trimmed event's stream id (scope `today` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    /// Bytes freed by this drop (sum over every view touched).
    pub bytes_freed: u64,
}

impl DropLogEntry {
    /// Entry for an oldest-date deletion (§5.5 step 2).
    pub fn date(when: &str, date: &str, bytes_freed: u64) -> Self {
        DropLogEntry {
            when: when.to_string(),
            scope: Scope::Date,
            date: Some(date.to_string()),
            stream_id: None,
            bytes_freed,
        }
    }

    /// Entry for a trimmed event (§5.5 step 3).
    pub fn event(when: &str, stream_id: &str, bytes_freed: u64) -> Self {
        DropLogEntry {
            when: when.to_string(),
            scope: Scope::Today,
            date: None,
            stream_id: Some(stream_id.to_string()),
            bytes_freed,
        }
    }
}

/// Bounded ring of recent drop-log entries (last [`DROP_LOG_CAP`]).
#[derive(Debug, Default, Clone)]
pub struct DropLog {
    entries: VecDeque<DropLogEntry>,
}

impl DropLog {
    pub fn new() -> Self {
        DropLog {
            entries: VecDeque::new(),
        }
    }

    /// Append an entry, evicting the oldest when over [`DROP_LOG_CAP`].
    pub fn push(&mut self, entry: DropLogEntry) {
        if self.entries.len() >= DROP_LOG_CAP {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Entries in chronological (append) order, oldest first.
    pub fn entries(&self) -> &VecDeque<DropLogEntry> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Total bytes freed across all entries (useful for tests / status).
    pub fn bytes_freed(&self) -> u64 {
        self.entries.iter().map(|e| e.bytes_freed).sum()
    }
}

// --- persistence (spec §5.4 feed) -------------------------------------------
//
// BON-69's ring is in-memory per enforcement run; §5.5 requires the drop log
// to stay visible to a later `wfdc status` process. The ring is persisted as
// `data_dir/DROP_LOG.json` (mode 0600, atomic `.tmp` + rename). `.json`, not
// `.jsonl`, so it never counts toward the max_mb cap nor bytes_used (§5.5).

/// Load the persisted ring. A missing file is an empty ring (never an error —
/// the shape is the contract); an unparsable file is an observability
/// surface, so it logs and returns an empty ring rather than failing.
pub fn load(data_dir: &Path) -> Result<DropLog, crate::Error> {
    let path = data_dir.join(DROP_LOG_FILE);
    if !path.is_file() {
        return Ok(DropLog::new());
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| crate::Error::Io(format!("read {}: {e}", path.display())))?;
    match serde_json::from_str::<Vec<DropLogEntry>>(&text) {
        Ok(entries) => {
            let mut log = DropLog::new();
            for e in entries {
                log.push(e);
            }
            Ok(log)
        }
        Err(e) => {
            log::warn!(
                "DROP_LOG.json at {} is unparsable ({e}); starting an empty ring",
                path.display()
            );
            Ok(DropLog::new())
        }
    }
}

/// Persist the ring atomically (`DROP_LOG.json.tmp` + fsync + rename),
/// mode 0600 (§2). The bounded ring is already capped at [`DROP_LOG_CAP`].
pub fn save(data_dir: &Path, log: &DropLog) -> Result<(), crate::Error> {
    let entries: Vec<DropLogEntry> = log.entries().iter().cloned().collect();
    let json = serde_json::to_string(&entries)
        .map_err(|e| crate::Error::Fatal(format!("serialize drop log: {e}")))?;
    let path = data_dir.join(DROP_LOG_FILE);
    let tmp = data_dir.join(format!("{DROP_LOG_FILE}.tmp"));
    let result = (|| -> Result<(), crate::Error> {
        let mut f = crate::fsutil::open_private(&tmp, false)?;
        f.write_all(json.as_bytes())
            .map_err(|e| crate::Error::Io(format!("write {}: {e}", tmp.display())))?;
        f.write_all(b"\n")
            .map_err(|e| crate::Error::Io(format!("write {}: {e}", tmp.display())))?;
        f.sync_all()
            .map_err(|e| crate::Error::Io(format!("fsync {}: {e}", tmp.display())))?;
        drop(f);
        std::fs::rename(&tmp, &path).map_err(|e| {
            crate::Error::Io(format!(
                "rename {} → {}: {e}",
                tmp.display(),
                path.display()
            ))
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Append entries to the persisted ring (load → push → save), keeping the
/// last [`DROP_LOG_CAP`] with the oldest evicted. Called after each cap
/// enforcement run so §5.5 drops stay visible to `wfdc status` (§5.4).
pub fn append(
    data_dir: &Path,
    entries: impl IntoIterator<Item = DropLogEntry>,
) -> Result<(), crate::Error> {
    let mut log = load(data_dir)?;
    for e in entries {
        log.push(e);
    }
    save(data_dir, &log)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_is_bounded_to_100() {
        let mut log = DropLog::new();
        for i in 0..120 {
            log.push(DropLogEntry::event(
                "2026-08-31T07:00:00Z",
                &format!("{i}-0"),
                i as u64,
            ));
        }
        assert_eq!(log.len(), DROP_LOG_CAP);
        // Oldest 20 were evicted: first entry is now stream 20-0.
        assert_eq!(
            log.entries().front().unwrap().stream_id.as_deref(),
            Some("20-0")
        );
        assert_eq!(
            log.entries().back().unwrap().stream_id.as_deref(),
            Some("119-0")
        );
    }

    #[test]
    fn entry_shapes_match_spec_fields() {
        let date = DropLogEntry::date("2026-08-31T07:00:00Z", "2026-08-29", 1234);
        assert_eq!(date.scope, Scope::Date);
        assert_eq!(date.date.as_deref(), Some("2026-08-29"));
        assert_eq!(date.stream_id, None);
        assert_eq!(date.bytes_freed, 1234);

        let ev = DropLogEntry::event("2026-08-31T07:00:00Z", "1725062400000-0", 88);
        assert_eq!(ev.scope, Scope::Today);
        assert_eq!(ev.stream_id.as_deref(), Some("1725062400000-0"));
        assert_eq!(ev.date, None);
        assert_eq!(ev.bytes_freed, 88);
    }

    #[test]
    fn serializes_with_stable_keys_and_omits_irrelevant_field() {
        let ev = DropLogEntry::event("2026-08-31T07:00:00Z", "1725062400000-0", 88);
        let s = serde_json::to_string(&ev).unwrap();
        assert_eq!(
            s,
            r#"{"when":"2026-08-31T07:00:00Z","scope":"today","stream_id":"1725062400000-0","bytes_freed":88}"#
        );
        let date = DropLogEntry::date("2026-08-31T07:00:00Z", "2026-08-29", 1234);
        let s = serde_json::to_string(&date).unwrap();
        assert_eq!(
            s,
            r#"{"when":"2026-08-31T07:00:00Z","scope":"date","date":"2026-08-29","bytes_freed":1234}"#
        );
    }

    // --- persistence (§5.4 feed: DROP_LOG.json) -----------------------------

    #[test]
    fn load_missing_file_is_empty_ring() {
        let dir = tempfile::tempdir().unwrap();
        let log = load(dir.path()).unwrap();
        assert!(log.is_empty());
    }

    #[test]
    fn save_then_load_roundtrips_and_sets_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let mut log = DropLog::new();
        log.push(DropLogEntry::event("2026-08-31T07:00:00Z", "1-0", 10));
        log.push(DropLogEntry::date(
            "2026-08-31T07:00:01Z",
            "2026-08-29",
            100,
        ));
        save(dir.path(), &log).unwrap();
        let path = dir.path().join(DROP_LOG_FILE);
        assert!(path.is_file(), "DROP_LOG.json written");
        assert!(!dir.path().join(format!("{DROP_LOG_FILE}.tmp")).exists());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "DROP_LOG.json must be 0600 (§2)");
        let loaded = load(dir.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.entries().front().unwrap().stream_id.as_deref(),
            Some("1-0")
        );
        assert_eq!(
            loaded.entries().get(1).unwrap().date.as_deref(),
            Some("2026-08-29")
        );
    }

    #[test]
    fn append_accumulates_across_calls_bounded_to_cap() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..3u64 {
            append(
                dir.path(),
                [DropLogEntry::event("when", &format!("{i}-0"), i)],
            )
            .unwrap();
        }
        let log = load(dir.path()).unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(
            log.entries().front().unwrap().stream_id.as_deref(),
            Some("0-0")
        );

        // Append past the cap: the oldest entries are evicted (last 100 kept).
        for i in 0..200u64 {
            append(
                dir.path(),
                [DropLogEntry::event("when", &format!("{i}-0"), i)],
            )
            .unwrap();
        }
        let log = load(dir.path()).unwrap();
        assert_eq!(log.len(), DROP_LOG_CAP);
        assert_eq!(
            log.entries().front().unwrap().stream_id.as_deref(),
            Some("100-0"),
            "oldest 100 evicted, 100..199 remain"
        );
    }

    #[test]
    fn unparsable_file_yields_empty_ring_not_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(DROP_LOG_FILE), "{not json").unwrap();
        let log = load(dir.path()).unwrap();
        assert!(
            log.is_empty(),
            "corrupt ring degrades to empty, never errors"
        );
    }

    #[test]
    fn json_extension_is_not_jsonl_so_cap_and_scan_ignore_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = DropLog::new();
        log.push(DropLogEntry::event("when", "1-0", 42));
        save(dir.path(), &log).unwrap();
        // layout::scan counts only *.jsonl — DROP_LOG.json must not appear
        // in jsonl_bytes() nor in any view.
        let tree = crate::layout::scan(dir.path()).unwrap();
        assert_eq!(tree.jsonl_bytes(), 0, "DROP_LOG.json is not JSONL");
        assert!(tree.raw_views.is_empty() && tree.other_jsonl.is_empty());
    }
}
