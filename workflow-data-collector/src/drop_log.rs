//! Drop log (spec §5.4 feed, produced by §5.5).
//!
//! One entry per dropped `dt=` partition (scope `date`) or per trimmed
//! event (scope `today`). The manifest (§5.4) exposes the last
//! [`DROP_LOG_CAP`] entries; this module is the ring that feeds it.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// The manifest keeps at most this many recent drop-log entries (§5.4).
pub const DROP_LOG_CAP: usize = 100;

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
}
