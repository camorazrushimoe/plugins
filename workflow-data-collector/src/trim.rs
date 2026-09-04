//! Disk cap enforcement — spec §5.5.
//!
//! Deterministic, observable trim:
//!
//! 1. Sum every `*.jsonl` byte under `data_dir`; under the cap → stop.
//! 2. While over the cap and more than one `dt=` date exists: delete the
//!    oldest date everywhere it appears (office `raw/`, every
//!    `teams/*/raw/`, every `teams/*/sessions/`), remove empty parent
//!    dirs, log one drop-log line per dropped date.
//! 3. With only one date left and still over the cap: trim complete
//!    events in ascending `stream_id` order, oldest first, removing the
//!    event's line from every view that contains it — office `raw/`,
//!    each `teams/*/raw/`, and the `sessions/` row when the event is the
//!    start or finish edge of a **closed** session. `open` rows are never
//!    trimmed, and no view ever drops to zero lines. Each trimmed event
//!    is recorded in the drop log. Rewrites go through `*.tmp` + atomic
//!    rename; a partial JSON line is never emitted.
//!
//! `CHECKPOINT` is never moved backward — the trim does not touch it (it
//! is not `*.jsonl`). Dropped rows are a visible gap, not a crash; Redis
//! itself is never trimmed.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::drop_log::{DropLog, DropLogEntry};
use crate::layout::{self, DataDir, RawView, SessionsView, ViewKind};

/// 1 MiB; the cap is `max_mb * MB` (spec §5.5 step 1).
pub const MB: u64 = 1024 * 1024;

/// Outcome of one enforcement run (one flush, or start of `follow`).
#[derive(Debug, Clone)]
pub struct TrimReport {
    /// JSONL bytes on disk before the trim.
    pub bytes_before: u64,
    /// JSONL bytes on disk after the trim (best effort — see §5.5).
    pub bytes_after: u64,
    /// `dt=` partitions deleted in order (oldest first).
    pub dates_deleted: Vec<String>,
    /// Events trimmed from today's views.
    pub events_trimmed: usize,
    /// Entries appended during this run (date deletions + event trims).
    pub drop_log: DropLog,
}

/// Enforce the `max_mb` cap (spec §5.5). Call after every successful
/// flush and at the start of `follow` (the cap may have been lowered).
pub fn enforce_cap(data_dir: &Path, max_mb: u64, now: SystemTime) -> io::Result<TrimReport> {
    enforce_bytes_cap(data_dir, max_mb.saturating_mul(MB), now)
}

/// Enforce a byte cap directly. `max_mb` callers should use
/// [`enforce_cap`]; this is exposed for tests and byte-exact callers.
pub fn enforce_bytes_cap(
    data_dir: &Path,
    max_bytes: u64,
    now: SystemTime,
) -> io::Result<TrimReport> {
    if !data_dir.is_dir() {
        return Ok(TrimReport {
            bytes_before: 0,
            bytes_after: 0,
            dates_deleted: Vec::new(),
            events_trimmed: 0,
            drop_log: DropLog::new(),
        });
    }

    let mut tree = layout::scan(data_dir)?;
    let bytes_before = tree.jsonl_bytes();
    let mut drop_log = DropLog::new();
    let mut bytes = bytes_before;
    let mut dates_deleted = Vec::new();
    let mut events_trimmed = 0usize;
    let when = rfc3339(now);

    // §5.5 step 2: while over the cap and more than one date exists,
    // delete the oldest date everywhere it appears.
    while bytes > max_bytes {
        let dates = tree.dates();
        if dates.len() <= 1 {
            break;
        }
        let oldest = dates.into_iter().next().expect("len > 1");
        let freed = delete_date(&mut tree, &oldest, data_dir)?;
        bytes = bytes.saturating_sub(freed);
        dates_deleted.push(oldest.clone());
        drop_log.push(DropLogEntry::date(&when, &oldest, freed));
    }

    // §5.5 step 3: only one date remains and still over the cap →
    // trim complete events in ascending stream_id order.
    if bytes > max_bytes {
        let (removed, freed) = trim_today(&mut tree, bytes, max_bytes, &when, &mut drop_log)?;
        events_trimmed = removed;
        bytes = bytes.saturating_sub(freed);
    }

    Ok(TrimReport {
        bytes_before,
        bytes_after: bytes,
        dates_deleted,
        events_trimmed,
        drop_log,
    })
}

/// §5.5 step 2: delete the oldest date from every view, remove empty
/// parent dirs, and return the bytes freed.
fn delete_date(tree: &mut DataDir, date: &str, root: &Path) -> io::Result<u64> {
    let mut freed = 0u64;

    let mut keep_raw = Vec::with_capacity(tree.raw_views.len());
    for v in std::mem::take(&mut tree.raw_views) {
        if v.date == date {
            freed += v.bytes;
            fs::remove_file(&v.path)?;
            remove_empty_parents(&v.path, root);
        } else {
            keep_raw.push(v);
        }
    }
    tree.raw_views = keep_raw;

    let mut keep_sess = Vec::with_capacity(tree.sessions_views.len());
    for v in std::mem::take(&mut tree.sessions_views) {
        if v.date == date {
            freed += v.bytes;
            fs::remove_file(&v.path)?;
            remove_empty_parents(&v.path, root);
        } else {
            keep_sess.push(v);
        }
    }
    tree.sessions_views = keep_sess;

    Ok(freed)
}

/// §5.5 step 3: event-level trim on the single remaining date.
/// Returns (events trimmed, bytes freed); drop-log entries are appended
/// to `drop_log` as events are removed.
fn trim_today(
    tree: &mut DataDir,
    bytes: u64,
    max_bytes: u64,
    when: &str,
    drop_log: &mut DropLog,
) -> io::Result<(usize, u64)> {
    // Phase A guarantees at most one date remains; anything else is a
    // no-op (no events to trim, or the tree shape changed under us).
    let dates = tree.dates();
    if dates.len() != 1 {
        return Ok((0, 0));
    }
    let date = dates.into_iter().next().expect("len == 1");

    let raw_idx: Vec<usize> = tree
        .raw_views
        .iter()
        .enumerate()
        .filter(|(_, v)| v.date == date)
        .map(|(i, _)| i)
        .collect();
    let sess_idx: Vec<usize> = tree
        .sessions_views
        .iter()
        .enumerate()
        .filter(|(_, v)| v.date == date)
        .map(|(i, _)| i)
        .collect();
    if raw_idx.is_empty() {
        return Ok((0, 0));
    }

    // Canonical event list: the office raw view contains every event and
    // is already in stream order; fall back to the union of team raws.
    let mut events: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    if let Some(&off) = raw_idx
        .iter()
        .find(|&&i| tree.raw_views[i].kind == ViewKind::OfficeRaw)
    {
        for line in &tree.raw_views[off].lines {
            if let Some(sid) = &line.stream_id {
                if seen.insert(sid.clone()) {
                    events.push(sid.clone());
                }
            }
        }
    }
    if events.is_empty() {
        for &i in &raw_idx {
            for line in &tree.raw_views[i].lines {
                if let Some(sid) = &line.stream_id {
                    if seen.insert(sid.clone()) {
                        events.push(sid.clone());
                    }
                }
            }
        }
    }
    // Ascending stream_id order, oldest first (spec §5.5 step 3).
    events.sort_by_key(|id| stream_key(id));
    if events.is_empty() {
        return Ok((0, 0));
    }

    // Per-view line lookup and remaining-line counts for the zero-line guard.
    let mut raw_lookup: Vec<HashMap<String, usize>> = Vec::with_capacity(raw_idx.len());
    let mut raw_remaining: Vec<usize> = Vec::with_capacity(raw_idx.len());
    for &i in &raw_idx {
        let mut m = HashMap::new();
        for (li, line) in tree.raw_views[i].lines.iter().enumerate() {
            if let Some(sid) = &line.stream_id {
                m.entry(sid.clone()).or_insert(li);
            }
        }
        raw_lookup.push(m);
        raw_remaining.push(tree.raw_views[i].lines.len());
    }
    let mut sess_remaining: Vec<usize> = sess_idx
        .iter()
        .map(|&i| tree.sessions_views[i].rows.len())
        .collect();

    let mut drop_raw: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); raw_idx.len()];
    let mut drop_sess: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); sess_idx.len()];

    let mut removed = 0usize;
    let mut freed_total = 0u64;
    let mut over = bytes;

    for eid in &events {
        let mut freed = 0u64;
        let mut guarded = false;

        // Raw views containing this event's line.
        for (vi, &i) in raw_idx.iter().enumerate() {
            if let Some(&li) = raw_lookup[vi].get(eid) {
                if raw_remaining[vi] <= 1 {
                    guarded = true; // removing eid would empty this view
                    break;
                }
                freed += tree.raw_views[i].lines[li].byte_len();
            }
        }
        if guarded {
            break;
        }

        // Sessions views: the event as an edge of a closed session row.
        // Rows already scheduled for removal (an earlier edge) are skipped.
        for (si, &i) in sess_idx.iter().enumerate() {
            for (ri, row) in tree.sessions_views[i].rows.iter().enumerate() {
                if !is_closed(&row.state) {
                    continue;
                }
                let is_edge = row.start_stream_id.as_deref() == Some(eid.as_str())
                    || row.finish_stream_id.as_deref() == Some(eid.as_str());
                if is_edge && !drop_sess[si].contains(&ri) {
                    if sess_remaining[si] <= 1 {
                        guarded = true;
                        break;
                    }
                    freed += row.byte_len();
                }
            }
            if guarded {
                break;
            }
        }
        if guarded {
            break;
        }

        // An event always has at least its office/team raw line; if freed
        // is 0 the event is in no view — nothing to trim.
        if freed == 0 {
            continue;
        }

        // Remove the event from every view that contains it.
        for (vi, _i) in raw_idx.iter().enumerate() {
            if let Some(&li) = raw_lookup[vi].get(eid) {
                drop_raw[vi].insert(li);
                raw_remaining[vi] -= 1;
            }
        }
        for (si, &i) in sess_idx.iter().enumerate() {
            let rows = &tree.sessions_views[i].rows;
            for (ri, row) in rows.iter().enumerate() {
                if !is_closed(&row.state) {
                    continue;
                }
                let is_edge = row.start_stream_id.as_deref() == Some(eid.as_str())
                    || row.finish_stream_id.as_deref() == Some(eid.as_str());
                if is_edge && drop_sess[si].insert(ri) {
                    // insert() returns false for a row already dropped by
                    // its other edge — only decrement on a new removal.
                    sess_remaining[si] -= 1;
                }
            }
        }

        over = over.saturating_sub(freed);
        freed_total += freed;
        removed += 1;
        drop_log.push(DropLogEntry::event(when, eid, freed));

        if over <= max_bytes {
            break; // under the cap — stop
        }
    }

    // Rewrite touched views via *.tmp + atomic rename.
    for (vi, &i) in raw_idx.iter().enumerate() {
        rewrite_raw_view(&mut tree.raw_views[i], &drop_raw[vi])?;
    }
    for (si, &i) in sess_idx.iter().enumerate() {
        rewrite_sessions_view(&mut tree.sessions_views[i], &drop_sess[si])?;
    }

    Ok((removed, freed_total))
}

/// Remove empty parent directories up to (not including) `root`.
fn remove_empty_parents(file: &Path, root: &Path) {
    let mut dir = file.parent();
    while let Some(d) = dir {
        if d == root || !d.starts_with(root) {
            break;
        }
        match d.read_dir() {
            // Only remove when truly empty; a non-empty dir means every
            // ancestor is non-empty too, so stop walking up.
            Ok(mut rd) => {
                if rd.next().is_none() {
                    let _ = fs::remove_dir(d);
                } else {
                    break;
                }
            }
            Err(_) => break,
        }
        dir = d.parent();
    }
}

/// Rewrite a raw view dropping the given line indices (via tmp + rename).
fn rewrite_raw_view(view: &mut RawView, drop: &BTreeSet<usize>) -> io::Result<()> {
    if drop.is_empty() {
        return Ok(());
    }
    let mut out = Vec::new();
    for (i, line) in view.lines.iter().enumerate() {
        if !drop.contains(&i) {
            out.extend_from_slice(&line.bytes);
        }
    }
    atomic_write(&view.path, &out)?;
    view.bytes = out.len() as u64;
    view.lines = view
        .lines
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop.contains(i))
        .map(|(_, l)| l.clone())
        .collect();
    Ok(())
}

/// Rewrite a sessions view dropping the given row indices.
fn rewrite_sessions_view(view: &mut SessionsView, drop: &BTreeSet<usize>) -> io::Result<()> {
    if drop.is_empty() {
        return Ok(());
    }
    let mut out = Vec::new();
    for (i, row) in view.rows.iter().enumerate() {
        if !drop.contains(&i) {
            out.extend_from_slice(&row.bytes);
        }
    }
    atomic_write(&view.path, &out)?;
    view.bytes = out.len() as u64;
    view.rows = view
        .rows
        .iter()
        .enumerate()
        .filter(|(i, _)| !drop.contains(i))
        .map(|(_, r)| r.clone())
        .collect();
    Ok(())
}

/// Atomic write: `*.tmp` + rename, 0600 perms, fsync before rename.
fn atomic_write(path: &Path, content: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    {
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp)?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// `events.jsonl` → `events.jsonl.tmp` (same directory → same filesystem).
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// `now` as RFC 3339 UTC (`2026-08-31T07:06:54Z`).
pub(crate) fn rfc3339(now: SystemTime) -> String {
    let secs = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    unix_to_rfc3339(secs)
}

fn unix_to_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Days since epoch → (year, month, day); Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Numeric sort key for a Redis stream id (`<ms>-<seq>`); unparseable
/// ids sort last, deterministically, by their raw text.
fn stream_key(id: &str) -> (u64, u64, String) {
    match id.split_once('-') {
        Some((ms, seq)) => match (ms.parse::<u64>(), seq.parse::<u64>()) {
            (Ok(m), Ok(s)) => (m, s, id.to_string()),
            _ => (u64::MAX, 0, id.to_string()),
        },
        None => (u64::MAX, 0, id.to_string()),
    }
}

/// A session row is "closed" (has a finish edge) when it is `completed`
/// or `orphan_finish`. `open`, `interrupted` and `expired` rows have no
/// finish edge and are never trimmed (§5.5 step 3).
fn is_closed(state: &str) -> bool {
    matches!(state, "completed" | "orphan_finish")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_matches_reference_epochs() {
        assert_eq!(unix_to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(unix_to_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(unix_to_rfc3339(1_788_160_014), "2026-08-31T07:06:54Z");
    }

    #[test]
    fn stream_key_orders_numerically() {
        let mut ids = vec![
            "1725062401000-0",
            "1725062400000-10",
            "999-0",
            "1725062400000-0",
        ];
        ids.sort_by_key(|i| stream_key(i));
        assert_eq!(
            ids,
            vec![
                "999-0",
                "1725062400000-0",
                "1725062400000-10",
                "1725062401000-0"
            ]
        );
    }

    #[test]
    fn closed_state_matches_only_finished_rows() {
        assert!(is_closed("completed"));
        assert!(is_closed("orphan_finish"));
        assert!(!is_closed("open"));
        assert!(!is_closed("interrupted"));
        assert!(!is_closed("expired"));
    }

    #[test]
    fn cap_math_is_mb_times_mib() {
        assert_eq!(MB, 1024 * 1024);
        assert_eq!(16_u64.saturating_mul(MB), 16_777_216);
        assert_eq!(500_u64.saturating_mul(MB), 524_288_000);
        // Saturation guards absurd values instead of overflowing.
        assert_eq!(u64::MAX.saturating_mul(MB), u64::MAX);
    }
}
