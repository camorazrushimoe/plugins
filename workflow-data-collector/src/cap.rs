//! Wiring seam for the §5.5 disk cap.
//!
//! The trim library (`trim::enforce_cap`) is invoked from the two places
//! the spec mandates — after every successful flush, and at the start of
//! `follow` (the cap may have been lowered while the collector was
//! stopped). This module owns the *policy* around that call: enforcement
//! is best-effort, never fatal. A failed trim is logged loudly, but it
//! must not stop collection — the cap is a disk guard, the pipeline is
//! the point. The outcome of a successful trim (dates deleted, events
//! trimmed, bytes freed, drop-log entries) is logged so it is observable
//! in the collector's logs; the entries are appended to the persisted
//! drop-log ring (`DROP_LOG.json`, §5.4) so a later `wfdc status` process
//! can show them too.

use std::path::Path;
use std::time::SystemTime;

/// Run one cap enforcement, log the outcome, and persist the successful
/// trim's drop-log entries to the on-disk ring (`DROP_LOG.json`, §5.4).
///
/// `max_mb` is expected to be already normalized (spec §2: 0/negative →
/// 500, 1–15 → 16). `now` is the wall clock used for drop-log `when`
/// timestamps. Ring persistence is best-effort (warn, never fatal) — the
/// cap is a disk guard, the manifest feed is observability.
pub fn enforce(data_dir: &Path, max_mb: u64, now: SystemTime) {
    match crate::trim::enforce_cap(data_dir, max_mb, now) {
        Ok(report) => {
            if !report.drop_log.is_empty() {
                log::info!(
                    "max_mb enforcement: {} -> {} bytes ({} date(s) deleted, {} event(s) trimmed, {} drop-log entries)",
                    report.bytes_before,
                    report.bytes_after,
                    report.dates_deleted.len(),
                    report.events_trimmed,
                    report.drop_log.len(),
                );
                // §5.4: persist the successful trim's entries so a later
                // `wfdc status` process shows them (last 100, oldest
                // evicted). Best-effort — never abort collection.
                if let Err(e) =
                    crate::drop_log::append(data_dir, report.drop_log.entries().iter().cloned())
                {
                    log::warn!("drop-log ring persistence failed: {e}");
                }
            }
        }
        Err(e) => log::warn!("max_mb enforcement failed: {e}"),
    }
}
