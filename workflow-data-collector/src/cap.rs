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
//! in the collector's logs; the drop-log ring itself feeds the
//! MANIFEST/status surface (§5.4).

use std::path::Path;
use std::time::SystemTime;

/// Run one cap enforcement and log the outcome.
///
/// `max_mb` is expected to be already normalized (spec §2: 0/negative →
/// 500, 1–15 → 16). `now` is the wall clock used for drop-log `when`
/// timestamps.
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
            }
        }
        Err(e) => log::warn!("max_mb enforcement failed: {e}"),
    }
}
