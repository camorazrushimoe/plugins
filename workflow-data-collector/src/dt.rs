//! Partitioning date (§4.2): `dt=YYYY-MM-DD` is the UTC date of the
//! `timestamp` (RFC 3339), falling back to the Redis stream-id millisecond
//! clock, which is the authority for partitioning and pairing order when the
//! timestamp is unparsable.

use chrono::{DateTime, Utc};

/// Parse an RFC 3339 timestamp to a UTC `DateTime`.
fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// The millisecond component of a Redis stream id (`"<ms>-<seq>"`).
fn stream_id_ms(stream_id: &str) -> u64 {
    crate::streamid::StreamId::parse(stream_id)
        .map(|id| id.ms())
        .unwrap_or(0)
}

/// `dt=` partition date for an event (§4.2): the envelope `timestamp` when it
/// parses as RFC 3339, else the stream-id millisecond clock.
pub fn dt_for(stream_id: &str, ts: Option<&str>) -> String {
    let ms = ts
        .and_then(parse_rfc3339)
        .map(|dt| dt.timestamp_millis() as u64)
        .unwrap_or_else(|| stream_id_ms(stream_id));
    dt_of_ms(ms)
}

/// `YYYY-MM-DD` (UTC) for epoch milliseconds.
fn dt_of_ms(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) as u32) * 1_000_000;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

/// Full UTC instant for an event (§4.2): the envelope `timestamp` when it
/// parses as RFC 3339, else the Redis stream-id millisecond clock, else the
/// Unix epoch. The pairing engine (§5.3) uses this for `started_at` /
/// `finished_at` / `duration_ms`; it applies exactly the same fallback rule
/// as [`dt_for`], so a session row's `dt=` partition always agrees with the
/// raw line's.
pub fn resolve(stream_id: &str, ts: Option<&str>) -> DateTime<Utc> {
    ts.and_then(parse_rfc3339).unwrap_or_else(|| {
        DateTime::<Utc>::from_timestamp_millis(stream_id_ms(stream_id) as i64)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_timestamp_wins() {
        assert_eq!(
            dt_for("9999999999999-0", Some("2026-08-30T21:00:00Z")),
            "2026-08-30"
        );
        assert_eq!(
            dt_for("9999999999999-0", Some("2026-08-30T23:59:59Z")),
            "2026-08-30"
        );
    }

    #[test]
    fn rfc3339_with_offset_uses_utc_date() {
        // 2026-08-31T00:30:00+02:00 == 2026-08-30T22:30:00Z → previous UTC day.
        assert_eq!(
            dt_for("1-0", Some("2026-08-31T00:30:00+02:00")),
            "2026-08-30"
        );
    }

    #[test]
    fn unparsable_timestamp_falls_back_to_stream_id_clock() {
        assert_eq!(dt_for("1725062400000-0", Some("not-a-date")), "2024-08-31");
        assert_eq!(dt_for("1725062400000-0", None), "2024-08-31");
    }

    #[test]
    fn stream_id_seq_ignored() {
        assert_eq!(dt_for("1725062400000-42", None), "2024-08-31");
    }

    #[test]
    fn garbage_stream_id_falls_back_to_epoch() {
        assert_eq!(dt_for("garbage", None), "1970-01-01");
        assert_eq!(dt_for("", None), "1970-01-01");
    }

    #[test]
    fn resolve_prefers_rfc3339() {
        let dt = resolve("9999999999999-0", Some("2026-08-30T21:00:00Z"));
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "2026-08-30T21:00:00Z"
        );
    }

    #[test]
    fn resolve_offset_timestamp_converts_to_utc() {
        let dt = resolve("1-0", Some("2026-08-31T00:30:00+02:00"));
        assert_eq!(
            dt.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "2026-08-30T22:30:00Z"
        );
    }

    #[test]
    fn resolve_falls_back_to_stream_id_clock() {
        let dt = resolve("1725062400000-42", Some("not-a-date"));
        assert_eq!(dt.timestamp_millis(), 1725062400000);
        let dt2 = resolve("1725062400000-42", None);
        assert_eq!(dt2.timestamp_millis(), 1725062400000);
    }

    #[test]
    fn resolve_garbage_falls_back_to_epoch() {
        let dt = resolve("garbage", None);
        assert_eq!(dt, DateTime::<Utc>::UNIX_EPOCH);
    }

    #[test]
    fn resolve_and_dt_for_agree() {
        // Same rule → the session row's dt= matches the raw line's dt=.
        assert_eq!(
            dt_for("1725062400000-0", Some("not-a-date")),
            resolve("1725062400000-0", Some("not-a-date"))
                .format("%Y-%m-%d")
                .to_string()
        );
        assert_eq!(
            dt_for("1725062400000-0", Some("2026-08-30T21:00:00Z")),
            resolve("1725062400000-0", Some("2026-08-30T21:00:00Z"))
                .format("%Y-%m-%d")
                .to_string()
        );
    }
}
