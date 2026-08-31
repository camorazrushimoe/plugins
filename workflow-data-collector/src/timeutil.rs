//! Timestamp contract (§4.2): `timestamp` is parsed as RFC 3339; anything
//! unparsable falls back to the Redis stream-id millisecond clock, which is
//! the authority for partitioning (`dt=`) and pairing order.

use chrono::{DateTime, Utc};

/// Parse an RFC 3339 timestamp into epoch milliseconds (UTC).
pub fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// The millisecond component of a Redis stream id ("<ms>-<seq>").
pub fn stream_id_ms(stream_id: &str) -> i64 {
    stream_id
        .split('-')
        .next()
        .and_then(|p| p.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Effective event clock in epoch ms: envelope RFC 3339 when valid, else the
/// stream-id millisecond clock (§4.2).
pub fn event_ms(stream_id: &str, ts: Option<&str>) -> i64 {
    ts.and_then(parse_rfc3339_ms)
        .unwrap_or_else(|| stream_id_ms(stream_id))
}

/// Partitioning date `YYYY-MM-DD` (UTC) for the given clock ms.
pub fn dt_of_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as u32) * 1_000_000;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

/// Combined: `(dt=YYYY-MM-DD, epoch_ms)` for one event, honouring the §4.2
/// fallback chain.
pub fn dt_and_ms(stream_id: &str, ts: Option<&str>) -> (String, i64) {
    let ms = event_ms(stream_id, ts);
    (dt_of_ms(ms), ms)
}

/// Format epoch ms as an RFC 3339 UTC string (for `started_at`/`finished_at`
/// when the envelope timestamp was unparsable — stream-id clock fallback).
pub fn ms_to_rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) as u32) * 1_000_000;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_with_z_and_offset() {
        assert_eq!(
            parse_rfc3339_ms("2026-08-30T21:00:00Z"),
            Some(1788123600000)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-08-30T23:00:00+02:00"),
            Some(1788123600000)
        );
    }

    #[test]
    fn garbage_falls_back_to_stream_clock() {
        assert_eq!(parse_rfc3339_ms("not-a-date"), None);
        let (dt, ms) = dt_and_ms("1725062400000-3", Some("garbage"));
        assert_eq!(ms, 1725062400000);
        assert_eq!(dt, "2024-08-31");
        // missing timestamp → stream clock too
        let (dt2, _) = dt_and_ms("1725062400000-3", None);
        assert_eq!(dt2, dt);
    }

    #[test]
    fn stream_id_ms_parsing() {
        assert_eq!(stream_id_ms("1725062400000-0"), 1725062400000);
        assert_eq!(stream_id_ms("0-1"), 0);
    }

    #[test]
    fn dt_partitioning_utc() {
        assert_eq!(dt_of_ms(1788123600000), "2026-08-30");
        assert_eq!(dt_of_ms(0), "1970-01-01");
    }
}
