//! Redis stream id — `<ms>-<seq>` — as one small comparable type.
//!
//! Shared by the three places that must understand stream ids: the follow
//! loop (dedupe ordering), the checkpoint (validity), and the `dt=`
//! partitioning (millisecond clock fallback, §4.2).

/// A Redis stream id, compared numerically (ms, then seq).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    ms: u64,
    seq: u64,
}

impl StreamId {
    /// Parse `<ms>-<seq>` (both u64) or the special `0` (before all entries).
    pub fn parse(s: &str) -> Option<StreamId> {
        if s == "0" {
            return Some(StreamId { ms: 0, seq: 0 });
        }
        let mut parts = s.split('-');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(ms), Some(seq), None) => Some(StreamId {
                ms: ms.parse().ok()?,
                seq: seq.parse().ok()?,
            }),
            _ => None,
        }
    }

    /// The millisecond component — the §4.2 `dt=` clock fallback.
    pub fn ms(self) -> u64 {
        self.ms
    }
}

/// CHECKPOINT validity: a full stream id or `0`.
pub fn is_valid(s: &str) -> bool {
    StreamId::parse(s).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_ids_and_zero() {
        assert_eq!(StreamId::parse("0"), Some(StreamId { ms: 0, seq: 0 }));
        assert_eq!(
            StreamId::parse("1725062400000-0"),
            Some(StreamId {
                ms: 1725062400000,
                seq: 0
            })
        );
        assert_eq!(
            StreamId::parse("1725062400000-42"),
            Some(StreamId {
                ms: 1725062400000,
                seq: 42
            })
        );
        assert_eq!(StreamId::parse(""), None);
        assert_eq!(StreamId::parse("1725062400000"), None);
        assert_eq!(StreamId::parse("abc-0"), None);
        assert_eq!(StreamId::parse("1-2-3"), None);
    }

    #[test]
    fn orders_numerically_not_lexically() {
        // "9" vs "10" must compare by value, not by string
        assert!(
            StreamId::parse("1725062400000-9").unwrap()
                < StreamId::parse("1725062400000-10").unwrap()
        );
        assert!(StreamId::parse("1-0").unwrap() > StreamId::parse("0").unwrap());
    }

    #[test]
    fn ms_component() {
        assert_eq!(
            StreamId::parse("1725062400000-7").unwrap().ms(),
            1725062400000
        );
        assert_eq!(StreamId::parse("0").unwrap().ms(), 0);
    }

    #[test]
    fn validity() {
        assert!(is_valid("0"));
        assert!(is_valid("1-0"));
        assert!(!is_valid("garbage"));
        assert!(!is_valid("123"));
    }
}
