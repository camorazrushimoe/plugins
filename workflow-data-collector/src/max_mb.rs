//! `max_mb` resolution and pinning (spec §2, §5.5).
//!
//! The raw value comes from the precedence chain CLI > env > config file
//! (resolved by the config module). This module applies the spec's pin:
//!
//! - `0`, a missing key, or a negative value → default **500**;
//! - values **1–15** → **16** (so one flush cannot livelock);
//! - anything ≥ 16 → unchanged.

/// Default cap when `max_mb` is 0, missing, or negative (spec §5.5).
pub const DEFAULT_MAX_MB: u64 = 500;

/// Floor applied to tiny positive values (spec §5.5: 1–15 → 16).
pub const FLOOR_MAX_MB: u64 = 16;

/// Pin a single raw `max_mb` value per spec §5.5.
///
/// `None` (key missing) behaves exactly like `0`/negative: default 500.
pub fn pin_max_mb(raw: Option<i64>) -> u64 {
    match raw {
        None => DEFAULT_MAX_MB,
        Some(v) if v <= 0 => DEFAULT_MAX_MB,
        Some(v) if (1..FLOOR_MAX_MB as i64).contains(&v) => FLOOR_MAX_MB,
        Some(v) => v as u64,
    }
}

/// Resolve `max_mb` from the precedence chain (CLI > env > file), then pin.
///
/// Each argument is the raw value from that source if present, else `None`.
/// The highest-precedence present value wins; `pin_max_mb` turns a missing
/// or non-positive result into the default.
pub fn resolve_max_mb(cli: Option<i64>, env: Option<i64>, file: Option<i64>) -> u64 {
    pin_max_mb(cli.or(env).or(file))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_zero_and_negative_default_to_500() {
        assert_eq!(pin_max_mb(None), 500);
        assert_eq!(pin_max_mb(Some(0)), 500);
        assert_eq!(pin_max_mb(Some(-1)), 500);
        assert_eq!(pin_max_mb(Some(-500)), 500);
    }

    #[test]
    fn one_through_fifteen_pin_to_16() {
        assert_eq!(pin_max_mb(Some(1)), 16);
        assert_eq!(pin_max_mb(Some(7)), 16);
        assert_eq!(pin_max_mb(Some(15)), 16);
        assert_eq!(pin_max_mb(Some(16)), 16);
    }

    #[test]
    fn values_at_or_above_16_are_unchanged() {
        assert_eq!(pin_max_mb(Some(16)), 16);
        assert_eq!(pin_max_mb(Some(17)), 17);
        assert_eq!(pin_max_mb(Some(500)), 500);
        assert_eq!(pin_max_mb(Some(1000)), 1000);
        assert_eq!(pin_max_mb(Some(2_000_000)), 2_000_000);
    }

    #[test]
    fn cli_beats_env_beats_file() {
        assert_eq!(resolve_max_mb(Some(1000), Some(2000), Some(3000)), 1000);
        assert_eq!(resolve_max_mb(None, Some(2000), Some(3000)), 2000);
        assert_eq!(resolve_max_mb(None, None, Some(3000)), 3000);
        assert_eq!(resolve_max_mb(None, None, None), 500);
        // A pinned CLI value still wins: cli 0 → default 500, env ignored.
        assert_eq!(resolve_max_mb(Some(0), Some(1000), None), 500);
        // cli 5 → 16 (floor) despite env 1000.
        assert_eq!(resolve_max_mb(Some(5), Some(1000), None), 16);
    }
}
