//! Acceptance: cap math, max_mb pinning, bytes accounting.

mod common;

use std::time::{Duration, SystemTime};
use wfdc::max_mb::{pin_max_mb, resolve_max_mb, DEFAULT_MAX_MB, FLOOR_MAX_MB};

const NOW: SystemTime = SystemTime::UNIX_EPOCH;

#[test]
fn max_mb_pinning_0_missing_negative_to_500() {
    assert_eq!(pin_max_mb(None), DEFAULT_MAX_MB);
    assert_eq!(pin_max_mb(Some(0)), DEFAULT_MAX_MB);
    assert_eq!(pin_max_mb(Some(-7)), DEFAULT_MAX_MB);
}

#[test]
fn max_mb_pinning_1_to_15_becomes_16() {
    for v in 1..=15 {
        assert_eq!(
            pin_max_mb(Some(v)),
            FLOOR_MAX_MB,
            "raw {v} should pin to 16"
        );
    }
    assert_eq!(pin_max_mb(Some(16)), FLOOR_MAX_MB);
}

#[test]
fn max_mb_pinning_large_values_kept() {
    assert_eq!(pin_max_mb(Some(500)), 500);
    assert_eq!(pin_max_mb(Some(2000)), 2000);
}

#[test]
fn max_mb_precedence_cli_env_file() {
    assert_eq!(resolve_max_mb(Some(42), Some(43), Some(44)), 42);
    assert_eq!(resolve_max_mb(None, Some(43), Some(44)), 43);
    assert_eq!(resolve_max_mb(None, None, Some(44)), 44);
    assert_eq!(resolve_max_mb(None, None, None), 500);
}

#[test]
fn under_cap_is_a_no_op() {
    use common::{file, raw_line, write};
    let t = common::TempDir::new("noop");
    write(
        &t.path,
        "raw/dt=2026-08-31/events.jsonl",
        &file(&[raw_line("1725062400000-0"), raw_line("1725062401000-0")]),
    );
    let report = wfdc::enforce_cap(&t.path, 500, NOW).unwrap();
    assert_eq!(report.bytes_before, report.bytes_after);
    assert!(report.dates_deleted.is_empty());
    assert_eq!(report.events_trimmed, 0);
    assert!(report.drop_log.is_empty());
    // Files untouched.
    assert_eq!(
        common::read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line("1725062400000-0"), raw_line("1725062401000-0")])
    );
}

#[test]
fn cap_counts_all_jsonl_and_excludes_manifest_checkpoint_lock() {
    use common::{bytes, file, raw_line, session_line, write};
    let t = common::TempDir::new("counts");
    let office = file(&[
        raw_line("1725062400000-0"),
        raw_line("1725062401000-0"),
        raw_line("1725062402000-0"),
    ]);
    let team = file(&[raw_line("1725062400000-0"), raw_line("1725062401000-0")]);
    let sessions = file(&[session_line(
        "completed",
        Some("1725062400000-0"),
        Some("1725062401000-0"),
    )]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(&t.path, "teams/dev-1/raw/dt=2026-08-31/events.jsonl", &team);
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl",
        &sessions,
    );
    // Non-jsonl files with large sizes: must NOT count toward the cap.
    write(&t.path, "MANIFEST.json", &"x".repeat(50_000));
    write(&t.path, "CHECKPOINT", "1725062402000-0");
    write(&t.path, ".lock", "pid 1234");

    let expected = bytes(&office) + bytes(&team) + bytes(&sessions);
    let dd = wfdc::layout::scan(&t.path).unwrap();
    assert_eq!(dd.jsonl_bytes(), expected);
    assert_eq!(dd.raw_views.len(), 2);
    assert_eq!(dd.sessions_views.len(), 1);
    assert_eq!(dd.other_jsonl.len(), 0);
}

#[test]
fn cap_math_max_mb_to_bytes() {
    // Spec §5.5: cap = max_mb * 1024 * 1024.
    assert_eq!(500_u64 * 1024 * 1024, wfdc::MB * 500);
    // enforce_cap(max_mb=16) is byte-identical to a 16 MiB byte cap.
    use common::{file, raw_line, write};
    let t = common::TempDir::new("capmath");
    let content = file(&[raw_line("1725062400000-0")]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &content);
    let via_mb = wfdc::enforce_cap(&t.path, 16, NOW).unwrap();
    let via_bytes = wfdc::enforce_bytes_cap(&t.path, 16 * wfdc::MB, NOW).unwrap();
    assert_eq!(via_mb.bytes_after, via_bytes.bytes_after);
    assert_eq!(via_mb.events_trimmed, via_bytes.events_trimmed);
}

#[test]
fn missing_data_dir_is_a_no_op() {
    let t = common::TempDir::new("missing");
    let report = wfdc::enforce_cap(&t.join("does-not-exist"), 1, NOW).unwrap();
    assert_eq!(report.bytes_before, 0);
    assert_eq!(report.bytes_after, 0);
    assert_eq!(report.events_trimmed, 0);
}

#[test]
fn fixed_now_yields_deterministic_rfc3339_when() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_788_160_014);
    use common::{file, raw_line, write};
    let t = common::TempDir::new("when");
    // Over the cap (cap 1 byte) with two dates → oldest date deleted.
    write(
        &t.path,
        "raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line("1725062000000-0")]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line("1725062400000-0")]),
    );
    let report = wfdc::enforce_bytes_cap(&t.path, 1, now).unwrap();
    let e = report.drop_log.entries().front().unwrap();
    assert_eq!(e.when, "2026-08-31T07:06:54Z");
}
