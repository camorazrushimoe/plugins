//! Acceptance: oldest-date deletion, today-trim across every view,
//! open-row protection, zero-line guard, drop-log entries, CHECKPOINT
//! never moved backward, atomic rewrites without partial lines.

mod common;

use std::time::{Duration, SystemTime};

use common::{bytes, exists, file, raw_line, read, session_line, tmp_leftovers, write};
use wfdc::drop_log::Scope;

const WHEN: &str = "2026-08-31T07:06:54Z";

/// Fixed wall clock so drop-log `when` values are deterministic.
fn now() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_788_160_014)
}

/// Stream ids used by the fixtures (ascending ms).
const A: &str = "1725062400000-0";
const B: &str = "1725062401000-0";
const C: &str = "1725062402000-0";
const D: &str = "1725062403000-0";
const E: &str = "1725062404000-0";

// ---------------------------------------------------------------------------
// Oldest-date deletion
// ---------------------------------------------------------------------------

#[test]
fn oldest_date_deleted_from_every_view_and_parents_removed() {
    let t = common::TempDir::new("datedel");

    // 2026-08-29 exists in office raw, team dev-1 raw + sessions, and is the
    // ONLY date for team dev-2 (so teams/dev-2 must vanish entirely).
    write(
        &t.path,
        "raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(A)]),
    );
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(A)]),
    );
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-29/sessions.jsonl",
        &file(&[session_line("completed", Some(A), Some(B))]),
    );
    write(
        &t.path,
        "teams/dev-2/raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(A)]),
    );
    // 2026-08-30 survives.
    write(
        &t.path,
        "raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line(C)]),
    );
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line(C)]),
    );

    let d29_office = bytes(&raw_line(A));
    let d29_dev1_raw = bytes(&raw_line(A));
    let d29_dev1_sess = bytes(&session_line("completed", Some(A), Some(B)));
    let d29_dev2 = bytes(&raw_line(A));
    let freed_29 = d29_office + d29_dev1_raw + d29_dev1_sess + d29_dev2;
    let after_29 = bytes(&raw_line(C)) + bytes(&raw_line(C)); // 08-30 office + dev-1 raw

    let report = wfdc::enforce_bytes_cap(&t.path, after_29, now()).unwrap();

    // Every view of 2026-08-29 is gone.
    assert!(!exists(&t.join("raw/dt=2026-08-29/events.jsonl")));
    assert!(!exists(
        &t.join("teams/dev-1/raw/dt=2026-08-29/events.jsonl")
    ));
    assert!(!exists(
        &t.join("teams/dev-1/sessions/dt=2026-08-29/sessions.jsonl")
    ));
    // Empty parents removed: dev-2 had only 08-29 → whole team dir gone.
    assert!(!exists(&t.join("teams/dev-2")));
    // Non-empty parents stay.
    assert!(exists(&t.join("raw/dt=2026-08-30/events.jsonl")));
    assert!(exists(
        &t.join("teams/dev-1/raw/dt=2026-08-30/events.jsonl")
    ));

    assert_eq!(report.dates_deleted, vec!["2026-08-29".to_string()]);
    assert_eq!(report.events_trimmed, 0);
    assert_eq!(report.bytes_before - report.bytes_after, freed_29);

    // One drop-log line per dropped date, with scope=date and freed bytes.
    assert_eq!(report.drop_log.len(), 1);
    let e = report.drop_log.entries().front().unwrap();
    assert_eq!(e.when, WHEN);
    assert_eq!(e.scope, Scope::Date);
    assert_eq!(e.date.as_deref(), Some("2026-08-29"));
    assert_eq!(e.stream_id, None);
    assert_eq!(e.bytes_freed, freed_29);
}

#[test]
fn multiple_oldest_dates_deleted_until_single_date_remains() {
    let t = common::TempDir::new("datedel2");
    write(
        &t.path,
        "raw/dt=2026-08-27/events.jsonl",
        &file(&[raw_line(A)]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-28/events.jsonl",
        &file(&[raw_line(B)]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(C)]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line(D)]),
    );

    // Cap so small that even after deleting 27/28/29 we're still over the
    // single remaining date (08-30) → today-trim kicks in on 08-30.
    let report = wfdc::enforce_bytes_cap(&t.path, 1, now()).unwrap();

    assert_eq!(
        report.dates_deleted,
        vec!["2026-08-27", "2026-08-28", "2026-08-29"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    );
    assert!(!exists(&t.join("raw/dt=2026-08-27/events.jsonl")));
    assert!(!exists(&t.join("raw/dt=2026-08-28/events.jsonl")));
    assert!(!exists(&t.join("raw/dt=2026-08-29/events.jsonl")));
    // 08-30 still has its file — but its only event is guarded (a view
    // never drops to zero lines), so it survives.
    assert!(exists(&t.join("raw/dt=2026-08-30/events.jsonl")));
    assert_eq!(read(&t.join("raw/dt=2026-08-30/events.jsonl")), raw_line(D));
    // Three date entries in the drop log, oldest first.
    let entries: Vec<_> = report.drop_log.entries().iter().collect();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].date.as_deref(), Some("2026-08-27"));
    assert_eq!(entries[1].date.as_deref(), Some("2026-08-28"));
    assert_eq!(entries[2].date.as_deref(), Some("2026-08-29"));
}

// ---------------------------------------------------------------------------
// Today-trim (single date remains, still over the cap)
// ---------------------------------------------------------------------------

#[test]
fn today_trim_removes_events_ascending_from_every_view_and_closed_sessions() {
    let t = common::TempDir::new("today");

    // office raw: A B C D E — the canonical ordering.
    let office = file(&[
        raw_line(A),
        raw_line(B),
        raw_line(C),
        raw_line(D),
        raw_line(E),
    ]);
    // team dev-1 raw: A B C D  (E not in dev-1)
    let dev1_raw = file(&[raw_line(A), raw_line(B), raw_line(C), raw_line(D)]);
    // team dev-2 raw: D E
    let dev2_raw = file(&[raw_line(D), raw_line(E)]);
    // sessions dev-1: row1 completed A→B, row2 open C, row3 completed D→E.
    let dev1_sess = file(&[
        session_line("completed", Some(A), Some(B)),
        session_line("open", Some(C), None),
        session_line("completed", Some(D), Some(E)),
    ]);

    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-31/events.jsonl",
        &dev1_raw,
    );
    write(
        &t.path,
        "teams/dev-2/raw/dt=2026-08-31/events.jsonl",
        &dev2_raw,
    );
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl",
        &dev1_sess,
    );

    // Freed by trimming A: office A + dev-1 A + closed row1 (start edge A).
    let freed_a = bytes(&raw_line(A)) * 2 + bytes(&session_line("completed", Some(A), Some(B)));
    // Freed by trimming B: office B + dev-1 B (row1 already gone).
    let freed_b = bytes(&raw_line(B)) * 2;
    // Cap: after A and B are removed we are exactly at the cap.
    let total = bytes(&office) + bytes(&dev1_raw) + bytes(&dev2_raw) + bytes(&dev1_sess);
    let cap = total - freed_a - freed_b;

    let report = wfdc::enforce_bytes_cap(&t.path, cap, now()).unwrap();

    assert_eq!(report.events_trimmed, 2);
    // office raw now starts at C (A and B gone, order preserved).
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(C), raw_line(D), raw_line(E)])
    );
    // dev-1 raw: A B removed → C D.
    assert_eq!(
        read(&t.join("teams/dev-1/raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(C), raw_line(D)])
    );
    // dev-2 raw untouched (D E).
    assert_eq!(
        read(&t.join("teams/dev-2/raw/dt=2026-08-31/events.jsonl")),
        dev2_raw
    );
    // sessions: row1 (completed A→B) removed with A; row2 open stays; row3
    // completed D→E stays (D/E not trimmed).
    assert_eq!(
        read(&t.join("teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl")),
        file(&[
            session_line("open", Some(C), None),
            session_line("completed", Some(D), Some(E)),
        ])
    );

    // Drop log: one entry per trimmed event, ascending stream_id, with
    // per-event freed bytes.
    let entries: Vec<_> = report.drop_log.entries().iter().collect();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].scope, Scope::Today);
    assert_eq!(entries[0].stream_id.as_deref(), Some(A));
    assert_eq!(entries[0].bytes_freed, freed_a);
    assert_eq!(entries[1].stream_id.as_deref(), Some(B));
    assert_eq!(entries[1].bytes_freed, freed_b);
    assert_eq!(entries[0].when, WHEN);
    assert_eq!(entries[1].when, WHEN);
}

#[test]
fn open_rows_are_never_trimmed_even_when_their_start_event_is() {
    let t = common::TempDir::new("openrow");

    let office = file(&[raw_line(A), raw_line(B)]);
    let dev1_raw = file(&[raw_line(A), raw_line(B)]);
    // open row A (no finish), interrupted row B (no finish).
    let dev1_sess = file(&[
        session_line("open", Some(A), None),
        session_line("interrupted", Some(B), None),
        session_line("expired", Some(A), None),
    ]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-31/events.jsonl",
        &dev1_raw,
    );
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl",
        &dev1_sess,
    );

    let total = bytes(&office) + bytes(&dev1_raw) + bytes(&dev1_sess);
    // Cap: removing A (raw lines only — no closed row) brings us under.
    let freed_a_raw = bytes(&raw_line(A)) * 2;
    let cap = total - freed_a_raw;

    let report = wfdc::enforce_bytes_cap(&t.path, cap, now()).unwrap();
    assert_eq!(report.events_trimmed, 1);
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(B)])
    );
    // Sessions rows all survive: open/interrupted/expired are not closed.
    assert_eq!(
        read(&t.join("teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl")),
        dev1_sess
    );
}

#[test]
fn orphan_finish_row_is_trimmed_with_its_finish_event() {
    let t = common::TempDir::new("orphan");

    // The orphan finish event is the OLDEST event (smallest stream id), so
    // ascending order trims it first and the cap stops right there.
    const O: &str = "1725062399000-0";
    let office = file(&[raw_line(O), raw_line(A), raw_line(C)]);
    // orphan_finish has only a finish edge (O); the open row keeps the
    // view from hitting the zero-line guard.
    let dev1_sess = file(&[
        session_line("orphan_finish", None, Some(O)),
        session_line("open", Some(C), None),
    ]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl",
        &dev1_sess,
    );

    let total = bytes(&office) + bytes(&dev1_sess);
    let freed_o = bytes(&raw_line(O)) + bytes(&session_line("orphan_finish", None, Some(O)));
    let cap = total - freed_o;

    let report = wfdc::enforce_bytes_cap(&t.path, cap, now()).unwrap();
    assert_eq!(report.events_trimmed, 1);
    assert_eq!(
        report
            .drop_log
            .entries()
            .front()
            .unwrap()
            .stream_id
            .as_deref(),
        Some(O)
    );
    // office raw: O's line removed, A and C remain in order.
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(A), raw_line(C)])
    );
    // sessions: the closed orphan row (finish edge O) is gone; the open
    // row stays.
    assert_eq!(
        read(&t.join("teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl")),
        file(&[session_line("open", Some(C), None)])
    );
}

#[test]
fn guard_keeps_the_last_sessions_row() {
    let t = common::TempDir::new("sessguard");

    // The sessions view has exactly one row: an orphan_finish whose finish
    // edge is B. Trimming B would empty the view → the guard stops before
    // B (A may still be trimmed — it is not the last line of any view).
    let office = file(&[raw_line(A), raw_line(B)]);
    let dev1_sess = file(&[session_line("orphan_finish", None, Some(B))]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl",
        &dev1_sess,
    );

    let report = wfdc::enforce_bytes_cap(&t.path, 1, now()).unwrap();
    assert_eq!(report.events_trimmed, 1); // only A
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(B)])
    );
    assert_eq!(
        read(&t.join("teams/dev-1/sessions/dt=2026-08-31/sessions.jsonl")),
        dev1_sess
    );
    assert_eq!(
        report
            .drop_log
            .entries()
            .front()
            .unwrap()
            .stream_id
            .as_deref(),
        Some(A)
    );
    assert_eq!(report.drop_log.len(), 1);
}

#[test]
fn zero_line_guard_never_empties_a_view() {
    let t = common::TempDir::new("guard");

    // office raw: A B C D; dev-1 raw: A B (two lines).
    let office = file(&[raw_line(A), raw_line(B), raw_line(C), raw_line(D)]);
    let dev1_raw = file(&[raw_line(A), raw_line(B)]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-31/events.jsonl",
        &dev1_raw,
    );

    // Tiny cap: would like to trim A and B, but B is the last line of
    // dev-1 raw — the guard must stop before B.
    let report = wfdc::enforce_bytes_cap(&t.path, 1, now()).unwrap();

    assert_eq!(report.events_trimmed, 1);
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(B), raw_line(C), raw_line(D)])
    );
    assert_eq!(
        read(&t.join("teams/dev-1/raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(B)])
    );
    assert_eq!(
        report
            .drop_log
            .entries()
            .front()
            .unwrap()
            .stream_id
            .as_deref(),
        Some(A)
    );
    assert_eq!(report.drop_log.len(), 1);
}

#[test]
fn trim_stops_immediately_once_under_cap() {
    let t = common::TempDir::new("stop");

    let office = file(&[raw_line(A), raw_line(B), raw_line(C)]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    let total = bytes(&office);
    // Cap: removing A alone brings us exactly to the cap → stop at A.
    let cap = total - bytes(&raw_line(A));
    let report = wfdc::enforce_bytes_cap(&t.path, cap, now()).unwrap();
    assert_eq!(report.events_trimmed, 1);
    assert_eq!(
        read(&t.join("raw/dt=2026-08-31/events.jsonl")),
        file(&[raw_line(B), raw_line(C)])
    );
}

// ---------------------------------------------------------------------------
// CHECKPOINT / drop-log / atomicity guarantees
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_and_manifest_are_never_touched_by_trim() {
    let t = common::TempDir::new("checkpoint");

    write(
        &t.path,
        "raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(A)]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line(B), raw_line(C)]),
    );
    write(&t.path, "CHECKPOINT", "1725062401000-0");
    write(
        &t.path,
        "MANIFEST.json",
        "{\"checkpoint\":\"1725062401000-0\"}",
    );

    let report = wfdc::enforce_bytes_cap(&t.path, 1, now()).unwrap();
    assert_eq!(report.dates_deleted, vec!["2026-08-29".to_string()]);

    // CHECKPOINT unchanged (never moved backward, never rewritten).
    assert_eq!(read(&t.join("CHECKPOINT")), "1725062401000-0");
    assert_eq!(
        read(&t.join("MANIFEST.json")),
        "{\"checkpoint\":\"1725062401000-0\"}"
    );
}

#[test]
fn rewrites_leave_no_tmp_files_and_no_partial_lines() {
    let t = common::TempDir::new("atomic");

    let office = file(&[raw_line(A), raw_line(B), raw_line(C)]);
    let dev1_raw = file(&[raw_line(A), raw_line(B), raw_line(C)]);
    write(&t.path, "raw/dt=2026-08-31/events.jsonl", &office);
    write(
        &t.path,
        "teams/dev-1/raw/dt=2026-08-31/events.jsonl",
        &dev1_raw,
    );

    let cap = bytes(&office) + bytes(&dev1_raw) - bytes(&raw_line(A)) * 2;
    let report = wfdc::enforce_bytes_cap(&t.path, cap, now()).unwrap();
    assert_eq!(report.events_trimmed, 1);

    assert!(tmp_leftovers(&t.path).is_empty());
    // Every surviving JSONL file ends with '\n' and every line parses.
    for rel in [
        "raw/dt=2026-08-31/events.jsonl",
        "teams/dev-1/raw/dt=2026-08-31/events.jsonl",
    ] {
        let content = read(&t.join(rel));
        assert!(content.ends_with('\n'), "{rel} must end with newline");
        for line in content.lines() {
            assert!(
                serde_json::from_str::<serde_json::Value>(line).is_ok(),
                "{rel} contains a partial/ malformed line"
            );
        }
    }
}

#[test]
fn drop_log_bytes_freed_matches_on_disk_delta() {
    let t = common::TempDir::new("delta");

    write(
        &t.path,
        "raw/dt=2026-08-29/events.jsonl",
        &file(&[raw_line(A), raw_line(B)]),
    );
    write(
        &t.path,
        "raw/dt=2026-08-30/events.jsonl",
        &file(&[raw_line(C)]),
    );
    let before = bytes(&file(&[raw_line(A), raw_line(B)])) + bytes(&raw_line(C));

    let report = wfdc::enforce_bytes_cap(&t.path, 1, now()).unwrap();
    let after = bytes(&raw_line(C));
    assert_eq!(report.bytes_before, before);
    assert_eq!(report.bytes_after, after);
    assert_eq!(report.drop_log.bytes_freed(), before - after);
}
