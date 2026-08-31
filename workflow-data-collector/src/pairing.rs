//! Session pairing (§5.3): one `task.started` paired with a later
//! `task.finished` per agent turn.
//!
//! Rules implemented here (all deterministic):
//!
//! - Unmatched starts are bucketed by `(team, actor)`; a finish attaches to
//!   the **oldest** unmatched start (lowest `start_stream_id`) with a
//!   compatible `session_id` (both empty or equal; missing ≡ empty).
//! - A finish pairs only with a start whose `start_stream_id <
//!   finish_stream_id` — otherwise `orphan_finish` (this prevents negative
//!   `duration_ms` structurally).
//! - A new start for an agent that already has an unmatched start marks the
//!   previous row `interrupted`. **`interrupted` is not terminal:** it stays
//!   in the pool and a later compatible finish still pairs with it, flipping
//!   it `interrupted` → `completed` via the normal upsert — the only legal
//!   state change for an interrupted row (so interrupted rows never expire).
//! - A start that already has a finish leaves the pool.
//! - Expiry is wall-clock elapsed since `started_at`, evaluated on every
//!   `age()` call (the follow loop calls it on every read iteration,
//!   including empty rounds). `expired` is terminal: a late finish becomes
//!   `orphan_finish`; an expired row is never resurrected.
//!
//! `session_pk` = sha256 of `team|actor|start_stream_id` — stable across
//! rebuilds of the derived table from the raw that is still on disk. Orphan
//! finishes (no start) use their own finish stream id.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::decode::Decoded;
use crate::sessions::{SessionRow, SessionWrite, State};
use crate::Error;

/// `session_pk` = sha256 of `team|actor|stream_id` (lowercase hex).
fn sha256_hex(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}

/// §5.3: sha256 of `team|actor|start_stream_id`; orphan finishes use their
/// own finish stream id (they have no start).
fn session_pk(team: &str, actor: &str, stream_id: &str) -> String {
    sha256_hex(&format!("{team}|{actor}|{stream_id}"))
}

/// session_id compatibility: both empty or equal; missing ≡ empty (§5.3).
fn session_id_compatible(start: Option<&str>, finish: Option<&str>) -> bool {
    let a = start.unwrap_or("");
    let b = finish.unwrap_or("");
    (a.is_empty() && b.is_empty()) || a == b
}

/// `session_id` from the decoded payload: missing or empty ≡ `None`.
fn payload_session_id(payload: &Value) -> Option<String> {
    payload
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn payload_snippet(payload: &Value) -> Option<String> {
    payload
        .get("snippet")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// One `issues` / `prs` / `linear` ref list from `task_ref` (a JSON object).
/// A non-object `task_ref` (plain string, §4.1 step 4) carries no refs here —
/// it stays on the raw line, never dropped.
fn refs_from(payload: &Value, name: &str) -> Option<Vec<String>> {
    let arr = payload.get("task_ref")?.get(name)?.as_array()?;
    let items: Vec<String> = arr
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

/// Union of two ref lists (start + finish), deduped, first-seen order kept.
fn union_refs(a: Option<Vec<String>>, b: Option<Vec<String>>) -> Option<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for item in a.into_iter().flatten().chain(b.into_iter().flatten()) {
        if seen.insert(item.clone()) {
            out.push(item);
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// `handoff` from the finish: a JSON string stays a string; any other JSON
/// value (object/array) is kept as compact JSON so nothing is dropped.
fn handoff_from(payload: &Value) -> Option<String> {
    match payload.get("handoff")? {
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Numeric sort key of a stream id (`StreamId` is `(ms, seq)` ordered);
/// `None` when the id is missing or unparsable.
fn stream_id_key(id: Option<&str>) -> Option<crate::streamid::StreamId> {
    id.and_then(crate::streamid::StreamId::parse)
}

/// The pairing engine. Feed decoded events in stream order; call `age(now)`
/// on every read iteration (including empty rounds); write `take_writes()`
/// results with a [`crate::sessions::SessionStore`].
#[derive(Debug)]
pub struct Pairer {
    expiry_hours: u64,
    /// All session rows by `session_pk` (open, completed, orphan, expired…).
    rows: HashMap<String, SessionRow>,
    /// Unmatched starts by `(team, actor)` — `session_pk`s, FIFO by
    /// `start_stream_id`. `open` + `interrupted` rows live here.
    pool: HashMap<(String, String), Vec<String>>,
    /// Partition keys `(team_folder, dt)` whose `sessions.jsonl` changed.
    dirty: BTreeSet<(String, String)>,
}

impl Pairer {
    pub fn new(expiry_hours: u64) -> Self {
        Pairer {
            expiry_hours,
            rows: HashMap::new(),
            pool: HashMap::new(),
            dirty: BTreeSet::new(),
        }
    }

    pub fn expiry_hours(&self) -> u64 {
        self.expiry_hours
    }

    /// Rebuild in-memory state from the rows already on disk (startup /
    /// backfill). `open` and `interrupted` rows re-enter the unmatched pool so
    /// a finish still pairs with a start flushed by an earlier run (§5.3 —
    /// cross-batch pool persistence).
    pub fn rebuild(&mut self, rows: Vec<SessionRow>) {
        self.rows.clear();
        self.pool.clear();
        self.dirty.clear();
        for row in rows {
            let pk = row.session_pk.clone();
            match row.state {
                State::Open | State::Interrupted => {
                    self.pool
                        .entry((row.team.clone(), row.actor.clone()))
                        .or_default()
                        .push(pk.clone());
                    self.rows.insert(pk, row);
                }
                _ => {
                    self.rows.insert(pk, row);
                }
            }
        }
    }

    /// Process one decoded event. Only `task.started` / `task.finished` pair
    /// (§4.2); every other action is ignored.
    pub fn ingest(&mut self, d: &Decoded) -> Result<(), Error> {
        match d.line.action.as_deref() {
            Some("task.started") => self.start(d),
            Some("task.finished") => self.finish(d),
            _ => Ok(()),
        }
    }

    /// §5.3 expiry: wall-clock elapsed since `started_at` on every read
    /// iteration. `open` rows past the window become `expired` (terminal) and
    /// leave the pool. `interrupted` rows are exempt — their only legal
    /// transition is `interrupted` → `completed`.
    pub fn age(&mut self, now: DateTime<Utc>) {
        let window = Duration::hours(self.expiry_hours as i64);
        let mut to_expire: Vec<String> = Vec::new();
        for pks in self.pool.values() {
            for pk in pks {
                let row = &self.rows[pk];
                if row.state != State::Open {
                    continue;
                }
                let started = row
                    .started_at
                    .as_deref()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.with_timezone(&Utc))
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                if now.signed_duration_since(started) > window {
                    to_expire.push(pk.clone());
                }
            }
        }
        for pk in to_expire {
            let row = self.rows.get_mut(&pk).expect("row in pool is in rows");
            row.state = State::Expired;
            let loc = row.location();
            self.dirty.insert(loc);
            self.pool
                .get_mut(&(row.team.clone(), row.actor.clone()))
                .expect("bucket exists")
                .retain(|p| p != &pk);
        }
        self.pool.retain(|_, pks| !pks.is_empty());
    }

    /// Full rewrites for every partition touched since the last call. Each
    /// `SessionWrite` carries the **complete** row set for that
    /// `(team_folder, dt)` file (§5.3 upsert semantics), in deterministic
    /// order.
    pub fn take_writes(&mut self) -> Vec<SessionWrite> {
        let dirty = std::mem::take(&mut self.dirty);
        let mut writes = Vec::with_capacity(dirty.len());
        for (folder, dt) in dirty {
            let mut rows: Vec<SessionRow> = self
                .rows
                .values()
                .filter(|r| r.location() == (folder.clone(), dt.clone()))
                .cloned()
                .collect();
            rows.sort_by(|a, b| {
                stream_id_key(
                    a.start_stream_id
                        .as_deref()
                        .or(a.finish_stream_id.as_deref()),
                )
                .cmp(&stream_id_key(
                    b.start_stream_id
                        .as_deref()
                        .or(b.finish_stream_id.as_deref()),
                ))
                .then_with(|| a.session_pk.cmp(&b.session_pk))
            });
            writes.push(SessionWrite {
                team_folder: folder,
                dt,
                rows,
            });
        }
        writes
    }

    /// A `task.started` opens a row; if the agent already has an unmatched
    /// start, that previous row becomes `interrupted` (§5.3 rule 3).
    fn start(&mut self, d: &Decoded) -> Result<(), Error> {
        let team = d.line.team.clone().unwrap_or_default();
        let actor = d.line.actor.clone().unwrap_or_default();
        let stream_id = d.line.stream_id.clone();
        let resolved = crate::dt::resolve(&stream_id, d.line.ts.as_deref());
        let pk = session_pk(&team, &actor, &stream_id);
        let location = (
            crate::team::team_safe(Some(&team), Some(&actor)),
            resolved.format("%Y-%m-%d").to_string(),
        );

        // §5.3 rule 3: the newest unmatched start (highest start_stream_id)
        // is interrupted by this newer start. Only an `open` row flips —
        // `interrupted` → `open` is not a legal transition.
        if let Some(pks) = self.pool.get_mut(&(team.clone(), actor.clone())) {
            if let Some(newest) = pks
                .iter()
                .max_by_key(|pk| stream_id_key(self.rows[*pk].start_stream_id.as_deref()))
                .cloned()
            {
                let row = self.rows.get_mut(&newest).expect("pool pk in rows");
                if row.state == State::Open {
                    row.state = State::Interrupted;
                    self.dirty.insert(row.location());
                }
            }
        }

        let row = SessionRow {
            session_pk: pk.clone(),
            team: team.clone(),
            actor: actor.clone(),
            session_id: payload_session_id(&d.line.payload),
            start_stream_id: Some(stream_id),
            finish_stream_id: None,
            started_at: Some(resolved.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)),
            finished_at: None,
            duration_ms: None,
            state: State::Open,
            snippet_in: payload_snippet(&d.line.payload),
            snippet_out: None,
            issues: refs_from(&d.line.payload, "issues"),
            prs: refs_from(&d.line.payload, "prs"),
            linear: refs_from(&d.line.payload, "linear"),
            handoff: None,
            project: d.line.project.clone(),
        };
        self.rows.insert(pk.clone(), row);
        self.pool.entry((team, actor)).or_default().push(pk);
        self.dirty.insert(location);
        Ok(())
    }

    /// A `task.finished` attaches to the oldest unmatched start in the
    /// `(team, actor)` bucket with a compatible `session_id` and a strictly
    /// earlier `start_stream_id`; otherwise it becomes `orphan_finish`.
    fn finish(&mut self, d: &Decoded) -> Result<(), Error> {
        let team = d.line.team.clone().unwrap_or_default();
        let actor = d.line.actor.clone().unwrap_or_default();
        let finish_id = d.line.stream_id.clone();
        let finish_ts = crate::dt::resolve(&finish_id, d.line.ts.as_deref());
        let finish_sid = payload_session_id(&d.line.payload);
        let bucket_key = (team.clone(), actor.clone());

        let matched: Option<String> = self.pool.get(&bucket_key).and_then(|pks| {
            let finish_key = stream_id_key(Some(&finish_id));
            pks.iter()
                .filter(|pk| {
                    let row = &self.rows[*pk];
                    // stream-id ordering: pair only with a start whose
                    // start_stream_id < finish_stream_id (both parseable);
                    // otherwise orphan_finish (no negative duration possible).
                    let start_before_finish =
                        match (stream_id_key(row.start_stream_id.as_deref()), finish_key) {
                            (Some(s), Some(f)) => s < f,
                            _ => false,
                        };
                    session_id_compatible(row.session_id.as_deref(), finish_sid.as_deref())
                        && start_before_finish
                })
                .min_by_key(|pk| stream_id_key(self.rows[*pk].start_stream_id.as_deref()))
                .cloned()
        });

        match matched {
            Some(pk) => {
                let row = self.rows.get_mut(&pk).expect("matched pk in rows");
                let started = row
                    .started_at
                    .as_deref()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                row.state = State::Completed;
                row.finish_stream_id = Some(finish_id);
                row.finished_at =
                    Some(finish_ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true));
                // Structurally non-negative (§5.3 stream-id ordering guard);
                // clamp so a skewed envelope clock can never yield a negative
                // duration on the row.
                row.duration_ms = Some((finish_ts - started).num_milliseconds().max(0));
                row.snippet_out = payload_snippet(&d.line.payload);
                row.handoff = handoff_from(&d.line.payload);
                row.issues = union_refs(row.issues.take(), refs_from(&d.line.payload, "issues"));
                row.prs = union_refs(row.prs.take(), refs_from(&d.line.payload, "prs"));
                row.linear = union_refs(row.linear.take(), refs_from(&d.line.payload, "linear"));
                if row.project.is_none() {
                    row.project = d.line.project.clone();
                }
                let loc = row.location();
                self.dirty.insert(loc);
                // A start that already has a finish leaves the pool.
                if let Some(pks) = self.pool.get_mut(&bucket_key) {
                    pks.retain(|p| p != &pk);
                }
                self.pool.retain(|_, pks| !pks.is_empty());
                Ok(())
            }
            None => {
                // orphan_finish: no compatible unmatched start. It has no
                // start, so it lives on the dt= of its own finish timestamp in
                // the team folder derived from its own team (§5.3). Kept.
                let pk = session_pk(&team, &actor, &finish_id);
                let location = (
                    crate::team::team_safe(Some(&team), Some(&actor)),
                    finish_ts.format("%Y-%m-%d").to_string(),
                );
                let row = SessionRow {
                    session_pk: pk.clone(),
                    team,
                    actor,
                    session_id: finish_sid,
                    start_stream_id: None,
                    finish_stream_id: Some(finish_id),
                    started_at: None,
                    finished_at: Some(
                        finish_ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                    ),
                    duration_ms: None,
                    state: State::OrphanFinish,
                    snippet_in: None,
                    snippet_out: payload_snippet(&d.line.payload),
                    issues: refs_from(&d.line.payload, "issues"),
                    prs: refs_from(&d.line.payload, "prs"),
                    linear: refs_from(&d.line.payload, "linear"),
                    handoff: handoff_from(&d.line.payload),
                    project: d.line.project.clone(),
                };
                self.rows.insert(pk, row);
                self.dirty.insert(location);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode;
    use std::collections::BTreeMap;

    /// 2026-08-30T21:00:00Z — the started_at used by most tests.
    fn t_start() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-30T21:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// Build a decoded event from a flat wire map.
    fn ev(id: &str, pairs: &[(&str, &str)]) -> Decoded {
        let flat: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        decode::decode(id, &flat)
    }

    fn start(id: &str, team: &str, actor: &str, ts: &str, sid: Option<&str>) -> Decoded {
        let mut pairs = vec![
            ("action", "task.started"),
            ("team", team),
            ("actor", actor),
            ("timestamp", ts),
        ];
        if let Some(s) = sid {
            pairs.push(("session_id", s));
        }
        ev(id, &pairs)
    }

    fn finish(id: &str, team: &str, actor: &str, ts: &str, sid: Option<&str>) -> Decoded {
        let mut pairs = vec![
            ("action", "task.finished"),
            ("team", team),
            ("actor", actor),
            ("timestamp", ts),
        ];
        if let Some(s) = sid {
            pairs.push(("session_id", s));
        }
        ev(id, &pairs)
    }

    fn write_set(p: &mut Pairer) -> Vec<SessionWrite> {
        p.take_writes()
    }

    fn rows_of(writes: &[SessionWrite]) -> Vec<SessionRow> {
        writes.iter().flat_map(|w| w.rows.iter().cloned()).collect()
    }

    #[test]
    fn start_then_finish_completes_with_fields() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "1725062400000-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            Some("s1"),
        ))
        .unwrap();
        let writes = write_set(&mut p);
        let rows = rows_of(&writes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, State::Open);
        p.ingest(&finish(
            "1725062401000-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some("s1"),
        ))
        .unwrap();
        let writes = write_set(&mut p);
        let rows = rows_of(&writes);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.state, State::Completed);
        assert_eq!(r.start_stream_id.as_deref(), Some("1725062400000-0"));
        assert_eq!(r.finish_stream_id.as_deref(), Some("1725062401000-0"));
        assert_eq!(r.duration_ms, Some(1000));
        assert_eq!(r.session_id.as_deref(), Some("s1"));
    }

    #[test]
    fn session_pk_is_sha256_of_team_actor_start_id() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "1725062400000-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        let writes = write_set(&mut p);
        let rows = rows_of(&writes);
        let pk = &rows[0].session_pk;
        let expected = format!("{:x}", Sha256::digest(b"dev-1|developer|1725062400000-0"));
        assert_eq!(pk, &expected);
    }

    #[test]
    fn fifo_pairs_oldest_unmatched_start() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            Some("s1"),
        ))
        .unwrap();
        p.ingest(&start(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some("s1"),
        ))
        .unwrap();
        p.take_writes();
        // finish compatible with both → must pair with the OLDEST (100-0)
        p.ingest(&finish(
            "102-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:02Z",
            Some("s1"),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let completed: Vec<&SessionRow> = rows
            .iter()
            .filter(|r| r.state == State::Completed)
            .collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start_stream_id.as_deref(), Some("100-0"));
        // the other start stays open
        let open: Vec<&SessionRow> = rows.iter().filter(|r| r.state == State::Open).collect();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].start_stream_id.as_deref(), Some("101-0"));
    }

    #[test]
    fn fifo_respects_session_id_filter() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            Some("a"),
        ))
        .unwrap();
        p.ingest(&start(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some("b"),
        ))
        .unwrap();
        p.take_writes();
        // finish session_id=b → skips the older incompatible start (a)
        p.ingest(&finish(
            "102-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:02Z",
            Some("b"),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let completed: Vec<&SessionRow> = rows
            .iter()
            .filter(|r| r.state == State::Completed)
            .collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start_stream_id.as_deref(), Some("101-0"));
    }

    #[test]
    fn empty_session_id_equals_missing() {
        let mut p = Pairer::new(6);
        // start missing session_id, finish with explicit empty string → pair
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        p.ingest(&finish(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some(""),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, State::Completed);
    }

    #[test]
    fn mismatched_session_id_is_orphan_finish() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            Some("a"),
        ))
        .unwrap();
        p.take_writes();
        // finish with a different session id → no compatible start → orphan
        p.ingest(&finish(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some("b"),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let orphan: Vec<&SessionRow> = rows
            .iter()
            .filter(|r| r.state == State::OrphanFinish)
            .collect();
        assert_eq!(orphan.len(), 1);
        assert_eq!(orphan[0].start_stream_id, None);
        assert_eq!(orphan[0].finish_stream_id.as_deref(), Some("101-0"));
        assert_eq!(orphan[0].session_id.as_deref(), Some("b"));
    }

    #[test]
    fn finish_before_any_start_is_orphan() {
        let mut p = Pairer::new(6);
        p.ingest(&finish(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, State::OrphanFinish);
    }

    #[test]
    fn stream_order_guard_prevents_negative_duration() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "200-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        // finish stream id EARLIER than the start's → must not pair
        p.ingest(&finish(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T20:00:00Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        assert!(
            rows.iter().any(|r| r.state == State::OrphanFinish),
            "must be orphan, not completed"
        );
        assert!(rows
            .iter()
            .all(|r| r.duration_ms.is_none() || r.duration_ms.unwrap() >= 0));
        // the start remains in the pool (open)
        assert!(rows.iter().any(|r| r.state == State::Open));
    }

    #[test]
    fn new_start_interrupts_previous_and_previous_stays_pairable() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            Some("s1"),
        ))
        .unwrap();
        p.ingest(&start(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            Some("s2"),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let s1 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("100-0"))
            .unwrap();
        assert_eq!(s1.state, State::Interrupted);
        let s2 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("101-0"))
            .unwrap();
        assert_eq!(s2.state, State::Open);

        // a finish compatible with the interrupted start flips it to completed
        p.ingest(&finish(
            "102-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:02Z",
            Some("s1"),
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let s1 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("100-0"))
            .unwrap();
        assert_eq!(
            s1.state,
            State::Completed,
            "interrupted → completed via upsert"
        );
        let s2 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("101-0"))
            .unwrap();
        assert_eq!(s2.state, State::Open, "unrelated start untouched");
    }

    #[test]
    fn start_with_finish_leaves_the_pool() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.ingest(&finish(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        // second finish: no unmatched start left → orphan
        p.ingest(&finish(
            "102-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:02Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let orphan: Vec<&SessionRow> = rows
            .iter()
            .filter(|r| r.state == State::OrphanFinish)
            .collect();
        assert_eq!(orphan.len(), 1);
    }

    #[test]
    fn orphan_placement_uses_finish_timestamp_and_finish_team() {
        let mut p = Pairer::new(6);
        // no start at all; finish's team maps to a specific folder
        p.ingest(&finish(
            "1725062400000-0",
            "lab-1",
            "researcher",
            "2026-08-30T23:59:00Z",
            None,
        ))
        .unwrap();
        let writes = write_set(&mut p);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].team_folder, "lab-1");
        assert_eq!(writes[0].dt, "2026-08-30");
        assert_eq!(writes[0].rows[0].state, State::OrphanFinish);
        // orphan rows are KEPT: they appear in writes and would be on disk
        assert_eq!(writes[0].rows.len(), 1);
    }

    #[test]
    fn orphan_with_unparsable_timestamp_uses_stream_id_clock() {
        let mut p = Pairer::new(6);
        p.ingest(&finish(
            "1725062400000-0",
            "lab-1",
            "researcher",
            "not-a-date",
            None,
        ))
        .unwrap();
        let writes = write_set(&mut p);
        assert_eq!(writes[0].dt, "2024-08-31", "stream-id ms clock fallback");
    }

    #[test]
    fn expiry_after_window_on_empty_round() {
        let mut p = Pairer::new(6);
        let t0 = t_start();
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        // empty round: no events, age() alone must expire the open row
        p.age(t0 + Duration::hours(6) + Duration::seconds(1));
        let rows = rows_of(&write_set(&mut p));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, State::Expired);
    }

    #[test]
    fn expiry_boundary_exactly_at_window_is_not_expired() {
        let mut p = Pairer::new(6);
        let t0 = t_start();
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        p.age(t0 + Duration::hours(6));
        assert!(
            p.rows.values().all(|r| r.state == State::Open),
            "strictly longer than the window is required to expire"
        );
        assert!(
            p.take_writes().is_empty(),
            "nothing is dirty at exactly the window"
        );
    }

    #[test]
    fn expired_is_terminal_late_finish_becomes_orphan() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        p.age(t_start() + Duration::hours(7));
        p.take_writes();
        // late finish must NOT resurrect the expired row → orphan_finish
        p.ingest(&finish(
            "200-0",
            "dev-1",
            "developer",
            "2026-08-30T22:00:00Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        assert!(
            rows.iter().any(|r| r.state == State::Expired),
            "expired row stays expired"
        );
        assert!(
            rows.iter().any(|r| r.state == State::OrphanFinish),
            "late finish is orphan"
        );
        assert!(!rows.iter().any(|r| r.state == State::Completed));
    }

    #[test]
    fn interrupted_never_expires() {
        let mut p = Pairer::new(6);
        let t0 = t_start();
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.ingest(&start(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:01Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        // 100 hours later: the open row expires, the interrupted row does not
        p.age(t0 + Duration::hours(100));
        let rows = rows_of(&write_set(&mut p));
        let s1 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("100-0"))
            .unwrap();
        assert_eq!(
            s1.state,
            State::Interrupted,
            "interrupted → completed is the only legal transition"
        );
        let s2 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("101-0"))
            .unwrap();
        assert_eq!(s2.state, State::Expired);
        // the interrupted start is still pairable
        p.ingest(&finish(
            "200-0",
            "dev-1",
            "developer",
            "2026-08-30T22:00:00Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut p));
        let s1 = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("100-0"))
            .unwrap();
        assert_eq!(s1.state, State::Completed);
    }

    #[test]
    fn midnight_session_lives_on_start_dt() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T23:59:00Z",
            None,
        ))
        .unwrap();
        p.take_writes();
        p.ingest(&finish(
            "101-0",
            "dev-1",
            "developer",
            "2026-08-31T00:01:00Z",
            None,
        ))
        .unwrap();
        let writes = write_set(&mut p);
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].dt, "2026-08-30",
            "open rows live on the dt= of started_at, even after midnight"
        );
        assert_eq!(writes[0].rows[0].state, State::Completed);
        assert_eq!(
            writes[0].rows[0].finished_at.as_deref(),
            Some("2026-08-31T00:01:00Z")
        );
    }

    #[test]
    fn other_actions_are_ignored() {
        let mut p = Pairer::new(6);
        let d = ev(
            "100-0",
            &[
                ("action", "agent.started"),
                ("team", "dev-1"),
                ("actor", "developer"),
            ],
        );
        p.ingest(&d).unwrap();
        assert!(
            write_set(&mut p).is_empty(),
            "only task.started/task.finished pair (§4.2)"
        );
    }

    #[test]
    fn deterministic_identical_stream_identical_writes() {
        let feed: Vec<Decoded> = vec![
            start(
                "100-0",
                "dev-1",
                "developer",
                "2026-08-30T21:00:00Z",
                Some("s1"),
            ),
            start(
                "101-0",
                "dev-1",
                "developer",
                "2026-08-30T21:00:01Z",
                Some("s2"),
            ),
            finish(
                "102-0",
                "dev-1",
                "developer",
                "2026-08-30T21:00:02Z",
                Some("s1"),
            ),
            finish("103-0", "lab-1", "researcher", "2026-08-30T21:00:03Z", None),
        ];
        let mut a = Pairer::new(6);
        let mut b = Pairer::new(6);
        for d in &feed {
            a.ingest(d).unwrap();
            b.ingest(d).unwrap();
        }
        let wa = a.take_writes();
        let wb = b.take_writes();
        assert_eq!(
            wa, wb,
            "identical stream → identical writes (and session_pk)"
        );
        // session_pk stability across rebuilds from the same rows: the rebuilt
        // engine holds the identical row set.
        let rows = wa
            .iter()
            .flat_map(|w| w.rows.iter())
            .cloned()
            .collect::<Vec<_>>();
        let mut c = Pairer::new(6);
        c.rebuild(rows);
        assert_eq!(a.rows, c.rows, "rebuild reproduces the identical row set");
        assert!(
            c.take_writes().is_empty(),
            "no partition is dirty after a bare rebuild"
        );
    }

    #[test]
    fn rebuild_restores_pool_from_disk_rows() {
        let mut p = Pairer::new(6);
        p.ingest(&start(
            "100-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:00Z",
            None,
        ))
        .unwrap();
        p.ingest(&finish(
            "102-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:02Z",
            None,
        ))
        .unwrap();
        p.ingest(&start(
            "103-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:03Z",
            None,
        ))
        .unwrap();
        let rows: Vec<SessionRow> = p.take_writes().into_iter().flat_map(|w| w.rows).collect();
        // restart: rebuild from disk; the open row must be pairable again
        let mut q = Pairer::new(6);
        q.rebuild(rows);
        q.ingest(&finish(
            "200-0",
            "dev-1",
            "developer",
            "2026-08-30T21:00:04Z",
            None,
        ))
        .unwrap();
        let rows = rows_of(&write_set(&mut q));
        let open_start = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("103-0"))
            .unwrap();
        assert_eq!(
            open_start.state,
            State::Completed,
            "post-restart finish pairs with reloaded start"
        );
        let old = rows
            .iter()
            .find(|r| r.start_stream_id.as_deref() == Some("100-0"))
            .unwrap();
        assert_eq!(
            old.state,
            State::Completed,
            "already-completed row is untouched"
        );
    }

    #[test]
    fn refs_union_from_start_and_finish() {
        let mut p = Pairer::new(6);
        let s = ev(
            "100-0",
            &[
                ("action", "task.started"),
                ("team", "dev-1"),
                ("actor", "developer"),
                ("timestamp", "2026-08-30T21:00:00Z"),
                ("task_ref", r#"{"issues":["A"],"prs":["1"]}"#),
            ],
        );
        p.ingest(&s).unwrap();
        let f = ev(
            "101-0",
            &[
                ("action", "task.finished"),
                ("team", "dev-1"),
                ("actor", "developer"),
                ("timestamp", "2026-08-30T21:00:01Z"),
                ("task_ref", r#"{"issues":["A","B"],"linear":["L1"]}"#),
                ("handoff", "summary here"),
            ],
        );
        p.ingest(&f).unwrap();
        let rows = rows_of(&write_set(&mut p));
        let r = &rows[0];
        assert_eq!(r.state, State::Completed);
        assert_eq!(
            r.issues.as_deref(),
            Some(&["A".to_string(), "B".to_string()][..])
        );
        assert_eq!(r.prs.as_deref(), Some(&["1".to_string()][..]));
        assert_eq!(r.linear.as_deref(), Some(&["L1".to_string()][..]));
        assert_eq!(r.handoff.as_deref(), Some("summary here"));
    }
}
