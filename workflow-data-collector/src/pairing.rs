//! Session pairing (§5.3): one `task.started` paired with a later
//! `task.finished` per agent turn. FIFO per `(team, actor)`, stream-id
//! ordering, `interrupted` stays pairable, expiry is terminal.
//!
//! Hermes `session_id` is a conversation id, not a turn id — the pairing key
//! is **not** `(team, actor, session_id)`.

use std::collections::HashMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::decoder::Decoded;
use crate::sessions::{
    Placement, SessionRow, SessionStore, STATE_COMPLETED, STATE_EXPIRED, STATE_INTERRUPTED,
    STATE_OPEN, STATE_ORPHAN_FINISH,
};
use crate::team::team_folder;
use crate::timeutil::{dt_and_ms, event_ms};
use crate::Error;

/// An unmatched start sitting in the pairing pool.
#[derive(Debug, Clone)]
struct StartRef {
    stream_id: String,
    /// Session id as on the start (None ≡ empty/missing).
    session_id: Option<String>,
    placement: Placement,
    pk: String,
}

/// The pairing engine. Feed events in stream order.
#[derive(Debug, Default)]
pub struct Pairer {
    store: SessionStore,
    /// Bucket: (team, actor) → FIFO of unmatched starts (oldest first).
    pool: HashMap<(String, String), Vec<StartRef>>,
}

fn sha256_hex(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}

/// `session_pk` = sha256 of `team|actor|start_stream_id`; orphan finishes use
/// their own finish stream id (they have no start).
fn session_pk(team: Option<&str>, actor: Option<&str>, id: &str) -> String {
    sha256_hex(&format!(
        "{}|{}|{}",
        team.unwrap_or(""),
        actor.unwrap_or(""),
        id
    ))
}

fn bucket_key(team: Option<&str>, actor: Option<&str>) -> (String, String) {
    (
        team.unwrap_or("").to_string(),
        actor.unwrap_or("").to_string(),
    )
}

/// session_id extraction: "" ≡ missing (None).
fn session_id_of(d: &Decoded) -> Option<String> {
    d.payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn snippet_of(d: &Decoded) -> Option<String> {
    d.payload
        .get("snippet")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn ref_of(d: &Decoded, name: &str) -> Option<Value> {
    d.payload.get("task_ref").and_then(|t| t.get(name)).cloned()
}

fn handoff_of(d: &Decoded) -> Option<Value> {
    d.payload.get("handoff").cloned()
}

/// Union of refs from start + finish (§5.3 "union of start+finish refs").
fn union_refs(a: Option<&Value>, b: Option<&Value>) -> Option<Value> {
    match (a, b) {
        (Some(Value::Array(x)), Some(Value::Array(y))) => {
            let mut v = x.clone();
            v.extend(y.iter().cloned());
            Some(Value::Array(v))
        }
        (Some(v), None) => Some(v.clone()),
        (None, Some(v)) => Some(v.clone()),
        (Some(v), Some(_)) => Some(v.clone()), // non-array pair: start wins (deterministic)
        (None, None) => None,
    }
}

impl Pairer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn store(&self) -> &SessionStore {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut SessionStore {
        &mut self.store
    }

    /// Rebuild the unmatched-start pool from the rows already in the store
    /// (`open` / `interrupted` rows on disk from a previous run). Backfill
    /// calls this after loading the store so a finish in the chosen range
    /// still pairs with a start that was flushed earlier (<= checkpoint) —
    /// the same cross-batch pool persistence follow has in memory.
    pub fn rebuild_pool(&mut self) {
        self.pool.clear();
        for (placement, rows) in self.store.rows() {
            for row in rows.values() {
                if row.state != STATE_OPEN && row.state != STATE_INTERRUPTED {
                    continue;
                }
                let Some(start_id) = row.start_stream_id.clone() else {
                    continue;
                };
                let key = bucket_key(row.team.as_deref(), row.actor.as_deref());
                self.pool.entry(key).or_default().push(StartRef {
                    stream_id: start_id,
                    session_id: row.session_id.clone().filter(|s| !s.is_empty()),
                    placement: placement.clone(),
                    pk: row.session_pk.clone(),
                });
            }
        }
        // Oldest first = lowest start_stream_id, per §5.3 rule 2.
        for v in self.pool.values_mut() {
            v.sort_by_key(|s| crate::checkpoint::parse_id(&s.stream_id));
        }
    }

    /// Handle one decoded event. Only `task.started` / `task.finished`
    /// participate (§4.2); every other action is stored raw and ignored here.
    pub fn on_event(&mut self, d: &Decoded, stream_id: &str) -> Result<(), Error> {
        match d.action.as_deref() {
            Some("task.started") => self.on_start(d, stream_id),
            Some("task.finished") => self.on_finish(d, stream_id),
            _ => Ok(()),
        }
    }

    fn on_start(&mut self, d: &Decoded, stream_id: &str) -> Result<(), Error> {
        let key = bucket_key(d.team.as_deref(), d.actor.as_deref());
        let session_id = session_id_of(d);
        let (dt, _ms) = dt_and_ms(stream_id, d.ts.as_deref());
        let folder = team_folder(d.team.as_deref(), d.actor.as_deref());
        let placement: Placement = (folder, dt);
        let pk = session_pk(d.team.as_deref(), d.actor.as_deref(), stream_id);

        // A new start while this agent already has an unmatched start marks the
        // previous (most recent) row `interrupted` and opens a new one.
        if let Some(prev) = self.pool.get(&key).and_then(|v| v.last()) {
            let prev_placement = prev.placement.clone();
            let prev_pk = prev.pk.clone();
            if let Some(row) = self.store.get(&prev_placement, &prev_pk) {
                if row.state == STATE_OPEN {
                    let mut updated = row.clone();
                    updated.state = STATE_INTERRUPTED.to_string();
                    self.store.upsert(prev_placement, updated);
                }
            }
        }

        let row = SessionRow {
            session_pk: pk.clone(),
            team: d.team.clone(),
            actor: d.actor.clone(),
            session_id: session_id.clone(),
            start_stream_id: Some(stream_id.to_string()),
            finish_stream_id: None,
            started_at: d.ts.clone(),
            finished_at: None,
            duration_ms: None,
            state: STATE_OPEN.to_string(),
            snippet_in: snippet_of(d),
            snippet_out: None,
            issues: ref_of(d, "issues"),
            prs: ref_of(d, "prs"),
            linear: ref_of(d, "linear"),
            handoff: None,
            project: d.project.clone(),
        };
        self.store.upsert(placement.clone(), row);
        self.pool.entry(key).or_default().push(StartRef {
            stream_id: stream_id.to_string(),
            session_id: session_id.clone(),
            placement,
            pk,
        });
        Ok(())
    }

    fn on_finish(&mut self, d: &Decoded, stream_id: &str) -> Result<(), Error> {
        let key = bucket_key(d.team.as_deref(), d.actor.as_deref());
        let session_id = session_id_of(d);
        let finish_id = stream_id.to_string();

        // Oldest compatible unmatched start with start_stream_id < finish id.
        let (start_id, start_seq) = crate::checkpoint::parse_id(stream_id).unwrap_or((0, 0));
        let compatible = |s: &StartRef, sid: Option<&str>| -> bool {
            match (&s.session_id, sid) {
                (None, None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        };
        let idx = self.pool.get(&key).and_then(|v| {
            v.iter().position(|s| {
                (match crate::checkpoint::parse_id(&s.stream_id) {
                    Some((ms, seq)) => (ms, seq) < (start_id, start_seq),
                    None => false,
                }) && compatible(s, session_id.as_deref())
            })
        });

        match idx {
            Some(i) => {
                let taken = self.pool.get_mut(&key).unwrap().remove(i);
                if self.pool.get(&key).map(|v| v.is_empty()).unwrap_or(false) {
                    self.pool.remove(&key);
                }
                let placement = taken.placement.clone();
                let mut row = self
                    .store
                    .get(&placement, &taken.pk)
                    .cloned()
                    .ok_or_else(|| Error::Fatal("pairing pool/table desync".into()))?;
                let start_ms = event_ms(
                    row.start_stream_id.as_deref().unwrap_or("0-0"),
                    row.started_at.as_deref(),
                );
                let finish_ms = event_ms(&finish_id, d.ts.as_deref());
                row.finish_stream_id = Some(finish_id);
                row.finished_at = d.ts.clone();
                row.duration_ms = Some(finish_ms.saturating_sub(start_ms));
                row.state = STATE_COMPLETED.to_string();
                row.snippet_out = snippet_of(d);
                row.handoff = handoff_of(d);
                row.issues = union_refs(row.issues.as_ref(), ref_of(d, "issues").as_ref());
                row.prs = union_refs(row.prs.as_ref(), ref_of(d, "prs").as_ref());
                row.linear = union_refs(row.linear.as_ref(), ref_of(d, "linear").as_ref());
                row.project = row.project.clone().or_else(|| d.project.clone());
                self.store.upsert(placement, row);
            }
            None => {
                // No compatible unmatched start → orphan_finish (§5.3), placed
                // on its own finish timestamp/team, kept (it is signal).
                let (dt, _ms) = dt_and_ms(stream_id, d.ts.as_deref());
                let folder = team_folder(d.team.as_deref(), d.actor.as_deref());
                let pk = session_pk(d.team.as_deref(), d.actor.as_deref(), stream_id);
                let row = SessionRow {
                    session_pk: pk,
                    team: d.team.clone(),
                    actor: d.actor.clone(),
                    session_id: session_id.clone(),
                    start_stream_id: None,
                    finish_stream_id: Some(finish_id),
                    started_at: None,
                    finished_at: d.ts.clone(),
                    duration_ms: None,
                    state: STATE_ORPHAN_FINISH.to_string(),
                    snippet_in: None,
                    snippet_out: snippet_of(d),
                    issues: ref_of(d, "issues"),
                    prs: ref_of(d, "prs"),
                    linear: ref_of(d, "linear"),
                    handoff: handoff_of(d),
                    project: d.project.clone(),
                };
                self.store.upsert((folder, dt), row);
            }
        }
        Ok(())
    }

    /// Expiry (§5.3): evaluated against **wall clock** elapsed since
    /// `started_at`. Any unmatched start (`open` or `interrupted`) older than
    /// the window becomes `expired` — terminal: removed from the pool, a later
    /// finish is recorded as `orphan_finish`.
    pub fn apply_expiry(&mut self, now_ms: i64, window_ms: i64) {
        if window_ms <= 0 {
            return;
        }
        let mut expired: Vec<(String, Placement, String)> = Vec::new(); // (bucket key, placement, pk)
        let keys: Vec<(String, String)> = self.pool.keys().cloned().collect();
        for key in keys {
            let mut keep = Vec::new();
            if let Some(starts) = self.pool.get(&key) {
                for s in starts {
                    let row = self.store.get(&s.placement, &s.pk);
                    let started_ms = row
                        .map(|r| {
                            event_ms(
                                r.start_stream_id.as_deref().unwrap_or("0-0"),
                                r.started_at.as_deref(),
                            )
                        })
                        .unwrap_or(0);
                    if now_ms.saturating_sub(started_ms) > window_ms {
                        expired.push((
                            format!("{}|{}", key.0, key.1),
                            s.placement.clone(),
                            s.pk.clone(),
                        ));
                    } else {
                        keep.push(s.clone());
                    }
                }
            }
            if keep.is_empty() {
                self.pool.remove(&key);
            } else {
                self.pool.insert(key, keep);
            }
        }
        for (_key, placement, pk) in expired {
            if let Some(row) = self.store.get(&placement, &pk) {
                if row.state == STATE_OPEN || row.state == STATE_INTERRUPTED {
                    let mut updated = row.clone();
                    updated.state = STATE_EXPIRED.to_string();
                    self.store.upsert(placement, updated);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn decoded(action: &str, team: &str, actor: &str, session_id: &str, snippet: &str) -> Decoded {
        let mut flat = HashMap::new();
        flat.insert("action".into(), action.to_string());
        flat.insert("team".into(), team.to_string());
        flat.insert("actor".into(), actor.to_string());
        if !session_id.is_empty() {
            flat.insert("session_id".into(), session_id.to_string());
        }
        if !snippet.is_empty() {
            flat.insert("snippet".into(), snippet.to_string());
        }
        flat.insert("timestamp".into(), "2026-08-30T10:00:00Z".to_string());
        crate::decoder::decode("0-0", &flat)
    }

    #[test]
    fn complete_pair_fifo_order() {
        let mut p = Pairer::new();
        // two turns, same actor, same session_id — must NOT collapse
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s1", "in-1"),
            "1725062400000-0",
        )
        .unwrap();
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "s1", "out-1"),
            "1725062400000-1",
        )
        .unwrap();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s1", "in-2"),
            "1725062400000-2",
        )
        .unwrap();
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "s1", "out-2"),
            "1725062400000-3",
        )
        .unwrap();

        let rows = p.store().all();
        assert_eq!(rows.len(), 2, "consecutive turns stay separate");
        assert!(rows.iter().all(|r| r.state == STATE_COMPLETED));
        let pks: Vec<&str> = rows.iter().map(|r| r.session_pk.as_str()).collect();
        assert_ne!(pks[0], pks[1]);
        // store.all() is sorted by session_pk, not creation order — assert the
        // (in, out) snippet pairs as a set.
        let pairs: Vec<(Option<&str>, Option<&str>)> = rows
            .iter()
            .map(|r| (r.snippet_in.as_deref(), r.snippet_out.as_deref()))
            .collect();
        assert!(pairs.contains(&(Some("in-1"), Some("out-1"))), "{pairs:?}");
        assert!(pairs.contains(&(Some("in-2"), Some("out-2"))), "{pairs:?}");
        assert!(rows.iter().all(|r| r.duration_ms == Some(0))); // same timestamps → 0
    }

    #[test]
    fn missing_session_id_equals_empty() {
        let mut p = Pairer::new();
        let s = decoded("task.started", "dev-1", "dev", "", "in");
        p.on_event(&s, "1725062400000-0").unwrap();
        let mut f = decoded("task.finished", "dev-1", "dev", "missing?", "out");
        // set no session_id on finish → None ≡ empty
        f.payload = serde_json::json!({});
        p.on_event(&f, "1725062400000-1").unwrap();
        let rows = p.store().all();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, STATE_COMPLETED);
        let _ = s;
    }

    #[test]
    fn incompatible_session_ids_do_not_pair() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "aaa", "in"),
            "1725062400000-0",
        )
        .unwrap();
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "bbb", "out"),
            "1725062400000-1",
        )
        .unwrap();
        let rows = p.store().all();
        assert_eq!(rows.len(), 2);
        let states: Vec<&str> = rows.iter().map(|r| r.state.as_str()).collect();
        assert!(states.contains(&STATE_ORPHAN_FINISH));
        assert!(states.contains(&STATE_OPEN));
    }

    #[test]
    fn finish_older_than_start_is_orphan() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s", ""),
            "1725062400000-5",
        )
        .unwrap();
        // finish with a stream id *before* the start cannot close it
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "s", ""),
            "1725062400000-3",
        )
        .unwrap();
        let rows = p.store().all();
        let states: Vec<&str> = rows.iter().map(|r| r.state.as_str()).collect();
        assert!(
            states.contains(&STATE_ORPHAN_FINISH),
            "structural no-negative-duration"
        );
        assert!(states.contains(&STATE_OPEN));
    }

    #[test]
    fn new_start_interrupts_previous_then_finish_flips_to_completed() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s1", "in-1"),
            "1725062400000-0",
        )
        .unwrap();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s1", "in-2"),
            "1725062400000-1",
        )
        .unwrap();
        let rows = p.store().all();
        assert_eq!(rows.len(), 2);
        let interrupted: Vec<&str> = rows
            .iter()
            .filter(|r| r.state == STATE_INTERRUPTED)
            .map(|r| r.session_pk.as_str())
            .collect();
        assert_eq!(interrupted.len(), 1);

        // finish pairs with the OLDEST compatible start (the interrupted one)
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "s1", "out-1"),
            "1725062400000-2",
        )
        .unwrap();
        let rows = p.store().all();
        let completed: Vec<&SessionRow> =
            rows.iter().filter(|r| r.state == STATE_COMPLETED).collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].snippet_in.as_deref(), Some("in-1"));
        assert_eq!(completed[0].snippet_out.as_deref(), Some("out-1"));
        // second start still open
        let open: Vec<&SessionRow> = rows.iter().filter(|r| r.state == STATE_OPEN).collect();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].snippet_in.as_deref(), Some("in-2"));
    }

    #[test]
    fn expiry_is_terminal_and_wall_clock_based() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s", "in"),
            "1725062400000-0",
        )
        .unwrap();
        // now = start + 10h → older than a 6h window
        let start_ms = event_ms("1725062400000-0", Some("2026-08-30T10:00:00Z"));
        let now_ms = start_ms + 10 * 3600 * 1000;
        p.apply_expiry(now_ms, 6 * 3600 * 1000);
        let rows = p.store().all();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, STATE_EXPIRED);

        // a finish arriving after expiry is orphan_finish, never resurrected
        p.on_event(
            &decoded("task.finished", "dev-1", "dev", "s", "out"),
            "1725062400000-1",
        )
        .unwrap();
        let rows = p.store().all();
        assert_eq!(rows.len(), 2);
        let states: Vec<&str> = rows.iter().map(|r| r.state.as_str()).collect();
        assert!(states.contains(&STATE_EXPIRED));
        assert!(states.contains(&STATE_ORPHAN_FINISH));
    }

    #[test]
    fn within_window_no_expiry() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s", "in"),
            "1725062400000-0",
        )
        .unwrap();
        let start_ms = event_ms("1725062400000-0", Some("2026-08-30T10:00:00Z"));
        p.apply_expiry(start_ms + 3600 * 1000, 6 * 3600 * 1000);
        let rows = p.store().all();
        assert_eq!(rows[0].state, STATE_OPEN);
    }

    #[test]
    fn session_pk_is_stable_hash() {
        let mut p = Pairer::new();
        p.on_event(
            &decoded("task.started", "dev-1", "dev", "s", ""),
            "1725062400000-0",
        )
        .unwrap();
        let rows = p.store().all();
        assert_eq!(
            rows[0].session_pk,
            session_pk(Some("dev-1"), Some("dev"), "1725062400000-0")
        );
        assert_eq!(rows[0].session_pk.len(), 64);
    }
}
