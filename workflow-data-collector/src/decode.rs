//! Wire decoder (§4.1) → raw JSONL line shape (§5.2).
//!
//! A stream entry is a flat list of string fields. The decoder builds the raw
//! line: envelope fields (`envelope_id`, `ts`, `actor`, `action`, `target`,
//! `team`, `project`), a decoded `payload`, the raw flat map under `fields`,
//! and `decode_ok`.

use std::collections::BTreeMap;

use serde::Serialize;

/// One raw JSONL line (§5.2). Serialized with stable key order.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RawLine {
    pub stream_id: String,
    pub envelope_id: Option<String>,
    pub ts: Option<String>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub target: Option<String>,
    pub team: Option<String>,
    pub project: Option<String>,
    pub payload: serde_json::Value,
    pub fields: BTreeMap<String, String>,
    pub decode_ok: bool,
}

/// Result of decoding one stream entry.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub line: RawLine,
    /// Reason for `decode_ok: false` (§4.1 step 5); the failure is never
    /// silent — the caller logs it with the stream id.
    pub decode_error: Option<String>,
}

/// Known payload keys (§4.2) — used when the payload has to be rebuilt from
/// flat top-level fields.
pub const KNOWN_PAYLOAD_KEYS: [&str; 5] =
    ["session_id", "snippet", "summary", "task_ref", "handoff"];

/// Decode a stream entry into a raw line (§4.1 + §5.2).
pub fn decode(stream_id: &str, flat: &BTreeMap<String, String>) -> Decoded {
    // Non-empty flat value for `key` (empty string ≡ missing, §4.1).
    let flat_non_empty =
        |key: &str| -> Option<String> { flat.get(key).filter(|v| !v.is_empty()).cloned() };

    // §4.1 step 2: `json` (preferred) or `envelope` must be a valid JSON
    // object to be the envelope; anything else present-but-invalid flips
    // decode_ok to false (§4.1 step 5) but the event is still written.
    let json_candidate = flat.get("json").or_else(|| flat.get("envelope"));
    let mut decode_ok = true;
    let mut decode_error: Option<String> = None;
    let envelope: Option<serde_json::Map<String, serde_json::Value>> = match json_candidate {
        Some(raw) => match serde_json::from_str::<serde_json::Value>(raw) {
            Ok(serde_json::Value::Object(map)) => Some(map),
            Ok(_) => {
                decode_ok = false;
                decode_error = Some("json/envelope is not a JSON object".to_string());
                None
            }
            Err(e) => {
                decode_ok = false;
                decode_error = Some(format!("json/envelope is invalid JSON: {e}"));
                None
            }
        },
        None => None,
    };

    // Envelope field value: authoritative when a non-empty string in the JSON
    // object, otherwise overlaid from the flat map ("" counts as missing).
    let overlay = |env: &serde_json::Map<String, serde_json::Value>, key: &str| -> Option<String> {
        match env.get(key) {
            Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => flat_non_empty(key),
        }
    };

    let (envelope_id, ts, actor, action, target, team, project, payload) = match &envelope {
        Some(env) => {
            let payload = env
                .get("payload")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            (
                overlay(env, "id"),
                overlay(env, "timestamp"),
                overlay(env, "actor"),
                overlay(env, "action"),
                overlay(env, "target"),
                overlay(env, "team"),
                overlay(env, "project"),
                payload,
            )
        }
        None => {
            // §4.1 step 3: no valid envelope — payload comes from the flat
            // `payload` field when it is a valid JSON object, else from the
            // known top-level payload keys.
            let payload = match flat.get("payload") {
                Some(raw) => match serde_json::from_str::<serde_json::Value>(raw) {
                    Ok(serde_json::Value::Object(_)) => serde_json::from_str(raw).unwrap(),
                    _ => payload_from_known_keys(flat),
                },
                None => payload_from_known_keys(flat),
            };
            (
                flat_non_empty("id"),
                flat_non_empty("timestamp"),
                flat_non_empty("actor"),
                flat_non_empty("action"),
                flat_non_empty("target"),
                flat_non_empty("team"),
                flat_non_empty("project"),
                payload,
            )
        }
    };

    Decoded {
        line: RawLine {
            stream_id: stream_id.to_string(),
            envelope_id,
            ts,
            actor,
            action,
            target,
            team,
            project,
            payload,
            fields: flat.clone(),
            decode_ok,
        },
        decode_error,
    }
}

/// §4.1 step 3: build the payload object from the known payload keys sitting
/// at the top level of the flat map. `task_ref` / `handoff` may be JSON
/// strings — parsed when valid, kept as plain strings otherwise (§4.1 step 4).
fn payload_from_known_keys(flat: &BTreeMap<String, String>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for key in KNOWN_PAYLOAD_KEYS {
        if let Some(v) = flat.get(key).filter(|v| !v.is_empty()) {
            let parsed = if key == "task_ref" || key == "handoff" {
                serde_json::from_str::<serde_json::Value>(v)
                    .ok()
                    .unwrap_or_else(|| serde_json::Value::String(v.clone()))
            } else {
                serde_json::Value::String(v.clone())
            };
            obj.insert(key.to_string(), parsed);
        }
    }
    serde_json::Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn flat(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn line(dec: &Decoded) -> &RawLine {
        &dec.line
    }

    /// §5.2 example shape: flat-only event with no payload keys.
    #[test]
    fn flat_only_event_matches_spec_line_shape() {
        let f = flat(&[
            ("action", "task.started"),
            ("actor", "developer"),
            ("target", "developer"),
            ("team", "dev-1"),
            ("timestamp", "2026-08-30T21:00:00Z"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(d.decode_error.is_none());
        assert!(line(&d).decode_ok);
        assert_eq!(line(&d).envelope_id, None);
        assert_eq!(line(&d).ts.as_deref(), Some("2026-08-30T21:00:00Z"));
        assert_eq!(line(&d).actor.as_deref(), Some("developer"));
        assert_eq!(line(&d).action.as_deref(), Some("task.started"));
        assert_eq!(line(&d).target.as_deref(), Some("developer"));
        assert_eq!(line(&d).team.as_deref(), Some("dev-1"));
        assert_eq!(line(&d).project, None);
        assert_eq!(line(&d).payload, json!({}));
        assert_eq!(line(&d).fields, f);
        // Exact JSON, §5.2 key order (fields sorted for determinism).
        let s = serde_json::to_string(line(&d)).unwrap();
        assert_eq!(
            s,
            r#"{"stream_id":"1725062400000-0","envelope_id":null,"ts":"2026-08-30T21:00:00Z","actor":"developer","action":"task.started","target":"developer","team":"dev-1","project":null,"payload":{},"fields":{"action":"task.started","actor":"developer","target":"developer","team":"dev-1","timestamp":"2026-08-30T21:00:00Z"},"decode_ok":true}"#
        );
    }

    /// §4.1 step 2: `json` object is authoritative for the envelope; flat
    /// known payload keys stay under `fields` and never rebuild the payload.
    #[test]
    fn json_envelope_is_authoritative() {
        let json_env = json!({
            "id": "env-1",
            "actor": "dev",
            "action": "task.started",
            "target": "dev",
            "timestamp": "2026-08-30T21:00:00Z",
            "team": "dev-1",
            "project": "p1",
            "payload": {"session_id": "s1", "snippet": "x"}
        });
        let f = flat(&[
            ("json", &json_env.to_string()),
            ("actor", "flat-actor"), // overridden by envelope
            ("session_id", "flat-sid"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(d.decode_error.is_none());
        assert!(line(&d).decode_ok);
        assert_eq!(line(&d).envelope_id.as_deref(), Some("env-1"));
        assert_eq!(line(&d).actor.as_deref(), Some("dev"));
        assert_eq!(line(&d).action.as_deref(), Some("task.started"));
        assert_eq!(line(&d).team.as_deref(), Some("dev-1"));
        assert_eq!(line(&d).project.as_deref(), Some("p1"));
        assert_eq!(
            line(&d).payload,
            json!({"session_id": "s1", "snippet": "x"})
        );
        // raw flat map kept as-is, including the json string itself
        assert_eq!(
            line(&d).fields.get("actor").map(String::as_str),
            Some("flat-actor")
        );
        assert_eq!(
            line(&d).fields.get("session_id").map(String::as_str),
            Some("flat-sid")
        );
        assert_eq!(
            line(&d).fields.get("json").map(String::as_str),
            Some(json_env.to_string().as_str())
        );
    }

    /// §4.1 step 2: envelope fields missing from the JSON object are overlaid
    /// from the flat map.
    #[test]
    fn json_envelope_overlays_missing_fields_from_flat() {
        let f = flat(&[
            ("json", r#"{"id":"env-1","action":"task.started"}"#),
            ("actor", "flatdev"),
            ("timestamp", "2026-08-30T21:00:00Z"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(line(&d).decode_ok);
        assert_eq!(line(&d).envelope_id.as_deref(), Some("env-1"));
        assert_eq!(line(&d).action.as_deref(), Some("task.started"));
        assert_eq!(line(&d).actor.as_deref(), Some("flatdev"));
        assert_eq!(line(&d).ts.as_deref(), Some("2026-08-30T21:00:00Z"));
        assert_eq!(line(&d).project, None);
    }

    /// §4.1 step 2 (v0.3.0 pin): an empty string in the JSON object counts as
    /// missing for the overlay — the flat value wins.
    #[test]
    fn empty_string_in_json_counts_as_missing_for_overlay() {
        let f = flat(&[
            (
                "json",
                r#"{"id":"env-1","actor":"","action":"task.started","target":""}"#,
            ),
            ("actor", "realactor"),
            ("target", ""),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(line(&d).decode_ok);
        assert_eq!(line(&d).actor.as_deref(), Some("realactor"));
        // target empty in both json and flat → null
        assert_eq!(line(&d).target, None);
    }

    /// §4.1 step 5: `json` present but invalid → decode_ok false, reason
    /// logged, all flat fields kept, payload falls back to known flat keys.
    #[test]
    fn invalid_json_sets_decode_ok_false_and_keeps_event() {
        let f = flat(&[
            ("json", "{not json"),
            ("action", "task.started"),
            ("actor", "dev"),
            ("session_id", "s1"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(!line(&d).decode_ok);
        assert!(d.decode_error.is_some(), "failure reason must be present");
        assert_eq!(line(&d).fields, f);
        assert_eq!(line(&d).payload, json!({"session_id": "s1"}));
    }

    /// §4.1 step 5: `json` valid JSON but not an object → invalid envelope.
    #[test]
    fn json_non_object_is_invalid_envelope() {
        for bad in [r#""just a string""#, r#"[1,2,3]"#, "42"] {
            let f = flat(&[("json", bad), ("action", "task.started")]);
            let d = decode("1-0", &f);
            assert!(!line(&d).decode_ok, "json {bad} must fail decode");
            assert!(d.decode_error.is_some());
            assert_eq!(line(&d).action.as_deref(), Some("task.started"));
        }
    }

    /// §4.1 step 3: `payload` is a valid JSON object → it is the payload;
    /// flat known keys still only live under `fields`.
    #[test]
    fn payload_field_valid_json_object_is_used() {
        let f = flat(&[
            ("payload", r#"{"session_id":"s1","custom":1}"#),
            ("session_id", "flat-sid"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(
            line(&d).decode_ok,
            "payload failure is not a decode failure"
        );
        assert_eq!(line(&d).payload, json!({"session_id": "s1", "custom": 1}));
        assert_eq!(
            line(&d).fields.get("session_id").map(String::as_str),
            Some("flat-sid")
        );
    }

    /// §4.1 step 3: `payload` not a valid JSON object → known top-level keys
    /// become the payload; decode_ok stays true.
    #[test]
    fn payload_invalid_falls_back_to_known_top_level_keys() {
        let f = flat(&[
            ("payload", "not json"),
            ("session_id", "s1"),
            ("snippet", "hello"),
        ]);
        let d = decode("1725062400000-0", &f);
        assert!(line(&d).decode_ok);
        assert_eq!(
            line(&d).payload,
            json!({"session_id": "s1", "snippet": "hello"})
        );
    }

    /// §4.1 step 4: task_ref / handoff JSON strings are parsed when valid.
    #[test]
    fn task_ref_and_handoff_json_strings_are_parsed() {
        let f = flat(&[
            ("task_ref", r#"{"issues":[1,2],"prs":[3]}"#),
            ("handoff", r#"["a","b"]"#),
        ]);
        let d = decode("1-0", &f);
        assert_eq!(
            line(&d).payload,
            json!({"task_ref": {"issues": [1, 2], "prs": [3]}, "handoff": ["a", "b"]})
        );
    }

    /// §4.1 step 4: a non-JSON plain string for task_ref is kept as-is —
    /// never dropped, never replaced with null.
    #[test]
    fn task_ref_plain_string_kept_as_is() {
        let f = flat(&[("task_ref", "plain-ref")]);
        let d = decode("1-0", &f);
        assert_eq!(line(&d).payload, json!({"task_ref": "plain-ref"}));
    }

    /// Empty entry → all-null line with empty payload and empty fields.
    #[test]
    fn empty_entry_decodes_to_empty_line() {
        let f = flat(&[]);
        let d = decode("1-0", &f);
        assert!(line(&d).decode_ok);
        assert_eq!(line(&d).payload, json!({}));
        assert_eq!(line(&d).fields, f);
        assert_eq!(line(&d).envelope_id, None);
        assert_eq!(line(&d).actor, None);
        assert_eq!(line(&d).project, None);
    }

    /// No envelope but a flat `id` → envelope_id (§4.1 overlay list).
    #[test]
    fn flat_id_becomes_envelope_id_without_json() {
        let f = flat(&[("id", "env-1"), ("action", "task.started")]);
        let d = decode("1-0", &f);
        assert_eq!(line(&d).envelope_id.as_deref(), Some("env-1"));
    }

    /// No known payload keys at top level → payload stays {} (§5.2 example).
    #[test]
    fn no_known_payload_keys_gives_empty_object() {
        let f = flat(&[("action", "task.started"), ("actor", "dev")]);
        let d = decode("1-0", &f);
        assert_eq!(line(&d).payload, json!({}));
    }
}
