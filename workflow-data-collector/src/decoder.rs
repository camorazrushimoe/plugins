//! Wire decoder (§4.1).
//!
//! A stream entry is a flat list of string fields. The v1 decoder:
//! 1. Reads all string fields.
//! 2. If `json`/`envelope` is present **and** a valid JSON object → it is the
//!    envelope; `payload` comes only from that object's `payload` key.
//!    Envelope fields missing (or `""`) in the JSON object are overlaid from
//!    the flat map (`""` ≡ missing; the JSON envelope stays authoritative).
//! 3. Otherwise, if `payload` is a valid JSON object string → that is the
//!    payload. If not, the known payload keys at the top level
//!    (`session_id`, `snippet`, `summary`, `task_ref`, `handoff`) are the
//!    payload.
//! 4. `task_ref`/`handoff` may be JSON strings → parsed when valid, kept
//!    as-is otherwise (never dropped, never null).
//! 5. Decode failures are never silent: `json`/`envelope` present but invalid
//!    → line keeps all flat fields, `decode_ok: false`, failure logged.

use std::collections::HashMap;

use serde_json::{Map, Value};

/// Known payload keys (§4.2).
pub const KNOWN_PAYLOAD_KEYS: &[&str] =
    &["session_id", "snippet", "summary", "task_ref", "handoff"];

/// Decoded view of one stream entry.
#[derive(Debug, Clone)]
pub struct Decoded {
    pub envelope_id: Option<String>,
    /// Resolved `timestamp` string (envelope, overlaid from flat map).
    pub ts: Option<String>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub target: Option<String>,
    pub team: Option<String>,
    pub project: Option<Value>,
    /// Decoded payload object.
    pub payload: Value,
    /// Raw flat map from Redis (all string fields).
    pub fields: Map<String, Value>,
    pub decode_ok: bool,
    /// Set when `decode_ok` is false: stream_id + reason for the log.
    pub warning: Option<String>,
}

fn is_blank(s: &str) -> bool {
    s.is_empty()
}

/// Convert a raw Redis wire value (stream field) into its string form.
pub fn field_to_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::Int(i) => i.to_string(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Okay => "OK".to_string(),
        redis::Value::Nil => String::new(),
        other => format!("{other:?}"),
    }
}

/// Parse a JSON-string value; return the parsed value, else the plain string.
fn json_or_plain(v: &str) -> Value {
    serde_json::from_str::<Value>(v).unwrap_or_else(|_| Value::String(v.to_string()))
}

/// Apply §4.1 step 4 to a payload object: `task_ref`/`handoff` JSON strings
/// are parsed when valid, kept as plain strings otherwise.
fn normalize_refs(payload: &mut Value) {
    if let Value::Object(map) = payload {
        for key in ["task_ref", "handoff"] {
            if let Some(Value::String(s)) = map.get(key) {
                if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                    map.insert(key.to_string(), parsed);
                }
            }
        }
    }
}

/// Decode one stream entry per §4.1.
pub fn decode(stream_id: &str, flat: &HashMap<String, String>) -> Decoded {
    let fields: Map<String, Value> = flat
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();

    let flat_get = |k: &str| flat.get(k).map(|s| s.as_str());

    // ---- does a json/envelope field exist? ----
    let env_candidate = flat
        .get("json")
        .or_else(|| flat.get("envelope"))
        .map(|s| s.as_str());

    let parsed_env = env_candidate.and_then(|s| serde_json::from_str::<Value>(s).ok());
    let env_is_object = matches!(&parsed_env, Some(Value::Object(_)));

    match (env_candidate, env_is_object) {
        // Step 2: json/envelope present and a valid JSON object → authoritative.
        (Some(_), true) => {
            let obj = parsed_env.unwrap().as_object().unwrap().clone();

            // Overlay: envelope fields missing or "" in the JSON object come
            // from the flat map.
            let overlay = |key: &str| -> Option<String> {
                match obj.get(key) {
                    Some(Value::String(s)) if !is_blank(s) => Some(s.clone()),
                    Some(Value::String(_)) => flat_get(key).map(|s| s.to_string()),
                    Some(v) => Some(v.to_string()),
                    None => flat_get(key).map(|s| s.to_string()),
                }
            };

            let envelope_id = overlay("id");
            let ts = overlay("timestamp");
            let actor = overlay("actor");
            let action = overlay("action");
            let target = overlay("target");
            let team = overlay("team");
            let project = obj
                .get("project")
                .cloned()
                .filter(|v| !(v.is_string() && is_blank(v.as_str().unwrap())))
                .or_else(|| flat_get("project").map(|s| Value::String(s.to_string())));

            let mut payload = match obj.get("payload") {
                Some(Value::Object(_)) | Some(Value::String(_)) | Some(_) => obj
                    .get("payload")
                    .cloned()
                    .unwrap_or(Value::Object(Map::new())),
                None => Value::Object(Map::new()),
            };
            normalize_refs(&mut payload);

            Decoded {
                envelope_id,
                ts,
                actor,
                action,
                target,
                team,
                project,
                payload,
                fields,
                decode_ok: true,
                warning: None,
            }
        }
        // Step 5: json/envelope present but invalid → decode_ok: false,
        // never silent. All flat fields are kept; payload is still best-effort
        // parsed so the event is preserved for Lab.
        (Some(raw), false) => {
            let mut payload =
                match flat_get("payload").and_then(|s| serde_json::from_str::<Value>(s).ok()) {
                    Some(Value::Object(_)) => flat_get("payload")
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or_else(|| Value::Object(Map::new())),
                    _ => {
                        let mut m = Map::new();
                        for k in KNOWN_PAYLOAD_KEYS {
                            if let Some(v) = flat.get(*k) {
                                m.insert(k.to_string(), json_or_plain(v));
                            }
                        }
                        Value::Object(m)
                    }
                };
            normalize_refs(&mut payload);

            Decoded {
                envelope_id: flat_get("id").map(|s| s.to_string()),
                ts: flat_get("timestamp").map(|s| s.to_string()),
                actor: flat_get("actor").map(|s| s.to_string()),
                action: flat_get("action").map(|s| s.to_string()),
                target: flat_get("target").map(|s| s.to_string()),
                team: flat_get("team").map(|s| s.to_string()),
                project: flat_get("project").map(|s| Value::String(s.to_string())),
                payload,
                fields,
                decode_ok: false,
                warning: Some(format!(
                    "stream_id {stream_id}: json/envelope present but not a valid JSON object (got {raw:?})"
                )),
            }
        }
        // Step 3: no json/envelope object.
        (None, _) => {
            let payload =
                match flat_get("payload").and_then(|s| serde_json::from_str::<Value>(s).ok()) {
                    Some(Value::Object(_)) => flat_get("payload")
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or_else(|| Value::Object(Map::new())),
                    _ => {
                        let mut m = Map::new();
                        for k in KNOWN_PAYLOAD_KEYS {
                            if let Some(v) = flat.get(*k) {
                                m.insert(k.to_string(), json_or_plain(v));
                            }
                        }
                        Value::Object(m)
                    }
                };

            Decoded {
                envelope_id: flat_get("id").map(|s| s.to_string()),
                ts: flat_get("timestamp").map(|s| s.to_string()),
                actor: flat_get("actor").map(|s| s.to_string()),
                action: flat_get("action").map(|s| s.to_string()),
                target: flat_get("target").map(|s| s.to_string()),
                team: flat_get("team").map(|s| s.to_string()),
                project: flat_get("project").map(|s| Value::String(s.to_string())),
                payload,
                fields,
                decode_ok: true,
                warning: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn fully_flattened_event_uses_known_keys_as_payload() {
        let d = decode(
            "1725062400000-0",
            &flat(&[
                ("action", "task.started"),
                ("actor", "developer"),
                ("team", "dev-1"),
                ("session_id", "s1"),
                ("snippet", "hello"),
            ]),
        );
        assert!(d.decode_ok);
        assert_eq!(d.actor.as_deref(), Some("developer"));
        assert_eq!(d.action.as_deref(), Some("task.started"));
        assert_eq!(d.payload["session_id"], "s1");
        assert_eq!(d.payload["snippet"], "hello");
        assert_eq!(d.fields["team"], "dev-1");
        assert_eq!(d.envelope_id, None);
    }

    #[test]
    fn json_envelope_is_authoritative() {
        let env = r#"{"id":"env-1","actor":"architect","action":"task.started","timestamp":"2026-08-30T21:00:00Z","payload":{"session_id":"s9","snippet":"from-envelope"}}"#;
        let d = decode(
            "1725062400000-1",
            &flat(&[
                ("json", env),
                ("actor", "flat-actor"), // overlay: JSON wins because present
                ("session_id", "flat-sid"),
            ]),
        );
        assert!(d.decode_ok);
        assert_eq!(d.envelope_id.as_deref(), Some("env-1"));
        assert_eq!(d.actor.as_deref(), Some("architect"));
        assert_eq!(d.payload["session_id"], "s9");
        // flat known keys are kept under fields, never used to rebuild payload
        assert_eq!(d.fields["session_id"], "flat-sid");
        assert_eq!(d.payload.get("flat-sid"), None);
    }

    #[test]
    fn empty_string_in_json_envelope_is_overlaid_from_flat_map() {
        let env = r#"{"id":"env-2","actor":"","action":"task.finished","timestamp":""}"#;
        let d = decode(
            "1725062400000-2",
            &flat(&[
                ("envelope", env),
                ("actor", "flat-actor"),
                ("timestamp", "2026-08-30T22:00:00Z"),
            ]),
        );
        assert!(d.decode_ok);
        assert_eq!(
            d.actor.as_deref(),
            Some("flat-actor"),
            "\"\" counts as missing"
        );
        assert_eq!(d.ts.as_deref(), Some("2026-08-30T22:00:00Z"));
    }

    #[test]
    fn payload_field_json_object_wins() {
        let d = decode(
            "1725062400000-3",
            &flat(&[
                ("action", "task.started"),
                ("actor", "dev"),
                ("payload", r#"{"session_id":"s1","snippet":"x"}"#),
                ("session_id", "flat-sid"),
            ]),
        );
        assert!(d.decode_ok);
        assert_eq!(d.payload["session_id"], "s1");
        assert_eq!(d.fields["session_id"], "flat-sid");
    }

    #[test]
    fn task_ref_json_string_is_parsed_plain_string_kept() {
        let d = decode(
            "1725062400000-4",
            &flat(&[
                ("action", "task.finished"),
                ("actor", "dev"),
                ("task_ref", r##"{"issues":["BON-1"],"prs":["#7"]}"##),
            ]),
        );
        assert!(d.decode_ok);
        assert_eq!(d.payload["task_ref"]["issues"][0], "BON-1");
        assert_eq!(d.payload["task_ref"]["prs"][0], "#7");

        let d2 = decode(
            "1725062400000-5",
            &flat(&[
                ("action", "task.finished"),
                ("actor", "dev"),
                ("task_ref", "plain-string"),
            ]),
        );
        assert!(d2.decode_ok);
        assert_eq!(d2.payload["task_ref"], "plain-string");
    }

    #[test]
    fn invalid_json_envelope_is_never_silent() {
        let d = decode(
            "1725062400000-6",
            &flat(&[
                ("json", "{not json"),
                ("actor", "dev"),
                ("action", "task.started"),
            ]),
        );
        assert!(!d.decode_ok);
        assert!(d.warning.is_some());
        assert_eq!(d.actor.as_deref(), Some("dev"));
        assert_eq!(d.fields["json"], "{not json");
        // payload best-effort from known flat keys
        assert!(d.payload.is_object());
    }

    #[test]
    fn valid_json_but_not_object_counts_as_invalid_envelope() {
        let d = decode("1725062400000-7", &flat(&[("json", "\"just a string\"")]));
        assert!(!d.decode_ok);
        assert!(d.warning.is_some());
    }

    #[test]
    fn envelope_id_from_flat_id_when_no_json() {
        let d = decode(
            "1725062400000-8",
            &flat(&[
                ("id", "flat-env"),
                ("action", "task.started"),
                ("actor", "dev"),
            ]),
        );
        assert_eq!(d.envelope_id.as_deref(), Some("flat-env"));
    }
}
