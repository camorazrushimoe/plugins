//! Team folder naming (§5.1).
//!
//! `team` from the bus is not a safe path. An empty team maps to `_office`
//! for known office actors, else `_unknown`; the string is sanitized to
//! `[A-Za-z0-9._-]` (runs of anything else collapse to `_`), leading dots are
//! stripped, the result is capped at 64 chars, and an empty result is
//! `_unknown`. The original `team` string is always kept on the JSONL row
//! (that is the decoder's job — see `decode::RawLine::team`).

/// Actors that map an empty team to `_office` (§5.1 rule 1).
pub const OFFICE_ACTORS: [&str; 6] = [
    "architect",
    "staff-engineer",
    "scrum-master",
    "super-devops",
    "lifecycle",
    "system",
];

const MAX_TEAM_LEN: usize = 64;

/// Safe folder name for a bus `team` value (§5.1).
pub fn team_safe(team: Option<&str>, actor: Option<&str>) -> String {
    let raw = team.unwrap_or("");
    if raw.is_empty() {
        return if actor.is_some_and(|a| OFFICE_ACTORS.contains(&a)) {
            "_office".to_string()
        } else {
            "_unknown".to_string()
        };
    }
    let sanitized = sanitize(raw);
    if sanitized.is_empty() {
        return "_unknown".to_string();
    }
    sanitized
}

/// Keep `[A-Za-z0-9._-]`, collapse runs of anything else to a single `_`,
/// strip leading dots, cap at 64 chars.
fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut pending_underscore = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            if pending_underscore {
                out.push('_');
                pending_underscore = false;
            }
            out.push(ch);
        } else {
            pending_underscore = true;
        }
        if out.len() >= MAX_TEAM_LEN {
            break;
        }
    }
    if pending_underscore {
        out.push('_');
    }
    let trimmed = out.trim_start_matches('.');
    trimmed.chars().take(MAX_TEAM_LEN).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_team_office_actors() {
        for actor in OFFICE_ACTORS {
            assert_eq!(team_safe(Some(""), Some(actor)), "_office", "actor {actor}");
        }
        assert_eq!(team_safe(None, Some("architect")), "_office");
    }

    #[test]
    fn empty_team_unknown_actor() {
        assert_eq!(team_safe(Some(""), Some("developer")), "_unknown");
        assert_eq!(team_safe(None, Some("developer")), "_unknown");
        assert_eq!(team_safe(None, None), "_unknown");
    }

    #[test]
    fn safe_team_passthrough() {
        assert_eq!(team_safe(Some("dev-1"), None), "dev-1");
        assert_eq!(team_safe(Some("a.b_c-d"), None), "a.b_c-d");
    }

    #[test]
    fn disallowed_chars_collapse_to_underscore() {
        assert_eq!(team_safe(Some("A/B C"), None), "A_B_C");
        assert_eq!(team_safe(Some("a//b"), None), "a_b");
        assert_eq!(team_safe(Some("dev:1"), None), "dev_1");
    }

    #[test]
    fn leading_dots_stripped() {
        assert_eq!(team_safe(Some("..team"), None), "team");
        assert_eq!(team_safe(Some(".. a"), None), "_a");
    }

    #[test]
    fn sanitized_empty_falls_back_to_unknown_even_for_office_actor() {
        // §5.1 rule 2: only the *initial* empty team uses the actor check.
        assert_eq!(team_safe(Some("..."), Some("architect")), "_unknown");
    }

    #[test]
    fn long_team_capped_at_64() {
        let long = "t".repeat(100);
        let out = team_safe(Some(&long), None);
        assert_eq!(out.len(), 64);
        assert!(out.chars().all(|c| c == 't'));
    }

    #[test]
    fn all_disallowed_becomes_single_underscore() {
        assert_eq!(team_safe(Some("///"), None), "_");
        assert_eq!(team_safe(Some(" "), None), "_");
    }
}
