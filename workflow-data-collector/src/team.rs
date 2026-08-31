//! Team folder name (§5.1): `team` from the bus is not a safe path.

/// Actors that map to `_office` when `team` is empty (§5.1.1).
const OFFICE_ACTORS: &[&str] = &[
    "architect",
    "staff-engineer",
    "scrum-master",
    "super-devops",
    "lifecycle",
    "system",
];

/// Compute the safe team folder name per §5.1.
pub fn team_folder(team: Option<&str>, actor: Option<&str>) -> String {
    let team = team.unwrap_or("").trim();
    let actor = actor.unwrap_or("").trim();

    let base = if team.is_empty() {
        if OFFICE_ACTORS.contains(&actor) {
            "_office".to_string()
        } else {
            "_unknown".to_string()
        }
    } else {
        team.to_string()
    };

    // Sanitize: keep [A-Za-z0-9._-], collapse anything else to '_',
    // strip leading dots, cap at 64 chars. Empty after that → "_unknown".
    let mut out = String::with_capacity(base.len());
    for ch in base.chars() {
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    while out.starts_with('.') {
        out.remove(0);
    }
    if out.len() > 64 {
        out.truncate(64);
    }
    if out.is_empty() {
        "_unknown".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_team_office_actors() {
        for a in OFFICE_ACTORS {
            assert_eq!(team_folder(None, Some(a)), "_office");
            assert_eq!(team_folder(Some(""), Some(a)), "_office");
        }
    }

    #[test]
    fn empty_team_unknown_otherwise() {
        assert_eq!(team_folder(None, Some("developer")), "_unknown");
        assert_eq!(team_folder(None, None), "_unknown");
    }

    #[test]
    fn sanitize_collapses_to_underscore() {
        assert_eq!(team_folder(Some("dev 1"), None), "dev_1");
        assert_eq!(team_folder(Some("a/b\\c"), None), "a_b_c");
        assert_eq!(team_folder(Some("team.name-x_y"), None), "team.name-x_y");
    }

    #[test]
    fn leading_dots_stripped_and_cap_64() {
        assert_eq!(team_folder(Some("..secret"), None), "secret");
        let long = "x".repeat(100);
        let f = team_folder(Some(&long), None);
        assert_eq!(f.len(), 64);
    }

    #[test]
    fn all_underscores_become_unknown() {
        // Per §5.1 the sanitized string is kept when non-empty ("///" → "___").
        assert_eq!(team_folder(Some("///"), None), "___");
        // A genuinely empty result still maps to _unknown (e.g. leading dots
        // stripped from an otherwise-dot-only team).
        assert_eq!(team_folder(Some("..."), None), "_unknown");
    }

    #[test]
    fn original_team_string_is_preserved_on_row_not_here() {
        // The JSONL row keeps the original team string; the folder is derived.
        assert_eq!(team_folder(Some("Dev Team!"), None), "Dev_Team_");
    }
}
