// SPDX-License-Identifier: MIT
//! `Allowlist` — the parsed `permissions.allow` / `.deny` arrays from
//! `~/.claude/settings.json`.

use std::path::Path;

use anyhow::{Context, Result};
use tracing::warn;

use super::pattern::Pattern;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The parsed permission pattern lists from settings.json.
#[derive(Debug, Default, Clone)]
pub struct Allowlist {
    /// Patterns from `permissions.allow`.  Confident match → short-circuit allow.
    pub allow: Vec<Pattern>,
    /// Patterns from `permissions.deny`.  Checked before allow; confident or
    /// ambiguous match → deny / ask.
    pub deny: Vec<Pattern>,
}

impl Allowlist {
    /// An empty allowlist — no patterns on either side.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Parse from a `serde_json::Value` representing the root `settings.json`.
    ///
    /// Looks for `permissions.allow` and `permissions.deny` arrays.
    /// - Missing or non-array value for either key → empty `Vec` for that side.
    /// - Non-string element in an array → `warn!` and skip; never panic.
    pub fn from_settings_json(root: &serde_json::Value) -> Self {
        let allow = parse_pattern_array(root, "allow");
        let deny = parse_pattern_array(root, "deny");
        Self { allow, deny }
    }

    /// Merge three allowlists in cascade priority order: `local → project → user`.
    ///
    /// The resulting `deny` list is the concatenation of all three sources in
    /// that order; same for `allow`.  This means patterns from `local` appear
    /// first in the merged vector and are evaluated first.
    ///
    /// This works correctly with the existing `evaluate()` accumulator logic:
    /// - A confident deny anywhere in the merged vec wins (early return on
    ///   first `Confident` hit).
    /// - An ambiguous deny accumulates across all three sources; the *first*
    ///   ambiguous pattern encountered (i.e. the local one) becomes the
    ///   annotation in `AskAnnotated` — which is the most-specific override.
    ///
    /// No dedup is performed.  Redundant patterns are harmless (short-circuit
    /// on the first match) and dedup adds complexity with no practical benefit.
    pub fn cascade(local: Self, project: Self, user: Self) -> Self {
        Self {
            allow: local.allow.into_iter()
                .chain(project.allow)
                .chain(user.allow)
                .collect(),
            deny: local.deny.into_iter()
                .chain(project.deny)
                .chain(user.deny)
                .collect(),
        }
    }

    /// Load and parse from a file path.
    ///
    /// - File does not exist → `Ok(Allowlist::empty())` (first-time user).
    /// - File exists but JSON is malformed → `Err`.
    pub fn from_path(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::empty());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        let root: serde_json::Value = serde_json::from_str(&text)
            .with_context(|| format!("parse JSON in {}", path.display()))?;
        Ok(Self::from_settings_json(&root))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_pattern_array(root: &serde_json::Value, key: &str) -> Vec<Pattern> {
    let arr = root
        .get("permissions")
        .and_then(|p| p.get(key))
        .and_then(|v| v.as_array());

    let arr = match arr {
        None => return Vec::new(),
        Some(a) => a,
    };

    let mut patterns = Vec::with_capacity(arr.len());
    for (i, entry) in arr.iter().enumerate() {
        match entry.as_str() {
            Some(s) => patterns.push(Pattern::parse(s)),
            None => {
                warn!(
                    "settings.json: permissions.{}[{}] is not a string ({:?}); skipping",
                    key, i, entry
                );
            }
        }
    }
    patterns
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn empty_json_object_gives_empty_allowlist() {
        let root = json!({});
        let a = Allowlist::from_settings_json(&root);
        assert!(a.allow.is_empty());
        assert!(a.deny.is_empty());
    }

    #[test]
    fn missing_permissions_key_gives_empty_allowlist() {
        let root = json!({"theme": "dark", "tui": "fullscreen"});
        let a = Allowlist::from_settings_json(&root);
        assert!(a.allow.is_empty());
        assert!(a.deny.is_empty());
    }

    #[test]
    fn empty_allow_deny_arrays() {
        let root = json!({"permissions": {"allow": [], "deny": []}});
        let a = Allowlist::from_settings_json(&root);
        assert!(a.allow.is_empty());
        assert!(a.deny.is_empty());
    }

    #[test]
    fn allow_and_deny_both_populated() {
        let root = json!({
            "permissions": {
                "allow": ["Skill", "mcp__plugin_backlog_tasks__*"],
                "deny":  ["Read(**/.env*)", "Edit(**/*.pem)"]
            }
        });
        let a = Allowlist::from_settings_json(&root);
        assert_eq!(a.allow.len(), 2);
        assert_eq!(a.deny.len(), 2);
    }

    #[test]
    fn non_string_entries_skipped_no_panic() {
        let root = json!({
            "permissions": {
                "allow": ["Skill", 42, null, true, "Bash"],
                "deny":  []
            }
        });
        // Should skip 42, null, true and keep only "Skill" and "Bash".
        let a = Allowlist::from_settings_json(&root);
        assert_eq!(a.allow.len(), 2, "non-string entries must be skipped");
    }

    #[test]
    fn real_world_settings_shape() {
        // Real-world settings.json shape: allow=5, deny=14.
        let root = json!({
            "permissions": {
                "allow": [
                    "Skill",
                    "mcp__plugin_context7_context7__resolve-library-id",
                    "mcp__plugin_context7_context7__query-docs",
                    "mcp__plugin_backlog_tasks__*",
                    "Agent(task-planner)"
                ],
                "deny": [
                    "Read(**/.env*)",
                    "Read(**/*.pem)",
                    "Read(**/*.key)",
                    "Read(**/*.p12)",
                    "Read(**/*.pfx)",
                    "Read(**/*.cer)",
                    "Read(**/*.crt)",
                    "Read(**/.ssh/**)",
                    "Read(**/*secret*)",
                    "Read(**/*credential*)",
                    "Read(**/.aws/credentials)",
                    "Edit(**/.env*)",
                    "Edit(**/*.pem)",
                    "Edit(**/*.key)",
                    "Edit(**/.ssh/**)"
                ]
            }
        });
        let a = Allowlist::from_settings_json(&root);
        assert_eq!(a.allow.len(), 5, "expected 5 allow patterns");
        assert_eq!(a.deny.len(), 15, "expected 15 deny patterns");
    }

    #[test]
    fn from_path_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        let a = Allowlist::from_path(&path).unwrap();
        assert!(a.allow.is_empty());
        assert!(a.deny.is_empty());
    }

    #[test]
    fn from_path_malformed_json_returns_err() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, b"not valid json").unwrap();
        assert!(Allowlist::from_path(&path).is_err());
    }

    // -----------------------------------------------------------------------
    // cascade tests
    // -----------------------------------------------------------------------

    fn allowlist_with_patterns(allow: &[&str], deny: &[&str]) -> Allowlist {
        Allowlist {
            allow: allow.iter().map(|s| Pattern::parse(s)).collect(),
            deny: deny.iter().map(|s| Pattern::parse(s)).collect(),
        }
    }

    fn raw_strs(patterns: &[Pattern]) -> Vec<&str> {
        patterns.iter().map(|p| p.raw()).collect()
    }

    #[test]
    fn cascade_deny_union() {
        let local = allowlist_with_patterns(&[], &["Skill"]);
        let project = allowlist_with_patterns(&[], &["Bash(npm test)"]);
        let user = allowlist_with_patterns(&[], &["Read(**/.env*)"]);

        let merged = Allowlist::cascade(local, project, user);

        let deny_raws = raw_strs(&merged.deny);
        assert!(deny_raws.contains(&"Skill"),            "local deny must be present");
        assert!(deny_raws.contains(&"Bash(npm test)"),   "project deny must be present");
        assert!(deny_raws.contains(&"Read(**/.env*)"),   "user deny must be present");
        assert_eq!(merged.deny.len(), 3);
    }

    #[test]
    fn cascade_allow_union() {
        let local = allowlist_with_patterns(&["Agent(task-planner)"], &[]);
        let project = allowlist_with_patterns(&["Bash(npm test)"],    &[]);
        let user = allowlist_with_patterns(&["Skill"],                &[]);

        let merged = Allowlist::cascade(local, project, user);

        let allow_raws = raw_strs(&merged.allow);
        assert!(allow_raws.contains(&"Agent(task-planner)"), "local allow must be present");
        assert!(allow_raws.contains(&"Bash(npm test)"),      "project allow must be present");
        assert!(allow_raws.contains(&"Skill"),               "user allow must be present");
        assert_eq!(merged.allow.len(), 3);
    }

    #[test]
    fn cascade_local_first_in_vec() {
        // Verify ordering: local patterns come before project, project before user.
        let local = allowlist_with_patterns(&["A"], &["X"]);
        let project = allowlist_with_patterns(&["B"], &["Y"]);
        let user = allowlist_with_patterns(&["C"], &["Z"]);

        let merged = Allowlist::cascade(local, project, user);

        let allow_raws = raw_strs(&merged.allow);
        assert_eq!(allow_raws, vec!["A", "B", "C"], "allow order must be local→project→user");

        let deny_raws = raw_strs(&merged.deny);
        assert_eq!(deny_raws, vec!["X", "Y", "Z"], "deny order must be local→project→user");
    }
}
