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
}
