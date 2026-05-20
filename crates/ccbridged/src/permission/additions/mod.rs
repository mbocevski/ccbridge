// SPDX-License-Identifier: MIT
//! Allowlist pattern derivation, settings.json writer, and audit log.
//!
//! # Purpose
//!
//! When the user clicks **Always** on a swaync approval notification,
//! ccbridge needs to:
//! 1. Derive the most-conservative pattern that would match this tool call.
//! 2. Write it to `~/.claude/settings.json`'s `permissions.allow` array.
//! 3. Append a line to the audit log so the user can review and undo.
//!
//! # Pattern derivation rules
//!
//! The goal is to derive a *literal* pattern — not a glob — so the user
//! explicitly opts into one specific operation.  A derived `Bash(rm -rf
//! /tmp/foo)` only allows exactly that command, not `rm -rf /home`.
//!
//! | tool_name | input field | Derived pattern |
//! |---|---|---|
//! | `mcp__*` | any | `mcp__plugin_X__method` (exact MCP id) |
//! | `Bash` | `command: str` | `Bash(<command>)` |
//! | `Read`/`Edit`/`Write`/`MultiEdit` | `file_path: str` | `<tool>(<path>)` |
//! | `Agent` | `subagent_type: str` | `Agent(<type>)` |
//! | `Glob`/`Grep` | — | `BareToolNeedsConfirmation` (known limitation: matcher doesn't support their input fields; use bare-tool with second confirmation rather than derive a pattern the matcher can't honor) |
//! | anything else | — | `BareToolNeedsConfirmation` |
//!
//! # Round-trip invariant
//!
//! For every `DerivedPattern::Specific(s)`, `Pattern::parse(&s).matches(event)
//! == Confident`.  Tests verify this for every supported derivation path.
//!
//! # Module layout
//!
//! - `derive`    — pure logic: hook event → allowlist pattern.
//! - `target`    — `WriteTarget` / `AuditTarget` and project-root resolution.
//! - `write`     — write a pattern into settings.local.json + record audit.
//! - `undo`      — `undo-last-allow` CLI dispatch + audit-root validation.
//! - `audit_log` — JSONL on-disk format, append + reverse-walk.

mod audit_log;
mod derive;
mod target;
mod undo;
mod write;

// Public re-exports — preserve the original `permission::additions::FOO`
// surface so external callers (main.rs, state/mod.rs, tests) don't need
// to change.
pub use audit_log::{audit_log_path, AdditionMetadata};
pub use derive::{derive_pattern, DerivedPattern};
pub use target::{resolve_write_target, AuditTarget, WriteTarget};
pub use undo::{undo_last_allow, validate_audit_root, UndoOutcome};
pub use write::write_allow_pattern;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// Tests are kept in this single mod.rs because most exercise multiple
// submodules end-to-end (e.g. write_allow_pattern → append_audit_entry →
// find_last_undone_addition → undo_last_allow). Splitting them across
// submodule test mods would force most helpers to be `pub(super)` purely
// for test reach. The cost of keeping them here is one larger test
// module; the benefit is that tests can name internals via
// super::audit_log::* without re-exporting.

#[cfg(test)]
mod tests {
    use super::audit_log::{append_audit_entry, find_last_undone_addition};
    use super::derive::derive_pattern;
    use super::target::{resolve_write_target, AuditTarget, WriteTarget};
    use super::undo::{lexically_normalize, undo_last_allow, validate_audit_root};
    use super::write::write_allow_pattern;
    use super::{AdditionMetadata, DerivedPattern, UndoOutcome};

    use ccbridge_proto::hook::{HookBase, PermissionMode, PreToolUseEvent};
    use serde_json::json;
    use std::path::Path;
    use tempfile::TempDir;

    fn event(tool: &str, input: serde_json::Value) -> PreToolUseEvent {
        PreToolUseEvent {
            base: HookBase {
                session_id: "3cb58992-935c-4fdd-9efd-1f160946e822".to_owned(),
                transcript_path: "/tmp/t.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: input,
            tool_use_id: "toolu_test_01".to_owned(),
            agent_id: None,
            agent_type: None,
        }
    }

    fn meta() -> AdditionMetadata {
        AdditionMetadata {
            tool_use_id: "toolu_test_01".to_owned(),
            session_id: "3cb58992-935c-4fdd-9efd-1f160946e822".to_owned(),
            agent_type: Some("core".to_owned()),
        }
    }

    // -----------------------------------------------------------------------
    // derive_pattern
    // -----------------------------------------------------------------------

    #[test]
    fn derive_pattern_bash_literal_command() {
        let e = event("Bash", json!({"command": "rm -rf /tmp/foo"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::Specific("Bash(rm -rf /tmp/foo)".to_owned())
        );
    }

    #[test]
    fn derive_pattern_read_exact_path() {
        let e = event("Read", json!({"file_path": "/home/user/.env"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::Specific("Read(/home/user/.env)".to_owned())
        );
    }

    #[test]
    fn derive_pattern_edit_exact_path() {
        let e = event(
            "Edit",
            json!({"file_path": "/tmp/foo.rs", "old_string": "a", "new_string": "b"}),
        );
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::Specific("Edit(/tmp/foo.rs)".to_owned())
        );
    }

    #[test]
    fn derive_pattern_agent_subagent_type() {
        let e = event("Agent", json!({"subagent_type": "task-planner"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::Specific("Agent(task-planner)".to_owned())
        );
    }

    #[test]
    fn derive_pattern_mcp_exact() {
        let e = event("mcp__plugin_context7_context7__query-docs", json!({}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::Specific("mcp__plugin_context7_context7__query-docs".to_owned())
        );
    }

    #[test]
    fn derive_pattern_unknown_tool_is_bare() {
        // Use a plausible future tool name, not a generic placeholder.
        let e = event("WebSearch", json!({"query": "Rust tokio tutorial"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::BareToolNeedsConfirmation {
                tool: "WebSearch".to_owned()
            }
        );
    }

    #[test]
    fn derive_pattern_bash_missing_command_is_bare() {
        let e = event("Bash", json!({"description": "no command field"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::BareToolNeedsConfirmation {
                tool: "Bash".to_owned()
            }
        );
    }

    #[test]
    fn derive_pattern_glob_falls_to_bare() {
        // Known limitation: Glob uses "pattern" not "file_path", so our matcher
        // would not recognise a derived Glob(...) as Confident.
        let e = event("Glob", json!({"pattern": "*.rs"}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::BareToolNeedsConfirmation {
                tool: "Glob".to_owned()
            }
        );
    }

    #[test]
    fn derive_pattern_non_string_field_falls_to_bare() {
        // Defensive: numeric field value must not be coerced to a string path.
        let e = event("Read", json!({"file_path": 42}));
        assert_eq!(
            derive_pattern(&e),
            DerivedPattern::BareToolNeedsConfirmation {
                tool: "Read".to_owned()
            }
        );
    }

    // -----------------------------------------------------------------------
    // Round-trip invariant
    // -----------------------------------------------------------------------

    fn assert_round_trip(tool: &str, input: serde_json::Value, expected_pattern: &str) {
        use crate::permission::pattern::{MatchResult, Pattern};
        let e = event(tool, input.clone());
        let derived = derive_pattern(&e);
        assert_eq!(
            derived,
            DerivedPattern::Specific(expected_pattern.to_owned()),
            "derive_pattern should produce Specific({expected_pattern:?}) for {tool}"
        );
        let parsed = Pattern::parse(expected_pattern);
        assert_eq!(
            parsed.matches(&e),
            MatchResult::Confident,
            "Pattern::parse({expected_pattern:?}).matches(event) must be Confident for round-trip"
        );
    }

    #[test]
    fn round_trip_bash_command() {
        assert_round_trip("Bash", json!({"command": "git status"}), "Bash(git status)");
    }

    #[test]
    fn round_trip_read_path() {
        assert_round_trip(
            "Read",
            json!({"file_path": "/tmp/file.txt"}),
            "Read(/tmp/file.txt)",
        );
    }

    #[test]
    fn round_trip_agent_subagent() {
        assert_round_trip(
            "Agent",
            json!({"subagent_type": "task-planner"}),
            "Agent(task-planner)",
        );
    }

    #[test]
    fn round_trip_mcp_exact() {
        assert_round_trip(
            "mcp__plugin_backlog_tasks__task_list",
            json!({}),
            "mcp__plugin_backlog_tasks__task_list",
        );
    }

    // -----------------------------------------------------------------------
    // write_allow_pattern
    // -----------------------------------------------------------------------

    #[test]
    fn write_allow_pattern_adds_to_array() {
        let dir = TempDir::new().unwrap();
        let settings = dir.path().join("settings.json");
        let audit = dir.path().join("audit.log");
        std::fs::write(&settings, r#"{"theme":"dark"}"#).unwrap();

        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };
        write_allow_pattern(&target, "Bash(git status)", &audit, meta()).unwrap();

        let loaded_path = dir.path().join(".claude").join("settings.local.json");
        let loaded = crate::setup::load_settings(&loaded_path).unwrap();
        let allow = loaded["permissions"]["allow"].as_array().unwrap();
        assert_eq!(allow.len(), 1);
        assert_eq!(allow[0], "Bash(git status)");
    }

    #[test]
    fn write_allow_pattern_idempotent() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Bash(echo hi)", &audit, meta()).unwrap();
        write_allow_pattern(&target, "Bash(echo hi)", &audit, meta()).unwrap();

        let loaded_path = dir.path().join(".claude").join("settings.local.json");
        let loaded = crate::setup::load_settings(&loaded_path).unwrap();
        let allow = loaded["permissions"]["allow"].as_array().unwrap();
        assert_eq!(allow.len(), 1, "duplicate pattern must not be added");
    }

    #[test]
    fn write_allow_pattern_writes_audit_log() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Read(/tmp/file.txt)", &audit, meta()).unwrap();

        let log = std::fs::read_to_string(&audit).unwrap();
        assert!(log.contains("added"), "audit log must contain 'added' op");
        assert!(
            log.contains("Read(/tmp/file.txt)"),
            "audit log must contain the pattern"
        );
    }

    #[test]
    fn write_allow_pattern_bails_when_allow_is_string() {
        // User wrote `"allow": "Bash(...)"` instead of an array. Earlier
        // versions silently clobbered this with `[]`. We must refuse and
        // preserve the file.
        let dir = TempDir::new().unwrap();
        let claude_dir = dir.path().join(".claude");
        std::fs::create_dir_all(&claude_dir).unwrap();
        let settings_path = claude_dir.join("settings.local.json");
        let original = r#"{"permissions":{"allow":"Bash(git status)"}}"#;
        std::fs::write(&settings_path, original).unwrap();
        let audit = dir.path().join("audit.log");

        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };
        let err = write_allow_pattern(&target, "Bash(echo hi)", &audit, meta()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(".permissions.allow") && msg.contains("string"),
            "error must point at .permissions.allow and name the actual type, got: {msg}"
        );
        let on_disk = std::fs::read_to_string(&settings_path).unwrap();
        assert_eq!(
            on_disk, original,
            "settings file must be untouched on error"
        );
    }

    #[test]
    fn write_allow_pattern_bails_when_allow_is_null() {
        // `"allow": null` — same data-loss footgun as the string case.
        let dir = TempDir::new().unwrap();
        let claude_dir = dir.path().join(".claude");
        std::fs::create_dir_all(&claude_dir).unwrap();
        let settings_path = claude_dir.join("settings.local.json");
        let original = r#"{"permissions":{"allow":null}}"#;
        std::fs::write(&settings_path, original).unwrap();
        let audit = dir.path().join("audit.log");

        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };
        let err = write_allow_pattern(&target, "Bash(echo hi)", &audit, meta()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(".permissions.allow") && msg.contains("null"),
            "error must point at .permissions.allow and name the actual type, got: {msg}"
        );
        let on_disk = std::fs::read_to_string(&settings_path).unwrap();
        assert_eq!(
            on_disk, original,
            "settings file must be untouched on error"
        );
    }

    // -----------------------------------------------------------------------
    // undo_last_allow
    // -----------------------------------------------------------------------

    #[test]
    fn undo_last_allow_removes_pattern() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Bash(echo undo_me)", &audit, meta()).unwrap();

        let loaded_path = dir.path().join(".claude").join("settings.local.json");
        assert_eq!(
            crate::setup::load_settings(&loaded_path).unwrap()["permissions"]["allow"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        undo_last_allow(&audit).unwrap();

        let allow = crate::setup::load_settings(&loaded_path).unwrap()["permissions"]["allow"]
            .as_array()
            .unwrap()
            .to_owned();
        assert!(allow.is_empty(), "pattern must be removed after undo");

        let log = std::fs::read_to_string(&audit).unwrap();
        assert!(
            log.contains("undone"),
            "audit log must contain 'undone' after undo"
        );
    }

    #[test]
    fn undo_last_allow_empty_audit_returns_error() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log"); // doesn't exist

        let err = undo_last_allow(&audit).unwrap_err();
        assert!(
            err.to_string().contains("no allowlist additions"),
            "error message must mention empty audit log"
        );
    }

    #[test]
    fn undo_last_allow_idempotent_when_pattern_already_gone() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Bash(already_gone)", &audit, meta()).unwrap();

        // Manually empty the allow list.
        let local_settings = dir.path().join(".claude").join("settings.local.json");
        std::fs::write(&local_settings, r#"{"permissions":{"allow":[]}}"#).unwrap();

        let outcome = undo_last_allow(&audit).unwrap();
        assert!(
            matches!(outcome, UndoOutcome::AlreadyGone { .. }),
            "must return AlreadyGone when pattern not in file"
        );
    }

    // -----------------------------------------------------------------------
    // G3: audit root validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn validate_audit_root_rejects_filesystem_root() {
        let ugd = std::path::PathBuf::from("/nonexistent-ccbridge-ugd");
        let err = validate_audit_root(Path::new("/"), &ugd).unwrap_err();
        assert!(
            err.to_string().contains("filesystem root"),
            "error must name the rule, got: {err}"
        );
    }

    #[test]
    fn validate_audit_root_rejects_relative_path() {
        let ugd = std::path::PathBuf::from("/nonexistent-ccbridge-ugd");
        let err = validate_audit_root(Path::new("relative/path"), &ugd).unwrap_err();
        assert!(
            err.to_string().contains("relative"),
            "error must name the rule, got: {err}"
        );
    }

    #[test]
    fn validate_audit_root_rejects_user_global_collision() {
        // Root equal to user-global config dir must be rejected.
        let ugd = TempDir::new().unwrap();
        let err = validate_audit_root(ugd.path(), ugd.path()).unwrap_err();
        assert!(
            err.to_string().contains("collides"),
            "error must name the collision, got: {err}"
        );
    }

    #[test]
    fn validate_audit_root_accepts_normal_project_dir() {
        let dir = TempDir::new().unwrap();
        let ugd = TempDir::new().unwrap();
        // Two distinct temp dirs — must pass.
        validate_audit_root(dir.path(), ugd.path()).unwrap();
    }

    #[test]
    fn validate_audit_root_rejects_trailing_dot_collision_when_dir_absent() {
        // Tampered audit line: root is `/path/to/.claude/.` and the
        // directory doesn't exist yet (so canonicalize can't run).
        // Without lexical normalisation this would pass the equality
        // check and later silently write to ~/.claude/settings.local.json.
        let nonexistent_ugd = std::path::PathBuf::from("/this/path/does/not/exist/.claude");
        let tampered_root = std::path::PathBuf::from("/this/path/does/not/exist/.claude/.");
        let err = validate_audit_root(&tampered_root, &nonexistent_ugd).unwrap_err();
        assert!(
            err.to_string().contains("collides"),
            "error must catch the lexical collision via `.` collapse, got: {err}"
        );
    }

    #[test]
    fn validate_audit_root_rejects_parent_dir_collision_when_dir_absent() {
        // Tampered audit line: `/path/foo/../.claude` resolves lexically
        // to `/path/.claude`. Must be rejected as a collision when that
        // matches the user-global dir.
        let nonexistent_ugd = std::path::PathBuf::from("/no/such/path/.claude");
        let tampered_root = std::path::PathBuf::from("/no/such/path/foo/../.claude");
        let err = validate_audit_root(&tampered_root, &nonexistent_ugd).unwrap_err();
        assert!(
            err.to_string().contains("collides"),
            "error must catch the lexical collision via `..` resolution, got: {err}"
        );
    }

    #[test]
    fn lexically_normalize_collapses_dot_and_parent() {
        let p = std::path::PathBuf::from("/a/b/c/./../d");
        assert_eq!(lexically_normalize(&p), std::path::PathBuf::from("/a/b/d"),);

        let p = std::path::PathBuf::from("/a/.");
        assert_eq!(lexically_normalize(&p), std::path::PathBuf::from("/a"));

        let p = std::path::PathBuf::from("/a/../../b");
        // `..` past the root is dropped — `/a/..` → `/`, then `..` past
        // `/` is dropped, then `/b`. Conservative for security checks.
        assert_eq!(lexically_normalize(&p), std::path::PathBuf::from("/b"));
    }

    #[test]
    fn undo_last_allow_rejects_root_slash() {
        // A JSONL audit line with root:"/" must cause undo to return Err.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");

        // Write a JSONL line with root:"/".
        let line = serde_json::json!({
            "ts": "2026-01-01T00:00:00Z",
            "op": "added",
            "pattern": "Bash(evil)",
            "tool_use_id": "toolu_evil",
            "session_id": "abc123",
            "target": {"project_local": {"root": "/"}}
        });
        std::fs::write(&audit, format!("{line}\n")).unwrap();

        let err = undo_last_allow(&audit).unwrap_err();
        assert!(
            err.to_string().contains("filesystem root") || err.to_string().contains("invalid"),
            "must reject root '/', got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // P3: new tests
    // -----------------------------------------------------------------------

    #[test]
    fn write_allow_pattern_project_local_creates_dotclaude_dir() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        // No .claude/ dir yet.
        assert!(!dir.path().join(".claude").exists());

        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };
        write_allow_pattern(&target, "Bash(npm test)", &audit, meta()).unwrap();

        let local = dir.path().join(".claude").join("settings.local.json");
        assert!(local.exists(), "settings.local.json must be created");
        let loaded = crate::setup::load_settings(&local).unwrap();
        assert_eq!(
            loaded["permissions"]["allow"].as_array().unwrap()[0],
            "Bash(npm test)"
        );
    }

    #[test]
    fn write_allow_pattern_project_local_records_target_in_audit() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Skill", &audit, meta()).unwrap();

        let log = std::fs::read_to_string(&audit).unwrap();
        // New JSONL format: the root path appears as a JSON string value.
        let root_str = dir.path().to_str().unwrap();
        assert!(
            log.contains(root_str),
            "audit log must contain project root path, got:\n{log}"
        );
        assert!(
            log.contains("project_local"),
            "audit log must contain 'project_local' key, got:\n{log}"
        );
    }

    #[test]
    fn audit_entry_user_global_encodes_as_jsonl() {
        // Verify the JSONL encoding for AuditTarget::UserGlobal.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let metadata = meta();

        append_audit_entry(
            &audit,
            "added",
            "Skill",
            &metadata,
            &AuditTarget::UserGlobal,
        )
        .unwrap();

        let log = std::fs::read_to_string(&audit).unwrap();
        let row: serde_json::Value = serde_json::from_str(log.trim()).unwrap();
        assert_eq!(
            row["target"],
            serde_json::json!("user_global"),
            "UserGlobal target must serialise as \"user_global\""
        );
    }

    // -----------------------------------------------------------------------
    // Phase E: JSONL audit log tests
    // -----------------------------------------------------------------------

    #[test]
    fn audit_log_jsonl_round_trip_project_local() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        // write_allow_pattern writes JSONL via append_audit_entry.
        write_allow_pattern(&target, "Bash(npm test)", &audit, meta()).unwrap();

        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("entry must be found");
        assert_eq!(entry.pattern, "Bash(npm test)");
        assert_eq!(entry.op_str(), "added");
        assert!(
            matches!(&entry.target, AuditTarget::ProjectLocal { root } if root == dir.path()),
            "target must be ProjectLocal with correct root"
        );
    }

    #[test]
    fn audit_log_jsonl_round_trip_legacy_user_target() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let metadata = meta();

        append_audit_entry(
            &audit,
            "added",
            "Skill",
            &metadata,
            &AuditTarget::UserGlobal,
        )
        .unwrap();

        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("entry must be found");
        assert_eq!(entry.pattern, "Skill");
        assert!(
            matches!(entry.target, AuditTarget::UserGlobal),
            "UserGlobal target must round-trip correctly"
        );
    }

    #[test]
    fn audit_log_mixed_tsv_legacy_then_jsonl_new() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");

        // Write a legacy 7-col TSV line first.
        let legacy_line = format!(
            "2026-01-01T00:00:00Z\tadded\tBash(legacy)\ttoolu_old\tabc123\tcore\tproject:{}\n",
            dir.path().display()
        );
        std::fs::write(&audit, &legacy_line).unwrap();

        // Append a new JSONL line via the current writer.
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };
        write_allow_pattern(&target, "Bash(new_cmd)", &audit, meta()).unwrap();

        // find_last_undone_addition must return the newest (JSONL) entry.
        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("entry must be found");
        assert_eq!(
            entry.pattern, "Bash(new_cmd)",
            "newest entry (JSONL) must be returned"
        );
        assert_eq!(entry.op_str(), "added");

        // Undo the newest, then the legacy one should surface.
        undo_last_allow(&audit).unwrap();

        let entry2 = find_last_undone_addition(&audit)
            .unwrap()
            .expect("legacy entry must surface after undo");
        assert_eq!(entry2.pattern, "Bash(legacy)");
    }

    #[test]
    fn audit_log_handles_bash_pattern_with_tab() {
        // Patterns containing \t must round-trip through JSONL without corruption.
        // This is the killer feature vs TSV — a tab in the pattern would break
        // column alignment in the old format.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        let pattern_with_tab = "Bash(echo \"hi\there\")";
        write_allow_pattern(&target, pattern_with_tab, &audit, meta()).unwrap();

        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("entry must be found");
        assert_eq!(
            entry.pattern, pattern_with_tab,
            "pattern with tab must round-trip correctly via JSONL"
        );
    }

    #[test]
    fn undo_last_allow_target_aware_project_local() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Bash(npm test)", &audit, meta()).unwrap();

        let local = dir.path().join(".claude").join("settings.local.json");
        assert_eq!(
            crate::setup::load_settings(&local).unwrap()["permissions"]["allow"]
                .as_array()
                .unwrap()
                .len(),
            1,
            "pattern must be in project-local file"
        );

        undo_last_allow(&audit).unwrap();

        let allow = crate::setup::load_settings(&local).unwrap()["permissions"]["allow"]
            .as_array()
            .unwrap()
            .to_owned();
        assert!(
            allow.is_empty(),
            "pattern must be removed from project-local file"
        );
    }

    #[test]
    fn find_last_undone_addition_skips_unknown_target() {
        // Newer daemon wrote an addition with a target type this binary
        // doesn't understand. Older daemon walking back must skip it
        // rather than try to undo a settings file it can't identify.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let unknown_line = r#"{"ts":"2026-02-01T00:00:00Z","op":"added","pattern":"Bash(future)","tool_use_id":"toolu_future","session_id":"xyz","target":"future_target"}"#;
        let known_line = r#"{"ts":"2026-01-01T00:00:00Z","op":"added","pattern":"Bash(known)","tool_use_id":"toolu_known","session_id":"abc","target":"user_global"}"#;
        // Order on disk: known (older) then unknown (newer); reverse walk
        // hits unknown first — must skip and reach known.
        std::fs::write(&audit, format!("{known_line}\n{unknown_line}\n")).unwrap();

        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("must skip unknown and return the known addition");
        assert_eq!(entry.pattern, "Bash(known)");
        assert!(matches!(entry.target, AuditTarget::UserGlobal));
    }

    #[test]
    fn undo_last_allow_bails_on_unknown_target_only() {
        // If the only addition has an unknown target, undo must bail
        // cleanly — walk-back skips it, find_last_undone_addition
        // returns None, undo reports "no additions to undo".
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let unknown_line = r#"{"ts":"2026-02-01T00:00:00Z","op":"added","pattern":"Bash(future)","tool_use_id":"toolu_future","session_id":"xyz","target":"future_target"}"#;
        std::fs::write(&audit, format!("{unknown_line}\n")).unwrap();

        let err = undo_last_allow(&audit).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no allowlist additions"),
            "must bail with 'no additions' (unknown was skipped), got: {msg}"
        );
    }

    #[test]
    fn find_last_undone_addition_legacy_6_column_treats_as_user() {
        // A 6-column legacy line (no target column) must parse as
        // WriteTarget::UserGlobal — backwards-compat for audit logs from
        // earlier daemon versions.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let legacy_line = "2026-01-01T00:00:00Z\tadded\tBash(legacy)\ttoolu_old\tabc123\t\n";
        std::fs::write(&audit, legacy_line).unwrap();

        let entry = find_last_undone_addition(&audit).unwrap().expect("entry");
        assert_eq!(entry.pattern, "Bash(legacy)");
        assert!(matches!(entry.target, AuditTarget::UserGlobal));
    }

    #[test]
    fn find_last_undone_addition_handles_re_always_after_undo() {
        // Pattern X, tool_use_id A: added → undone → added (same id).
        // The second `added` is un-undone and must be returned.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");

        let line = |op: &str, ts: &str| {
            serde_json::json!({
                "ts": ts,
                "op": op,
                "pattern": "Bash(test)",
                "tool_use_id": "toolu_A",
                "session_id": "abc",
                "target": {"project_local": {"root": "/tmp/proj"}},
            })
        };
        let log = format!(
            "{}\n{}\n{}\n",
            line("added", "2026-01-01T00:00:00Z"),
            line("undone", "2026-01-01T00:00:01Z"),
            line("added", "2026-01-01T00:00:02Z"),
        );
        std::fs::write(&audit, log).unwrap();

        let entry = find_last_undone_addition(&audit)
            .unwrap()
            .expect("re-Always should surface as un-undone");
        assert_eq!(entry.pattern, "Bash(test)");
        assert_eq!(entry.op_str(), "added");
    }

    #[test]
    fn resolve_write_target_uses_cwd_as_root_when_no_ancestor_marker() {
        // No .claude/ or .git anywhere in the path → cwd itself becomes the
        // project root.  write_allow_pattern will create <cwd>/.claude/.
        let cwd = std::path::Path::new("/nonexistent-ccbridge-test-xyz/sub");
        let target = resolve_write_target(cwd);
        assert_eq!(
            target.root, cwd,
            "root must equal cwd when no project marker found"
        );
    }
}
