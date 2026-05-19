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

use std::io::Write as IoWrite;
use std::path::Path;

use anyhow::{Context, Result};
use ccbridge_proto::hook::PreToolUseEvent;

use crate::setup::{load_settings, save_settings};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Result of `derive_pattern`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedPattern {
    /// A specific, literal pattern — write it directly to settings.json.
    Specific(String),

    /// A bare tool name (e.g. `Bash` with no args) — requires explicit
    /// secondary confirmation before writing, since it allows ALL calls to
    /// this tool.
    BareToolNeedsConfirmation { tool: String },
}

/// Metadata attached to an audit log entry.
pub struct AdditionMetadata {
    pub tool_use_id: String,
    pub session_id: String,
    pub agent_type: Option<String>,
}

/// Where an allowlist addition should be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteTarget {
    /// `<project_root>/.claude/settings.local.json`
    /// Created (including the `.claude/` directory) if it doesn't exist.
    ProjectLocal { root: std::path::PathBuf },
    /// `~/.claude/settings.json` (user-global fallback).  Only used when
    /// `cwd` is `$HOME` itself — in every other case we make `cwd` a project
    /// root by creating `.claude/` in it.
    UserGlobal,
}

/// Resolve the write target from a `cwd` path.
///
/// - If `find_project_root` finds a real project root (an ancestor with
///   `.claude/` or `.git`), use it.
/// - Otherwise, treat `cwd` itself as the project root — `write_allow_pattern`
///   will create `<cwd>/.claude/` if absent.  This means clicking Always from
///   a scratch directory bootstraps it as a Claude project rather than leaking
///   the pattern into user-global config.
/// - Edge case: `cwd == $HOME` falls back to [`WriteTarget::UserGlobal`] so
///   we never silently overwrite the user's `~/.claude/` setup with a
///   project-style file.
pub fn resolve_write_target(cwd: &std::path::Path) -> WriteTarget {
    if let Some(root) = crate::permission::project::find_project_root(cwd) {
        return WriteTarget::ProjectLocal { root };
    }
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    if home.as_deref() == Some(cwd) {
        return WriteTarget::UserGlobal;
    }
    WriteTarget::ProjectLocal {
        root: cwd.to_path_buf(),
    }
}

// ---------------------------------------------------------------------------
// Pattern derivation
// ---------------------------------------------------------------------------

/// Derive the most-conservative `permissions.allow` pattern for a tool call.
///
/// See module documentation for the full derivation table and the round-trip
/// invariant.
pub fn derive_pattern(event: &PreToolUseEvent) -> DerivedPattern {
    let tool = event.tool_name.as_str();

    // MCP methods — always specific (exact ID).
    if tool.starts_with("mcp__") {
        return DerivedPattern::Specific(tool.to_owned());
    }

    match tool {
        "Bash" => {
            // Derive a literal command match.  Defensive: only accept a JSON
            // string — don't coerce numbers or booleans to strings.
            if let Some(cmd) = event.tool_input.get("command").and_then(|v| v.as_str()) {
                DerivedPattern::Specific(format!("Bash({cmd})"))
            } else {
                DerivedPattern::BareToolNeedsConfirmation {
                    tool: tool.to_owned(),
                }
            }
        }

        "Read" | "Edit" | "Write" | "MultiEdit" => {
            // Path-based tools — derive an exact-path pattern.
            if let Some(path) = event.tool_input.get("file_path").and_then(|v| v.as_str()) {
                DerivedPattern::Specific(format!("{tool}({path})"))
            } else {
                DerivedPattern::BareToolNeedsConfirmation {
                    tool: tool.to_owned(),
                }
            }
        }

        "Agent" => {
            if let Some(t) = event
                .tool_input
                .get("subagent_type")
                .and_then(|v| v.as_str())
            {
                DerivedPattern::Specific(format!("Agent({t})"))
            } else {
                DerivedPattern::BareToolNeedsConfirmation {
                    tool: tool.to_owned(),
                }
            }
        }

        // Glob and Grep use input fields ("pattern", "path") that our matcher
        // doesn't currently map to Confident matches.  Deriving a pattern that
        // the matcher wouldn't recognize as Confident would violate the
        // round-trip invariant, so we fall to BareToolNeedsConfirmation.
        // This is a known limitation; improve the matcher in a follow-up task.
        _ => DerivedPattern::BareToolNeedsConfirmation {
            tool: tool.to_owned(),
        },
    }
}

// ---------------------------------------------------------------------------
// Settings.json writer
// ---------------------------------------------------------------------------

/// Append `pattern` to the allowlist file specified by `target` and record
/// the addition in the audit log.
///
/// - For `WriteTarget::ProjectLocal { root }`: writes to
///   `<root>/.claude/settings.local.json`, creating `.claude/` if absent.
/// - For `WriteTarget::UserGlobal`: writes to `~/.claude/settings.json`.
///
/// Idempotent: if the pattern is already present, returns `Ok(())`.
pub fn write_allow_pattern(
    target: &WriteTarget,
    pattern: &str,
    audit_log_path: &Path,
    metadata: AdditionMetadata,
) -> Result<()> {
    let settings_path = match target {
        WriteTarget::ProjectLocal { root } => {
            let dir = root.join(".claude");
            std::fs::create_dir_all(&dir).with_context(|| format!("mkdir -p {}", dir.display()))?;
            dir.join("settings.local.json")
        }
        WriteTarget::UserGlobal => crate::permission::settings_path(),
    };

    let mut settings = load_settings(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;

    // Ensure settings["permissions"]["allow"] exists and is an array.
    let allow_arr = settings
        .as_object_mut()
        .unwrap()
        .entry("permissions")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .unwrap()
        .entry("allow")
        .or_insert_with(|| serde_json::json!([]));

    if !allow_arr.is_array() {
        *allow_arr = serde_json::json!([]);
    }

    let arr = allow_arr.as_array_mut().unwrap();

    // Idempotency check.
    if arr.iter().any(|v| v.as_str() == Some(pattern)) {
        tracing::debug!(
            "pattern {:?} already present in allow list; skipping write",
            pattern
        );
        return Ok(());
    }

    arr.push(serde_json::Value::String(pattern.to_owned()));

    save_settings(&settings_path, &settings)
        .with_context(|| format!("write {}", settings_path.display()))?;

    // Append audit log entry.
    append_audit_entry(audit_log_path, "added", pattern, &metadata, target)?;

    tracing::info!(
        pattern = %pattern,
        tool_use_id = %metadata.tool_use_id,
        session = %short_id(&metadata.session_id),
        agent = ?metadata.agent_type,
        ?target,
        "allowlist: added pattern",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// undo-last-allow
// ---------------------------------------------------------------------------

/// Remove the most-recent un-undone allowlist addition and mark it as undone.
///
/// The target file (project-local or user-global) is read from the audit log,
/// so the caller doesn't need to pass a settings path.
///
/// - Pattern not in the target file (manually removed): prints a notice, returns `Ok(())`.
/// - Audit log empty / no `added` entries: returns an `Err`.
pub fn undo_last_allow(audit_log_path: &Path) -> Result<()> {
    let entry = find_last_undone_addition(audit_log_path)
        .context("reading audit log")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no allowlist additions in audit log to undo ({})",
                audit_log_path.display()
            )
        })?;

    // Resolve path from the target stored in the audit log.
    let settings_path = match &entry.target {
        WriteTarget::ProjectLocal { root } => root.join(".claude").join("settings.local.json"),
        WriteTarget::UserGlobal => crate::permission::settings_path(),
    };

    let mut settings = load_settings(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;

    let allow_arr = settings
        .get_mut("permissions")
        .and_then(|p| p.get_mut("allow"))
        .and_then(|a| a.as_array_mut());

    match allow_arr {
        None => {
            println!(
                "Pattern {:?} not present in {} (already removed?).",
                entry.pattern,
                settings_path.display(),
            );
        }
        Some(arr) => {
            let before = arr.len();
            arr.retain(|v| v.as_str() != Some(&entry.pattern));
            if arr.len() == before {
                println!(
                    "Pattern {:?} not present in {} (already removed?).",
                    entry.pattern,
                    settings_path.display(),
                );
            } else {
                save_settings(&settings_path, &settings)
                    .with_context(|| format!("write {}", settings_path.display()))?;
                println!(
                    "Removed pattern {:?} from {}.",
                    entry.pattern,
                    settings_path.display(),
                );
            }
        }
    }

    // Record the undo in the audit log regardless.
    append_audit_entry(
        audit_log_path,
        "undone",
        &entry.pattern,
        &AdditionMetadata {
            tool_use_id: entry.tool_use_id,
            session_id: entry.session_id,
            agent_type: entry.agent_type,
        },
        &entry.target,
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Audit log helpers
// ---------------------------------------------------------------------------

/// Audit log path: `$XDG_STATE_HOME/ccbridge/allowlist-additions.log`.
pub fn audit_log_path() -> anyhow::Result<std::path::PathBuf> {
    let base = if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        std::path::PathBuf::from(xdg)
    } else if let Some(home) = std::env::var_os("HOME") {
        std::path::PathBuf::from(home).join(".local").join("state")
    } else {
        anyhow::bail!("neither $XDG_STATE_HOME nor $HOME is set");
    };
    Ok(base.join("ccbridge").join("allowlist-additions.log"))
}

/// Append one TSV line to the audit log.
///
/// Format (7 columns):
/// `{ts}\t{op}\t{pattern}\t{tool_use_id}\t{session_short}\t{agent}\t{target}\n`
///
/// `{target}` is `project:<abs_path>` or `user`.
fn append_audit_entry(
    log_path: &Path,
    op: &str,
    pattern: &str,
    metadata: &AdditionMetadata,
    target: &WriteTarget,
) -> Result<()> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let ts = utc_now_iso8601();
    let session_short = short_id(&metadata.session_id);
    let agent = metadata.agent_type.as_deref().unwrap_or("");
    let target_col = match target {
        WriteTarget::ProjectLocal { root } => format!("project:{}", root.display()),
        WriteTarget::UserGlobal => "user".to_owned(),
    };
    let line = format!(
        "{ts}\t{op}\t{pattern}\t{tool_use_id}\t{session_short}\t{agent}\t{target_col}\n",
        tool_use_id = metadata.tool_use_id,
    );

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| format!("open audit log {}", log_path.display()))?;

    file.write_all(line.as_bytes())
        .with_context(|| format!("write audit log {}", log_path.display()))?;

    Ok(())
}

struct AuditEntry {
    pattern: String,
    tool_use_id: String,
    session_id: String,
    agent_type: Option<String>,
    target: WriteTarget,
}

/// Find the most-recent `added` line in the audit log that has no subsequent
/// `undone` line for the same pattern + tool_use_id pair.
///
/// The 7th column (target) is parsed when present; missing column or
/// unrecognised value → `WriteTarget::UserGlobal` (backwards compat with
/// 6-column logs from earlier daemon versions).
fn find_last_undone_addition(log_path: &Path) -> Result<Option<AuditEntry>> {
    if !log_path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(log_path)
        .with_context(|| format!("read {}", log_path.display()))?;

    // Walk lines in reverse; collect "added" entries and their subsequent undos.
    let mut undone_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut result: Option<AuditEntry> = None;

    for line in text.lines().rev() {
        // splitn(7) — up to 7 columns; 7th may be absent (legacy lines).
        let cols: Vec<&str> = line.splitn(7, '\t').collect();
        if cols.len() < 3 {
            continue;
        }
        let op = cols[1];
        let pattern = cols[2];
        let tool_use_id = cols.get(3).copied().unwrap_or("");
        let key = format!("{pattern}\x00{tool_use_id}");

        match op {
            "undone" => {
                undone_keys.insert(key);
            }
            "added" if !undone_keys.contains(&key) => {
                let session_short = cols.get(4).copied().unwrap_or("");
                let agent = cols
                    .get(5)
                    .copied()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned);

                // Parse target column (col 6); missing/unknown → UserGlobal.
                let target = {
                    let raw = cols.get(6).copied().unwrap_or("user");
                    if let Some(path) = raw.strip_prefix("project:") {
                        WriteTarget::ProjectLocal {
                            root: std::path::PathBuf::from(path),
                        }
                    } else {
                        WriteTarget::UserGlobal
                    }
                };

                result = Some(AuditEntry {
                    pattern: pattern.to_owned(),
                    tool_use_id: tool_use_id.to_owned(),
                    session_id: session_short.to_owned(),
                    agent_type: agent,
                    target,
                });
                break;
            }
            _ => {}
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

fn short_id(id: &str) -> String {
    id.chars().take(6).collect()
}

fn utc_now_iso8601() -> String {
    // Simple: seconds-since-epoch formatted as ISO 8601 UTC.
    // No chrono dep — same approach as the calendar math in jsonl.rs.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, mo, d) = crate::ingest::jsonl::days_to_ymd(secs / 86400);
    let rem = secs % 86400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ccbridge_proto::hook::{HookBase, PermissionMode, PreToolUseEvent};
    use serde_json::json;
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

        // Use UserGlobal target pointing at our temp settings file.
        // (We can't use WriteTarget::UserGlobal directly since it resolves
        // to ~/.claude/settings.json, so we test via ProjectLocal.)
        let target = WriteTarget::ProjectLocal {
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
        let target = WriteTarget::ProjectLocal {
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
        let target = WriteTarget::ProjectLocal {
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

    // -----------------------------------------------------------------------
    // undo_last_allow
    // -----------------------------------------------------------------------

    #[test]
    fn undo_last_allow_removes_pattern() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget::ProjectLocal {
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
        let target = WriteTarget::ProjectLocal {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Bash(already_gone)", &audit, meta()).unwrap();

        // Manually empty the allow list.
        let local_settings = dir.path().join(".claude").join("settings.local.json");
        std::fs::write(&local_settings, r#"{"permissions":{"allow":[]}}"#).unwrap();

        undo_last_allow(&audit).unwrap(); // must not panic
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

        let target = WriteTarget::ProjectLocal {
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
        let target = WriteTarget::ProjectLocal {
            root: dir.path().to_path_buf(),
        };

        write_allow_pattern(&target, "Skill", &audit, meta()).unwrap();

        let log = std::fs::read_to_string(&audit).unwrap();
        let expected = format!("project:{}", dir.path().display());
        assert!(
            log.contains(&expected),
            "audit log must contain project:<root>, got:\n{log}"
        );
    }

    #[test]
    fn write_allow_pattern_user_global_records_user_target() {
        // We can't write to the real user settings.json in tests,
        // but we can build a fake UserGlobal that writes somewhere we control
        // by mocking via ProjectLocal (or just test that the target encoding is right).
        // Here we verify the 7th column encoding via a direct append_audit_entry call.
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let metadata = meta();

        append_audit_entry(
            &audit,
            "added",
            "Skill",
            &metadata,
            &WriteTarget::UserGlobal,
        )
        .unwrap();

        let log = std::fs::read_to_string(&audit).unwrap();
        let cols: Vec<&str> = log.trim_end_matches('\n').split('\t').collect();
        assert_eq!(
            cols.get(6).copied(),
            Some("user"),
            "7th column must be 'user'"
        );
    }

    #[test]
    fn undo_last_allow_target_aware_project_local() {
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");
        let target = WriteTarget::ProjectLocal {
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
        assert!(matches!(entry.target, WriteTarget::UserGlobal));
    }

    #[test]
    fn resolve_write_target_uses_cwd_as_root_when_no_ancestor_marker() {
        // No .claude/ or .git anywhere in the path → cwd itself becomes the
        // project root.  write_allow_pattern will create <cwd>/.claude/.
        let cwd = std::path::Path::new("/nonexistent-ccbridge-test-xyz/sub");
        let target = resolve_write_target(cwd);
        match target {
            WriteTarget::ProjectLocal { ref root } => {
                assert_eq!(root, cwd);
            }
            other => panic!("expected ProjectLocal {{ root: cwd }}, got {other:?}"),
        }
    }
}
