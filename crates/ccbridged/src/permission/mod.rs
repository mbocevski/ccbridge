// SPDX-License-Identifier: MIT
//! Permission evaluator — decides what to do with a `PreToolUse` event
//! before the aggregator registers an approval.
//!
//! # Architecture
//!
//! `Aggregator::handle_hook_event` calls [`evaluate`] for every `PreToolUse`.
//! Based on the returned [`Decision`], it either:
//! - Short-circuits with an immediate allow/deny (no notification, no oneshot),
//! - Or calls `start_intercept()` for the normal hold-and-wait flow.
//!
//! ## Phase 1
//! Only checks `permission_mode`.  Permissive modes (`acceptEdits`, `auto`,
//! `dontAsk`, `bypassPermissions`) → `Decision::Allow`.  Others → `Decision::Intercept`.
//!
//! ## Phase 2
//! Also checks `permissions.allow` / `permissions.deny` from `settings.json`
//! via the [`Allowlist`].  Deny wins over allow; ambiguous patterns surface
//! as [`Decision::AskAnnotated`].

use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ccbridge_proto::hook::{PermissionMode, PreToolUseEvent};

pub mod additions;
pub mod allowlist;
pub mod cache;
pub mod pattern;
pub mod project;

pub use allowlist::Allowlist;
pub use cache::ProjectAllowlistCache;
pub use pattern::{MatchResult, Pattern};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// What the permission evaluator decided about a `PreToolUse` event.
#[derive(Debug, Clone)]
pub enum Decision {
    /// Confident allow.  Auto-approve without surfacing a notification.
    Allow { reason: String },

    /// Confident deny.  Hard-deny without surfacing a notification.
    ///
    /// Produced by deny-list pattern matches or by explicit user denies
    /// forwarded back as [`crate::state::HookResponse::HardDeny`].
    Deny { reason: String },

    /// A pattern was found in the allowlist but cannot be fully evaluated.
    ///
    /// The notification body should name the matched pattern so the user
    /// understands why ccbridge is intercepting a call they may have
    /// intended to allow/deny.
    AskAnnotated {
        matched_pattern: String,
        source: AllowOrDeny,
    },

    /// No confident decision.  Use the normal hold-for-approval flow.
    Intercept,
}

/// Which side of the allowlist a pattern came from (for annotations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowOrDeny {
    Allow,
    Deny,
}

// ---------------------------------------------------------------------------
// settings_path()
// ---------------------------------------------------------------------------

/// Return the path to Claude Code's `settings.json`.
///
/// Priority: `$CLAUDE_CONFIG_DIR/settings.json` → `$HOME/.claude/settings.json`.
/// Panics if neither variable is set (same constraint as elsewhere in the daemon).
pub fn settings_path() -> PathBuf {
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        return PathBuf::from(dir).join("settings.json");
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("$HOME must be set");
    home.join(".claude").join("settings.json")
}

// ---------------------------------------------------------------------------
// spawn_settings_watcher()
// ---------------------------------------------------------------------------

/// Spawn a task that watches `settings_path` for changes and atomically
/// replaces the in-memory allowlist via [`ArcSwap`].
///
/// # Behavior on file change
///
/// - Parse succeeds → `allowlist.store(Arc::new(new))`. Log info with new counts.
/// - Parse fails (settings.json transiently broken during an editor save) →
///   log `warn!`, **keep the old allowlist**.  We never silently lose deny-list
///   enforcement because of a partial file write.
///
/// # Portability
///
/// Watches the **parent directory** of `settings_path` rather than the file
/// directly.  `notify` can be unreliable when watching a file that gets
/// atomically replaced (rename-on-write, which editors use) — watching the
/// parent and filtering events is the portable pattern.
///
/// Uses the same sync-notify→async-tokio bridge as `ingest::jsonl`.
pub fn spawn_settings_watcher(
    settings_path: PathBuf,
    allowlist: Arc<ArcSwap<Allowlist>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_settings_watcher(settings_path, allowlist).await {
            tracing::error!("settings watcher exited with error: {e:#}");
        }
    })
}

async fn run_settings_watcher(
    settings_path: PathBuf,
    allowlist: Arc<ArcSwap<Allowlist>>,
) -> anyhow::Result<()> {
    use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
    use std::sync::mpsc as std_mpsc;

    // Watch the parent directory — see function doc.
    let watch_dir = settings_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("settings_path has no parent"))?
        .to_path_buf();

    let (ev_tx, ev_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher = RecommendedWatcher::new(ev_tx, Config::default())
        .map_err(|e| anyhow::anyhow!("create watcher: {e}"))?;
    watcher
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .map_err(|e| anyhow::anyhow!("watch {}: {e}", watch_dir.display()))?;

    tracing::info!(
        path = %settings_path.display(),
        "settings watcher started",
    );

    loop {
        // Drain all pending notify events.
        loop {
            match ev_rx.try_recv() {
                Ok(Ok(event)) => {
                    use notify::EventKind;
                    let relevant = matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_)
                    );
                    if !relevant {
                        continue;
                    }
                    // Filter to the specific settings.json path.
                    let touches_settings = event
                        .paths
                        .iter()
                        .any(|p| p == &settings_path);
                    if !touches_settings {
                        continue;
                    }
                    // Re-parse and swap.
                    match Allowlist::from_path(&settings_path) {
                        Ok(new) => {
                            tracing::info!(
                                allow_patterns = new.allow.len(),
                                deny_patterns = new.deny.len(),
                                "settings.json changed — reloaded allowlist",
                            );
                            allowlist.store(Arc::new(new));
                        }
                        Err(e) => {
                            tracing::warn!(
                                "settings.json changed but is invalid ({e:#}) — \
                                 keeping previous allowlist",
                            );
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!("settings watcher notify error: {e}");
                }
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    tracing::debug!("settings watcher channel closed — exiting");
                    return Ok(());
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// evaluate()
// ---------------------------------------------------------------------------

/// Evaluate a `PreToolUse` event against the current allowlist.
///
/// The evaluation order follows the spec:
/// 1. `permission_mode` short-circuit — permissive modes auto-allow.
/// 2. Deny patterns — confident match → `Deny`; ambiguous denies are
///    accumulated and surface as `AskAnnotated` only if no confident deny
///    matched later in the list.
/// 3. Allow patterns — confident match → `Allow`; ambiguous is remembered.
/// 4. No confident match: an accumulated ambiguous deny (priority) or
///    ambiguous allow surfaces as `AskAnnotated`.
/// 5. Nothing matched → `Intercept`.
pub fn evaluate(event: &PreToolUseEvent, allowlist: &Allowlist) -> Decision {
    // Step 1: permission_mode short-circuit.
    // `plan` is intentionally excluded — it is more restrictive, not permissive.
    match event.permission_mode {
        PermissionMode::AcceptEdits
        | PermissionMode::Auto
        | PermissionMode::DontAsk
        | PermissionMode::BypassPermissions => {
            return Decision::Allow {
                reason: format!("permission_mode={:?}", event.permission_mode),
            };
        }
        _ => {}
    }

    // Step 2: deny patterns.  Confident deny wins immediately.  Ambiguous
    // denies are accumulated (same pattern as the allow accumulator) so that
    // a Confident deny later in the list is not shadowed by an earlier
    // Ambiguous match.  After the loop, if we saw ambiguous but no confident,
    // return AskAnnotated — deny-side ambiguity is still concerning enough to
    // surface.
    let mut ambiguous_deny: Option<String> = None;
    for p in &allowlist.deny {
        match p.matches(event) {
            MatchResult::Confident => {
                return Decision::Deny {
                    reason: format!(
                        "settings.json deny-list rule {:?} matched",
                        p.raw()
                    ),
                };
            }
            MatchResult::Ambiguous => {
                ambiguous_deny.get_or_insert_with(|| p.raw().to_owned());
            }
            MatchResult::NoMatch => continue,
        }
    }
    if let Some(matched) = ambiguous_deny {
        return Decision::AskAnnotated {
            matched_pattern: matched,
            source: AllowOrDeny::Deny,
        };
    }

    // Step 3: allow patterns (only confident matches short-circuit).
    let mut ambiguous_allow: Option<String> = None;
    for p in &allowlist.allow {
        match p.matches(event) {
            MatchResult::Confident => {
                return Decision::Allow {
                    reason: format!(
                        "settings.json allow-list rule {:?} matched",
                        p.raw()
                    ),
                };
            }
            MatchResult::Ambiguous => {
                ambiguous_allow.get_or_insert_with(|| p.raw().to_owned());
            }
            MatchResult::NoMatch => continue,
        }
    }

    // Step 4: ambiguous allow with no confident match.
    if let Some(matched) = ambiguous_allow {
        return Decision::AskAnnotated {
            matched_pattern: matched,
            source: AllowOrDeny::Allow,
        };
    }

    // Step 5: nothing matched.
    Decision::Intercept
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ccbridge_proto::hook::{HookBase, PreToolUseEvent};
    use serde_json::json;

    fn pre_tool_use(tool: &str, mode: PermissionMode, input: serde_json::Value) -> PreToolUseEvent {
        PreToolUseEvent {
            base: HookBase {
                session_id: "sess".to_owned(),
                transcript_path: "/tmp/t.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: mode,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: input,
            tool_use_id: "toolu_test".to_owned(),
            agent_id: None,
            agent_type: None,
        }
    }

    fn empty() -> Allowlist { Allowlist::empty() }

    fn allowlist_with(allow: &[&str], deny: &[&str]) -> Allowlist {
        Allowlist {
            allow: allow.iter().map(|s| Pattern::parse(s)).collect(),
            deny:  deny.iter().map(|s| Pattern::parse(s)).collect(),
        }
    }

    // -----------------------------------------------------------------------
    // Phase 1: permission_mode short-circuit (unchanged from Phase 1 tests;
    // now also pass an empty allowlist to ensure mode still wins).
    // -----------------------------------------------------------------------

    #[test]
    fn bypass_permissions_allows() {
        let e = pre_tool_use("Bash", PermissionMode::BypassPermissions, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Allow { .. }));
    }

    #[test]
    fn auto_allows() {
        let e = pre_tool_use("Bash", PermissionMode::Auto, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Allow { .. }));
    }

    #[test]
    fn dont_ask_allows() {
        let e = pre_tool_use("Bash", PermissionMode::DontAsk, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Allow { .. }));
    }

    #[test]
    fn accept_edits_allows() {
        let e = pre_tool_use("Edit", PermissionMode::AcceptEdits, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Allow { .. }));
    }

    #[test]
    fn default_mode_intercepts() {
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Intercept));
    }

    #[test]
    fn plan_mode_intercepts() {
        let e = pre_tool_use("Bash", PermissionMode::Plan, json!({}));
        assert!(matches!(evaluate(&e, &empty()), Decision::Intercept));
    }

    #[test]
    fn allow_reason_includes_mode_name() {
        let e = pre_tool_use("Bash", PermissionMode::BypassPermissions, json!({}));
        match evaluate(&e, &empty()) {
            Decision::Allow { reason } => assert!(reason.contains("BypassPermissions")),
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: allowlist matching
    // -----------------------------------------------------------------------

    #[test]
    fn evaluate_bypass_mode_ignores_allowlist() {
        // Even with a deny pattern, bypass mode short-circuits to Allow first.
        let al = allowlist_with(&[], &["Bash"]);
        let e = pre_tool_use("Bash", PermissionMode::BypassPermissions, json!({}));
        assert!(matches!(evaluate(&e, &al), Decision::Allow { .. }));
    }

    #[test]
    fn evaluate_deny_match_wins() {
        let al = allowlist_with(&[], &["Bash"]);
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({}));
        assert!(matches!(evaluate(&e, &al), Decision::Deny { .. }));
    }

    #[test]
    fn evaluate_allow_match_returns_allow() {
        let al = allowlist_with(&["Skill"], &[]);
        let e = pre_tool_use("Skill", PermissionMode::Default, json!({}));
        assert!(matches!(evaluate(&e, &al), Decision::Allow { .. }));
    }

    #[test]
    fn evaluate_deny_beats_allow_when_both_match() {
        // Same tool in both lists — deny must win.
        let al = allowlist_with(&["Bash"], &["Bash"]);
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({}));
        assert!(
            matches!(evaluate(&e, &al), Decision::Deny { .. }),
            "deny must win over allow when both match"
        );
    }

    #[test]
    fn evaluate_ambiguous_deny_returns_ask_annotated_deny() {
        // "Bash(git status:*)" is a ToolWithArgs with a BashPrefix matcher.
        // For a Bash call with NO command field, the result is Ambiguous.
        let al = allowlist_with(&[], &["Bash(git status:*)"]);
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({})); // no command
        assert!(
            matches!(
                evaluate(&e, &al),
                Decision::AskAnnotated { source: AllowOrDeny::Deny, .. }
            ),
            "ambiguous deny must return AskAnnotated with source=Deny"
        );
    }

    #[test]
    fn evaluate_confident_deny_beats_ambiguous_deny() {
        // Regression test: ambiguous deny earlier in the list must NOT shadow a
        // Confident deny later in the list.  Without the accumulator fix, this
        // test returns AskAnnotated instead of Deny.
        let al = Allowlist {
            deny: vec![
                // Unparseable → Ambiguous when "Bash" appears in the raw string.
                Pattern::Unparseable { raw: "Bash[[invalid".to_owned() },
                // BashPrefix → Confident on `rm` commands.
                Pattern::parse("Bash(rm:*)"),
            ],
            allow: vec![],
        };
        let e = pre_tool_use(
            "Bash",
            PermissionMode::Default,
            json!({"command": "rm -rf /tmp/foo"}),
        );
        match evaluate(&e, &al) {
            Decision::Deny { reason } => {
                assert!(
                    reason.contains("Bash(rm:*)"),
                    "reason should name the Confident pattern, got: {reason:?}"
                );
            }
            other => panic!("expected Confident Deny, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_ambiguous_allow_returns_ask_annotated_allow() {
        // Agent(task-planner) where the event has NO subagent_type — NoMatch.
        // Use a pattern that truly produces Ambiguous: a bare "Agent" call with
        // an arg-matcher that has no command/file_path → use Bash with no command.
        let al = allowlist_with(&["Bash(git status:*)"], &[]);
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({})); // no command field
        assert!(
            matches!(
                evaluate(&e, &al),
                Decision::AskAnnotated { source: AllowOrDeny::Allow, .. }
            ),
            "ambiguous allow must return AskAnnotated with source=Allow"
        );
    }

    #[test]
    fn evaluate_confident_allow_beats_ambiguous_allow() {
        // Two allow patterns: ambiguous comes first in the list, then confident.
        // The function should scan all allows and return confident when found,
        // rather than stopping at the first ambiguous.
        let al = allowlist_with(&["Bash(git status:*)", "Skill"], &[]);
        // Bash event with no command → Bash pattern is Ambiguous, Skill is NoMatch.
        // Add Skill to allow list but fire a Skill event → Confident.
        let e_skill = pre_tool_use("Skill", PermissionMode::Default, json!({}));
        assert!(
            matches!(evaluate(&e_skill, &al), Decision::Allow { .. }),
            "confident allow must win over a previous ambiguous match"
        );
    }

    #[test]
    fn evaluate_no_match_returns_intercept() {
        let al = allowlist_with(&["Skill"], &["Read(**/.env*)"]);
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({"command": "echo hi"}));
        assert!(matches!(evaluate(&e, &al), Decision::Intercept));
    }

    #[test]
    fn evaluate_ambiguous_allow_returns_decision_with_pattern() {
        // The raw pattern string from settings.json must appear verbatim in the
        // AskAnnotated decision so the heartbeat and notify body can display it.
        let al = allowlist_with(&["Bash(git status:*)"], &[]);
        // Bash event with NO command field → Ambiguous.
        let e = pre_tool_use("Bash", PermissionMode::Default, json!({}));
        match evaluate(&e, &al) {
            Decision::AskAnnotated { ref matched_pattern, source } => {
                assert_eq!(
                    matched_pattern, "Bash(git status:*)",
                    "matched_pattern must be the raw settings.json string"
                );
                assert_eq!(source, AllowOrDeny::Allow);
            }
            other => panic!("expected AskAnnotated, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_real_world_skill_allows() {
        // Real-world settings: "Skill" in allow.
        let al = allowlist_with(
            &["Skill", "mcp__plugin_context7_context7__resolve-library-id",
              "mcp__plugin_context7_context7__query-docs",
              "mcp__plugin_backlog_tasks__*", "Agent(task-planner)"],
            &[],
        );
        let e = pre_tool_use("Skill", PermissionMode::Default, json!({}));
        assert!(matches!(evaluate(&e, &al), Decision::Allow { .. }));
    }

    #[test]
    fn evaluate_real_world_deny_dotenv_read() {
        // Real-world deny pattern: "Read(**/.env*)".
        let al = allowlist_with(
            &[],
            &["Read(**/.env*)"],
        );
        let e = pre_tool_use(
            "Read",
            PermissionMode::Default,
            json!({"file_path": "/home/user/.env.production"}),
        );
        assert!(matches!(evaluate(&e, &al), Decision::Deny { .. }));
    }

    // -----------------------------------------------------------------------
    // Hot-reload watcher tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn settings_watcher_picks_up_changes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("settings.json");

        // Initial: empty settings.
        std::fs::write(&path, r#"{}"#).unwrap();
        let allowlist = Arc::new(ArcSwap::new(Arc::new(Allowlist::empty())));
        let _handle = spawn_settings_watcher(path.clone(), allowlist.clone());

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(allowlist.load().allow.len(), 0, "initial allowlist must be empty");

        // Update file to add one allow pattern.
        std::fs::write(&path, r#"{"permissions":{"allow":["Skill"]}}"#).unwrap();

        // Poll until the watcher picks up the change (max 2s).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if allowlist.load().allow.len() == 1 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("settings watcher did not pick up the change within 2s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(allowlist.load().allow.len(), 1);
        assert_eq!(allowlist.load().allow[0].raw(), "Skill");
    }

    #[tokio::test]
    async fn settings_watcher_keeps_previous_on_malformed_json() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("settings.json");

        // Initial: valid settings with one allow pattern.
        std::fs::write(&path, r#"{"permissions":{"allow":["Skill"]}}"#).unwrap();
        let initial = Allowlist::from_path(&path).unwrap();
        let allowlist = Arc::new(ArcSwap::new(Arc::new(initial)));
        let _handle = spawn_settings_watcher(path.clone(), allowlist.clone());

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(allowlist.load().allow.len(), 1, "pre-condition: one allow pattern");

        // Overwrite with invalid JSON (simulates an editor mid-save).
        std::fs::write(&path, b"not valid json at all").unwrap();

        // Wait long enough for the watcher to see the change.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // The allowlist must still have the original pattern — never lose deny enforcement.
        assert_eq!(
            allowlist.load().allow.len(),
            1,
            "malformed settings.json must not clear the allowlist"
        );
    }
}
