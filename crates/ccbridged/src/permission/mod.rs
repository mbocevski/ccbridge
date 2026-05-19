//! Permission evaluator — decides what to do with a `PreToolUse` event
//! before the aggregator registers an approval.
//!
//! # Phase 1 scope
//!
//! This module currently implements only the `permission_mode` short-circuit
//! (Bug E).  The allowlist matcher (Phase 2) will extend `evaluate()` to also
//! consult `permissions.allow` / `permissions.deny` from `settings.json`.
//!
//! # How it fits in
//!
//! `Aggregator::handle_hook_event` calls `evaluate()` for every `PreToolUse`.
//! Based on the returned [`Decision`], it either:
//! - Short-circuits with an immediate allow/deny (no swaync, no oneshot),
//! - Or calls `start_intercept()` for the normal hold-and-wait flow.
//!
//! The `AskAnnotated` variant is reserved for Phase 2 (ambiguous allowlist
//! matches) and is never returned by the Phase 1 evaluator.

use ccbridge_proto::hook::{PermissionMode, PreToolUseEvent};

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
    /// Produced by deny-list pattern matches (Phase 2+) or by explicit user
    /// denies that are forwarded back through the oneshot as
    /// [`HookResponse::HardDeny`](crate::state::HookResponse::HardDeny).
    Deny { reason: String },

    /// A pattern was found in the allowlist but cannot be fully evaluated.
    ///
    /// **Phase 1: never returned.**  The evaluator will return this once the
    /// allowlist matcher (Phase 2) is wired up.
    AskAnnotated {
        matched_pattern: String,
        source: AllowOrDeny,
    },

    /// No confident decision.  Use the normal hold-for-approval flow
    /// (register a pending approval, broadcast heartbeat, wait for a decision
    /// from swaync / ctrl / BLE).
    Intercept,
}

/// Which side of the allowlist a pattern came from (for annotations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowOrDeny {
    Allow,
    Deny,
}

// ---------------------------------------------------------------------------
// evaluate()
// ---------------------------------------------------------------------------

/// Evaluate a `PreToolUse` event and return the permission decision.
///
/// **Phase 1** checks only `permission_mode`.  When the user is in a
/// permissive mode (`acceptEdits`, `auto`, `dontAsk`, `bypassPermissions`)
/// Claude Code would not have prompted at all, so ccbridge short-circuits to
/// [`Decision::Allow`] without surfacing a notification.
///
/// `plan` is intentionally **not** in the permissive set — it is a
/// *restrictive* (read-only) mode, not a permissive one.
///
/// **Phase 2** will extend this to also consult `permissions.allow` /
/// `permissions.deny` from `~/.claude/settings.json`.
pub fn evaluate(event: &PreToolUseEvent) -> Decision {
    // Step 1: permission_mode short-circuit.
    match event.permission_mode {
        PermissionMode::AcceptEdits
        | PermissionMode::Auto
        | PermissionMode::DontAsk
        | PermissionMode::BypassPermissions => {
            return Decision::Allow {
                reason: format!("permission_mode={:?}", event.permission_mode),
            };
        }
        // default, plan, Unknown — fall through to intercept.
        _ => {}
    }

    // Phase 2 will insert allowlist matching here before returning Intercept.
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

    fn pre_tool_use(tool: &str, permission_mode: PermissionMode) -> PreToolUseEvent {
        PreToolUseEvent {
            base: HookBase {
                session_id: "sess".to_owned(),
                transcript_path: "/tmp/t.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: json!({"command": "echo test"}),
            tool_use_id: "toolu_test".to_owned(),
            agent_id: None,
            agent_type: None,
        }
    }

    #[test]
    fn bypass_permissions_allows() {
        let e = pre_tool_use("Bash", PermissionMode::BypassPermissions);
        assert!(
            matches!(evaluate(&e), Decision::Allow { .. }),
            "bypassPermissions must auto-allow"
        );
    }

    #[test]
    fn auto_allows() {
        let e = pre_tool_use("Bash", PermissionMode::Auto);
        assert!(matches!(evaluate(&e), Decision::Allow { .. }));
    }

    #[test]
    fn dont_ask_allows() {
        let e = pre_tool_use("Bash", PermissionMode::DontAsk);
        assert!(matches!(evaluate(&e), Decision::Allow { .. }));
    }

    #[test]
    fn accept_edits_allows() {
        let e = pre_tool_use("Edit", PermissionMode::AcceptEdits);
        assert!(matches!(evaluate(&e), Decision::Allow { .. }));
    }

    #[test]
    fn default_mode_intercepts() {
        let e = pre_tool_use("Bash", PermissionMode::Default);
        assert!(
            matches!(evaluate(&e), Decision::Intercept),
            "default mode must use the normal intercept flow"
        );
    }

    #[test]
    fn plan_mode_intercepts() {
        // plan is read-only-restrictive, NOT permissive — must NOT auto-allow.
        let e = pre_tool_use("Bash", PermissionMode::Plan);
        assert!(
            matches!(evaluate(&e), Decision::Intercept),
            "plan mode must NOT auto-allow (it is more restrictive, not less)"
        );
    }

    #[test]
    fn allow_reason_includes_mode_name() {
        let e = pre_tool_use("Bash", PermissionMode::BypassPermissions);
        match evaluate(&e) {
            Decision::Allow { reason } => {
                assert!(
                    reason.contains("BypassPermissions"),
                    "reason should name the mode, got: {reason:?}"
                );
            }
            other => panic!("expected Allow, got {other:?}"),
        }
    }
}
