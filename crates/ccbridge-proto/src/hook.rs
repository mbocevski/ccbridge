// SPDX-License-Identifier: MIT
//! Claude Code hook event shapes.
//!
//! Claude Code writes one JSON object to stdin for each hook invocation.
//! All variants share the base fields [`HookBase`]; the `hook_event_name`
//! field is used as the serde tag to pick the right variant.
//!
//! Reference: <https://code.claude.com/docs/en/hooks>
//!
//! # Hook stdin shapes
//!
//! ```json
//! // PreToolUse
//! {"session_id":"…","transcript_path":"…","cwd":"…","permission_mode":"default",
//!  "hook_event_name":"PreToolUse","tool_name":"Bash",
//!  "tool_input":{"command":"ls"},"tool_use_id":"toolu_01…"}
//!
//! // PostToolUse — adds tool_result
//! {"session_id":"…","hook_event_name":"PostToolUse","tool_name":"Bash",
//!  "tool_input":{"command":"ls"},"tool_use_id":"toolu_01…","tool_result":"…"}
//!
//! // Notification
//! {"session_id":"…","hook_event_name":"Notification",
//!  "notification_type":"permission_prompt","message":"…"}
//!
//! // Stop
//! {"session_id":"…","hook_event_name":"Stop","response":"…"}
//!
//! // SessionStart
//! {"session_id":"…","hook_event_name":"SessionStart","source":"startup","model":"…"}
//! ```

use alloc::string::String;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Permission mode (shared field)
// ---------------------------------------------------------------------------

/// The permission mode Claude Code is running under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionMode {
    Default,
    Plan,
    AcceptEdits,
    Auto,
    DontAsk,
    BypassPermissions,
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// Effort level (shared field on PreToolUse, PostToolUse, Stop)
// ---------------------------------------------------------------------------

/// Effort level hint from the model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffortLevel {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    #[serde(other)]
    Unknown,
}

/// Wrapper for the `effort` field: `{"level": "…"}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Effort {
    pub level: EffortLevel,
}

// ---------------------------------------------------------------------------
// SessionStart source
// ---------------------------------------------------------------------------

/// Why this session started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionSource {
    Startup,
    Resume,
    Clear,
    Compact,
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// Hook event enum — tagged by `hook_event_name`
// ---------------------------------------------------------------------------

/// A Claude Code hook event, as received on stdin.
///
/// Serialized with an internally-tagged `"hook_event_name"` discriminant so
/// callers can match:
///
/// ```text
/// let event: HookEvent = serde_json::from_str(&stdin_line)?;
/// match event {
///     HookEvent::PreToolUse(e) => { … }
///     HookEvent::Unknown => { /* forward-compat: log and skip */ }
///     _ => {}
/// }
/// ```
///
/// The `Unknown` unit variant catches any `hook_event_name` values not listed
/// here (e.g. new hooks added in future Claude Code versions).  serde's
/// internal-tag `#[serde(other)]` only works on unit variants; that is fine
/// because the daemon never needs to inspect the payload of an unknown event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "hook_event_name")]
pub enum HookEvent {
    PreToolUse(PreToolUseEvent),
    PostToolUse(PostToolUseEvent),
    Notification(NotificationEvent),
    Stop(StopEvent),
    SessionStart(SessionStartEvent),
    UserPromptSubmit(UserPromptSubmitEvent),
    SessionEnd(SessionEndEvent),
    /// Forward-compatibility catch-all: any unknown `hook_event_name` value.
    /// The daemon must log and skip these rather than crash.
    #[serde(other)]
    Unknown,
}

impl HookEvent {
    /// The session ID present on every known variant, or `""` for `Unknown`.
    pub fn session_id(&self) -> &str {
        match self {
            HookEvent::PreToolUse(e) => &e.base.session_id,
            HookEvent::PostToolUse(e) => &e.base.session_id,
            HookEvent::Notification(e) => &e.base.session_id,
            HookEvent::Stop(e) => &e.base.session_id,
            HookEvent::SessionStart(e) => &e.base.session_id,
            HookEvent::UserPromptSubmit(e) => &e.base.session_id,
            HookEvent::SessionEnd(e) => &e.base.session_id,
            HookEvent::Unknown => "",
        }
    }
}

// ---------------------------------------------------------------------------
// Shared base fields present on every hook event
// ---------------------------------------------------------------------------

/// Fields present on every hook event variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookBase {
    pub session_id: String,
    /// Absolute path to the session JSONL transcript file.
    pub transcript_path: String,
    /// Working directory of the Claude Code session.
    pub cwd: String,
}

// ---------------------------------------------------------------------------
// PreToolUse
// ---------------------------------------------------------------------------

/// Fires before a tool call executes; can allow, deny, or modify the input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreToolUseEvent {
    #[serde(flatten)]
    pub base: HookBase,

    pub permission_mode: PermissionMode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,

    /// Name of the tool being called (e.g. `"Bash"`, `"Edit"`, `"Write"`).
    pub tool_name: String,

    /// Tool-specific input object. Kept as [`Value`] because the schema
    /// differs per tool and callers that care can deserialize further.
    pub tool_input: Value,

    /// Stable ID for this tool call within the session.
    pub tool_use_id: String,

    /// Present when fired from within a sub-agent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
}

// ---------------------------------------------------------------------------
// PostToolUse
// ---------------------------------------------------------------------------

/// Fires after a tool call completes successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostToolUseEvent {
    #[serde(flatten)]
    pub base: HookBase,

    pub permission_mode: PermissionMode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,

    pub tool_name: String,
    pub tool_input: Value,
    pub tool_use_id: String,

    /// The output produced by the tool (string or structured object).
    ///
    /// Per Claude Code docs, `tool_result` is **optional**: it is absent when
    /// the tool call errored out before producing output, or for tools that
    /// have no output.  Using `Option<Value>` with `#[serde(default)]` ensures
    /// these events are parsed correctly instead of silently dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_result: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
}

// ---------------------------------------------------------------------------
// Notification
// ---------------------------------------------------------------------------

/// Fires when Claude Code emits a notification (permission prompt, idle, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEvent {
    #[serde(flatten)]
    pub base: HookBase,

    /// One of: `permission_prompt`, `idle_prompt`, `auth_success`,
    /// `elicitation_dialog`, `elicitation_complete`, `elicitation_response`.
    pub notification_type: String,

    pub message: String,
}

// ---------------------------------------------------------------------------
// Stop
// ---------------------------------------------------------------------------

/// Fires when Claude finishes its response turn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopEvent {
    #[serde(flatten)]
    pub base: HookBase,

    pub permission_mode: PermissionMode,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,

    /// The full text of Claude's response.
    ///
    /// Per Claude Code's observed behavior, `response` is sometimes absent (e.g.
    /// when the session stops with no assistant turn, or on timeout stops).
    /// `serde(default)` treats a missing field as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
}

// ---------------------------------------------------------------------------
// SessionStart
// ---------------------------------------------------------------------------

/// Fires when a session begins or resumes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStartEvent {
    #[serde(flatten)]
    pub base: HookBase,

    pub source: SessionSource,

    pub model: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
}

// ---------------------------------------------------------------------------
// UserPromptSubmit
// ---------------------------------------------------------------------------

/// Fires when the user submits a prompt.  Can inject additional context or
/// block submission (exit 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPromptSubmitEvent {
    #[serde(flatten)]
    pub base: HookBase,

    pub permission_mode: PermissionMode,

    /// The text the user submitted.
    pub prompt: String,
}

// ---------------------------------------------------------------------------
// SessionEnd
// ---------------------------------------------------------------------------

/// Fires when a session ends.  Non-blocking — cannot prevent session end.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEndEvent {
    #[serde(flatten)]
    pub base: HookBase,

    /// Why the session ended.  Known values: `clear`, `resume`, `logout`,
    /// `prompt_input_exit`, `bypass_permissions_disabled`, `other`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Hook stdout response shapes
// ---------------------------------------------------------------------------

/// What ccbridge-hook writes to stdout for a `PreToolUse` event.
///
/// The outer `hookSpecificOutput` wrapper is required by Claude Code.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreToolUseResponse {
    pub hook_specific_output: PreToolUseOutput,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreToolUseOutput {
    /// Must equal `"PreToolUse"`.
    pub hook_event_name: String,

    pub permission_decision: PermissionDecision,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_decision_reason: Option<String>,

    /// Optional modified tool input (allow the call but change the args).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_input: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<String>,
}

/// Decision the hook returns for a `PreToolUse` event.
///
/// Values documented at <https://code.claude.com/docs/en/hooks>:
/// - `allow` — approve this tool call
/// - `deny`  — reject this tool call
/// - `ask`   — surface a prompt to the user
///
/// **`passthrough` / "defer" is NOT a decision value.** When the daemon wants
/// to fall back to Claude Code's own TUI prompt it exits 0 with no stdout
/// (i.e. no `hookSpecificOutput` at all).  That is handled in the hook binary
/// logic, not via this enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionDecision {
    Allow,
    Deny,
    Ask,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_pre_tool_use() {
        let raw = json!({
            "session_id": "sess_01",
            "transcript_path": "/home/user/.claude/projects/foo/sess_01.jsonl",
            "cwd": "/home/user/dev/foo",
            "permission_mode": "default",
            "hook_event_name": "PreToolUse",
            "effort": {"level": "medium"},
            "tool_name": "Bash",
            "tool_input": {"command": "ls -la"},
            "tool_use_id": "toolu_01abc"
        });
        let evt: HookEvent = serde_json::from_value(raw.clone()).unwrap();
        match &evt {
            HookEvent::PreToolUse(e) => {
                assert_eq!(e.base.session_id, "sess_01");
                assert_eq!(e.tool_name, "Bash");
                assert_eq!(e.tool_use_id, "toolu_01abc");
            }
            _ => panic!("wrong variant"),
        }
        // Round-trip: re-serialise must contain hook_event_name tag
        let reserialised = serde_json::to_value(&evt).unwrap();
        assert_eq!(reserialised["hook_event_name"], "PreToolUse");
        assert_eq!(reserialised["tool_name"], "Bash");
    }

    #[test]
    fn round_trip_post_tool_use() {
        let raw = json!({
            "session_id": "sess_02",
            "transcript_path": "/tmp/sess_02.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "PostToolUse",
            "tool_name": "Read",
            "tool_input": {"file_path": "/tmp/foo.txt"},
            "tool_use_id": "toolu_02",
            "tool_result": "file contents here"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        assert!(matches!(evt, HookEvent::PostToolUse(_)));
        assert_eq!(evt.session_id(), "sess_02");
    }

    #[test]
    fn round_trip_notification() {
        let raw = json!({
            "session_id": "sess_03",
            "transcript_path": "/tmp/sess_03.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "Notification",
            "notification_type": "permission_prompt",
            "message": "Bash wants to run: rm -rf /tmp/foo"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        match &evt {
            HookEvent::Notification(e) => {
                assert_eq!(e.notification_type, "permission_prompt");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_stop() {
        let raw = json!({
            "session_id": "sess_04",
            "transcript_path": "/tmp/sess_04.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "Stop",
            "response": "Done!"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        match &evt {
            HookEvent::Stop(e) => {
                assert_eq!(e.response.as_deref(), Some("Done!"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_stop_without_response() {
        // Real Claude Code sessions emit Stop events without a `response` field
        // (e.g. timeout stops or sessions that end with no assistant turn).
        // This must parse successfully — same pattern as PostToolUse.tool_result.
        let raw = json!({
            "session_id": "sess_04b",
            "transcript_path": "/tmp/sess_04b.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "Stop"
            // response intentionally absent
        });
        let evt: HookEvent =
            serde_json::from_value(raw).expect("Stop without response must parse successfully");
        match &evt {
            HookEvent::Stop(e) => {
                assert!(e.response.is_none(), "absent response must be None");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_session_start() {
        let raw = json!({
            "session_id": "sess_05",
            "transcript_path": "/tmp/sess_05.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "SessionStart",
            "source": "startup",
            "model": "claude-opus-4-7"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        match &evt {
            HookEvent::SessionStart(e) => {
                assert_eq!(e.source, SessionSource::Startup);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pre_tool_use_response_serialises() {
        let resp = PreToolUseResponse {
            hook_specific_output: PreToolUseOutput {
                hook_event_name: "PreToolUse".into(),
                permission_decision: PermissionDecision::Allow,
                permission_decision_reason: None,
                updated_input: None,
                additional_context: None,
            },
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["hookSpecificOutput"]["permissionDecision"], "allow");
        // Defer is gone — only allow|deny|ask are valid
        let decisions = ["allow", "deny", "ask"];
        for d in decisions {
            let pd: PermissionDecision = serde_json::from_str(&format!("\"{}\"", d)).unwrap();
            assert_eq!(serde_json::to_value(&pd).unwrap(), d);
        }
    }

    #[test]
    fn round_trip_user_prompt_submit() {
        let raw = json!({
            "session_id": "sess_06",
            "transcript_path": "/tmp/sess_06.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "UserPromptSubmit",
            "prompt": "Write a factorial function"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        match &evt {
            HookEvent::UserPromptSubmit(e) => {
                assert_eq!(e.prompt, "Write a factorial function");
                assert_eq!(e.base.session_id, "sess_06");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn round_trip_session_end() {
        let raw = json!({
            "session_id": "sess_07",
            "transcript_path": "/tmp/sess_07.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "SessionEnd",
            "reason": "logout"
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        match &evt {
            HookEvent::SessionEnd(e) => {
                assert_eq!(e.reason.as_deref(), Some("logout"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn post_tool_use_without_tool_result_parses_ok() {
        // tool_result is optional: absent when the tool errored before producing
        // output or for tools with no output.  This must NOT return a parse error.
        let raw = json!({
            "session_id": "sess_pt",
            "transcript_path": "/tmp/sess_pt.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "PostToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "exit 1"},
            "tool_use_id": "toolu_err_01"
            // tool_result intentionally absent
        });
        let evt: HookEvent = serde_json::from_value(raw)
            .expect("PostToolUse without tool_result must parse successfully");
        match &evt {
            HookEvent::PostToolUse(e) => {
                assert!(
                    e.tool_result.is_none(),
                    "tool_result should be None when absent"
                );
                assert_eq!(e.tool_name, "Bash");
            }
            _ => panic!("wrong variant"),
        }
        // With tool_result present it should still parse as Some.
        let with_result = json!({
            "session_id": "sess_pt2",
            "transcript_path": "/tmp/sess_pt2.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "PostToolUse",
            "tool_name": "Read",
            "tool_input": {"file_path": "/tmp/foo.txt"},
            "tool_use_id": "toolu_ok_01",
            "tool_result": "contents"
        });
        let evt2: HookEvent = serde_json::from_value(with_result).unwrap();
        match evt2 {
            HookEvent::PostToolUse(e) => assert!(e.tool_result.is_some()),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn unknown_event_name_deserialises_to_unknown_variant() {
        // Future Claude Code versions may add new hook events. We must not crash.
        let raw = json!({
            "session_id": "sess_08",
            "transcript_path": "/tmp/sess_08.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "PreCompact",
            "some_future_field": 42
        });
        let evt: HookEvent = serde_json::from_value(raw).unwrap();
        assert!(
            matches!(evt, HookEvent::Unknown),
            "expected Unknown variant for unrecognised hook_event_name"
        );
        assert_eq!(evt.session_id(), "");
    }
}
