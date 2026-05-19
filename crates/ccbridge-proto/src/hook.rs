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
/// ```rust,ignore
/// let event: HookEvent = serde_json::from_str(&stdin_line)?;
/// match event {
///     HookEvent::PreToolUse(e) => { … }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "hook_event_name")]
pub enum HookEvent {
    PreToolUse(PreToolUseEvent),
    PostToolUse(PostToolUseEvent),
    Notification(NotificationEvent),
    Stop(StopEvent),
    SessionStart(SessionStartEvent),
}

impl HookEvent {
    /// The session ID present on every variant.
    pub fn session_id(&self) -> &str {
        match self {
            HookEvent::PreToolUse(e) => &e.base.session_id,
            HookEvent::PostToolUse(e) => &e.base.session_id,
            HookEvent::Notification(e) => &e.base.session_id,
            HookEvent::Stop(e) => &e.base.session_id,
            HookEvent::SessionStart(e) => &e.base.session_id,
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
    pub tool_result: Value,

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
    pub response: String,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionDecision {
    Allow,
    Deny,
    Ask,
    Defer,
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
        assert!(matches!(evt, HookEvent::Stop(_)));
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
    }
}
