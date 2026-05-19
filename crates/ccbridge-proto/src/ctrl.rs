// SPDX-License-Identifier: MIT
//! Control-socket protocol types.
//!
//! `$XDG_RUNTIME_DIR/ccbridge/ctrl.sock` is the stable, bidirectional
//! interface for any client that wants to read ccbridge state or send commands.
//! It is **the** integration point for future TUI/GUI/CLI work.
//!
//! **Framing:** newline-delimited JSON (one object per line, UTF-8).
//! Mirrors the BLE NUS wire format where message types overlap, so a future
//! TUI can share parsers with ESP32 firmware.
//!
//! # Connection lifecycle
//!
//! ```text
//! client connects
//!   ← server: {"hello": {"version": 1, "owner": "Felix", "time": [epoch, tz]}}
//!   ← server: {<full heartbeat snapshot>}    (current state, immediately)
//!   → client: {"cmd": "subscribe", "topics": ["heartbeat", "turn"]}
//!   ← server: {"ack": "subscribe", "ok": true}
//!   ← server: {<heartbeat>}                   (streamed on change + keepalive)
//!   ← server: {"evt": "turn", …}              (streamed as turns complete)
//!   → client: {"cmd": "permission", "id": "req_abc", "decision": "once"}
//!   ← server: {"ack": "permission", "ok": true}
//! client closes
//! ```
//!
//! # Versioning
//!
//! `hello.version` increments on backwards-incompatible changes.  Additive
//! changes (new topics, new commands, new heartbeat fields) do NOT bump the
//! version.  Clients must ignore unknown fields and unknown event types.
//!
//! # Topics
//!
//! | Topic | Stream |
//! |---|---|
//! | `heartbeat` | Full snapshot on every state change + 10s keepalive |
//! | `turn` | Assistant turn events (same shape as buddy's `turn` event) |
//! | `log` | Daemon-internal debug events (errors, hook ingest, BLE pair/unpair) |
//!
//! A client that doesn't subscribe still receives the initial `hello` and
//! heartbeat snapshot, then nothing further — useful for one-shot status
//! queries (waybar `custom/exec` style).

use serde::{Deserialize, Serialize};

use crate::buddy::WireDecision;

// ---------------------------------------------------------------------------
// Hello (server → client, sent immediately on connect)
// ---------------------------------------------------------------------------

/// Sent by the server to every client immediately on connect, before the
/// initial heartbeat snapshot.
///
/// Wire shape: `{"hello": {"version": 1, "owner": "Felix", "time": [epoch, tz]}}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloMessage {
    pub hello: Hello,
}

/// Payload of the `hello` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// Protocol version.  Currently `1`.  Bumps on backwards-incompatible changes.
    pub version: u32,
    /// Owner name (from `git config user.name` or `$USER`).
    pub owner: String,
    /// `[epoch_seconds, tz_offset_seconds]` — same shape as [`crate::buddy::TimeSync`].
    pub time: (i64, i32),
}

// ---------------------------------------------------------------------------
// Topic enum
// ---------------------------------------------------------------------------

/// A subscription topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Topic {
    /// Full heartbeat snapshot on every state change + 10s keepalive.
    Heartbeat,
    /// One-shot assistant turn events.
    Turn,
    /// Daemon-internal debug events (off by default).
    Log,
}

// ---------------------------------------------------------------------------
// Command (client → server)
// ---------------------------------------------------------------------------

/// A command sent from a control-socket client to the daemon.
///
/// Tagged by `"cmd"` field.  Every command receives a matching [`Ack`].
/// Unknown commands receive `{"ack":"<cmd>","ok":false,"error":"unknown_command"}`
/// rather than closing the connection — forward compatibility is the goal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Add topics to the subscription set (idempotent).
    ///
    /// Wire shape: `{"cmd":"subscribe","topics":["heartbeat","turn"]}`
    Subscribe { topics: Vec<Topic> },

    /// Remove topics from the subscription set.
    ///
    /// Wire shape: `{"cmd":"unsubscribe","topics":["log"]}`
    Unsubscribe { topics: Vec<Topic> },

    /// Approve or deny a pending tool-call permission prompt.
    ///
    /// Wire shape: `{"cmd":"permission","id":"req_abc","decision":"once"}`
    ///
    /// Reuses [`WireDecision`] so the control socket and BLE path share the
    /// same wire bytes and parser.
    Permission {
        /// Must match the `prompt.id` in the current heartbeat exactly.
        id: String,
        decision: WireDecision,
    },

    /// Request the buddy-style status payload.
    ///
    /// Wire shape: `{"cmd":"status"}`
    Status,

    /// Request the last N heartbeat snapshots (debug front-ends).
    ///
    /// Wire shape: `{"cmd":"replay","n":50}`
    Replay {
        /// Number of recent heartbeats to replay. Clamped to available history.
        n: u32,
    },

    /// Drop a BLE bond by device MAC address.
    ///
    /// Wire shape: `{"cmd":"forget_device","addr":"AA:BB:CC:DD:EE:FF"}`
    ForgetDevice { addr: String },

    /// Inject a synthetic hook event (dev-only, gated by `config allow_simulate`).
    ///
    /// Wire shape: `{"cmd":"simulate","event":{…hook event…}}`
    Simulate { event: serde_json::Value },
}

// ---------------------------------------------------------------------------
// Ack (server → client)
// ---------------------------------------------------------------------------

/// Generic ack sent by the server in response to every [`Command`].
///
/// Wire shape: `{"ack":"subscribe","ok":true}` or
/// `{"ack":"status","ok":false,"error":"not_ready"}`.
///
/// Mirrors the BLE convention ([`crate::buddy::DeviceAck`]) so a future TUI
/// can share parsing logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    /// Echoes the `cmd` value of the command being acknowledged.
    pub ack: String,
    pub ok: bool,
    /// Human-readable error description when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Ack {
    /// Successful ack for `cmd`.
    pub fn ok(cmd: impl Into<String>) -> Self {
        Self {
            ack: cmd.into(),
            ok: true,
            error: None,
        }
    }

    /// Error ack for `cmd`.
    pub fn err(cmd: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            ack: cmd.into(),
            ok: false,
            error: Some(error.into()),
        }
    }

    /// Standard ack for an unrecognised command.
    pub fn unknown(cmd: impl Into<String>) -> Self {
        Self::err(cmd, "unknown_command")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // Hello
    // -----------------------------------------------------------------------

    #[test]
    fn hello_round_trip() {
        let raw = json!({
            "hello": {
                "version": 1,
                "owner": "Felix",
                "time": [1_775_731_234_i64, -25200]
            }
        });
        let msg: HelloMessage = serde_json::from_value(raw).unwrap();
        assert_eq!(msg.hello.version, 1);
        assert_eq!(msg.hello.owner, "Felix");
        assert_eq!(msg.hello.time.0, 1_775_731_234);
        assert_eq!(msg.hello.time.1, -25200);

        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v["hello"]["version"], 1);
        assert_eq!(v["hello"]["owner"], "Felix");
        assert_eq!(v["hello"]["time"][1], -25200);
    }

    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    #[test]
    fn subscribe_round_trip() {
        let raw = json!({"cmd": "subscribe", "topics": ["heartbeat", "turn"]});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::Subscribe { topics } => {
                assert_eq!(topics.len(), 2);
                assert!(topics.contains(&Topic::Heartbeat));
                assert!(topics.contains(&Topic::Turn));
            }
            _ => panic!("wrong variant"),
        }
        // re-serialise
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(v["cmd"], "subscribe");
    }

    #[test]
    fn unsubscribe_round_trip() {
        let raw = json!({"cmd": "unsubscribe", "topics": ["log"]});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::Unsubscribe { topics } => {
                assert_eq!(topics, &[Topic::Log]);
            }
            _ => panic!("wrong variant"),
        }
    }

    // -----------------------------------------------------------------------
    // Permission — reuses WireDecision
    // -----------------------------------------------------------------------

    #[test]
    fn permission_once_round_trip() {
        let raw = json!({"cmd": "permission", "id": "req_abc", "decision": "once"});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::Permission { id, decision } => {
                assert_eq!(id, "req_abc");
                assert_eq!(*decision, WireDecision::Once);
            }
            _ => panic!("wrong variant"),
        }
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(v["cmd"], "permission");
        assert_eq!(v["decision"], "once");
    }

    #[test]
    fn permission_deny_round_trip() {
        let raw = json!({"cmd": "permission", "id": "req_xyz", "decision": "deny"});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        assert!(matches!(
            cmd,
            Command::Permission {
                decision: WireDecision::Deny,
                ..
            }
        ));
    }

    // -----------------------------------------------------------------------
    // Status (unit variant)
    // -----------------------------------------------------------------------

    #[test]
    fn status_round_trip() {
        let raw = json!({"cmd": "status"});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        assert!(matches!(cmd, Command::Status));
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(v["cmd"], "status");
    }

    // -----------------------------------------------------------------------
    // Replay
    // -----------------------------------------------------------------------

    #[test]
    fn replay_round_trip() {
        let raw = json!({"cmd": "replay", "n": 50});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::Replay { n } => assert_eq!(*n, 50),
            _ => panic!("wrong variant"),
        }
    }

    // -----------------------------------------------------------------------
    // ForgetDevice
    // -----------------------------------------------------------------------

    #[test]
    fn forget_device_round_trip() {
        let raw = json!({"cmd": "forget_device", "addr": "AA:BB:CC:DD:EE:FF"});
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::ForgetDevice { addr } => {
                assert_eq!(addr, "AA:BB:CC:DD:EE:FF");
            }
            _ => panic!("wrong variant"),
        }
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(v["cmd"], "forget_device");
    }

    // -----------------------------------------------------------------------
    // Simulate
    // -----------------------------------------------------------------------

    #[test]
    fn simulate_round_trip() {
        let raw = json!({
            "cmd": "simulate",
            "event": {
                "hook_event_name": "PreToolUse",
                "session_id": "fake_session",
                "transcript_path": "/tmp/fake.jsonl",
                "cwd": "/tmp",
                "permission_mode": "default",
                "tool_name": "Bash",
                "tool_input": {"command": "echo hello"},
                "tool_use_id": "toolu_fake"
            }
        });
        let cmd: Command = serde_json::from_value(raw).unwrap();
        match &cmd {
            Command::Simulate { event } => {
                assert_eq!(event["hook_event_name"], "PreToolUse");
            }
            _ => panic!("wrong variant"),
        }
    }

    // -----------------------------------------------------------------------
    // Ack helpers
    // -----------------------------------------------------------------------

    #[test]
    fn ack_ok_serialises() {
        let ack = Ack::ok("subscribe");
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["ack"], "subscribe");
        assert_eq!(v["ok"], true);
        assert!(v.get("error").is_none());
    }

    #[test]
    fn ack_err_serialises() {
        let ack = Ack::err("permission", "no_pending_prompt");
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "no_pending_prompt");
    }

    #[test]
    fn ack_unknown_command() {
        let ack = Ack::unknown("future_cmd");
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["ack"], "future_cmd");
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "unknown_command");
    }
}
