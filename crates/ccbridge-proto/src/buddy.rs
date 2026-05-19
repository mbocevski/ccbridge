//! BLE Hardware Buddy wire-protocol types.
//!
//! Exact JSON shapes from `~/dev/claude-desktop-buddy/REFERENCE.md`.
//! ccbridged emits these over the Nordic UART Service TX characteristic
//! (and identically over the control socket — types are shared).
//!
//! # Wire format
//!
//! Everything on the wire is UTF-8 JSON, one object per line, terminated
//! with `\n`.  Lines that would exceed 4 KB (UTF-8 bytes) are dropped.
//!
//! ## Desktop → device (emitted by ccbridged)
//!
//! * [`Heartbeat`]  — snapshot on every state change + 10s keepalive
//! * [`TurnEvent`]  — one-shot on each completed assistant turn
//! * [`TimeSync`]   — one-shot on connect: `{"time": [epoch, tz_offset]}`
//! * [`OwnerMessage`] — one-shot on connect: `{"cmd":"owner","name":"…"}`
//!
//! ## Device → desktop (received by ccbridged from BLE / ctrl socket)
//!
//! * [`DeviceCommand`] — permission decision, status poll, name, unpair
//! * [`DeviceAck`]     — ack for any command the desktop sent
//! * [`StatusAck`]     — special ack for `{"cmd":"status"}`

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Heartbeat snapshot (desktop → device)
// ---------------------------------------------------------------------------

/// Periodic snapshot emitted on every state change and every 10 seconds.
///
/// Wire shape:
/// ```json
/// {
///   "total": 3, "running": 1, "waiting": 1,
///   "msg": "approve: Bash",
///   "entries": ["10:42 git push", "10:41 yarn test"],
///   "tokens": 184502, "tokens_today": 31200,
///   "prompt": {"id": "req_abc123", "tool": "Bash", "hint": "rm -rf /tmp/foo"}
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Total number of open sessions.
    pub total: u32,
    /// Sessions actively generating output.
    pub running: u32,
    /// Sessions blocked on a permission prompt.
    pub waiting: u32,
    /// One-line summary for a small display.
    pub msg: String,
    /// Recent transcript lines, newest first (capped to a few entries).
    pub entries: Vec<String>,
    /// Cumulative output tokens since the daemon started.
    pub tokens: u64,
    /// Output tokens since local midnight (persisted across restarts).
    pub tokens_today: u64,
    /// Present only when a permission decision is pending.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<PromptInfo>,
}

/// Pending permission prompt embedded in [`Heartbeat`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptInfo {
    /// Opaque request ID — must be echoed back in the permission decision.
    pub id: String,
    /// Name of the tool requesting permission (e.g. `"Bash"`).
    pub tool: String,
    /// Short hint showing what the tool intends to do.
    pub hint: String,
}

// ---------------------------------------------------------------------------
// Turn event (desktop → device, one-shot per completed assistant turn)
// ---------------------------------------------------------------------------

/// Fires once per completed assistant turn, carrying the raw SDK content array.
///
/// Wire shape:
/// ```json
/// {"evt": "turn", "role": "assistant", "content": [{"type":"text","text":"…"}]}
/// ```
///
/// Events larger than 4 KB (UTF-8) are dropped before transmission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnEvent {
    /// Always `"turn"`.
    pub evt: String,
    /// Always `"assistant"` for now.
    pub role: String,
    /// Raw SDK content array — text blocks, tool calls, etc.
    pub content: Vec<Value>,
}

impl TurnEvent {
    /// Construct a new turn event with `evt = "turn"`.
    pub fn new(role: impl Into<String>, content: Vec<Value>) -> Self {
        Self {
            evt: "turn".into(),
            role: role.into(),
            content,
        }
    }
}

// ---------------------------------------------------------------------------
// One-shot on-connect messages (desktop → device)
// ---------------------------------------------------------------------------

/// Time sync sent once on connect.
///
/// Wire shape: `{"time": [<epoch_secs>, <tz_offset_secs>]}`
///
/// `tz_offset_secs` is the local timezone offset in seconds west of UTC
/// (e.g. UTC-7 = -25200).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSync {
    /// `[epoch_seconds, tz_offset_seconds]`
    pub time: (i64, i32),
}

/// Owner name sent once on connect (and also a device → desktop command).
///
/// Wire shape: `{"cmd": "owner", "name": "Felix"}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnerMessage {
    /// Always `"owner"`.
    pub cmd: String,
    pub name: String,
}

impl OwnerMessage {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            cmd: "owner".into(),
            name: name.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Permission decision (device → desktop)
// ---------------------------------------------------------------------------

/// A permission response sent from the device (or control socket) back to the
/// daemon.
///
/// Wire shape:
/// ```json
/// {"cmd":"permission","id":"req_abc123","decision":"once"}
/// {"cmd":"permission","id":"req_abc123","decision":"deny"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionCmd {
    /// Always `"permission"`.
    pub cmd: String,
    /// Must match [`PromptInfo::id`] exactly.
    pub id: String,
    pub decision: WireDecision,
}

/// The two permission decisions a BLE device (or control-socket client) can send.
///
/// Named `WireDecision` (not `PermissionDecision`) to avoid confusion with
/// [`hook::PermissionDecision`], which carries the hook-stdout values
/// `allow|deny|ask` — a different semantic domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireDecision {
    /// Approve this tool call once.
    Once,
    /// Deny this tool call.
    Deny,
}

// ---------------------------------------------------------------------------
// Device commands (device → desktop)
// ---------------------------------------------------------------------------

/// Any command a BLE device (or control-socket client) can send.
///
/// Uses an internally-tagged `"cmd"` field.
///
/// Note: `{"cmd":"owner","name":"…"}` is **desktop → device only** — see
/// [`OwnerMessage`].  Devices never send an `owner` command back.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "lowercase")]
pub enum DeviceCommand {
    /// `{"cmd":"permission","id":"…","decision":"once"|"deny"}`
    Permission {
        id: String,
        decision: WireDecision,
    },
    /// `{"cmd":"status"}` — polls the daemon for a [`StatusAck`].
    Status,
    /// `{"cmd":"name","name":"Clawd"}` — sets the device's own display name.
    Name { name: String },
    /// `{"cmd":"unpair"}` — erase stored BLE bonds.
    Unpair,
}

// ---------------------------------------------------------------------------
// Generic ack (desktop → device, for most commands)
// ---------------------------------------------------------------------------

/// Generic ack sent by the desktop for every command it receives.
///
/// Wire shape: `{"ack":"<cmd>","ok":true,"n":0}`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceAck {
    /// Echoes the `cmd` field of the command being acked.
    pub ack: String,
    pub ok: bool,
    /// Generic counter (bytes written for chunk acks; 0 otherwise).
    #[serde(default)]
    pub n: u64,
    /// Human-readable error when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DeviceAck {
    pub fn ok(cmd: impl Into<String>) -> Self {
        Self {
            ack: cmd.into(),
            ok: true,
            n: 0,
            error: None,
        }
    }

    pub fn err(cmd: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            ack: cmd.into(),
            ok: false,
            n: 0,
            error: Some(error.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Status ack (desktop → device, in response to {"cmd":"status"})
// ---------------------------------------------------------------------------

/// Response to a `{"cmd":"status"}` poll.
///
/// Wire shape:
/// ```json
/// {
///   "ack": "status", "ok": true,
///   "data": {
///     "name": "Clawd", "sec": true,
///     "bat": {"pct": 87, "mV": 4012, "mA": -120, "usb": true},
///     "sys": {"up": 8412, "heap": 84200},
///     "stats": {"appr": 42, "deny": 3, "vel": 8, "nap": 12, "lvl": 5}
///   }
/// }
/// ```
///
/// All sub-fields are optional — omit what you don't have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusAck {
    /// Always `"status"`.
    pub ack: String,
    pub ok: bool,
    pub data: StatusData,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusData {
    /// Device display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `true` if the BLE link is LE Secure Connections encrypted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sec: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bat: Option<BatteryStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sys: Option<SysStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<DeviceStats>,
}

/// Battery status (all fields optional).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatteryStatus {
    /// Battery percentage (0–100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pct: Option<u8>,
    /// Battery voltage in millivolts.
    #[serde(rename = "mV", skip_serializing_if = "Option::is_none")]
    pub mv: Option<i32>,
    /// Current draw in milliamps (negative = charging).
    #[serde(rename = "mA", skip_serializing_if = "Option::is_none")]
    pub ma: Option<i32>,
    /// `true` if USB power is connected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usb: Option<bool>,
}

/// System status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SysStatus {
    /// Uptime in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub up: Option<u64>,
    /// Free heap in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heap: Option<u64>,
}

/// Lifetime device approval stats.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceStats {
    /// Total approvals granted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub appr: Option<u32>,
    /// Total denials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny: Option<u32>,
    /// Velocity (recent approvals per hour, device-defined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vel: Option<u32>,
    /// Nap count (device-defined idle metric).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nap: Option<u32>,
    /// Level (device-defined gamification counter).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lvl: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn heartbeat_no_prompt() {
        let raw = json!({
            "total": 2, "running": 1, "waiting": 0,
            "msg": "writing code",
            "entries": ["10:42 Edit foo.rs", "10:41 Bash: cargo check"],
            "tokens": 50000, "tokens_today": 12000
        });
        let hb: Heartbeat = serde_json::from_value(raw).unwrap();
        assert_eq!(hb.total, 2);
        assert!(hb.prompt.is_none());
        // tokens_today field name preserved on round-trip
        let v = serde_json::to_value(&hb).unwrap();
        assert_eq!(v["tokens_today"], 12000);
    }

    #[test]
    fn heartbeat_with_prompt() {
        let raw = json!({
            "total": 1, "running": 0, "waiting": 1,
            "msg": "approve: Bash",
            "entries": ["10:42 Bash: rm -rf /tmp/foo"],
            "tokens": 184502, "tokens_today": 31200,
            "prompt": {
                "id": "req_abc123",
                "tool": "Bash",
                "hint": "rm -rf /tmp/foo"
            }
        });
        let hb: Heartbeat = serde_json::from_value(raw).unwrap();
        let p = hb.prompt.unwrap();
        assert_eq!(p.id, "req_abc123");
        assert_eq!(p.tool, "Bash");
        assert_eq!(p.hint, "rm -rf /tmp/foo");
    }

    #[test]
    fn permission_cmd_once() {
        let raw = json!({"cmd":"permission","id":"req_abc123","decision":"once"});
        let cmd: DeviceCommand = serde_json::from_value(raw).unwrap();
        match cmd {
            DeviceCommand::Permission { id, decision } => {
                assert_eq!(id, "req_abc123");
                assert_eq!(decision, WireDecision::Once);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn permission_cmd_deny() {
        let raw = json!({"cmd":"permission","id":"req_xyz","decision":"deny"});
        let cmd: DeviceCommand = serde_json::from_value(raw).unwrap();
        match cmd {
            DeviceCommand::Permission { decision, .. } => {
                assert_eq!(decision, WireDecision::Deny);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn turn_event_round_trip() {
        let raw = json!({"evt":"turn","role":"assistant","content":[{"type":"text","text":"Hello"}]});
        let evt: TurnEvent = serde_json::from_value(raw).unwrap();
        assert_eq!(evt.evt, "turn");
        let v = serde_json::to_value(&evt).unwrap();
        assert_eq!(v["evt"], "turn");
        assert_eq!(v["role"], "assistant");
    }

    #[test]
    fn time_sync_round_trip() {
        let ts = TimeSync { time: (1_775_731_234, -25200) };
        let v = serde_json::to_value(&ts).unwrap();
        assert_eq!(v["time"][0], 1_775_731_234_i64);
        assert_eq!(v["time"][1], -25200);
    }

    #[test]
    fn status_ack_partial_fields() {
        // Device only sends name + sec; other fields absent
        let raw = json!({
            "ack": "status", "ok": true,
            "data": {"name": "Clawd", "sec": true}
        });
        let ack: StatusAck = serde_json::from_value(raw).unwrap();
        assert!(ack.ok);
        assert_eq!(ack.data.name.unwrap(), "Clawd");
        assert!(ack.data.bat.is_none());
    }

    #[test]
    fn device_ack_ok_helper() {
        let ack = DeviceAck::ok("permission");
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["ack"], "permission");
        assert_eq!(v["ok"], true);
        // error field omitted when None
        assert!(v.get("error").is_none());
    }
}
