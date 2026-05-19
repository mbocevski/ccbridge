// SPDX-License-Identifier: MIT
//! Integration tests for the hook ingest socket.
//!
//! Each test:
//! 1. Creates a `tempdir` and sets `XDG_RUNTIME_DIR` to point into it.
//! 2. Spawns the aggregator + hook ingest socket.
//! 3. Connects as a mock hook binary via `tokio::net::UnixStream`.
//! 4. Writes a hook-event JSON line and reads the response (if any).

use std::path::{Path, PathBuf};
use std::time::Duration;

use ccbridge_proto::buddy::WireDecision;
use ccbridge_proto::hook::{PermissionDecision, PreToolUseResponse};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use ccbridged::ingest::hooks as hook_ingest;
use ccbridged::state::{AggregatorMsg, DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator};

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

/// Set up a tempdir as a fake `XDG_RUNTIME_DIR/ccbridge/`, start the
/// aggregator and hook ingest socket.  Returns the tempdir (must stay alive
/// for the duration of the test) and the aggregator tx.
async fn setup(
    approval_timeout: Duration,
) -> (TempDir, ccbridged::state::AggregatorTx, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime_dir = dir.path().to_path_buf();

    // Create the ccbridge sub-directory that systemd would normally provision.
    std::fs::create_dir_all(runtime_dir.join("ccbridge")).expect("mkdir ccbridge");

    let (agg_tx, _hb_rx) = spawn_aggregator(approval_timeout, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));
    hook_ingest::spawn(runtime_dir.clone(), agg_tx.clone());

    // Give the accept loop a moment to bind.
    tokio::time::sleep(Duration::from_millis(10)).await;

    (dir, agg_tx, runtime_dir)
}

fn sock_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("ccbridge").join("hooks.sock")
}

/// Connect to the hook socket and return a split reader/writer.
async fn connect(runtime_dir: &Path) -> (
    BufReader<tokio::net::unix::OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let stream = UnixStream::connect(sock_path(runtime_dir))
        .await
        .expect("connect to hooks.sock");
    let (r, w) = stream.into_split();
    (BufReader::new(r), w)
}

/// Write a JSON value as a single line.
async fn send_line<T: serde::Serialize>(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    value: &T,
) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
}

/// Read one line from the reader (strips trailing `\n`).
async fn recv_line(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Option<String> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await.unwrap();
    if n == 0 {
        return None; // EOF — passthrough
    }
    Some(line.trim_end_matches('\n').trim_end_matches('\r').to_owned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Non-PreToolUse events receive no output (passthrough).
#[tokio::test]
async fn stop_event_passthrough() {
    let (_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let stop = json!({
        "session_id": "s1",
        "transcript_path": "/tmp/s1.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "Stop",
        "response": "all done"
    });
    send_line(&mut writer, &stop).await;
    drop(writer); // signal EOF on write side

    let line = recv_line(&mut reader).await;
    assert!(line.is_none(), "Stop should produce no output (passthrough)");
}

/// PreToolUse followed by a permission decision produces the right JSON.
#[tokio::test]
async fn pre_tool_use_allow_decision() {
    let (_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let pre_tool_use = json!({
        "session_id": "sess_allow",
        "transcript_path": "/tmp/sess_allow.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo hello"},
        "tool_use_id": "toolu_allow_001"
    });
    send_line(&mut writer, &pre_tool_use).await;

    // Give the aggregator time to register the pending approval.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Send the permission decision from a parallel "emit module".
    agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_allow_001".to_owned(),
            decision: WireDecision::Once,
        })
        .await
        .unwrap();

    // The hook connection should now receive the allow response.
    let line = recv_line(&mut reader).await.expect("expected JSON response");
    let resp: PreToolUseResponse = serde_json::from_str(&line).expect("parse response JSON");
    assert_eq!(
        resp.hook_specific_output.permission_decision,
        PermissionDecision::Allow,
    );
    // Allow carries no reason.
    assert!(
        resp.hook_specific_output.permission_decision_reason.is_none(),
        "Allow should carry no permissionDecisionReason"
    );
}

/// PreToolUse with a Deny decision.
#[tokio::test]
async fn pre_tool_use_deny_decision() {
    let (_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let pre_tool_use = json!({
        "session_id": "sess_deny",
        "transcript_path": "/tmp/sess_deny.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "rm -rf /important"},
        "tool_use_id": "toolu_deny_001"
    });
    send_line(&mut writer, &pre_tool_use).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_deny_001".to_owned(),
            decision: WireDecision::Deny,
        })
        .await
        .unwrap();

    let line = recv_line(&mut reader).await.expect("expected JSON response");
    let resp: PreToolUseResponse = serde_json::from_str(&line).unwrap();
    assert_eq!(
        resp.hook_specific_output.permission_decision,
        PermissionDecision::Deny,
    );
    // Deny must carry a reason so Claude doesn't silently retry.
    let reason = resp
        .hook_specific_output
        .permission_decision_reason
        .expect("Deny must include permissionDecisionReason");
    assert!(!reason.is_empty(), "reason must be non-empty");
}

/// PreToolUse with no decision within the timeout → sends Ask with reason
/// so Claude Code surfaces its own TUI prompt regardless of permission mode.
#[tokio::test]
async fn pre_tool_use_timeout_sends_ask() {
    // Use a very short timeout so the test doesn't take 30 s.
    let (_dir, _agg_tx, runtime_dir) = setup(Duration::from_millis(50)).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let pre_tool_use = json!({
        "session_id": "sess_timeout",
        "transcript_path": "/tmp/sess_timeout.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo timeout"},
        "tool_use_id": "toolu_timeout_001"
    });
    send_line(&mut writer, &pre_tool_use).await;

    // Wait longer than the timeout.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Must receive an "ask" decision — not EOF.
    let line = recv_line(&mut reader).await.expect(
        "timeout must produce an 'ask' response, not EOF",
    );
    let v: serde_json::Value = serde_json::from_str(&line).expect("must be valid JSON");
    assert_eq!(v["hookSpecificOutput"]["permissionDecision"], "ask");
    let reason = v["hookSpecificOutput"]["permissionDecisionReason"]
        .as_str()
        .expect("ask must include permissionDecisionReason");
    assert!(!reason.is_empty(), "timeout ask reason must be non-empty");
}

/// After timeout, the aggregator's pending approval is cleared so emitters
/// (swaync, ctrl) see prompt:None / waiting:0 on the next heartbeat.
#[tokio::test]
async fn pre_tool_use_timeout_clears_aggregator_state() {
    let (_dir, agg_tx, runtime_dir) = setup(Duration::from_millis(50)).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let pre_tool_use = json!({
        "session_id": "sess_timeout_state",
        "transcript_path": "/tmp/sess_ts.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo state-check"},
        "tool_use_id": "toolu_ts_001"
    });
    send_line(&mut writer, &pre_tool_use).await;

    // Wait for the timeout to fire and the Ask response to arrive.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _line = recv_line(&mut reader).await.expect("should get ask response");

    // Give the ApprovalTimedOut message time to be processed by the aggregator.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Query the aggregator: prompt must be gone.
    let (tx, rx) = tokio::sync::oneshot::channel();
    agg_tx
        .send(ccbridged::state::AggregatorMsg::GetHeartbeat { respond: tx })
        .await
        .unwrap();
    let hb = rx.await.unwrap();
    assert_eq!(hb.waiting, 0, "waiting must be 0 after timeout");
    assert!(hb.prompt.is_none(), "prompt must be None after timeout — emitters must not re-pop");
}

/// Malformed JSON input → connection closes cleanly, no panic.
#[tokio::test]
async fn malformed_json_closes_cleanly() {
    let (_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    writer.write_all(b"not valid JSON at all\n").await.unwrap();
    drop(writer);

    // Should produce no output and close cleanly.
    let line = recv_line(&mut reader).await;
    assert!(line.is_none(), "malformed JSON should produce no output");
}

/// Aggregator mpsc dropped mid-flight → hook connection closes cleanly.
#[tokio::test]
async fn aggregator_gone_closes_cleanly() {
    let (_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    // Drop the aggregator tx — the daemon is "shutting down".
    drop(agg_tx);

    // Give the accept loop a moment to notice, then send an event.
    // (The connection task will try to send and fail.)
    let stop = json!({
        "session_id": "s_gone",
        "transcript_path": "/tmp/s_gone.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "Stop",
        "response": "done"
    });
    send_line(&mut writer, &stop).await;
    drop(writer);

    // Should produce no output and close without panicking.
    let line = recv_line(&mut reader).await;
    assert!(line.is_none());
}

/// Verify WireDecision::Once → PermissionDecision::Allow mapping explicitly.
#[tokio::test]
async fn wire_once_maps_to_allow() {
    let (_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    send_line(&mut writer, &json!({
        "session_id": "s_map", "transcript_path": "/tmp/s_map.jsonl",
        "cwd": "/tmp", "permission_mode": "default",
        "hook_event_name": "PreToolUse", "tool_name": "Write",
        "tool_input": {"file_path": "/tmp/out.txt"},
        "tool_use_id": "toolu_map_001"
    })).await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    agg_tx.send(AggregatorMsg::PermissionDecision {
        tool_use_id: "toolu_map_001".to_owned(),
        decision: WireDecision::Once,
    }).await.unwrap();

    let line = recv_line(&mut reader).await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecision"],
        "allow",
        "WireDecision::Once must serialise to 'allow' on the hook stdout wire"
    );
    assert_eq!(v["hookSpecificOutput"]["hookEventName"], "PreToolUse");
}

/// Unknown hook_event_name (forward-compat) produces no output.
#[tokio::test]
async fn unknown_hook_event_passthrough() {
    let (_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    send_line(&mut writer, &json!({
        "session_id": "s_unk",
        "transcript_path": "/tmp/s_unk.jsonl",
        "cwd": "/tmp",
        "hook_event_name": "PreCompact",
        "some_future_field": 42
    })).await;
    drop(writer);

    let line = recv_line(&mut reader).await;
    assert!(line.is_none(), "Unknown event should passthrough");
}
