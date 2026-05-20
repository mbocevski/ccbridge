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
use ccbridged::state::{spawn as spawn_aggregator, AggregatorMsg, DEFAULT_APPROVAL_TIMEOUT};

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

/// Set up a tempdir as a fake `XDG_RUNTIME_DIR/ccbridge/`, start the
/// aggregator and hook ingest socket.  Returns the tempdir (must stay alive
/// for the duration of the test) and the aggregator tx.
async fn setup(approval_timeout: Duration) -> (TempDir, ccbridged::state::AggregatorTx, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime_dir = dir.path().to_path_buf();

    // Create the ccbridge sub-directory that systemd would normally provision.
    std::fs::create_dir_all(runtime_dir.join("ccbridge")).expect("mkdir ccbridge");

    let (agg_tx, _hb_rx) = spawn_aggregator(
        approval_timeout,
        ccbridged::config::Fallback::default(),
        std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(
            ccbridged::permission::Allowlist::empty(),
        ))),
    );
    hook_ingest::spawn(runtime_dir.clone(), agg_tx.clone());

    // Poll for the listener to accept connections — replaces a bind-sleep
    // that races on busy CI.
    wait_for_socket(&sock_path(&runtime_dir), Duration::from_secs(5)).await;

    (dir, agg_tx, runtime_dir)
}

/// Wait for a unix socket to be present and accepting connections.
/// The accept loop binds the inode before it's ready, so existence
/// alone isn't enough — actually probe with connect().
async fn wait_for_socket(path: &Path, deadline: Duration) {
    let start = std::time::Instant::now();
    loop {
        if path.exists() {
            if let Ok(s) = UnixStream::connect(path).await {
                drop(s);
                return;
            }
        }
        if start.elapsed() >= deadline {
            panic!(
                "socket {} not accepting connections within {deadline:?}",
                path.display(),
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Poll the aggregator's heartbeat until `pred(hb)` holds. Replaces fixed
/// sleeps used to wait for "the aggregator must have processed the previous
/// message by now" — racy on busy runners.
async fn poll_heartbeat<F>(agg_tx: &ccbridged::state::AggregatorTx, deadline: Duration, pred: F)
where
    F: Fn(&ccbridge_proto::buddy::Heartbeat) -> bool,
{
    let start = std::time::Instant::now();
    loop {
        let (tx, rx) = tokio::sync::oneshot::channel();
        agg_tx
            .send(AggregatorMsg::GetHeartbeat { respond: tx })
            .await
            .unwrap();
        let hb = rx.await.unwrap();
        if pred(&hb) {
            return;
        }
        if start.elapsed() >= deadline {
            panic!(
                "heartbeat predicate never satisfied within {deadline:?}; last: {hb:?}",
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn sock_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("ccbridge").join("hooks.sock")
}

/// Connect to the hook socket and return a split reader/writer.
async fn connect(
    runtime_dir: &Path,
) -> (
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
async fn send_line<T: serde::Serialize>(writer: &mut tokio::net::unix::OwnedWriteHalf, value: &T) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
}

/// Read one line from the reader (strips trailing `\n`).
async fn recv_line(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Option<String> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await.unwrap();
    if n == 0 {
        return None; // EOF — passthrough
    }
    Some(
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_owned(),
    )
}

/// Read one line with a generous wall-clock deadline. Returns `Ok(Some(line))`
/// on data, `Ok(None)` on EOF, `Err(_)` on deadline exceeded.
///
/// Use this when waiting for an event whose timing is bounded but not exact
/// (e.g. an approval timeout firing) — better than a fixed `sleep(N)` then
/// `recv_line`, which races on busy CI runners.
async fn recv_line_within(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    deadline: Duration,
) -> Result<Option<String>, tokio::time::error::Elapsed> {
    tokio::time::timeout(deadline, recv_line(reader)).await
}

/// Poll a closure until it returns `Some(_)` or the deadline expires.
/// Returns `Ok(value)` on success, `Err(())` on timeout.
async fn poll_until<T, F, Fut>(deadline: Duration, mut f: F) -> Result<T, ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = f().await {
            return Ok(v);
        }
        if start.elapsed() >= deadline {
            return Err(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Non-PreToolUse events receive no output (passthrough).
#[tokio::test]
async fn stop_event_passthrough() {
    let (_keep_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
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
    assert!(
        line.is_none(),
        "Stop should produce no output (passthrough)"
    );
}

/// PreToolUse followed by a permission decision produces the right JSON.
#[tokio::test]
async fn pre_tool_use_allow_decision() {
    let (_keep_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
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

    // Wait until the aggregator has registered the pending approval —
    // sending the decision before that races and the decision is dropped.
    poll_heartbeat(&agg_tx, Duration::from_secs(2), |hb| hb.waiting >= 1).await;

    // Send the permission decision from a parallel "emit module".
    agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_allow_001".to_owned(),
            decision: WireDecision::Once,
            respond: None,
        })
        .await
        .unwrap();

    // The hook connection should now receive the allow response.
    let line = recv_line(&mut reader)
        .await
        .expect("expected JSON response");
    let resp: PreToolUseResponse = serde_json::from_str(&line).expect("parse response JSON");
    assert_eq!(
        resp.hook_specific_output.permission_decision,
        PermissionDecision::Allow,
    );
    // Allow carries no reason.
    assert!(
        resp.hook_specific_output
            .permission_decision_reason
            .is_none(),
        "Allow should carry no permissionDecisionReason"
    );
}

/// PreToolUse with a Deny decision.
#[tokio::test]
async fn pre_tool_use_deny_decision() {
    let (_keep_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
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
    poll_heartbeat(&agg_tx, Duration::from_secs(2), |hb| hb.waiting >= 1).await;

    agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_deny_001".to_owned(),
            decision: WireDecision::Deny,
            respond: None,
        })
        .await
        .unwrap();

    let line = recv_line(&mut reader)
        .await
        .expect("expected JSON response");
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
    let (_keep_dir, _agg_tx, runtime_dir) = setup(Duration::from_millis(50)).await;
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

    // Wait for the response (timeout fires at 50ms; give 2s of CI headroom).
    // Predicate-driven, not sleep-driven — busy runner can't shift the
    // deadline past the read window.
    let line = recv_line_within(&mut reader, Duration::from_secs(2))
        .await
        .expect("timeout response must arrive within deadline")
        .expect("timeout must produce an 'ask' response, not EOF");
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
    let (_keep_dir, agg_tx, runtime_dir) = setup(Duration::from_millis(50)).await;
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

    // Wait for the Ask response to arrive (data-driven, not sleep-driven).
    let _line = recv_line_within(&mut reader, Duration::from_secs(2))
        .await
        .expect("timeout response must arrive within deadline")
        .expect("should get ask response");

    // Poll the aggregator until pending state has been cleared. The
    // ApprovalTimedOut message is processed asynchronously after the response
    // is sent, so the clear is racy w.r.t. the read above on busy runners.
    let agg_tx_q = agg_tx.clone();
    let hb = poll_until(Duration::from_secs(2), || {
        let agg_tx = agg_tx_q.clone();
        async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            agg_tx
                .send(ccbridged::state::AggregatorMsg::GetHeartbeat { respond: tx })
                .await
                .ok()?;
            let hb = rx.await.ok()?;
            (hb.waiting == 0 && hb.prompts.is_empty()).then_some(hb)
        }
    })
    .await
    .expect("aggregator must clear pending state within deadline");
    assert_eq!(hb.waiting, 0, "waiting must be 0 after timeout");
    assert!(
        hb.prompts.is_empty(),
        "prompt must be None after timeout — emitters must not re-pop"
    );
}

/// Malformed JSON input → connection closes cleanly, no panic.
#[tokio::test]
async fn malformed_json_closes_cleanly() {
    let (_keep_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
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
    let (_keep_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
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
    let (_keep_dir, agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    send_line(
        &mut writer,
        &json!({
            "session_id": "s_map", "transcript_path": "/tmp/s_map.jsonl",
            "cwd": "/tmp", "permission_mode": "default",
            "hook_event_name": "PreToolUse", "tool_name": "Write",
            "tool_input": {"file_path": "/tmp/out.txt"},
            "tool_use_id": "toolu_map_001"
        }),
    )
    .await;

    poll_heartbeat(&agg_tx, Duration::from_secs(2), |hb| hb.waiting >= 1).await;
    agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_map_001".to_owned(),
            decision: WireDecision::Once,
            respond: None,
        })
        .await
        .unwrap();

    let line = recv_line(&mut reader).await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecision"], "allow",
        "WireDecision::Once must serialise to 'allow' on the hook stdout wire"
    );
    assert_eq!(v["hookSpecificOutput"]["hookEventName"], "PreToolUse");
}

/// Unknown hook_event_name (forward-compat) produces no output.
#[tokio::test]
async fn unknown_hook_event_passthrough() {
    let (_keep_dir, _agg_tx, runtime_dir) = setup(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    send_line(
        &mut writer,
        &json!({
            "session_id": "s_unk",
            "transcript_path": "/tmp/s_unk.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "PreCompact",
            "some_future_field": 42
        }),
    )
    .await;
    drop(writer);

    let line = recv_line(&mut reader).await;
    assert!(line.is_none(), "Unknown event should passthrough");
}

/// PreToolUse matching a deny-list pattern → `HardDeny` with reason containing
/// the raw pattern string.
///
/// This exercises the end-to-end path:
///   allowlist deny match → `Decision::Deny` → `HardDeny { reason }` →
///   `pre_tool_use_response(Deny, Some(reason))` on the hook socket.
#[tokio::test]
async fn pre_tool_use_hard_deny_via_allowlist() {
    use ccbridged::permission::{Allowlist, Pattern};

    // Build an allowlist with "Bash(rm:*)" in the deny list.
    let allowlist = Allowlist {
        allow: vec![],
        deny: vec![Pattern::parse("Bash(rm:*)")],
    };
    let al = std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(allowlist)));
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime_dir = dir.path().to_path_buf();
    std::fs::create_dir_all(runtime_dir.join("ccbridge")).expect("mkdir ccbridge");
    let (agg_tx, _hb_rx) = ccbridged::state::spawn(
        DEFAULT_APPROVAL_TIMEOUT,
        ccbridged::config::Fallback::default(),
        al,
    );
    ccbridged::ingest::hooks::spawn(runtime_dir.clone(), agg_tx.clone());
    wait_for_socket(&sock_path(&runtime_dir), Duration::from_secs(5)).await;

    let (mut reader, mut writer) = connect(&runtime_dir).await;

    // A Bash rm command — should match "Bash(rm:*)" confidently → HardDeny.
    send_line(
        &mut writer,
        &json!({
            "session_id": "sess_hd",
            "transcript_path": "/tmp/sess_hd.jsonl",
            "cwd": "/tmp",
            "permission_mode": "default",
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "rm -rf /tmp/test"},
            "tool_use_id": "toolu_hd_001"
        }),
    )
    .await;

    let line = recv_line(&mut reader)
        .await
        .expect("HardDeny must produce a response");
    let v: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecision"], "deny",
        "deny-list match must produce 'deny'"
    );
    let reason = v["hookSpecificOutput"]["permissionDecisionReason"]
        .as_str()
        .expect("HardDeny must include permissionDecisionReason");
    assert!(
        reason.contains("Bash(rm:*)"),
        "reason must name the matched pattern, got: {reason:?}"
    );
}
