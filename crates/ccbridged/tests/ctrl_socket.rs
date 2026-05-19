//! Integration tests for the ctrl socket emitter.
//!
//! Each test:
//! 1. Creates a `tempdir` for the runtime directory.
//! 2. Spawns the aggregator + ctrl socket.
//! 3. Connects as a ctrl client via `tokio::net::UnixStream`.
//! 4. Exercises the protocol and asserts the responses.

use std::path::PathBuf;
use std::time::Duration;

use ccbridge_proto::buddy::{Heartbeat, StatusAck, WireDecision};
use ccbridge_proto::ctrl::{Ack, HelloMessage};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::oneshot;

use ccbridged::emit::ctrl as ctrl_emit;
use ccbridged::state::{AggregatorMsg, DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator};

// ---------------------------------------------------------------------------
// Test harness helpers
// ---------------------------------------------------------------------------

/// Start the aggregator + ctrl socket, return (TempDir, agg_tx, runtime_dir).
///
/// `TempDir` must stay alive for the duration of the test to keep the socket
/// directory present.
async fn setup() -> (TempDir, ccbridged::state::AggregatorTx, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime_dir = dir.path().to_path_buf();

    // Create the ccbridge sub-directory that systemd would normally provision.
    std::fs::create_dir_all(runtime_dir.join("ccbridge")).expect("mkdir ccbridge");

    let (agg_tx, hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));

    ctrl_emit::spawn(
        runtime_dir.clone(),
        agg_tx.clone(),
        hb_rx,
        "TestOwner".to_owned(),
        0, // UTC offset — deterministic in tests
    );

    // Give the accept loop a moment to bind.
    tokio::time::sleep(Duration::from_millis(20)).await;

    (dir, agg_tx, runtime_dir)
}

fn ctrl_sock_path(runtime_dir: &PathBuf) -> PathBuf {
    runtime_dir.join("ccbridge").join("ctrl.sock")
}

/// Connect to the ctrl socket and return a split (reader, writer).
async fn connect(
    runtime_dir: &PathBuf,
) -> (
    BufReader<tokio::net::unix::OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
) {
    let stream = UnixStream::connect(ctrl_sock_path(runtime_dir))
        .await
        .expect("connect to ctrl.sock");
    let (r, w) = stream.into_split();
    (BufReader::new(r), w)
}

/// Read one line and deserialize as T.
async fn read_json<T: serde::de::DeserializeOwned>(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> T {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("read line");
    serde_json::from_str(line.trim_end()).expect("deserialize JSON")
}

/// Write a JSON line to the writer.
async fn write_json<T: serde::Serialize>(writer: &mut tokio::net::unix::OwnedWriteHalf, value: &T) {
    let mut bytes = serde_json::to_vec(value).expect("serialize");
    bytes.push(b'\n');
    writer.write_all(&bytes).await.expect("write");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// On connect, the server immediately sends a HelloMessage followed by the
/// current heartbeat snapshot.
#[tokio::test]
async fn connect_receives_hello_and_snapshot() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, _writer) = connect(&runtime_dir).await;

    // Line 1: HelloMessage
    let hello: HelloMessage = read_json(&mut reader).await;
    assert_eq!(hello.hello.version, 1);
    assert_eq!(hello.hello.owner, "TestOwner");
    // epoch should be non-zero (we're past 1970)
    assert!(hello.hello.time.0 > 0, "epoch should be positive");
    // tz offset is 0 in tests (we pass 0 explicitly)
    assert_eq!(hello.hello.time.1, 0);

    // Line 2: Heartbeat snapshot (fresh aggregator has no sessions)
    let hb: Heartbeat = read_json(&mut reader).await;
    assert_eq!(hb.total, 0);
    assert_eq!(hb.running, 0);
    assert_eq!(hb.waiting, 0);
    assert!(hb.prompt.is_none());
}

/// `{"cmd":"subscribe","topics":["heartbeat"]}` receives `{"ack":"subscribe","ok":true}`.
#[tokio::test]
async fn subscribe_heartbeat_acked() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    // Consume hello + snapshot.
    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(
        &mut writer,
        &serde_json::json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;

    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "subscribe");
    assert!(ack.ok);
    assert!(ack.error.is_none());
}

/// `{"cmd":"unsubscribe","topics":["heartbeat"]}` receives `{"ack":"unsubscribe","ok":true}`.
#[tokio::test]
async fn unsubscribe_acked() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(
        &mut writer,
        &serde_json::json!({"cmd": "unsubscribe", "topics": ["heartbeat"]}),
    )
    .await;

    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "unsubscribe");
    assert!(ack.ok);
}

/// Permission command: pre-register a PreToolUse in the aggregator, then send
/// a `{"cmd":"permission",...}` over the ctrl socket, and verify the decision
/// oneshot fires with the correct `WireDecision`.
#[tokio::test]
async fn permission_command_resolves_approval() {
    let (_dir, agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    // Consume hello + snapshot.
    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    // Register a PreToolUse with the aggregator directly.
    let tool_use_id = "toolu_ctrl_test_001".to_owned();
    let (respond_tx, respond_rx) = oneshot::channel();
    agg_tx
        .send(AggregatorMsg::HookEvent {
            event: ccbridge_proto::hook::HookEvent::PreToolUse(
                ccbridge_proto::hook::PreToolUseEvent {
                    base: ccbridge_proto::hook::HookBase {
                        session_id: "ctrl-test-session".to_owned(),
                        transcript_path: "/tmp/test.jsonl".to_owned(),
                        cwd: "/tmp".to_owned(),
                    },
                    permission_mode: ccbridge_proto::hook::PermissionMode::Default,
                    effort: None,
                    tool_name: "Bash".to_owned(),
                    tool_input: serde_json::json!({"command": "echo hello"}),
                    tool_use_id: tool_use_id.clone(),
                    agent_id: None,
                    agent_type: None,
                },
            ),
            respond: respond_tx,
        })
        .await
        .expect("send HookEvent");

    // Extract the decision receiver from the AwaitDecision response.
    let response = respond_rx.await.expect("aggregator should respond");
    let decision_rx = match response {
        ccbridged::state::HookResponse::AwaitDecision { rx, .. } => rx,
        _ => panic!("expected AwaitDecision"),
    };

    // Now send the permission command over ctrl.
    write_json(
        &mut writer,
        &serde_json::json!({
            "cmd": "permission",
            "id": tool_use_id,
            "decision": "once"
        }),
    )
    .await;

    // Assert the ctrl ack.
    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "permission");
    assert!(ack.ok);

    // Assert the aggregator's oneshot fired with the correct decision.
    let decision = tokio::time::timeout(Duration::from_secs(1), decision_rx)
        .await
        .expect("decision should arrive within 1s")
        .expect("decision_rx should not be dropped");
    assert_eq!(decision, WireDecision::Once);
}

/// `{"cmd":"status"}` returns a StatusAck with name="ccbridge".
#[tokio::test]
async fn status_command_returns_status_ack() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(&mut writer, &serde_json::json!({"cmd": "status"})).await;

    let ack: StatusAck = read_json(&mut reader).await;
    assert_eq!(ack.ack, "status");
    assert!(ack.ok);
    assert_eq!(ack.data.name.as_deref(), Some("ccbridge"));
    // Hardware fields absent for a software bridge.
    assert!(ack.data.bat.is_none());
    assert!(ack.data.sys.is_none());
    assert!(ack.data.stats.is_none());
}

/// `{"cmd":"replay","n":5}` returns `{"ack":"replay","ok":false,"error":"unknown_command"}`.
#[tokio::test]
async fn replay_returns_unknown_command() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(&mut writer, &serde_json::json!({"cmd": "replay", "n": 5})).await;

    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "replay");
    assert!(!ack.ok);
    assert_eq!(ack.error.as_deref(), Some("unknown_command"));
}

/// `{"cmd":"forget_device","addr":"AA:BB:CC:DD:EE:FF"}` → unknown_command until BLE lands.
#[tokio::test]
async fn forget_device_returns_unknown_command() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(
        &mut writer,
        &serde_json::json!({"cmd": "forget_device", "addr": "AA:BB:CC:DD:EE:FF"}),
    )
    .await;

    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "forget_device");
    assert!(!ack.ok);
    assert_eq!(ack.error.as_deref(), Some("unknown_command"));
}

/// A completely unknown `cmd` string also gets a proper `unknown_command` ack.
#[tokio::test]
async fn completely_unknown_cmd_returns_unknown_command() {
    let (_dir, _agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await;
    let _: serde_json::Value = read_json(&mut reader).await;

    write_json(
        &mut writer,
        &serde_json::json!({"cmd": "future_command", "data": 42}),
    )
    .await;

    let ack: Ack = read_json(&mut reader).await;
    assert_eq!(ack.ack, "future_command");
    assert!(!ack.ok);
    assert_eq!(ack.error.as_deref(), Some("unknown_command"));
}

/// Subscribed client receives a streamed heartbeat when state changes.
#[tokio::test]
async fn subscribed_client_receives_heartbeats() {
    let (_dir, agg_tx, runtime_dir) = setup().await;
    let (mut reader, mut writer) = connect(&runtime_dir).await;

    let _: serde_json::Value = read_json(&mut reader).await; // hello
    let _: serde_json::Value = read_json(&mut reader).await; // initial snapshot

    // Subscribe to heartbeats.
    write_json(
        &mut writer,
        &serde_json::json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
    let _: Ack = read_json(&mut reader).await; // subscribe ack

    // Trigger a state change in the aggregator.
    let (respond_tx, respond_rx) = oneshot::channel();
    agg_tx
        .send(AggregatorMsg::HookEvent {
            event: ccbridge_proto::hook::HookEvent::SessionStart(
                ccbridge_proto::hook::SessionStartEvent {
                    base: ccbridge_proto::hook::HookBase {
                        session_id: "hb-stream-test".to_owned(),
                        transcript_path: "/tmp/test.jsonl".to_owned(),
                        cwd: "/tmp".to_owned(),
                    },
                    source: ccbridge_proto::hook::SessionSource::Startup,
                    model: "claude-test".to_owned(),
                    agent_type: None,
                },
            ),
            respond: respond_tx,
        })
        .await
        .expect("send SessionStart");
    respond_rx.await.expect("aggregator response");

    // The client should receive a heartbeat reflecting total=1.
    let hb = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let hb: Heartbeat = read_json(&mut reader).await;
            if hb.total == 1 {
                return hb;
            }
        }
    })
    .await
    .expect("heartbeat with total=1 should arrive within 1s");

    assert_eq!(hb.total, 1);
}
