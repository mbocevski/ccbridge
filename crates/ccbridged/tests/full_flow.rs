//! Full-flow integration tests.
//!
//! Exercises all four moving parts together:
//!
//! 1. **JSONL watcher** — tails fake project files for token counts.
//! 2. **Hook ingest socket** — accepts `ccbridge-hook` subprocess connections.
//! 3. **Ctrl socket** — bidirectional client for heartbeat subscription and
//!    permission decisions.
//! 4. **`ccbridge-hook` binary** — real subprocess driven via
//!    `CARGO_BIN_EXE_ccbridge-hook`.
//!
//! Tests are async (`#[tokio::test]`) and share a [`FullSetup`] harness that
//! wires all four components into a single `TempDir`.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use ccbridge_proto::buddy::Heartbeat;
use ccbridge_proto::ctrl::{Ack, HelloMessage};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use ccbridged::emit::ctrl as ctrl_emit;
use ccbridged::ingest::{hooks as hook_ingest, jsonl::{spawn_watcher, PersistedTokens}};
use ccbridged::state::{DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator};

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

struct FullSetup {
    /// Kept alive so the tempdir isn't deleted while the test runs.
    _dir: TempDir,
    /// `$XDG_RUNTIME_DIR` — contains `ccbridge/hooks.sock` and `ccbridge/ctrl.sock`.
    pub runtime_dir: PathBuf,
    /// Fake `~/.claude/projects/` directory for JSONL files.
    pub projects_dir: PathBuf,
}

/// Spin up aggregator + ctrl socket + hook ingest socket + JSONL watcher,
/// all sharing one `TempDir`.
async fn setup_full(approval_timeout: Duration) -> FullSetup {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime_dir = dir.path().to_path_buf();
    let ccbridge_dir = runtime_dir.join("ccbridge");
    let projects_dir = runtime_dir.join("projects");
    let state_path = runtime_dir.join("tokens.json");

    std::fs::create_dir_all(&ccbridge_dir).expect("mkdir ccbridge");
    std::fs::create_dir_all(&projects_dir).expect("mkdir projects");

    let (agg_tx, hb_rx) = spawn_aggregator(approval_timeout, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));

    ctrl_emit::spawn(
        runtime_dir.clone(),
        agg_tx.clone(),
        hb_rx,
        "TestOwner".to_owned(),
        0,
    );

    hook_ingest::spawn(runtime_dir.clone(), agg_tx.clone());

    spawn_watcher(
        projects_dir.clone(),
        state_path,
        agg_tx.clone(),
        PersistedTokens::default(),
    );

    // Give all four accept loops a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    FullSetup { _dir: dir, runtime_dir, projects_dir }
}

// ---------------------------------------------------------------------------
// Helpers (mirrors ctrl_socket.rs / hook_ingest.rs patterns)
// ---------------------------------------------------------------------------

fn ctrl_sock_path(runtime_dir: &PathBuf) -> PathBuf {
    runtime_dir.join("ccbridge").join("ctrl.sock")
}

async fn ctrl_connect(
    runtime_dir: &PathBuf,
) -> (BufReader<tokio::net::unix::OwnedReadHalf>, tokio::net::unix::OwnedWriteHalf) {
    let stream = UnixStream::connect(ctrl_sock_path(runtime_dir))
        .await
        .expect("connect to ctrl.sock");
    let (r, w) = stream.into_split();
    (BufReader::new(r), w)
}

async fn read_json<T: serde::de::DeserializeOwned>(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> T {
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read_line");
    serde_json::from_str(line.trim_end()).expect("deserialize JSON")
}

async fn write_json<T: serde::Serialize>(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    value: &T,
) {
    let mut bytes = serde_json::to_vec(value).expect("serialize");
    bytes.push(b'\n');
    writer.write_all(&bytes).await.expect("write");
}

/// Consume the initial hello + snapshot that every ctrl client receives on connect.
async fn drain_handshake(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) {
    let _: HelloMessage = read_json(reader).await; // hello
    let _: Heartbeat = read_json(reader).await;    // initial snapshot
}

/// Poll `reader` until the next heartbeat satisfying `pred` arrives, or panic on timeout.
async fn wait_for_heartbeat<F>(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    pred: F,
) -> Heartbeat
where
    F: Fn(&Heartbeat) -> bool,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let hb: Heartbeat = read_json(reader).await;
            if pred(&hb) {
                return hb;
            }
        }
    })
    .await
    .expect("timed out waiting for expected heartbeat state")
}

/// One assistant JSONL line with the given output_tokens.
fn assistant_line(output_tokens: u64) -> String {
    serde_json::to_string(&json!({
        "type": "assistant",
        "message": {
            "role": "assistant",
            "usage": {
                "input_tokens": 10,
                "output_tokens": output_tokens,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0
            },
            "content": [{"type": "text", "text": "response"}]
        }
    }))
    .unwrap()
        + "\n"
}

/// Path to the compiled `ccbridge-hook` binary (set by cargo in test envs).
fn hook_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_ccbridge-hook") {
        return PathBuf::from(p);
    }
    let mut p = std::env::current_exe().unwrap();
    p.pop(); p.pop();
    p.push("ccbridge-hook");
    p
}

// ---------------------------------------------------------------------------
// Test 1: golden path
// ---------------------------------------------------------------------------

/// Full end-to-end: JSONL tokens + hook approval + ctrl decision.
///
/// 1. Write a JSONL file (200 output tokens) to projects_dir.
/// 2. Subscribe a ctrl client to heartbeats.
/// 3. Spawn `ccbridge-hook` with a PreToolUse event, writing stdin in a
///    background thread.
/// 4. Wait for a heartbeat with `waiting=1`, send a `permission` decision.
/// 5. Assert the hook subprocess produced `"permissionDecision":"allow"`.
/// 6. Assert a subsequent heartbeat reflects `tokens >= 200`.
#[tokio::test]
async fn golden_path() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    // ── 1. Write JSONL file ──────────────────────────────────────────────────
    let jsonl_path = setup.projects_dir.join("session_golden.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).unwrap();
        write!(f, "{}", assistant_line(200)).unwrap();
    }

    // ── 2. Ctrl client: subscribe to heartbeats ──────────────────────────────
    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;
    write_json(&mut ctrl_w, &json!({"cmd": "subscribe", "topics": ["heartbeat"]})).await;
    let _: Ack = read_json(&mut ctrl_r).await; // subscribe ack

    // ── 3. Spawn ccbridge-hook ────────────────────────────────────────────────
    let tool_use_id = "toolu_golden_001";
    let event = json!({
        "session_id": "sess_golden",
        "transcript_path": "/tmp/sess_golden.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo golden"},
        "tool_use_id": tool_use_id
    });
    let event_str = serde_json::to_string(&event).unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", &setup.runtime_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ccbridge-hook");

    // Write hook stdin in a background thread so we can drive the async ctrl
    // side concurrently.  The hook connects to hooks.sock and awaits a
    // decision; we send the decision via ctrl.sock on this thread.
    let stdin = child.stdin.take().unwrap();
    std::thread::spawn(move || {
        let mut w = stdin;
        w.write_all(event_str.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
        // Drop closes the pipe → hook reads EOF on stdin.
    });

    // ── 4. Wait for waiting=1 and send permission ────────────────────────────
    let hb = wait_for_heartbeat(&mut ctrl_r, |hb| {
        hb.waiting == 1
            && hb.prompt.as_ref().map_or(false, |p| p.id == tool_use_id)
    })
    .await;
    assert_eq!(hb.waiting, 1);

    write_json(
        &mut ctrl_w,
        &json!({"cmd": "permission", "id": tool_use_id, "decision": "once"}),
    )
    .await;
    let perm_ack: Ack = read_json(&mut ctrl_r).await;
    assert_eq!(perm_ack.ack, "permission");
    assert!(perm_ack.ok);

    // ── 5. Assert hook output ────────────────────────────────────────────────
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "hook must exit 0: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"permissionDecision\":\"allow\""),
        "hook stdout should contain allow decision, got: {stdout}",
    );

    // ── 6. Wait for tokens ≥ 200 ─────────────────────────────────────────────
    // Give the JSONL watcher time to pick up the file (polls at 50ms).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let hb = wait_for_heartbeat(&mut ctrl_r, |hb| hb.tokens >= 200).await;
    assert!(hb.tokens >= 200, "expected tokens >= 200, got {}", hb.tokens);
}

// ---------------------------------------------------------------------------
// Test 2: timeout passthrough
// ---------------------------------------------------------------------------

/// PreToolUse with no decision within the approval timeout → hook exits 0,
/// empty stdout.
#[tokio::test]
async fn timeout_passthrough() {
    // Short timeout: 50ms.
    let setup = setup_full(Duration::from_millis(50)).await;

    let event = serde_json::to_string(&json!({
        "session_id": "sess_timeout",
        "transcript_path": "/tmp/sess_timeout.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo timeout"},
        "tool_use_id": "toolu_timeout_001"
    }))
    .unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", &setup.runtime_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(event.as_bytes()).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);

    // Don't send any permission decision — timeout fires after 50ms.
    let output = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();

    assert!(
        output.status.success(),
        "hook must exit 0 on timeout: {:?}",
        output.status,
    );
    // Timeout must produce an "ask" response — not empty stdout.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.is_empty(),
        "hook must produce an 'ask' response on timeout, not empty stdout"
    );
    let v: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("timeout stdout must be valid JSON");
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecision"],
        "ask",
        "timeout fallback must be 'ask', got: {stdout:?}",
    );
    assert!(
        v["hookSpecificOutput"]["permissionDecisionReason"]
            .as_str()
            .map(|r| !r.is_empty())
            .unwrap_or(false),
        "timeout ask must carry a reason"
    );
}

// ---------------------------------------------------------------------------
// Test 3: multiple ctrl subscribers
// ---------------------------------------------------------------------------

/// Two ctrl clients both receive the same heartbeat stream.
#[tokio::test]
async fn multiple_ctrl_subscribers() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    // Connect two ctrl clients and subscribe both to heartbeats.
    let (mut r1, mut w1) = ctrl_connect(&setup.runtime_dir).await;
    let (mut r2, mut w2) = ctrl_connect(&setup.runtime_dir).await;

    drain_handshake(&mut r1).await;
    drain_handshake(&mut r2).await;

    write_json(&mut w1, &json!({"cmd": "subscribe", "topics": ["heartbeat"]})).await;
    let _: Ack = read_json(&mut r1).await;
    write_json(&mut w2, &json!({"cmd": "subscribe", "topics": ["heartbeat"]})).await;
    let _: Ack = read_json(&mut r2).await;

    // Inject a SessionStart via the hook binary to exercise the real ingress path.
    let event = serde_json::to_string(&json!({
        "session_id": "sess_multi",
        "transcript_path": "/tmp/sess_multi.jsonl",
        "cwd": "/tmp",
        "hook_event_name": "SessionStart",
        "source": "startup",
        "model": "claude-test"
    }))
    .unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", &setup.runtime_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(event.as_bytes()).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);

    // Both ctrl clients should receive a heartbeat reflecting total=1.
    let hb1 = wait_for_heartbeat(&mut r1, |hb| hb.total == 1).await;
    let hb2 = wait_for_heartbeat(&mut r2, |hb| hb.total == 1).await;

    assert_eq!(hb1.total, 1, "subscriber 1 should see total=1");
    assert_eq!(hb2.total, 1, "subscriber 2 should see total=1");

    // Wait for the hook to exit cleanly.
    let _ = tokio::task::spawn_blocking(move || child.wait()).await;
}

// ---------------------------------------------------------------------------
// Test 4: JSONL tokens reflected in heartbeat
// ---------------------------------------------------------------------------

/// JSONL watcher → aggregator → ctrl fanout: writing tokens to a JSONL file
/// eventually shows up in a ctrl-socket heartbeat.
#[tokio::test]
async fn jsonl_tokens_reflected_in_heartbeat() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    // Connect ctrl client and subscribe.
    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;
    write_json(&mut ctrl_w, &json!({"cmd": "subscribe", "topics": ["heartbeat"]})).await;
    let _: Ack = read_json(&mut ctrl_r).await;

    // Write a JSONL file with 350 output tokens.
    let jsonl_path = setup.projects_dir.join("session_tokens.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).unwrap();
        write!(f, "{}", assistant_line(350)).unwrap();
    }

    // Wait for a heartbeat showing tokens >= 350.  Tolerant of timing:
    // the watcher polls at ~50ms intervals.
    let hb = wait_for_heartbeat(&mut ctrl_r, |hb| hb.tokens >= 350).await;
    assert!(
        hb.tokens >= 350,
        "expected tokens >= 350 after JSONL write, got {}",
        hb.tokens,
    );
}
