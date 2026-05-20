// SPDX-License-Identifier: MIT
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
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use ccbridge_proto::buddy::Heartbeat;
use ccbridge_proto::ctrl::{Ack, HelloMessage};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use ccbridged::emit::ctrl as ctrl_emit;
use ccbridged::ingest::{
    hooks as hook_ingest,
    jsonl::{PersistedTokens, spawn_watcher},
};
use ccbridged::state::{
    AggregatorMsg, AggregatorTx, DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator,
};

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

struct FullSetup {
    /// Holds the tempdir alive for the duration of the test.
    /// The leading underscore is intentional — Rust treats `_keep_dir` as
    /// "intentionally unused" so it doesn't trigger dead-code lints, but
    /// the descriptive name signals "removing this will break the test"
    /// to anyone tempted to clean it up.
    _keep_dir: TempDir,
    /// `$XDG_RUNTIME_DIR` — contains `ccbridge/hooks.sock` and `ccbridge/ctrl.sock`.
    pub runtime_dir: PathBuf,
    /// Fake `~/.claude/projects/` directory for JSONL files.
    pub projects_dir: PathBuf,
    /// Direct handle to the aggregator — for tests that inject messages without
    /// going through the hook or ctrl sockets.
    pub agg_tx: AggregatorTx,
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

    let (agg_tx, hb_rx) = spawn_aggregator(
        approval_timeout,
        ccbridged::config::Fallback::default(),
        std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(
            ccbridged::permission::Allowlist::empty(),
        ))),
    );

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

    // Probe each socket until its listener is bound, instead of sleeping
    // a fixed 50ms. The hook subprocess (used by some tests below) does
    // not retry connect() — fail-silent on ENOENT — so an unprobed bind
    // race would silently turn the hook into a passthrough.
    wait_for_socket(&ccbridge_dir.join("hooks.sock"), Duration::from_secs(5)).await;
    wait_for_socket(&ccbridge_dir.join("ctrl.sock"), Duration::from_secs(5)).await;

    FullSetup {
        _keep_dir: dir,
        runtime_dir,
        projects_dir,
        agg_tx,
    }
}

// ---------------------------------------------------------------------------
// Helpers (mirrors ctrl_socket.rs / hook_ingest.rs patterns)
// ---------------------------------------------------------------------------

fn ctrl_sock_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("ccbridge").join("ctrl.sock")
}

async fn ctrl_connect(
    runtime_dir: &Path,
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

/// Poll for `path` to exist. Used after operations that complete via the
/// aggregator task (e.g. AllowlistAlways → settings.local.json write).
async fn wait_for_path(path: &Path, deadline: Duration) {
    let start = std::time::Instant::now();
    while !path.exists() {
        if start.elapsed() >= deadline {
            panic!("path {} did not appear within {deadline:?}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Wait for a unix socket to be both present and accepting connections —
/// poll connect() rather than just stat(). The accept loop binds the
/// inode before it's ready to accept, so existence alone is not enough.
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

async fn read_json<T: serde::de::DeserializeOwned>(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> T {
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read_line");
    serde_json::from_str(line.trim_end()).expect("deserialize JSON")
}

async fn write_json<T: serde::Serialize>(writer: &mut tokio::net::unix::OwnedWriteHalf, value: &T) {
    let mut bytes = serde_json::to_vec(value).expect("serialize");
    bytes.push(b'\n');
    writer.write_all(&bytes).await.expect("write");
}

/// Consume the initial hello + snapshot that every ctrl client receives on connect.
async fn drain_handshake(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) {
    let _: HelloMessage = read_json(reader).await; // hello
    let _: Heartbeat = read_json(reader).await; // initial snapshot
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
    p.pop();
    p.pop();
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
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
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
        hb.waiting == 1 && hb.prompts.iter().any(|p| p.id == tool_use_id)
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
    assert!(
        output.status.success(),
        "hook must exit 0: {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"permissionDecision\":\"allow\""),
        "hook stdout should contain allow decision, got: {stdout}",
    );

    // ── 6. Wait for tokens ≥ 200 ─────────────────────────────────────────────
    // wait_for_heartbeat already polls with a 5s deadline — no fixed sleep
    // needed for the JSONL watcher pickup latency.
    let hb = wait_for_heartbeat(&mut ctrl_r, |hb| hb.tokens >= 200).await;
    assert!(
        hb.tokens >= 200,
        "expected tokens >= 200, got {}",
        hb.tokens
    );
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
        v["hookSpecificOutput"]["permissionDecision"], "ask",
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

    write_json(
        &mut w1,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
    let _: Ack = read_json(&mut r1).await;
    write_json(
        &mut w2,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
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
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
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

// ---------------------------------------------------------------------------
// AllowlistAlways end-to-end
// ---------------------------------------------------------------------------

/// Full Always path:
/// 1. Send PreToolUse via hook socket.
/// 2. Wait for heartbeat with waiting=1.
/// 3. Send AllowlistAlways directly via agg_tx.
/// 4. Assert hook exits with Once (allow) decision.
/// 5. Assert settings.local.json was created with the derived pattern.
///
/// Uses `cwd` = a fresh TempDir (no .claude/) so the "cwd becomes root" path
/// is exercised — write_allow_pattern creates <cwd>/.claude/ on first write.
#[tokio::test]
async fn allowlist_always_writes_pattern_and_approves() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    // TempDir acts as the project cwd — no .claude/ dir so cwd becomes root.
    let project_dir = tempfile::tempdir().expect("project tempdir");
    let cwd = project_dir.path().to_str().unwrap().to_owned();
    let tool_use_id = "toolu_always_full_001";

    // ── Subscribe ctrl for heartbeats ────────────────────────────────────────
    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
    let _: Ack = read_json(&mut ctrl_r).await;

    // ── Send PreToolUse via hook binary ───────────────────────────────────────
    let event = serde_json::to_string(&json!({
        "session_id": "sess_always_full",
        "transcript_path": "/tmp/sess_always_full.jsonl",
        "cwd": cwd,
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo always_full"},
        "tool_use_id": tool_use_id
    }))
    .unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", &setup.runtime_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ccbridge-hook");

    let stdin = child.stdin.take().unwrap();
    std::thread::spawn(move || {
        let mut w = stdin;
        w.write_all(event.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    });

    // ── Wait for waiting=1 ───────────────────────────────────────────────────
    wait_for_heartbeat(&mut ctrl_r, |hb| {
        hb.waiting == 1 && hb.prompts.iter().any(|p| p.id == tool_use_id)
    })
    .await;

    // ── Trigger AllowlistAlways via agg_tx ────────────────────────────────────
    setup
        .agg_tx
        .send(AggregatorMsg::AllowlistAlways {
            tool_use_id: tool_use_id.to_owned(),
        })
        .await
        .expect("send AllowlistAlways");

    // ── Assert hook exits with allow ──────────────────────────────────────────
    let output = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(output.status.success(), "hook must exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"permissionDecision\":\"allow\""),
        "AllowlistAlways must approve the in-flight call, got: {stdout}"
    );

    // ── Assert settings.local.json created with derived pattern ───────────────
    // The write happens on the aggregator task; the fsync chain in
    // save_settings can land later than expected on slow disks, so
    // poll for the file rather than guessing a fixed sleep budget.
    let local = project_dir
        .path()
        .join(".claude")
        .join("settings.local.json");
    wait_for_path(&local, Duration::from_secs(2)).await;
    let content = std::fs::read_to_string(&local).unwrap();
    let v: serde_json::Value = serde_json::from_str(&content).unwrap();
    let allow = v["permissions"]["allow"].as_array().unwrap();
    assert!(
        allow
            .iter()
            .any(|e| e.as_str() == Some("Bash(echo always_full)")),
        "settings.local.json must contain the derived pattern, got: {content}"
    );
}

// ---------------------------------------------------------------------------
// symlinked .claude must be rejected as a project-root marker
// ---------------------------------------------------------------------------

/// A `.claude` directory that is a symlink (even to a real dir) must NOT
/// be treated as a project-root marker.  Otherwise an attacker who can
/// drop a symlink in a parent directory of the user's cwd could redirect
/// `settings.local.json` writes to an arbitrary location.
///
/// Setup:
///     parent/
///       .claude -> decoy/.claude   ← symlinked, must be rejected
///       inner/                     ← cwd; no .claude of its own
///     decoy/
///       .claude/                   ← real, but should NOT be picked up
///
/// Expected: AllowlistAlways writes to `inner/.claude/settings.local.json`
/// (cwd-as-root fallback), not to `parent/.claude/...` (the symlink) or
/// `decoy/.claude/...` (the symlink target).
#[tokio::test]
async fn symlinked_dotclaude_is_rejected_in_full_flow() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    let scratch = tempfile::tempdir().expect("scratch tempdir");
    let parent = scratch.path().join("parent");
    let inner = parent.join("inner");
    let decoy = scratch.path().join("decoy");
    let decoy_claude = decoy.join(".claude");
    std::fs::create_dir_all(&inner).unwrap();
    std::fs::create_dir_all(&decoy_claude).unwrap();
    // parent/.claude → decoy/.claude (symlink, must be rejected).
    std::os::unix::fs::symlink(&decoy_claude, parent.join(".claude")).unwrap();

    let cwd = inner.to_str().unwrap().to_owned();
    let tool_use_id = "toolu_symlink_j2_001";

    // ── Subscribe ctrl for heartbeats ────────────────────────────────────────
    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
    let _: Ack = read_json(&mut ctrl_r).await;

    // ── Send PreToolUse via hook binary ──────────────────────────────────────
    let event = serde_json::to_string(&json!({
        "session_id": "sess_symlink_j2",
        "transcript_path": "/tmp/sess_symlink_j2.jsonl",
        "cwd": cwd,
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo j2"},
        "tool_use_id": tool_use_id
    }))
    .unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", &setup.runtime_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ccbridge-hook");

    let stdin = child.stdin.take().unwrap();
    std::thread::spawn(move || {
        let mut w = stdin;
        w.write_all(event.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    });

    wait_for_heartbeat(&mut ctrl_r, |hb| {
        hb.waiting == 1 && hb.prompts.iter().any(|p| p.id == tool_use_id)
    })
    .await;

    setup
        .agg_tx
        .send(AggregatorMsg::AllowlistAlways {
            tool_use_id: tool_use_id.to_owned(),
        })
        .await
        .expect("send AllowlistAlways");

    let output = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(output.status.success(), "hook must exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"permissionDecision\":\"allow\""),
        "AllowlistAlways must approve the in-flight call, got: {stdout}"
    );

    // ── Assert settings landed at inner/.claude (cwd fallback), NOT
    // parent/.claude (symlink) or decoy/.claude (symlink target).
    let inner_local = inner.join(".claude").join("settings.local.json");
    wait_for_path(&inner_local, Duration::from_secs(2)).await;

    // The decoy must remain pristine — never written through.
    assert!(
        !decoy_claude.join("settings.local.json").exists(),
        "settings.local.json must NOT have been written to the symlink target",
    );
    // parent/.claude/settings.local.json would resolve to decoy via the
    // symlink — also must not exist (covered by the previous assert, but
    // belt-and-braces).
    let parent_local_via_symlink = parent.join(".claude").join("settings.local.json");
    assert!(
        !parent_local_via_symlink.exists(),
        "settings.local.json must NOT have been written through the parent symlink",
    );
}

// ---------------------------------------------------------------------------
// Multi-session prompts in a single heartbeat
// ---------------------------------------------------------------------------

/// Two parallel sessions both have a pending PreToolUse → the heartbeat
/// surface both prompts so the notify emitter can post a notification per
/// session rather than collapsing to one.
#[tokio::test]
async fn heartbeat_lists_prompts_for_parallel_sessions() {
    use ccbridge_proto::hook::{HookBase, HookEvent, PermissionMode, PreToolUseEvent};

    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;
    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "subscribe", "topics": ["heartbeat"]}),
    )
    .await;
    let _: Ack = read_json(&mut ctrl_r).await;

    // Inject two parallel PreToolUse events through the aggregator.  We
    // don't drive the hook subprocess here because that would deadlock
    // waiting for an approval per call — direct injection is enough to
    // exercise the heartbeat shape we care about.
    for (sid, tid, tool) in [
        ("sess-parallel-A", "toolu_pa", "Bash"),
        ("sess-parallel-B", "toolu_pb", "Edit"),
    ] {
        let (resp, _) = tokio::sync::oneshot::channel();
        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: sid.to_owned(),
                transcript_path: format!("/tmp/{sid}.jsonl"),
                cwd: format!("/tmp/{sid}"),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: json!({"command": "echo hi"}),
            tool_use_id: tid.to_owned(),
            agent_id: None,
            agent_type: None,
        });
        setup
            .agg_tx
            .send(AggregatorMsg::HookEvent {
                event: Box::new(event),
                respond: resp,
            })
            .await
            .unwrap();
    }

    // Wait for a heartbeat that reflects both pending prompts.
    let hb = wait_for_heartbeat(&mut ctrl_r, |hb| hb.waiting == 2 && hb.prompts.len() == 2).await;
    let by_session: std::collections::HashMap<String, &ccbridge_proto::buddy::PromptInfo> = hb
        .prompts
        .iter()
        .map(|p| (p.session_id.clone().unwrap(), p))
        .collect();
    assert_eq!(by_session["sess-parallel-A"].id, "toolu_pa");
    assert_eq!(by_session["sess-parallel-A"].tool, "Bash");
    assert_eq!(by_session["sess-parallel-B"].id, "toolu_pb");
    assert_eq!(by_session["sess-parallel-B"].tool, "Edit");
    assert_eq!(hb.msg, "approve: 2 pending");
}

// ---------------------------------------------------------------------------
// expired id ctrl decision doesn't hang
// ---------------------------------------------------------------------------

/// Sending a permission decision for an unknown tool_use_id must be acked
/// quickly (no hang) and the daemon must not panic.
#[tokio::test]
async fn expired_id_ctrl_decision_returns_ack() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;

    // No pending approval for this id.
    write_json(
        &mut ctrl_w,
        &json!({"cmd": "permission", "id": "toolu_nonexistent", "decision": "once"}),
    )
    .await;

    // Must receive an ack promptly (ctrl always acks permission commands).
    let ack: Ack = tokio::time::timeout(Duration::from_secs(2), read_json(&mut ctrl_r))
        .await
        .expect("ack must arrive within 2s");
    assert_eq!(ack.ack, "permission", "ack must name the command");
    assert!(
        !ack.ok,
        "ack.ok must be false for an unknown tool_use_id (got: {ack:?})",
    );
    assert_eq!(
        ack.error.as_deref(),
        Some("unknown_id"),
        "unknown-id ack must carry error=\"unknown_id\", got: {ack:?}",
    );
}

// ---------------------------------------------------------------------------
// simulate command returns unknown_command ack
// ---------------------------------------------------------------------------

/// `{"cmd":"simulate",...}` over ctrl must be acked with `ok:false` since
/// `allow_simulate` is not implemented yet in the ctrl handler.
#[tokio::test]
async fn simulate_command_returns_unknown_ack() {
    let setup = setup_full(DEFAULT_APPROVAL_TIMEOUT).await;

    let (mut ctrl_r, mut ctrl_w) = ctrl_connect(&setup.runtime_dir).await;
    drain_handshake(&mut ctrl_r).await;

    write_json(
        &mut ctrl_w,
        &json!({"cmd": "simulate", "event": {"hook_event_name": "PreToolUse"}}),
    )
    .await;

    let ack: Ack = tokio::time::timeout(Duration::from_secs(2), read_json(&mut ctrl_r))
        .await
        .expect("ack must arrive within 2s");
    assert_eq!(ack.ack, "simulate");
    assert!(!ack.ok, "simulate must not be ok (not implemented)");
}

// Note: daemon_restart_clears_active_notif_map is not tested here because
// it requires a live org.freedesktop.Notifications DBus session which is not
// available in CI.  The notify state machine starts fresh on every daemon
// startup by construction (HashMap::new() in the run task).
