// SPDX-License-Identifier: MIT
//! End-to-end test: daemon reads `~/.config/ccbridge/config.toml` and applies
//! `approval_timeout` correctly.
//!
//! This catches regressions where a config field is renamed or the loader
//! stops wiring a key through to the aggregator.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ccbridged::ingest::hooks as hook_ingest;
use ccbridged::state::{spawn as spawn_aggregator, AggregatorMsg};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write a config.toml with an extremely short timeout, load it, and return
/// the loaded `Config`.
fn write_and_load_config(dir: &TempDir, timeout_ms: u64) -> ccbridged::config::Config {
    let config_dir = dir.path().join("ccbridge");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.toml");

    std::fs::write(
        &config_path,
        format!("[approvals]\ntimeout_ms = {timeout_ms}\n"),
    )
    .unwrap();

    // Point XDG_CONFIG_HOME at the tempdir so Config::load() finds our file.
    std::env::set_var("XDG_CONFIG_HOME", dir.path());
    let cfg = ccbridged::config::Config::load().expect("config must load");
    // Reset so other tests aren't affected.
    std::env::remove_var("XDG_CONFIG_HOME");
    cfg
}

// ---------------------------------------------------------------------------
// Test: config.toml timeout wires through to the approval flow
// ---------------------------------------------------------------------------

/// Write a config with `timeout_ms = 100`, spawn the aggregator + hook ingest
/// with that timeout, send a PreToolUse event (no decision), and verify the
/// timeout fires within ~200ms — not the default 30 seconds.
///
/// This confirms the full path:
///   config.toml → Config::load() → spawn_aggregator(timeout) →
///   start_intercept(fallback) → AwaitDecision { approval_timeout } →
///   Err(_elapsed) in hook handler
#[tokio::test]
async fn config_timeout_ms_wires_through_to_approval_flow() {
    let dir = TempDir::new().unwrap();
    let config = write_and_load_config(&dir, 100);
    assert_eq!(
        config.approvals.timeout_ms, 100,
        "config must reflect written value"
    );

    let runtime_dir = dir.path().to_path_buf();
    std::fs::create_dir_all(runtime_dir.join("ccbridge")).unwrap();

    let (agg_tx, _hb_rx) = spawn_aggregator(
        config.approvals.timeout(),
        config.approvals.fallback,
        Arc::new(arc_swap::ArcSwap::new(Arc::new(
            ccbridged::permission::Allowlist::empty(),
        ))),
    );
    hook_ingest::spawn(runtime_dir.clone(), agg_tx.clone());

    // Give the accept loop a moment to bind.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sock = runtime_dir.join("ccbridge").join("hooks.sock");
    let stream = UnixStream::connect(&sock)
        .await
        .expect("connect hooks.sock");
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);

    // Write a PreToolUse event — no decision will be sent, so the 100ms
    // timeout should fire.
    let event = serde_json::json!({
        "session_id": "sess_e2e",
        "transcript_path": "/tmp/e2e.jsonl",
        "cwd": "/tmp",
        "permission_mode": "default",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "echo e2e"},
        "tool_use_id": "toolu_e2e_001"
    });
    let mut bytes = serde_json::to_vec(&event).unwrap();
    bytes.push(b'\n');
    w.write_all(&bytes).await.unwrap();

    let start = Instant::now();
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("hook response");
    let elapsed = start.elapsed();

    // The timeout should fire at ~100ms.  We give 500ms of headroom.
    assert!(
        elapsed < Duration::from_millis(500),
        "approval timeout should fire near 100ms, not 30s; elapsed: {elapsed:?}"
    );

    // The response should be the timeout fallback (default = Passthrough → Ask).
    let v: serde_json::Value = serde_json::from_str(line.trim()).expect("valid JSON");
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecision"], "ask",
        "default fallback on timeout must be 'ask'"
    );

    // After timeout, the aggregator should have cleared the pending state.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    agg_tx
        .send(AggregatorMsg::GetHeartbeat { respond: tx })
        .await
        .unwrap();
    let hb = rx.await.unwrap();
    assert_eq!(hb.waiting, 0, "aggregator must clear waiting after timeout");
}
