// SPDX-License-Identifier: MIT
//! Integration tests for the JSONL watcher.
//!
//! Uses a tempdir as a fake `~/.claude/projects/` directory.
//! Writes assistant-message JSONL lines into a file and asserts that the
//! aggregator receives `TokensUpdate` messages with the right values.

use std::io::Write;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::sync::oneshot;

use ccbridged::ingest::jsonl::{spawn_watcher, PersistedTokens};
use ccbridged::state::{AggregatorMsg, DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write one assistant JSONL line with the given output_tokens.
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
            "content": [{"type": "text", "text": "Some assistant response text."}]
        }
    }))
    .unwrap()
        + "\n"
}

/// Write a non-assistant line (should be ignored by the watcher).
fn user_line() -> String {
    serde_json::to_string(&json!({
        "type": "user",
        "message": {"role": "user", "content": "hello"}
    }))
    .unwrap()
        + "\n"
}

/// Drain `TokensUpdate` messages from `agg_tx` by polling `GetHeartbeat`
/// until `heartbeat.tokens >= expected_cumulative` or we time out.
async fn wait_for_tokens(
    agg_tx: &ccbridged::state::AggregatorTx,
    expected_cumulative: u64,
) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for tokens={expected_cumulative}");
        }
        let (tx, rx) = oneshot::channel();
        agg_tx
            .send(AggregatorMsg::GetHeartbeat { respond: tx })
            .await
            .unwrap();
        let hb = rx.await.unwrap();
        if hb.tokens >= expected_cumulative {
            return hb.tokens;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two assistant lines appended after watcher start → aggregator sees both.
#[tokio::test]
async fn watcher_picks_up_new_lines() {
    let dir = TempDir::new().unwrap();
    let projects_dir = dir.path().to_path_buf();
    let state_path = dir.path().join("tokens.json");

    let (agg_tx, _hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));

    // Start watcher (snapshots existing files — dir is empty, offset = 0).
    spawn_watcher(
        projects_dir.clone(),
        state_path.clone(),
        agg_tx.clone(),
        PersistedTokens::default(),
    );

    // Give watcher a moment to set up the notify subscription.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write a JSONL file with two assistant lines.
    let jsonl_path = projects_dir.join("session1.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).unwrap();
        write!(f, "{}", assistant_line(150)).unwrap();
        write!(f, "{}", assistant_line(250)).unwrap();
    }

    // Aggregator should eventually see cumulative = 400.
    let total = wait_for_tokens(&agg_tx, 400).await;
    assert_eq!(total, 400);
}

/// User lines are ignored; only output_tokens from assistant messages count.
#[tokio::test]
async fn watcher_ignores_non_assistant_lines() {
    let dir = TempDir::new().unwrap();
    let projects_dir = dir.path().to_path_buf();
    let state_path = dir.path().join("tokens.json");

    let (agg_tx, _hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));
    spawn_watcher(
        projects_dir.clone(),
        state_path,
        agg_tx.clone(),
        PersistedTokens::default(),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let jsonl_path = projects_dir.join("session_mixed.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).unwrap();
        write!(f, "{}", user_line()).unwrap();           // 0 tokens
        write!(f, "{}", assistant_line(100)).unwrap();  // 100 tokens
        write!(f, "{}", user_line()).unwrap();           // 0 tokens
    }

    let total = wait_for_tokens(&agg_tx, 100).await;
    assert_eq!(total, 100);
}

/// Malformed lines in a JSONL file don't crash the watcher.
#[tokio::test]
async fn watcher_tolerates_malformed_lines() {
    let dir = TempDir::new().unwrap();
    let projects_dir = dir.path().to_path_buf();
    let state_path = dir.path().join("tokens.json");

    let (agg_tx, _hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));
    spawn_watcher(
        projects_dir.clone(),
        state_path,
        agg_tx.clone(),
        PersistedTokens::default(),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let jsonl_path = projects_dir.join("session_bad.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).unwrap();
        writeln!(f, "not json at all").unwrap();
        writeln!(f, "{{broken").unwrap();
        write!(f, "{}", assistant_line(77)).unwrap(); // valid line after bad ones
    }

    // Watcher must not have crashed — valid line should still produce tokens.
    let total = wait_for_tokens(&agg_tx, 77).await;
    assert_eq!(total, 77);
}

/// Non-JSONL files in the directory are ignored silently.
#[tokio::test]
async fn watcher_ignores_non_jsonl_files() {
    let dir = TempDir::new().unwrap();
    let projects_dir = dir.path().to_path_buf();
    let state_path = dir.path().join("tokens.json");

    let (agg_tx, _hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));
    spawn_watcher(
        projects_dir.clone(),
        state_path,
        agg_tx.clone(),
        PersistedTokens::default(),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Write a .txt file (should be ignored).
    std::fs::write(projects_dir.join("readme.txt"), b"hello").unwrap();

    // Write a real JSONL file.
    let mut f = std::fs::File::create(projects_dir.join("real.jsonl")).unwrap();
    write!(f, "{}", assistant_line(55)).unwrap();

    let total = wait_for_tokens(&agg_tx, 55).await;
    // Only the JSONL file's tokens count.
    assert_eq!(total, 55);
}

/// Initial token state loaded from PersistedTokens is reflected in the
/// first heartbeat.
#[tokio::test]
async fn watcher_loads_initial_token_state() {
    let dir = TempDir::new().unwrap();
    let projects_dir = dir.path().to_path_buf();
    let state_path = dir.path().join("tokens.json");

    let (agg_tx, _hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, ccbridged::config::Fallback::default(), std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(ccbridged::permission::Allowlist::empty()))));

    // Simulate a prior daemon run with 10_000 cumulative tokens.
    let initial = PersistedTokens {
        date: "2026-05-19".to_owned(),
        today: 5_000,
        cumulative: 10_000,
    };

    // Pre-seed the aggregator with the loaded tokens before starting the watcher.
    agg_tx
        .send(AggregatorMsg::TokensUpdate { output_tokens: 10_000 })
        .await
        .unwrap();

    spawn_watcher(projects_dir, state_path, agg_tx.clone(), initial);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Heartbeat should reflect the pre-seeded cumulative.
    let (tx, rx) = oneshot::channel();
    agg_tx
        .send(AggregatorMsg::GetHeartbeat { respond: tx })
        .await
        .unwrap();
    let hb = rx.await.unwrap();
    assert_eq!(hb.tokens, 10_000);
}
