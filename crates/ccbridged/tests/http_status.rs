//! Integration tests for the HTTP /status endpoint.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ccbridge_proto::buddy::Heartbeat;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use ccbridged::emit::http as http_emit;
use ccbridged::state::{DEFAULT_APPROVAL_TIMEOUT, spawn as spawn_aggregator};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spawn an aggregator + HTTP server on a random port; return the bound address.
async fn setup() -> (ccbridged::state::AggregatorTx, SocketAddr) {
    let (agg_tx, _hb_rx) = spawn_aggregator(
        DEFAULT_APPROVAL_TIMEOUT,
        ccbridged::config::Fallback::default(),
        Arc::new(arc_swap::ArcSwap::new(Arc::new(
            ccbridged::permission::Allowlist::empty(),
        ))),
    );
    let (_, addr) = http_emit::spawn(agg_tx.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .expect("http server must bind");
    (agg_tx, addr)
}

/// Send a minimal HTTP/1.1 request and return the status code + body.
async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf).into_owned();

    // Parse status code from "HTTP/1.1 NNN ..."
    let status: u16 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Body is after the blank line.
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("").to_owned();

    (status, body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_status_returns_heartbeat_json() {
    let (_agg_tx, addr) = setup().await;

    let (status, body) = http_get(addr, "/status").await;
    assert_eq!(status, 200, "GET /status must return 200");

    // Body must be valid JSON that deserialises into a Heartbeat.
    let hb: Heartbeat = serde_json::from_str(&body).unwrap_or_else(|e| {
        panic!("GET /status body is not a valid Heartbeat: {e}\nbody: {body}")
    });

    assert_eq!(hb.total, 0, "fresh aggregator has no sessions");
    assert!(hb.tokens_today < u64::MAX);
}

#[tokio::test]
async fn unknown_path_returns_404() {
    let (_agg_tx, addr) = setup().await;
    let (status, _) = http_get(addr, "/unknown").await;
    assert_eq!(status, 404, "unknown paths must return 404");
}

#[tokio::test]
async fn post_status_returns_404() {
    // Only GET /status is handled; POST is not.
    let (_agg_tx, addr) = setup().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = "POST /status HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf);
    let status: u16 = resp
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert_eq!(status, 404);
}
