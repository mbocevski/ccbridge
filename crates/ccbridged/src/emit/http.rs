// SPDX-License-Identifier: MIT
//! Optional HTTP `/status` endpoint for waybar custom modules.
//!
//! Disabled by default (`emit.http.enabled = false` in config).  When enabled,
//! binds to the configured address (default `127.0.0.1:9876`) and serves one
//! endpoint:
//!
//! - `GET /status` → JSON heartbeat snapshot (current state from the Aggregator).
//! - Everything else → 404.
//!
//! No auth, no CORS, no streaming.  Bound to `127.0.0.1` only by default.
//!
//! # Waybar example
//!
//! ```jsonc
//! "custom/ccbridge": {
//!   "format": "{} 󱙯",
//!   "interval": 10,
//!   "exec": "curl -sf http://127.0.0.1:9876/status | jq -r '\"\\(.tokens_today) toks\"'",
//!   "tooltip": false
//! }
//! ```
//!
//! Remember to enable the endpoint in `~/.config/ccbridge/config.toml`:
//!
//! ```toml
//! [emit.http]
//! enabled = true
//! ```

use std::convert::Infallible;
use std::net::SocketAddr;

use anyhow::Context;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::state::{AggregatorMsg, AggregatorTx};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Bind the HTTP server and spawn the accept loop.
///
/// Binding happens **before** the task is spawned so that the bound address
/// is returned to the caller.  This lets `main.rs` log the actual port on
/// success, and lets tests use `"127.0.0.1:0"` to get an OS-assigned port
/// without a probe-then-rebind race.
///
/// Returns `(JoinHandle, bound_addr)` on success, or `Err` if the bind failed.
/// On bind failure, the daemon should log the error and continue without the
/// HTTP endpoint.
pub async fn spawn(
    agg_tx: AggregatorTx,
    addr: SocketAddr,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("http: bind {addr}"))?;
    let bound = listener.local_addr()?;
    info!(addr = %bound, "http: /status endpoint listening");

    let handle = tokio::spawn(async move {
        if let Err(e) = serve_loop(listener, agg_tx).await {
            warn!("http: server exited with error: {e:#}");
        }
    });
    Ok((handle, bound))
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn serve_loop(listener: TcpListener, agg_tx: AggregatorTx) -> anyhow::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "http: accepted connection");
                let tx = agg_tx.clone();
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let svc = service_fn(move |req| handle(req, tx.clone()));
                    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                        debug!("http: connection error: {e}");
                    }
                });
            }
            Err(e) => {
                warn!("http: accept error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Request handler
// ---------------------------------------------------------------------------

async fn handle(
    req: Request<hyper::body::Incoming>,
    agg_tx: AggregatorTx,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/status") => {
            let (tx, rx) = oneshot::channel();
            if agg_tx
                .send(AggregatorMsg::GetHeartbeat { respond: tx })
                .await
                .is_err()
            {
                return Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "aggregator unavailable",
                ));
            }
            match rx.await {
                Ok(hb) => {
                    let body = match serde_json::to_vec(&hb) {
                        Ok(b) => b,
                        Err(e) => {
                            warn!("http: serialise heartbeat: {e}");
                            return Ok(error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "serialisation error",
                            ));
                        }
                    };
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(body)))
                        .unwrap())
                }
                Err(_) => Ok(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "aggregator dropped response",
                )),
            }
        }
        _ => Ok(error_response(StatusCode::NOT_FOUND, "not found")),
    }
}

fn error_response(status: StatusCode, msg: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}
