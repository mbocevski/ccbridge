//! Hook ingest socket — Unix stream listener for `ccbridge-hook` events.
//!
//! # Protocol
//!
//! One connection per hook invocation.  The hook binary:
//! 1. Connects to `$XDG_RUNTIME_DIR/ccbridge/hooks.sock`.
//! 2. Writes one UTF-8 JSON line (the hook event).
//! 3. Reads one UTF-8 JSON line (the response, or EOF for passthrough).
//! 4. Exits.
//!
//! The daemon side:
//! 1. Accepts the connection and spawns a task.
//! 2. Reads one line, deserialises into [`HookEvent`].
//! 3. Sends [`AggregatorMsg::HookEvent`] to the aggregator.
//! 4. Receives a [`HookResponse`] back via a oneshot channel.
//! 5. Writes the response (or nothing) and closes.
//!
//! # Reliability invariant
//!
//! **Daemon-down ≠ Claude breaks.**  If the socket does not exist, the hook
//! binary exits 0 with no output and Claude Code behaves normally.  The
//! daemon side mirrors this: any error in an ingest task (parse failure,
//! aggregator gone, socket write failure, timeout) is logged and the
//! connection is closed silently.  We *never* propagate errors to the
//! aggregator and never panic.

use std::path::PathBuf;

use anyhow::Result;
use ccbridge_proto::hook::{PermissionDecision, PreToolUseOutput, PreToolUseResponse};
use ccbridge_proto::buddy::WireDecision;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, warn};

use crate::state::{AggregatorMsg, AggregatorTx, HookResponse};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Bind the hook ingest socket and spawn the accept loop.
///
/// The socket path is `<runtime_dir>/ccbridge/hooks.sock`.
///
/// **Under systemd** (`$XDG_RUNTIME_DIR` is set): the directory is provisioned
/// by `RuntimeDirectory=ccbridge` and cleaned on service stop.  `bind()` fails
/// loudly if the socket already exists — that means another ccbridged is running.
///
/// **Outside systemd** (dev-loop, `$XDG_RUNTIME_DIR` unset): a stale socket
/// from a prior unsupervised run is removed before binding so `cargo run`
/// iteration stays smooth.
///
/// Returns a handle to the spawned accept task.
pub fn spawn(runtime_dir: PathBuf, agg_tx: AggregatorTx) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = accept_loop(runtime_dir, agg_tx).await {
            error!("hook ingest accept loop failed: {e:#}");
        }
    })
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(runtime_dir: PathBuf, agg_tx: AggregatorTx) -> Result<()> {
    let sock_path = runtime_dir.join("ccbridge").join("hooks.sock");

    // Under systemd, `RuntimeDirectory=ccbridge` cleans up `$XDG_RUNTIME_DIR/ccbridge/`
    // on service stop, so a stale socket from a prior run should never exist in
    // production.  We do NOT remove it proactively:
    //
    // - `EADDRINUSE` from `bind()` means another ccbridged is running → fail loudly
    //   so systemd's `Restart=on-failure` doesn't loop against a live peer.
    // - A foreign socket at this path (wrong permissions, different owner) should
    //   also be a hard failure, not silently removed.
    //
    // Dev-loop exception: when `$XDG_RUNTIME_DIR` is unset we're running outside
    // systemd (e.g. `cargo run` in a terminal).  In that case clean up any stale
    // socket from a previous un-supervised run so iteration stays smooth.
    let under_systemd = std::env::var_os("XDG_RUNTIME_DIR").is_some();
    if !under_systemd && sock_path.exists() {
        tracing::debug!("dev-mode: removing stale socket at {}", sock_path.display());
        std::fs::remove_file(&sock_path)?;
    }

    let listener = UnixListener::bind(&sock_path)?;
    tracing::info!(path = %sock_path.display(), "hook ingest socket listening");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tx = agg_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, tx).await {
                        // Errors from individual connections are debug-level only —
                        // they're almost always Claude Code closing before reading.
                        debug!("hook connection error: {e:#}");
                    }
                });
            }
            Err(e) => {
                // Accept errors (e.g. EMFILE) are transient — log and continue.
                warn!("hook ingest accept error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

/// Handle one hook connection: read event → send to aggregator → write response.
async fn handle_connection(stream: UnixStream, agg_tx: mpsc::Sender<AggregatorMsg>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // --- 1. Read one JSON line ---
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    // Parse the hook event. On failure: log and close (passthrough semantics).
    let event = match serde_json::from_str(line) {
        Ok(e) => e,
        Err(err) => {
            warn!("hook: failed to parse event JSON: {err} — input: {line:.80}");
            // Write nothing → hook binary exits 0 → Claude Code behaves normally.
            return Ok(());
        }
    };
    debug!("hook: received event {}", event_name_str(&event));

    // --- 2. Send to aggregator ---
    let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
    if agg_tx
        .send(AggregatorMsg::HookEvent {
            event,
            respond: respond_tx,
        })
        .await
        .is_err()
    {
        // Aggregator is gone (daemon shutting down). Passthrough.
        warn!("hook: aggregator channel closed — passthrough");
        return Ok(());
    }

    // --- 3. Await response ---
    let response = match respond_rx.await {
        Ok(r) => r,
        Err(_) => {
            // Aggregator dropped the sender — shouldn't happen in normal operation.
            warn!("hook: aggregator dropped respond sender — passthrough");
            return Ok(());
        }
    };

    // --- 4. Write response ---
    match response {
        HookResponse::Passthrough => {
            // Write nothing. Hook exits 0 with no stdout → Claude Code takes over.
        }

        HookResponse::PermissionDecision(decision) => {
            let resp = pre_tool_use_response(decision);
            write_json_line(&mut writer, &resp).await?;
        }

        HookResponse::AwaitDecision {
            rx,
            approval_timeout,
            ..
        } => {
            // Wait for an emit module (swaync / BLE / ctrl-socket) to resolve.
            // On timeout: passthrough (Claude Code's own TUI handles it).
            match timeout(approval_timeout, rx).await {
                Ok(Ok(decision)) => {
                    let resp = pre_tool_use_response(decision);
                    write_json_line(&mut writer, &resp).await?;
                }
                Ok(Err(_)) => {
                    // oneshot sender dropped (aggregator shutting down mid-wait).
                    debug!("hook: approval sender dropped — passthrough");
                }
                Err(_elapsed) => {
                    debug!("hook: approval timeout elapsed — passthrough");
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialise `WireDecision` (BLE/ctrl protocol) to `hook::PermissionDecision`
/// (Claude Code hook stdout protocol).
///
/// | Wire      | Hook stdout |
/// |-----------|-------------|
/// | `Once`    | `Allow`     |
/// | `Deny`    | `Deny`      |
fn wire_to_hook_decision(d: WireDecision) -> PermissionDecision {
    match d {
        WireDecision::Once => PermissionDecision::Allow,
        WireDecision::Deny => PermissionDecision::Deny,
    }
}

/// Build the `PreToolUseResponse` that Claude Code expects on hook stdout.
fn pre_tool_use_response(decision: WireDecision) -> PreToolUseResponse {
    PreToolUseResponse {
        hook_specific_output: PreToolUseOutput {
            hook_event_name: "PreToolUse".to_owned(),
            permission_decision: wire_to_hook_decision(decision),
            permission_decision_reason: None,
            updated_input: None,
            additional_context: None,
        },
    }
}

/// Write a JSON value followed by `\n` to `writer`.
async fn write_json_line<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    Ok(())
}

/// Extract a display name from a hook event for debug logging.
fn event_name_str(event: &ccbridge_proto::hook::HookEvent) -> &'static str {
    use ccbridge_proto::hook::HookEvent;
    match event {
        HookEvent::PreToolUse(_) => "PreToolUse",
        HookEvent::PostToolUse(_) => "PostToolUse",
        HookEvent::Notification(_) => "Notification",
        HookEvent::Stop(_) => "Stop",
        HookEvent::SessionStart(_) => "SessionStart",
        HookEvent::UserPromptSubmit(_) => "UserPromptSubmit",
        HookEvent::SessionEnd(_) => "SessionEnd",
        HookEvent::Unknown => "Unknown",
    }
}
