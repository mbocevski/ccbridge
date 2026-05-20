// SPDX-License-Identifier: MIT
//! Control socket emitter — bidirectional NDJSON over a Unix domain socket.
//!
//! Binds `$XDG_RUNTIME_DIR/ccbridge/ctrl.sock` (mode 0600) and speaks the
//! `ccbridge_proto::ctrl` protocol.
//!
//! # Connection lifecycle
//!
//! ```text
//! client connects
//!   ← HelloMessage          (version, owner name, epoch + tz offset)
//!   ← Heartbeat             (current snapshot via GetHeartbeat)
//!   → Command               (optional: Subscribe, Permission, Status, …)
//!   ← Ack / StatusAck       (every command receives exactly one ack)
//!   ← Heartbeat             (if subscribed to Topic::Heartbeat, streamed on change)
//! client closes
//! ```
//!
//! # Reliability
//!
//! * Bind failure → propagate (daemon won't start — socket dir missing).
//! * Accept errors (EMFILE, etc.) → `warn!`, continue.
//! * Per-connection write failures → `debug!`, close silently.
//! * Aggregator gone → `warn!`, close.
//! * Client disconnect → `debug!`, close cleanly.
//! * Task exit is non-fatal to the daemon.

use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use ccbridge_proto::buddy::{Heartbeat, StatusAck, StatusData};
use ccbridge_proto::ctrl::{Ack, Command, Hello, HelloMessage, Topic};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, oneshot};
use tracing::{debug, info, warn};

use crate::state::{AggregatorMsg, AggregatorTx};

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Spawn the control-socket accept loop as a tokio task.
///
/// `tz_offset_secs` should be captured at daemon startup (before tokio spawns
/// its thread pool) via `time::UtcOffset::current_local_offset()`.  Passing it
/// explicitly avoids the multi-threaded TZ-read unsafety in the `time` crate.
///
/// `owner` is the display name sent in the `HelloMessage` on every new
/// connection.  Resolve it once at startup (e.g. `git config user.name`,
/// falling back to `$USER`, then `"unknown"`) and pass the cached value here.
///
/// The returned [`tokio::task::JoinHandle`] exits when the accept loop fails
/// (hard bind error is fatal; the daemon will not start).  Per-connection
/// errors are non-fatal.
pub fn spawn(
    runtime_dir: PathBuf,
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    owner: String,
    tz_offset_secs: i32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = accept_loop(runtime_dir, agg_tx, hb_rx, owner, tz_offset_secs).await {
            warn!("ctrl: accept loop failed: {e:#}");
        }
    })
}

// ---------------------------------------------------------------------------
// Two startup helpers called by main.rs before spawning the tokio runtime.
// ---------------------------------------------------------------------------

/// Resolve the daemon owner name for `HelloMessage`.
///
/// Priority: `git config user.name` → `$USER` → `"unknown"`.
///
/// Must be called after the tokio runtime is started (uses
/// `tokio::process::Command`).
pub async fn resolve_owner() -> String {
    // Try git config user.name with a 5s timeout.
    if let Ok(Ok(output)) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new("git")
            .args(["config", "user.name"])
            .output(),
    )
    .await
    {
        if output.status.success() {
            let name = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !name.is_empty() {
                return name;
            }
        }
    }
    // Fall back to $USER
    if let Ok(user) = std::env::var("USER") {
        if !user.is_empty() {
            return user;
        }
    }
    "unknown".to_owned()
}

/// Resolve the local UTC offset in seconds, safe to call before the tokio
/// thread pool starts (avoids the multi-threaded glibc TZ unsafety).
///
/// Returns 0 if the local offset cannot be determined.
pub fn resolve_tz_offset() -> i32 {
    time::UtcOffset::current_local_offset()
        .map(|o| o.whole_seconds())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(
    runtime_dir: PathBuf,
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    owner: String,
    tz_offset_secs: i32,
) -> Result<()> {
    let sock_path = runtime_dir.join("ccbridge").join("ctrl.sock");

    // Same stale-socket heuristic as ingest::hooks::accept_loop.
    // Under systemd RuntimeDirectory= cleans up on service stop.
    // In dev mode (XDG_RUNTIME_DIR unset) remove stale sockets so
    // `cargo run` iteration stays smooth.
    let under_systemd = std::env::var_os("XDG_RUNTIME_DIR").is_some();
    if !under_systemd && sock_path.exists() {
        debug!(
            "dev-mode: removing stale ctrl socket at {}",
            sock_path.display()
        );
        std::fs::remove_file(&sock_path)?;
    }

    let listener = UnixListener::bind(&sock_path)?;
    // Mode 0600 — only the owning user can connect.
    std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))?;
    info!(path = %sock_path.display(), "ctrl socket listening");

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let tx = agg_tx.clone();
                // Each connection gets its own broadcast receiver so it tracks
                // from the current head, independent of other connections.
                let rx = hb_rx.resubscribe();
                let owner = owner.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, tx, rx, &owner, tz_offset_secs).await
                    {
                        debug!("ctrl: connection error: {e:#}");
                    }
                });
            }
            Err(e) => {
                warn!("ctrl: accept error: {e}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: UnixStream,
    agg_tx: AggregatorTx,
    mut hb_rx: broadcast::Receiver<Heartbeat>,
    owner: &str,
    tz_offset_secs: i32,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // --- 1. Send HelloMessage ---
    let hello = build_hello(owner, tz_offset_secs);
    write_json_line(&mut writer, &hello).await?;

    // --- 2. Send initial heartbeat snapshot ---
    let (hb_tx, hb_oneshot_rx) = oneshot::channel();
    if agg_tx
        .send(AggregatorMsg::GetHeartbeat { respond: hb_tx })
        .await
        .is_err()
    {
        warn!("ctrl: aggregator gone on initial GetHeartbeat");
        return Ok(());
    }
    match hb_oneshot_rx.await {
        Ok(hb) => write_json_line(&mut writer, &hb).await?,
        Err(_) => {
            warn!("ctrl: aggregator dropped GetHeartbeat sender");
            return Ok(());
        }
    }

    // --- 3. Command / heartbeat fanout loop ---
    // Per-connection subscription set.  Starts empty — client must subscribe
    // to receive streamed heartbeats.
    let mut topics: HashSet<Topic> = HashSet::new();
    let mut line = String::new();

    loop {
        // The heartbeat branch is only enabled when Topic::Heartbeat is subscribed.
        // When disabled the receiver silently accumulates lag — correct, because
        // if the client later subscribes they'll skip to current state.
        let hb_subscribed = topics.contains(&Topic::Heartbeat);

        tokio::select! {
            // --- inbound command ---
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        // EOF — clean disconnect.
                        debug!("ctrl: client disconnected");
                        break;
                    }
                    Ok(_) => {
                        // Per-line 1 MiB cap — a legitimate JSON command is
                        // never this large; oversized input is malformed or
                        // malicious.  Drop the connection.
                        if line.len() > 1 << 20 {
                            warn!(
                                "ctrl: inbound line too large ({} bytes) — dropping connection",
                                line.len()
                            );
                            break;
                        }
                        let trimmed = line.trim_end_matches(['\n', '\r']).to_owned();
                        line.clear();
                        if let Err(e) = handle_command(
                            &trimmed,
                            &mut writer,
                            &agg_tx,
                            &mut topics,
                        ).await {
                            debug!("ctrl: command handler error: {e:#}");
                            break;
                        }
                    }
                    Err(e) => {
                        debug!("ctrl: read error: {e}");
                        break;
                    }
                }
            }

            // --- heartbeat fanout (only when subscribed) ---
            recv = hb_rx.recv(), if hb_subscribed => {
                match recv {
                    Ok(hb) => {
                        if let Err(e) = write_json_line(&mut writer, &hb).await {
                            debug!("ctrl: write error sending heartbeat: {e}");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("ctrl: broadcast lagged by {n} — skipping");
                        // Continue — next heartbeat arrives within 10 s.
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("ctrl: broadcast channel closed — exiting connection");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Command handler
// ---------------------------------------------------------------------------

async fn handle_command<W>(
    line: &str,
    writer: &mut W,
    agg_tx: &AggregatorTx,
    topics: &mut HashSet<Topic>,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    // Attempt to parse as a known Command.
    match serde_json::from_str::<Command>(line) {
        Ok(cmd) => dispatch_command(cmd, writer, agg_tx, topics).await,

        Err(_parse_err) => {
            // Try to fish out the "cmd" field for a well-formed unknown_command ack.
            if let Ok(raw) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(cmd_str) = raw.get("cmd").and_then(|v| v.as_str()) {
                    debug!("ctrl: unknown command {:?}", cmd_str);
                    return write_json_line(writer, &Ack::unknown(cmd_str)).await;
                }
            }
            // Malformed JSON — log and continue without closing.
            debug!("ctrl: unparseable input: {:.80}", line);
            Ok(())
        }
    }
}

async fn dispatch_command<W>(
    cmd: Command,
    writer: &mut W,
    agg_tx: &AggregatorTx,
    topics: &mut HashSet<Topic>,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    match cmd {
        Command::Subscribe { topics: new_topics } => {
            topics.extend(new_topics);
            write_json_line(writer, &Ack::ok("subscribe")).await
        }

        Command::Unsubscribe { topics: rem_topics } => {
            for t in rem_topics {
                topics.remove(&t);
            }
            write_json_line(writer, &Ack::ok("unsubscribe")).await
        }

        Command::Permission { id, decision } => {
            // Round-trip through the aggregator so the ack reflects whether
            // the decision actually applied.  Stale clicks (id no longer in
            // the pending map after a daemon restart or post-timeout race)
            // ack with ok:false / "unknown_id" so the client knows the
            // user's input was discarded.
            use crate::state::PermissionDecisionResult;
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            if agg_tx
                .send(AggregatorMsg::PermissionDecision {
                    tool_use_id: id,
                    decision,
                    respond: Some(resp_tx),
                })
                .await
                .is_err()
            {
                // Aggregator is shutting down — connection will close on the
                // next iteration. Ack with the failure for this one cmd.
                return write_json_line(writer, &Ack::err("permission", "aggregator_gone")).await;
            }
            let ack = match resp_rx.await {
                Ok(PermissionDecisionResult::Applied) => Ack::ok("permission"),
                Ok(PermissionDecisionResult::UnknownId) => Ack::err("permission", "unknown_id"),
                Err(_) => Ack::err("permission", "aggregator_gone"),
            };
            write_json_line(writer, &ack).await
        }

        Command::Status => {
            // buddy::StatusAck is the canonical wire shape for {"cmd":"status"}
            // per spec ("ctrl protocol mirrors BLE NUS where types overlap").
            // ctrl::Ack is for everything else.
            let ack = StatusAck {
                ack: "status".to_owned(),
                ok: true,
                data: StatusData {
                    name: Some("ccbridge".to_owned()),
                    // We're a software bridge, not a battery-powered device.
                    // All hardware fields are intentionally absent.
                    ..Default::default()
                },
            };
            write_json_line(writer, &ack).await
        }

        // Not yet implemented — ack as unknown_command per spec.
        // Replay: heartbeat history not kept by the aggregator yet.
        // ForgetDevice: BLE-specific, lands with emit::ble.
        // Simulate: gated by config.allow_simulate which doesn't exist yet.
        Command::Replay { .. } => write_json_line(writer, &Ack::unknown("replay")).await,
        Command::ForgetDevice { .. } => {
            write_json_line(writer, &Ack::unknown("forget_device")).await
        }
        Command::Simulate { .. } => write_json_line(writer, &Ack::unknown("simulate")).await,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_hello(owner: &str, tz_offset_secs: i32) -> HelloMessage {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    HelloMessage {
        hello: Hello {
            version: 1,
            owner: owner.to_owned(),
            time: (epoch, tz_offset_secs),
        },
    }
}

/// Serialise `value` as a JSON line (`\n` terminated) and write it to `writer`.
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
