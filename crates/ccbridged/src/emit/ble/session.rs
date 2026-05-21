// SPDX-License-Identifier: MIT
//! Per-device GATT session.
//!
//! One [`start`] task per paired NUS device.  Owns the connect / GATT
//! enumeration / TX subscribe / RX write lifecycle, performs the on-connect
//! handshake (`OwnerMessage` → `TimeSync` → initial `Heartbeat`), and runs a
//! select loop that fans heartbeats out to the device and routes inbound
//! commands back into the aggregator.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bluer::gatt::remote::Characteristic;
use bluer::{Adapter, Address, Device};
use ccbridge_proto::buddy::{
    DeviceAck, DeviceCommand, Heartbeat, OwnerMessage, StatusAck, StatusData, TimeSync,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use super::{NUS_RX_CHAR_UUID, NUS_SERVICE_UUID, NUS_TX_CHAR_UUID};
use crate::state::{AggregatorMsg, AggregatorTx, PermissionDecisionResult};

/// Handle returned by [`start`].  Call [`Handle::shutdown`] to stop the task.
pub struct Handle {
    cancel: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

impl Handle {
    /// Signal the session task to exit on its next loop turn.
    pub fn shutdown(self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

/// Spawn the per-device session task.
pub fn start(
    adapter: Adapter,
    addr: Address,
    nickname: Option<String>,
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    owner: String,
    tz_offset_secs: i32,
) -> Handle {
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_child = cancel.clone();
    let task = tokio::spawn(async move {
        let label = nickname.clone().unwrap_or_else(|| addr.to_string());
        let mut backoff = Duration::from_secs(1);
        while !cancel_child.load(Ordering::SeqCst) {
            match run_once(
                &adapter,
                addr,
                &label,
                agg_tx.clone(),
                hb_rx.resubscribe(),
                &owner,
                tz_offset_secs,
                cancel_child.clone(),
            )
            .await
            {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(e) => warn!(device = %label, "ble: session error: {e:#}"),
            }

            if cancel_child.load(Ordering::SeqCst) {
                break;
            }
            sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
        debug!(device = %label, "ble: session exited");
    });
    Handle {
        cancel,
        _task: task,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    adapter: &Adapter,
    addr: Address,
    label: &str,
    agg_tx: AggregatorTx,
    mut hb_rx: broadcast::Receiver<Heartbeat>,
    owner: &str,
    tz_offset_secs: i32,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    let device = adapter
        .device(addr)
        .with_context(|| format!("ble: device handle for {addr}"))?;

    if !device.is_connected().await.unwrap_or(false) {
        debug!(device = %label, "ble: connecting");
        device
            .connect()
            .await
            .with_context(|| format!("connect to {label}"))?;
    }

    // Wait for service resolution (BlueZ resolves asynchronously).
    for _ in 0..30 {
        if device.is_services_resolved().await.unwrap_or(false) {
            break;
        }
        sleep(Duration::from_millis(200)).await;
    }

    let (rx_char, tx_char) = find_nus_chars(&device).await?;
    info!(device = %label, "ble: connected, NUS resolved");

    // notify_io() / write_io() give us tokio AsyncRead / AsyncWrite, which
    // means the same line-framing primitives ctrl.rs uses (BufReader +
    // read_line, write_all) work directly here.
    let read_io = tx_char
        .notify_io()
        .await
        .context("subscribe to NUS TX (notify_io)")?;
    let mut reader = BufReader::new(read_io);
    let mut writer = rx_char
        .write_io()
        .await
        .context("acquire NUS RX writer (write_io)")?;

    // --- Handshake: OwnerMessage, TimeSync, initial Heartbeat ---
    write_json_line(&mut writer, &OwnerMessage::new(owner.to_owned())).await?;
    write_json_line(
        &mut writer,
        &TimeSync {
            time: (
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
                tz_offset_secs,
            ),
        },
    )
    .await?;

    let (resp_tx, resp_rx) = oneshot::channel();
    if agg_tx
        .send(AggregatorMsg::GetHeartbeat { respond: resp_tx })
        .await
        .is_ok()
    {
        if let Ok(hb) = resp_rx.await {
            write_json_line(&mut writer, &hb).await?;
        }
    }

    // --- Steady state: fan out heartbeats, route inbound commands ---
    let mut line = String::new();
    let cancel_check_interval = tokio::time::Duration::from_millis(500);
    loop {
        if cancel.load(Ordering::SeqCst) {
            debug!(device = %label, "ble: cancellation requested, disconnecting");
            let _ = writer.shutdown().await;
            let _ = device.disconnect().await;
            return Ok(());
        }

        tokio::select! {
            biased;

            // Heartbeat fanout.
            recv = hb_rx.recv() => {
                match recv {
                    Ok(hb) => {
                        if let Err(e) = write_json_line(&mut writer, &hb).await {
                            debug!(device = %label, "ble: write hb failed: {e:#}");
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!(device = %label, "ble: hb broadcast lagged by {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }

            // Inbound line.
            n = reader.read_line(&mut line) => {
                match n {
                    Ok(0) => {
                        debug!(device = %label, "ble: TX read EOF (peripheral disconnected)");
                        return Ok(());
                    }
                    Ok(_) => {
                        let trimmed = line.trim_end_matches(['\n', '\r']).to_owned();
                        line.clear();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if let Err(e) = handle_inbound(&trimmed, &mut writer, &agg_tx, label).await {
                            debug!(device = %label, "ble: inbound dispatch error: {e:#}");
                        }
                    }
                    Err(e) => {
                        debug!(device = %label, "ble: read error: {e}");
                        return Ok(());
                    }
                }
            }

            // Periodic cancel-check tick so we don't sit forever on a quiet
            // connection when shutdown was requested.
            _ = sleep(cancel_check_interval) => {}
        }
    }
}

async fn find_nus_chars(device: &Device) -> Result<(Characteristic, Characteristic)> {
    for service in device.services().await? {
        if service.uuid().await? != NUS_SERVICE_UUID {
            continue;
        }
        let mut rx = None;
        let mut tx = None;
        for ch in service.characteristics().await? {
            let u = ch.uuid().await?;
            if u == NUS_RX_CHAR_UUID {
                rx = Some(ch);
            } else if u == NUS_TX_CHAR_UUID {
                tx = Some(ch);
            }
        }
        return match (rx, tx) {
            (Some(r), Some(t)) => Ok((r, t)),
            _ => anyhow::bail!("NUS service present but RX/TX characteristics missing"),
        };
    }
    anyhow::bail!("NUS service not found on device")
}

async fn write_json_line<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let mut buf = serde_json::to_vec(value).context("serialize ble line")?;
    buf.push(b'\n');
    writer.write_all(&buf).await.context("ble: write")?;
    writer.flush().await.context("ble: flush")?;
    Ok(())
}

async fn handle_inbound<W>(
    line: &str,
    writer: &mut W,
    agg_tx: &AggregatorTx,
    label: &str,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    match serde_json::from_str::<DeviceCommand>(line) {
        Ok(DeviceCommand::Permission { id, decision }) => {
            let (resp_tx, resp_rx) = oneshot::channel();
            let send_ok = agg_tx
                .send(AggregatorMsg::PermissionDecision {
                    tool_use_id: id.clone(),
                    decision,
                    respond: Some(resp_tx),
                })
                .await
                .is_ok();
            let ack = if !send_ok {
                DeviceAck::err("permission", "aggregator_gone")
            } else {
                match resp_rx.await {
                    Ok(PermissionDecisionResult::Applied) => DeviceAck::ok("permission"),
                    Ok(PermissionDecisionResult::UnknownId) => {
                        DeviceAck::err("permission", "unknown_id")
                    }
                    Err(_) => DeviceAck::err("permission", "aggregator_gone"),
                }
            };
            debug!(device = %label, decision = ?decision, id = %id, "ble: permission");
            write_json_line(writer, &ack).await
        }

        Ok(DeviceCommand::Status) => {
            let ack = StatusAck {
                ack: "status".to_owned(),
                ok: true,
                data: StatusData {
                    name: Some("ccbridge".to_owned()),
                    ..Default::default()
                },
            };
            write_json_line(writer, &ack).await
        }

        Ok(DeviceCommand::Name { name }) => {
            // Per-device nickname is config-managed; ack so the device knows
            // we received the request even though we don't apply it.
            debug!(device = %label, requested = %name, "ble: name change ignored (config-managed)");
            write_json_line(writer, &DeviceAck::ok("name")).await
        }

        Ok(DeviceCommand::Unpair) => {
            // Pairing is the OS's job — bluez owns the bond store.
            write_json_line(writer, &DeviceAck::err("unpair", "manage_via_os")).await
        }

        Err(_) => {
            if let Ok(v) = serde_json::from_str::<Value>(line) {
                if let Some(cmd) = v.get("cmd").and_then(Value::as_str) {
                    return write_json_line(writer, &DeviceAck::err(cmd, "unknown msg")).await;
                }
            }
            debug!(device = %label, len = line.len(), "ble: unparseable inbound");
            Ok(())
        }
    }
}
