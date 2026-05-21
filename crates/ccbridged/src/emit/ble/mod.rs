// SPDX-License-Identifier: MIT
//! BlueZ NUS bridge — central role.
//!
//! ccbridged watches BlueZ for paired devices that advertise the Nordic UART
//! Service (NUS) and runs one session per device.  Pairing is the user's
//! job (`bluetoothctl`, blueman, or any GUI Bluetooth tool); we only consume
//! already-paired devices.  This keeps key storage, scanning UI, and trust
//! decisions in the OS where they belong.
//!
//! Wire protocol is the same JSON-on-NUS dialect used by every other ccbridge
//! emit module — see `ccbridge_proto::buddy`.

mod discovery;
mod session;

use std::collections::HashMap;
use std::sync::Arc;

use bluer::Address;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::BleConfig;
use crate::state::AggregatorTx;
use ccbridge_proto::buddy::Heartbeat;

pub use discovery::{DeviceEvent, watch_paired};

/// Standard Nordic UART Service UUID.  The desktop expects every ccbridge
/// peripheral to advertise this so OS-level scanners can identify our gear.
pub const NUS_SERVICE_UUID: Uuid = Uuid::from_u128(0x6e400001_b5a3_f393_e0a9_e50e24dcca9e);
/// RX characteristic — desktop *writes* commands here (peripheral input).
pub const NUS_RX_CHAR_UUID: Uuid = Uuid::from_u128(0x6e400002_b5a3_f393_e0a9_e50e24dcca9e);
/// TX characteristic — desktop *subscribes* for notifications (peripheral
/// output).
pub const NUS_TX_CHAR_UUID: Uuid = Uuid::from_u128(0x6e400003_b5a3_f393_e0a9_e50e24dcca9e);

/// Spawn the BLE manager.
///
/// Returns a [`JoinHandle`] that exits when bluez is permanently unreachable
/// (e.g. `bluetoothd` not installed); transient errors are retried inside.
pub fn spawn(
    config: BleConfig,
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    owner: String,
    tz_offset_secs: i32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run(config, agg_tx, hb_rx, owner, tz_offset_secs).await {
            warn!("ble: manager exited: {e:#}");
        }
    })
}

async fn run(
    config: BleConfig,
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    owner: String,
    tz_offset_secs: i32,
) -> anyhow::Result<()> {
    let service_uuid = config
        .service_uuid
        .parse::<Uuid>()
        .map_err(|e| anyhow::anyhow!("ble: invalid service_uuid {:?}: {e}", config.service_uuid))?;

    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    adapter.set_powered(true).await?;
    let adapter_name = adapter.name().to_owned();
    let adapter_address = adapter.address().await?;
    info!(
        adapter = %adapter_name,
        address = %adapter_address,
        "ble: adapter ready",
    );

    let overrides: Arc<HashMap<Address, crate::config::BleDeviceOverride>> = Arc::new(
        config
            .device
            .iter()
            .filter_map(|d| {
                d.address
                    .parse::<Address>()
                    .ok()
                    .map(|addr| (addr, d.clone()))
            })
            .collect(),
    );

    // Per-device session handles, so we can stop one when bluez says the
    // device was unpaired or disabled mid-flight.
    let mut sessions: HashMap<Address, session::Handle> = HashMap::new();

    let mut events = watch_paired(adapter.clone(), service_uuid).await?;
    while let Some(evt) = events.recv().await {
        match evt {
            DeviceEvent::Add(addr) => {
                if let Some(o) = overrides.get(&addr) {
                    if o.disabled {
                        debug!(%addr, "ble: device disabled by config");
                        continue;
                    }
                }
                if sessions.contains_key(&addr) {
                    continue;
                }
                info!(%addr, "ble: device available, starting session");
                let h = session::start(
                    adapter.clone(),
                    addr,
                    overrides.get(&addr).and_then(|o| o.nickname.clone()),
                    agg_tx.clone(),
                    hb_rx.resubscribe(),
                    owner.clone(),
                    tz_offset_secs,
                );
                sessions.insert(addr, h);
            }
            DeviceEvent::Remove(addr) => {
                if let Some(h) = sessions.remove(&addr) {
                    info!(%addr, "ble: device gone, stopping session");
                    h.shutdown();
                }
            }
        }
    }

    Ok(())
}
