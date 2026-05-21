// SPDX-License-Identifier: MIT
//! Watches BlueZ for paired devices that advertise our service UUID.
//!
//! Yields [`DeviceEvent::Add`] for every device that's already paired at
//! startup and every device that becomes paired afterwards; yields
//! [`DeviceEvent::Remove`] when a device is unpaired or its UUIDs no longer
//! include ours.  Devices that are merely powered off (but still paired)
//! are kept in `Add` state — the per-device session handles reconnects.

use anyhow::Result;
use bluer::{Adapter, AdapterEvent, Address};
use futures_util::stream::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

/// Add/remove signal emitted by the discovery watcher.
#[derive(Debug, Clone, Copy)]
pub enum DeviceEvent {
    Add(Address),
    Remove(Address),
}

/// Spawn the BlueZ watcher; returns the `Add`/`Remove` event stream.
///
/// The watcher exits when the adapter is removed.  The receiver yields `None`
/// in that case.
pub async fn watch_paired(
    adapter: Adapter,
    service_uuid: Uuid,
) -> Result<mpsc::Receiver<DeviceEvent>> {
    let (tx, rx) = mpsc::channel(16);

    // Seed with already-paired devices.
    for addr in adapter.device_addresses().await? {
        if let Ok(dev) = adapter.device(addr) {
            if matches_service(&dev, service_uuid).await {
                let _ = tx.send(DeviceEvent::Add(addr)).await;
            }
        }
    }

    let monitor = adapter.events().await?;
    tokio::spawn(async move {
        if let Err(e) = run_loop(adapter, service_uuid, monitor, tx).await {
            warn!("ble: discovery loop exited: {e:#}");
        }
    });

    Ok(rx)
}

async fn run_loop<S>(
    adapter: Adapter,
    service_uuid: Uuid,
    mut monitor: S,
    tx: mpsc::Sender<DeviceEvent>,
) -> Result<()>
where
    S: futures_util::Stream<Item = AdapterEvent> + Unpin,
{
    while let Some(evt) = monitor.next().await {
        match evt {
            AdapterEvent::DeviceAdded(addr) => {
                let dev = match adapter.device(addr) {
                    Ok(d) => d,
                    Err(e) => {
                        debug!(%addr, "ble: device handle: {e}");
                        continue;
                    }
                };
                if matches_service(&dev, service_uuid).await {
                    let _ = tx.send(DeviceEvent::Add(addr)).await;
                }
            }
            AdapterEvent::DeviceRemoved(addr) => {
                let _ = tx.send(DeviceEvent::Remove(addr)).await;
            }
            AdapterEvent::PropertyChanged(_) => {
                // Adapter-level changes (e.g. powered toggle); not our concern.
            }
        }

        if tx.is_closed() {
            break;
        }
    }
    Ok(())
}

/// True iff the device is paired AND advertises the service UUID we're
/// looking for.  We don't gate on `is_connected()` — the session handles
/// the actual connect/reconnect.
async fn matches_service(dev: &bluer::Device, service_uuid: Uuid) -> bool {
    let paired = dev.is_paired().await.unwrap_or(false);
    if !paired {
        return false;
    }
    match dev.uuids().await {
        Ok(Some(uuids)) => uuids.contains(&service_uuid),
        _ => false,
    }
}
