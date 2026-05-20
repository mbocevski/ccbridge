// SPDX-License-Identifier: MIT
//! Emitter modules.
//! notify: freedesktop notification daemon — always-on
//! ble:    BlueZ NUS placeholder for the future BLE bridge — feature = "ble"
//! ctrl:   bidirectional control socket — always-on
//! http:   optional /status endpoint — runtime-gated by config

#[cfg(feature = "ble")]
pub mod ble;
pub mod ctrl;
pub mod http;
pub mod notify;
