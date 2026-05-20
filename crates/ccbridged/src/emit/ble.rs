// SPDX-License-Identifier: MIT
//! BlueZ BLE bridge.
//!
//! ccbridged plays the **central** role: it scans for and connects to
//! a peripheral that advertises the Nordic UART Service.  Gated behind
//! `feature = "ble"`.
