// SPDX-License-Identifier: MIT
//! BlueZ BLE bridge — placeholder for v2.
//!
//! ccbridged plays the **central** role (it scans for and connects to
//! the buddy peripheral, not the other way around).  An older comment
//! in this file described a peripheral emitter; that was incorrect and
//! is corrected here so the module's intent is unambiguous if someone
//! grep-searches it before v2 begins.
//!
//! Gated behind `feature = "ble"`.  When v2 implementation starts the
//! current plan (recorded in `docs/v2-ble-readiness.md`) is to delete
//! this module entirely and ship the BLE bridge as a sibling binary
//! (`ccbridge-ble`) that talks to ccbridged via the existing ctrl
//! socket — failure isolation, easier iteration, no daemon rebuilds.
//!
//! Until then the module is empty; the `default = ["ble"]` Cargo
//! feature is a reservation, not a working build flag.
