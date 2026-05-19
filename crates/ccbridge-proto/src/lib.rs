// SPDX-License-Identifier: MIT
//! ccbridge-proto — shared wire-protocol types for the ccbridge ecosystem.
//!
//! Three modules, each independently re-exportable:
//!
//! * [`hook`]  — Claude Code hook event shapes (stdin JSON for each hook type)
//! * [`buddy`] — BLE Nordic-UART-Service wire format (heartbeat, turn events, …)
//! * [`ctrl`]  — Control-socket protocol (Hello, Subscribe, Command, Ack)
//!
//! All types derive [`serde::Serialize`] / [`serde::Deserialize`] and use
//! `#[serde(rename_all = "camelCase")]` or `#[serde(rename = "…")]` to match
//! the exact JSON field names on the wire.

pub mod buddy;
pub mod ctrl;
pub mod hook;
