// SPDX-License-Identifier: MIT
//! ccbridged library — exposes internal modules for integration tests.
//!
//! This lib target exists solely so `tests/` can reference `ccbridged::state`
//! and `ccbridged::ingest`.  It is not shipped as a separate artifact; the
//! installed binary is the `ccbridged` bin target.

pub mod emit;
pub mod ingest;
pub mod permission;
pub mod state;
#[doc(hidden)]
pub mod setup;

// Config module — public so tests and main.rs can reach it.
pub mod config;
