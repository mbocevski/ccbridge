//! BLE Hardware Buddy wire-protocol types.
//!
//! Mirrors the exact JSON shapes from `fun/claude-desktop-buddy/REFERENCE.md`.
//! The desktop app / ccbridged emits these; BLE devices consume them.
//!
//! Key types (stubs — filled in by task 1ecf3330):
//!   [`Heartbeat`]  — periodic snapshot (total, running, waiting, tokens, …)
//!   [`TurnEvent`]  — one-shot assistant turn content
//!   [`TimeSync`]   — epoch + timezone offset sent on connect
//!   [`OwnerCmd`]   — owner name sent on connect

// Placeholder — full serde types land in task 1ecf3330.
