//! Control-socket protocol types.
//!
//! The control socket (`$XDG_RUNTIME_DIR/ccbridge/ctrl.sock`) is the stable
//! bidirectional interface for future TUI/GUI/CLI clients.  The framing is
//! newline-delimited JSON — one object per line, UTF-8 — mirroring the BLE
//! NUS wire format so serde types are shared.
//!
//! Key types (stubs — filled in by task cd003d15):
//!   [`Hello`]    — server → client immediately on connect
//!   [`Subscribe`] / [`Unsubscribe`] — client chooses topic streams
//!   [`Command`]  — client → server (permission, status, replay, …)
//!   [`Ack`]      — server → client, echoes cmd name

// Placeholder — full serde types land in task cd003d15.
