//! Emitter modules.
//! notify: freedesktop notification daemon (swaync/mako/dunst/GNOME/KDE) — always-on
//! ble:    BlueZ NUS peripheral — feature = "ble", disabled on Pixelbook
//! ctrl:   bidirectional control socket — always-on
//! http:   optional /status endpoint — runtime-gated by config

#[cfg(feature = "ble")]
pub mod ble;
pub mod ctrl;
pub mod http;
pub mod notify;
