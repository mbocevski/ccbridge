//! Emitter stubs.
//! swaync: task a7b7f234.   (always-on)
//! ble:    task 6bc0ede9.   (feature = "ble" — disabled on Pixelbook via --no-default-features)
//! ctrl:   task 1351d215.   (always-on)
//! http:   task b84f70d7.   (always-on, runtime-gated by config emit.http.enabled)

#[cfg(feature = "ble")]
pub mod ble;
pub mod ctrl;
pub mod swaync;
