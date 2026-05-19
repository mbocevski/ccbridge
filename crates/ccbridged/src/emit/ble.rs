//! BlueZ NUS peripheral emitter stub.
//!
//! Gated behind `feature = "ble"`.  Pixelbook builds use `--no-default-features`
//! because no BLE controller is exposed through Sommelier.  All other emit
//! paths (swaync, ctrl, http) compile unconditionally.
//!
//! Full implementation: task 6bc0ede9.
