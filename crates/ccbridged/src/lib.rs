//! ccbridged library — exposes internal modules for integration tests.
//!
//! This lib target exists solely so `tests/` can reference `ccbridged::state`
//! and `ccbridged::ingest`.  It is not shipped as a separate artifact; the
//! installed binary is the `ccbridged` bin target.

pub mod ingest;
pub mod state;

// Internal modules required by the above.
pub(crate) mod config;
pub(crate) mod emit;
