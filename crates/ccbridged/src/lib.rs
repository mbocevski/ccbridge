//! ccbridged library — exposes internal modules for integration tests.
//!
//! This lib target exists solely so `tests/` can reference `ccbridged::state`
//! and `ccbridged::ingest`.  It is not shipped as a separate artifact; the
//! installed binary is the `ccbridged` bin target.

pub mod emit;
pub mod ingest;
pub mod state;
#[doc(hidden)]
pub mod setup;

// Internal modules required by the above.
pub(crate) mod config;
