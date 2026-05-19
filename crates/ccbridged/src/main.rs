//! ccbridged — Claude Code aggregator bridge daemon.
//!
//! Aggregates state across all running Claude Code sessions on this machine
//! and re-emits the claude-desktop-buddy BLE wire protocol, swaync
//! notifications, and a bidirectional control socket.
//!
//! # Socket directory
//!
//! The daemon binds sockets under `$XDG_RUNTIME_DIR/ccbridge/`.  That
//! directory is **not** created by the daemon — it is provisioned by systemd
//! via `RuntimeDirectory=ccbridge` in the unit file.  If the directory is
//! absent when the daemon starts, `bind()` will fail loudly; that is an
//! installation bug, not a runtime concern to paper over.
//!
//! # Feature flags
//!
//! * `ble` (default) — BlueZ/bluer NUS peripheral.  Pixelbook builds pass
//!   `--no-default-features`; all other emit paths compile unconditionally.
//!
//! # Modules (stubs until their respective tasks are implemented)
//!
//! * `ingest::hooks`  — Unix socket listener for ccbridge-hook events
//! * `ingest::jsonl`  — inotify-driven tail of ~/.claude/projects/**/*.jsonl
//! * `state`          — Aggregator (single-writer task, mpsc fanout)
//! * `emit::swaync`   — DBus notification emitter
//! * `emit::ble`      — BlueZ NUS peripheral (feature = "ble")
//! * `emit::ctrl`     — bidirectional control socket
//! * `emit::http`     — optional HTTP /status (runtime-gated by config)
//! * `config`         — config.toml loader

use anyhow::Result;
use tracing::info;

fn main() {
    // Dispatch on the first argument — no clap dep needed for one subcommand.
    match std::env::args().nth(1).as_deref() {
        Some("setup") => ccbridged::setup::run(),
        Some(other) => {
            eprintln!("ccbridged: unknown subcommand {other:?}");
            eprintln!("usage: ccbridged [setup]");
            std::process::exit(1);
        }
        None => {
            // Default: run as the daemon.
            tokio::runtime::Runtime::new()
                .expect("tokio runtime")
                .block_on(daemon_main())
                .unwrap_or_else(|e| {
                    eprintln!("ccbridged: fatal: {e:#}");
                    std::process::exit(1);
                });
        }
    }
}

async fn daemon_main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ccbridged=info".parse()?),
        )
        .init();

    info!("ccbridged starting");

    // TODO (task 362c957e): start hook ingest socket
    // TODO (task 362c957e): start Aggregator
    // TODO (task 27993d8d): start JSONL tail
    // TODO (task 0432dcb9): wire approval flow
    // TODO (task 1351d215): start control socket
    // TODO (task 8564c3f5): load config

    info!("ccbridged ready");

    // Tell systemd we're ready (Type=notify in the unit file).
    // Best-effort: under non-systemd contexts (cargo run) this is a no-op.
    if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]) {
        tracing::debug!("sd_notify ready failed (not running under systemd?): {e}");
    }

    tokio::signal::ctrl_c().await?;
    info!("ccbridged shutting down");
    Ok(())
}
