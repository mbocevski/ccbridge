//! ccbridged — Claude Code aggregator bridge daemon.
//!
//! Aggregates state across all running Claude Code sessions on this machine
//! and surfaces them through freedesktop notifications (swaync, mako, dunst,
//! GNOME, KDE) and a bidirectional control socket. The control socket also
//! exposes the claude-desktop-buddy wire protocol for future BLE bridges.
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

use std::sync::Arc;

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
            // Resolve the local TZ offset BEFORE the tokio thread pool starts.
            // The `time` crate's current_local_offset() is unsafe to call from
            // a multi-threaded context (glibc TZ env-var race); calling it here
            // in the single-threaded sync main is the documented safe pattern.
            let tz_offset = ccbridged::emit::ctrl::resolve_tz_offset();

            tokio::runtime::Runtime::new()
                .expect("tokio runtime")
                .block_on(daemon_main(tz_offset))
                .unwrap_or_else(|e| {
                    eprintln!("ccbridged: fatal: {e:#}");
                    std::process::exit(1);
                });
        }
    }
}

async fn daemon_main(tz_offset: i32) -> Result<()> {
    use ccbridged::emit::{ctrl as ctrl_emit, notify as notify_emit};
    use ccbridged::ingest::{hooks as hook_ingest, jsonl as jsonl_ingest};
    use ccbridged::permission::{settings_path, Allowlist};
    use ccbridged::state::{spawn as spawn_aggregator, DEFAULT_APPROVAL_TIMEOUT};

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ccbridged=info".parse()?),
        )
        .init();

    info!("ccbridged starting");

    // Resolve the runtime dir (systemd provisions $XDG_RUNTIME_DIR/ccbridge/).
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "XDG_RUNTIME_DIR not set; run under systemd or set it manually"
            )
        })?;

    // Resolve owner name (async: shells out to git; fine inside the runtime).
    let owner = ctrl_emit::resolve_owner().await;
    info!(owner = %owner, "resolved owner");

    // Token state path + load persisted state (best-effort; default on first run).
    let tokens_path = jsonl_ingest::tokens_state_path()
        .unwrap_or_else(|e| {
            tracing::warn!("cannot determine token state path: {e:#}; persistence disabled");
            std::path::PathBuf::from("/dev/null")
        });
    let initial_tokens = jsonl_ingest::PersistedTokens::load(&tokens_path)
        .unwrap_or_default();
    info!(
        cumulative = initial_tokens.cumulative,
        today = initial_tokens.today,
        "loaded token state",
    );

    // Load allowlist from settings.json (best-effort; empty on first run or error).
    let allowlist = {
        let sp = settings_path();
        match Allowlist::from_path(&sp) {
            Ok(a) => {
                info!(
                    allow_patterns = a.allow.len(),
                    deny_patterns = a.deny.len(),
                    path = %sp.display(),
                    "loaded allowlist from settings.json",
                );
                Arc::new(a)
            }
            Err(e) => {
                tracing::warn!(
                    "failed to load allowlist from {}: {e:#} — proceeding with empty allowlist",
                    sp.display(),
                );
                Arc::new(Allowlist::empty())
            }
        }
    };

    // Spawn the aggregator (single-writer state task + broadcast channel).
    let (agg_tx, hb_rx) = spawn_aggregator(DEFAULT_APPROVAL_TIMEOUT, allowlist);

    // Spawn hook ingest socket.
    hook_ingest::spawn(runtime_dir.clone(), agg_tx.clone());

    // Spawn JSONL watcher + midnight-reset task (skip if projects dir absent —
    // it won't exist on a fresh machine until Claude Code first runs).
    let projects_dir = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .map(|h| h.join(".claude").join("projects"))
        .ok_or_else(|| anyhow::anyhow!("HOME not set"))?;

    if projects_dir.exists() {
        jsonl_ingest::spawn_watcher(
            projects_dir,
            tokens_path.clone(),
            agg_tx.clone(),
            initial_tokens,
        );
        jsonl_ingest::spawn_midnight_reset(tokens_path, agg_tx.clone());
    } else {
        tracing::warn!(
            dir = %projects_dir.display(),
            "~/.claude/projects/ does not exist; JSONL watcher disabled \
             (will be active after first Claude Code session)",
        );
    }

    // Spawn emit tasks.
    // swaync subscribes via resubscribe() so ctrl can consume hb_rx directly.
    notify_emit::spawn(agg_tx.clone(), hb_rx.resubscribe());
    ctrl_emit::spawn(runtime_dir, agg_tx, hb_rx, owner, tz_offset);

    info!("ccbridged ready");

    // Tell systemd we're ready (Type=notify in the unit file).
    // Best-effort: no-op when NOTIFY_SOCKET is unset (e.g. cargo run).
    if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]) {
        tracing::debug!("sd_notify ready failed (not running under systemd?): {e}");
    }

    tokio::signal::ctrl_c().await?;
    info!("ccbridged shutting down");
    Ok(())
}
