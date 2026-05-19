// SPDX-License-Identifier: MIT
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
        Some("undo-last-allow") => {
            use ccbridged::permission::additions::{audit_log_path, undo_last_allow};
            use ccbridged::permission::settings_path;
            let sp = settings_path();
            let alp = audit_log_path().unwrap_or_else(|e| {
                eprintln!("ccbridged: cannot locate audit log: {e:#}");
                std::process::exit(1);
            });
            if let Err(e) = undo_last_allow(&sp, &alp) {
                eprintln!("ccbridged undo-last-allow: {e:#}");
                std::process::exit(1);
            }
        }
        Some(other) => {
            eprintln!("ccbridged: unknown subcommand {other:?}");
            eprintln!("usage: ccbridged [setup|undo-last-allow]");
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
    use ccbridged::emit::{ctrl as ctrl_emit, http as http_emit, notify as notify_emit};
    use ccbridged::ingest::{hooks as hook_ingest, jsonl as jsonl_ingest};
    use arc_swap::ArcSwap;
    use ccbridged::permission::{settings_path, spawn_settings_watcher, Allowlist, ProjectAllowlistCache};

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ccbridged=info".parse()?),
        )
        .init();

    // Load config early; exit(1) on parse errors so typos are never silently
    // swallowed.
    let config = ccbridged::config::Config::load().unwrap_or_else(|e| {
        eprintln!("ccbridged: failed to load config: {e:#}");
        std::process::exit(1);
    });

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
    let sp = settings_path();
    let initial_allowlist = match Allowlist::from_path(&sp) {
        Ok(a) => {
            info!(
                allow_patterns = a.allow.len(),
                deny_patterns = a.deny.len(),
                path = %sp.display(),
                "loaded allowlist from settings.json",
            );
            a
        }
        Err(e) => {
            tracing::warn!(
                "failed to load allowlist from {}: {e:#} — proceeding with empty allowlist",
                sp.display(),
            );
            Allowlist::empty()
        }
    };
    let allowlist = Arc::new(ArcSwap::new(Arc::new(initial_allowlist)));

    // Watch settings.json for live edits (hot-reload).
    spawn_settings_watcher(sp.clone(), allowlist.clone());

    // Resolve audit log path for AllowlistAlways.
    let audit_log = ccbridged::permission::additions::audit_log_path()
        .unwrap_or_else(|e| {
            tracing::warn!("cannot resolve audit log path: {e:#}; AllowlistAlways audit disabled");
            std::path::PathBuf::from("/dev/null")
        });

    // Build the per-project allowlist cache and spawn the aggregator.
    let allowlist_cache = Arc::new(ProjectAllowlistCache::new(Arc::clone(&allowlist)));
    let (agg_tx, hb_rx) = ccbridged::state::spawn_with_paths(
        config.approvals.timeout(),
        config.approvals.fallback,
        allowlist_cache,
        sp,
        audit_log,
    );

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

    // Spawn emit tasks (guarded by config flags).
    // swaync subscribes via resubscribe() so ctrl can consume hb_rx directly.
    if config.emit.notify.enabled {
        notify_emit::spawn(agg_tx.clone(), hb_rx.resubscribe());
    }
    if config.emit.ctrl.enabled {
        ctrl_emit::spawn(runtime_dir, agg_tx.clone(), hb_rx, owner, tz_offset);
    }
    if config.emit.http.enabled {
        match config.emit.http.addr.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                match http_emit::spawn(agg_tx.clone(), addr).await {
                    Ok((_, bound)) => {
                        info!(addr = %bound, "http: /status endpoint enabled");
                    }
                    Err(e) => {
                        tracing::warn!("http: {e:#} — disabling HTTP endpoint");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "http: cannot parse addr {:?}: {e} — disabling HTTP endpoint",
                    config.emit.http.addr,
                );
            }
        }
    }

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
