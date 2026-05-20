// SPDX-License-Identifier: MIT
//! Freedesktop notification daemon emitter (works with swaync, mako, dunst,
//! GNOME, KDE, …) — speaks `org.freedesktop.Notifications` via zbus.
//!
//! # What this module does
//!
//! Subscribes to the aggregator's heartbeat broadcast channel.  On every
//! heartbeat that carries a [`PromptInfo`], it posts a critical-urgency
//! `org.freedesktop.Notifications.Notify` call with two actions:
//!
//! * `"default"` / `"Approve once"` — clicking the notification body
//! * `"once"` / `"Approve once"` — explicit approve button
//! * `"deny"` / `"Deny"` — explicit deny button
//!
//! When the user clicks an action, the module sends
//! [`AggregatorMsg::PermissionDecision`] and closes any remaining active
//! notifications.  When a heartbeat arrives without a prompt (decision came
//! from another emitter), any outstanding notification is closed silently.
//!
//! # Reliability
//!
//! * Session bus unavailable → `warn!` once, task exits, daemon keeps running.
//! * Individual `Notify` call fails → `warn!` and continue.
//! * `NotificationClosed` signal (user dismissed without clicking) → drop the
//!   map entry silently; no [`AggregatorMsg`] sent.
//! * `RecvError::Lagged` on the broadcast channel → skip, next heartbeat
//!   arrives within 10 s.
//! * `RecvError::Closed` (aggregator gone) → break, task exits cleanly.
//!
//! # Tests
//!
//! This module has no automated unit tests.  Testing `replaces_id` tracking,
//! dismissed-set behaviour, and action routing requires a live
//! `org.freedesktop.Notifications` DBus session, which is not available in CI.
//! The state machine is small and has been verified manually plus through the
//! heartbeat broadcast in `tests/full_flow.rs`.

use std::collections::HashMap;

use anyhow::Result;
use ccbridge_proto::buddy::{Heartbeat, MatchSource, WireDecision};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use zbus::proxy;
use zbus::Connection;
// zbus signal streams implement futures_core::Stream; .next() requires StreamExt.
use futures_lite::StreamExt as _;

use crate::state::{AggregatorMsg, AggregatorTx};

// ---------------------------------------------------------------------------
// DBus proxy
// ---------------------------------------------------------------------------

/// Subset of `org.freedesktop.Notifications` that ccbridge uses.
///
/// zbus derives async method wrappers and signal stream accessors from this
/// trait definition.  Both methods and the two signals we care about
/// (`ActionInvoked`, `NotificationClosed`) live on the same interface, so
/// one proxy struct is sufficient.
#[proxy(
    interface = "org.freedesktop.Notifications",
    default_service = "org.freedesktop.Notifications",
    default_path = "/org/freedesktop/Notifications"
)]
trait Notifications {
    /// Post a notification.  Returns the server-assigned notification ID.
    #[allow(clippy::too_many_arguments)]
    fn notify(
        &self,
        app_name: &str,
        replaces_id: u32,
        app_icon: &str,
        summary: &str,
        body: &str,
        actions: &[&str],
        hints: &HashMap<&str, zbus::zvariant::Value<'_>>,
        expire_timeout: i32,
    ) -> zbus::Result<u32>;

    /// Close a notification by ID.  No-op if the ID is stale.
    fn close_notification(&self, id: u32) -> zbus::Result<()>;

    /// Returns the server capabilities (e.g. `["actions", "body", "body-markup"]`).
    fn get_capabilities(&self) -> zbus::Result<Vec<String>>;

    /// Fired when the user clicks a notification action.
    #[zbus(signal)]
    fn action_invoked(&self, id: u32, action_key: String) -> zbus::Result<()>;

    /// Fired when a notification is closed for any reason (action, expiry,
    /// explicit close, etc.).  `reason` values: 1=expired, 2=dismissed by
    /// user, 3=closed by app, 4=undefined.
    #[zbus(signal)]
    fn notification_closed(&self, id: u32, reason: u32) -> zbus::Result<()>;
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Spawn the notify emitter as a tokio task.
///
/// If the session bus is unreachable, the task exits immediately after logging
/// a warning.  The spawned [`tokio::task::JoinHandle`] is always returned so
/// the caller can optionally join it; the daemon must treat its exit as
/// non-fatal.
pub fn spawn(
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
    turn_done_rx: broadcast::Receiver<crate::state::TurnDoneEvent>,
    turn_done_expire_ms: i32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run(agg_tx, hb_rx, turn_done_rx, turn_done_expire_ms).await {
            warn!("notify: emitter exited with error: {e:#}");
        }
    })
}

// ---------------------------------------------------------------------------
// Internal run loop
// ---------------------------------------------------------------------------

async fn run(
    agg_tx: AggregatorTx,
    mut hb_rx: broadcast::Receiver<Heartbeat>,
    mut turn_done_rx: broadcast::Receiver<crate::state::TurnDoneEvent>,
    turn_done_expire_ms: i32,
) -> Result<()> {
    // Connect to the session bus.  If swaync / a notifications daemon is not
    // running, or the bus itself is absent (headless CI), this returns an
    // error and we bail out before any loop begins.
    let conn = match Connection::session().await {
        Ok(c) => c,
        Err(e) => {
            warn!("notify: cannot connect to session bus: {e} — disabling notify emitter");
            return Ok(());
        }
    };

    let proxy = match NotificationsProxy::new(&conn).await {
        Ok(p) => p,
        Err(e) => {
            warn!("notify: cannot create Notifications proxy: {e} — disabling notify emitter");
            return Ok(());
        }
    };

    // Subscribe to signals before entering the select! loop.
    let mut action_stream = match proxy.receive_action_invoked().await {
        Ok(s) => s,
        Err(e) => {
            warn!("notify: cannot subscribe to ActionInvoked: {e} — disabling notify emitter");
            return Ok(());
        }
    };

    let mut closed_stream = match proxy.receive_notification_closed().await {
        Ok(s) => s,
        Err(e) => {
            warn!("notify: cannot subscribe to NotificationClosed: {e} — disabling notify emitter");
            return Ok(());
        }
    };

    info!("notify: emitter connected to session bus");

    // Probe server capabilities once — diagnostic only, no behavior branches.
    match proxy.get_capabilities().await {
        Ok(caps) => {
            info!(capabilities = ?caps, "notify: server capabilities");
            if !caps.iter().any(|c| c == "actions") {
                warn!(
                    "notify: server does not advertise the 'actions' capability — \
                     Approve/Deny buttons may not be visible. Configure your daemon \
                     to render notification actions, or use the ctrl socket / Claude \
                     TUI fallback to decide."
                );
            }
        }
        Err(e) => {
            debug!("notify: GetCapabilities failed (non-fatal): {e}");
        }
    }

    // notif_id → tool_use_id for every currently-visible approval notification.
    // In normal operation at most one entry lives here (one pending prompt),
    // but a HashMap is future-proof and the close-all path is idempotent.
    let mut active: HashMap<u32, String> = HashMap::new();

    // tool_use_ids the user has dismissed (closed without acting).
    // When a heartbeat arrives for a prompt id in this set, we do NOT
    // re-post the notification — the user already said "go away".
    // The set is cleared when the prompt id changes (new prompt or no prompt).
    let mut dismissed: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_prompt_id: Option<String> = None;

    // First-stale-click feedback: after a daemon restart, an orphaned swaync
    // action click arrives for an id we don't recognise.  Post a one-time
    // notification explaining what happened so the user isn't confused.
    //
    // The flag's lifetime is the lifetime of this `run` task — i.e. one
    // daemon process.  It resets on every daemon restart, which is exactly
    // the right scope: the "ccbridge restarted" notification should fire
    // once per restart, not once per process lifetime of the user's
    // notification daemon.
    let mut first_stale_click_seen = false;

    // The replaces_id we pass on the next Notify call.  Starts at 0 ("no
    // replacement").  After the first successful notify we keep it as the
    // last issued ID so new prompts replace rather than stack.
    let mut last_notif_id: u32 = 0;

    // Per-session "Claude is done" notification IDs.  Used as the
    // `replaces_id` for the next turn-done notif so consecutive turns in
    // the same session collapse to one notification rather than stacking.
    let mut turn_done_notif_ids: HashMap<String, u32> = HashMap::new();

    loop {
        tokio::select! {
            // --- heartbeat branch ---
            recv = hb_rx.recv() => {
                match recv {
                    Ok(hb) => handle_heartbeat(
                        &proxy,
                        hb,
                        &mut active,
                        &mut last_notif_id,
                        &mut dismissed,
                        &mut last_prompt_id,
                    ).await,

                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("notify: broadcast lagged by {n} — skipping ahead");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("notify: broadcast channel closed — exiting");
                        break;
                    }
                }
            }

            // --- turn-done branch ---
            recv = turn_done_rx.recv() => {
                match recv {
                    Ok(evt) => handle_turn_done(
                        &proxy,
                        evt,
                        &mut turn_done_notif_ids,
                        turn_done_expire_ms,
                    ).await,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        debug!("notify: turn-done broadcast lagged by {n} — skipping ahead");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("notify: turn-done broadcast closed — exiting");
                        break;
                    }
                }
            }

            // --- ActionInvoked signal ---
            signal = action_stream.next() => {
                match signal {
                    Some(sig) => {
                        if let Ok(args) = sig.args() {
                            handle_action(
                                &proxy,
                                &agg_tx,
                                args.id,
                                &args.action_key,
                                &mut active,
                                &mut first_stale_click_seen,
                            ).await;
                        }
                    }
                    None => {
                        warn!("notify: ActionInvoked signal stream ended — exiting");
                        break;
                    }
                }
            }

            // --- NotificationClosed signal ---
            signal = closed_stream.next() => {
                match signal {
                    Some(sig) => {
                        if let Ok(args) = sig.args() {
                            handle_closed(args.id, &mut active, &mut dismissed);
                        }
                    }
                    None => {
                        warn!("notify: NotificationClosed signal stream ended — exiting");
                        break;
                    }
                }
            }
        }
    }

    // Clean up any lingering notifications before the task exits.
    close_all(&proxy, &mut active).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Heartbeat handler
// ---------------------------------------------------------------------------

async fn handle_heartbeat(
    proxy: &NotificationsProxy<'_>,
    hb: Heartbeat,
    active: &mut HashMap<u32, String>,
    last_notif_id: &mut u32,
    dismissed: &mut std::collections::HashSet<String>,
    last_prompt_id: &mut Option<String>,
) {
    match hb.prompt {
        Some(prompt) => {
            // If the prompt id changed from the last heartbeat, clear the
            // dismissed set — this is a new prompt the user hasn't seen yet.
            if last_prompt_id.as_deref() != Some(&prompt.id) {
                dismissed.clear();
                *last_prompt_id = Some(prompt.id.clone());
            }

            // If the user already dismissed this notification, do not re-post.
            if dismissed.contains(&prompt.id) {
                return;
            }

            // A permission prompt is pending.  Post (or replace) the notification.

            // Derive display helpers for session context.
            let cwd_short = prompt
                .cwd
                .as_deref()
                .map(shorten_cwd)
                .filter(|s| !s.is_empty());
            let agent_or_main = prompt.agent_type.as_deref().unwrap_or("main");
            let session_short = prompt
                .session_id
                .as_deref()
                .map(crate::util::short_session_id);

            // Summary: include cwd when available so parallel notifications
            // are visually distinct in the swaync stack.
            let summary = match cwd_short.as_deref() {
                Some(c) => format!("Claude Code [{}]: approve {}?", c, prompt.tool),
                None => format!("Claude Code: approve {}?", prompt.tool),
            };

            // Body: start with the hint, then add session/agent context line,
            // then the allowlist-match annotation if present.
            let mut body = prompt.hint.clone();

            // Context line — omit when we have no useful identifiers.
            if cwd_short.is_some() || session_short.is_some() {
                let context = format!(
                    "[{} · {} · {}]",
                    cwd_short.as_deref().unwrap_or("?"),
                    agent_or_main,
                    session_short.as_deref().unwrap_or("?"),
                );
                body.push('\n');
                body.push_str(&context);
            }

            // Allowlist annotation.
            if let (Some(pattern), Some(source)) = (
                prompt.matched_pattern.as_ref(),
                prompt.matched_source.as_ref(),
            ) {
                let source_label = match source {
                    MatchSource::Allow => "allowlists",
                    MatchSource::Deny => "denies",
                };
                body.push_str(&format!(
                    "\n[Claude {} this with pattern {:?} — confirm to override]",
                    source_label, pattern,
                ));
            }

            // (body is now the composite of hint + context + optional annotation)

            // actions: flat list of (key, label) pairs.
            // "default" key = clicking the notification body.
            let actions: &[&str] = &[
                "default",
                "Approve once",
                "once",
                "Approve once",
                "always",
                "Always",
                "deny",
                "Deny",
            ];

            // hints: urgency = 2 (critical) — never auto-dismissed.
            let mut hints: HashMap<&str, zbus::zvariant::Value<'_>> = HashMap::new();
            hints.insert("urgency", zbus::zvariant::Value::U8(2));

            // expire_timeout: 0 = never expire (spec §4.4).
            let expire_timeout: i32 = 0;

            // replaces_id: pass the last issued ID so swaync replaces in-place.
            // On the very first call last_notif_id is 0 ("no replacement").
            let replaces_id = *last_notif_id;

            match proxy
                .notify(
                    "ccbridge",
                    replaces_id,
                    "",
                    &summary,
                    &body,
                    actions,
                    &hints,
                    expire_timeout,
                )
                .await
            {
                Ok(id) => {
                    debug!(
                        notif_id = id,
                        tool_use_id = %prompt.id,
                        tool = %prompt.tool,
                        "notify: notification posted",
                    );
                    // If this replaced a previous notification it already closed
                    // the old one server-side; we just update our map.
                    if replaces_id != 0 && replaces_id != id {
                        active.remove(&replaces_id);
                    }
                    active.insert(id, prompt.id);
                    *last_notif_id = id;
                }
                Err(e) => {
                    warn!("notify: Notify call failed: {e}");
                }
            }
        }

        None => {
            // No pending prompt — a decision arrived from another emitter (BLE,
            // ctrl socket) or the session resolved naturally.  Close everything.
            if !active.is_empty() {
                debug!(
                    "notify: prompt cleared externally — closing {} notification(s)",
                    active.len()
                );
                close_all(proxy, active).await;
                *last_notif_id = 0;
            }
            // Clear dismissed set and last_prompt_id: next prompt is fresh.
            if last_prompt_id.is_some() {
                dismissed.clear();
                *last_prompt_id = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Turn-done handler
// ---------------------------------------------------------------------------

/// Post the "Claude is done" notification for one session.
///
/// Replaces the previous turn-done notification for that session (so two
/// consecutive idle turns don't stack two visible notifs), normal urgency,
/// no actions, auto-expires per `expire_ms`.
async fn handle_turn_done(
    proxy: &NotificationsProxy<'_>,
    evt: crate::state::TurnDoneEvent,
    turn_done_notif_ids: &mut HashMap<String, u32>,
    expire_ms: i32,
) {
    let cwd_basename = std::path::Path::new(&evt.cwd)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&evt.cwd);

    let summary = "Claude is done";
    let body = if evt.response_snippet.is_empty() {
        format!("{}  ·  {} tokens", cwd_basename, evt.tokens_cumulative)
    } else {
        format!(
            "{}\n{}  ·  {} tokens",
            evt.response_snippet, cwd_basename, evt.tokens_cumulative,
        )
    };

    let mut hints = HashMap::new();
    hints.insert(
        "urgency",
        zbus::zvariant::Value::U8(1), // 1 = normal
    );

    let replaces_id = turn_done_notif_ids
        .get(&evt.session_id)
        .copied()
        .unwrap_or(0);

    match proxy
        .notify(
            "ccbridge",
            replaces_id,
            "",
            summary,
            &body,
            &[], // no actions
            &hints,
            expire_ms,
        )
        .await
    {
        Ok(id) => {
            debug!(
                session_id = %evt.session_id,
                notif_id = id,
                "notify: posted turn-done notification",
            );
            turn_done_notif_ids.insert(evt.session_id, id);
        }
        Err(e) => {
            warn!(
                session_id = %evt.session_id,
                "notify: turn-done Notify failed: {e}",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ActionInvoked handler
// ---------------------------------------------------------------------------

async fn handle_action(
    proxy: &NotificationsProxy<'_>,
    agg_tx: &AggregatorTx,
    notif_id: u32,
    action_key: &str,
    active: &mut HashMap<u32, String>,
    first_stale_click_seen: &mut bool,
) {
    // Stale-click guard: if this notif_id isn't in our map it's from a previous
    // prompt that we already handled or replaced (e.g. after a daemon restart).
    let tool_use_id = match active.remove(&notif_id) {
        Some(id) => id,
        None => {
            debug!(
                notif_id,
                action_key, "notify: ActionInvoked for unknown/stale notification — ignoring",
            );
            // On first stale click after startup, post a one-time info
            // notification so the user knows to re-trigger their action.
            if !*first_stale_click_seen {
                *first_stale_click_seen = true;
                let mut hints = HashMap::new();
                hints.insert(
                    "urgency",
                    zbus::zvariant::Value::U8(2), // critical — persistent until dismissed
                );
                let _ = proxy
                    .notify(
                        "ccbridge",
                        0,
                        "",
                        "ccbridge restarted",
                        "Please re-trigger the action you were approving. \
                         The previous approval window expired when the daemon restarted.",
                        &[],
                        &hints,
                        0, // 0 = server default (persistent for critical urgency)
                    )
                    .await;
            }
            return;
        }
    };

    debug!(
        notif_id,
        tool_use_id = %tool_use_id,
        action = action_key,
        "notify: user actioned notification",
    );

    match action_key {
        "default" | "once" => {
            let _ = agg_tx
                .send(AggregatorMsg::PermissionDecision {
                    tool_use_id,
                    decision: WireDecision::Once,
                    respond: None,
                })
                .await;
        }
        "always" => {
            // Tell the aggregator to derive + write the allow pattern, then
            // approve this call.
            let _ = agg_tx
                .send(AggregatorMsg::AllowlistAlways { tool_use_id })
                .await;
        }
        "deny" => {
            let _ = agg_tx
                .send(AggregatorMsg::PermissionDecision {
                    tool_use_id,
                    decision: WireDecision::Deny,
                    respond: None,
                })
                .await;
        }
        other => {
            debug!(
                notif_id,
                action_key = other,
                "notify: unknown action key — ignoring"
            );
            // Put it back: we consumed it from the map but didn't act.
            active.insert(notif_id, tool_use_id);
            return;
        }
    }

    // Close any remaining active notifications for the same prompt (there
    // should be at most one, but close_all is idempotent).
    close_all(proxy, active).await;
}

// ---------------------------------------------------------------------------
// NotificationClosed handler
// ---------------------------------------------------------------------------

fn handle_closed(
    notif_id: u32,
    active: &mut HashMap<u32, String>,
    dismissed: &mut std::collections::HashSet<String>,
) {
    if let Some(tool_use_id) = active.remove(&notif_id) {
        // User dismissed without clicking an action.
        // Record the tool_use_id so the next heartbeat (which still carries
        // the same prompt) does not immediately re-post the notification.
        // The dismissed set is cleared when the prompt id changes or clears.
        debug!(
            notif_id,
            tool_use_id = %tool_use_id,
            "notify: notification dismissed — suppressing re-post until prompt clears",
        );
        dismissed.insert(tool_use_id);
    }
}

// ---------------------------------------------------------------------------
// close_all helper
// ---------------------------------------------------------------------------

/// Close every notification in `active` and clear the map.
///
/// `CloseNotification` on a stale/already-closed ID is a no-op per spec, so
/// it is always safe to call this unconditionally.
async fn close_all(proxy: &NotificationsProxy<'_>, active: &mut HashMap<u32, String>) {
    for (id, tool_use_id) in active.drain() {
        debug!(notif_id = id, tool_use_id = %tool_use_id, "notify: closing notification");
        if let Err(e) = proxy.close_notification(id).await {
            debug!(
                notif_id = id,
                "notify: CloseNotification error (stale id?): {e}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Session context helpers
// ---------------------------------------------------------------------------

/// Maximum number of display characters before truncating with `…`.
const MAX_DISPLAY_LEN: usize = 30;

/// Shorten a cwd path for compact display in notification bodies.
///
/// - Replaces `$HOME` prefix with `~` (e.g. `/home/u/dev/x` → `~/dev/x`).
/// - Leaves short absolute paths intact (e.g. `/tmp/new` → `/tmp/new`).
/// - Truncates only when the result would be ≥ [`MAX_DISPLAY_LEN`] chars,
///   replacing the middle with `…`.
/// - Empty string → empty string.
///
/// Delegates to [`shorten_cwd_with_home`] using the process `$HOME`.
pub fn shorten_cwd(cwd: &str) -> String {
    let home = std::env::var_os("HOME")
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();
    shorten_cwd_with_home(cwd, &home)
}

/// Testable inner implementation — accepts `home` as a parameter so tests
/// don't need to mutate process environment.
pub fn shorten_cwd_with_home(cwd: &str, home: &str) -> String {
    if cwd.is_empty() {
        return String::new();
    }

    // Substitute $HOME prefix with ~.  Guard against partial matches by
    // ensuring the char after the home prefix (if any) is '/'.
    let displayed: String = if !home.is_empty() {
        let home_trailing_slash = if home.ends_with('/') {
            home.to_owned()
        } else {
            format!("{home}/")
        };
        if cwd == home {
            // Exact match — the path IS $HOME.
            "~".to_owned()
        } else if cwd.starts_with(home_trailing_slash.as_str()) {
            // Starts with "$HOME/" — substitute prefix.
            let rest = &cwd[home.len()..]; // includes the leading '/'
            format!("~{rest}")
        } else {
            cwd.to_owned()
        }
    } else {
        cwd.to_owned()
    };

    // Already short enough? Return as-is.
    if displayed.len() <= MAX_DISPLAY_LEN {
        return displayed;
    }

    // Too long — keep the prefix anchor (~/ or /) and the last 2 components.
    let p = std::path::Path::new(&displayed);
    let comps: Vec<&str> = p
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .filter(|s| !s.is_empty() && *s != "/")
        .collect();

    if comps.len() <= 2 {
        return displayed; // can't truncate further; return as-is
    }

    let tail: Vec<&str> = comps.iter().rev().take(2).rev().copied().collect();
    let tail_str = tail.join("/");

    if displayed.starts_with('~') {
        format!("~/…/{tail_str}")
    } else {
        format!("/…/{tail_str}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shorten_cwd_replaces_home_with_tilde() {
        assert_eq!(shorten_cwd_with_home("/home/u/dev/x", "/home/u"), "~/dev/x");
        assert_eq!(shorten_cwd_with_home("/home/u", "/home/u"), "~");
        assert_eq!(shorten_cwd_with_home("/home/u/x", "/home/u"), "~/x");
    }

    #[test]
    fn shorten_cwd_keeps_short_absolute_paths() {
        assert_eq!(shorten_cwd_with_home("/tmp/new", "/home/u"), "/tmp/new");
        assert_eq!(shorten_cwd_with_home("/srv", "/home/u"), "/srv");
        assert_eq!(shorten_cwd_with_home("/", "/home/u"), "/");
    }

    #[test]
    fn shorten_cwd_truncates_long_home_paths() {
        let long = "/home/u/dev/aiven/aiven-design-system/very/deep";
        let out = shorten_cwd_with_home(long, "/home/u");
        assert!(out.starts_with("~/…/"), "expected ~/…/... got {out:?}");
        assert!(
            out.contains("very/deep"),
            "expected tail very/deep, got {out:?}"
        );
    }

    #[test]
    fn shorten_cwd_truncates_long_absolute_paths() {
        let long = "/var/lib/postgres/data/very/deep/path";
        let out = shorten_cwd_with_home(long, "/home/u");
        assert!(out.starts_with("/…/"), "expected /…/... got {out:?}");
        assert!(
            out.contains("deep/path"),
            "expected tail deep/path, got {out:?}"
        );
    }

    #[test]
    fn shorten_cwd_empty_path() {
        assert_eq!(shorten_cwd_with_home("", "/home/u"), "");
    }

    #[test]
    fn shorten_cwd_no_home_env_var() {
        // When home is empty, paths under what would normally be home stay absolute.
        assert_eq!(shorten_cwd_with_home("/home/u/dev/x", ""), "/home/u/dev/x");
    }

    #[test]
    fn shorten_cwd_partial_home_match_not_substituted() {
        // /home/used must NOT match /home/u — substring vs prefix.
        let out = shorten_cwd_with_home("/home/used/x", "/home/u");
        assert!(
            !out.starts_with('~'),
            "partial prefix must not substitute ~, got {out:?}"
        );
        assert_eq!(out, "/home/used/x");
    }

    // Regression for the original shorten_cwd_basics — now rendered differently.
    #[test]
    fn shorten_cwd_root_returns_slash() {
        assert_eq!(shorten_cwd_with_home("/", "/home/u"), "/");
    }
}
