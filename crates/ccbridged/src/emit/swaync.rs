//! swaync DBus emitter — `org.freedesktop.Notifications` via zbus.
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

use std::collections::HashMap;

use anyhow::Result;
use ccbridge_proto::buddy::{Heartbeat, WireDecision};
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

/// Spawn the swaync emitter as a tokio task.
///
/// If the session bus is unreachable, the task exits immediately after logging
/// a warning.  The spawned [`tokio::task::JoinHandle`] is always returned so
/// the caller can optionally join it; the daemon must treat its exit as
/// non-fatal.
pub fn spawn(
    agg_tx: AggregatorTx,
    hb_rx: broadcast::Receiver<Heartbeat>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run(agg_tx, hb_rx).await {
            warn!("swaync: emitter exited with error: {e:#}");
        }
    })
}

// ---------------------------------------------------------------------------
// Internal run loop
// ---------------------------------------------------------------------------

async fn run(agg_tx: AggregatorTx, mut hb_rx: broadcast::Receiver<Heartbeat>) -> Result<()> {
    // Connect to the session bus.  If swaync / a notifications daemon is not
    // running, or the bus itself is absent (headless CI), this returns an
    // error and we bail out before any loop begins.
    let conn = match Connection::session().await {
        Ok(c) => c,
        Err(e) => {
            warn!("swaync: cannot connect to session bus: {e} — disabling swaync emitter");
            return Ok(());
        }
    };

    let proxy = match NotificationsProxy::new(&conn).await {
        Ok(p) => p,
        Err(e) => {
            warn!("swaync: cannot create Notifications proxy: {e} — disabling swaync emitter");
            return Ok(());
        }
    };

    // Subscribe to signals before entering the select! loop.
    let mut action_stream = match proxy.receive_action_invoked().await {
        Ok(s) => s,
        Err(e) => {
            warn!("swaync: cannot subscribe to ActionInvoked: {e} — disabling swaync emitter");
            return Ok(());
        }
    };

    let mut closed_stream = match proxy.receive_notification_closed().await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "swaync: cannot subscribe to NotificationClosed: {e} — disabling swaync emitter"
            );
            return Ok(());
        }
    };

    info!("swaync: emitter connected to session bus");

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

    // The replaces_id we pass on the next Notify call.  Starts at 0 ("no
    // replacement").  After the first successful notify we keep it as the
    // last issued ID so new prompts replace rather than stack.
    let mut last_notif_id: u32 = 0;

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
                        debug!("swaync: broadcast lagged by {n} — skipping ahead");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("swaync: broadcast channel closed — exiting");
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
                            ).await;
                        }
                    }
                    None => {
                        warn!("swaync: ActionInvoked signal stream ended — exiting");
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
                        warn!("swaync: NotificationClosed signal stream ended — exiting");
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
            let summary = format!("Claude Code: approve {}?", prompt.tool);
            let body = prompt.hint.clone();

            // actions: flat list of (key, label) pairs.
            // "default" key = clicking the notification body.
            let actions: &[&str] = &[
                "default", "Approve once",
                "once",    "Approve once",
                "deny",    "Deny",
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
                        "swaync: notification posted",
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
                    warn!("swaync: Notify call failed: {e}");
                }
            }
        }

        None => {
            // No pending prompt — a decision arrived from another emitter (BLE,
            // ctrl socket) or the session resolved naturally.  Close everything.
            if !active.is_empty() {
                debug!("swaync: prompt cleared externally — closing {} notification(s)", active.len());
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
// ActionInvoked handler
// ---------------------------------------------------------------------------

async fn handle_action(
    proxy: &NotificationsProxy<'_>,
    agg_tx: &AggregatorTx,
    notif_id: u32,
    action_key: &str,
    active: &mut HashMap<u32, String>,
) {
    // Stale-click guard: if this notif_id isn't in our map it's from a previous
    // prompt that we already handled or replaced.  Ignore silently.
    let tool_use_id = match active.remove(&notif_id) {
        Some(id) => id,
        None => {
            debug!(
                notif_id,
                action_key,
                "swaync: ActionInvoked for unknown/stale notification — ignoring",
            );
            return;
        }
    };

    let decision = match action_key {
        "default" | "once" => WireDecision::Once,
        "deny" => WireDecision::Deny,
        other => {
            debug!(notif_id, action_key = other, "swaync: unknown action key — ignoring");
            // Put it back: we consumed it from the map but didn't act.
            active.insert(notif_id, tool_use_id);
            return;
        }
    };

    debug!(
        notif_id,
        tool_use_id = %tool_use_id,
        action = action_key,
        "swaync: user actioned notification",
    );

    // Send the decision to the aggregator.
    let _ = agg_tx
        .send(AggregatorMsg::PermissionDecision {
            tool_use_id,
            decision,
        })
        .await;

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
            "swaync: notification dismissed — suppressing re-post until prompt clears",
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
        debug!(notif_id = id, tool_use_id = %tool_use_id, "swaync: closing notification");
        if let Err(e) = proxy.close_notification(id).await {
            debug!(notif_id = id, "swaync: CloseNotification error (stale id?): {e}");
        }
    }
}
