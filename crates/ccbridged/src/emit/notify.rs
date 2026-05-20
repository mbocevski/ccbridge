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
use ccbridge_proto::buddy::{Heartbeat, MatchSource, PromptInfo, WireDecision};
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

    // Per-notif metadata for every currently-visible ccbridge notification
    // (both approval prompts AND turn-done notifications).  Used by
    // ActionInvoked / NotificationClosed to figure out what the user
    // actioned, and by close_all on shutdown.
    let mut active: HashMap<u32, ActiveNotif> = HashMap::new();

    // session_id → currently visible notif_id for that session.  At most one
    // ccbridge notif per session at any time (an approval and a turn-done
    // can't coexist for the same session — Stop clears `pending`).  Used as
    // `replaces_id` so the next post on the same session collapses in-place
    // rather than stacking a second notification.
    let mut session_notif: HashMap<String, u32> = HashMap::new();

    // tool_use_ids the user dismissed (closed without acting).  When a
    // heartbeat arrives for a prompt id in this set, we do NOT re-post the
    // notification — the user already said "go away".  The corresponding
    // entry is removed when the session's prompt id rotates, when the
    // session's pending state clears, or when the session goes away.
    let mut dismissed: std::collections::HashSet<String> = std::collections::HashSet::new();

    // session_id → most-recent prompt id we posted a notification for.
    // Used to detect "same session, new prompt" so we can drop the old
    // dismissal and let the new prompt through.
    let mut last_prompt_ids: HashMap<String, String> = HashMap::new();

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

    loop {
        tokio::select! {
            // --- heartbeat branch ---
            recv = hb_rx.recv() => {
                match recv {
                    Ok(hb) => handle_heartbeat(
                        &proxy,
                        hb,
                        &mut active,
                        &mut session_notif,
                        &mut dismissed,
                        &mut last_prompt_ids,
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
                        &mut active,
                        &mut session_notif,
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
                                &mut session_notif,
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
                            handle_closed(
                                args.id,
                                &mut active,
                                &mut session_notif,
                                &mut dismissed,
                            );
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
    close_all(&proxy, &mut active, &mut session_notif).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// ActiveNotif — what's currently on screen, per notif_id
// ---------------------------------------------------------------------------

/// Metadata for one currently-visible notification.  Lives in the
/// notif_id-keyed `active` map so signal handlers can look up "what was
/// this notification about?" by `notif_id`.
struct ActiveNotif {
    /// Session this notification belongs to — used to clear the
    /// `session_notif` reverse-map entry on close.
    session_id: String,
    /// What kind of notification this is.
    kind: ActiveKind,
}

enum ActiveKind {
    /// An approval-prompt notification with action buttons.
    /// `tool_use_id` is what we'd send back to the aggregator on click.
    Approval { tool_use_id: String },
    /// A "Claude is done" turn-done notification — no actions, but we
    /// still track it so the next post on the same session can use the
    /// previous notif_id as `replaces_id`.
    TurnDone,
}

// ---------------------------------------------------------------------------
// Heartbeat handler
// ---------------------------------------------------------------------------

async fn handle_heartbeat(
    proxy: &NotificationsProxy<'_>,
    hb: Heartbeat,
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
    dismissed: &mut std::collections::HashSet<String>,
    last_prompt_ids: &mut HashMap<String, String>,
) {
    use std::collections::HashSet;

    // Index incoming prompts by session_id.  A prompt without a session_id
    // is treated as belonging to a synthetic session whose id is the prompt
    // id itself — that's a defensive path; the aggregator always populates
    // session_id, so this branch should never hit in practice.
    let mut by_session: HashMap<String, &PromptInfo> = HashMap::new();
    for p in &hb.prompts {
        let key = p.session_id.clone().unwrap_or_else(|| p.id.clone());
        by_session.insert(key, p);
    }

    // (1) Close notifications for sessions that are no longer waiting.
    let stale_sessions: Vec<String> = session_notif
        .keys()
        .filter(|sid| {
            // Only close approval-slot notifications when the session no
            // longer has a pending prompt.  Turn-done notifications stay
            // visible for their own expire_timeout — heartbeat changes
            // shouldn't yank them off screen.
            if by_session.contains_key(sid.as_str()) {
                return false;
            }
            session_notif
                .get(sid.as_str())
                .and_then(|nid| active.get(nid))
                .map(|n| matches!(n.kind, ActiveKind::Approval { .. }))
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    for sid in stale_sessions {
        if let Some(nid) = session_notif.remove(&sid) {
            if let Some(notif) = active.remove(&nid) {
                debug!(
                    notif_id = nid,
                    session_id = %sid,
                    "notify: prompt cleared — closing approval notification",
                );
                if let Err(e) = proxy.close_notification(nid).await {
                    debug!(notif_id = nid, "notify: CloseNotification: {e}");
                }
                // Drop dismissal tracking for any prompt that belonged to
                // this session.
                if let ActiveKind::Approval { tool_use_id } = notif.kind {
                    dismissed.remove(&tool_use_id);
                }
            }
        }
        last_prompt_ids.remove(&sid);
    }

    // Also drop dismissal entries whose tool_use_id is no longer in any
    // visible heartbeat — covers the case where a session's prompt id
    // rotated mid-session but we hadn't seen the next heartbeat yet.
    let live_ids: HashSet<&str> = hb.prompts.iter().map(|p| p.id.as_str()).collect();
    dismissed.retain(|tid| live_ids.contains(tid.as_str()));

    // (2) Post / replace one approval notification per pending session.
    for (session_key, prompt) in by_session.iter() {
        // Detect prompt rotation within the same session — drop the old
        // dismissal record so the new prompt isn't suppressed.
        let prev = last_prompt_ids.insert(session_key.clone(), prompt.id.clone());
        if let Some(old_id) = prev {
            if old_id != prompt.id {
                dismissed.remove(&old_id);
            }
        }

        if dismissed.contains(&prompt.id) {
            continue; // user dismissed this exact prompt; don't re-post
        }

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

        let mut hints: HashMap<&str, zbus::zvariant::Value<'_>> = HashMap::new();
        hints.insert("urgency", zbus::zvariant::Value::U8(2)); // critical

        // replaces_id: prior notif for this session, if any.  0 = new slot.
        // This may be a turn-done notif id from a previous turn — replacing
        // it with a fresh approval is the correct behaviour (the stale
        // "Claude is done" disappears the moment a new prompt arrives).
        let replaces_id = session_notif.get(session_key).copied().unwrap_or(0);

        match proxy
            .notify(
                "ccbridge",
                replaces_id,
                "",
                &summary,
                &body,
                actions,
                &hints,
                0, // expire_timeout: 0 = never expire (critical urgency)
            )
            .await
        {
            Ok(id) => {
                debug!(
                    notif_id = id,
                    session_id = %session_key,
                    tool_use_id = %prompt.id,
                    tool = %prompt.tool,
                    "notify: approval notification posted",
                );
                // Server replaced the old notif in place — drop our stale
                // active entry for the previous notif_id.
                if replaces_id != 0 && replaces_id != id {
                    active.remove(&replaces_id);
                }
                active.insert(
                    id,
                    ActiveNotif {
                        session_id: session_key.clone(),
                        kind: ActiveKind::Approval {
                            tool_use_id: prompt.id.clone(),
                        },
                    },
                );
                session_notif.insert(session_key.clone(), id);
            }
            Err(e) => {
                warn!("notify: Notify call failed: {e}");
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
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
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

    // Reuse this session's notif slot if any — replaces a previous
    // turn-done OR a stale approval prompt.  The Stop handler in the
    // aggregator clears `pending` before broadcasting, so an approval
    // for the same session can't actually still be pending here, but
    // collapsing to one slot is still the right shape if it ever did.
    let replaces_id = session_notif.get(&evt.session_id).copied().unwrap_or(0);

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
            if replaces_id != 0 && replaces_id != id {
                active.remove(&replaces_id);
            }
            active.insert(
                id,
                ActiveNotif {
                    session_id: evt.session_id.clone(),
                    kind: ActiveKind::TurnDone,
                },
            );
            session_notif.insert(evt.session_id, id);
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
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
    first_stale_click_seen: &mut bool,
) {
    // Stale-click guard: if this notif_id isn't in our map it's from a previous
    // prompt that we already handled or replaced (e.g. after a daemon restart).
    let notif = match active.remove(&notif_id) {
        Some(n) => n,
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

    let ActiveKind::Approval { tool_use_id } = notif.kind else {
        // Turn-done has no actions — clicking the body would only fire
        // `default`, which we don't bind for turn-done.  Defensive: put
        // the entry back and ignore.
        debug!(notif_id, "notify: action on non-approval notif — ignoring");
        active.insert(notif_id, notif);
        return;
    };
    let session_id = notif.session_id;

    debug!(
        notif_id,
        session_id = %session_id,
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
            active.insert(
                notif_id,
                ActiveNotif {
                    session_id,
                    kind: ActiveKind::Approval { tool_use_id },
                },
            );
            return;
        }
    }

    // Drop the session-slot mapping; the next heartbeat (with the prompt
    // gone for this session) will close anything else that lingers.
    if session_notif.get(&session_id).copied() == Some(notif_id) {
        session_notif.remove(&session_id);
    }
}

// ---------------------------------------------------------------------------
// NotificationClosed handler
// ---------------------------------------------------------------------------

fn handle_closed(
    notif_id: u32,
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
    dismissed: &mut std::collections::HashSet<String>,
) {
    if let Some(notif) = active.remove(&notif_id) {
        // Drop the session→notif_id mapping if it still points here (the
        // server may already have replaced this notif on its end).
        if session_notif.get(&notif.session_id).copied() == Some(notif_id) {
            session_notif.remove(&notif.session_id);
        }
        match notif.kind {
            ActiveKind::Approval { tool_use_id } => {
                // User dismissed an approval prompt without clicking an action.
                // Record the tool_use_id so the next heartbeat (which still
                // carries the same prompt) does not immediately re-post.
                debug!(
                    notif_id,
                    tool_use_id = %tool_use_id,
                    "notify: approval dismissed — suppressing re-post until prompt clears",
                );
                dismissed.insert(tool_use_id);
            }
            ActiveKind::TurnDone => {
                // Turn-done dismissal is silent — there's no re-post path
                // (broadcasts fire once per Stop), so nothing to record.
                debug!(notif_id, "notify: turn-done notification closed");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// close_all helper
// ---------------------------------------------------------------------------

/// Close every notification in `active` and clear the map (and the
/// `session_notif` reverse-map).
///
/// `CloseNotification` on a stale/already-closed ID is a no-op per spec, so
/// it is always safe to call this unconditionally.
async fn close_all(
    proxy: &NotificationsProxy<'_>,
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
) {
    for (id, notif) in active.drain() {
        debug!(
            notif_id = id,
            session_id = %notif.session_id,
            "notify: closing notification",
        );
        if let Err(e) = proxy.close_notification(id).await {
            debug!(
                notif_id = id,
                "notify: CloseNotification error (stale id?): {e}"
            );
        }
    }
    session_notif.clear();
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
