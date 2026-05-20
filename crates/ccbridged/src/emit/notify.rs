// SPDX-License-Identifier: MIT
//! Freedesktop notification daemon emitter — speaks
//! `org.freedesktop.Notifications` via zbus.  Compatible with any daemon
//! that implements the spec.
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
use zbus::Connection;
use zbus::proxy;
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
    // Connect to the session bus.  If a notifications daemon is not
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

    // First-stale-click feedback: after a daemon restart, an orphaned
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
                                &mut dismissed,
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
        let prompt_changed = match last_prompt_ids.get(session_key) {
            Some(prev) => prev != &prompt.id,
            None => true, // never seen this session — first post
        };
        if let Some(old_id) = last_prompt_ids.get(session_key).cloned() {
            if old_id != prompt.id {
                dismissed.remove(&old_id);
            }
        }

        if dismissed.contains(&prompt.id) {
            continue; // user dismissed this exact prompt; don't re-post
        }

        // Already posted this exact prompt for this session and the user
        // hasn't dismissed it — heartbeats are broadcast on every state
        // change (token updates, transcript entries, etc.), so re-posting
        // the same prompt would needlessly cycle the notif_id and create
        // a window where the user's click lands on a stale id that
        // ActionInvoked can't resolve.  `last_prompt_ids` is updated only
        // after a successful Notify so a transient DBus failure doesn't
        // leave us thinking we already posted.
        if !prompt_changed && session_notif.contains_key(session_key) {
            continue;
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
        // are visually distinct in the notification stack.
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
                last_prompt_ids.insert(session_key.clone(), prompt.id.clone());
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
    // Match the approval-prompt summary/body format so the visual surface
    // is consistent: "Claude Code [~/dev/x]: done" and a context line
    // "[~/dev/x · main · 3cb589]" mirroring the approval notification.
    let cwd_short = if evt.cwd.is_empty() {
        None
    } else {
        let s = shorten_cwd(&evt.cwd);
        if s.is_empty() { None } else { Some(s) }
    };
    let session_short = if evt.session_id.is_empty() {
        None
    } else {
        Some(crate::util::short_session_id(&evt.session_id))
    };

    let summary = match cwd_short.as_deref() {
        Some(c) => format!("Claude Code [{}]: done", c),
        None => "Claude Code: done".to_owned(),
    };

    // Body: response snippet (when present), then the same
    // "[cwd · agent_or_main · session]" context line as approvals, then
    // the cumulative token count.  TurnDoneEvent doesn't carry
    // agent_type — Stop is a session-level event, not per-tool-call —
    // so we always write "main" here.
    let mut body = String::new();
    if !evt.response_snippet.is_empty() {
        body.push_str(&evt.response_snippet);
    }

    if cwd_short.is_some() || session_short.is_some() {
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&format!(
            "[{} · main · {}]",
            cwd_short.as_deref().unwrap_or("?"),
            session_short.as_deref().unwrap_or("?"),
        ));
    }

    if !body.is_empty() {
        body.push('\n');
    }
    // Just the per-turn count.  Cumulative + today are already visible
    // via the heartbeat (ctrl socket / Waybar); the notification body is
    // about *this* task.
    body.push_str(&format!(
        "{} tokens/turn",
        format_token_count(evt.tokens_this_turn)
    ));

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
            &summary,
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

#[allow(clippy::too_many_arguments)] // 8 fields of independent state — a wrapper struct
// would just rename the same pieces.
async fn handle_action(
    proxy: &NotificationsProxy<'_>,
    agg_tx: &AggregatorTx,
    notif_id: u32,
    action_key: &str,
    active: &mut HashMap<u32, ActiveNotif>,
    session_notif: &mut HashMap<String, u32>,
    dismissed: &mut std::collections::HashSet<String>,
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
                    tool_use_id: tool_use_id.clone(),
                    decision: WireDecision::Once,
                    respond: None,
                })
                .await;
        }
        "always" => {
            let _ = agg_tx
                .send(AggregatorMsg::AllowlistAlways {
                    tool_use_id: tool_use_id.clone(),
                })
                .await;
        }
        "deny" => {
            let _ = agg_tx
                .send(AggregatorMsg::PermissionDecision {
                    tool_use_id: tool_use_id.clone(),
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

    // Record the actioned tool_use_id so any heartbeat that still carries
    // this prompt — possible during the race window between our send to
    // the aggregator and the aggregator clearing `pending` — does not
    // re-post the notification we just acted on.  The dismissal cleanup
    // in handle_heartbeat will drop this entry once the prompt is no
    // longer in any heartbeat.
    dismissed.insert(tool_use_id);
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
// Token count formatter
// ---------------------------------------------------------------------------

/// Format a token count compactly: `1.2k`, `184k`, `2.5M`.
///
/// The compact form is also a defence against notification daemons
/// that auto-detect long digit clusters as "OTP-like" and inject a
/// "copy code" action button into the notification.  Splitting `1172`
/// into `1.2k` defeats the heuristic and is easier to read on a
/// compact display.
fn format_token_count(n: u64) -> String {
    const K: u64 = 1_000;
    const M: u64 = 1_000_000;
    if n < K {
        // Three digits or fewer — no daemon flags this as a code.
        n.to_string()
    } else if n < 10 * K {
        // 1.0k–9.9k: show one decimal.
        format!("{:.1}k", n as f64 / K as f64)
    } else if n < M {
        // 10k–999k: integer thousands.
        format!("{}k", n / K)
    } else if n < 10 * M {
        // 1.0M–9.9M.
        format!("{:.1}M", n as f64 / M as f64)
    } else {
        format!("{}M", n / M)
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
/// - Truncates only when the result would be ≥ `MAX_DISPLAY_LEN` chars,
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

    // -----------------------------------------------------------------------
    // format_token_count
    // -----------------------------------------------------------------------

    #[test]
    fn format_token_count_under_1k_uses_raw() {
        assert_eq!(format_token_count(0), "0");
        assert_eq!(format_token_count(42), "42");
        assert_eq!(format_token_count(999), "999");
    }

    #[test]
    fn format_token_count_thousands_uses_decimal_or_integer() {
        // 1.0k–9.9k: one decimal digit.
        assert_eq!(format_token_count(1_000), "1.0k");
        assert_eq!(format_token_count(1_172), "1.2k");
        assert_eq!(format_token_count(9_999), "10.0k");
        // 10k–999k: integer.
        assert_eq!(format_token_count(10_000), "10k");
        assert_eq!(format_token_count(184_502), "184k");
        assert_eq!(format_token_count(999_999), "999k");
    }

    #[test]
    fn format_token_count_millions() {
        assert_eq!(format_token_count(1_000_000), "1.0M");
        assert_eq!(format_token_count(2_500_000), "2.5M");
        assert_eq!(format_token_count(10_000_000), "10M");
    }

    #[test]
    fn format_token_count_no_long_digit_runs() {
        // The defence against the notification daemon's "OTP detector":
        // no compact representation should have 4+ consecutive digits
        // (which is what such heuristics flag as a copyable code).
        for n in [1_000_u64, 1_172, 9_999, 10_000, 184_502, 1_000_000] {
            let s = format_token_count(n);
            let max_run = s
                .chars()
                .fold((0_usize, 0_usize), |(cur, max), c| {
                    if c.is_ascii_digit() {
                        let cur = cur + 1;
                        (cur, max.max(cur))
                    } else {
                        (0, max)
                    }
                })
                .1;
            assert!(
                max_run <= 3,
                "format_token_count({n}) = {s:?} has a {max_run}-digit run; \
                 the notification daemon's OTP heuristic may flag it",
            );
        }
    }
}
