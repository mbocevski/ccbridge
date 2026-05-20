// SPDX-License-Identifier: MIT
//! Aggregator — single-writer task owning all daemon state.
//!
//! # Architecture
//!
//! One tokio task owns [`Aggregator`] entirely.  All other tasks communicate
//! with it through an [`mpsc::Sender<AggregatorMsg>`] that is cheaply cloned
//! and passed to every module that needs to report state or request a decision.
//!
//! The aggregator itself is **never blocked** waiting for external input.
//! For `PreToolUse` events it stores the response side of a oneshot channel
//! and returns immediately; the ingest handler holds the receive side and
//! waits (with a configurable timeout) for a [`WireDecision`] to be fired
//! back through a subsequent [`AggregatorMsg::PermissionDecision`].
//!
//! # Heartbeat fanout
//!
//! The aggregator owns a [`broadcast::Sender<Heartbeat>`] (capacity 16).
//! Every emit module (swaync, BLE, ctrl-socket, HTTP) subscribes to a
//! [`broadcast::Receiver`] before the aggregator task starts.  The aggregator
//! calls `hb_tx.send()` on every state change and on a 10 s keepalive tick.
//! Slow receivers that fall behind get a `Lagged` error and skip ahead —
//! that's the right semantic; the next heartbeat will arrive within 10 s.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use ccbridge_proto::buddy::{Heartbeat, MatchSource, PromptInfo, WireDecision};
use ccbridge_proto::hook::HookEvent;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info, warn};

use arc_swap::ArcSwap;

use crate::config::Fallback;
use crate::permission::{AllowOrDeny, Allowlist, ProjectAllowlistCache};

// ---------------------------------------------------------------------------
// AllowOrDeny → MatchSource conversion (wire boundary)
// ---------------------------------------------------------------------------

impl From<AllowOrDeny> for MatchSource {
    fn from(v: AllowOrDeny) -> Self {
        match v {
            AllowOrDeny::Allow => MatchSource::Allow,
            AllowOrDeny::Deny => MatchSource::Deny,
        }
    }
}

// ---------------------------------------------------------------------------
// Public type aliases
// ---------------------------------------------------------------------------

pub type SessionId = String;
pub type ToolUseId = String;

/// A cloneable handle for sending messages to the [`Aggregator`] task.
pub type AggregatorTx = mpsc::Sender<AggregatorMsg>;

// ---------------------------------------------------------------------------
// AggregatorMsg
// ---------------------------------------------------------------------------

/// Messages sent to the single-writer [`Aggregator`] task.
pub enum AggregatorMsg {
    /// A hook event has arrived from the ingest socket.
    ///
    /// The responder is fired exactly once with a [`HookOutcome`]:
    /// - For most events: `HookOutcome::Immediate(HookResponse::Passthrough)`.
    /// - For `PreToolUse`: either an immediate decision (allow/deny) or
    ///   `HookOutcome::Await`, which carries the receive side of the approval
    ///   oneshot for the ingest handler to await.
    HookEvent {
        event: Box<HookEvent>,
        respond: oneshot::Sender<HookOutcome>,
    },

    /// An emit module (swaync, BLE, ctrl-socket) has resolved a pending
    /// permission prompt.  The aggregator pops the waiting oneshot from
    /// `pending` and fires it.
    ///
    /// `respond` is optional: emitters that don't care about the result
    /// (notify, AllowlistAlways) pass `None`.  ctrl passes `Some(tx)` so
    /// it can ack `ok:false / "unknown_id"` for stale clicks instead of
    /// optimistically returning `ok:true`.
    PermissionDecision {
        tool_use_id: ToolUseId,
        decision: WireDecision,
        respond: Option<oneshot::Sender<PermissionDecisionResult>>,
    },

    /// Return a snapshot of the current heartbeat state.  Used by emit modules
    /// that need the current state on demand (e.g. ctrl-socket initial burst).
    GetHeartbeat { respond: oneshot::Sender<Heartbeat> },

    /// Token counts updated by the JSONL tail (task 27993d8d).
    TokensUpdate { output_tokens: u64 },

    /// One-shot at daemon startup: seed the aggregator's `TokenState`
    /// from the persisted `tokens.json` so heartbeats reflect the full
    /// historical totals rather than just the deltas observed since
    /// this aggregator booted.
    ///
    /// Sent by the JSONL watcher as the very first thing it does, before
    /// any [`Self::TokensUpdate`] deltas.  Idempotent: if multiple senders
    /// race (shouldn't happen — only the watcher sends this), the larger
    /// value wins so we never regress the displayed total.
    SetInitialTokens {
        cumulative: u64,
        today: u64,
        date: String,
    },

    /// The daily token counter reset at local midnight.
    ///
    /// `date` is the new date string (`"YYYY-MM-DD"` in UTC) computed by the
    /// JSONL midnight-reset task before it sends this message, so the aggregator
    /// never needs to compute dates itself.
    DailyReset { date: String },

    /// Push a transcript entry into the entries ring buffer (capacity 8).
    ///
    /// Sent by the JSONL tail when it extracts assistant text content or tool
    /// summaries that should appear in `heartbeat.entries`.
    AddEntry { text: String },

    /// The approval timeout in `ingest::hooks` fired before any emit module
    /// sent a decision.  The hook has already written `Ask` to its stdout so
    /// Claude Code's own TUI will handle the decision; the aggregator clears
    /// its pending state so the next heartbeat shows `prompt: None` / `waiting: 0`.
    ///
    /// Dropping the sender from `pending` is safe: if a late decision somehow
    /// arrives (race between timeout + emit module), the existing
    /// `handle_permission_decision` path logs a warn and discards it.
    ApprovalTimedOut { tool_use_id: ToolUseId },

    /// User clicked "Always" on a swaync notification.  The aggregator
    /// derives the most-conservative allowlist pattern for the pending event,
    /// writes it to `settings.local.json`, and approves the current call.
    ///
    /// For tools where a specific pattern cannot be auto-derived (bare-tool
    /// case), the call is denied with a helpful reason rather than risking
    /// a too-broad pattern being silently written.
    AllowlistAlways { tool_use_id: ToolUseId },

    /// Idle-gate timer for a `Stop` event has elapsed.  Sent by a task
    /// spawned from the `Stop` handler after `idle_grace_ms`.
    ///
    /// The aggregator checks `activity_serial` against the session's
    /// current `last_activity_serial`: if they still match, nothing has
    /// happened on this session during the grace window — Claude is
    /// genuinely done — and the `TurnDoneEvent` is broadcast.  If any
    /// activity bumped the counter (new user prompt, a tool call mid-task,
    /// etc.), the serials diverge and we skip the notification.
    CheckTurnDone {
        session_id: SessionId,
        activity_serial: u64,
        snapshot: TurnDoneSnapshot,
    },
}

/// Frozen-at-Stop data that travels with `CheckTurnDone` and is the payload
/// of the eventual broadcast `TurnDoneEvent`. Captured at Stop time because
/// `tokens.cumulative` and the session's `cwd`/`response` could change in
/// the grace window.
#[derive(Debug, Clone)]
pub struct TurnDoneSnapshot {
    /// Last assistant response, truncated to ~80 chars.  May be empty when
    /// Claude Code emits a Stop with no assistant turn (e.g. timeout stop).
    pub response_snippet: String,
    /// Working directory of the session (basename presented in the notif).
    pub cwd: String,
    /// Cumulative token count at Stop time.
    pub tokens_cumulative: u64,
}

/// Outcome of an `AggregatorMsg::PermissionDecision`, returned through
/// the optional `respond` oneshot.
///
/// `Applied` means the aggregator found a pending approval matching the
/// `tool_use_id` and dispatched the decision to the hook handler.
///
/// `UnknownId` means there was no pending approval — typically a stale
/// click after a daemon restart, or a decision that arrived after the
/// approval timeout already fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecisionResult {
    Applied,
    UnknownId,
}

// ---------------------------------------------------------------------------
// HookResponse / HookOutcome
// ---------------------------------------------------------------------------

/// What the hook ingest handler writes back to the hook binary's stdout.
///
/// This type represents the wire-serialisable response variants only.
/// The await-and-wait control-flow case lives in [`HookOutcome::Await`].
#[derive(Debug)]
pub enum HookResponse {
    /// Exit 0 with no output — Claude Code's own TUI handles the decision.
    Passthrough,

    /// Write `{"hookSpecificOutput":{"hookEventName":"PreToolUse",
    ///          "permissionDecision":"allow"}}` to stdout.
    PermissionDecision(WireDecision),

    /// Write `{"hookSpecificOutput":{"hookEventName":"PreToolUse",
    ///          "permissionDecision":"deny","permissionDecisionReason":"<reason>"}}`.
    ///
    /// Used when a confident deny is reached — either by a deny-list rule or
    /// when the user explicitly clicks Deny via an emit module.
    HardDeny { reason: String },
}

/// What the aggregator returns to the hook ingest handler.
///
/// Either an immediate wire response, or a signal to run the await loop with
/// the provided oneshot receiver.
#[derive(Debug)]
pub enum HookOutcome {
    /// Write this response immediately and close the connection.
    Immediate(HookResponse),

    /// Stash the approval and await a decision from an emit module.
    ///
    /// The ingest handler should await `rx` with `approval_timeout`, then write
    /// a `PermissionDecision` or (on timeout) a response determined by `fallback`.
    Await {
        /// Fires when a [`AggregatorMsg::PermissionDecision`] arrives for this id.
        rx: oneshot::Receiver<WireDecision>,
        tool_use_id: ToolUseId,
        session_id: SessionId,
        approval_timeout: Duration,
        /// What to do when `approval_timeout` elapses with no decision.
        fallback: Fallback,
    },
}

// ---------------------------------------------------------------------------
// PendingApproval
// ---------------------------------------------------------------------------

/// All per-approval context needed for heartbeat display, Always writes,
/// and annotation rendering.  Keyed by `tool_use_id` in `Aggregator.pending`.
struct PendingApproval {
    /// Full event — needed by `handle_allowlist_always` to derive the pattern.
    event: Box<ccbridge_proto::hook::PreToolUseEvent>,
    /// Display name for `PromptInfo.tool`.
    tool_name: String,
    /// Short hint for `PromptInfo.hint`.
    tool_hint: String,
    /// Pattern that produced `AskAnnotated`; `None` for plain intercept.
    matched_pattern: Option<String>,
    /// Which side of the allowlist the matched pattern came from.
    match_source: Option<AllowOrDeny>,
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

/// Per-session state tracked by the aggregator.
#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    pub cwd: String,
    /// `true` while the session is actively generating a response.
    pub running: bool,
    /// `true` while the session is blocked waiting for a permission decision.
    pub waiting: bool,
    /// The `tool_use_id` of the pending `PreToolUse`, if any.
    ///
    /// Used as the lookup key into `Aggregator.pending` for heartbeat display.
    pub pending_tool_use_id: Option<ToolUseId>,
    /// Monotonic counter, bumped on every hook event that means
    /// "this session is still doing something" — `UserPromptSubmit`,
    /// `PreToolUse`, `PostToolUse`, `SessionStart`.
    ///
    /// Used by the turn-done idle gate: when a `Stop` fires, we capture
    /// the current value, sleep `idle_grace_ms`, then re-check.  If the
    /// counter has advanced — meaning the user submitted a new prompt OR
    /// Claude continued to a tool call mid-task — we skip the
    /// "Claude is done" notification.  Stop itself does NOT bump (a Stop
    /// is exactly the moment we want to start the idle clock from).
    pub last_activity_serial: u64,
}

impl Session {
    fn new(id: impl Into<String>, cwd: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            cwd: cwd.into(),
            running: false,
            waiting: false,
            pending_tool_use_id: None,
            last_activity_serial: 0,
        }
    }

    /// Clear pending-approval state on this session.
    fn clear_pending(&mut self) {
        self.waiting = false;
        self.pending_tool_use_id = None;
    }

    /// Bump the activity serial.  Called from every handler that means
    /// "this session is still doing something" — see field doc.
    fn bump_activity(&mut self) {
        self.last_activity_serial = self.last_activity_serial.saturating_add(1);
    }
}

// ---------------------------------------------------------------------------
// TurnDoneEvent
// ---------------------------------------------------------------------------

/// Broadcast when a session has been idle for the configured grace period
/// after a `Stop` event — i.e. Claude finished its turn and the user
/// hasn't followed up.
///
/// Subscribed to by [`crate::emit::notify`] to post a non-actionable info
/// notification.  Multiple sessions can fire independently.
#[derive(Debug, Clone)]
pub struct TurnDoneEvent {
    pub session_id: SessionId,
    pub response_snippet: String,
    pub cwd: String,
    pub tokens_cumulative: u64,
}

// ---------------------------------------------------------------------------
// Token state
// ---------------------------------------------------------------------------

/// Cumulative and daily token counters.
///
/// `date` is stored as `"YYYY-MM-DD"` (UTC).  The midnight-reset task fires
/// [`AggregatorMsg::DailyReset`] carrying the new date string; the aggregator
/// zeroes `today` and writes `self.tokens.date = date`.  No chrono/time crate
/// dependency in the aggregator — the JSONL module owns midnight scheduling.
#[derive(Debug, Default)]
pub struct TokenState {
    pub cumulative: u64,
    pub today: u64,
    /// Date of the current `today` counter, `"YYYY-MM-DD"` in UTC.
    pub date: String,
}

// ---------------------------------------------------------------------------
// Aggregator
// ---------------------------------------------------------------------------

/// Single-writer task owning all ccbridge runtime state.
pub struct Aggregator {
    /// Active Claude Code sessions, keyed by session ID.
    sessions: HashMap<SessionId, Session>,

    /// Pending `PreToolUse` approvals, keyed by `tool_use_id`.
    ///
    /// Each entry holds:
    /// - The oneshot sender that fires a [`WireDecision`] back to the ingest handler.
    /// - The [`PendingApproval`] payload (event, display fields, annotation).
    ///
    /// Single map avoids coordination between two separate maps and is the
    /// authoritative source of truth for all per-approval context.
    pending: HashMap<ToolUseId, (oneshot::Sender<WireDecision>, PendingApproval)>,

    /// Token counters.
    tokens: TokenState,

    /// Recent transcript lines for `heartbeat.entries`, newest-first.
    ///
    /// Note: `VecDeque::with_capacity(ENTRIES_CAP)` is a pre-allocation hint,
    /// not a hard cap.  The manual cap-on-push in [`Self::push_entry`] is what
    /// enforces the ceiling.
    entries: VecDeque<String>,

    /// Broadcast channel for heartbeat fanout to all emit modules.
    hb_tx: broadcast::Sender<Heartbeat>,

    /// How long the ingest handler waits for an approval before applying the
    /// fallback policy.  Owned here so it is surfaced in tests.
    pub approval_timeout: Duration,

    /// What the ingest handler does when the approval timer elapses with no
    /// decision from any emit module.
    pub fallback: Fallback,

    /// Per-project allowlist cache, cascaded with the user-global allowlist.
    allowlist_cache: Arc<ProjectAllowlistCache>,

    /// Path to the allowlist audit log.
    /// Used by `AllowlistAlways` to append an audit entry.
    audit_log_path: std::path::PathBuf,

    /// Broadcast channel for "Claude finished" notifications.
    /// Subscribers (notify emitter) receive an event when a session has been
    /// idle for `turn_done_idle_grace` after a `Stop`.
    turn_done_tx: broadcast::Sender<TurnDoneEvent>,

    /// How long after `Stop` we wait before firing `TurnDoneEvent`.
    /// `Duration::ZERO` disables the feature (no idle-gate task spawned).
    turn_done_idle_grace: Duration,

    /// Sender clone for spawning idle-gate tasks back to ourselves.
    /// Held inside the aggregator so the spawn site doesn't need to
    /// thread `tx` through `handle_hook_event`'s callers.
    self_tx: Option<AggregatorTx>,
}

/// Maximum number of transcript entries kept for the heartbeat `entries` field.
const ENTRIES_CAP: usize = 8;

/// Heartbeat keepalive interval (10 seconds per BLE spec).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Broadcast channel capacity.  On overflow, slow subscribers get `Lagged`
/// and skip ahead — correct for heartbeats (next one arrives within 10 s).
const BROADCAST_CAPACITY: usize = 16;

/// Default approval timeout — matches spec `[approvals] timeout_ms = 30000`.
pub const DEFAULT_APPROVAL_TIMEOUT: Duration = Duration::from_millis(30_000);

impl Aggregator {
    /// Create a new [`Aggregator`] and return it together with the broadcast
    /// receivers that emit modules should subscribe to.
    pub fn new(
        approval_timeout: Duration,
        fallback: Fallback,
        allowlist_cache: Arc<ProjectAllowlistCache>,
        audit_log_path: std::path::PathBuf,
    ) -> (
        Self,
        broadcast::Receiver<Heartbeat>,
        broadcast::Receiver<TurnDoneEvent>,
    ) {
        let (hb_tx, hb_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let (turn_done_tx, turn_done_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let agg = Self {
            sessions: HashMap::new(),
            pending: HashMap::new(),
            tokens: TokenState::default(),
            entries: VecDeque::with_capacity(ENTRIES_CAP),
            hb_tx,
            approval_timeout,
            fallback,
            allowlist_cache,
            audit_log_path,
            turn_done_tx,
            turn_done_idle_grace: Duration::ZERO, // disabled until set
            self_tx: None,
        };
        (agg, hb_rx, turn_done_rx)
    }

    /// Configure the turn-done idle-gate window.  `Duration::ZERO` disables
    /// the feature entirely (no idle-gate tasks spawned, no broadcasts).
    ///
    /// Must be set before [`Self::run`] is called; the spawn helpers below
    /// wire this from `[emit.notify.turn_done] idle_grace_ms`.
    pub fn set_turn_done_idle_grace(&mut self, idle_grace: Duration) {
        self.turn_done_idle_grace = idle_grace;
    }

    /// Plumb a self-clone of the aggregator's mpsc sender so the Stop
    /// handler can spawn idle-gate tasks that send `CheckTurnDone` back.
    ///
    /// Set by the spawn helpers in this module; tests that build the
    /// `Aggregator` by hand can leave it `None` (idle gate is a no-op).
    fn set_self_tx(&mut self, tx: AggregatorTx) {
        self.self_tx = Some(tx);
    }

    // -----------------------------------------------------------------------
    // Heartbeat construction
    // -----------------------------------------------------------------------

    /// Compute the current heartbeat snapshot from live state.
    ///
    /// Never cached — called on every state change and on the keepalive tick.
    pub fn snapshot(&self) -> Heartbeat {
        let total = self.sessions.len() as u32;
        let running = self.sessions.values().filter(|s| s.running).count() as u32;
        let waiting = self.sessions.values().filter(|s| s.waiting).count() as u32;

        // Build one `PromptInfo` per waiting session, in session-iteration
        // order.  Session HashMap order is unspecified; clients must key by
        // `session_id` rather than rely on positional stability.
        let prompts: Vec<PromptInfo> = self
            .sessions
            .values()
            .filter(|s| s.waiting)
            .filter_map(|s| {
                let id = s.pending_tool_use_id.as_ref()?;
                let (_, approval) = self.pending.get(id)?;
                Some(PromptInfo {
                    id: id.clone(),
                    tool: approval.tool_name.clone(),
                    hint: approval.tool_hint.clone(),
                    matched_pattern: approval.matched_pattern.clone(),
                    matched_source: approval.match_source.map(MatchSource::from),
                    session_id: Some(s.id.clone()),
                    cwd: Some(s.cwd.clone()),
                    agent_type: approval.event.agent_type.clone(),
                })
            })
            .collect();

        let msg = match prompts.len() {
            0 if running > 0 => format!("running ({})", running),
            0 if total > 0 => format!("idle ({})", total),
            0 => "no sessions".to_owned(),
            1 => format!("approve: {}", prompts[0].tool),
            n => format!("approve: {n} pending"),
        };

        let entries: Vec<String> = self.entries.iter().cloned().collect();

        // Defensive observability: entries should be non-empty whenever a
        // session has been seen.  An empty entries Vec with total > 0 means
        // the transcript-line plumbing is broken upstream — log at debug so
        // we have a breadcrumb, but don't fail the snapshot.  Demote to
        // tracing/debug to keep noise out of normal operation.
        if entries.is_empty() && total > 0 {
            tracing::debug!(
                total,
                running,
                waiting,
                "snapshot: entries empty despite total > 0 — transcript plumbing may be broken",
            );
        }

        Heartbeat {
            total,
            running,
            waiting,
            msg,
            entries,
            tokens: self.tokens.cumulative,
            tokens_today: self.tokens.today,
            prompts,
        }
    }

    // -----------------------------------------------------------------------
    // Broadcast helpers
    // -----------------------------------------------------------------------

    fn broadcast_heartbeat(&self) {
        let hb = self.snapshot();
        // Ignore Err — no active subscribers is fine.
        let _ = self.hb_tx.send(hb);
    }

    /// Subscribe to the heartbeat broadcast channel.
    pub fn subscribe(&self) -> broadcast::Receiver<Heartbeat> {
        self.hb_tx.subscribe()
    }

    // -----------------------------------------------------------------------
    // Event handlers (called from the run loop)
    // -----------------------------------------------------------------------

    fn handle_hook_event(&mut self, event: HookEvent, respond: oneshot::Sender<HookOutcome>) {
        match event {
            HookEvent::SessionStart(e) => {
                info!(session_id = %e.base.session_id, cwd = %e.base.cwd, "session started");
                let session = self
                    .sessions
                    .entry(e.base.session_id.clone())
                    .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
                session.bump_activity();
                self.push_entry(format!("session: {}", e.base.cwd));
                self.broadcast_heartbeat();
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::SessionEnd(e) => {
                info!(session_id = %e.base.session_id, "session ended");
                self.sessions.remove(&e.base.session_id);
                self.push_entry("session ended".to_owned());
                self.broadcast_heartbeat();
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::PreToolUse(e) => {
                use crate::permission::{self, Decision};

                // Bump activity so any pending turn-done idle gate for this
                // session sees that Claude is still actively driving tools
                // and suppresses the "Claude is done" notification.
                self.sessions
                    .entry(e.base.session_id.clone())
                    .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd))
                    .bump_activity();

                // Cascade project-local + project + user allowlists for this cwd.
                let cascade = self
                    .allowlist_cache
                    .cascade_for(std::path::Path::new(&e.base.cwd));
                match permission::evaluate(&e, &cascade) {
                    Decision::Allow { reason } => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            %reason,
                            "PreToolUse allowed without prompt",
                        );
                        let _ = respond.send(HookOutcome::Immediate(
                            HookResponse::PermissionDecision(WireDecision::Once),
                        ));
                    }
                    Decision::Deny { reason } => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            %reason,
                            "PreToolUse denied without prompt",
                        );
                        let _ =
                            respond.send(HookOutcome::Immediate(HookResponse::HardDeny { reason }));
                    }
                    Decision::AskAnnotated(ann) => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            pattern = %ann.matched_pattern,
                            source = ?ann.source,
                            "PreToolUse ambiguous match — intercepting with annotation",
                        );
                        self.start_intercept(e, respond, Some(ann));
                    }
                    Decision::Intercept => {
                        self.start_intercept(e, respond, None);
                    }
                }
            }

            HookEvent::PostToolUse(e) => {
                debug!(session_id = %e.base.session_id, tool = %e.tool_name, "PostToolUse");
                if let Some(s) = self.sessions.get_mut(&e.base.session_id) {
                    s.running = false;
                    s.clear_pending();
                    // Tool just ran — Claude is still working, idle gate
                    // for any preceding Stop should be invalidated.
                    s.bump_activity();
                }
                self.push_entry(format!(
                    "{}: {}",
                    e.tool_name,
                    format_tool_hint(&e.tool_input),
                ));
                self.broadcast_heartbeat();
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::Stop(e) => {
                debug!(session_id = %e.base.session_id, "Stop");
                // Stop means the response turn ended — clear running AND any
                // stale waiting state (Claude Code won't fire Stop while
                // genuinely waiting for an approval, but guard against it so
                // heartbeat.waiting never gets stuck at 1 forever).
                if let Some(s) = self.sessions.get_mut(&e.base.session_id) {
                    s.running = false;
                    s.clear_pending();
                }
                self.broadcast_heartbeat();
                self.spawn_turn_done_gate(&e);
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::Notification(e) => {
                debug!(
                    session_id = %e.base.session_id,
                    notification_type = %e.notification_type,
                    "Notification",
                );
                self.push_entry(format!("notif: {}", e.message));
                self.broadcast_heartbeat();
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::UserPromptSubmit(e) => {
                debug!(session_id = %e.base.session_id, "UserPromptSubmit");
                let session = self
                    .sessions
                    .entry(e.base.session_id.clone())
                    .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
                session.running = true;
                // Bump the activity serial so any pending CheckTurnDone task
                // for this session sees the user is back at the keyboard
                // and skips the "Claude is done" notification.
                session.bump_activity();
                self.broadcast_heartbeat();
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }

            HookEvent::Unknown => {
                debug!("Unknown hook event — ignoring");
                let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            }
        }
    }

    /// Register a `PreToolUse` event into the hold-and-wait approval flow.
    ///
    /// Creates a oneshot channel, stashes the sender (plus all approval context)
    /// in `pending`, marks the session as `waiting`, and returns
    /// [`HookOutcome::Await`] to the ingest handler, which drives the timeout loop.
    ///
    /// `annotation` is `Some(PatternAnnotation)` when the decision came from
    /// [`crate::permission::Decision::AskAnnotated`].  Pass `None` for plain intercept.
    fn start_intercept(
        &mut self,
        e: ccbridge_proto::hook::PreToolUseEvent,
        respond: oneshot::Sender<HookOutcome>,
        annotation: Option<crate::permission::PatternAnnotation>,
    ) {
        // Guard against duplicate tool_use_ids across sessions.  Claude Code
        // generates globally-unique IDs within a session, but two concurrent
        // sessions could theoretically collide.  Overwriting the existing entry
        // would silently contaminate the first session's approval flow; instead,
        // fall through to Claude's own TUI for the second call.
        if self.pending.contains_key(&e.tool_use_id) {
            warn!(
                tool_use_id = %e.tool_use_id,
                new_session = %e.base.session_id,
                "duplicate tool_use_id in start_intercept — \
                 dropping new approval, falling through to Claude TUI",
            );
            let _ = respond.send(HookOutcome::Immediate(HookResponse::Passthrough));
            return;
        }

        let hint = format_tool_hint(&e.tool_input);
        debug!(
            session_id = %e.base.session_id,
            tool = %e.tool_name,
            tool_use_id = %e.tool_use_id,
            hint = %hint,
            "PreToolUse — holding for approval",
        );

        let (decision_tx, decision_rx) = oneshot::channel::<WireDecision>();

        let (matched_pattern, match_source) = match annotation {
            Some(ann) => (Some(ann.matched_pattern), Some(ann.source)),
            None => (None, None),
        };

        let approval = PendingApproval {
            event: Box::new(e.clone()),
            tool_name: e.tool_name.clone(),
            tool_hint: hint,
            matched_pattern,
            match_source,
        };
        self.pending
            .insert(e.tool_use_id.clone(), (decision_tx, approval));

        let session = self
            .sessions
            .entry(e.base.session_id.clone())
            .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
        session.waiting = true;
        session.pending_tool_use_id = Some(e.tool_use_id.clone());

        self.broadcast_heartbeat();

        let _ = respond.send(HookOutcome::Await {
            rx: decision_rx,
            tool_use_id: e.tool_use_id,
            session_id: e.base.session_id,
            approval_timeout: self.approval_timeout,
            fallback: self.fallback,
        });
    }

    fn handle_permission_decision(
        &mut self,
        tool_use_id: ToolUseId,
        decision: WireDecision,
    ) -> PermissionDecisionResult {
        match self.pending.remove(&tool_use_id) {
            Some((tx, _approval)) => {
                for session in self.sessions.values_mut() {
                    if session.pending_tool_use_id.as_deref() == Some(&tool_use_id) {
                        session.clear_pending();
                    }
                }
                self.broadcast_heartbeat();
                if tx.send(decision).is_err() {
                    warn!(
                        tool_use_id = %tool_use_id,
                        "approval receiver gone before decision fired (timeout won the race)",
                    );
                }
                PermissionDecisionResult::Applied
            }
            None => {
                warn!(tool_use_id = %tool_use_id, "no pending approval for this tool_use_id");
                PermissionDecisionResult::UnknownId
            }
        }
    }

    fn handle_allowlist_always(&mut self, tool_use_id: ToolUseId) {
        use crate::permission::additions::{
            derive_pattern, resolve_write_target, write_allow_pattern, AdditionMetadata,
            DerivedPattern,
        };

        // O(1) lookup — no more session scan.
        let event = match self.pending.get(&tool_use_id) {
            Some((_, approval)) => (*approval.event).clone(),
            None => {
                // The approval was already resolved before the Always click
                // arrived — either the timeout fired and Claude's TUI handled
                // it, or another emitter (ctrl socket, BLE) sent a decision
                // first.  Nothing to write; the user should re-trigger if they
                // still want an Always entry.
                warn!(
                    tool_use_id = %tool_use_id,
                    "AllowlistAlways: approval already resolved \
                     (timeout fired or another emitter decided) — nothing to write",
                );
                return;
            }
        };

        match derive_pattern(&event) {
            DerivedPattern::Specific(pattern) => {
                let target = resolve_write_target(std::path::Path::new(&event.base.cwd));
                let metadata = AdditionMetadata {
                    tool_use_id: tool_use_id.clone(),
                    session_id: event.base.session_id.clone(),
                    agent_type: event.agent_type.clone(),
                };
                match write_allow_pattern(&target, &pattern, &self.audit_log_path, metadata) {
                    Ok(()) => {
                        info!(%pattern, root = %target.root.display(), "AllowlistAlways: wrote allow pattern")
                    }
                    Err(e) => warn!("AllowlistAlways: failed to write pattern: {e:#}"),
                }
                // Approve this specific call regardless of write success.
                // Settings watcher will reload for future calls.
                self.handle_permission_decision(tool_use_id, WireDecision::Once);
            }
            DerivedPattern::BareToolNeedsConfirmation { tool } => {
                warn!(
                    %tool,
                    tool_use_id = %tool_use_id,
                    "AllowlistAlways: no specific pattern derivable; denying",
                );
                self.handle_permission_decision(tool_use_id, WireDecision::Deny);
            }
        }
    }

    /// Spawn an idle-gate timer for a `Stop` event.  After
    /// `turn_done_idle_grace`, the spawned task sends `CheckTurnDone` back
    /// to the aggregator with a snapshot of session state at Stop time.
    ///
    /// No-op when:
    /// - `turn_done_idle_grace` is `Duration::ZERO` (feature disabled), or
    /// - `self_tx` is `None` (test fixture without a real spawn loop).
    fn spawn_turn_done_gate(&self, e: &ccbridge_proto::hook::StopEvent) {
        if self.turn_done_idle_grace.is_zero() {
            return;
        }
        let Some(tx) = self.self_tx.clone() else {
            return;
        };
        let session_id = e.base.session_id.clone();
        let activity_serial = self
            .sessions
            .get(&session_id)
            .map(|s| s.last_activity_serial)
            .unwrap_or(0);
        let snapshot = TurnDoneSnapshot {
            response_snippet: e
                .response
                .as_deref()
                .map(truncate_snippet)
                .unwrap_or_default(),
            cwd: e.base.cwd.clone(),
            tokens_cumulative: self.tokens.cumulative,
        };
        let grace = self.turn_done_idle_grace;
        tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            // Best-effort: if the aggregator has shut down between Stop and
            // grace expiry, the send fails — the broadcast is gone anyway.
            let _ = tx
                .send(AggregatorMsg::CheckTurnDone {
                    session_id,
                    activity_serial,
                    snapshot,
                })
                .await;
        });
    }

    /// Idle-gate handler.  Fires `TurnDoneEvent` if the session's
    /// `last_activity_serial` hasn't advanced since the Stop captured it.
    fn handle_check_turn_done(
        &self,
        session_id: SessionId,
        activity_serial: u64,
        snapshot: TurnDoneSnapshot,
    ) {
        let current = self
            .sessions
            .get(&session_id)
            .map(|s| s.last_activity_serial)
            .unwrap_or(activity_serial); // session gone → treat as still idle
        if current != activity_serial {
            debug!(
                %session_id,
                "turn-done: session activity advanced during idle grace — skipping notification",
            );
            return;
        }
        // Best-effort broadcast; no subscribers is fine.
        let _ = self.turn_done_tx.send(TurnDoneEvent {
            session_id,
            response_snippet: snapshot.response_snippet,
            cwd: snapshot.cwd,
            tokens_cumulative: snapshot.tokens_cumulative,
        });
    }

    fn push_entry(&mut self, entry: String) {
        if self.entries.len() >= ENTRIES_CAP {
            self.entries.pop_back();
        }
        self.entries.push_front(entry);
    }

    // -----------------------------------------------------------------------
    // Main run loop
    // -----------------------------------------------------------------------

    /// Consume the aggregator, running it as a tokio task until `rx` is closed.
    pub async fn run(mut self, mut rx: mpsc::Receiver<AggregatorMsg>) {
        let mut tick = interval(HEARTBEAT_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        None => {
                            info!("AggregatorMsg channel closed — shutting down aggregator");
                            break;
                        }
                        Some(AggregatorMsg::HookEvent { event, respond }) => {
                            self.handle_hook_event(*event, respond);
                        }
                        Some(AggregatorMsg::PermissionDecision { tool_use_id, decision, respond }) => {
                            let result = self.handle_permission_decision(tool_use_id, decision);
                            if let Some(tx) = respond {
                                // Receiver may have given up — fine to ignore.
                                let _ = tx.send(result);
                            }
                        }
                        Some(AggregatorMsg::GetHeartbeat { respond }) => {
                            let _ = respond.send(self.snapshot());
                        }
                        Some(AggregatorMsg::TokensUpdate { output_tokens }) => {
                            self.tokens.cumulative += output_tokens;
                            self.tokens.today += output_tokens;
                            self.broadcast_heartbeat();
                        }
                        Some(AggregatorMsg::SetInitialTokens { cumulative, today, date }) => {
                            // Seed from persisted state.  `max` so a late
                            // SetInitialTokens (e.g. arriving after a
                            // TokensUpdate raced ahead) never regresses
                            // the visible total.
                            self.tokens.cumulative = self.tokens.cumulative.max(cumulative);
                            self.tokens.today = self.tokens.today.max(today);
                            if self.tokens.date.is_empty() {
                                self.tokens.date = date;
                            }
                            debug!(
                                cumulative = self.tokens.cumulative,
                                today = self.tokens.today,
                                date = %self.tokens.date,
                                "tokens: seeded from persisted state",
                            );
                            self.broadcast_heartbeat();
                        }
                        Some(AggregatorMsg::DailyReset { date }) => {
                            debug!(today = self.tokens.today, new_date = %date, "daily token reset");
                            self.tokens.today = 0;
                            self.tokens.date = date;
                            self.broadcast_heartbeat();
                        }
                        Some(AggregatorMsg::AddEntry { text }) => {
                            self.push_entry(text);
                            self.broadcast_heartbeat();
                        }
                        Some(AggregatorMsg::ApprovalTimedOut { tool_use_id }) => {
                            // Drop the entry — any late decision arriving after
                            // this will hit the "no pending approval" warn in
                            // handle_permission_decision and be discarded.
                            self.pending.remove(&tool_use_id);
                            // Clear session waiting flags so the next heartbeat
                            // has prompt: None / waiting: 0.
                            for session in self.sessions.values_mut() {
                                if session.pending_tool_use_id.as_deref() == Some(&tool_use_id) {
                                    session.clear_pending();
                                }
                            }
                            self.broadcast_heartbeat();
                        }
                        Some(AggregatorMsg::AllowlistAlways { tool_use_id }) => {
                            self.handle_allowlist_always(tool_use_id);
                        }
                        Some(AggregatorMsg::CheckTurnDone { session_id, activity_serial, snapshot }) => {
                            self.handle_check_turn_done(session_id, activity_serial, snapshot);
                        }
                    }
                }
                _ = tick.tick() => {
                    self.broadcast_heartbeat();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spawn the Aggregator with an explicit audit log path (production).
pub fn spawn_with_paths(
    approval_timeout: Duration,
    fallback: Fallback,
    allowlist_cache: Arc<ProjectAllowlistCache>,
    audit_log_path: std::path::PathBuf,
    turn_done_idle_grace: Duration,
) -> (
    AggregatorTx,
    broadcast::Receiver<Heartbeat>,
    broadcast::Receiver<TurnDoneEvent>,
) {
    let (mut agg, hb_rx, turn_done_rx) =
        Aggregator::new(approval_timeout, fallback, allowlist_cache, audit_log_path);
    agg.set_turn_done_idle_grace(turn_done_idle_grace);
    let (tx, rx) = mpsc::channel(256);
    agg.set_self_tx(tx.clone());
    tokio::spawn(agg.run(rx));
    (tx, hb_rx, turn_done_rx)
}

/// Spawn the Aggregator with a bare user allowlist (test shim).
///
/// Wraps the user allowlist in a `ProjectAllowlistCache` with a no-op
/// audit log path.  Integration tests that don't exercise `AllowlistAlways`
/// or project-local evaluation can pass a plain `Arc<ArcSwap<Allowlist>>` here.
///
/// Turn-done idle gate is disabled by default — tests that exercise it use
/// [`spawn_with_paths`] directly.
pub fn spawn(
    approval_timeout: Duration,
    fallback: Fallback,
    user_allowlist: Arc<ArcSwap<Allowlist>>,
) -> (AggregatorTx, broadcast::Receiver<Heartbeat>) {
    let cache = Arc::new(ProjectAllowlistCache::new(user_allowlist, None));
    let (tx, hb_rx, _turn_done_rx) = spawn_with_paths(
        approval_timeout,
        fallback,
        cache,
        std::path::PathBuf::from("/dev/null"),
        Duration::ZERO,
    );
    (tx, hb_rx)
}

/// Truncate `s` to at most 80 Unicode scalar values, taking the first line
/// only.  Used for the body of the turn-done notification.
fn truncate_snippet(s: &str) -> String {
    let first_line = s.lines().next().unwrap_or("").trim();
    let mut chars = first_line.chars();
    let head: String = chars.by_ref().take(80).collect();
    if chars.next().is_some() {
        format!("{head}…")
    } else {
        head
    }
}

/// Extract a short hint from a tool_input JSON value for the heartbeat entries
/// log and for `PromptInfo.hint`.
pub(crate) fn format_tool_hint(input: &serde_json::Value) -> String {
    for key in &["command", "file_path", "path", "query", "url"] {
        if let Some(v) = input.get(key).and_then(|v| v.as_str()) {
            return if v.len() > 60 {
                format!("{}…", &v[..60])
            } else {
                v.to_owned()
            };
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ccbridge_proto::hook::{
        HookBase, PermissionMode, PostToolUseEvent, PreToolUseEvent, SessionSource,
        SessionStartEvent, StopEvent, UserPromptSubmitEvent,
    };
    use serde_json::json;
    use tokio::sync::oneshot;

    // -----------------------------------------------------------------------
    // Helpers to build test hook events
    // -----------------------------------------------------------------------

    fn base(session_id: &str) -> HookBase {
        HookBase {
            session_id: session_id.to_owned(),
            transcript_path: "/tmp/test.jsonl".to_owned(),
            cwd: "/tmp".to_owned(),
        }
    }

    fn session_start_event(session_id: &str) -> HookEvent {
        HookEvent::SessionStart(SessionStartEvent {
            base: base(session_id),
            source: SessionSource::Startup,
            model: "claude-test".to_owned(),
            agent_type: None,
        })
    }

    fn pre_tool_use_event(session_id: &str, tool_use_id: &str, tool: &str) -> HookEvent {
        HookEvent::PreToolUse(PreToolUseEvent {
            base: base(session_id),
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: json!({"command": "echo hello"}),
            tool_use_id: tool_use_id.to_owned(),
            agent_id: None,
            agent_type: None,
        })
    }

    fn stop_event(session_id: &str) -> HookEvent {
        HookEvent::Stop(StopEvent {
            base: base(session_id),
            permission_mode: PermissionMode::Default,
            effort: None,
            response: Some("done".to_owned()),
        })
    }

    fn user_prompt_event(session_id: &str) -> HookEvent {
        HookEvent::UserPromptSubmit(UserPromptSubmitEvent {
            base: base(session_id),
            permission_mode: PermissionMode::Default,
            prompt: "do something".to_owned(),
        })
    }

    fn post_tool_use_event(session_id: &str, tool_use_id: &str, tool: &str) -> HookEvent {
        HookEvent::PostToolUse(PostToolUseEvent {
            base: base(session_id),
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: json!({"command": "echo hello"}),
            tool_use_id: tool_use_id.to_owned(),
            tool_result: Some(json!("output")),
            agent_id: None,
            agent_type: None,
        })
    }

    // -----------------------------------------------------------------------
    // Direct Aggregator method tests (no tokio runtime needed)
    // -----------------------------------------------------------------------

    fn new_agg() -> Aggregator {
        let user = Arc::new(ArcSwap::new(
            Arc::new(crate::permission::Allowlist::empty()),
        ));
        let cache = Arc::new(crate::permission::ProjectAllowlistCache::new(user, None));
        let (agg, _rx, _td_rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            cache,
            std::path::PathBuf::from("/dev/null"),
        );
        agg
    }

    #[test]
    fn snapshot_empty() {
        let agg = new_agg();
        let hb = agg.snapshot();
        assert_eq!(hb.total, 0);
        assert_eq!(hb.running, 0);
        assert_eq!(hb.waiting, 0);
        assert!(hb.prompts.is_empty());
        assert_eq!(hb.msg, "no sessions");
    }

    #[test]
    fn snapshot_session_counts() {
        let mut agg = new_agg();

        // Add two sessions — both idle.
        let (tx1, _rx1) = oneshot::channel();
        agg.handle_hook_event(session_start_event("s1"), tx1);
        let (tx2, _rx2) = oneshot::channel();
        agg.handle_hook_event(session_start_event("s2"), tx2);

        let hb = agg.snapshot();
        assert_eq!(hb.total, 2);
        assert_eq!(hb.running, 0);
        assert_eq!(hb.waiting, 0);

        // Mark s1 as running via UserPromptSubmit.
        let (tx3, _rx3) = oneshot::channel();
        agg.handle_hook_event(user_prompt_event("s1"), tx3);

        let hb = agg.snapshot();
        assert_eq!(hb.running, 1);
        assert_eq!(hb.waiting, 0);
    }

    #[test]
    fn pre_tool_use_stores_oneshot_and_populates_prompt() {
        let mut agg = new_agg();

        // Register the session first.
        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        // Fire PreToolUse.
        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess", "toolu_abc", "Bash"), respond_tx);

        // Aggregator should have stored the approval.
        assert!(agg.pending.contains_key("toolu_abc"));

        // The respond channel should have an Await outcome.
        let outcome = respond_rx.try_recv().expect("respond should be fired");
        assert!(matches!(outcome, HookOutcome::Await { .. }));

        // Heartbeat should reflect waiting=1 with populated prompt.
        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 1);
        assert_eq!(hb.prompts.len(), 1, "exactly one prompt expected");
        let prompt = hb.prompts.into_iter().next().expect("prompt must be set");
        assert_eq!(prompt.id, "toolu_abc");
        assert_eq!(prompt.tool, "Bash");
        assert_eq!(prompt.hint, "echo hello");
    }

    #[test]
    fn ask_annotated_decision_surfaces_in_snapshot() {
        // Build an aggregator with an allowlist that produces AskAnnotated for Bash.
        let al = crate::permission::Allowlist {
            allow: vec![crate::permission::Pattern::parse("Bash(git status:*)")],
            deny: vec![],
        };
        let cache = Arc::new(crate::permission::ProjectAllowlistCache::new(
            Arc::new(ArcSwap::new(Arc::new(al))),
            None,
        ));
        let (mut agg, _rx, _td_rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            cache,
            std::path::PathBuf::from("/dev/null"),
        );

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_ann"), tx0);

        // Fire PreToolUse for Bash with no command field → Ambiguous → AskAnnotated.
        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess_ann".to_owned(),
                transcript_path: "/tmp/ann.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: "Bash".to_owned(),
            tool_input: serde_json::json!({}), // no command → Ambiguous
            tool_use_id: "toolu_ann_001".to_owned(),
            agent_id: None,
            agent_type: None,
        });
        let (respond_tx, _) = oneshot::channel();
        agg.handle_hook_event(event, respond_tx);

        // Snapshot must carry the annotation fields.
        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 1);
        assert_eq!(hb.prompts.len(), 1);
        let prompt = hb
            .prompts
            .into_iter()
            .next()
            .expect("prompt must be present for waiting session");
        assert_eq!(
            prompt.matched_pattern.as_deref(),
            Some("Bash(git status:*)"),
            "matched_pattern must be the raw settings.json pattern string"
        );
        assert_eq!(
            prompt.matched_source,
            Some(ccbridge_proto::buddy::MatchSource::Allow),
        );
    }

    #[test]
    fn snapshot_includes_session_id_and_cwd() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess-cwd"), tx0);

        let (respond_tx, _) = oneshot::channel();
        agg.handle_hook_event(
            pre_tool_use_event("sess-cwd", "toolu_cwd_01", "Bash"),
            respond_tx,
        );

        let hb = agg.snapshot();
        assert_eq!(hb.prompts.len(), 1, "exactly one prompt expected");
        let prompt = hb.prompts.into_iter().next().expect("prompt must be set");
        assert_eq!(
            prompt.session_id.as_deref(),
            Some("sess-cwd"),
            "session_id must be populated in PromptInfo"
        );
        assert_eq!(
            prompt.cwd.as_deref(),
            Some("/tmp"),
            "cwd must be populated in PromptInfo"
        );
        assert!(
            prompt.agent_type.is_none(),
            "agent_type must be None for a top-level session"
        );
    }

    #[test]
    fn start_intercept_captures_agent_type() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess-agent"), tx0);

        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess-agent".to_owned(),
                transcript_path: "/tmp/agent.jsonl".to_owned(),
                cwd: "/home/user/dev/project".to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: "Bash".to_owned(),
            tool_input: serde_json::json!({"command": "echo agent"}),
            tool_use_id: "toolu_agent_01".to_owned(),
            agent_id: Some("core@ccbridge".to_owned()),
            agent_type: Some("general-purpose".to_owned()),
        });
        let (respond_tx, _) = oneshot::channel();
        agg.handle_hook_event(event, respond_tx);

        let hb = agg.snapshot();
        assert_eq!(hb.prompts.len(), 1, "exactly one prompt expected");
        let prompt = hb.prompts.into_iter().next().expect("prompt must be set");
        assert_eq!(
            prompt.agent_type.as_deref(),
            Some("general-purpose"),
            "agent_type must be captured from the PreToolUse event"
        );
        assert_eq!(prompt.session_id.as_deref(), Some("sess-agent"),);
    }

    #[test]
    fn snapshot_lists_all_pending_sessions() {
        // Two parallel sessions, each waiting on its own PreToolUse → snapshot
        // surfaces both prompts (no longer collapsed to one).  Order is
        // unspecified (HashMap iteration), so we key the assertion by
        // session_id.
        let mut agg = new_agg();

        for sid in ["sess-A", "sess-B"] {
            let (tx0, _) = oneshot::channel();
            agg.handle_hook_event(session_start_event(sid), tx0);
        }

        for (sid, tid, tool) in [
            ("sess-A", "toolu_a_01", "Bash"),
            ("sess-B", "toolu_b_01", "Edit"),
        ] {
            let (resp, _) = oneshot::channel();
            agg.handle_hook_event(pre_tool_use_event(sid, tid, tool), resp);
        }

        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 2, "both sessions must be counted as waiting");
        assert_eq!(hb.prompts.len(), 2, "both prompts must be in the heartbeat");
        assert_eq!(
            hb.msg, "approve: 2 pending",
            "msg must summarise multi-pending state",
        );

        let by_session: std::collections::HashMap<&str, &PromptInfo> = hb
            .prompts
            .iter()
            .map(|p| (p.session_id.as_deref().unwrap(), p))
            .collect();
        assert_eq!(by_session["sess-A"].id, "toolu_a_01");
        assert_eq!(by_session["sess-A"].tool, "Bash");
        assert_eq!(by_session["sess-B"].id, "toolu_b_01");
        assert_eq!(by_session["sess-B"].tool, "Edit");
    }

    #[test]
    fn snapshot_msg_for_one_pending_names_the_tool() {
        // Sanity check that the single-pending case still produces the
        // friendly "approve: <tool>" form rather than "approve: 1 pending".
        let mut agg = new_agg();
        let (t0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess-solo"), t0);
        let (r, _) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess-solo", "toolu_solo", "Bash"), r);

        let hb = agg.snapshot();
        assert_eq!(hb.msg, "approve: Bash");
    }

    // -----------------------------------------------------------------------
    // AllowlistAlways tests (use spawn_with_paths for real tempdir paths)
    // -----------------------------------------------------------------------

    fn new_agg_with_paths(audit: &std::path::Path) -> Aggregator {
        let user = Arc::new(ArcSwap::new(
            Arc::new(crate::permission::Allowlist::empty()),
        ));
        let cache = Arc::new(crate::permission::ProjectAllowlistCache::new(user, None));
        let (agg, _rx, _td_rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            cache,
            audit.to_path_buf(),
        );
        agg
    }

    #[tokio::test]
    async fn allowlist_always_writes_project_local_when_cwd_has_root() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        // Create .claude/ so find_project_root returns this dir as the root.
        std::fs::create_dir(dir.path().join(".claude")).unwrap();
        let audit = dir.path().join("audit.log");

        let mut agg = new_agg_with_paths(&audit);

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_always"), tx0);

        let cwd = dir.path().to_str().unwrap().to_owned();
        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess_always".to_owned(),
                transcript_path: format!("{}/always.jsonl", cwd),
                cwd: cwd.clone(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: "Bash".to_owned(),
            tool_input: serde_json::json!({"command": "echo always_test"}),
            tool_use_id: "toolu_always_01".to_owned(),
            agent_id: None,
            agent_type: None,
        });
        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(event, respond_tx);

        let mut decision_rx = match respond_rx.try_recv().unwrap() {
            HookOutcome::Await { rx, .. } => rx,
            _ => panic!("expected Await"),
        };

        agg.handle_allowlist_always("toolu_always_01".to_owned());

        let decision = decision_rx
            .try_recv()
            .expect("AllowlistAlways must fire Once");
        assert_eq!(decision, WireDecision::Once);

        // Pattern must be in the project-local settings.local.json, not user file.
        let local = dir.path().join(".claude").join("settings.local.json");
        assert!(local.exists(), "settings.local.json must be created");
        let loaded = crate::setup::load_settings(&local).unwrap();
        let allow = loaded["permissions"]["allow"].as_array().unwrap();
        assert!(
            allow
                .iter()
                .any(|v| v.as_str() == Some("Bash(echo always_test)")),
            "pattern must be in project-local settings.local.json"
        );
    }

    #[test]
    fn allowlist_always_bare_tool_denies_with_reason() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let audit = dir.path().join("audit.log");

        let mut agg = new_agg_with_paths(&audit);

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_bare"), tx0);

        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess_bare".to_owned(),
                transcript_path: "/tmp/bare.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: "WebSearch".to_owned(),
            tool_input: serde_json::json!({"query": "Rust tokio"}),
            tool_use_id: "toolu_bare_01".to_owned(),
            agent_id: None,
            agent_type: None,
        });
        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(event, respond_tx);

        let mut decision_rx = match respond_rx.try_recv().unwrap() {
            HookOutcome::Await { rx, .. } => rx,
            _ => panic!("expected Await"),
        };

        agg.handle_allowlist_always("toolu_bare_01".to_owned());

        let decision = decision_rx.try_recv().expect("AllowlistAlways must fire");
        assert_eq!(
            decision,
            WireDecision::Deny,
            "bare-tool AllowlistAlways must deny"
        );
    }

    #[test]
    fn allowlist_always_unknown_tool_use_id_no_panic() {
        let mut agg = new_agg();
        agg.handle_allowlist_always("toolu_nonexistent".to_owned());
    }

    #[test]
    fn duplicate_tool_use_id_doesnt_corrupt_state() {
        // Two PreToolUse events with the same tool_use_id (from different sessions)
        // must not overwrite the first session's pending entry. The second call
        // should fall through to Claude's TUI (Passthrough) and the first session's
        // pending state must be unaffected.
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess1"), tx0);
        let (tx1, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess2"), tx1);

        // First session registers the approval.
        let (r1_tx, mut r1_rx) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess1", "toolu_dup", "Bash"), r1_tx);
        assert!(matches!(
            r1_rx.try_recv().unwrap(),
            HookOutcome::Await { .. }
        ));
        assert!(agg.pending.contains_key("toolu_dup"));

        // Second session sends the SAME tool_use_id — must get Passthrough.
        let (r2_tx, mut r2_rx) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess2", "toolu_dup", "Bash"), r2_tx);
        assert!(
            matches!(
                r2_rx.try_recv().unwrap(),
                HookOutcome::Immediate(HookResponse::Passthrough)
            ),
            "duplicate tool_use_id must fall through to Claude TUI"
        );

        // First session's entry must still be intact.
        assert!(
            agg.pending.contains_key("toolu_dup"),
            "first session pending entry must survive duplicate attempt"
        );
        assert_eq!(agg.snapshot().waiting, 1, "waiting count must still be 1");
    }

    #[test]
    fn permissive_mode_pre_tool_use_auto_allows_without_prompt() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_bypass"), tx0);

        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess_bypass".to_owned(),
                transcript_path: "/tmp/bypass.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: PermissionMode::BypassPermissions,
            effort: None,
            tool_name: "Bash".to_owned(),
            tool_input: serde_json::json!({"command": "echo hi"}),
            tool_use_id: "toolu_bypass_01".to_owned(),
            agent_id: None,
            agent_type: None,
        });

        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(event, respond_tx);

        let outcome = respond_rx
            .try_recv()
            .expect("short-circuit must fire immediately");
        assert!(
            matches!(
                outcome,
                HookOutcome::Immediate(HookResponse::PermissionDecision(WireDecision::Once))
            ),
            "permissive mode must auto-allow: got {outcome:?}",
        );

        assert!(
            !agg.pending.contains_key("toolu_bypass_01"),
            "permissive mode must not register an approval",
        );
        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 0, "permissive mode must not set waiting");
    }

    #[test]
    fn permission_decision_fires_oneshot_and_clears_waiting() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess", "toolu_xyz", "Bash"), respond_tx);

        let outcome = respond_rx.try_recv().unwrap();
        let mut decision_rx = match outcome {
            HookOutcome::Await { rx, .. } => rx,
            _ => panic!("expected Await"),
        };

        agg.handle_permission_decision("toolu_xyz".to_owned(), WireDecision::Once);

        let decision = decision_rx
            .try_recv()
            .expect("decision should have been fired");
        assert_eq!(decision, WireDecision::Once);

        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 0);
        assert!(hb.prompts.is_empty());

        assert!(agg.pending.is_empty());
    }

    #[test]
    fn approval_timed_out_clears_pending() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(
            pre_tool_use_event("sess", "toolu_timeout", "Bash"),
            respond_tx,
        );

        let _ = respond_rx.try_recv().unwrap();
        assert_eq!(agg.snapshot().waiting, 1);
        assert!(!agg.pending.is_empty());

        // Simulate what the ingest handler does when the timeout fires.
        agg.pending.remove("toolu_timeout");
        for session in agg.sessions.values_mut() {
            if session.pending_tool_use_id.as_deref() == Some("toolu_timeout") {
                session.clear_pending();
            }
        }
        agg.broadcast_heartbeat();

        assert!(agg.pending.is_empty(), "pending must be cleared");
        assert_eq!(agg.snapshot().waiting, 0, "waiting must be 0 after timeout");
        assert!(
            agg.snapshot().prompts.is_empty(),
            "prompt must be None after timeout"
        );
    }

    #[test]
    fn permission_decision_unknown_id_does_not_panic() {
        let mut agg = new_agg();
        agg.handle_permission_decision("toolu_nonexistent".to_owned(), WireDecision::Deny);
    }

    #[test]
    fn stop_clears_running_and_waiting() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        let (tx1, _) = oneshot::channel();
        agg.handle_hook_event(user_prompt_event("sess"), tx1);
        assert_eq!(agg.snapshot().running, 1);

        // Simulate a stale waiting state by inserting directly.
        if let Some(s) = agg.sessions.get_mut("sess") {
            s.waiting = true;
            s.pending_tool_use_id = Some("toolu_stale".to_owned());
        }
        let (dummy_tx, _) = oneshot::channel::<WireDecision>();
        agg.pending.insert(
            "toolu_stale".to_owned(),
            (
                dummy_tx,
                PendingApproval {
                    event: Box::new(PreToolUseEvent {
                        base: base("sess"),
                        permission_mode: PermissionMode::Default,
                        effort: None,
                        tool_name: "Bash".to_owned(),
                        tool_input: json!({}),
                        tool_use_id: "toolu_stale".to_owned(),
                        agent_id: None,
                        agent_type: None,
                    }),
                    tool_name: "Bash".to_owned(),
                    tool_hint: "rm -rf".to_owned(),
                    matched_pattern: None,
                    match_source: None,
                },
            ),
        );
        assert_eq!(agg.snapshot().waiting, 1);

        // Fire Stop — should clear session waiting state.
        let (tx2, _) = oneshot::channel();
        agg.handle_hook_event(stop_event("sess"), tx2);

        let hb = agg.snapshot();
        assert_eq!(hb.running, 0);
        assert_eq!(hb.waiting, 0);
        assert!(hb.prompts.is_empty());
    }

    #[test]
    fn daily_reset_zeroes_today_keeps_cumulative_updates_date() {
        let mut agg = new_agg();
        agg.tokens.cumulative = 50_000;
        agg.tokens.today = 12_000;
        agg.tokens.date = "2026-05-19".to_owned();

        agg.tokens.today = 0;
        agg.tokens.date = "2026-05-20".to_owned();

        assert_eq!(
            agg.tokens.cumulative, 50_000,
            "cumulative must survive reset"
        );
        assert_eq!(agg.tokens.today, 0);
        assert_eq!(agg.tokens.date, "2026-05-20");
    }

    #[test]
    fn entries_ring_buffer_caps_at_eight_newest_first() {
        let mut agg = new_agg();
        for i in 0..12u32 {
            agg.push_entry(format!("entry-{}", i));
        }
        assert_eq!(agg.entries.len(), ENTRIES_CAP);
        assert_eq!(agg.entries[0], "entry-11");
        assert_eq!(agg.entries[ENTRIES_CAP - 1], "entry-4");
    }

    #[test]
    fn snapshot_entries_order_is_newest_first() {
        let mut agg = new_agg();
        agg.push_entry("oldest".to_owned());
        agg.push_entry("middle".to_owned());
        agg.push_entry("newest".to_owned());

        let hb = agg.snapshot();
        assert_eq!(hb.entries[0], "newest");
        assert_eq!(hb.entries[2], "oldest");
    }

    // -----------------------------------------------------------------------
    // Run-loop tests (require tokio runtime)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_loop_responds_to_get_heartbeat() {
        let (tx, hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );
        drop(hb_rx);

        let (respond_tx, respond_rx) = oneshot::channel();
        tx.send(AggregatorMsg::GetHeartbeat {
            respond: respond_tx,
        })
        .await
        .unwrap();

        let hb = respond_rx.await.unwrap();
        assert_eq!(hb.total, 0);
    }

    #[tokio::test]
    async fn run_loop_session_start_increments_total() {
        let (tx, hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );
        drop(hb_rx);

        let (respond_tx, respond_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("run-sess")),
            respond: respond_tx,
        })
        .await
        .unwrap();

        let resp = respond_rx.await.unwrap();
        assert!(matches!(
            resp,
            HookOutcome::Immediate(HookResponse::Passthrough)
        ));

        let (hb_tx, hb_rx2) = oneshot::channel();
        tx.send(AggregatorMsg::GetHeartbeat { respond: hb_tx })
            .await
            .unwrap();
        let hb = hb_rx2.await.unwrap();
        assert_eq!(hb.total, 1);
    }

    #[tokio::test]
    async fn run_loop_pre_tool_use_then_permission_decision() {
        let (tx, mut hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );

        let (r1_tx, r1_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("psess")),
            respond: r1_tx,
        })
        .await
        .unwrap();
        r1_rx.await.unwrap();

        while hb_rx.try_recv().is_ok() {}

        let (r2_tx, r2_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(pre_tool_use_event("psess", "toolu_run1", "Bash")),
            respond: r2_tx,
        })
        .await
        .unwrap();

        let resp = r2_rx.await.unwrap();
        let decision_rx = match resp {
            HookOutcome::Await { rx, .. } => rx,
            _ => panic!("expected Await"),
        };

        let hb = hb_rx.recv().await.unwrap();
        assert_eq!(hb.waiting, 1);
        assert_eq!(hb.prompts.len(), 1);

        tx.send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_run1".to_owned(),
            decision: WireDecision::Once,
            respond: None,
        })
        .await
        .unwrap();

        let decision = decision_rx.await.unwrap();
        assert_eq!(decision, WireDecision::Once);

        let hb2 = hb_rx.recv().await.unwrap();
        assert_eq!(hb2.waiting, 0);
        assert!(hb2.prompts.is_empty());
    }

    #[tokio::test]
    async fn run_loop_daily_reset_zeroes_today_keeps_cumulative() {
        let (tx, mut hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );

        tx.send(AggregatorMsg::TokensUpdate {
            output_tokens: 5_000,
        })
        .await
        .unwrap();
        tx.send(AggregatorMsg::TokensUpdate {
            output_tokens: 3_000,
        })
        .await
        .unwrap();

        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens == 8_000 {
                break;
            }
        }

        tx.send(AggregatorMsg::DailyReset {
            date: "2026-05-20".to_owned(),
        })
        .await
        .unwrap();

        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens_today == 0 {
                assert_eq!(hb.tokens, 8_000, "cumulative must survive reset");
                break;
            }
        }
    }

    #[tokio::test]
    async fn set_initial_tokens_seeds_aggregator_state() {
        // The bug: a fresh daemon's aggregator starts with cumulative=0
        // and today=0, so heartbeats only reflected post-startup deltas.
        // SetInitialTokens seeds the aggregator from the persisted
        // tokens.json so heartbeats carry the full historical totals
        // immediately.
        let (tx, mut hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );

        tx.send(AggregatorMsg::SetInitialTokens {
            cumulative: 184_502,
            today: 12_000,
            date: "2026-05-20".to_owned(),
        })
        .await
        .unwrap();

        // Heartbeat must reflect the seeded values, not zeros.
        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens == 184_502 {
                assert_eq!(hb.tokens_today, 12_000);
                break;
            }
        }

        // A subsequent TokensUpdate adds on top, not replaces.
        tx.send(AggregatorMsg::TokensUpdate { output_tokens: 200 })
            .await
            .unwrap();
        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens == 184_702 {
                assert_eq!(hb.tokens_today, 12_200);
                break;
            }
        }
    }

    #[tokio::test]
    async fn set_initial_tokens_does_not_regress_higher_running_total() {
        // Defensive race-guard: if a TokensUpdate races ahead of
        // SetInitialTokens (e.g. the watcher's seed message gets
        // queued behind a TokensUpdate from the same batch), the
        // visible total must not regress to the persisted value.
        let (tx, mut hb_rx) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );

        // Simulate a TokensUpdate landing first.
        tx.send(AggregatorMsg::TokensUpdate {
            output_tokens: 200_000,
        })
        .await
        .unwrap();
        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens == 200_000 {
                break;
            }
        }

        // Now SetInitialTokens with a smaller persisted cumulative.
        tx.send(AggregatorMsg::SetInitialTokens {
            cumulative: 50_000,
            today: 10_000,
            date: "2026-05-20".to_owned(),
        })
        .await
        .unwrap();

        // Snapshot must keep the higher running total (200_000), not
        // regress to 50_000.
        let (htx, hrx) = oneshot::channel();
        tx.send(AggregatorMsg::GetHeartbeat { respond: htx })
            .await
            .unwrap();
        let hb = hrx.await.unwrap();
        assert_eq!(hb.tokens, 200_000, "cumulative must not regress");
        assert_eq!(hb.tokens_today, 200_000, "today must not regress");
    }

    #[tokio::test]
    async fn run_loop_multiple_subscribers_all_receive_heartbeat() {
        let (tx, hb_rx1) = spawn(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(
                Arc::new(crate::permission::Allowlist::empty()),
            )),
        );
        let mut hb_rx2 = hb_rx1.resubscribe();
        let mut hb_rx1 = hb_rx1;

        let (r_tx, r_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("multi-sess")),
            respond: r_tx,
        })
        .await
        .unwrap();
        r_rx.await.unwrap();

        let hb1 = loop {
            let h = hb_rx1.recv().await.unwrap();
            if h.total == 1 {
                break h;
            }
        };
        let hb2 = loop {
            let h = hb_rx2.recv().await.unwrap();
            if h.total == 1 {
                break h;
            }
        };

        assert_eq!(hb1.total, 1);
        assert_eq!(hb2.total, 1);
    }

    // -----------------------------------------------------------------------
    // Turn-done idle gate
    // -----------------------------------------------------------------------

    /// Helper: spawn an aggregator with a short turn-done grace.  Returns
    /// the tx, heartbeat rx (dropped), and turn-done rx.
    fn spawn_with_turn_done(
        idle_grace: Duration,
    ) -> (
        AggregatorTx,
        broadcast::Receiver<Heartbeat>,
        broadcast::Receiver<TurnDoneEvent>,
    ) {
        let user = Arc::new(ArcSwap::new(
            Arc::new(crate::permission::Allowlist::empty()),
        ));
        let cache = Arc::new(crate::permission::ProjectAllowlistCache::new(user, None));
        spawn_with_paths(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            cache,
            std::path::PathBuf::from("/dev/null"),
            idle_grace,
        )
    }

    #[tokio::test]
    async fn turn_done_fires_after_idle_grace_with_no_followup() {
        // 50 ms grace so the test stays fast.
        let (tx, _hb_rx, mut td_rx) = spawn_with_turn_done(Duration::from_millis(50));

        // Establish a session, then send Stop. With no UserPromptSubmit
        // afterwards, the idle gate fires within ~50ms.
        let (r0, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("sess_td")),
            respond: r0,
        })
        .await
        .unwrap();
        let (r1, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(stop_event("sess_td")),
            respond: r1,
        })
        .await
        .unwrap();

        let evt = tokio::time::timeout(Duration::from_secs(2), td_rx.recv())
            .await
            .expect("turn-done must fire within 2s")
            .expect("broadcast still open");
        assert_eq!(evt.session_id, "sess_td");
        assert_eq!(evt.response_snippet, "done");
    }

    #[tokio::test]
    async fn turn_done_skipped_if_user_prompt_arrives_in_grace() {
        // Generous grace so we have time to inject UserPromptSubmit.
        let (tx, _hb_rx, mut td_rx) = spawn_with_turn_done(Duration::from_millis(200));

        let (r0, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("sess_followup")),
            respond: r0,
        })
        .await
        .unwrap();
        let (r1, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(stop_event("sess_followup")),
            respond: r1,
        })
        .await
        .unwrap();

        // User comes back before grace expires — bumps the serial.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (r2, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(user_prompt_event("sess_followup")),
            respond: r2,
        })
        .await
        .unwrap();

        // Wait past the grace deadline; no event must arrive.
        let result = tokio::time::timeout(Duration::from_millis(400), td_rx.recv()).await;
        assert!(
            result.is_err(),
            "turn-done must NOT fire when user submitted a follow-up; got {:?}",
            result,
        );
    }

    #[tokio::test]
    async fn turn_done_skipped_if_pre_tool_use_arrives_in_grace() {
        // Claude finishes a "turn" mid-task — Stop fires — then immediately
        // resumes with another tool call. The idle gate must NOT misfire as
        // "Claude is done" because Claude is clearly still working.
        let (tx, _hb_rx, mut td_rx) = spawn_with_turn_done(Duration::from_millis(200));

        let (r0, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("sess_midtask")),
            respond: r0,
        })
        .await
        .unwrap();
        let (r1, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(stop_event("sess_midtask")),
            respond: r1,
        })
        .await
        .unwrap();

        // Claude resumes with a tool call before grace expires.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (r2, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(pre_tool_use_event("sess_midtask", "toolu_mid", "Bash")),
            respond: r2,
        })
        .await
        .unwrap();

        let result = tokio::time::timeout(Duration::from_millis(400), td_rx.recv()).await;
        assert!(
            result.is_err(),
            "turn-done must NOT fire when Claude continued with a tool call; got {:?}",
            result,
        );
    }

    #[tokio::test]
    async fn turn_done_skipped_if_post_tool_use_arrives_in_grace() {
        // Same shape as the PreToolUse test but for PostToolUse — covers
        // the case where the Stop arrived between PreToolUse (allowed
        // immediately by the allowlist) and PostToolUse.
        let (tx, _hb_rx, mut td_rx) = spawn_with_turn_done(Duration::from_millis(200));

        let (r0, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("sess_post")),
            respond: r0,
        })
        .await
        .unwrap();
        let (r1, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(stop_event("sess_post")),
            respond: r1,
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let (r2, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(post_tool_use_event("sess_post", "toolu_p", "Bash")),
            respond: r2,
        })
        .await
        .unwrap();

        let result = tokio::time::timeout(Duration::from_millis(400), td_rx.recv()).await;
        assert!(
            result.is_err(),
            "turn-done must NOT fire when a PostToolUse arrives during grace; got {:?}",
            result,
        );
    }

    #[tokio::test]
    async fn turn_done_disabled_when_grace_is_zero() {
        // idle_grace = ZERO disables the feature: no spawn, no broadcast.
        let (tx, _hb_rx, mut td_rx) = spawn_with_turn_done(Duration::ZERO);

        let (r0, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("sess_off")),
            respond: r0,
        })
        .await
        .unwrap();
        let (r1, _) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(stop_event("sess_off")),
            respond: r1,
        })
        .await
        .unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), td_rx.recv()).await;
        assert!(
            result.is_err(),
            "turn-done must NOT fire when idle_grace is zero",
        );
    }
}
