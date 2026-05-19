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
use crate::permission::{AllowOrDeny, Allowlist};

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
    /// The responder must be fired exactly once:
    /// - For most events: immediately, with [`HookResponse::Passthrough`].
    /// - For `PreToolUse`: the aggregator stores the decision-tx side of a
    ///   oneshot in `pending_approvals` and responds with
    ///   [`HookResponse::AwaitDecision`], which carries the receive side.
    HookEvent {
        event: Box<HookEvent>,
        respond: oneshot::Sender<HookResponse>,
    },

    /// An emit module (swaync, BLE, ctrl-socket) has resolved a pending
    /// permission prompt.  The aggregator pops the waiting oneshot from
    /// `pending_approvals` and fires it.
    PermissionDecision {
        tool_use_id: ToolUseId,
        decision: WireDecision,
    },

    /// Return a snapshot of the current heartbeat state.  Used by emit modules
    /// that need the current state on demand (e.g. ctrl-socket initial burst).
    GetHeartbeat { respond: oneshot::Sender<Heartbeat> },

    /// Token counts updated by the JSONL tail (task 27993d8d).
    TokensUpdate { output_tokens: u64 },

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

    /// The 30 s approval timeout in `ingest::hooks` fired before any emit
    /// module (notify / BLE / ctrl) sent a decision.  The hook has already
    /// written `Ask` to its stdout so Claude Code's own TUI will handle the
    /// decision; the aggregator just needs to clear its pending state so the
    /// next heartbeat shows `prompt: None` / `waiting: 0`.
    ///
    /// Dropping the sender from `pending_approvals` is safe: if a late decision
    /// somehow arrives (race between timeout + emit module), the existing
    /// `handle_permission_decision` path logs a warn and discards it.
    ApprovalTimedOut { tool_use_id: ToolUseId },

    /// User clicked "Always" on a swaync notification.  The aggregator
    /// derives the most-conservative allowlist pattern for the pending event,
    /// writes it to `settings.json`, and approves the current call.
    ///
    /// For tools where a specific pattern cannot be auto-derived (bare-tool
    /// case), the call is denied with a helpful reason rather than risking
    /// a too-broad pattern being silently written.
    AllowlistAlways { tool_use_id: ToolUseId },
}

// ---------------------------------------------------------------------------
// HookResponse
// ---------------------------------------------------------------------------

/// What the hook ingest handler writes back to the hook binary's stdout.
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
    /// Used when a confident deny is reached — either by a deny-list rule
    /// (Phase 2+) or when the user explicitly clicks Deny via an emit module.
    /// The reason string tells Claude not to retry with alternative approaches.
    HardDeny { reason: String },

    /// The aggregator stored the approval oneshot; the ingest handler should
    /// await `rx` with `approval_timeout`, then write a `PermissionDecision`
    /// or (on timeout) a response determined by `fallback`.
    ///
    /// This variant is consumed entirely within `ingest::hooks` and is never
    /// serialised to the wire.
    AwaitDecision {
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
    pub pending_tool_use_id: Option<ToolUseId>,
    /// Tool name for the pending approval — displayed on BLE device screen.
    pub pending_tool_name: Option<String>,
    /// Short hint for the pending approval — displayed on BLE device screen.
    pub pending_tool_hint: Option<String>,
    /// Phase 2+: the allowlist pattern that produced an `AskAnnotated` decision.
    /// Stored for future use in annotation rendering (Phase 4).
    pub pending_matched_pattern: Option<String>,
    /// Phase 2+: which side of the allowlist the matched pattern came from.
    pub pending_match_source: Option<AllowOrDeny>,
    /// Agent type from the hook event (`agent_type` field); `None` for
    /// top-level sessions.  Surfaced in the heartbeat `PromptInfo` to
    /// disambiguate sub-agent prompts from main-session prompts.
    pub pending_agent_type: Option<String>,

    /// Full event stashed for the "Always" flow: if the user clicks Always,
    /// the aggregator needs `tool_input` (and `agent_type`) to derive the
    /// most-conservative allowlist pattern.  Boxed to keep variant sizes even.
    pub pending_event: Option<Box<ccbridge_proto::hook::PreToolUseEvent>>,
}

impl Session {
    fn new(id: impl Into<String>, cwd: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            cwd: cwd.into(),
            running: false,
            waiting: false,
            pending_tool_use_id: None,
            pending_tool_name: None,
            pending_tool_hint: None,
            pending_matched_pattern: None,
            pending_match_source: None,
            pending_agent_type: None,
            pending_event: None,
        }
    }

    /// Clear all pending-approval state.
    fn clear_pending(&mut self) {
        self.waiting = false;
        self.pending_tool_use_id = None;
        self.pending_tool_name = None;
        self.pending_tool_hint = None;
        self.pending_matched_pattern = None;
        self.pending_match_source = None;
        self.pending_agent_type = None;
        self.pending_event = None;
    }
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

    /// Pending `PreToolUse` approvals.  The oneshot fires a [`WireDecision`]
    /// back to the waiting ingest handler.  Keyed by `tool_use_id` alone —
    /// Claude Code tool-use IDs are globally unique within a session.
    pending_approvals: HashMap<ToolUseId, oneshot::Sender<WireDecision>>,

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

    /// Parsed `permissions.allow` / `.deny` from `~/.claude/settings.json`.
    ///
    /// Wrapped in `ArcSwap` so the settings watcher can atomically swap in a
    /// freshly-parsed allowlist without holding any lock on the hot path.
    /// The outer `Arc` lets both the aggregator and the watcher task hold a
    /// handle to the same `ArcSwap` without lifetime complications.
    allowlist: Arc<ArcSwap<Allowlist>>,

    /// Path to `~/.claude/settings.json` (or `$CLAUDE_CONFIG_DIR/settings.json`).
    /// Used by `AllowlistAlways` to atomically write a new allow pattern.
    settings_path: std::path::PathBuf,

    /// Path to the allowlist audit log.
    /// Used by `AllowlistAlways` to append an audit entry.
    audit_log_path: std::path::PathBuf,
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
    /// receiver that emit modules should subscribe to.
    pub fn new(
        approval_timeout: Duration,
        fallback: Fallback,
        allowlist: Arc<ArcSwap<Allowlist>>,
        settings_path: std::path::PathBuf,
        audit_log_path: std::path::PathBuf,
    ) -> (Self, broadcast::Receiver<Heartbeat>) {
        let (hb_tx, hb_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let agg = Self {
            sessions: HashMap::new(),
            pending_approvals: HashMap::new(),
            tokens: TokenState::default(),
            entries: VecDeque::with_capacity(ENTRIES_CAP),
            hb_tx,
            approval_timeout,
            fallback,
            allowlist,
            settings_path,
            audit_log_path,
        };
        (agg, hb_rx)
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

        // Build `prompt` from the first waiting session (at most one in practice).
        let prompt = self.sessions.values().find(|s| s.waiting).map(|s| PromptInfo {
            id: s.pending_tool_use_id.clone().unwrap_or_default(),
            tool: s.pending_tool_name.clone().unwrap_or_default(),
            hint: s.pending_tool_hint.clone().unwrap_or_default(),
            matched_pattern: s.pending_matched_pattern.clone(),
            matched_source: s.pending_match_source.map(MatchSource::from),
            // Session/agent context — always populated when a prompt is waiting.
            session_id: Some(s.id.clone()),
            cwd: Some(s.cwd.clone()),
            agent_type: s.pending_agent_type.clone(),
        });

        let msg = if waiting > 0 {
            // Include tool name if we have it.
            let tool = self
                .sessions
                .values()
                .find(|s| s.waiting)
                .and_then(|s| s.pending_tool_name.as_deref())
                .unwrap_or("tool");
            format!("approve: {}", tool)
        } else if running > 0 {
            format!("running ({})", running)
        } else if total > 0 {
            format!("idle ({})", total)
        } else {
            "no sessions".to_owned()
        };

        Heartbeat {
            total,
            running,
            waiting,
            msg,
            entries: self.entries.iter().cloned().collect(),
            tokens: self.tokens.cumulative,
            tokens_today: self.tokens.today,
            prompt,
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

    fn handle_hook_event(
        &mut self,
        event: HookEvent,
        respond: oneshot::Sender<HookResponse>,
    ) {
        match event {
            HookEvent::SessionStart(e) => {
                info!(session_id = %e.base.session_id, cwd = %e.base.cwd, "session started");
                self.sessions
                    .entry(e.base.session_id.clone())
                    .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
                self.push_entry(format!("session: {}", e.base.cwd));
                self.broadcast_heartbeat();
                let _ = respond.send(HookResponse::Passthrough);
            }

            HookEvent::SessionEnd(e) => {
                info!(session_id = %e.base.session_id, "session ended");
                self.sessions.remove(&e.base.session_id);
                self.push_entry("session ended".to_owned());
                self.broadcast_heartbeat();
                let _ = respond.send(HookResponse::Passthrough);
            }

            HookEvent::PreToolUse(e) => {
                use crate::permission::{self, Decision};

                // Load the current allowlist snapshot (lock-free).
                let al = self.allowlist.load();
                match permission::evaluate(&e, &al) {
                    Decision::Allow { reason } => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            %reason,
                            "PreToolUse allowed without prompt",
                        );
                        let _ = respond.send(HookResponse::PermissionDecision(WireDecision::Once));
                    }
                    Decision::Deny { reason } => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            %reason,
                            "PreToolUse denied without prompt",
                        );
                        let _ = respond.send(HookResponse::HardDeny { reason });
                    }
                    Decision::AskAnnotated { matched_pattern, source } => {
                        debug!(
                            session_id = %e.base.session_id,
                            tool = %e.tool_name,
                            pattern = %matched_pattern,
                            ?source,
                            "PreToolUse ambiguous match — intercepting with annotation",
                        );
                        self.start_intercept(e, respond, Some((matched_pattern, source)));
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
                }
                self.push_entry(format!(
                    "{}: {}",
                    e.tool_name,
                    format_tool_hint(&e.tool_input),
                ));
                self.broadcast_heartbeat();
                let _ = respond.send(HookResponse::Passthrough);
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
                let _ = respond.send(HookResponse::Passthrough);
            }

            HookEvent::Notification(e) => {
                debug!(
                    session_id = %e.base.session_id,
                    notification_type = %e.notification_type,
                    "Notification",
                );
                self.push_entry(format!("notif: {}", e.message));
                self.broadcast_heartbeat();
                let _ = respond.send(HookResponse::Passthrough);
            }

            HookEvent::UserPromptSubmit(e) => {
                debug!(session_id = %e.base.session_id, "UserPromptSubmit");
                let session = self
                    .sessions
                    .entry(e.base.session_id.clone())
                    .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
                session.running = true;
                self.broadcast_heartbeat();
                let _ = respond.send(HookResponse::Passthrough);
            }

            HookEvent::Unknown => {
                // Forward-compat: log and skip, never crash.
                debug!("Unknown hook event — ignoring");
                let _ = respond.send(HookResponse::Passthrough);
            }
        }
    }

    /// Register a `PreToolUse` event into the hold-and-wait approval flow.
    ///
    /// Creates a oneshot channel, stashes the sender in `pending_approvals`,
    /// marks the session as `waiting`, and returns [`HookResponse::AwaitDecision`]
    /// to the ingest handler, which drives the timeout loop.
    ///
    /// `annotation` is `Some((pattern, source))` when the decision came from
    /// [`crate::permission::Decision::AskAnnotated`] — i.e., the allowlist had
    /// a pattern that matched ambiguously.  The fields are stored on the session
    /// for future annotation rendering (Phase 4).  Pass `None` for plain intercept.
    fn start_intercept(
        &mut self,
        e: ccbridge_proto::hook::PreToolUseEvent,
        respond: oneshot::Sender<HookResponse>,
        annotation: Option<(String, AllowOrDeny)>,
    ) {
        let hint = format_tool_hint(&e.tool_input);
        debug!(
            session_id = %e.base.session_id,
            tool = %e.tool_name,
            tool_use_id = %e.tool_use_id,
            hint = %hint,
            "PreToolUse — holding for approval",
        );

        let (decision_tx, decision_rx) = oneshot::channel::<WireDecision>();
        self.pending_approvals.insert(e.tool_use_id.clone(), decision_tx);

        let session = self
            .sessions
            .entry(e.base.session_id.clone())
            .or_insert_with(|| Session::new(&e.base.session_id, &e.base.cwd));
        session.waiting = true;
        session.pending_tool_use_id = Some(e.tool_use_id.clone());
        session.pending_tool_name = Some(e.tool_name.clone());
        session.pending_tool_hint = Some(hint);
        session.pending_agent_type = e.agent_type.clone();
        session.pending_event = Some(Box::new(e.clone()));
        if let Some((pattern, source)) = annotation {
            session.pending_matched_pattern = Some(pattern);
            session.pending_match_source = Some(source);
        }

        self.broadcast_heartbeat();

        let _ = respond.send(HookResponse::AwaitDecision {
            rx: decision_rx,
            tool_use_id: e.tool_use_id,
            session_id: e.base.session_id,
            approval_timeout: self.approval_timeout,
            fallback: self.fallback,
        });
    }

    fn handle_permission_decision(&mut self, tool_use_id: ToolUseId, decision: WireDecision) {
        match self.pending_approvals.remove(&tool_use_id) {
            Some(tx) => {
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
            }
            None => {
                warn!(tool_use_id = %tool_use_id, "no pending approval for this tool_use_id");
            }
        }
    }

    fn handle_allowlist_always(&mut self, tool_use_id: ToolUseId) {
        use crate::permission::additions::{
            derive_pattern, write_allow_pattern, AdditionMetadata, DerivedPattern,
        };

        // Look up the stashed PreToolUseEvent for this approval.
        let event = self
            .sessions
            .values()
            .find(|s| s.pending_tool_use_id.as_deref() == Some(&tool_use_id))
            .and_then(|s| s.pending_event.as_deref().cloned());

        let Some(event) = event else {
            warn!(
                tool_use_id = %tool_use_id,
                "AllowlistAlways: no pending event found; ignoring",
            );
            return;
        };

        match derive_pattern(&event) {
            DerivedPattern::Specific(pattern) => {
                let metadata = AdditionMetadata {
                    tool_use_id: tool_use_id.clone(),
                    session_id: event.base.session_id.clone(),
                    agent_type: event.agent_type.clone(),
                };
                match write_allow_pattern(
                    &self.settings_path,
                    &pattern,
                    &self.audit_log_path,
                    metadata,
                ) {
                    Ok(()) => info!(%pattern, "AllowlistAlways: wrote allow pattern"),
                    Err(e) => warn!("AllowlistAlways: failed to write pattern: {e:#}"),
                }
                // Approve this specific call regardless of write success.
                // Settings watcher will reload for future calls.
                self.handle_permission_decision(tool_use_id, WireDecision::Once);
            }
            DerivedPattern::BareToolNeedsConfirmation { tool } => {
                // A bare-tool pattern like `Bash` would allow ALL future Bash calls —
                // almost certainly not what the user wants.  Deny this call with a
                // clear reason rather than silently writing an overly broad pattern.
                warn!(
                    %tool,
                    tool_use_id = %tool_use_id,
                    "AllowlistAlways: no specific pattern derivable; denying",
                );
                // The deny reason is surfaced via HardDeny in handle_permission_decision
                // path.  We re-use PermissionDecision(Deny) here; the swaync emitter
                // will show the deny.  A future enhancement could surface a more
                // specific error notification.
                self.handle_permission_decision(tool_use_id, WireDecision::Deny);
            }
        }
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
                        Some(AggregatorMsg::PermissionDecision { tool_use_id, decision }) => {
                            self.handle_permission_decision(tool_use_id, decision);
                        }
                        Some(AggregatorMsg::GetHeartbeat { respond }) => {
                            let _ = respond.send(self.snapshot());
                        }
                        Some(AggregatorMsg::TokensUpdate { output_tokens }) => {
                            self.tokens.cumulative += output_tokens;
                            self.tokens.today += output_tokens;
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
                            // Drop the sender — any late decision arriving after
                            // this will hit the "no pending approval" warn in
                            // handle_permission_decision and be discarded.
                            self.pending_approvals.remove(&tool_use_id);
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

/// Spawn the Aggregator with explicit settings and audit-log paths.
///
/// Used by the real daemon so `AllowlistAlways` writes to the right places.
pub fn spawn_with_paths(
    approval_timeout: Duration,
    fallback: Fallback,
    allowlist: Arc<ArcSwap<Allowlist>>,
    settings_path: std::path::PathBuf,
    audit_log_path: std::path::PathBuf,
) -> (AggregatorTx, broadcast::Receiver<Heartbeat>) {
    let (agg, hb_rx) =
        Aggregator::new(approval_timeout, fallback, allowlist, settings_path, audit_log_path);
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(agg.run(rx));
    (tx, hb_rx)
}

/// Spawn the Aggregator with no-op paths for settings/audit.
///
/// Used by unit and integration tests where `AllowlistAlways` is either not
/// exercised or given explicit temp-dir paths in the test body.
pub fn spawn(
    approval_timeout: Duration,
    fallback: Fallback,
    allowlist: Arc<ArcSwap<Allowlist>>,
) -> (AggregatorTx, broadcast::Receiver<Heartbeat>) {
    spawn_with_paths(
        approval_timeout,
        fallback,
        allowlist,
        std::path::PathBuf::from("/dev/null"),
        std::path::PathBuf::from("/dev/null"),
    )
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
        HookBase, PostToolUseEvent, PreToolUseEvent, SessionStartEvent, SessionSource,
        StopEvent, UserPromptSubmitEvent, PermissionMode,
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

    #[allow(dead_code)] // available for future tests that exercise PostToolUse handling
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
        let (agg, _rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))),
            std::path::PathBuf::from("/dev/null"),
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
        assert!(hb.prompt.is_none());
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
        agg.handle_hook_event(
            pre_tool_use_event("sess", "toolu_abc", "Bash"),
            respond_tx,
        );

        // Aggregator should have stored the approval.
        assert!(agg.pending_approvals.contains_key("toolu_abc"));

        // The respond channel should have an AwaitDecision.
        let response = respond_rx.try_recv().expect("respond should be fired");
        assert!(matches!(response, HookResponse::AwaitDecision { .. }));

        // Heartbeat should reflect waiting=1 with populated prompt.
        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 1);
        let prompt = hb.prompt.expect("prompt must be set");
        assert_eq!(prompt.id, "toolu_abc");
        assert_eq!(prompt.tool, "Bash");
        assert_eq!(prompt.hint, "echo hello");
    }

    #[test]
    fn ask_annotated_decision_surfaces_in_snapshot() {
        // Build an aggregator with an allowlist that produces AskAnnotated for Bash.
        // A Bash call with no command field → Ambiguous allow → AskAnnotated.
        let al = crate::permission::Allowlist {
            allow: vec![crate::permission::Pattern::parse("Bash(git status:*)")],
            deny: vec![],
        };
        let (mut agg, _rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(Arc::new(al))),
            std::path::PathBuf::from("/dev/null"),
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
        let prompt = hb.prompt.expect("prompt must be present for waiting session");
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
        // Bare PreToolUse (default permission mode): snapshot must carry
        // session_id and cwd in PromptInfo.
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess-cwd"), tx0);

        let (respond_tx, _) = oneshot::channel();
        agg.handle_hook_event(
            pre_tool_use_event("sess-cwd", "toolu_cwd_01", "Bash"),
            respond_tx,
        );

        let hb = agg.snapshot();
        let prompt = hb.prompt.expect("prompt must be set");
        // session_id is the UUID-like session identifier used internally.
        assert_eq!(
            prompt.session_id.as_deref(),
            Some("sess-cwd"),
            "session_id must be populated in PromptInfo"
        );
        // cwd comes from HookBase; the test helper uses "/tmp".
        assert_eq!(
            prompt.cwd.as_deref(),
            Some("/tmp"),
            "cwd must be populated in PromptInfo"
        );
        // No agent_type in the test helper.
        assert!(
            prompt.agent_type.is_none(),
            "agent_type must be None for a top-level session"
        );
    }

    #[test]
    fn start_intercept_captures_agent_type() {
        // When a PreToolUse event carries agent_type, it must appear in snapshot.
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess-agent"), tx0);

        // Fire PreToolUse with agent_type set.
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
        let prompt = hb.prompt.expect("prompt must be set");
        assert_eq!(
            prompt.agent_type.as_deref(),
            Some("general-purpose"),
            "agent_type must be captured from the PreToolUse event"
        );
        assert_eq!(
            prompt.session_id.as_deref(),
            Some("sess-agent"),
        );
    }

    // -----------------------------------------------------------------------
    // AllowlistAlways tests (use spawn_with_paths for real tempdir paths)
    // -----------------------------------------------------------------------

    fn new_agg_with_paths(
        settings: &std::path::Path,
        audit: &std::path::Path,
    ) -> Aggregator {
        let (agg, _rx) = Aggregator::new(
            DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))),
            settings.to_path_buf(),
            audit.to_path_buf(),
        );
        agg
    }

    #[test]
    fn allowlist_always_writes_pattern() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let settings = dir.path().join("settings.json");
        let audit = dir.path().join("audit.log");
        std::fs::write(&settings, r#"{}"#).unwrap();

        let mut agg = new_agg_with_paths(&settings, &audit);

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_always"), tx0);

        // Fire a Bash PreToolUse — should derive "Bash(echo always_test)".
        let event = HookEvent::PreToolUse(PreToolUseEvent {
            base: HookBase {
                session_id: "sess_always".to_owned(),
                transcript_path: "/tmp/always.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
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

        // Extract decision receiver from AwaitDecision.
        let mut decision_rx = match respond_rx.try_recv().unwrap() {
            HookResponse::AwaitDecision { rx, .. } => rx,
            _ => panic!("expected AwaitDecision"),
        };

        // Trigger AllowlistAlways.
        agg.handle_allowlist_always("toolu_always_01".to_owned());

        // Decision should be Once (approved).
        let decision = decision_rx.try_recv().expect("AllowlistAlways must fire Once");
        assert_eq!(decision, WireDecision::Once);

        // Pattern should be in settings.json.
        let loaded = crate::setup::load_settings(&settings).unwrap();
        let allow = loaded["permissions"]["allow"].as_array().unwrap();
        assert!(
            allow.iter().any(|v| v.as_str() == Some("Bash(echo always_test)")),
            "pattern must be in settings.json allow list"
        );
    }

    #[test]
    fn allowlist_always_bare_tool_denies_with_reason() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let settings = dir.path().join("settings.json");
        let audit = dir.path().join("audit.log");
        std::fs::write(&settings, r#"{}"#).unwrap();

        let mut agg = new_agg_with_paths(&settings, &audit);

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_bare"), tx0);

        // WebSearch has no derivable specific pattern → BareToolNeedsConfirmation.
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
            HookResponse::AwaitDecision { rx, .. } => rx,
            _ => panic!("expected AwaitDecision"),
        };

        // Trigger AllowlistAlways for a bare tool.
        agg.handle_allowlist_always("toolu_bare_01".to_owned());

        // Should be denied (bare-tool guardrail).
        let decision = decision_rx.try_recv().expect("AllowlistAlways must fire");
        assert_eq!(decision, WireDecision::Deny, "bare-tool AllowlistAlways must deny");

        // No pattern should be written.
        let loaded = crate::setup::load_settings(&settings).unwrap();
        let allow = loaded.get("permissions").and_then(|p| p.get("allow"));
        assert!(
            allow.map(|a| a.as_array().map(|arr| arr.is_empty()).unwrap_or(true))
                .unwrap_or(true),
            "bare-tool AllowlistAlways must not write any pattern"
        );
    }

    #[test]
    fn allowlist_always_unknown_tool_use_id_no_panic() {
        let mut agg = new_agg();
        // Should log a warning and return without panic.
        agg.handle_allowlist_always("toolu_nonexistent".to_owned());
    }

    #[test]
    fn permissive_mode_pre_tool_use_auto_allows_without_prompt() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess_bypass"), tx0);

        // Fire PreToolUse with bypassPermissions mode.
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

        // Must respond immediately with PermissionDecision::Once — no oneshot wait.
        let response = respond_rx.try_recv().expect("short-circuit must fire immediately");
        assert!(
            matches!(response, HookResponse::PermissionDecision(WireDecision::Once)),
            "permissive mode must auto-allow: got {response:?}",
        );

        // Must NOT enter pending_approvals or set waiting=1.
        assert!(
            !agg.pending_approvals.contains_key("toolu_bypass_01"),
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

        // Extract the decision rx from the AwaitDecision response.
        let response = respond_rx.try_recv().unwrap();
        let mut decision_rx = match response {
            HookResponse::AwaitDecision { rx, .. } => rx,
            _ => panic!("expected AwaitDecision"),
        };

        // Send the decision.
        agg.handle_permission_decision("toolu_xyz".to_owned(), WireDecision::Once);

        // The oneshot should have fired.
        let decision = decision_rx.try_recv().expect("decision should have been fired");
        assert_eq!(decision, WireDecision::Once);

        // Session should no longer be waiting.
        let hb = agg.snapshot();
        assert_eq!(hb.waiting, 0);
        assert!(hb.prompt.is_none());

        // Pending approvals map should be empty.
        assert!(agg.pending_approvals.is_empty());
    }

    #[test]
    fn approval_timed_out_clears_pending() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        // Register a pending approval.
        let (respond_tx, mut respond_rx) = oneshot::channel();
        agg.handle_hook_event(pre_tool_use_event("sess", "toolu_timeout", "Bash"), respond_tx);

        // Consume the AwaitDecision response (we won't use it — simulating timeout).
        let _ = respond_rx.try_recv().unwrap();
        assert_eq!(agg.snapshot().waiting, 1);
        assert!(!agg.pending_approvals.is_empty());

        // Simulate what the ingest handler does when the timeout fires.
        agg.pending_approvals.remove("toolu_timeout");
        for session in agg.sessions.values_mut() {
            if session.pending_tool_use_id.as_deref() == Some("toolu_timeout") {
                session.clear_pending();
            }
        }
        agg.broadcast_heartbeat();

        // State must be clean — no waiting, no prompt.
        assert!(agg.pending_approvals.is_empty(), "pending approvals must be cleared");
        assert_eq!(agg.snapshot().waiting, 0, "waiting must be 0 after timeout");
        assert!(agg.snapshot().prompt.is_none(), "prompt must be None after timeout");
    }

    #[test]
    fn permission_decision_unknown_id_does_not_panic() {
        let mut agg = new_agg();
        // No prior PreToolUse — should just log a warning.
        agg.handle_permission_decision("toolu_nonexistent".to_owned(), WireDecision::Deny);
        // If we get here without panic, the test passes.
    }

    #[test]
    fn stop_clears_running_and_waiting() {
        let mut agg = new_agg();

        let (tx0, _) = oneshot::channel();
        agg.handle_hook_event(session_start_event("sess"), tx0);

        // Simulate running.
        let (tx1, _) = oneshot::channel();
        agg.handle_hook_event(user_prompt_event("sess"), tx1);
        assert_eq!(agg.snapshot().running, 1);

        // Simulate a stale waiting state (shouldn't happen in production, but
        // we must handle it defensively).
        if let Some(s) = agg.sessions.get_mut("sess") {
            s.waiting = true;
            s.pending_tool_use_id = Some("toolu_stale".to_owned());
            s.pending_tool_name = Some("Bash".to_owned());
            s.pending_tool_hint = Some("rm -rf".to_owned());
        }
        assert_eq!(agg.snapshot().waiting, 1);

        // Fire Stop — should clear both.
        let (tx2, _) = oneshot::channel();
        agg.handle_hook_event(stop_event("sess"), tx2);

        let hb = agg.snapshot();
        assert_eq!(hb.running, 0);
        assert_eq!(hb.waiting, 0);
        assert!(hb.prompt.is_none());
    }

    #[test]
    fn daily_reset_zeroes_today_keeps_cumulative_updates_date() {
        // Test DailyReset handling directly on the Aggregator struct
        // (no run loop needed — verifies the state mutation in isolation).
        let mut agg = new_agg();
        agg.tokens.cumulative = 50_000;
        agg.tokens.today = 12_000;
        agg.tokens.date = "2026-05-19".to_owned();

        // Simulate the DailyReset arm of the run loop.
        agg.tokens.today = 0;
        agg.tokens.date = "2026-05-20".to_owned();

        assert_eq!(agg.tokens.cumulative, 50_000, "cumulative must survive reset");
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
        // Newest entry (entry-11) should be at the front.
        assert_eq!(agg.entries[0], "entry-11");
        // Oldest surviving entry should be entry-4 (entries 0-3 were evicted).
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
        let (tx, hb_rx) = spawn(DEFAULT_APPROVAL_TIMEOUT, crate::config::Fallback::default(), Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))));
        drop(hb_rx); // not needed here

        let (respond_tx, respond_rx) = oneshot::channel();
        tx.send(AggregatorMsg::GetHeartbeat { respond: respond_tx })
            .await
            .unwrap();

        let hb = respond_rx.await.unwrap();
        assert_eq!(hb.total, 0);
    }

    #[tokio::test]
    async fn run_loop_session_start_increments_total() {
        let (tx, hb_rx) = spawn(DEFAULT_APPROVAL_TIMEOUT, crate::config::Fallback::default(), Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))));
        drop(hb_rx);

        let (respond_tx, respond_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("run-sess")),
            respond: respond_tx,
        })
        .await
        .unwrap();

        let resp = respond_rx.await.unwrap();
        assert!(matches!(resp, HookResponse::Passthrough));

        let (hb_tx, hb_rx2) = oneshot::channel();
        tx.send(AggregatorMsg::GetHeartbeat { respond: hb_tx }).await.unwrap();
        let hb = hb_rx2.await.unwrap();
        assert_eq!(hb.total, 1);
    }

    #[tokio::test]
    async fn run_loop_pre_tool_use_then_permission_decision() {
        let (tx, mut hb_rx) = spawn(DEFAULT_APPROVAL_TIMEOUT, crate::config::Fallback::default(), Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))));

        // Start a session.
        let (r1_tx, r1_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("psess")),
            respond: r1_tx,
        })
        .await
        .unwrap();
        r1_rx.await.unwrap();

        // Drain keepalive heartbeats.
        while hb_rx.try_recv().is_ok() {}

        // Fire PreToolUse.
        let (r2_tx, r2_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(pre_tool_use_event("psess", "toolu_run1", "Bash")),
            respond: r2_tx,
        })
        .await
        .unwrap();

        // The aggregator should have sent AwaitDecision with the approval rx.
        let resp = r2_rx.await.unwrap();
        let decision_rx = match resp {
            HookResponse::AwaitDecision { rx, .. } => rx,
            _ => panic!("expected AwaitDecision"),
        };

        // Heartbeat should now show waiting=1.
        let hb = hb_rx.recv().await.unwrap();
        assert_eq!(hb.waiting, 1);
        assert!(hb.prompt.is_some());

        // Resolve the approval.
        tx.send(AggregatorMsg::PermissionDecision {
            tool_use_id: "toolu_run1".to_owned(),
            decision: WireDecision::Once,
        })
        .await
        .unwrap();

        // The decision_rx should fire.
        let decision = decision_rx.await.unwrap();
        assert_eq!(decision, WireDecision::Once);

        // Next heartbeat should show waiting=0.
        let hb2 = hb_rx.recv().await.unwrap();
        assert_eq!(hb2.waiting, 0);
        assert!(hb2.prompt.is_none());
    }

    #[tokio::test]
    async fn run_loop_daily_reset_zeroes_today_keeps_cumulative() {
        let (tx, mut hb_rx) = spawn(DEFAULT_APPROVAL_TIMEOUT, crate::config::Fallback::default(), Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))));

        tx.send(AggregatorMsg::TokensUpdate { output_tokens: 5_000 }).await.unwrap();
        tx.send(AggregatorMsg::TokensUpdate { output_tokens: 3_000 }).await.unwrap();

        // Drain until we see cumulative=8000.
        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens == 8_000 { break; }
        }

        // Reset.
        tx.send(AggregatorMsg::DailyReset { date: "2026-05-20".to_owned() }).await.unwrap();

        loop {
            let hb = hb_rx.recv().await.unwrap();
            if hb.tokens_today == 0 {
                assert_eq!(hb.tokens, 8_000, "cumulative must survive reset");
                break;
            }
        }
    }

    #[tokio::test]
    async fn run_loop_multiple_subscribers_all_receive_heartbeat() {
        let (tx, hb_rx1) = spawn(DEFAULT_APPROVAL_TIMEOUT, crate::config::Fallback::default(), Arc::new(ArcSwap::new(Arc::new(crate::permission::Allowlist::empty()))));
        // Subscribe a second receiver before sending any messages.
        let mut hb_rx2 = hb_rx1.resubscribe();
        let mut hb_rx1 = hb_rx1;

        // Trigger a state change.
        let (r_tx, r_rx) = oneshot::channel();
        tx.send(AggregatorMsg::HookEvent {
            event: Box::new(session_start_event("multi-sess")),
            respond: r_tx,
        })
        .await
        .unwrap();
        r_rx.await.unwrap();

        // Both subscribers should see a heartbeat with total=1.
        let hb1 = loop {
            let h = hb_rx1.recv().await.unwrap();
            if h.total == 1 { break h; }
        };
        let hb2 = loop {
            let h = hb_rx2.recv().await.unwrap();
            if h.total == 1 { break h; }
        };

        assert_eq!(hb1.total, 1);
        assert_eq!(hb2.total, 1);
    }
}
