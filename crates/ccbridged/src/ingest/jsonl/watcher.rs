// SPDX-License-Identifier: MIT
//! Filesystem watcher: tail JSONL files in `~/.claude/projects/` and
//! forward token deltas + entry text to the aggregator.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::warn;

use super::dates::current_local_date_string;
use super::offsets::FileOffsets;
use super::parse::parse_jsonl_line;
use super::tokens::PersistedTokens;
use crate::state::AggregatorTx;

/// Spawn the JSONL watcher task.
///
/// Watches `projects_dir` recursively with [`notify::RecommendedWatcher`].
/// For each new assistant line in a `*.jsonl` file, sends:
/// - [`crate::state::AggregatorMsg::TokensUpdate`] with `output_tokens`
/// - [`crate::state::AggregatorMsg::AddEntry`] with the entry text (if any)
///
/// Token counts are persisted to `state_path` (debounced, every
/// `PERSIST_DEBOUNCE`).  If `state_path` cannot be determined or written,
/// the watcher logs and continues — token tracking in memory is unaffected.
///
/// On any watcher or parse error: log via `warn!`, never crash.
pub fn spawn_watcher(
    projects_dir: PathBuf,
    state_path: PathBuf,
    agg_tx: AggregatorTx,
    initial_tokens: PersistedTokens,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_watcher(projects_dir, state_path, agg_tx, initial_tokens).await {
            warn!("JSONL watcher exited with error: {e:#}");
        }
    })
}

// How long to wait between persist flushes when tokens have changed.
const PERSIST_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(5);

async fn run_watcher(
    projects_dir: PathBuf,
    state_path: PathBuf,
    agg_tx: AggregatorTx,
    initial_tokens: PersistedTokens,
) -> Result<()> {
    use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
    use std::sync::mpsc as std_mpsc;

    // Initialise in-memory token state from the persisted file.
    let mut cumulative = initial_tokens.cumulative;
    let mut today = initial_tokens.today;
    // Track the date the current `today` counter belongs to.  We use the
    // persisted date if available so we don't recompute it on every persist;
    // self-heal logic on each TokensUpdate catches the suspend-across-midnight
    // case when the in-process timer never fires.
    let mut current_date = if initial_tokens.date.is_empty() {
        current_local_date_string()
    } else {
        initial_tokens.date.clone()
    };

    // Seed the aggregator with the persisted totals so its first
    // heartbeat carries the full historical counts, not just the
    // deltas observed since this aggregator booted.  Without this,
    // a fresh daemon (with `tokens.cumulative=0`) would drift behind
    // our in-memory counters until the first persist debounce.
    if (cumulative > 0 || today > 0 || !current_date.is_empty())
        && agg_tx
            .send(crate::state::AggregatorMsg::SetInitialTokens {
                cumulative,
                today,
                date: current_date.clone(),
            })
            .await
            .is_err()
    {
        warn!("JSONL: aggregator gone before seeding tokens");
    }

    // Snapshot existing file offsets so we only process *new* lines.
    let mut offsets = FileOffsets::new();
    offsets.snapshot_existing(&projects_dir);

    // notify 6 only delivers events on a synchronous std::mpsc.  Bridge
    // it to a tokio mpsc via a small forwarder thread so the main loop
    // can `await` events instead of polling with a 50ms sleep — that
    // wakes the runtime ~20×/s on every machine, idle or not.
    let (sync_tx, sync_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher =
        RecommendedWatcher::new(sync_tx, Config::default()).context("create filesystem watcher")?;
    watcher
        .watch(&projects_dir, RecursiveMode::Recursive)
        .context("watch projects dir")?;

    let (async_tx, mut async_rx) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while let Ok(event) = sync_rx.recv() {
            if async_tx.send(event).is_err() {
                break;
            }
        }
    });

    tracing::info!(dir = %projects_dir.display(), "JSONL watcher started");

    // Debounce timer: tokens changed since last persist?
    let mut tokens_dirty = false;

    // Persist debounce ticker: fires every PERSIST_DEBOUNCE.  Combined
    // with the event channel via select! so persist happens promptly
    // after a quiet period, not "up to PERSIST_DEBOUNCE + 50ms after".
    let mut persist_tick = tokio::time::interval(PERSIST_DEBOUNCE);
    persist_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    persist_tick.tick().await; // consume the immediate tick

    // Hold the watcher alive for the duration of this loop — dropping
    // it would close the sync channel and wedge the forwarder thread.
    let _watcher_keep = watcher;

    loop {
        tokio::select! {
            event_result = async_rx.recv() => {
                let Some(event_result) = event_result else {
                    warn!("JSONL watcher channel disconnected");
                    return Ok(());
                };
                match event_result {
                    Ok(event) => {
                        handle_event(
                            event,
                            &mut offsets,
                            &agg_tx,
                            &mut cumulative,
                            &mut today,
                            &mut current_date,
                            &state_path,
                            &mut tokens_dirty,
                        )
                        .await;
                    }
                    Err(e) => warn!("JSONL watcher error: {e}"),
                }
            }
            _ = persist_tick.tick() => {
                if tokens_dirty {
                    let snap = PersistedTokens {
                        date: current_date.clone(),
                        cumulative,
                        today,
                    };
                    if let Err(e) = snap.save(&state_path) {
                        warn!("JSONL watcher: failed to persist tokens: {e:#}");
                    } else {
                        tokens_dirty = false;
                    }
                }
            }
        }
    }
}

/// Process one notify event.
#[allow(clippy::too_many_arguments)]
async fn handle_event(
    event: notify::Event,
    offsets: &mut FileOffsets,
    agg_tx: &AggregatorTx,
    cumulative: &mut u64,
    today: &mut u64,
    current_date: &mut String,
    state_path: &std::path::Path,
    tokens_dirty: &mut bool,
) {
    use notify::EventKind;

    let is_relevant = matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_));
    if !is_relevant {
        return;
    }

    for path in &event.paths {
        // Ignore non-.jsonl paths silently.
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }

        // The JSONL filename (without .jsonl) is the session UUID.
        // Pass it on TokensUpdate so the aggregator can track per-session
        // running totals (tokens_this_turn for the turn-done notification).
        // Falls back to None on a non-utf8 stem; the aggregator's global
        // cumulative still updates either way.
        let session_id = path.file_stem().and_then(|s| s.to_str()).map(str::to_owned);

        offsets.drain_new_lines(path, |line| {
            let Some(parsed) = parse_jsonl_line(line) else {
                return;
            };

            if parsed.output_tokens > 0 {
                // Self-heal: if the in-memory date is stale (timer missed
                // midnight, e.g. laptop was suspended), zero `today` before
                // incrementing.  Cheap string compare on the hot path.
                let local_today = current_local_date_string();
                if *current_date != local_today {
                    let from = std::mem::replace(current_date, local_today.clone());
                    let lost = *today;
                    *today = 0;
                    *tokens_dirty = true;
                    tracing::info!(
                        from = %from,
                        to = %local_today,
                        rolled_over = lost,
                        "tokens: date drift detected on TokensUpdate; zeroing `today`",
                    );
                    let new_date = local_today.clone();
                    let tx = agg_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx
                            .send(crate::state::AggregatorMsg::DailyReset { date: new_date })
                            .await;
                    });
                    // Persist immediately so a crash before the debounce
                    // doesn't lose the rollover.
                    let snap = PersistedTokens {
                        date: current_date.clone(),
                        cumulative: *cumulative,
                        today: 0,
                    };
                    if let Err(e) = snap.save(state_path) {
                        warn!("JSONL watcher: failed to persist rollover: {e:#}");
                    }
                }

                *cumulative += parsed.output_tokens;
                *today += parsed.output_tokens;
                *tokens_dirty = true;

                // Fire-and-forget: if aggregator is gone, we just log.
                let tx = agg_tx.clone();
                let tokens = parsed.output_tokens;
                let sid = session_id.clone();
                tokio::spawn(async move {
                    if tx
                        .send(crate::state::AggregatorMsg::TokensUpdate {
                            output_tokens: tokens,
                            session_id: sid,
                        })
                        .await
                        .is_err()
                    {
                        warn!("JSONL: aggregator gone, dropping TokensUpdate");
                    }
                });
            }

            if let Some(text) = parsed.entry_text {
                let tx = agg_tx.clone();
                tokio::spawn(async move {
                    let _ = tx
                        .send(crate::state::AggregatorMsg::AddEntry { text })
                        .await;
                });
            }
        });
    }
}
