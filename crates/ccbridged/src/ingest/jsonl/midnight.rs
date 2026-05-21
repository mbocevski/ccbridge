// SPDX-License-Identifier: MIT
//! Midnight-reset task: at local midnight, advance the persisted date,
//! zero `today`, and notify the aggregator via `DailyReset`.
//!
//! Delegates the actual rollover to [`super::reset::catch_up_to_local_date`]
//! so the daemon-startup path, the timer path, and the watcher self-heal
//! path all share one implementation.

use std::path::{Path, PathBuf};

use tracing::warn;

use super::dates::secs_until_next_local_midnight;
use super::reset::catch_up_to_local_date;
use super::tokens::PersistedTokens;
use crate::state::AggregatorTx;

/// Spawn the midnight-reset task.
///
/// Sleeps until next local midnight, then runs the date catch-up helper.
/// On a clean continuous-run, that drops `today` to 0 and advances the
/// date.  When the daemon resumes from suspend after a missed midnight,
/// the same helper reconciles whatever was persisted last.
///
/// Cumulative is preserved by reading the persisted file each iteration
/// rather than zeroing locally — a crash between this write and the
/// next `TokensUpdate` therefore keeps the pre-midnight total.
pub fn spawn_midnight_reset(
    state_path: PathBuf,
    agg_tx: AggregatorTx,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_dur = secs_until_next_local_midnight();
            tokio::time::sleep(sleep_dur).await;

            if perform_midnight_reset(&state_path, &agg_tx)
                .await
                .is_break()
            {
                break;
            }
        }
    })
}

/// One iteration of the midnight reset.  Loads the latest persisted
/// state from disk (so we pick up cumulative writes the watcher made
/// since boot), runs the shared rollover helper, and lets that
/// helper persist the new state.
///
/// Returns `ControlFlow::Break(())` when the aggregator has gone away.
pub(super) async fn perform_midnight_reset(
    state_path: &Path,
    agg_tx: &AggregatorTx,
) -> std::ops::ControlFlow<()> {
    let mut state = PersistedTokens::load(state_path).unwrap_or_else(|e| {
        warn!("midnight reset: failed to load tokens.json: {e:#}; using defaults");
        PersistedTokens::default()
    });
    catch_up_to_local_date(&mut state, state_path, agg_tx).await;
    if agg_tx.is_closed() {
        return std::ops::ControlFlow::Break(());
    }
    std::ops::ControlFlow::Continue(())
}
