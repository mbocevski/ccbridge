// SPDX-License-Identifier: MIT
//! Midnight-reset task: at local midnight, query the aggregator's
//! cumulative token total, persist a zeroed `today` for the new date,
//! then send `DailyReset`.

use std::path::{Path, PathBuf};

use tracing::warn;

use super::dates::{current_utc_date_string, secs_until_next_local_midnight};
use super::tokens::PersistedTokens;
use crate::state::AggregatorTx;

/// Spawn the midnight-reset task.
///
/// Sleeps until next local midnight, then:
/// 1. Queries the aggregator for the current cumulative token count.
/// 2. Persists reset token state (`today = 0`, `cumulative = <queried>`,
///    `date = new_date`) to `state_path`.
/// 3. Sends [`crate::state::AggregatorMsg::DailyReset`] to the aggregator.
/// 4. Sleeps until the following midnight.
///
/// Persisting before sending ensures a daemon restart immediately after midnight
/// doesn't think the day has not yet rolled over.
///
/// The cumulative-query in step 1 closes a crash race: the previous version
/// persisted `cumulative=0` here, so a daemon crash between this write and
/// the next [`crate::state::AggregatorMsg::TokensUpdate`] would lose the
/// entire pre-midnight token total on restart. Querying the aggregator first
/// preserves it across the rollover.
pub fn spawn_midnight_reset(
    state_path: PathBuf,
    agg_tx: AggregatorTx,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_dur = secs_until_next_local_midnight();
            tokio::time::sleep(sleep_dur).await;

            if perform_midnight_reset(&state_path, &agg_tx).await.is_break() {
                break;
            }
        }
    })
}

/// One iteration of the midnight reset.  Extracted so it can be unit-tested
/// without waiting for actual local midnight.
///
/// Returns `ControlFlow::Break(())` when the aggregator has gone away
/// (terminal — caller should exit the loop).
pub(super) async fn perform_midnight_reset(
    state_path: &Path,
    agg_tx: &AggregatorTx,
) -> std::ops::ControlFlow<()> {
    let new_date = current_utc_date_string();

    // Read the current cumulative from the aggregator before persisting
    // so a crash between persist and the next TokensUpdate doesn't lose
    // history. Falls back to 0 if the query fails.
    let cumulative = match query_cumulative(agg_tx).await {
        Some(c) => c,
        None => {
            warn!(
                "midnight reset: failed to query cumulative before \
                 persist; falling back to 0 (history may not survive a \
                 crash before the next TokensUpdate)"
            );
            0
        }
    };

    let tokens_snapshot = PersistedTokens {
        date: new_date.clone(),
        today: 0,
        cumulative,
    };
    if let Err(e) = tokens_snapshot.save(state_path) {
        warn!("midnight reset: failed to persist tokens.json: {e:#}");
    }

    if agg_tx
        .send(crate::state::AggregatorMsg::DailyReset { date: new_date })
        .await
        .is_err()
    {
        warn!("midnight reset: aggregator gone, stopping midnight task");
        return std::ops::ControlFlow::Break(());
    }
    std::ops::ControlFlow::Continue(())
}

/// Round-trip a `GetHeartbeat` through the aggregator to fetch the current
/// cumulative token count. Returns `None` on send/recv failure.
async fn query_cumulative(agg_tx: &AggregatorTx) -> Option<u64> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    agg_tx
        .send(crate::state::AggregatorMsg::GetHeartbeat { respond: tx })
        .await
        .ok()?;
    let hb = rx.await.ok()?;
    Some(hb.tokens)
}
