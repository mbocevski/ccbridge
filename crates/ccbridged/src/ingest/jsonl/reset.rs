// SPDX-License-Identifier: MIT
//! Single source of truth for "is the persisted date stale?" — used by
//! the startup catch-up, the midnight timer, and the per-event
//! self-heal in the watcher.
//!
//! The midnight task only fires while the daemon is running; if the
//! laptop is suspended across midnight, the timer wakes late and
//! `tokio::time::sleep` doesn't catch up. The watcher's per-message
//! self-heal closes that gap.

use std::path::Path;

use tracing::info;

use super::dates::current_local_date_string;
use super::tokens::PersistedTokens;
use crate::state::{AggregatorMsg, AggregatorTx};

/// Outcome of [`catch_up_to_local_date`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatchUp {
    /// Date matched today — nothing changed.
    UpToDate,
    /// Date was stale; `today` was zeroed and the date advanced.
    Rolled,
}

/// If `state.date` is older than today's local date, zero `today`, advance
/// the date, persist, and notify the aggregator. `cumulative` is preserved.
///
/// Returns `Rolled` when a rollover happened so callers can log it.
///
/// `state` is updated in place to reflect the new values when a rollover
/// fires; callers should use the mutated value going forward.
pub(super) async fn catch_up_to_local_date(
    state: &mut PersistedTokens,
    state_path: &Path,
    agg_tx: &AggregatorTx,
) -> CatchUp {
    let today = current_local_date_string();

    // Empty date means "first-run, never persisted" — adopt today and
    // continue. No rollover semantics needed.
    if state.date.is_empty() {
        state.date = today;
        return CatchUp::UpToDate;
    }
    if state.date == today {
        return CatchUp::UpToDate;
    }

    let from = std::mem::replace(&mut state.date, today.clone());
    let lost = state.today;
    state.today = 0;

    if let Err(e) = state.save(state_path) {
        tracing::warn!("date catch-up: failed to persist tokens: {e:#}");
    }

    if agg_tx
        .send(AggregatorMsg::DailyReset {
            date: today.clone(),
        })
        .await
        .is_err()
    {
        tracing::warn!("date catch-up: aggregator gone, DailyReset not delivered");
    }

    info!(
        from = %from,
        to = %today,
        rolled_over = lost,
        "tokens: date rolled over while daemon was off (or timer missed midnight); zeroed `today`",
    );
    CatchUp::Rolled
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn no_rollover_when_date_matches_today() {
        let (tx, _rx) = mpsc::channel(1);
        let today = current_local_date_string();
        let mut state = PersistedTokens {
            date: today.clone(),
            today: 100,
            cumulative: 500,
        };
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let outcome = catch_up_to_local_date(&mut state, tmp.path(), &tx).await;
        assert_eq!(outcome, CatchUp::UpToDate);
        assert_eq!(state.today, 100, "today must be preserved");
        assert_eq!(state.date, today);
    }

    #[tokio::test]
    async fn rollover_zeroes_today_and_preserves_cumulative() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut state = PersistedTokens {
            date: "2020-01-01".to_owned(), // stale
            today: 4_700_000,
            cumulative: 4_700_000,
        };
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let outcome = catch_up_to_local_date(&mut state, tmp.path(), &tx).await;
        assert_eq!(outcome, CatchUp::Rolled);
        assert_eq!(state.today, 0);
        assert_eq!(state.cumulative, 4_700_000, "cumulative must be preserved");
        assert_ne!(state.date, "2020-01-01");

        // DailyReset must have been delivered.
        let msg = rx.try_recv().expect("DailyReset must be sent");
        match msg {
            AggregatorMsg::DailyReset { date } => assert_eq!(date, state.date),
            _ => panic!("expected DailyReset"),
        }
    }

    #[tokio::test]
    async fn empty_date_adopts_today_without_rolling() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut state = PersistedTokens {
            date: String::new(),
            today: 0,
            cumulative: 0,
        };
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let outcome = catch_up_to_local_date(&mut state, tmp.path(), &tx).await;
        assert_eq!(outcome, CatchUp::UpToDate);
        assert_eq!(state.date, current_local_date_string());
        assert!(rx.try_recv().is_err(), "no DailyReset on first-run");
    }
}
