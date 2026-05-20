// SPDX-License-Identifier: MIT
//! Date helpers used by the watcher and midnight-reset task.
//!
//! No `chrono` dep — a minimal Gregorian implementation is sufficient
//! for emitting `"YYYY-MM-DD"` strings.  Local-offset computation uses
//! the `time` crate's `OffsetDateTime`.

/// Return the current date as a `"YYYY-MM-DD"` string in UTC.
pub(crate) fn current_utc_date_string() -> String {
    // Compute from Unix epoch: days since 1970-01-01.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs / 86400;
    // Simple Gregorian calendar computation (no time crate needed for this).
    let (y, m, d) = days_to_ymd(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Compute how long to sleep until the next local midnight.
///
/// Uses the `time` crate for local-offset awareness.  Falls back to UTC
/// if the local offset cannot be determined.
pub(crate) fn secs_until_next_local_midnight() -> std::time::Duration {
    use time::{macros::time, OffsetDateTime};

    let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());

    // Next midnight in local time.
    let tomorrow_midnight = now
        .replace_time(time!(00:00:00))
        // Advance by one day.
        + time::Duration::days(1);

    let secs_remaining = (tomorrow_midnight - now).whole_seconds().max(0) as u64;
    std::time::Duration::from_secs(secs_remaining)
}

/// Convert days-since-1970-01-01 to (year, month, day).
///
/// Standalone implementation so we don't need a calendar crate just for
/// formatting a date string.
pub(crate) fn days_to_ymd(mut days: u64) -> (u32, u32, u32) {
    // Shift epoch to 1 March 0 (makes leap-year logic simpler).
    days += 719468;
    let era = days / 146097;
    let doe = days % 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u32, m as u32, d as u32)
}
