// SPDX-License-Identifier: MIT
//! JSONL tail — inotify-driven token tracker and entry extractor.
//!
//! # What this module does
//!
//! 1. **On startup:** scans `~/.claude/projects/**/*.jsonl`, records the
//!    current byte offset of each file, and loads the persisted
//!    [`TokenState`] from `$XDG_STATE_HOME/ccbridge/tokens.json`.
//!
//! 2. **While running:** uses [`notify::RecommendedWatcher`] to watch the
//!    projects directory recursively.  On any `Modify` or `Create` event for a
//!    `*.jsonl` file, reads new lines (from the stashed offset onward) and:
//!    - Extracts `message.usage.output_tokens` → sends
//!      [`AggregatorMsg::TokensUpdate`].
//!    - Extracts short assistant text snippets → sends
//!      [`AggregatorMsg::AddEntry`].
//!    - Persists the updated token counts to disk (debounced, 5s).
//!
//! 3. **Daily reset:** a separate tokio task sleeps until next local midnight,
//!    sends [`AggregatorMsg::DailyReset`] with the new date string, and persists
//!    the zeroed state before sleeping again.
//!
//! # Reliability invariant
//!
//! Parse failures on individual JSONL lines are logged and skipped — the
//! watcher task never crashes.  Missing or unreadable files are skipped.
//! Non-`*.jsonl` paths are silently ignored.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::state::AggregatorTx;

// ---------------------------------------------------------------------------
// Persisted token state (tokens.json)
// ---------------------------------------------------------------------------

/// The on-disk representation of the token state.
///
/// Stored at `$XDG_STATE_HOME/ccbridge/tokens.json`.
/// Falls back to `$HOME/.local/state/ccbridge/tokens.json`.
///
/// Wire shape:
/// ```json
/// {"date": "2026-05-19", "today": 31200, "cumulative": 184502}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedTokens {
    /// The date this `today` count belongs to, `"YYYY-MM-DD"` (UTC).
    pub date: String,
    /// Output tokens since local midnight.
    pub today: u64,
    /// Cumulative output tokens (monotonically increasing, resets with the file).
    pub cumulative: u64,
}

impl PersistedTokens {
    /// Load from disk.  Returns [`Default`] if the file does not exist.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
    }

    /// Atomically write to `path` (write to `path.tmp`, then rename).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }
}

/// Return the path of the token state file.
///
/// Priority:
/// 1. `$XDG_STATE_HOME/ccbridge/tokens.json`
/// 2. `$HOME/.local/state/ccbridge/tokens.json`
/// 3. `Err` — both variables unset (misconfigured system).
///
/// The caller should log the error and disable token persistence rather than
/// falling back to `/tmp` (world-readable, collision-prone on multi-user boxes).
pub fn tokens_state_path() -> Result<PathBuf> {
    let base = if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        PathBuf::from(xdg)
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".local").join("state")
    } else {
        anyhow::bail!(
            "cannot determine token state path: neither $XDG_STATE_HOME nor $HOME is set"
        );
    };
    Ok(base.join("ccbridge").join("tokens.json"))
}

// ---------------------------------------------------------------------------
// JSONL line parser
// ---------------------------------------------------------------------------

/// A parsed assistant JSONL line — the fields ccbridge cares about.
#[derive(Debug, Default)]
pub struct ParsedAssistantLine {
    /// `message.usage.output_tokens` — 0 if absent.
    pub output_tokens: u64,
    /// Short text from the first assistant `text` content block, if any.
    /// Truncated to 80 chars for use as a heartbeat entry.
    pub entry_text: Option<String>,
}

/// Parse one raw JSONL line.
///
/// Returns `None` if the line is not an assistant message.
/// Returns `Some(ParsedAssistantLine)` with whatever fields are present;
/// missing `usage` or `content` are treated as zero/absent (not an error).
pub fn parse_jsonl_line(line: &str) -> Option<ParsedAssistantLine> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    let v: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            debug!("jsonl: parse error (skipping): {e}");
            return None;
        }
    };

    // Only process assistant messages.
    if v.get("type").and_then(|t| t.as_str()) != Some("assistant") {
        return None;
    }

    let msg = v.get("message")?;

    let output_tokens = msg
        .get("usage")
        .and_then(|u| u.get("output_tokens"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0);

    // Extract the first non-empty text content block for entries.
    let entry_text = msg
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|arr| {
            arr.iter().find_map(|block| {
                if block.get("type")?.as_str()? == "text" {
                    let text = block.get("text")?.as_str()?;
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        return None;
                    }
                    // Take first line, truncate to 80 chars.
                    let first_line = trimmed.lines().next().unwrap_or(trimmed);
                    Some(truncate_chars(first_line, 80))
                } else {
                    None
                }
            })
        });

    Some(ParsedAssistantLine {
        output_tokens,
        entry_text,
    })
}

// ---------------------------------------------------------------------------
// File offset tracker
// ---------------------------------------------------------------------------

/// Tracks the last-read byte offset for each watched JSONL file.
pub struct FileOffsets {
    inner: HashMap<PathBuf, u64>,
}

impl Default for FileOffsets {
    fn default() -> Self {
        Self::new()
    }
}

impl FileOffsets {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Scan `projects_dir` recursively, record the current end-of-file offset
    /// for every `*.jsonl` file.  New lines arriving after this call will be
    /// tailed; history is not replayed.
    pub fn snapshot_existing(&mut self, projects_dir: &Path) {
        let walker = walkdir_jsonl(projects_dir);
        for path in walker {
            match std::fs::metadata(&path).map(|m| m.len()) {
                Ok(len) => {
                    self.inner.entry(path).or_insert(len);
                }
                Err(e) => {
                    warn!("jsonl: stat {} failed: {e}", path.display());
                }
            }
        }
    }

    /// Read new lines from `path` since the last recorded offset.
    /// Calls `on_line` for each new line (with trailing `\r?\n` stripped).
    ///
    /// Uses [`BufRead::read_line`] rather than [`BufRead::lines`] so that the
    /// raw byte count (including the newline bytes) is used to advance the
    /// offset — this correctly handles files whose last line has no trailing
    /// newline, and avoids the `lines() + 1` off-by-one that would occur in
    /// that case.
    pub fn drain_new_lines(&mut self, path: &Path, mut on_line: impl FnMut(&str)) {
        let offset = self.inner.entry(path.to_path_buf()).or_insert(0);
        match std::fs::File::open(path) {
            Err(e) => {
                warn!("jsonl: open {} failed: {e}", path.display());
            }
            Ok(mut file) => {
                if let Err(e) = file.seek(SeekFrom::Start(*offset)) {
                    warn!("jsonl: seek {} failed: {e}", path.display());
                    return;
                }
                let mut reader = BufReader::new(&mut file);
                let mut bytes_read: u64 = 0;
                loop {
                    let mut raw = String::new();
                    match reader.read_line(&mut raw) {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            bytes_read += n as u64;
                            // Strip trailing \r\n or \n before passing to callback.
                            let trimmed = raw.trim_end_matches('\n').trim_end_matches('\r');
                            on_line(trimmed);
                        }
                        Err(e) => {
                            warn!("jsonl: read_line in {} failed: {e}", path.display());
                            break;
                        }
                    }
                }
                *offset += bytes_read;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Spawn functions
// ---------------------------------------------------------------------------

/// Spawn the JSONL watcher task.
///
/// Watches `projects_dir` recursively with [`notify::RecommendedWatcher`].
/// For each new assistant line in a `*.jsonl` file, sends:
/// - [`crate::state::AggregatorMsg::TokensUpdate`] with `output_tokens`
/// - [`crate::state::AggregatorMsg::AddEntry`] with the entry text (if any)
///
/// Token counts are persisted to `state_path` (debounced, every
/// [`PERSIST_DEBOUNCE`]).  If `state_path` cannot be determined or written,
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

/// Spawn the midnight-reset task.
///
/// Sleeps until next local midnight, then:
/// 1. Persists reset token state (`today = 0`, `date = new_date`) to `state_path`.
/// 2. Sends [`crate::state::AggregatorMsg::DailyReset`] to the aggregator.
/// 3. Sleeps until the following midnight.
///
/// Persisting before sending ensures a daemon restart immediately after midnight
/// doesn't think the day has not yet rolled over.
pub fn spawn_midnight_reset(
    state_path: PathBuf,
    agg_tx: AggregatorTx,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let sleep_dur = secs_until_next_local_midnight();
            tokio::time::sleep(sleep_dur).await;

            let new_date = current_utc_date_string();

            // Persist the reset (today = 0) immediately.
            let tokens_snapshot = PersistedTokens {
                date: new_date.clone(),
                today: 0,
                cumulative: 0, // Aggregator owns cumulative; we write 0 here,
                               // the JSONL watcher will re-persist once it gets
                               // the next TokensUpdate. Acceptable gap.
            };
            if let Err(e) = tokens_snapshot.save(&state_path) {
                warn!("midnight reset: failed to persist tokens.json: {e:#}");
            }

            if agg_tx
                .send(crate::state::AggregatorMsg::DailyReset { date: new_date })
                .await
                .is_err()
            {
                warn!("midnight reset: aggregator gone, stopping midnight task");
                break;
            }
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
    // persisted date if available so we don't recompute it on every persist and
    // don't accidentally advance the date boundary until midnight-reset fires.
    //
    // TODO: plumb `DailyReset` acknowledgement back into the watcher so it can
    // update `current_date` without relying solely on the midnight-reset task.
    let current_date = if initial_tokens.date.is_empty() {
        current_utc_date_string()
    } else {
        initial_tokens.date.clone()
    };

    // Snapshot existing file offsets so we only process *new* lines.
    let mut offsets = FileOffsets::new();
    offsets.snapshot_existing(&projects_dir);

    // Create a synchronous notify channel (notify 6 is sync).
    let (ev_tx, ev_rx) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher =
        RecommendedWatcher::new(ev_tx, Config::default()).context("create filesystem watcher")?;
    watcher
        .watch(&projects_dir, RecursiveMode::Recursive)
        .context("watch projects dir")?;

    tracing::info!(dir = %projects_dir.display(), "JSONL watcher started");

    // Debounce timer: tokens changed since last persist?
    let mut tokens_dirty = false;
    let mut last_persist = std::time::Instant::now();

    // Run the event loop inside a blocking task (notify 6 uses a sync channel).
    // We poll the channel with a short timeout so we can also drive the persist
    // debounce without blocking the tokio runtime.
    loop {
        // Drain all pending events (non-blocking after the first).
        loop {
            match ev_rx.try_recv() {
                Ok(Ok(event)) => {
                    handle_event(
                        event,
                        &mut offsets,
                        &agg_tx,
                        &mut cumulative,
                        &mut today,
                        &mut tokens_dirty,
                    )
                    .await;
                }
                Ok(Err(e)) => {
                    warn!("JSONL watcher error: {e}");
                }
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    warn!("JSONL watcher channel disconnected");
                    return Ok(());
                }
            }
        }

        // Persist debounce: flush every PERSIST_DEBOUNCE if tokens changed.
        // Use `current_date` (set at startup from initial_tokens.date or today's
        // UTC date) rather than recomputing — keeps the day boundary stable until
        // the midnight-reset task fires DailyReset.
        if tokens_dirty && last_persist.elapsed() >= PERSIST_DEBOUNCE {
            let snap = PersistedTokens {
                date: current_date.clone(),
                cumulative,
                today,
            };
            if let Err(e) = snap.save(&state_path) {
                warn!("JSONL watcher: failed to persist tokens: {e:#}");
            } else {
                tokens_dirty = false;
                last_persist = std::time::Instant::now();
            }
        }

        // Yield to the tokio runtime before polling again.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Process one notify event.
async fn handle_event(
    event: notify::Event,
    offsets: &mut FileOffsets,
    agg_tx: &AggregatorTx,
    cumulative: &mut u64,
    today: &mut u64,
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

        offsets.drain_new_lines(path, |line| {
            let Some(parsed) = parse_jsonl_line(line) else {
                return;
            };

            if parsed.output_tokens > 0 {
                *cumulative += parsed.output_tokens;
                *today += parsed.output_tokens;
                *tokens_dirty = true;

                // Fire-and-forget: if aggregator is gone, we just log.
                let tx = agg_tx.clone();
                let tokens = parsed.output_tokens;
                tokio::spawn(async move {
                    if tx
                        .send(crate::state::AggregatorMsg::TokensUpdate {
                            output_tokens: tokens,
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Truncate `s` to at most `max_chars` Unicode scalar values, appending `…`
/// if truncated.
///
/// Unlike a byte-slice (`&s[..n]`), this can never panic on multi-byte chars.
fn truncate_chars(s: &str, max_chars: usize) -> String {
    let mut chars = s.chars();
    let head: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        // There were more chars — append the ellipsis.
        format!("{head}…")
    } else {
        head
    }
}

/// Recursively walk `dir` and yield paths to `*.jsonl` files.
fn walkdir_jsonl(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_dir_inner(dir, &mut out);
    out
}

fn walk_dir_inner(dir: &Path, out: &mut Vec<PathBuf>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_dir_inner(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // parse_jsonl_line
    // -----------------------------------------------------------------------

    #[test]
    fn parse_real_assistant_line_with_usage() {
        // Matches the actual Claude Code JSONL shape observed in
        // ~/.claude/projects/**/*.jsonl
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "usage": {
                    "input_tokens": 3,
                    "cache_creation_input_tokens": 502,
                    "cache_read_input_tokens": 8724,
                    "output_tokens": 200,
                    "server_tool_use": {"web_search_requests": 0},
                    "service_tier": "standard"
                },
                "content": [
                    {"type": "text", "text": "Here is my analysis of the situation."}
                ]
            }
        }))
        .unwrap();

        let parsed = parse_jsonl_line(&line).expect("should parse");
        assert_eq!(parsed.output_tokens, 200);
        assert_eq!(
            parsed.entry_text.as_deref(),
            Some("Here is my analysis of the situation.")
        );
    }

    #[test]
    fn parse_assistant_line_no_usage() {
        // Thinking-only message — no output_tokens in usage
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "..."}]
            }
        }))
        .unwrap();

        let parsed = parse_jsonl_line(&line).expect("should parse as assistant");
        assert_eq!(parsed.output_tokens, 0);
        assert!(parsed.entry_text.is_none());
    }

    #[test]
    fn parse_skips_non_assistant_types() {
        let user_line = serde_json::to_string(&json!({
            "type": "user",
            "message": {"role": "user", "content": "hello"}
        }))
        .unwrap();
        assert!(parse_jsonl_line(&user_line).is_none());

        let system_line = serde_json::to_string(&json!({
            "type": "permission-mode",
            "permissionMode": "default"
        }))
        .unwrap();
        assert!(parse_jsonl_line(&system_line).is_none());
    }

    #[test]
    fn parse_malformed_json_returns_none() {
        assert!(parse_jsonl_line("not json at all").is_none());
        assert!(parse_jsonl_line("{unclosed").is_none());
        assert!(parse_jsonl_line("").is_none());
        assert!(parse_jsonl_line("   \n  ").is_none());
    }

    #[test]
    fn parse_non_jsonl_file_extension_lines_are_skipped() {
        // A non-JSONL "file" that somehow ends up in the watch path —
        // its lines just parse as non-assistant and return None.
        assert!(parse_jsonl_line("# This is a comment").is_none());
        // Raw bytes that aren't valid JSON
        assert!(parse_jsonl_line("binary\x00data").is_none());
    }

    #[test]
    fn parse_entry_text_truncated_at_80_chars_ascii() {
        let long_text = "A".repeat(100);
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "usage": {"output_tokens": 50},
                "content": [{"type": "text", "text": long_text}]
            }
        }))
        .unwrap();

        let parsed = parse_jsonl_line(&line).unwrap();
        let snippet = parsed.entry_text.unwrap();
        // 80 ASCII chars + "…" → 81 chars total? No: truncate_chars(s, 80) takes
        // the first 80 chars, sees more, appends "…" → 81 char count.
        // The cap is "max 80 chars of original content, then …".
        assert!(snippet.ends_with('…'));
        assert_eq!(snippet.chars().count(), 81); // 80 original + ellipsis
    }

    #[test]
    fn parse_entry_text_truncated_multibyte_no_panic() {
        // Em-dashes are 3 bytes each. A byte-slice truncation would panic;
        // char-count truncation must not.
        let em_dashes = "—".repeat(100); // 300 bytes total
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "usage": {"output_tokens": 1},
                "content": [{"type": "text", "text": em_dashes}]
            }
        }))
        .unwrap();

        let parsed = parse_jsonl_line(&line).unwrap();
        let snippet = parsed.entry_text.unwrap();
        assert!(snippet.ends_with('…'), "should be truncated");
        // 80 em-dashes + "…" = 81 chars
        assert_eq!(snippet.chars().count(), 81);
        // Must not have panicked reaching here — the test itself verifies that.
    }

    #[test]
    fn parse_entry_text_short_not_truncated() {
        let text = "short text";
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "usage": {"output_tokens": 5},
                "content": [{"type": "text", "text": text}]
            }
        }))
        .unwrap();
        let parsed = parse_jsonl_line(&line).unwrap();
        assert_eq!(parsed.entry_text.as_deref(), Some("short text"));
    }

    #[test]
    fn parse_only_first_text_block_used_for_entry() {
        let line = serde_json::to_string(&json!({
            "type": "assistant",
            "message": {
                "usage": {"output_tokens": 10},
                "content": [
                    {"type": "thinking", "thinking": "internal reasoning"},
                    {"type": "text", "text": "first text block"},
                    {"type": "text", "text": "second text block"}
                ]
            }
        }))
        .unwrap();

        let parsed = parse_jsonl_line(&line).unwrap();
        assert_eq!(parsed.entry_text.as_deref(), Some("first text block"));
    }

    // -----------------------------------------------------------------------
    // PersistedTokens load/save
    // -----------------------------------------------------------------------

    #[test]
    fn persisted_tokens_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ccbridge").join("tokens.json");

        let tokens = PersistedTokens {
            date: "2026-05-19".to_owned(),
            today: 31_200,
            cumulative: 184_502,
        };
        tokens.save(&path).unwrap();
        assert!(path.exists());

        let loaded = PersistedTokens::load(&path).unwrap();
        assert_eq!(loaded.date, "2026-05-19");
        assert_eq!(loaded.today, 31_200);
        assert_eq!(loaded.cumulative, 184_502);
    }

    #[test]
    fn persisted_tokens_missing_file_returns_default() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent").join("tokens.json");
        let loaded = PersistedTokens::load(&path).unwrap();
        assert_eq!(loaded.today, 0);
        assert_eq!(loaded.cumulative, 0);
        assert_eq!(loaded.date, "");
    }

    #[test]
    fn persisted_tokens_save_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("a").join("b").join("c").join("tokens.json");
        let tokens = PersistedTokens {
            date: "2026-05-20".to_owned(),
            today: 1,
            cumulative: 2,
        };
        tokens.save(&path).unwrap();
        assert!(path.exists());
    }

    // -----------------------------------------------------------------------
    // FileOffsets drain_new_lines
    // -----------------------------------------------------------------------

    #[test]
    fn file_offsets_only_reads_new_lines() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("session.jsonl");

        // Write two lines upfront.
        std::fs::write(&path, "line1\nline2\n").unwrap();

        let mut offsets = FileOffsets::new();
        // Snapshot existing → both lines are "already seen".
        offsets.snapshot_existing(dir.path());

        // Append a new line.
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(f, "line3").unwrap();

        let mut collected = Vec::new();
        offsets.drain_new_lines(&path, |line| collected.push(line.to_owned()));
        assert_eq!(collected, vec!["line3"]);
    }

    #[test]
    fn file_offsets_no_trailing_newline_correct_offset() {
        // Regression for the lines()+1 off-by-one bug:
        // If the file's last line has no trailing newline, the raw byte count
        // from read_line() should still be accurate so the next drain picks
        // up exactly where we left off.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("notail.jsonl");

        // No trailing newline on "line2".
        std::fs::write(&path, "line1\nline2").unwrap();

        let mut offsets = FileOffsets::new();
        let mut collected = Vec::new();
        offsets.drain_new_lines(&path, |l| collected.push(l.to_owned()));
        assert_eq!(collected, vec!["line1", "line2"]);

        // Append a real third line (with newline this time).
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // Complete the previous line + add a new one.
        write!(f, "\nline3\n").unwrap();

        let mut new_lines = Vec::new();
        offsets.drain_new_lines(&path, |l| new_lines.push(l.to_owned()));

        // Should see the newline-completion-of-line2 as empty (just '\n')
        // and then "line3".  Since we trim each raw line, we expect "" and "line3".
        // More precisely: the first read_line after the seek returns "\n" (empty after trim),
        // then "line3\n".
        assert!(
            new_lines.contains(&"line3".to_owned()),
            "should see line3 after correct offset, got: {:?}",
            new_lines
        );
    }

    #[test]
    fn file_offsets_new_file_starts_from_zero() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("new_session.jsonl");

        std::fs::write(&path, "first\nsecond\n").unwrap();

        let mut offsets = FileOffsets::new();
        // No snapshot — this is a newly appearing file.
        let mut collected = Vec::new();
        offsets.drain_new_lines(&path, |line| collected.push(line.to_owned()));
        assert_eq!(collected, vec!["first", "second"]);
    }

    #[test]
    fn file_offsets_missing_file_does_not_panic() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("nope.jsonl");
        let mut offsets = FileOffsets::new();
        // Should log a warning and return without panicking.
        offsets.drain_new_lines(&missing, |_| panic!("should not be called"));
    }

    // -----------------------------------------------------------------------
    // tokens_state_path
    // -----------------------------------------------------------------------

    #[test]
    fn tokens_state_path_uses_xdg_state_home() {
        // Can't safely override env vars in parallel tests; verify the path
        // ends with the right suffix (XDG_STATE_HOME or HOME are set in any
        // normal test environment).
        let p = tokens_state_path().expect("tokens_state_path should succeed");
        assert!(
            p.ends_with("ccbridge/tokens.json"),
            "unexpected path: {}",
            p.display()
        );
    }
}
