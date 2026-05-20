// SPDX-License-Identifier: MIT
//! JSONL tail — inotify-driven token tracker and entry extractor.
//!
//! # What this module does
//!
//! 1. **On startup:** scans `~/.claude/projects/**/*.jsonl`, records the
//!    current byte offset of each file, and loads the persisted
//!    [`PersistedTokens`] from `$XDG_STATE_HOME/ccbridge/tokens.json`.
//!
//! 2. **While running:** uses [`notify::RecommendedWatcher`] to watch the
//!    projects directory recursively.  On any `Modify` or `Create` event for a
//!    `*.jsonl` file, reads new lines (from the stashed offset onward) and:
//!    - Extracts `message.usage.output_tokens` → sends
//!      [`crate::state::AggregatorMsg::TokensUpdate`].
//!    - Extracts short assistant text snippets → sends
//!      [`crate::state::AggregatorMsg::AddEntry`].
//!    - Persists the updated token counts to disk (debounced, 5s).
//!
//! 3. **Daily reset:** a separate tokio task sleeps until next local midnight,
//!    queries the aggregator's cumulative, persists `today=0` for the new
//!    date, then sends [`crate::state::AggregatorMsg::DailyReset`].
//!
//! # Reliability invariant
//!
//! Parse failures on individual JSONL lines are logged and skipped — the
//! watcher task never crashes.  Missing or unreadable files are skipped.
//! Non-`*.jsonl` paths are silently ignored.
//!
//! # Module layout
//!
//! - [`tokens`]   — `PersistedTokens` + `tokens_state_path`.
//! - [`parse`]    — `parse_jsonl_line` + `ParsedAssistantLine`.
//! - [`offsets`]  — `FileOffsets` (per-file byte tracking with inode identity).
//! - [`dates`]    — date / midnight calendar math (no chrono dep).
//! - [`watcher`]  — `spawn_watcher` + the run loop.
//! - [`midnight`] — `spawn_midnight_reset` + the per-iteration helper.

mod dates;
mod midnight;
mod offsets;
mod parse;
mod tokens;
mod watcher;

// Public re-exports — preserve the original `ingest::jsonl::FOO` surface.
pub use midnight::spawn_midnight_reset;
pub use offsets::FileOffsets;
pub use parse::{parse_jsonl_line, ParsedAssistantLine};
pub use tokens::{tokens_state_path, PersistedTokens};
pub use watcher::spawn_watcher;

// Crate-internal re-export for `permission/additions/audit_log.rs` which
// builds an ISO-8601 timestamp via `days_to_ymd`.
pub(crate) use dates::days_to_ymd;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// Tests live in this single mod.rs (rather than per-submodule) because the
// midnight-reset test reaches into `midnight::perform_midnight_reset`
// (pub(super) for that purpose) and several tests cover end-to-end
// interaction across submodules. The cost is a longer test module; the
// benefit is no `pub(crate)` annotations purely for test reach.

#[cfg(test)]
mod tests {
    use super::midnight::perform_midnight_reset;
    use super::offsets::FileOffsets;
    use super::parse::parse_jsonl_line;
    use super::tokens::{tokens_state_path, PersistedTokens};

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
    // perform_midnight_reset
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn midnight_reset_preserves_cumulative_from_aggregator() {
        // Spin up a real aggregator and seed it with non-zero output tokens
        // (simulating a normal day's accumulation). Then run one iteration
        // of perform_midnight_reset and assert the persisted file carries
        // the actual cumulative — not 0.
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("tokens.json");

        let (agg_tx, _hb_rx) = crate::state::spawn(
            crate::state::DEFAULT_APPROVAL_TIMEOUT,
            crate::config::Fallback::default(),
            std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(
                crate::permission::Allowlist::empty(),
            ))),
        );

        // Drive the aggregator's cumulative up to 184_502 via TokensUpdate.
        agg_tx
            .send(crate::state::AggregatorMsg::TokensUpdate {
                output_tokens: 184_502,
            })
            .await
            .unwrap();

        // Round-trip a heartbeat to confirm the aggregator has processed
        // the update before we run the reset (otherwise we race the read).
        let (htx, hrx) = tokio::sync::oneshot::channel();
        agg_tx
            .send(crate::state::AggregatorMsg::GetHeartbeat { respond: htx })
            .await
            .unwrap();
        let hb = hrx.await.unwrap();
        assert_eq!(hb.tokens, 184_502, "precondition: aggregator must hold 184_502");

        // Run one reset iteration.
        let cf = perform_midnight_reset(&state_path, &agg_tx).await;
        assert!(matches!(cf, std::ops::ControlFlow::Continue(())));

        // The persisted file must carry the queried cumulative, not 0.
        let loaded = PersistedTokens::load(&state_path).unwrap();
        assert_eq!(loaded.today, 0, "today must reset to 0");
        assert_eq!(
            loaded.cumulative, 184_502,
            "cumulative must be queried from the aggregator, not zeroed"
        );
        assert!(!loaded.date.is_empty(), "date must be set to today");
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

    // -----------------------------------------------------------------------
    // I2: inode tracking regression test
    // -----------------------------------------------------------------------

    #[test]
    fn file_offsets_resets_on_atomic_replace() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.jsonl");

        // Write v1 and record the offset.
        std::fs::write(&file_path, "line1\nline2\n").unwrap();
        let mut offsets = FileOffsets::new();
        let mut lines_v1: Vec<String> = Vec::new();
        offsets.drain_new_lines(&file_path, |l| lines_v1.push(l.to_owned()));
        assert_eq!(lines_v1, vec!["line1", "line2"], "v1 lines must be read");
        // Offset is now at end of v1.

        // Atomically replace file via tmp+rename (simulates backup tool / our own save_settings).
        let tmp = dir.path().join("test.jsonl.tmp");
        std::fs::write(&tmp, "line3\n").unwrap();
        std::fs::rename(&tmp, &file_path).unwrap();

        // drain_new_lines must detect the inode change and reset offset to 0.
        let mut lines_v2: Vec<String> = Vec::new();
        offsets.drain_new_lines(&file_path, |l| lines_v2.push(l.to_owned()));
        assert_eq!(
            lines_v2,
            vec!["line3"],
            "after atomic replace, new file must be read from start"
        );
    }
}
