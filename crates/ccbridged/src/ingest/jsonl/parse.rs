// SPDX-License-Identifier: MIT
//! JSONL line parser — extracts the fields ccbridge cares about from
//! `~/.claude/projects/**/*.jsonl` lines.

use tracing::debug;

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
