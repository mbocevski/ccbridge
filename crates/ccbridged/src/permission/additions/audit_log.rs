// SPDX-License-Identifier: MIT
//! Audit log: JSONL format on disk, with backwards-compat for legacy TSV
//! lines.  Provides append + reverse-walk over the persisted entries.

use std::io::Write as IoWrite;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::target::AuditTarget;

/// Metadata attached to an audit log entry.
pub struct AdditionMetadata {
    pub tool_use_id: String,
    pub session_id: String,
    pub agent_type: Option<String>,
}

/// Audit log path: `$XDG_STATE_HOME/ccbridge/allowlist-additions.log`.
pub fn audit_log_path() -> anyhow::Result<std::path::PathBuf> {
    Ok(crate::util::xdg_state_dir()?
        .join("ccbridge")
        .join("allowlist-additions.log"))
}

// ---------------------------------------------------------------------------
// JSONL on-disk row
// ---------------------------------------------------------------------------

/// One line in the audit log (JSONL format, new writes only).
///
/// Example:
/// ```json
/// {"ts":"2026-05-19T22:00:00Z","op":"added","pattern":"Bash(npm test)",
///  "tool_use_id":"toolu_01abc","session_id":"3cb589","agent":"core",
///  "target":{"project_local":{"root":"/home/user/proj"}}}
/// ```
#[derive(Serialize, Deserialize)]
struct AuditLogRow {
    ts: String,
    op: String,
    pattern: String,
    tool_use_id: String,
    session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent: Option<String>,
    target: AuditTarget,
}

impl AuditLogRow {
    fn into_entry(self) -> AuditEntry {
        AuditEntry {
            op: self.op,
            pattern: self.pattern,
            tool_use_id: self.tool_use_id,
            session_id: self.session_id,
            agent_type: self.agent,
            target: self.target,
        }
    }
}

/// One parsed audit-log entry, regardless of on-disk encoding.
///
/// `pub(super)` so that `undo` can read entries through
/// `find_last_undone_addition` without forcing the inner fields public.
pub(super) struct AuditEntry {
    pub op: String,
    pub pattern: String,
    pub tool_use_id: String,
    pub session_id: String,
    pub agent_type: Option<String>,
    pub target: AuditTarget,
}

impl AuditEntry {
    pub(super) fn op_str(&self) -> &str {
        &self.op
    }
}

/// Append one JSONL line to the audit log.
///
/// New format: one JSON object per line, `\n`-terminated.  Free escaping —
/// patterns containing `\t` or `\n` round-trip correctly (unlike the old TSV).
pub(super) fn append_audit_entry(
    log_path: &Path,
    op: &str,
    pattern: &str,
    metadata: &AdditionMetadata,
    target: &AuditTarget,
) -> Result<()> {
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let row = AuditLogRow {
        ts: utc_now_iso8601(),
        op: op.to_owned(),
        pattern: pattern.to_owned(),
        tool_use_id: metadata.tool_use_id.clone(),
        session_id: crate::util::short_session_id(&metadata.session_id),
        agent: metadata.agent_type.clone(),
        target: target.clone(),
    };

    let mut json = serde_json::to_vec(&row).with_context(|| "serialise audit log row")?;
    json.push(b'\n');

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| format!("open audit log {}", log_path.display()))?;

    file.write_all(&json)
        .with_context(|| format!("write audit log {}", log_path.display()))?;

    Ok(())
}

/// Find the most-recent `added` line in the audit log that has no subsequent
/// `undone` line for the same pattern + tool_use_id pair.
///
/// Handles mixed files: new JSONL lines (starting with `{`) and legacy TSV
/// lines (starting with a year digit, e.g. `2026-`) are both parsed correctly.
///
/// Legacy 7-col TSV: `{ts}\t{op}\t{pattern}\t{tool_use_id}\t{session}\t{agent}\t{target}`
/// Legacy 6-col TSV: same without the target column → `AuditTarget::UserGlobal`.
///
/// # Re-Always after undo correctness
///
/// When the same `(pattern, tool_use_id)` pair appears in the log as:
///   `added → undone → added`
/// the second `added` is the most-recent un-undone entry and must be returned.
/// We use a `HashMap<key, usize>` counter instead of a `HashSet`: walking in
/// reverse, `undone` increments the counter and `added` decrements it — the
/// first `added` whose counter is already 0 is the un-undone one.
pub(super) fn find_last_undone_addition(log_path: &Path) -> Result<Option<AuditEntry>> {
    if !log_path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(log_path)
        .with_context(|| format!("read {}", log_path.display()))?;

    // Walk lines in reverse.  `undo_counts` maps (pattern, tool_use_id) →
    // number of subsequent `undone` lines seen so far (reverse order).
    let mut undo_counts: std::collections::HashMap<(String, String), usize> =
        std::collections::HashMap::new();
    let mut result: Option<AuditEntry> = None;

    for line in text.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let entry = if trimmed.starts_with('{') {
            // JSONL line — parse via serde.
            match serde_json::from_str::<AuditLogRow>(trimmed) {
                Ok(row) => row.into_entry(),
                Err(e) => {
                    tracing::warn!("audit log: failed to parse JSONL line: {e} — skipping");
                    continue;
                }
            }
        } else {
            // Legacy TSV line — parse manually.
            match parse_tsv_audit_line(trimmed) {
                Some(e) => e,
                None => {
                    tracing::warn!("audit log: unrecognised line format — skipping");
                    continue;
                }
            }
        };

        let key = (entry.pattern.clone(), entry.tool_use_id.clone());

        match entry.op_str() {
            "undone" => {
                *undo_counts.entry(key).or_insert(0) += 1;
            }
            "added" => {
                let count = undo_counts.entry(key.clone()).or_insert(0);
                if *count > 0 {
                    // This added is "balanced" by a subsequent undone — skip.
                    *count -= 1;
                } else if matches!(entry.target, AuditTarget::Unknown) {
                    // Forward-compat sentinel: a newer daemon wrote a target
                    // we don't understand. Keep walking back — undoing this
                    // line would mean operating on a settings file we
                    // couldn't identify.
                    tracing::warn!(
                        pattern = %entry.pattern,
                        "audit log: skipping addition with unknown target type"
                    );
                    continue;
                } else {
                    // No pending undone for this key — this is the most-recent
                    // un-undone addition.
                    result = Some(entry);
                    break;
                }
            }
            _ => {}
        }
    }

    Ok(result)
}

/// Parse a legacy TSV audit line into an `AuditEntry`.
///
/// Returns `None` for lines with fewer than 3 tab-separated fields.
fn parse_tsv_audit_line(line: &str) -> Option<AuditEntry> {
    // splitn(7) — up to 7 columns; 7th may be absent (6-col legacy lines).
    let cols: Vec<&str> = line.splitn(7, '\t').collect();
    if cols.len() < 3 {
        return None;
    }
    let op = cols[1].to_owned();
    let pattern = cols[2].to_owned();
    let tool_use_id = cols.get(3).copied().unwrap_or("").to_owned();
    let session_id = cols.get(4).copied().unwrap_or("").to_owned();
    let agent_type = cols
        .get(5)
        .copied()
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    // Parse target column (col 6); missing/unknown → UserGlobal.
    let target = {
        let raw = cols.get(6).copied().unwrap_or("user");
        if let Some(path) = raw.strip_prefix("project:") {
            AuditTarget::ProjectLocal {
                root: std::path::PathBuf::from(path),
            }
        } else {
            AuditTarget::UserGlobal
        }
    };

    Some(AuditEntry {
        op,
        pattern,
        tool_use_id,
        session_id,
        agent_type,
        target,
    })
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

fn utc_now_iso8601() -> String {
    // Simple: seconds-since-epoch formatted as ISO 8601 UTC.
    // No chrono dep — same approach as the calendar math in jsonl.rs.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, mo, d) = crate::ingest::jsonl::days_to_ymd(secs / 86400);
    let rem = secs % 86400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}
