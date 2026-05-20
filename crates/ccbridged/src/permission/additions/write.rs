// SPDX-License-Identifier: MIT
//! Write a derived pattern into `<root>/.claude/settings.local.json` and
//! record the addition in the audit log.

use std::path::Path;

use anyhow::{Context, Result};

use super::audit_log::{append_audit_entry, AdditionMetadata};
use super::target::{AuditTarget, WriteTarget};
use crate::setup::{load_settings, save_settings};

/// Append `pattern` to `<target.root>/.claude/settings.local.json` and
/// record the addition in the audit log.  Creates `.claude/` if absent.
///
/// Idempotent: if the pattern is already present, returns `Ok(())`.
pub fn write_allow_pattern(
    target: &WriteTarget,
    pattern: &str,
    audit_log_path: &Path,
    metadata: AdditionMetadata,
) -> Result<()> {
    let dir = target.root.join(".claude");
    std::fs::create_dir_all(&dir).with_context(|| format!("mkdir -p {}", dir.display()))?;
    let settings_path = dir.join("settings.local.json");

    let mut settings = load_settings(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;

    // Ensure settings["permissions"]["allow"] exists and is an array.
    // Guard against non-object root (e.g. corrupted settings file).
    if !settings.is_object() {
        anyhow::bail!(
            "settings file {} has unexpected root shape (expected JSON object, got {})",
            settings_path.display(),
            crate::setup::json_type_name(&settings),
        );
    }
    let perms = settings
        .as_object_mut()
        .unwrap()
        .entry("permissions")
        .or_insert_with(|| serde_json::json!({}));
    if !perms.is_object() {
        anyhow::bail!(
            "settings file {} has unexpected shape at .permissions (expected object)",
            settings_path.display(),
        );
    }
    let allow_arr = perms
        .as_object_mut()
        .unwrap()
        .entry("allow")
        .or_insert_with(|| serde_json::json!([]));

    if !allow_arr.is_array() {
        anyhow::bail!(
            "settings file {} has unexpected shape at .permissions.allow \
             (expected array, got {})",
            settings_path.display(),
            crate::setup::json_type_name(allow_arr),
        );
    }

    let arr = allow_arr.as_array_mut().unwrap();

    // Idempotency check.
    if arr.iter().any(|v| v.as_str() == Some(pattern)) {
        tracing::debug!(
            "pattern {:?} already present in allow list; skipping write",
            pattern
        );
        return Ok(());
    }

    arr.push(serde_json::Value::String(pattern.to_owned()));

    save_settings(&settings_path, &settings)
        .with_context(|| format!("write {}", settings_path.display()))?;

    // Append audit log entry.
    let audit_target = AuditTarget::from(target);
    append_audit_entry(audit_log_path, "added", pattern, &metadata, &audit_target)?;

    tracing::info!(
        pattern = %pattern,
        tool_use_id = %metadata.tool_use_id,
        session = %crate::util::short_session_id(&metadata.session_id),
        agent = ?metadata.agent_type,
        root = %target.root.display(),
        "allowlist: added pattern",
    );

    Ok(())
}
