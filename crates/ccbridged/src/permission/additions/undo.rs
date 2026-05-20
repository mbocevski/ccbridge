// SPDX-License-Identifier: MIT
//! `undo-last-allow` CLI dispatch + the security checks that protect it.
//!
//! Removes the most-recent un-undone allowlist addition recorded in the
//! audit log, and validates the project root before touching any settings
//! file — a tampered audit line must not be able to redirect us at the
//! user-global config dir.

use std::path::Path;

use anyhow::{Context, Result};

use super::audit_log::{
    append_audit_entry, find_last_undone_addition, AdditionMetadata,
};
use super::target::AuditTarget;
use crate::setup::{load_settings, save_settings};

/// Outcome of [`undo_last_allow`].
///
/// The caller is responsible for printing / logging as appropriate.
#[derive(Debug, PartialEq, Eq)]
pub enum UndoOutcome {
    /// Pattern was found and removed.
    Removed {
        pattern: String,
        file: std::path::PathBuf,
    },
    /// Pattern was not in the target file (manually removed before undo).
    AlreadyGone {
        pattern: String,
        file: std::path::PathBuf,
    },
    /// The target settings file itself is absent.
    FileMissing {
        pattern: String,
        file: std::path::PathBuf,
    },
}

/// Validate that an `AuditTarget::ProjectLocal` root is safe to use for undo.
///
/// Checks:
/// 1. `root` must be absolute — rejects relative paths injected via log tampering.
/// 2. `root` must not be `/` — writing to `/.claude/` is nonsensical and dangerous.
/// 3. The best-effort canonical `root` (skipped if directory doesn't exist yet)
///    must not equal `user_global_dir` — blocks attacks where the root points at
///    the user-global config directory, which would trigger a write to
///    `~/.claude/settings.local.json` instead of the expected project-local file,
///    silently modifying the user's global config.
///
/// `user_global_dir` should be `permission::settings_path().parent()`.
///
/// # Errors
/// Returns `Err` with a message naming the root and the violated rule.
pub fn validate_audit_root(root: &Path, user_global_dir: &Path) -> Result<()> {
    if !root.is_absolute() {
        anyhow::bail!(
            "audit log contains a relative project root {:?} — refusing undo (log may be tampered)",
            root
        );
    }
    if root == Path::new("/") {
        anyhow::bail!("audit log contains filesystem root '/' as project root — refusing undo");
    }
    // Canonicalise when the directory exists; otherwise fall back to a
    // purely lexical normalisation so paths like `/home/user/.claude/.`
    // or `/home/user/foo/../.claude` collapse before the collision check.
    // Without this, a tampered audit line can bypass the user-global
    // collision check until the directory is materialised, then start
    // writing to ~/.claude/settings.local.json.
    let canonical_root = if root.exists() {
        root.canonicalize()
            .with_context(|| format!("canonicalize {:?}", root))?
    } else {
        lexically_normalize(root)
    };
    let canonical_ugd = if user_global_dir.exists() {
        user_global_dir
            .canonicalize()
            .with_context(|| format!("canonicalize {:?}", user_global_dir))?
    } else {
        lexically_normalize(user_global_dir)
    };
    if canonical_root == canonical_ugd {
        anyhow::bail!(
            "audit log project root {:?} collides with user-global config dir {:?} — \
             refusing undo to prevent unintended modification of user-global settings",
            root,
            user_global_dir
        );
    }
    Ok(())
}

/// Collapse `.` and `..` components in `path` without touching the
/// filesystem. Used as a fallback when `canonicalize` can't run because
/// the directory doesn't exist yet.
///
/// `..` past the root (or before any normal component) is silently dropped;
/// in security-validation contexts that's the conservative choice — the
/// caller is checking equality with a known-good prefix, and an
/// over-popped path won't accidentally match it.
pub(super) fn lexically_normalize(path: &Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir => out.push(comp),
            Component::CurDir => {} // drop "."
            Component::ParentDir => {
                // Pop the last Normal component if there is one; otherwise
                // drop (don't escape past the root we've already pushed).
                let popped = out
                    .components()
                    .next_back()
                    .map(|c| matches!(c, Component::Normal(_)));
                if popped == Some(true) {
                    out.pop();
                }
            }
            Component::Normal(c) => out.push(c),
        }
    }
    out
}

/// Remove the most-recent un-undone allowlist addition and mark it as undone.
///
/// The target file (project-local or user-global) is read from the audit log,
/// so the caller doesn't need to pass a settings path.
///
/// Returns [`UndoOutcome`] describing what happened; the caller is responsible
/// for printing user-facing messages.  Returns `Err` only on I/O failure, log
/// parse error, empty log, or audit root validation failure.
pub fn undo_last_allow(audit_log_path: &Path) -> Result<UndoOutcome> {
    let entry = find_last_undone_addition(audit_log_path)
        .context("reading audit log")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no allowlist additions in audit log to undo ({})",
                audit_log_path.display()
            )
        })?;

    // Resolve path from the target stored in the audit log.
    let settings_path = match &entry.target {
        AuditTarget::ProjectLocal { root } => {
            // Validate the root before constructing any path from it.
            let user_global_dir = crate::permission::settings_path()
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from("/"));
            validate_audit_root(root, &user_global_dir)
                .with_context(|| format!("invalid project root in audit log: {:?}", root))?;
            root.join(".claude").join("settings.local.json")
        }
        AuditTarget::UserGlobal => crate::permission::settings_path(),
        AuditTarget::Unknown => {
            anyhow::bail!(
                "audit log entry has an unknown target type — likely written by a \
                 newer ccbridged version after downgrade. Refusing to undo."
            );
        }
    };

    let outcome = if !settings_path.exists() {
        UndoOutcome::FileMissing {
            pattern: entry.pattern.clone(),
            file: settings_path.clone(),
        }
    } else {
        let mut settings = load_settings(&settings_path)
            .with_context(|| format!("read {}", settings_path.display()))?;

        let allow_arr = settings
            .get_mut("permissions")
            .and_then(|p| p.get_mut("allow"))
            .and_then(|a| a.as_array_mut());

        match allow_arr {
            None => UndoOutcome::AlreadyGone {
                pattern: entry.pattern.clone(),
                file: settings_path.clone(),
            },
            Some(arr) => {
                let before = arr.len();
                arr.retain(|v| v.as_str() != Some(&entry.pattern));
                if arr.len() == before {
                    UndoOutcome::AlreadyGone {
                        pattern: entry.pattern.clone(),
                        file: settings_path.clone(),
                    }
                } else {
                    save_settings(&settings_path, &settings)
                        .with_context(|| format!("write {}", settings_path.display()))?;
                    UndoOutcome::Removed {
                        pattern: entry.pattern.clone(),
                        file: settings_path.clone(),
                    }
                }
            }
        }
    };

    // Record the undo in the audit log regardless of outcome.
    append_audit_entry(
        audit_log_path,
        "undone",
        &entry.pattern,
        &AdditionMetadata {
            tool_use_id: entry.tool_use_id,
            session_id: entry.session_id,
            agent_type: entry.agent_type,
        },
        &entry.target,
    )?;

    Ok(outcome)
}
