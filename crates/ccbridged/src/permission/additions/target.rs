// SPDX-License-Identifier: MIT
//! Where allowlist additions are written and where historic audit-log
//! entries point.

use serde::{Deserialize, Serialize};

/// Where a NEW allow pattern is written — always project-local.
///
/// `write_allow_pattern` writes to `<root>/.claude/settings.local.json`,
/// creating the `.claude/` directory if absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTarget {
    /// Project root directory.  The settings file is `<root>/.claude/settings.local.json`.
    pub root: std::path::PathBuf,
}

/// Where a HISTORIC audit-log entry pointed.
///
/// New entries are always `ProjectLocal`.  `UserGlobal` exists only for
/// backwards compatibility with 6-column audit logs written by daemons
/// predating the project-local rework (P3).
///
/// Serialises as an adjacently-tagged JSON value:
/// - `{"project_local": {"root": "/path/to/project"}}`
/// - `"user_global"`
/// - `"unknown"` — forward-compat sentinel for variants written by a
///   newer daemon that this binary doesn't understand. Persistent
///   audit-log state is read by older binaries after a downgrade, so a
///   missing variant must not hard-fail line parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditTarget {
    /// `<root>/.claude/settings.local.json`.
    ProjectLocal { root: std::path::PathBuf },
    /// `~/.claude/settings.json` — legacy 6-column audit lines only.
    UserGlobal,
    /// Unknown target written by a newer daemon. Operations that need a
    /// target (undo) refuse to act on this; the line itself still parses
    /// cleanly so adjacent entries remain readable.
    #[serde(other)]
    Unknown,
}

impl From<&WriteTarget> for AuditTarget {
    fn from(t: &WriteTarget) -> Self {
        AuditTarget::ProjectLocal {
            root: t.root.clone(),
        }
    }
}

/// Resolve the write target from a `cwd` path.
///
/// - If `find_project_root` finds an ancestor with `.claude/` or `.git`,
///   that ancestor becomes the project root.
/// - Otherwise `cwd` itself is used as the root (creates
///   `<cwd>/.claude/settings.local.json` on first write).
pub fn resolve_write_target(cwd: &std::path::Path) -> WriteTarget {
    let root =
        crate::permission::project::find_project_root(cwd).unwrap_or_else(|| cwd.to_path_buf());
    WriteTarget { root }
}
