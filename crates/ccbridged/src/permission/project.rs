// SPDX-License-Identifier: MIT
//! Project-root detection for project-local allowlist support.
//!
//! Walks up the directory tree from a given `cwd` to find the nearest ancestor
//! that looks like a "project" — either a Claude Code project (has `.claude/`)
//! or any git project (has `.git`).
//!
//! Used by the aggregator and the Always writer to scope allowlist patterns
//! to the correct project rather than writing them globally.

use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public function
// ---------------------------------------------------------------------------

/// Walk up from `cwd` to find the nearest ancestor containing `.claude/`
/// (preferred) or `.git` (fallback).
///
/// Returns `Some(project_root)` or `None` if neither marker is found up to
/// the filesystem root.
///
/// # Ordering
///
/// At each directory level, `.claude/` is checked **before** `.git`.  When
/// both exist at the same level, `.claude/` wins — it signals an explicit
/// Claude Code project setup, which is more specific than a generic git repo.
///
/// `.git` can be a file (git worktree) or a directory; [`Path::exists`]
/// matches both.
///
/// # I/O
///
/// Makes filesystem stat calls (is_dir / exists) but no I/O beyond that.
/// The result is stable for the lifetime of a Claude Code session (cwd
/// doesn't change), so callers should cache it per-session.
pub fn find_project_root(cwd: &Path) -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    find_project_root_with_home(cwd, home.as_deref())
}

/// Same as [`find_project_root`] but takes `home` explicitly so tests don't
/// need to mutate `$HOME`.
fn find_project_root_with_home(cwd: &Path, home: Option<&Path>) -> Option<PathBuf> {
    let mut p: &Path = cwd;
    loop {
        // Never treat $HOME itself as a project root — its `.claude/` is the
        // user-global config dir, not a project marker.
        if home == Some(p) {
            return None;
        }
        if p.join(".claude").is_dir() {
            return Some(p.to_path_buf());
        }
        if p.join(".git").exists() {
            return Some(p.to_path_buf());
        }
        match p.parent() {
            Some(parent) => p = parent,
            None => return None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn find_project_root_with_dotclaude() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();

        let result = find_project_root(dir.path());
        assert_eq!(result.as_deref(), Some(dir.path()));
    }

    #[test]
    fn find_project_root_with_dotgit() {
        let dir = TempDir::new().unwrap();
        // Simulate a git worktree: .git is a *file*, not a directory.
        std::fs::write(dir.path().join(".git"), "gitdir: ../../.git").unwrap();

        let result = find_project_root(dir.path());
        assert_eq!(result.as_deref(), Some(dir.path()));
    }

    #[test]
    fn find_project_root_dotclaude_beats_dotgit_same_level() {
        // Both markers at the same directory level: .claude/ should win.
        // Functionally identical (both return the same dir), but this
        // test documents that we check .claude/ first.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();
        std::fs::write(dir.path().join(".git"), "gitdir: other").unwrap();

        let result = find_project_root(dir.path());
        // Both markers exist at the same level; we expect the root dir
        // (since .claude/ is checked first and we return immediately).
        assert_eq!(result.as_deref(), Some(dir.path()));
    }

    #[test]
    fn find_project_root_dotclaude_beats_dotgit_different_levels() {
        // .git in parent, .claude/ in child — child should be returned
        // (closest marker wins; .claude/ beats .git even across levels
        // when .claude/ is nearer).
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".git"), "gitdir: other").unwrap();

        let child = dir.path().join("subproject");
        std::fs::create_dir_all(&child).unwrap();
        std::fs::create_dir(child.join(".claude")).unwrap();

        let result = find_project_root(&child);
        assert_eq!(result.as_deref(), Some(child.as_path()));
    }

    #[test]
    fn find_project_root_returns_none_when_no_markers() {
        // Non-existent absolute path: every `.join("...").is_dir()/.exists()`
        // call up the chain returns false, so the walk terminates at `/` → None.
        let result = find_project_root(Path::new("/nonexistent-ccbridge-test-path-xyz/sub"));
        assert!(
            result.is_none(),
            "no markers anywhere → expected None, got {result:?}",
        );
    }

    #[test]
    fn find_project_root_skips_home_dotclaude() {
        // Regression: cwd outside any project, with $HOME containing the
        // user-global ~/.claude/ dir.  The walker must NOT treat $HOME as
        // a project root just because of the user-global config dir.
        let home = TempDir::new().unwrap();
        std::fs::create_dir(home.path().join(".claude")).unwrap();
        let cwd = home.path().join("dev").join("tmp");
        std::fs::create_dir_all(&cwd).unwrap();

        let result = find_project_root_with_home(&cwd, Some(home.path()));
        assert!(
            result.is_none(),
            "$HOME with .claude/ must not be treated as a project root, got {result:?}",
        );
    }

    #[test]
    fn find_project_root_returns_ancestor_not_descendant() {
        // .claude/ lives at an ancestor level, not at cwd itself.
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();

        let src = dir.path().join("src").join("main");
        std::fs::create_dir_all(&src).unwrap();

        let result = find_project_root(&src);
        assert_eq!(
            result.as_deref(),
            Some(dir.path()),
            "must return the ancestor that has .claude/, not the starting dir"
        );
    }
}
