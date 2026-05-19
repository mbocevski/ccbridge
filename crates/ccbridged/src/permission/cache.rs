// SPDX-License-Identifier: MIT
//! Per-project allowlist cache.
//!
//! Loads and caches per-project allowlists lazily on first encounter of each
//! project root, then cascades them with the user-global allowlist to produce
//! the effective `Allowlist` used by `evaluate()`.
//!
//! # Concurrency
//!
//! The user allowlist is behind an [`ArcSwap`] (lock-free reads, swapped by
//! the user-file watcher).  Per-project entries are in a [`Mutex<HashMap>`];
//! the lock is held only while looking up or inserting the entry — the
//! expensive `cascade()` call happens with the lock released.
//!
//! # Precondition
//!
//! [`ProjectAllowlistCache::cascade_for`] must be called from within a tokio
//! runtime because it may spawn notify watcher tasks via [`spawn_settings_watcher`].
//! The aggregator task satisfies this precondition.  Use `#[tokio::test]` for
//! any test that triggers a cache miss.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use super::allowlist::Allowlist;
use super::project::find_project_root;
use super::spawn_settings_watcher;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Lazily-populated cache of per-project allowlists, cascaded with the shared
/// user-global allowlist.
pub struct ProjectAllowlistCache {
    /// User-global allowlist (`~/.claude/settings.json`).
    /// Also held by the user-file watcher, which swaps it on change.
    user: Arc<ArcSwap<Allowlist>>,

    /// Per-project entries keyed by project root path.
    projects: Mutex<HashMap<PathBuf, ProjectEntry>>,
}

struct ProjectEntry {
    /// `<project_root>/.claude/settings.json`
    project: Arc<ArcSwap<Allowlist>>,
    /// `<project_root>/.claude/settings.local.json`
    local: Arc<ArcSwap<Allowlist>>,
    /// Held to keep watcher tasks alive (dropped when cache entry is dropped).
    _project_watcher: tokio::task::JoinHandle<()>,
    _local_watcher: tokio::task::JoinHandle<()>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl ProjectAllowlistCache {
    /// Construct the cache with a shared user-global allowlist handle.
    ///
    /// The caller must also pass `user` to [`spawn_settings_watcher`] so the
    /// user-file watcher can swap it on change — this cache holds a clone of
    /// the same handle.
    pub fn new(user: Arc<ArcSwap<Allowlist>>) -> Self {
        Self {
            user,
            projects: Mutex::new(HashMap::new()),
        }
    }

    /// Compute the effective cascade allowlist for an event's `cwd`.
    ///
    /// Returns a freshly-built `Allowlist` merging `local → project → user`.
    /// If `cwd` has no detectable project root, returns the user allowlist
    /// cascaded over two empty sources (identical result to user-only, uniform
    /// code path).
    ///
    /// Lazily loads project files and spawns two notify watchers on the first
    /// encounter of each project root.  Subsequent calls for the same root are
    /// a `Mutex` lock + `HashMap` lookup + two `Arc` clones before the lock is
    /// released, followed by a lock-free cascade construction.
    ///
    /// # Precondition
    ///
    /// Must be called from within a tokio runtime.  See module-level doc.
    pub fn cascade_for(&self, cwd: &Path) -> Allowlist {
        let root = match find_project_root(cwd) {
            None => {
                return Allowlist::cascade(
                    &Allowlist::empty(),
                    &Allowlist::empty(),
                    self.user.load().as_ref(),
                );
            }
            Some(r) => r,
        };

        // Acquire lock only long enough to get Arc handles — not during cascade.
        let (local_handle, project_handle) = {
            let mut guard = self.projects.lock().unwrap();
            let entry = guard.entry(root.clone()).or_insert_with(|| {
                let proj_path = root.join(".claude").join("settings.json");
                let local_path = root.join(".claude").join("settings.local.json");

                let project_al = Arc::new(ArcSwap::new(Arc::new(
                    Allowlist::from_path(&proj_path).unwrap_or_default(),
                )));
                let local_al = Arc::new(ArcSwap::new(Arc::new(
                    Allowlist::from_path(&local_path).unwrap_or_default(),
                )));

                let ph = spawn_settings_watcher(proj_path, project_al.clone());
                let lh = spawn_settings_watcher(local_path, local_al.clone());

                tracing::debug!(
                    project_root = %root.display(),
                    "allowlist cache: loaded project-local allowlists + spawned watchers",
                );

                ProjectEntry {
                    project: project_al,
                    local: local_al,
                    _project_watcher: ph,
                    _local_watcher: lh,
                }
            });
            (Arc::clone(&entry.local), Arc::clone(&entry.project))
        };
        // Lock released before cascade — no blocking while iterating patterns.

        Allowlist::cascade(
            local_handle.load().as_ref(),
            project_handle.load().as_ref(),
            self.user.load().as_ref(),
        )
    }

    /// Number of project roots currently cached.  Test helper.
    #[cfg(test)]
    pub fn cached_project_count(&self) -> usize {
        self.projects.lock().unwrap().len()
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

    fn user_with_allow(patterns: &[&str]) -> Arc<ArcSwap<Allowlist>> {
        let al = Allowlist {
            allow: patterns
                .iter()
                .map(|s| super::super::pattern::Pattern::parse(s))
                .collect(),
            deny: vec![],
        };
        Arc::new(ArcSwap::new(Arc::new(al)))
    }

    fn write_settings(dir: &Path, filename: &str, allow: &[&str], deny: &[&str]) {
        let v = json!({
            "permissions": {
                "allow": allow,
                "deny":  deny
            }
        });
        std::fs::write(dir.join(filename), serde_json::to_string(&v).unwrap()).unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 1 — sync (no watchers spawned, cwd has no project root)
    // -----------------------------------------------------------------------

    #[test]
    fn cascade_for_unknown_cwd_returns_user_only() {
        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user);

        // A path that provably has no .claude/.git ancestors.
        let cascade = cache.cascade_for(Path::new("/nonexistent-ccbridge-test-xyz/sub"));

        let allow_raws: Vec<&str> = cascade.allow.iter().map(|p| p.raw()).collect();
        assert!(allow_raws.contains(&"Skill"), "user allow must be present");
        assert!(cascade.deny.is_empty(), "no deny patterns expected");
    }

    // -----------------------------------------------------------------------
    // Tests 2–6 — async (cache miss triggers watcher spawn)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cascade_for_project_loads_both_files() {
        let dir = TempDir::new().unwrap();
        let claude_dir = dir.path().join(".claude");
        std::fs::create_dir(&claude_dir).unwrap();

        write_settings(&claude_dir, "settings.json", &[], &["Bash(rm:*)"]);
        write_settings(
            &claude_dir,
            "settings.local.json",
            &["Bash(echo test)"],
            &[],
        );

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user);

        let cascade = cache.cascade_for(dir.path());

        let allow_raws: Vec<&str> = cascade.allow.iter().map(|p| p.raw()).collect();
        let deny_raws: Vec<&str> = cascade.deny.iter().map(|p| p.raw()).collect();

        assert!(
            allow_raws.contains(&"Bash(echo test)"),
            "local allow must be present"
        );
        assert!(allow_raws.contains(&"Skill"), "user allow must be present");
        assert!(
            deny_raws.contains(&"Bash(rm:*)"),
            "project deny must be present"
        );
    }

    #[tokio::test]
    async fn cascade_for_project_with_only_local() {
        let dir = TempDir::new().unwrap();
        let claude_dir = dir.path().join(".claude");
        std::fs::create_dir(&claude_dir).unwrap();

        // Only settings.local.json — no settings.json.
        write_settings(&claude_dir, "settings.local.json", &["Bash(echo hi)"], &[]);

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user);

        let cascade = cache.cascade_for(dir.path());

        let allow_raws: Vec<&str> = cascade.allow.iter().map(|p| p.raw()).collect();
        assert!(
            allow_raws.contains(&"Bash(echo hi)"),
            "local allow must be present"
        );
        assert!(allow_raws.contains(&"Skill"), "user allow must be present");
    }

    #[tokio::test]
    async fn cascade_for_project_with_no_files() {
        let dir = TempDir::new().unwrap();
        // .claude/ exists but no settings files.
        std::fs::create_dir(dir.path().join(".claude")).unwrap();

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user);

        let cascade = cache.cascade_for(dir.path());

        let allow_raws: Vec<&str> = cascade.allow.iter().map(|p| p.raw()).collect();
        assert_eq!(allow_raws, vec!["Skill"], "only user allow expected");
        assert!(cascade.deny.is_empty());
    }

    #[tokio::test]
    async fn cascade_for_caches_entry() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();

        let user = user_with_allow(&[]);
        let cache = ProjectAllowlistCache::new(user);

        cache.cascade_for(dir.path());
        cache.cascade_for(dir.path());

        assert_eq!(
            cache.cached_project_count(),
            1,
            "same project root must only produce one cache entry"
        );
    }

    #[tokio::test]
    async fn evaluate_uses_project_local_deny() {
        use ccbridge_proto::hook::{HookBase, PermissionMode, PreToolUseEvent};

        let dir = TempDir::new().unwrap();
        let claude_dir = dir.path().join(".claude");
        std::fs::create_dir(&claude_dir).unwrap();

        // Project-local deny: Bash(npm test)
        write_settings(&claude_dir, "settings.local.json", &[], &["Bash(npm test)"]);

        let user = user_with_allow(&["Skill"]); // user has no Bash deny
        let cache = ProjectAllowlistCache::new(user);

        let event = PreToolUseEvent {
            base: HookBase {
                session_id: "sess".to_owned(),
                transcript_path: "/tmp/t.jsonl".to_owned(),
                cwd: dir.path().to_str().unwrap().to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: "Bash".to_owned(),
            tool_input: serde_json::json!({"command": "npm test"}),
            tool_use_id: "toolu_eval_01".to_owned(),
            agent_id: None,
            agent_type: None,
        };

        let cascade = cache.cascade_for(Path::new(&event.base.cwd));
        let decision = super::super::evaluate(&event, &cascade);

        assert!(
            matches!(decision, super::super::Decision::Deny { .. }),
            "project-local deny must flow through evaluate() as Deny, got {decision:?}"
        );
    }
}
