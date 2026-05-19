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
//! the user-file watcher).  Per-project entries are in a [`Mutex<LruCache>`];
//! the lock is held only while looking up or inserting the entry — the
//! expensive `cascade()` call happens with the lock released.
//!
//! # LRU bound
//!
//! The cache is capped at [`LRU_CAPACITY`] entries.  Each entry holds two
//! inotify watchers (project and local settings files); evicting an entry
//! explicitly aborts both tasks to release the inotify fds promptly rather
//! than waiting for them to self-terminate.
//!
//! # Precondition
//!
//! [`ProjectAllowlistCache::cascade_for`] must be called from within a tokio
//! runtime because it may spawn notify watcher tasks via [`spawn_settings_watcher`].
//! The aggregator task satisfies this precondition.  Use `#[tokio::test]` for
//! any test that triggers a cache miss.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use lru::LruCache;

use super::allowlist::Allowlist;
use super::project::find_project_root_with_home;
use super::spawn_settings_watcher;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of project roots held in the LRU cache simultaneously.
///
/// Each entry holds 2 inotify watchers (project + local settings files).
/// At capacity = 64 the daemon uses at most 128 inotify watches out of the
/// typical `fs.inotify.max_user_watches = 8192` kernel default.
const LRU_CAPACITY: usize = 64;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Lazily-populated LRU cache of per-project allowlists, cascaded with the
/// shared user-global allowlist.
pub struct ProjectAllowlistCache {
    /// User-global allowlist (`~/.claude/settings.json`).
    /// Also held by the user-file watcher, which swaps it on change.
    user: Arc<ArcSwap<Allowlist>>,

    /// Per-project entries keyed by project root path, bounded by LRU_CAPACITY.
    projects: Mutex<LruCache<PathBuf, ProjectEntry>>,

    /// `$HOME` resolved once at startup — avoids a per-call `std::env::var_os`.
    /// Passed to `find_project_root_with_home` to skip `$HOME/.claude/` as a
    /// project root marker.
    home: Option<PathBuf>,
}

struct ProjectEntry {
    /// `<project_root>/.claude/settings.json`
    project: Arc<ArcSwap<Allowlist>>,
    /// `<project_root>/.claude/settings.local.json`
    local: Arc<ArcSwap<Allowlist>>,
    /// Watcher tasks — aborted on eviction to release inotify fds promptly.
    project_watcher: tokio::task::JoinHandle<()>,
    local_watcher: tokio::task::JoinHandle<()>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl ProjectAllowlistCache {
    /// Construct the cache with a shared user-global allowlist handle.
    ///
    /// `home` is the user's home directory resolved once at startup — passed to
    /// `find_project_root_with_home` to skip `$HOME/.claude/` as a project root.
    /// Pass `None` to fall back to per-call `std::env::var_os("HOME")` (old behaviour).
    ///
    /// The caller must also pass `user` to [`spawn_settings_watcher`] so the
    /// user-file watcher can swap it on change — this cache holds a clone of
    /// the same handle.
    pub fn new(user: Arc<ArcSwap<Allowlist>>, home: Option<PathBuf>) -> Self {
        Self {
            user,
            projects: Mutex::new(LruCache::new(
                NonZeroUsize::new(LRU_CAPACITY).expect("LRU_CAPACITY > 0"),
            )),
            home,
        }
    }

    /// Construct the cache with an explicit capacity.  Test helper.
    #[cfg(test)]
    fn with_capacity(user: Arc<ArcSwap<Allowlist>>, capacity: usize) -> Self {
        Self {
            user,
            projects: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("capacity > 0"),
            )),
            home: std::env::var_os("HOME").map(PathBuf::from),
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
    /// a `Mutex` lock + LRU lookup (which also promotes the entry) + two `Arc`
    /// clones before the lock is released, followed by a lock-free cascade.
    ///
    /// When the cache is full, the least-recently-used entry is evicted and
    /// its watcher tasks are aborted before the new entry is inserted.
    ///
    /// # Precondition
    ///
    /// Must be called from within a tokio runtime.  See module-level doc.
    pub fn cascade_for(&self, cwd: &Path) -> Allowlist {
        let root = match find_project_root_with_home(cwd, self.home.as_deref()) {
            None => {
                // No project root — return user allowlist directly (item 5).
                return (*self.user.load_full()).clone();
            }
            Some(r) => r,
        };

        // Acquire lock only long enough to get Arc handles — not during cascade.
        let (local_handle, project_handle) = {
            let mut guard = self.projects.lock().unwrap();

            // `get` promotes the entry in LRU order (records the access).
            // `contains_key` would not promote; `get` is correct here.
            if let Some(entry) = guard.get(&root) {
                (Arc::clone(&entry.local), Arc::clone(&entry.project))
            } else {
                // Cache miss — load files and spawn watchers.
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

                let new_entry = ProjectEntry {
                    project: project_al.clone(),
                    local: local_al.clone(),
                    project_watcher: ph,
                    local_watcher: lh,
                };

                // If the cache is at capacity, peek the LRU entry for logging,
                // then insert (which evicts it and returns the evicted value).
                let at_cap = guard.len() == guard.cap().get();
                let evicted_root_for_log = if at_cap {
                    guard.peek_lru().map(|(k, _)| k.clone())
                } else {
                    None
                };

                if let Some(evicted) = guard.put(root, new_entry) {
                    evicted.project_watcher.abort();
                    evicted.local_watcher.abort();
                    if let Some(ref evicted_root) = evicted_root_for_log {
                        tracing::debug!(
                            evicted_root = %evicted_root.display(),
                            "allowlist cache: evicted LRU entry, aborted watchers",
                        );
                    }
                }

                (local_al, project_al)
            }
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

    /// Returns `true` if `root` is currently in the cache.  Test helper.
    #[cfg(test)]
    fn contains(&self, root: &Path) -> bool {
        self.projects.lock().unwrap().contains(root)
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

    /// Create a TempDir with a `.claude/` subdirectory and return both.
    fn make_project() -> TempDir {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();
        dir
    }

    // -----------------------------------------------------------------------
    // Test 1 — sync (no watchers spawned, cwd has no project root)
    // -----------------------------------------------------------------------

    #[test]
    fn cascade_for_unknown_cwd_returns_user_only() {
        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user, None);

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
        let dir = make_project();
        let claude_dir = dir.path().join(".claude");

        write_settings(&claude_dir, "settings.json", &[], &["Bash(rm:*)"]);
        write_settings(
            &claude_dir,
            "settings.local.json",
            &["Bash(echo test)"],
            &[],
        );

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user, None);

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
        let dir = make_project();
        let claude_dir = dir.path().join(".claude");

        // Only settings.local.json — no settings.json.
        write_settings(&claude_dir, "settings.local.json", &["Bash(echo hi)"], &[]);

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user, None);

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
        let dir = make_project();

        let user = user_with_allow(&["Skill"]);
        let cache = ProjectAllowlistCache::new(user, None);

        let cascade = cache.cascade_for(dir.path());

        let allow_raws: Vec<&str> = cascade.allow.iter().map(|p| p.raw()).collect();
        assert_eq!(allow_raws, vec!["Skill"], "only user allow expected");
        assert!(cascade.deny.is_empty());
    }

    #[tokio::test]
    async fn cascade_for_caches_entry() {
        let dir = make_project();

        let user = user_with_allow(&[]);
        let cache = ProjectAllowlistCache::new(user, None);

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

        let dir = make_project();
        let claude_dir = dir.path().join(".claude");

        // Project-local deny: Bash(npm test)
        write_settings(&claude_dir, "settings.local.json", &[], &["Bash(npm test)"]);

        let user = user_with_allow(&["Skill"]); // user has no Bash deny
        let cache = ProjectAllowlistCache::new(user, None);

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

    // -----------------------------------------------------------------------
    // LRU eviction tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cache_evicts_least_recently_used_entry() {
        // Capacity = 2: populate 3 distinct project roots, assert the first is evicted.
        let dir_a = make_project();
        let dir_b = make_project();
        let dir_c = make_project();

        let user = user_with_allow(&[]);
        let cache = ProjectAllowlistCache::with_capacity(user, 2);

        cache.cascade_for(dir_a.path()); // A inserted, LRU: [A]
        cache.cascade_for(dir_b.path()); // B inserted, LRU: [A, B]
        assert_eq!(cache.cached_project_count(), 2);

        cache.cascade_for(dir_c.path()); // C inserted, A evicted, LRU: [B, C]
        assert_eq!(cache.cached_project_count(), 2, "capacity must stay at 2");
        assert!(
            !cache.contains(dir_a.path()),
            "A must have been evicted (LRU)"
        );
        assert!(cache.contains(dir_b.path()), "B must still be in cache");
        assert!(cache.contains(dir_c.path()), "C must be in cache");
    }

    #[tokio::test]
    async fn cache_re_inserts_after_eviction() {
        // Evict A, then re-encounter it — must get a fresh entry.
        let dir_a = make_project();
        let dir_b = make_project();
        let dir_c = make_project();

        let user = user_with_allow(&[]);
        let cache = ProjectAllowlistCache::with_capacity(user, 2);

        cache.cascade_for(dir_a.path()); // A inserted
        cache.cascade_for(dir_b.path()); // B inserted; LRU: [A, B]
        cache.cascade_for(dir_c.path()); // C inserted; A evicted; LRU: [B, C]

        assert!(
            !cache.contains(dir_a.path()),
            "A must be evicted before re-insert"
        );

        // Re-encounter A — must succeed and produce a valid cascade.
        let cascade = cache.cascade_for(dir_a.path());
        assert!(cascade.allow.is_empty()); // no patterns in project; user also empty
        assert_eq!(cache.cached_project_count(), 2, "capacity must still be 2");
        assert!(cache.contains(dir_a.path()), "A must be back in cache");
    }

    #[tokio::test]
    async fn cache_lru_hit_promotes_entry() {
        // Access order: A, B, A (promotes A), then insert C → B should be evicted, not A.
        let dir_a = make_project();
        let dir_b = make_project();
        let dir_c = make_project();

        let user = user_with_allow(&[]);
        let cache = ProjectAllowlistCache::with_capacity(user, 2);

        cache.cascade_for(dir_a.path()); // A inserted, LRU: [A]
        cache.cascade_for(dir_b.path()); // B inserted, LRU: [A, B] — A is LRU
        cache.cascade_for(dir_a.path()); // A hit → promoted, LRU: [B, A] — B is now LRU
        cache.cascade_for(dir_c.path()); // C inserted → B evicted, LRU: [A, C]

        assert_eq!(cache.cached_project_count(), 2);
        assert!(
            cache.contains(dir_a.path()),
            "A must survive (was promoted before C inserted)"
        );
        assert!(
            !cache.contains(dir_b.path()),
            "B must be evicted (was LRU after A was promoted)"
        );
        assert!(cache.contains(dir_c.path()), "C must be in cache");
    }
}
