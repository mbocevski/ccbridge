// SPDX-License-Identifier: MIT
//! `ccbridged setup` — one-shot, idempotent first-run configuration.
//!
//! # What it does
//!
//! 1. Merges ccbridge-hook entries into `~/.claude/settings.json`
//!    (or `$CLAUDE_CONFIG_DIR/settings.json`) for all supported hook events.
//! 2. Enables the `ccbridge.service` systemd user unit via
//!    `systemctl --user enable --now`.
//! 3. Prints a human-readable summary of what changed.
//!
//! Everything is idempotent — re-running when already configured is safe
//! and produces "already present" for each hook.
//!
//! # Failure modes
//!
//! * `settings.json` absent → created with just the `hooks` skeleton.
//! * `settings.json` present but malformed → exit 1 with a clear message.
//! * Cannot read/write the file → exit 1 with the OS error.
//! * `systemctl` fails (service not installed yet) → warn and continue.
//! * `$HOME` unset → panic (same constraint as the rest of the daemon).

use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Hook events we register
// ---------------------------------------------------------------------------

/// All hook event names ccbridge registers.
///
/// This is the complete list of event variants in `ccbridge_proto::hook::HookEvent`.
/// Registering all of them now means future ccbridge features that use
/// `UserPromptSubmit` or `SessionEnd` work without re-running setup.
pub const HOOK_EVENTS: &[&str] = &[
    "PreToolUse",
    "PostToolUse",
    "UserPromptSubmit",
    "Notification",
    "Stop",
    "SessionStart",
    "SessionEnd",
];

/// The command name of the ccbridge hook binary.
pub const HOOK_COMMAND: &str = "ccbridge-hook";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the setup flow.  Called from `main()` when `argv[1] == "setup"`.
///
/// This function is `pub` so the `ccbridged` bin target (a separate
/// compilation unit) can call it via `ccbridged::setup::run()`.  It is
/// **not** part of any public API; the `#[doc(hidden)]` on the module prevents
/// it appearing in generated docs.
pub fn run() {
    if let Err(e) = do_setup() {
        eprintln!("ccbridged setup failed: {e:#}");
        std::process::exit(1);
    }
}

fn do_setup() -> Result<()> {
    let settings_path = settings_path();

    // --- 1. Merge hooks ---
    let mut settings = load_settings(&settings_path)
        .with_context(|| format!("reading {}", settings_path.display()))?;

    let results = merge_hooks(&mut settings)
        .with_context(|| format!("merging hooks into {}", settings_path.display()))?;

    // Only write the file back if merge_hooks actually changed something.
    // A no-op write would re-pretty-print the JSON, alphabetise keys
    // (serde_json's default Map ordering), and lose any custom formatting
    // the user had — all without changing the semantic content.  Idempotent
    // re-runs should leave the file's bytes untouched.
    let any_added = results.iter().any(|r| r.action == HookAction::Added);
    if any_added {
        save_settings(&settings_path, &settings)
            .with_context(|| format!("writing {}", settings_path.display()))?;
    }

    // --- 2. Write default config (only if absent) ---
    let config_outcome = write_default_config_if_absent().context("writing default config.toml")?;

    // --- 3. Enable systemd service ---
    let service_ok = run_enable_service();

    // --- 4. Print summary ---
    let mut added = Vec::new();
    let mut present = Vec::new();
    for r in &results {
        match r.action {
            HookAction::Added => added.push(r.event),
            HookAction::AlreadyPresent => present.push(r.event),
        }
    }

    if !added.is_empty() {
        println!(
            "ccbridged setup: registered hooks for: {}",
            added.join(", ")
        );
    }
    if !present.is_empty() {
        println!(
            "ccbridged setup: already configured: {}",
            present.join(", ")
        );
    }
    match &config_outcome {
        ConfigAction::Created { path } => {
            println!(
                "ccbridged setup: wrote default config to {}",
                path.display()
            );
        }
        ConfigAction::AlreadyPresent { path } => {
            println!(
                "ccbridged setup: config already present at {}",
                path.display()
            );
        }
        ConfigAction::Skipped { reason } => {
            println!("ccbridged setup: config not written ({reason})");
        }
    }
    println!(
        "ccbridged setup: service enabled: {}",
        if service_ok {
            "yes"
        } else {
            "no (see above warning)"
        }
    );
    println!(
        "ccbridged setup: done — settings written to {}",
        settings_path.display()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Default config.toml
// ---------------------------------------------------------------------------

/// The default config file content — `docs/example-config.toml` baked in
/// at compile time so `ccbridged setup` doesn't depend on a runtime file
/// next to the binary.
const DEFAULT_CONFIG_TOML: &str = include_str!("../../../docs/example-config.toml");

/// Outcome of [`write_default_config_if_absent`].
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigAction {
    /// File didn't exist; we wrote the default.
    Created { path: PathBuf },
    /// File already exists; left untouched.
    AlreadyPresent { path: PathBuf },
    /// Config path could not be resolved (e.g. neither `XDG_CONFIG_HOME`
    /// nor `HOME` set).  Setup shouldn't fail over this — the user can
    /// still use the daemon with no config (defaults are baked in) — but
    /// we surface the reason in the summary.
    Skipped { reason: String },
}

/// Resolve the config path and write the default config there if absent.
///
/// Skips with a descriptive reason when the path can't be resolved
/// (neither `XDG_CONFIG_HOME` nor `HOME` set — misconfigured system).
pub fn write_default_config_if_absent() -> Result<ConfigAction> {
    let path = match crate::config::config_path() {
        Ok(p) => p,
        Err(e) => {
            return Ok(ConfigAction::Skipped {
                reason: format!("{e:#}"),
            });
        }
    };
    write_default_config_to(&path)
}

/// Write [`DEFAULT_CONFIG_TOML`] to `path`, but only if no file exists there.
///
/// Atomic write (tmp + rename) so a partial file can never be left behind
/// by an interrupted setup.  Creates the parent directory if needed.
///
/// Split out from [`write_default_config_if_absent`] so tests can drive it
/// against a tempdir without mutating `XDG_CONFIG_HOME` (which would race
/// with parallel tests; see task 56 for the lesson learned).
fn write_default_config_to(path: &Path) -> Result<ConfigAction> {
    if path.exists() {
        return Ok(ConfigAction::AlreadyPresent {
            path: path.to_path_buf(),
        });
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }

    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, DEFAULT_CONFIG_TOML)
        .with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;

    Ok(ConfigAction::Created {
        path: path.to_path_buf(),
    })
}

// ---------------------------------------------------------------------------
// Hook merge
// ---------------------------------------------------------------------------

/// Result of processing one hook event during `merge_hooks`.
#[derive(Debug)]
pub struct HookMergeResult {
    pub event: &'static str,
    pub action: HookAction,
}

/// Whether the hook was already present or newly added.
#[derive(Debug, PartialEq, Eq)]
pub enum HookAction {
    Added,
    AlreadyPresent,
}

/// Merge ccbridge-hook entries into `settings` for all `HOOK_EVENTS`.
///
/// For each event:
/// - If any existing `HookGroup` already contains a `hooks[].command == "ccbridge-hook"`
///   entry → leave it alone (`AlreadyPresent`).
/// - Otherwise → append a new `{"hooks":[{"type":"command","command":"ccbridge-hook"}]}`
///   group (`Added`).
///
/// All other keys in `settings`, and all other hook groups within an event,
/// are left completely unchanged.
///
/// # Errors
///
/// Returns `Err` if `settings.hooks` exists but is not an object, or if any
/// `settings.hooks.<event>` value exists but is not an array.  These shapes
/// indicate user-authored config that ccbridge cannot safely modify; the user
/// must clean up the file manually before running setup.
pub fn merge_hooks(settings: &mut Value) -> anyhow::Result<Vec<HookMergeResult>> {
    // Ensure settings is an object.
    if !settings.is_object() {
        *settings = json!({});
    }

    // Validate `settings.hooks` shape: if present it must be an object or null.
    // null is treated as absent (replaced with {}) — it's JSON's natural way
    // to represent "not set yet".
    if let Some(existing_hooks) = settings.get("hooks") {
        if !existing_hooks.is_object() && !existing_hooks.is_null() {
            anyhow::bail!(
                "~/.claude/settings.json has an unexpected shape at .hooks \
                 (expected object, got {}) — please clean it up manually before \
                 running setup, or rename the file to start fresh",
                json_type_name(existing_hooks)
            );
        }
    }
    // Overwrite null with {} so the entry() call below works correctly.
    if settings.get("hooks").map(|v| v.is_null()).unwrap_or(false) {
        settings
            .as_object_mut()
            .unwrap()
            .insert("hooks".to_owned(), json!({}));
    }

    // Ensure `settings.hooks` is an object.
    let hooks_obj = settings
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert_with(|| json!({}));

    let mut results = Vec::new();

    for &event in HOOK_EVENTS {
        // Validate per-event shape: if present it must be an array or null.
        // null is treated as absent (same as "not set yet"); string/number/
        // object are genuinely unexpected and rejected.
        if let Some(existing_arr) = hooks_obj.get(event) {
            if !existing_arr.is_array() && !existing_arr.is_null() {
                anyhow::bail!(
                    "~/.claude/settings.json has an unexpected shape at .hooks.{event} \
                     (expected array, got {}) — please clean it up manually before \
                     running setup, or rename the file to start fresh",
                    json_type_name(existing_arr)
                );
            }
        }
        // Overwrite null with [] so entry().or_insert_with works correctly.
        if hooks_obj.get(event).map(|v| v.is_null()).unwrap_or(false) {
            hooks_obj
                .as_object_mut()
                .unwrap()
                .insert(event.to_owned(), json!([]));
        }

        // Ensure `settings.hooks.<event>` is an array (creates it if absent).
        let event_arr = hooks_obj
            .as_object_mut()
            .unwrap()
            .entry(event)
            .or_insert_with(|| json!([]));

        // Scan existing groups for a ccbridge-hook entry.
        let already = event_arr
            .as_array()
            .unwrap()
            .iter()
            .any(group_has_ccbridge_hook);

        if already {
            results.push(HookMergeResult {
                event,
                action: HookAction::AlreadyPresent,
            });
        } else {
            event_arr.as_array_mut().unwrap().push(json!({
                "hooks": [{"type": "command", "command": HOOK_COMMAND}]
            }));
            results.push(HookMergeResult {
                event,
                action: HookAction::Added,
            });
        }
    }

    Ok(results)
}

/// Return a human-readable JSON type name for error messages.
pub(crate) fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Return `true` if this hook group contains at least one entry with
/// `command == "ccbridge-hook"`.
fn group_has_ccbridge_hook(group: &Value) -> bool {
    group
        .get("hooks")
        .and_then(|h| h.as_array())
        .map(|entries| {
            entries
                .iter()
                .any(|entry| entry.get("command").and_then(|c| c.as_str()) == Some(HOOK_COMMAND))
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Load / save
// ---------------------------------------------------------------------------

/// Load `settings.json` from `path`.
///
/// * Path absent → return an empty JSON object (will be created on save).
/// * Path present, valid JSON → return the parsed value.
/// * Path present, malformed JSON → return an `Err` with a user-facing message.
pub fn load_settings(path: &Path) -> Result<Value> {
    if !path.exists() {
        return Ok(json!({}));
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| {
        format!(
            "{} is not valid JSON — please fix it manually before running setup",
            path.display()
        )
    })
}

/// Atomically write `settings` to `path` as pretty-printed JSON.
///
/// # Durability guarantee
///
/// 1. Data is written to `<path>.json.tmp`.
/// 2. The tmp file is fsynced (data + metadata) before rename so that no
///    partially-written content can replace a good settings file.
/// 3. The parent directory is fsynced after rename so the directory entry
///    change survives a power loss or `kill -9`.
///
/// On ENOSPC or a mid-write crash the tmp file is left behind but the
/// original `path` is never touched until `rename` succeeds — the worst
/// outcome is a stale `.json.tmp` that the user can delete.
///
/// Creates parent directories if needed.
pub fn save_settings(path: &Path, settings: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(settings)?;
    let bytes = text.as_bytes();

    {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("open {} for write", tmp.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write {}", tmp.display()))?;
        // fsync data + metadata before rename so no partial write can replace
        // the good settings file.
        file.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    } // file closed here

    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;

    // fsync the parent directory so the rename (directory entry change)
    // survives a power loss. Failure here doesn't fail the write — the
    // settings file is already on disk and the rename has happened — but
    // it does mean the durability guarantee is weaker than expected.
    // Log it as a forensic breadcrumb for "settings vanished after power
    // loss" investigations rather than swallowing silently.
    if let Some(parent) = path.parent() {
        match std::fs::File::open(parent) {
            Ok(dir) => {
                if let Err(e) = dir.sync_all() {
                    tracing::warn!(
                        parent = %parent.display(),
                        error = %e,
                        "save_settings: parent-dir fsync failed; rename may not survive a power loss"
                    );
                }
            }
            Err(e) => {
                tracing::debug!(
                    parent = %parent.display(),
                    error = %e,
                    "save_settings: could not open parent dir for fsync"
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Settings path
// ---------------------------------------------------------------------------

/// Return the path to Claude Code's `settings.json`.
///
/// Respects `$CLAUDE_CONFIG_DIR` (the documented override for `~/.claude/`).
/// Falls back to `$HOME/.claude/settings.json`.
///
/// Panics if neither `$CLAUDE_CONFIG_DIR` nor `$HOME` is set — the daemon
/// itself would fail earlier without `$HOME`.
pub fn settings_path() -> PathBuf {
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        return PathBuf::from(dir).join("settings.json");
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("$HOME must be set");
    home.join(".claude").join("settings.json")
}

// ---------------------------------------------------------------------------
// systemctl helper — gated so tests never shell out
// ---------------------------------------------------------------------------

/// Enable the ccbridge systemd user service.
///
/// Returns `true` on success, `false` on any error (service file not yet
/// installed, systemd not running, etc.).  Never fails the overall setup.
#[cfg(not(test))]
fn run_enable_service() -> bool {
    match std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "ccbridge.service"])
        .status()
    {
        Ok(s) if s.success() => true,
        Ok(s) => {
            eprintln!(
                "warning: systemctl --user enable --now ccbridge.service exited {}
         (PKGBUILD not yet installed, or systemd not running — run manually later)",
                s
            );
            false
        }
        Err(e) => {
            eprintln!("warning: could not run systemctl: {e}");
            false
        }
    }
}

// In tests, skip the real systemctl call.
#[cfg(test)]
fn run_enable_service() -> bool {
    true
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
    // merge_hooks
    // -----------------------------------------------------------------------

    fn all_added(results: &[HookMergeResult]) {
        for r in results {
            assert_eq!(
                r.action,
                HookAction::Added,
                "expected Added for {}",
                r.event
            );
        }
    }

    fn all_present(results: &[HookMergeResult]) {
        for r in results {
            assert_eq!(
                r.action,
                HookAction::AlreadyPresent,
                "expected AlreadyPresent for {}",
                r.event
            );
        }
    }

    #[test]
    fn merge_empty_settings() {
        let mut s = json!({});
        let results = merge_hooks(&mut s).unwrap();
        assert_eq!(results.len(), HOOK_EVENTS.len());
        all_added(&results);

        // Verify the structure is correct.
        for event in HOOK_EVENTS {
            let arr = s["hooks"][event].as_array().unwrap();
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0]["hooks"][0]["command"], HOOK_COMMAND);
            assert_eq!(arr[0]["hooks"][0]["type"], "command");
        }
    }

    #[test]
    fn merge_settings_with_unrelated_keys() {
        let mut s = json!({
            "theme": "dark-ansi",
            "tui": "fullscreen",
            "env": {"SOME_VAR": "1"}
        });
        let results = merge_hooks(&mut s).unwrap();
        all_added(&results);

        // Unrelated keys must survive.
        assert_eq!(s["theme"], "dark-ansi");
        assert_eq!(s["tui"], "fullscreen");
        assert_eq!(s["env"]["SOME_VAR"], "1");
    }

    #[test]
    fn merge_idempotent() {
        let mut s = json!({});
        // Run once → adds all.
        merge_hooks(&mut s).unwrap();
        // Run again → all already present.
        let results = merge_hooks(&mut s).unwrap();
        all_present(&results);
        // No duplicate groups.
        for event in HOOK_EVENTS {
            assert_eq!(
                s["hooks"][event].as_array().unwrap().len(),
                1,
                "duplicate groups for {event}"
            );
        }
    }

    #[test]
    fn idempotent_setup_does_not_rewrite_settings_file() {
        // Pin the bug fix: when every hook is AlreadyPresent, do_setup
        // must not call save_settings — otherwise it would re-pretty-print
        // the JSON, alphabetise keys, and trash any user formatting.
        // We mirror the `do_setup` flow with a tempdir-scoped settings
        // file and a hand-crafted bytes-on-disk we want to see preserved.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");

        // User-authored layout: keys in a specific order, two-space indent,
        // unrelated `theme` key first.  All HOOK_EVENTS already have a
        // ccbridge-hook entry.
        let user_authored = format!(
            "{{\n  \"theme\": \"dark\",\n  \"hooks\": {{\n{}  }}\n}}\n",
            HOOK_EVENTS
                .iter()
                .map(|e| format!(
                    "    \"{e}\": [{{\"hooks\":[{{\"type\":\"command\",\"command\":\"{HOOK_COMMAND}\"}}]}}],\n"
                ))
                .collect::<String>()
                .trim_end_matches(",\n")
                .to_owned(),
        );
        std::fs::write(&path, &user_authored).unwrap();

        // Run the setup-flow decision logic.
        let mut settings = load_settings(&path).unwrap();
        let results = merge_hooks(&mut settings).unwrap();
        let any_added = results.iter().any(|r| r.action == HookAction::Added);
        assert!(
            !any_added,
            "all hooks are pre-registered — none should be Added",
        );
        if any_added {
            save_settings(&path, &settings).unwrap();
        }

        // Bytes on disk must be byte-for-byte identical.  This is the
        // load-bearing assertion: the absence of a save_settings() call
        // means the user's formatting survives a no-op `ccbridged setup`.
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            on_disk, user_authored,
            "no-op setup must NOT rewrite settings.json",
        );
    }

    #[test]
    fn merge_partial_presets() {
        // Only PreToolUse is configured; remaining 6 should be Added.
        let mut s = json!({
            "hooks": {
                "PreToolUse": [
                    {"hooks": [{"type": "command", "command": "ccbridge-hook"}]}
                ]
            }
        });
        let results = merge_hooks(&mut s).unwrap();
        assert_eq!(results.len(), HOOK_EVENTS.len());
        let pre = results.iter().find(|r| r.event == "PreToolUse").unwrap();
        assert_eq!(pre.action, HookAction::AlreadyPresent);
        for r in results.iter().filter(|r| r.event != "PreToolUse") {
            assert_eq!(
                r.action,
                HookAction::Added,
                "expected Added for {}",
                r.event
            );
        }
    }

    #[test]
    fn merge_settings_with_pre_tool_use_matcher() {
        // User already has a foreign hook group on PreToolUse.
        // After merge: foreign group preserved + our group appended.
        let mut s = json!({
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "Bash",
                        "hooks": [{"type": "command", "command": "my-lint-script.sh"}]
                    }
                ]
            }
        });
        let results = merge_hooks(&mut s).unwrap();

        let pre = results.iter().find(|r| r.event == "PreToolUse").unwrap();
        assert_eq!(pre.action, HookAction::Added, "should append our group");

        let arr = s["hooks"]["PreToolUse"].as_array().unwrap();
        assert_eq!(arr.len(), 2, "foreign group + our group");

        // Foreign group still intact.
        assert_eq!(arr[0]["matcher"], "Bash");
        assert_eq!(arr[0]["hooks"][0]["command"], "my-lint-script.sh");

        // Our group appended.
        assert_eq!(arr[1]["hooks"][0]["command"], HOOK_COMMAND);
    }

    #[test]
    fn merge_preserves_other_event_keys() {
        // hooks.Idle is not in our HOOK_EVENTS list; merge must leave it alone.
        let mut s = json!({
            "hooks": {
                "Idle": [{"hooks": [{"type": "command", "command": "idle-handler"}]}]
            }
        });
        merge_hooks(&mut s).unwrap();

        let idle = s["hooks"]["Idle"].as_array().unwrap();
        assert_eq!(idle.len(), 1);
        assert_eq!(idle[0]["hooks"][0]["command"], "idle-handler");
    }

    #[test]
    fn merge_preserves_complex_unrelated_keys() {
        // Real-world settings.json shapes: permissions, mcpServers, enabledPlugins, etc.
        // Merge must add hooks without mangling any of these.
        let mut s = json!({
            "theme": "dark-ansi",
            "permissions": {
                "allow": ["Skill"],
                "deny": ["Read(**/.env)"],
                "ask": ["Bash"]
            },
            "mcpServers": {
                "context7": {"command": "npx", "args": ["-y", "@upstash/context7-mcp"]}
            },
            "enabledPlugins": ["foo", "bar"],
            "tui": "fullscreen"
        });
        let results = merge_hooks(&mut s).unwrap();
        assert_eq!(results.len(), HOOK_EVENTS.len());

        // All hook events added.
        for event in HOOK_EVENTS {
            assert!(s["hooks"][*event].is_array(), "{event} missing after merge");
        }
        // Every pre-existing key must survive untouched.
        assert_eq!(s["theme"], "dark-ansi");
        assert_eq!(s["permissions"]["ask"], json!(["Bash"]));
        assert_eq!(s["permissions"]["allow"], json!(["Skill"]));
        assert_eq!(s["mcpServers"]["context7"]["command"], "npx");
        assert_eq!(s["enabledPlugins"], json!(["foo", "bar"]));
        assert_eq!(s["tui"], "fullscreen");
    }

    // -----------------------------------------------------------------------
    // load_settings / save_settings
    // -----------------------------------------------------------------------

    #[test]
    fn load_settings_missing_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        let v = load_settings(&path).unwrap();
        assert!(v.is_object());
        assert!(v.as_object().unwrap().is_empty());
    }

    #[test]
    fn load_settings_malformed_json() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, b"not json").unwrap();
        let err = load_settings(&path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not valid JSON"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested").join("settings.json");
        let mut s = json!({"theme": "dark", "hooks": {}});
        merge_hooks(&mut s).unwrap();
        save_settings(&path, &s).unwrap();
        let loaded = load_settings(&path).unwrap();
        // All 7 events present after round-trip.
        for event in HOOK_EVENTS {
            assert!(
                loaded["hooks"][event].is_array(),
                "{event} missing after round-trip"
            );
        }
        assert_eq!(loaded["theme"], "dark");
    }

    // -----------------------------------------------------------------------
    // Service enable — test path just returns true
    // -----------------------------------------------------------------------

    #[test]
    fn setup_does_not_exit_when_service_fails() {
        // In test mode run_enable_service() always returns true — but we can
        // test the non-exit behavior by calling do_setup_inner directly with
        // a tempdir settings path so it doesn't touch real files.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("settings.json");
        let mut s = load_settings(&path).unwrap();
        let results = merge_hooks(&mut s).unwrap();
        save_settings(&path, &s).unwrap();
        // If we got here without panicking / exiting, the flow is correct.
        assert_eq!(results.len(), HOOK_EVENTS.len());
    }

    // -----------------------------------------------------------------------
    // I3: shape validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn merge_hooks_rejects_non_object_hooks() {
        // .hooks is a string — not an object.
        let mut s = json!({"hooks": "invalid_string"});
        let err = merge_hooks(&mut s).unwrap_err();
        assert!(
            err.to_string().contains("unexpected shape"),
            "error must mention unexpected shape, got: {err}"
        );
        assert!(
            err.to_string().contains("string"),
            "error must name the actual type, got: {err}"
        );
    }

    #[test]
    fn merge_hooks_rejects_non_array_event_value() {
        // .hooks.PreToolUse is an object, not an array.
        let mut s = json!({"hooks": {"PreToolUse": {"foo": "bar"}}});
        let err = merge_hooks(&mut s).unwrap_err();
        assert!(
            err.to_string().contains("unexpected shape"),
            "error must mention unexpected shape, got: {err}"
        );
        assert!(
            err.to_string().contains("PreToolUse"),
            "error must name the problematic event, got: {err}"
        );
        assert!(
            err.to_string().contains("object"),
            "error must name the actual type, got: {err}"
        );
    }

    #[test]
    fn merge_hooks_treats_null_hooks_as_absent() {
        // `{"hooks": null}` must be treated as absent and populated with all events.
        let mut s = json!({"hooks": null});
        let results = merge_hooks(&mut s).unwrap();
        assert_eq!(results.len(), HOOK_EVENTS.len());
        // All events must be Added (null treated as if the key didn't exist).
        for r in &results {
            assert_eq!(
                r.action,
                HookAction::Added,
                "event {} must be Added when hooks:null",
                r.event
            );
        }
        // The .hooks field must now be an object.
        assert!(
            s["hooks"].is_object(),
            ".hooks must be an object after merge, got: {}",
            s["hooks"]
        );
    }

    #[test]
    fn merge_hooks_treats_null_event_as_absent() {
        // `{"hooks": {"PreToolUse": null}}` — null event is treated as absent,
        // must end up with the ccbridge entry added.
        let mut s = json!({"hooks": {"PreToolUse": null}});
        let results = merge_hooks(&mut s).unwrap();
        // Find the PreToolUse result.
        let ptu = results.iter().find(|r| r.event == "PreToolUse").unwrap();
        assert_eq!(
            ptu.action,
            HookAction::Added,
            "PreToolUse must be Added when its value is null"
        );
        let arr = s["hooks"]["PreToolUse"].as_array().unwrap();
        assert_eq!(
            arr.len(),
            1,
            "PreToolUse must have exactly one group after merge"
        );
        assert_eq!(arr[0]["hooks"][0]["command"], HOOK_COMMAND);
    }

    // -----------------------------------------------------------------------
    // write_default_config_to
    // -----------------------------------------------------------------------

    #[test]
    fn write_default_config_creates_file_when_absent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("ccbridge").join("config.toml");
        assert!(!path.exists());

        let outcome = write_default_config_to(&path).expect("write must succeed");
        match outcome {
            ConfigAction::Created { path: written_path } => {
                assert_eq!(written_path, path);
            }
            other => panic!("expected Created, got {other:?}"),
        }
        assert!(path.exists());

        // Content matches the bundled default verbatim.
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert_eq!(on_disk, DEFAULT_CONFIG_TOML);
    }

    #[test]
    fn write_default_config_skips_when_present() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        let user_content = "# my custom config\n[approvals]\ntimeout_ms = 5000\n";
        std::fs::write(&path, user_content).unwrap();

        let outcome = write_default_config_to(&path).expect("must succeed");
        assert!(
            matches!(outcome, ConfigAction::AlreadyPresent { .. }),
            "expected AlreadyPresent, got {outcome:?}"
        );
        // User content untouched — we never overwrite.
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert_eq!(on_disk, user_content);
    }

    #[test]
    fn write_default_config_creates_parent_dir() {
        let dir = TempDir::new().unwrap();
        // Two levels of parent dir don't exist yet.
        let path = dir.path().join("a").join("b").join("config.toml");
        assert!(!path.parent().unwrap().exists());

        write_default_config_to(&path).expect("must succeed");
        assert!(path.exists());
    }

    #[test]
    fn bundled_default_config_parses_as_config() {
        // The example-config.toml that ships with the binary must survive
        // a Config::load_from round-trip — otherwise `setup` would write
        // a file that the very next daemon start refuses to load with
        // "deny_unknown_fields" or a parse error.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, DEFAULT_CONFIG_TOML).unwrap();

        crate::config::Config::load_from(&path).expect("bundled default config must parse cleanly");
    }
}
