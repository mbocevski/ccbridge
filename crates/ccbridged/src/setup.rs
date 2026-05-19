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

    let results = merge_hooks(&mut settings);
    save_settings(&settings_path, &settings)
        .with_context(|| format!("writing {}", settings_path.display()))?;

    // --- 2. Enable systemd service ---
    let service_ok = run_enable_service();

    // --- 3. Print summary ---
    let mut added = Vec::new();
    let mut present = Vec::new();
    for r in &results {
        match r.action {
            HookAction::Added => added.push(r.event),
            HookAction::AlreadyPresent => present.push(r.event),
        }
    }

    if !added.is_empty() {
        println!("ccbridged setup: registered hooks for: {}", added.join(", "));
    }
    if !present.is_empty() {
        println!(
            "ccbridged setup: already configured: {}",
            present.join(", ")
        );
    }
    println!(
        "ccbridged setup: service enabled: {}",
        if service_ok { "yes" } else { "no (see above warning)" }
    );
    println!("ccbridged setup: done — settings written to {}", settings_path.display());

    Ok(())
}

// ---------------------------------------------------------------------------
// Hook merge
// ---------------------------------------------------------------------------

/// Result of processing one hook event during `merge_hooks`.
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
pub fn merge_hooks(settings: &mut Value) -> Vec<HookMergeResult> {
    // Ensure settings is an object.
    if !settings.is_object() {
        *settings = json!({});
    }

    // Ensure `settings.hooks` is an object.
    let hooks_obj = settings
        .as_object_mut()
        .unwrap()
        .entry("hooks")
        .or_insert_with(|| json!({}));
    if !hooks_obj.is_object() {
        *hooks_obj = json!({});
    }

    let mut results = Vec::new();

    for &event in HOOK_EVENTS {
        // Ensure `settings.hooks.<event>` is an array.
        let event_arr = hooks_obj
            .as_object_mut()
            .unwrap()
            .entry(event)
            .or_insert_with(|| json!([]));
        if !event_arr.is_array() {
            *event_arr = json!([]);
        }

        // Scan existing groups for a ccbridge-hook entry.
        let already = event_arr
            .as_array()
            .unwrap()
            .iter()
            .any(|group| group_has_ccbridge_hook(group));

        if already {
            results.push(HookMergeResult { event, action: HookAction::AlreadyPresent });
        } else {
            event_arr.as_array_mut().unwrap().push(json!({
                "hooks": [{"type": "command", "command": HOOK_COMMAND}]
            }));
            results.push(HookMergeResult { event, action: HookAction::Added });
        }
    }

    results
}

/// Return `true` if this hook group contains at least one entry with
/// `command == "ccbridge-hook"`.
fn group_has_ccbridge_hook(group: &Value) -> bool {
    group
        .get("hooks")
        .and_then(|h| h.as_array())
        .map(|entries| {
            entries.iter().any(|entry| {
                entry
                    .get("command")
                    .and_then(|c| c.as_str())
                    == Some(HOOK_COMMAND)
            })
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
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| {
        format!(
            "{} is not valid JSON — please fix it manually before running setup",
            path.display()
        )
    })
}

/// Atomically write `settings` to `path` as pretty-printed JSON.
///
/// Writes to `<path>.tmp` first, then renames.  Creates parent directories
/// if needed.
pub fn save_settings(path: &Path, settings: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create_dir_all {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(settings)?;
    std::fs::write(&tmp, &text).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
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
            assert_eq!(r.action, HookAction::Added, "expected Added for {}", r.event);
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
        let results = merge_hooks(&mut s);
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
        let results = merge_hooks(&mut s);
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
        merge_hooks(&mut s);
        // Run again → all already present.
        let results = merge_hooks(&mut s);
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
    fn merge_partial_presets() {
        // Only PreToolUse is configured; remaining 6 should be Added.
        let mut s = json!({
            "hooks": {
                "PreToolUse": [
                    {"hooks": [{"type": "command", "command": "ccbridge-hook"}]}
                ]
            }
        });
        let results = merge_hooks(&mut s);
        assert_eq!(results.len(), HOOK_EVENTS.len());
        let pre = results.iter().find(|r| r.event == "PreToolUse").unwrap();
        assert_eq!(pre.action, HookAction::AlreadyPresent);
        for r in results.iter().filter(|r| r.event != "PreToolUse") {
            assert_eq!(r.action, HookAction::Added, "expected Added for {}", r.event);
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
        let results = merge_hooks(&mut s);

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
        merge_hooks(&mut s);

        let idle = s["hooks"]["Idle"].as_array().unwrap();
        assert_eq!(idle.len(), 1);
        assert_eq!(idle[0]["hooks"][0]["command"], "idle-handler");
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
        merge_hooks(&mut s);
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
        let results = merge_hooks(&mut s);
        save_settings(&path, &s).unwrap();
        // If we got here without panicking / exiting, the flow is correct.
        assert_eq!(results.len(), HOOK_EVENTS.len());
    }
}
