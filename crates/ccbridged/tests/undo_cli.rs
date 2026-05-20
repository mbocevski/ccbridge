// SPDX-License-Identifier: MIT
//! Integration tests for `ccbridged undo-last-allow` CLI dispatch.
//!
//! These tests run the real `ccbridged` binary as a subprocess and verify
//! that audit-root validation (G3) is exercised through the CLI path, not
//! just the library unit tests. The failure modes we care about — tampered
//! audit logs with "/" or trailing-`.` roots — must produce a non-zero
//! exit code and a descriptive error on stderr.

use std::path::PathBuf;
use std::process::{Command, Stdio};

fn ccbridged_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_ccbridged") {
        return PathBuf::from(p);
    }
    let mut p = std::env::current_exe().unwrap();
    p.pop();
    p.pop();
    p.push("ccbridged");
    p
}

/// Write `lines` to `<state_dir>/ccbridge/allowlist-additions.log`. The
/// path matches `audit_log_path()` when `XDG_STATE_HOME = state_dir`.
fn write_audit_log(state_dir: &std::path::Path, lines: &[&str]) -> PathBuf {
    let log_dir = state_dir.join("ccbridge");
    std::fs::create_dir_all(&log_dir).unwrap();
    let log_path = log_dir.join("allowlist-additions.log");
    let body: String = lines
        .iter()
        .map(|l| format!("{l}\n"))
        .collect::<Vec<_>>()
        .join("");
    std::fs::write(&log_path, body).unwrap();
    log_path
}

fn run_undo(state_dir: &std::path::Path) -> std::process::Output {
    Command::new(ccbridged_bin())
        .arg("undo-last-allow")
        .env("XDG_STATE_HOME", state_dir)
        // Some test envs may have HOME unset; pin it so the daemon's
        // settings_path() resolution doesn't panic — it's only used when
        // we successfully reach the legacy UserGlobal branch.
        .env("HOME", state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn ccbridged undo-last-allow")
}

#[test]
fn undo_cli_rejects_filesystem_root_in_audit_log() {
    // Tampered audit line: project root is `/` — must be rejected by
    // validate_audit_root, surfaced through the CLI as exit 1.
    let tmp = tempfile::tempdir().unwrap();
    let bad_line = r#"{"ts":"2026-05-19T22:00:00Z","op":"added","pattern":"Bash(evil)","tool_use_id":"toolu_evil","session_id":"abc","target":{"project_local":{"root":"/"}}}"#;
    write_audit_log(tmp.path(), &[bad_line]);

    let out = run_undo(tmp.path());
    assert!(
        !out.status.success(),
        "must exit non-zero on tampered root '/', got: {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("filesystem root") || stderr.contains("refusing"),
        "stderr must explain the refusal, got: {stderr}"
    );
}

#[test]
fn undo_cli_rejects_relative_root_in_audit_log() {
    // Tampered audit line: project root is `relative/path` — must fail
    // the absolute-path check.
    let tmp = tempfile::tempdir().unwrap();
    let bad_line = r#"{"ts":"2026-05-19T22:00:00Z","op":"added","pattern":"Bash(evil)","tool_use_id":"toolu_rel","session_id":"abc","target":{"project_local":{"root":"relative/path"}}}"#;
    write_audit_log(tmp.path(), &[bad_line]);

    let out = run_undo(tmp.path());
    assert!(
        !out.status.success(),
        "must exit non-zero on relative root, got: {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("relative") || stderr.contains("refusing"),
        "stderr must explain the refusal, got: {stderr}"
    );
}

#[test]
fn undo_cli_empty_audit_log_returns_error() {
    // No audit log at all — undo must exit 1 with a clear message.
    let tmp = tempfile::tempdir().unwrap();
    // Don't write any log.
    let out = run_undo(tmp.path());
    assert!(
        !out.status.success(),
        "must exit non-zero on empty audit log"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no allowlist additions") || stderr.contains("audit"),
        "stderr must explain the empty log, got: {stderr}"
    );
}
