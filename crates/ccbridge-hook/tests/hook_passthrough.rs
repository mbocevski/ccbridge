// SPDX-License-Identifier: MIT
//! Integration tests for the ccbridge-hook binary.
//!
//! Tests run the real `ccbridge-hook` binary as a subprocess via
//! `std::process::Command` so they verify the compiled binary end-to-end.
//!
//! Two scenarios:
//! 1. **Round-trip** — a fake daemon listens on a temp socket, the hook
//!    binary connects, sends the event, and the fake daemon echoes a
//!    response.  Assert the hook's stdout matches the response verbatim.
//! 2. **No socket** — `$XDG_RUNTIME_DIR` points at a tempdir with no
//!    socket file.  Assert exit code 0 and empty stdout.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

fn hook_bin() -> PathBuf {
    // cargo sets CARGO_BIN_EXE_<name> when running tests in the same workspace.
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_ccbridge-hook") {
        return PathBuf::from(p);
    }
    // Fallback: adjacent to the test binary in the target dir.
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // remove test binary
    p.pop(); // remove "deps"
    p.push("ccbridge-hook");
    p
}

const HOOK_INPUT: &str = concat!(
    r#"{"session_id":"s1","transcript_path":"/tmp/s1.jsonl","cwd":"/tmp","#,
    r#""permission_mode":"default","hook_event_name":"Stop","response":"done"}"#,
);
const FAKE_RESPONSE: &str =
    r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}}"#;

// ---------------------------------------------------------------------------
// Test 1: round-trip through a fake daemon
// ---------------------------------------------------------------------------

#[test]
fn round_trip_via_fake_daemon() {
    let tmp = tempfile::tempdir().unwrap();
    let ccbridge_dir = tmp.path().join("ccbridge");
    std::fs::create_dir_all(&ccbridge_dir).unwrap();
    let sock_path = ccbridge_dir.join("hooks.sock");

    // Bind the listening socket before spawning the hook so the hook can
    // connect immediately without a ENOENT race.
    let listener = UnixListener::bind(&sock_path).unwrap();

    // Spawn the hook binary.
    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", tmp.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ccbridge-hook");

    // Write the event to the hook's stdin in a background thread so we can
    // drive the accept loop here without deadlocking.
    let stdin = child.stdin.take().unwrap();
    std::thread::spawn(move || {
        let mut w = stdin;
        w.write_all(HOOK_INPUT.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
        // Drop closes the pipe → hook reads EOF on stdin after the line.
    });

    // Accept the one connection the hook makes.
    let (stream, _) = {
        listener.set_nonblocking(false).unwrap();
        // Use a timeout so a broken test binary doesn't hang the suite.
        // UnixListener doesn't expose set_accept_timeout directly, so we set
        // a read timeout on the underlying fd via a temporary duration.
        // Simply call accept() — it will complete once the hook connects.
        listener.accept().expect("accept hook connection")
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Read the event line the hook sent.
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(
        !line.trim().is_empty(),
        "hook must send a non-empty event line"
    );

    // Write the fake response and close.
    {
        let mut w = &stream;
        w.write_all(FAKE_RESPONSE.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    }
    drop(stream); // close connection → hook's read_line returns

    // Collect the hook's output.
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "hook must exit 0, got: {:?}",
        output.status,
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.trim_end_matches('\n').trim_end_matches('\r'),
        FAKE_RESPONSE,
        "hook stdout must echo the daemon response verbatim",
    );
}

// ---------------------------------------------------------------------------
// Test 1b: stdin held open past timeout → hook exits 0 without wedging
// ---------------------------------------------------------------------------

#[test]
fn stdin_held_open_times_out_cleanly() {
    use std::time::Instant;

    let tmp = tempfile::tempdir().unwrap();
    // No socket — the hook should fall through to passthrough on stdin
    // timeout, but the failure mode we're verifying is "doesn't wedge".
    std::fs::create_dir_all(tmp.path().join("ccbridge")).unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", tmp.path())
        // Tighten the budget so the test doesn't add 5s to the suite.
        .env("CCBRIDGE_HOOK_STDIN_TIMEOUT_MS", "200")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn ccbridge-hook");

    // Hold stdin open without writing — simulating Claude Code crashing
    // or a PTY misbehaviour.  The hook must give up after the budget.
    let stdin = child.stdin.take().unwrap();

    let start = Instant::now();
    let output = child.wait_with_output().unwrap();
    let elapsed = start.elapsed();

    // Drop the stdin handle after waiting, so it stays open during the run.
    drop(stdin);

    assert!(
        output.status.success(),
        "hook must exit 0 on stdin timeout, got: {:?}",
        output.status,
    );
    assert!(
        output.stdout.is_empty(),
        "hook must produce no stdout on stdin timeout, got: {:?}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "hook must exit within 2s of the 200ms budget, took {elapsed:?}",
    );
}

// ---------------------------------------------------------------------------
// Test 2: no socket → exit 0, no stdout
// ---------------------------------------------------------------------------

#[test]
fn no_socket_exits_zero_with_no_output() {
    let tmp = tempfile::tempdir().unwrap();
    // Create the ccbridge dir but NOT the socket file.
    std::fs::create_dir_all(tmp.path().join("ccbridge")).unwrap();

    let mut child = Command::new(hook_bin())
        .env("XDG_RUNTIME_DIR", tmp.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Write a valid event to stdin — the hook should still passthrough.
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(HOOK_INPUT.as_bytes()).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "hook must exit 0 when socket is absent, got: {:?}",
        output.status,
    );
    assert!(
        output.stdout.is_empty(),
        "hook must produce no stdout when socket is absent, got: {:?}",
        String::from_utf8_lossy(&output.stdout),
    );
}
