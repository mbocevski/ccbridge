// SPDX-License-Identifier: MIT
//! ccbridge-hook — Claude Code hook shim.
//!
//! Reads a JSON event from stdin, forwards it to the daemon over
//! `$XDG_RUNTIME_DIR/ccbridge/hooks.sock`, waits for a single-line
//! response, and writes that response to stdout.
//!
//! **Reliability invariant:** any error (socket absent, connect refused,
//! read/write failure, timeout) → exit 0 with no output.  Daemon-down ≠
//! Claude Code breaks.  The hook is intentionally fail-silent.

use std::io::{self, BufRead, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use std::time::Duration;

/// Timeout for connecting to and reading from the daemon socket.
///
/// The daemon controls the actual approval wait; this timeout only needs to
/// outlive it.  60 s is generous but harmless — Claude Code will have already
/// handled the event long before this fires in the normal case.
const SOCKET_TIMEOUT: Duration = Duration::from_secs(60);

/// Wall-clock budget for reading the event line from stdin.
///
/// Claude Code closes stdin promptly after writing the line, so this fires
/// only when something upstream is broken (test harness left stdin open,
/// PTY misbehaviour).  Without this cap the hook would wedge forever and
/// stall whatever process is waiting on it.
const STDIN_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);

/// Maximum bytes accepted on stdin.  Mirrors the daemon's per-line cap so
/// neither side has to buffer pathological inputs.
const STDIN_MAX_BYTES: u64 = 1 << 20; // 1 MiB

/// Resolve the stdin read timeout, allowing tests to override via env var.
fn stdin_timeout() -> Duration {
    std::env::var("CCBRIDGE_HOOK_STDIN_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(STDIN_TIMEOUT_DEFAULT)
}

fn main() {
    // Any error anywhere → exit 0 with no output (passthrough semantics).
    // run() handles Ok/Err via Option<()>, but a panic would still
    // terminate with exit code 101 by default — Claude Code would see
    // a non-zero exit and surface it to the user. Replace the hook with
    // a no-op silent exit so a panic also passes through cleanly.
    std::panic::set_hook(Box::new(|_| {
        // Intentionally empty — exit_silently below handles the exit.
    }));
    let _ = std::panic::catch_unwind(run);
}

fn run() -> Option<()> {
    // ── 1. Compute socket path ──────────────────────────────────────────────
    // If $XDG_RUNTIME_DIR is unset there is no daemon path → passthrough.
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")?;
    let sock_path = {
        let mut p = std::path::PathBuf::from(runtime_dir);
        p.push("ccbridge");
        p.push("hooks.sock");
        p
    };

    // ── 2. Read stdin (bounded + time-capped) ───────────────────────────────
    // Claude Code writes exactly one JSON line and then closes stdin.  Two
    // belts: cap the byte count via Read::take so a runaway producer can't
    // exhaust memory, and put the read on a thread with a wall-clock
    // deadline so a producer that forgets to close stdin can't wedge us.
    let input = read_stdin_bounded(STDIN_MAX_BYTES, stdin_timeout())?;
    let input = input.trim_end_matches('\n').trim_end_matches('\r');
    if input.is_empty() {
        return None;
    }

    // ── 3. Connect (fail-silent on ENOENT / ECONNREFUSED / timeout) ─────────
    let stream = UnixStream::connect(&sock_path).ok()?;
    stream.set_write_timeout(Some(SOCKET_TIMEOUT)).ok()?;
    stream.set_read_timeout(Some(SOCKET_TIMEOUT)).ok()?;

    // ── 4. Write event line ──────────────────────────────────────────────────
    {
        let mut writer = io::BufWriter::new(&stream);
        writer.write_all(input.as_bytes()).ok()?;
        writer.write_all(b"\n").ok()?;
        writer.flush().ok()?;
    }
    // Shut down the write half so the daemon's read_line sees EOF after our
    // newline rather than blocking waiting for more data.
    stream.shutdown(std::net::Shutdown::Write).ok()?;

    // ── 5. Read one response line ────────────────────────────────────────────
    let mut response = String::new();
    let mut reader = io::BufReader::new(&stream);
    let n = reader.read_line(&mut response).ok()?;
    if n == 0 {
        // EOF — daemon sent nothing → passthrough.
        return None;
    }

    // ── 6. Write response to stdout ──────────────────────────────────────────
    let response = response.trim_end_matches('\n').trim_end_matches('\r');
    if response.is_empty() {
        return None;
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();
    out.write_all(response.as_bytes()).ok()?;
    out.write_all(b"\n").ok()?;
    out.flush().ok()?;

    Some(())
}

/// Read up to `max_bytes` from stdin, giving up after `timeout`.
///
/// Returns `None` on timeout, I/O error, or oversized input — all of which
/// trip the hook's passthrough semantics (caller exits 0 with no output).
///
/// The reader thread is intentionally orphaned on timeout: if stdin is wedged
/// waiting on an upstream that never closes, the thread will sit blocked in
/// the kernel until the process exits anyway.  That's fine for a short-lived
/// shim — the OS reaps it on exit.
fn read_stdin_bounded(max_bytes: u64, timeout: Duration) -> Option<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = String::new();
        let result = io::stdin()
            .lock()
            .take(max_bytes.saturating_add(1))
            .read_to_string(&mut buf);
        // Channel send failure means the receiver already gave up — nothing
        // left to do.
        let _ = tx.send(result.map(|_| buf));
    });

    match rx.recv_timeout(timeout) {
        Ok(Ok(buf)) => {
            // If the reader stopped exactly at max_bytes+1, the producer
            // exceeded our cap.  Refuse rather than silently truncating.
            if buf.len() as u64 > max_bytes {
                return None;
            }
            Some(buf)
        }
        Ok(Err(_)) | Err(_) => None,
    }
}
