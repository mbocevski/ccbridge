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
use std::time::Duration;

/// Timeout for connecting to and reading from the daemon socket.
///
/// The daemon controls the actual approval wait; this timeout only needs to
/// outlive it.  60 s is generous but harmless — Claude Code will have already
/// handled the event long before this fires in the normal case.
const SOCKET_TIMEOUT: Duration = Duration::from_secs(60);

fn main() {
    // Any error anywhere → exit 0 with no output (passthrough semantics).
    let _ = run();
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

    // ── 2. Read stdin ────────────────────────────────────────────────────────
    // Claude Code writes exactly one JSON line.  Read the whole stdin buffer.
    let mut input = String::new();
    io::stdin().lock().read_to_string(&mut input).ok()?;
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
