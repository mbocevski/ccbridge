//! ccbridge-hook — Claude Code hook shim.
//!
//! Reads a JSON event from stdin, forwards it to the daemon over
//! `$XDG_RUNTIME_DIR/ccbridge/hooks.sock`, waits for a single-line
//! response, and writes that response to stdout.
//!
//! **Reliability invariant:** if the socket does not exist or the
//! connect/write/read fails for any reason, the hook exits 0 with no
//! output — Claude Code continues exactly as if no hook were installed.
//! Daemon-down ≠ Claude breaks.
//!
//! Full implementation: packaging task (not owned by core agent).
//! This stub exists so the workspace builds cleanly.

fn main() {
    // Stub: exit 0 silently. No daemon = Claude Code behaves normally.
}
