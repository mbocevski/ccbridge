//! Claude Code hook event shapes.
//!
//! Claude Code writes one JSON object to stdin for each hook invocation.
//! The exact shape depends on `$CLAUDE_HOOK_EVENT_NAME`.  All variants
//! share a `session_id` field; remaining fields are event-specific.
//!
//! Reference: Claude Code docs + observed `.jsonl` session files under
//! `~/.claude/projects/<encoded-cwd>/*.jsonl`.

// Placeholder — full serde types land in task 1ecf3330.
