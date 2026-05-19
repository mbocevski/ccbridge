// SPDX-License-Identifier: MIT
//! Pattern type + matcher for individual `permissions.allow` / `.deny` entries.
//!
//! # Design invariant
//!
//! [`Pattern::parse`] is **infallible** — it never returns an error.  Unrecognised
//! syntax falls through to [`Pattern::Unparseable`], which matches ambiguously
//! when the tool name appears anywhere in the raw string.  This ensures that
//! future Claude Code pattern syntax additions produce "ask the user" rather
//! than "silently ignore the user's intent."
//!
//! # Security note
//!
//! Every "Confident" match must be conservative: when in doubt, return
//! [`MatchResult::Ambiguous`] not `Confident`.  A false-confident-allow skips
//! the swaync prompt entirely for a call the user never explicitly okayed.

use ccbridge_proto::hook::PreToolUseEvent;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A parsed entry from `permissions.allow` or `permissions.deny`.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// Bare tool name: `"Skill"`, `"Bash"`.
    BareTool { name: String, raw: String },

    /// Full MCP method id: `"mcp__plugin_context7_context7__resolve-library-id"`.
    McpExact { full_id: String, raw: String },

    /// MCP method prefix with wildcard: `"mcp__plugin_backlog_tasks__*"`.
    /// `prefix` is the raw string with the trailing `*` stripped.
    McpPrefix { prefix: String, raw: String },

    /// Tool call with argument matcher: `"Read(**/.env*)"`, `"Agent(task-planner)"`.
    ToolWithArgs {
        tool: String,
        arg_matcher: ArgMatcher,
        raw: String,
    },

    /// Pattern we could not parse.  Matches `Ambiguous` when the tool name
    /// appears anywhere in `raw` (user probably intended to match this tool),
    /// `NoMatch` otherwise.
    Unparseable { raw: String },
}

/// Argument-level matcher within a `ToolWithArgs` pattern.
#[derive(Debug, Clone)]
pub enum ArgMatcher {
    /// Exact string match against a specific input field.
    ///
    /// For `Agent(…)`: matches `tool_input["subagent_type"]`.
    /// For other tools: always [`MatchResult::Ambiguous`] (field unknown).
    Exact(String),

    /// Path glob match against `tool_input["file_path"]` or `tool_input["path"]`.
    ///
    /// Only confident for Read/Edit/Write/MultiEdit/Glob/Grep.
    PathGlob(globset::Glob),

    /// Bash command prefix match against `tool_input["command"]`.
    ///
    /// Parsed from `"Bash(prefix:*)"` — the `:*` suffix is stripped.
    /// Only confident for `Bash`.
    BashPrefix(String),

    /// Exact Bash command match against `tool_input["command"]`.
    ///
    /// Parsed from `"Bash(exact_command)"` (no `:*` suffix).
    /// Produced by `permission::additions::derive_pattern` — matches only
    /// when the command is exactly this string.
    BashExact(String),

    /// We recognised the wrapper syntax but cannot confidently evaluate the
    /// argument semantics.  Always returns [`MatchResult::Ambiguous`] when the
    /// tool name matches.
    Ambiguous,
}

/// Result of matching a [`Pattern`] against a [`PreToolUseEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchResult {
    /// The pattern definitively matches this event.  Safe to allow/deny without
    /// further prompting.
    Confident,

    /// The tool name matched but we cannot fully evaluate the argument
    /// semantics.  Surface to the user.
    Ambiguous,

    /// The pattern does not match this event.  Continue checking other patterns.
    NoMatch,
}

// ---------------------------------------------------------------------------
// Pattern impl
// ---------------------------------------------------------------------------

impl Pattern {
    /// Parse a raw pattern string into a [`Pattern`].
    ///
    /// **Never fails.**  Unrecognised syntax → [`Pattern::Unparseable`].
    pub fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();

        if trimmed.is_empty() {
            return Self::Unparseable {
                raw: raw.to_owned(),
            };
        }

        // Tool-with-args: contains a `(`.
        if trimmed.contains('(') {
            return Self::parse_tool_with_args(trimmed, raw);
        }

        // MCP method: starts with `mcp__`.
        if trimmed.starts_with("mcp__") {
            return Self::parse_mcp(trimmed, raw);
        }

        // Bare tool name.
        Self::BareTool {
            name: trimmed.to_owned(),
            raw: raw.to_owned(),
        }
    }

    /// Match this pattern against a `PreToolUse` event.
    pub fn matches(&self, event: &PreToolUseEvent) -> MatchResult {
        match self {
            Self::BareTool { name, .. } => {
                if event.tool_name == *name {
                    MatchResult::Confident
                } else {
                    MatchResult::NoMatch
                }
            }

            Self::McpExact { full_id, .. } => {
                if event.tool_name == *full_id {
                    MatchResult::Confident
                } else {
                    MatchResult::NoMatch
                }
            }

            Self::McpPrefix { prefix, .. } => {
                if event.tool_name.starts_with(prefix.as_str()) {
                    MatchResult::Confident
                } else {
                    MatchResult::NoMatch
                }
            }

            Self::ToolWithArgs {
                tool, arg_matcher, ..
            } => {
                if event.tool_name != *tool {
                    return MatchResult::NoMatch;
                }
                match_tool_args(tool, arg_matcher, &event.tool_input)
            }

            Self::Unparseable { raw } => {
                // Conservative: if the event's tool name appears anywhere in
                // the raw pattern string, the user probably intended to match
                // this tool — surface it rather than silently ignoring.
                if raw.contains(event.tool_name.as_str()) {
                    MatchResult::Ambiguous
                } else {
                    MatchResult::NoMatch
                }
            }
        }
    }

    /// The original raw string from settings.json.
    pub fn raw(&self) -> &str {
        match self {
            Self::BareTool { raw, .. }
            | Self::McpExact { raw, .. }
            | Self::McpPrefix { raw, .. }
            | Self::ToolWithArgs { raw, .. }
            | Self::Unparseable { raw } => raw.as_str(),
        }
    }

    // -----------------------------------------------------------------------
    // Private parse helpers
    // -----------------------------------------------------------------------

    fn parse_tool_with_args(trimmed: &str, raw: &str) -> Self {
        // Find first `(` and last `)`.
        let paren_open = match trimmed.find('(') {
            Some(i) => i,
            None => unreachable!("caller guarantees '(' is present"),
        };
        let paren_close = match trimmed.rfind(')') {
            Some(i) => i,
            None => {
                // No closing paren — treat as unparseable.
                return Self::Unparseable {
                    raw: raw.to_owned(),
                };
            }
        };
        if paren_close <= paren_open {
            return Self::Unparseable {
                raw: raw.to_owned(),
            };
        }

        let tool = trimmed[..paren_open].trim().to_owned();
        let args = trimmed[paren_open + 1..paren_close].trim();

        let arg_matcher = match tool.as_str() {
            "Agent" => ArgMatcher::Exact(args.to_owned()),

            "Read" | "Edit" | "Write" | "MultiEdit" | "Glob" | "Grep" => {
                match globset::Glob::new(args) {
                    Ok(g) => ArgMatcher::PathGlob(g),
                    Err(_) => ArgMatcher::Ambiguous,
                }
            }

            "Bash" => {
                if let Some(prefix) = args.strip_suffix(":*") {
                    // "Bash(git status:*)" → prefix match
                    ArgMatcher::BashPrefix(prefix.trim_end().to_owned())
                } else {
                    // "Bash(git status)" → exact command match (produced by
                    // derive_pattern; also accepted from user-written patterns)
                    ArgMatcher::BashExact(args.to_owned())
                }
            }

            _ => ArgMatcher::Ambiguous,
        };

        Self::ToolWithArgs {
            tool,
            arg_matcher,
            raw: raw.to_owned(),
        }
    }

    fn parse_mcp(trimmed: &str, raw: &str) -> Self {
        // If the string ends with `*`, strip it and use as a prefix matcher.
        // Only a trailing `*` — mid-string wildcards are Unparseable.
        if let Some(without_star) = trimmed.strip_suffix('*') {
            let prefix = without_star.to_owned();
            // Sanity check: prefix should still start with mcp__ after stripping.
            if prefix.starts_with("mcp__") {
                return Self::McpPrefix {
                    prefix,
                    raw: raw.to_owned(),
                };
            }
            // Degenerate case like raw = "*" — treat as unparseable.
            return Self::Unparseable {
                raw: raw.to_owned(),
            };
        }

        Self::McpExact {
            full_id: trimmed.to_owned(),
            raw: raw.to_owned(),
        }
    }
}

// ---------------------------------------------------------------------------
// Argument matcher helper
// ---------------------------------------------------------------------------

/// Tools that use a file-path argument.
const FILE_PATH_TOOLS: &[&str] = &["Read", "Edit", "Write", "MultiEdit", "Glob", "Grep"];

fn match_tool_args(
    tool: &str,
    arg_matcher: &ArgMatcher,
    tool_input: &serde_json::Value,
) -> MatchResult {
    match arg_matcher {
        ArgMatcher::Exact(expected) => {
            if tool == "Agent" {
                // Match against tool_input["subagent_type"].
                match tool_input.get("subagent_type").and_then(|v| v.as_str()) {
                    Some(actual) if actual == expected.as_str() => MatchResult::Confident,
                    Some(_) => MatchResult::NoMatch, // field present, wrong value
                    None => MatchResult::NoMatch,    // field absent — not the allowed form
                }
            } else {
                // We don't know which field carries the arg semantics.
                MatchResult::Ambiguous
            }
        }

        ArgMatcher::PathGlob(glob) => {
            if !FILE_PATH_TOOLS.contains(&tool) {
                return MatchResult::Ambiguous;
            }
            // Try file_path first, then path (fallback for future tools).
            let path = tool_input
                .get("file_path")
                .or_else(|| tool_input.get("path"))
                .and_then(|v| v.as_str());

            match path {
                None => MatchResult::Ambiguous, // field absent — can't evaluate
                Some(p) => {
                    if glob.compile_matcher().is_match(p) {
                        MatchResult::Confident
                    } else {
                        MatchResult::NoMatch
                    }
                }
            }
        }

        ArgMatcher::BashPrefix(prefix) => {
            if tool != "Bash" {
                return MatchResult::Ambiguous;
            }
            match tool_input.get("command").and_then(|v| v.as_str()) {
                None => MatchResult::Ambiguous,
                Some(cmd) => {
                    if cmd.starts_with(prefix.as_str()) {
                        MatchResult::Confident
                    } else {
                        MatchResult::NoMatch
                    }
                }
            }
        }

        ArgMatcher::BashExact(expected) => {
            // Exact Bash command match — produced by derive_pattern; also
            // accepted when the user writes "Bash(some command)" by hand.
            if tool != "Bash" {
                return MatchResult::Ambiguous;
            }
            match tool_input.get("command").and_then(|v| v.as_str()) {
                None => MatchResult::Ambiguous,
                Some(cmd) => {
                    if cmd == expected.as_str() {
                        MatchResult::Confident
                    } else {
                        MatchResult::NoMatch
                    }
                }
            }
        }

        ArgMatcher::Ambiguous => MatchResult::Ambiguous,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper: build a minimal PreToolUseEvent-like tool_input Value.
    fn event_for(tool: &str, input: serde_json::Value) -> PreToolUseEvent {
        use ccbridge_proto::hook::{HookBase, PermissionMode};
        PreToolUseEvent {
            base: HookBase {
                session_id: "test".to_owned(),
                transcript_path: "/tmp/t.jsonl".to_owned(),
                cwd: "/tmp".to_owned(),
            },
            permission_mode: PermissionMode::Default,
            effort: None,
            tool_name: tool.to_owned(),
            tool_input: input,
            tool_use_id: "toolu_test".to_owned(),
            agent_id: None,
            agent_type: None,
        }
    }

    // -----------------------------------------------------------------------
    // Parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bare_tool_skill() {
        let p = Pattern::parse("Skill");
        assert!(matches!(p, Pattern::BareTool { ref name, .. } if name == "Skill"));
    }

    #[test]
    fn parse_mcp_exact() {
        let raw = "mcp__plugin_context7_context7__resolve-library-id";
        let p = Pattern::parse(raw);
        assert!(
            matches!(p, Pattern::McpExact { ref full_id, .. } if full_id == raw),
            "got {p:?}"
        );
    }

    #[test]
    fn parse_mcp_prefix_wildcard() {
        let p = Pattern::parse("mcp__plugin_backlog_tasks__*");
        match p {
            Pattern::McpPrefix { ref prefix, .. } => {
                assert_eq!(prefix, "mcp__plugin_backlog_tasks__");
            }
            _ => panic!("expected McpPrefix, got {p:?}"),
        }
    }

    #[test]
    fn parse_agent_exact_arg() {
        let p = Pattern::parse("Agent(task-planner)");
        match p {
            Pattern::ToolWithArgs {
                ref tool,
                arg_matcher: ArgMatcher::Exact(ref s),
                ..
            } => {
                assert_eq!(tool, "Agent");
                assert_eq!(s, "task-planner");
            }
            _ => panic!("expected ToolWithArgs/Exact, got {p:?}"),
        }
    }

    #[test]
    fn parse_read_path_glob() {
        let p = Pattern::parse("Read(**/.env*)");
        assert!(
            matches!(p, Pattern::ToolWithArgs { ref tool, arg_matcher: ArgMatcher::PathGlob(_), .. } if tool == "Read"),
            "got {p:?}"
        );
    }

    #[test]
    fn parse_edit_path_glob() {
        let p = Pattern::parse("Edit(**/*.pem)");
        assert!(
            matches!(p, Pattern::ToolWithArgs { ref tool, arg_matcher: ArgMatcher::PathGlob(_), .. } if tool == "Edit"),
            "got {p:?}"
        );
    }

    #[test]
    fn parse_bash_prefix() {
        let p = Pattern::parse("Bash(git status:*)");
        match p {
            Pattern::ToolWithArgs {
                ref tool,
                arg_matcher: ArgMatcher::BashPrefix(ref prefix),
                ..
            } => {
                assert_eq!(tool, "Bash");
                assert_eq!(prefix, "git status");
            }
            _ => panic!("expected BashPrefix, got {p:?}"),
        }
    }

    #[test]
    fn parse_bash_no_colon_star_uses_exact_matcher() {
        // Without ":*" suffix, "Bash(cmd)" produces an exact-command matcher
        // (introduced so derive_pattern can produce round-trippable patterns).
        let p = Pattern::parse("Bash(rm -rf)");
        assert!(
            matches!(p, Pattern::ToolWithArgs { ref tool, arg_matcher: ArgMatcher::BashExact(_), .. } if tool == "Bash"),
            "expected ToolWithArgs with BashExact matcher, got {p:?}"
        );
    }

    #[test]
    fn parse_unknown_tool_with_args_uses_ambiguous_matcher() {
        let p = Pattern::parse("FutureTool(some-arg)");
        assert!(
            matches!(
                p,
                Pattern::ToolWithArgs {
                    arg_matcher: ArgMatcher::Ambiguous,
                    ..
                }
            ),
            "expected ToolWithArgs with Ambiguous matcher, got {p:?}"
        );
    }

    #[test]
    fn parse_no_closing_paren_is_unparseable() {
        let p = Pattern::parse("Read(**/.env*");
        assert!(matches!(p, Pattern::Unparseable { .. }), "got {p:?}");
    }

    #[test]
    fn parse_empty_string_is_unparseable() {
        let p = Pattern::parse("");
        assert!(matches!(p, Pattern::Unparseable { .. }), "got {p:?}");
        let p2 = Pattern::parse("   ");
        assert!(matches!(p2, Pattern::Unparseable { .. }), "got {p2:?}");
    }

    #[test]
    fn parse_glob_compile_error_falls_back_to_ambiguous_matcher() {
        // A glob with unmatched `[` is invalid.
        let p = Pattern::parse("Read([invalid)");
        assert!(
            matches!(
                p,
                Pattern::ToolWithArgs {
                    arg_matcher: ArgMatcher::Ambiguous,
                    ..
                }
            ),
            "glob compile failure must produce Ambiguous matcher, got {p:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Matching tests
    // -----------------------------------------------------------------------

    #[test]
    fn bare_tool_matches_same_tool_name() {
        let p = Pattern::parse("Skill");
        let e = event_for("Skill", json!({}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn bare_tool_no_match_different_tool() {
        let p = Pattern::parse("Skill");
        let e = event_for("Bash", json!({}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn mcp_exact_matches_full_id() {
        let raw = "mcp__plugin_context7_context7__resolve-library-id";
        let p = Pattern::parse(raw);
        let e = event_for(raw, json!({}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn mcp_exact_no_match_partial_id() {
        let p = Pattern::parse("mcp__plugin_context7_context7__resolve-library-id");
        let e = event_for("mcp__plugin_context7_context7__query-docs", json!({}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn mcp_prefix_matches_method_in_server() {
        let p = Pattern::parse("mcp__plugin_backlog_tasks__*");
        let e = event_for("mcp__plugin_backlog_tasks__task_list", json!({}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn mcp_prefix_no_match_different_server() {
        let p = Pattern::parse("mcp__plugin_backlog_tasks__*");
        let e = event_for("mcp__plugin_context7_context7__query-docs", json!({}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn agent_exact_subagent_type_matches() {
        let p = Pattern::parse("Agent(task-planner)");
        let e = event_for("Agent", json!({"subagent_type": "task-planner"}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn agent_exact_different_subagent_type_no_match() {
        let p = Pattern::parse("Agent(task-planner)");
        let e = event_for("Agent", json!({"subagent_type": "code-reviewer"}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn agent_exact_missing_subagent_type_is_no_match() {
        // User allowed a specific subagent; a call without subagent_type is not
        // the thing they allowed — intercept normally, don't ambiguously "ask".
        let p = Pattern::parse("Agent(task-planner)");
        let e = event_for("Agent", json!({}));
        assert_eq!(
            p.matches(&e),
            MatchResult::NoMatch,
            "missing subagent_type must be NoMatch (not Ambiguous)"
        );
    }

    #[test]
    fn read_path_glob_matches_dotenv() {
        let p = Pattern::parse("Read(**/.env*)");
        let e = event_for(
            "Read",
            json!({"file_path": "/home/user/project/.env.production"}),
        );
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn read_path_glob_matches_pem_file() {
        let p = Pattern::parse("Read(**/*.pem)");
        let e = event_for("Read", json!({"file_path": "/etc/ssl/certs/server.pem"}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn read_path_glob_no_match_unrelated_path() {
        let p = Pattern::parse("Read(**/.env*)");
        let e = event_for("Read", json!({"file_path": "/home/user/README.md"}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn read_path_glob_missing_file_path_field_is_ambiguous() {
        let p = Pattern::parse("Read(**/.env*)");
        // No file_path in tool_input — can't evaluate, must not false-NoMatch.
        let e = event_for("Read", json!({}));
        assert_eq!(p.matches(&e), MatchResult::Ambiguous);
    }

    #[test]
    fn bash_prefix_matches_git_status_command() {
        let p = Pattern::parse("Bash(git status:*)");
        let e = event_for("Bash", json!({"command": "git status --porcelain"}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn bash_prefix_no_match_different_command() {
        let p = Pattern::parse("Bash(git status:*)");
        let e = event_for("Bash", json!({"command": "npm install"}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn unparseable_with_tool_name_in_raw_is_ambiguous() {
        // A pattern that names Bash but we can't parse.
        let p = Pattern::Unparseable {
            raw: "Bash[[invalid".to_owned(),
        };
        let e = event_for("Bash", json!({}));
        assert_eq!(p.matches(&e), MatchResult::Ambiguous);
    }

    #[test]
    fn unparseable_without_tool_name_is_no_match() {
        let p = Pattern::Unparseable {
            raw: "SomeWeirdSyntax".to_owned(),
        };
        let e = event_for("Bash", json!({}));
        assert_eq!(p.matches(&e), MatchResult::NoMatch);
    }

    #[test]
    fn globset_smoke_test_double_star_behavior() {
        // Verify that **/.env* matches both a bare filename and an absolute path.
        // This is a critical assumption underlying the deny-list patterns.
        let p = Pattern::parse("Read(**/.env*)");
        // Bare filename (no leading path).
        let e_bare = event_for("Read", json!({"file_path": ".env.production"}));
        // Absolute path with several components.
        let e_abs = event_for(
            "Read",
            json!({"file_path": "/home/user/dev/project/.env.production"}),
        );
        assert_eq!(
            p.matches(&e_bare),
            MatchResult::Confident,
            "**/.env* must match bare .env.production"
        );
        assert_eq!(
            p.matches(&e_abs),
            MatchResult::Confident,
            "**/.env* must match absolute path"
        );
    }

    #[test]
    fn read_ssh_glob_matches_ssh_file() {
        // Real-world deny pattern: "Read(**/.ssh/**)"
        let p = Pattern::parse("Read(**/.ssh/**)");
        let e = event_for("Read", json!({"file_path": "/home/user/.ssh/id_rsa"}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }

    #[test]
    fn read_secret_glob_matches_secret_file() {
        // Real-world deny pattern: "Read(**/*secret*)"
        let p = Pattern::parse("Read(**/*secret*)");
        let e = event_for("Read", json!({"file_path": "/tmp/my_secret_key.txt"}));
        assert_eq!(p.matches(&e), MatchResult::Confident);
    }
}
