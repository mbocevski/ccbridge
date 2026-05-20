// SPDX-License-Identifier: MIT
//! Pattern derivation: hook event → most-conservative allowlist pattern.
//!
//! See the `additions` module-level docs for the derivation table and the
//! `Pattern::parse(s).matches(event) == Confident` round-trip invariant.

use ccbridge_proto::hook::PreToolUseEvent;

/// Result of [`derive_pattern`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedPattern {
    /// A specific, literal pattern — write it directly to settings.json.
    Specific(String),

    /// A bare tool name (e.g. `Bash` with no args) — requires explicit
    /// secondary confirmation before writing, since it allows ALL calls to
    /// this tool.
    BareToolNeedsConfirmation { tool: String },
}

/// Derive the most-conservative `permissions.allow` pattern for a tool call.
///
/// See module documentation for the full derivation table and the round-trip
/// invariant.
pub fn derive_pattern(event: &PreToolUseEvent) -> DerivedPattern {
    let tool = event.tool_name.as_str();

    // MCP methods — always specific (exact ID).
    if tool.starts_with("mcp__") {
        return DerivedPattern::Specific(tool.to_owned());
    }

    match tool {
        // Derive a literal command match.  Defensive: only accept a JSON
        // string — don't coerce numbers or booleans to strings.
        "Bash" => specific_or_bare(tool, event.tool_input.get("command"), |v| {
            format!("Bash({v})")
        }),

        // Path-based tools — derive an exact-path pattern.
        "Read" | "Edit" | "Write" | "MultiEdit" => {
            specific_or_bare(tool, event.tool_input.get("file_path"), |v| {
                format!("{tool}({v})")
            })
        }

        "Agent" => specific_or_bare(tool, event.tool_input.get("subagent_type"), |v| {
            format!("Agent({v})")
        }),

        // Glob and Grep use input fields ("pattern", "path") that our matcher
        // doesn't currently map to Confident matches.  Deriving a pattern that
        // the matcher wouldn't recognize as Confident would violate the
        // round-trip invariant, so we fall to BareToolNeedsConfirmation.
        // This is a known limitation; improve the matcher in a follow-up task.
        _ => DerivedPattern::BareToolNeedsConfirmation {
            tool: tool.to_owned(),
        },
    }
}

/// Extract a string value from a JSON field and format it into a specific
/// pattern, or fall back to `BareToolNeedsConfirmation`.
///
/// `field_value` is `Option<&serde_json::Value>` from `tool_input.get(key)`.
/// `fmt` converts the string value to the final pattern string.
fn specific_or_bare(
    tool: &str,
    field_value: Option<&serde_json::Value>,
    fmt: impl FnOnce(&str) -> String,
) -> DerivedPattern {
    match field_value.and_then(|v| v.as_str()) {
        Some(s) => DerivedPattern::Specific(fmt(s)),
        None => DerivedPattern::BareToolNeedsConfirmation {
            tool: tool.to_owned(),
        },
    }
}
