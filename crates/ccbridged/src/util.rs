// SPDX-License-Identifier: MIT
//! Cross-cutting utilities shared by the daemon.

use std::path::PathBuf;

use anyhow::Result;

/// Resolve `$XDG_STATE_HOME` or fall back to `$HOME/.local/state`.
///
/// Returns `Err` when neither variable is set (misconfigured system).
pub fn xdg_state_dir() -> Result<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(xdg));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".local").join("state"));
    }
    anyhow::bail!("neither $XDG_STATE_HOME nor $HOME is set");
}

/// Resolve `$XDG_CONFIG_HOME` or fall back to `$HOME/.config`.
///
/// Returns `Err` when neither variable is set (misconfigured system), matching
/// the behaviour of [`xdg_state_dir`].
pub fn xdg_config_dir() -> Result<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".config"));
    }
    anyhow::bail!("neither $XDG_CONFIG_HOME nor $HOME is set");
}

/// Truncate a session/tool-use id to its first 6 characters for log lines
/// and notification bodies.
pub fn short_session_id(id: &str) -> String {
    id.chars().take(6).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xdg_state_dir_succeeds_in_test_env() {
        // HOME or XDG_STATE_HOME is always set in CI and developer environments.
        let dir = xdg_state_dir().expect("xdg_state_dir should succeed");
        assert!(dir.ends_with(".local/state") || std::env::var_os("XDG_STATE_HOME").is_some());
    }

    #[test]
    fn xdg_config_dir_succeeds_in_test_env() {
        let dir = xdg_config_dir().expect("xdg_config_dir should succeed");
        assert!(dir.ends_with(".config") || std::env::var_os("XDG_CONFIG_HOME").is_some());
    }

    #[test]
    fn short_session_id_takes_six_chars() {
        assert_eq!(
            short_session_id("3cb58992-935c-4fdd-9efd-1f160946e822"),
            "3cb589"
        );
        assert_eq!(short_session_id("abc"), "abc");
        assert_eq!(short_session_id(""), "");
    }
}
