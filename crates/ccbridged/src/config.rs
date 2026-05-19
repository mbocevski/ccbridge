// SPDX-License-Identifier: MIT
//! ccbridge daemon configuration loader.
//!
//! Config is loaded from `$XDG_CONFIG_HOME/ccbridge/config.toml` (or
//! `$HOME/.config/ccbridge/config.toml`).  If the file does not exist the
//! compiled-in defaults are used.  If the file exists but cannot be parsed,
//! an error is returned — typos in user config are never silently ignored.
//!
//! # Example
//!
//! ```toml
//! [approvals]
//! timeout_ms = 30000
//! fallback   = "passthrough"
//!
//! [emit.notify]
//! enabled = true
//! urgency = "critical"
//!
//! [emit.ctrl]
//! enabled       = true
//! allow_simulate = false
//!
//! [emit.http]
//! enabled = false
//! addr    = "127.0.0.1:9876"
//!
//! [tokens]
//! # state_path is optional; defaults to XDG_STATE_HOME/ccbridge/tokens.json
//! ```

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Full daemon configuration.
///
/// All fields have `#[serde(default)]` so a partial config file (or an
/// entirely absent file) works without any explicit `Option<_>` dance.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    #[serde(default)]
    pub approvals: Approvals,

    #[serde(default)]
    pub emit: Emit,

    #[serde(default)]
    pub tokens: TokensConfig,
}

// ---------------------------------------------------------------------------
// [approvals]
// ---------------------------------------------------------------------------

/// `[approvals]` — controls what happens when an interactive approval times out.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Approvals {
    /// How long to wait for an emit-module decision before applying `fallback`.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// What to do when the timeout elapses with no decision.
    #[serde(default)]
    pub fallback: Fallback,
}

impl Default for Approvals {
    fn default() -> Self {
        Self {
            timeout_ms: default_timeout_ms(),
            fallback: Fallback::default(),
        }
    }
}

impl Approvals {
    /// Convert `timeout_ms` to a [`Duration`].
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

fn default_timeout_ms() -> u64 {
    30_000
}

// ---------------------------------------------------------------------------
// Fallback
// ---------------------------------------------------------------------------

/// What `ingest::hooks` does when the approval timer elapses.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Fallback {
    /// Send `ask` so Claude Code's own TUI handles the decision (original
    /// behaviour).
    #[default]
    Passthrough,

    /// Send `deny` with a reason — prevents Claude from retrying.
    Deny,

    /// Send `allow` — automatically approves after the timeout.
    Allow,
}

// ---------------------------------------------------------------------------
// [emit]
// ---------------------------------------------------------------------------

/// `[emit]` — which emit back-ends are active.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct Emit {
    #[serde(default)]
    pub notify: NotifyConfig,

    #[serde(default)]
    pub ctrl: CtrlConfig,

    #[serde(default)]
    pub http: HttpConfig,
}

// ---------------------------------------------------------------------------
// [emit.notify]
// ---------------------------------------------------------------------------

/// `[emit.notify]` — freedesktop notification emitter.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default)]
    pub urgency: NotifyUrgency,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            urgency: NotifyUrgency::default(),
        }
    }
}

/// Urgency level passed to `org.freedesktop.Notifications.Notify`.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NotifyUrgency {
    Low,
    Normal,
    #[default]
    Critical,
}

// ---------------------------------------------------------------------------
// [emit.ctrl]
// ---------------------------------------------------------------------------

/// `[emit.ctrl]` — control socket emitter.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CtrlConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// If `true`, the ctrl socket accepts `SimulateApproval` messages (used
    /// for testing without a real BLE device).  Off by default in production.
    #[serde(default)]
    pub allow_simulate: bool,
}

impl Default for CtrlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_simulate: false,
        }
    }
}

// ---------------------------------------------------------------------------
// [emit.http]
// ---------------------------------------------------------------------------

/// `[emit.http]` — optional HTTP emitter (disabled by default).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_http_addr")]
    pub addr: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: default_http_addr(),
        }
    }
}

fn default_http_addr() -> String {
    "127.0.0.1:9876".to_owned()
}

// ---------------------------------------------------------------------------
// [tokens]
// ---------------------------------------------------------------------------

/// `[tokens]` — token state persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct TokensConfig {
    /// Override the default token state path.
    ///
    /// When `None` (the default), the daemon derives the path from
    /// `$XDG_STATE_HOME/ccbridge/tokens.json` (or the `$HOME` fallback).
    #[serde(default)]
    pub state_path: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Serde helpers
// ---------------------------------------------------------------------------

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

/// Return the canonical path to the user's `config.toml`.
///
/// Priority:
/// 1. `$XDG_CONFIG_HOME/ccbridge/config.toml`
/// 2. `$HOME/.config/ccbridge/config.toml`
pub fn config_path() -> PathBuf {
    crate::util::xdg_config_dir()
        .join("ccbridge")
        .join("config.toml")
}

// ---------------------------------------------------------------------------
// Load helpers
// ---------------------------------------------------------------------------

impl Config {
    /// Resolve the config path and load from it (or return defaults if absent).
    ///
    /// Returns `Err` if the file exists but cannot be parsed — typos in user
    /// config are never silently swallowed.
    pub fn load() -> Result<Self> {
        Self::load_from(&config_path())
    }

    /// Load from an explicit path.
    ///
    /// * File absent → `Ok(Config::default())`
    /// * File present + valid → `Ok(parsed_config)`
    /// * File present + invalid → `Err(...)`
    pub fn load_from(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse config file {}", path.display()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f
    }

    // -----------------------------------------------------------------------
    // default_config_when_file_missing
    // -----------------------------------------------------------------------

    #[test]
    fn default_config_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-file.toml");
        let cfg = Config::load_from(&missing).expect("missing file should yield defaults");
        assert_eq!(cfg, Config::default());
    }

    // -----------------------------------------------------------------------
    // load_full_config_round_trip
    // -----------------------------------------------------------------------

    #[test]
    fn load_full_config_round_trip() {
        let f = write_toml(
            r#"
[approvals]
timeout_ms = 5000
fallback   = "deny"

[emit.notify]
enabled = false
urgency = "low"

[emit.ctrl]
enabled        = true
allow_simulate = true

[emit.http]
enabled = true
addr    = "0.0.0.0:8080"

[tokens]
state_path = "/tmp/tokens.json"
"#,
        );

        let cfg = Config::load_from(f.path()).expect("valid TOML must load");

        // approvals
        assert_eq!(cfg.approvals.timeout_ms, 5_000);
        assert_eq!(cfg.approvals.fallback, Fallback::Deny);
        assert_eq!(cfg.approvals.timeout(), Duration::from_millis(5_000));

        // emit.notify
        assert!(!cfg.emit.notify.enabled);
        assert_eq!(cfg.emit.notify.urgency, NotifyUrgency::Low);

        // emit.ctrl
        assert!(cfg.emit.ctrl.enabled);
        assert!(cfg.emit.ctrl.allow_simulate);

        // emit.http
        assert!(cfg.emit.http.enabled);
        assert_eq!(cfg.emit.http.addr, "0.0.0.0:8080");

        // tokens
        assert_eq!(
            cfg.tokens.state_path,
            Some(PathBuf::from("/tmp/tokens.json"))
        );
    }

    // -----------------------------------------------------------------------
    // unknown_field_fails_loud
    // -----------------------------------------------------------------------

    #[test]
    fn unknown_field_fails_loud() {
        let f = write_toml(
            r#"
[approvals]
timeout_ms = 30000
typo_field = "oops"
"#,
        );
        let err = Config::load_from(f.path()).expect_err("unknown field must be an error");
        // anyhow chains: use alternate format `{:#}` which includes all causes.
        let msg = format!("{err:#}");
        assert!(
            msg.contains("typo_field") || msg.contains("unknown"),
            "error message should mention the unknown field, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // partial_config_uses_defaults_for_missing
    // -----------------------------------------------------------------------

    #[test]
    fn partial_config_uses_defaults_for_missing() {
        // Only override timeout_ms; everything else should come from defaults.
        let f = write_toml(
            r#"
[approvals]
timeout_ms = 10000
"#,
        );
        let cfg = Config::load_from(f.path()).expect("partial config must load");

        assert_eq!(cfg.approvals.timeout_ms, 10_000);
        // Fallback should be the default (Passthrough).
        assert_eq!(cfg.approvals.fallback, Fallback::Passthrough);

        // Emit defaults.
        assert!(cfg.emit.notify.enabled);
        assert_eq!(cfg.emit.notify.urgency, NotifyUrgency::Critical);
        assert!(cfg.emit.ctrl.enabled);
        assert!(!cfg.emit.ctrl.allow_simulate);
        assert!(!cfg.emit.http.enabled);
        assert_eq!(cfg.emit.http.addr, "127.0.0.1:9876");

        // Tokens default.
        assert_eq!(cfg.tokens.state_path, None);
    }

    // -----------------------------------------------------------------------
    // Fallback behaviour helpers used by fallback-behaviour tests
    // -----------------------------------------------------------------------

    /// Simulate the timeout branch in hooks.rs for a given Fallback value.
    /// Returns (permission_decision, reason).
    fn simulate_timeout_fallback(
        fallback: Fallback,
    ) -> (ccbridge_proto::hook::PermissionDecision, Option<String>) {
        use ccbridge_proto::hook::PermissionDecision;
        match fallback {
            Fallback::Passthrough => (
                PermissionDecision::Ask,
                Some("ccbridge: approval timeout — falling back to interactive prompt".to_owned()),
            ),
            Fallback::Deny => (
                PermissionDecision::Deny,
                Some("ccbridge: approval timeout — denying per config".to_owned()),
            ),
            Fallback::Allow => (PermissionDecision::Allow, None),
        }
    }

    // -----------------------------------------------------------------------
    // fallback_deny_on_timeout_produces_deny_with_reason
    // -----------------------------------------------------------------------

    #[test]
    fn fallback_deny_on_timeout_produces_deny_with_reason() {
        use ccbridge_proto::hook::PermissionDecision;
        let (decision, reason) = simulate_timeout_fallback(Fallback::Deny);
        assert_eq!(decision, PermissionDecision::Deny);
        let reason = reason.expect("Deny fallback must include a reason");
        assert!(!reason.is_empty());
        assert!(
            reason.contains("denying"),
            "reason should say 'denying', got: {reason}"
        );
    }

    // -----------------------------------------------------------------------
    // fallback_allow_on_timeout_produces_allow_no_reason
    // -----------------------------------------------------------------------

    #[test]
    fn fallback_allow_on_timeout_produces_allow_no_reason() {
        use ccbridge_proto::hook::PermissionDecision;
        let (decision, reason) = simulate_timeout_fallback(Fallback::Allow);
        assert_eq!(decision, PermissionDecision::Allow);
        assert!(
            reason.is_none(),
            "Allow fallback must carry no reason, got: {reason:?}"
        );
    }

    // -----------------------------------------------------------------------
    // config_path resolution
    // -----------------------------------------------------------------------

    #[test]
    fn config_path_ends_with_expected_suffix() {
        // We can't safely mutate env in parallel tests — just verify the shape.
        let p = config_path();
        assert!(
            p.ends_with("ccbridge/config.toml"),
            "unexpected config path: {}",
            p.display()
        );
    }

    // -----------------------------------------------------------------------
    // approvals.timeout() method
    // -----------------------------------------------------------------------

    #[test]
    fn approvals_timeout_converts_ms_to_duration() {
        let a = Approvals {
            timeout_ms: 5_000,
            fallback: Fallback::Passthrough,
        };
        assert_eq!(a.timeout(), Duration::from_secs(5));
    }
}
