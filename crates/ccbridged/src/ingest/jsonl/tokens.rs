// SPDX-License-Identifier: MIT
//! Persisted token state — `~/.local/state/ccbridge/tokens.json`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// The on-disk representation of the token state.
///
/// Stored at `$XDG_STATE_HOME/ccbridge/tokens.json`.
/// Falls back to `$HOME/.local/state/ccbridge/tokens.json`.
///
/// Wire shape:
/// ```json
/// {"date": "2026-05-19", "today": 31200, "cumulative": 184502}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedTokens {
    /// The date this `today` count belongs to, `"YYYY-MM-DD"` (UTC).
    pub date: String,
    /// Output tokens since local midnight.
    pub today: u64,
    /// Cumulative output tokens (monotonically increasing, resets with the file).
    pub cumulative: u64,
}

impl PersistedTokens {
    /// Load from disk.  Returns [`Default`] if the file does not exist.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
    }

    /// Atomically write to `path` (write to `path.tmp`, then rename).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }
}

/// Return the path of the token state file.
///
/// Priority:
/// 1. `$XDG_STATE_HOME/ccbridge/tokens.json`
/// 2. `$HOME/.local/state/ccbridge/tokens.json`
/// 3. `Err` — both variables unset (misconfigured system).
///
/// The caller should log the error and disable token persistence rather than
/// falling back to `/tmp` (world-readable, collision-prone on multi-user boxes).
pub fn tokens_state_path() -> Result<PathBuf> {
    Ok(crate::util::xdg_state_dir()?
        .join("ccbridge")
        .join("tokens.json"))
}
