// SPDX-License-Identifier: MIT
//! Per-file byte-offset tracking with `(dev, inode)` identity, so atomic
//! replace (tmp+rename) is detected and the offset reset rather than
//! seeking past valid data on the new inode.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};

/// Per-file tracking entry: byte offset plus the (dev, inode) identity at the
/// time of the last read.  When the identity changes (atomic replace via
/// tmp+rename), the offset is reset to 0 so the new file is read from the
/// beginning rather than seeking past valid data.
#[derive(Debug, Clone)]
struct FileOffsetEntry {
    offset: u64,
    dev: u64,
    inode: u64,
}

/// Tracks the last-read byte offset for each watched JSONL file, keyed by
/// path.  Includes `(dev, inode)` identity tracking so that atomic-write
/// replacements (tmp+rename, backup/sync tools) are detected and the offset
/// is reset rather than seeking past the new file's content.
pub struct FileOffsets {
    inner: HashMap<PathBuf, FileOffsetEntry>,
}

impl Default for FileOffsets {
    fn default() -> Self {
        Self::new()
    }
}

impl FileOffsets {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Scan `projects_dir` recursively, record the current end-of-file offset
    /// for every `*.jsonl` file.  New lines arriving after this call will be
    /// tailed; history is not replayed.
    pub fn snapshot_existing(&mut self, projects_dir: &Path) {
        let walker = walkdir_jsonl(projects_dir);
        for path in walker {
            match std::fs::metadata(&path) {
                Ok(meta) => {
                    self.inner.entry(path).or_insert(FileOffsetEntry {
                        offset: meta.len(),
                        dev: meta.dev(),
                        inode: meta.ino(),
                    });
                }
                Err(e) => {
                    warn!("jsonl: stat {} failed: {e}", path.display());
                }
            }
        }
    }

    /// Read new lines from `path` since the last recorded offset.
    /// Calls `on_line` for each new line (with trailing `\r?\n` stripped).
    ///
    /// If the file's `(dev, inode)` has changed since the last read (atomic
    /// replacement), the offset is reset to 0 so the new file is read from
    /// the beginning.
    ///
    /// Uses [`BufRead::read_line`] rather than [`BufRead::lines`] so that the
    /// raw byte count (including the newline bytes) is used to advance the
    /// offset — this correctly handles files whose last line has no trailing
    /// newline, and avoids the `lines() + 1` off-by-one that would occur in
    /// that case.
    pub fn drain_new_lines(&mut self, path: &Path, mut on_line: impl FnMut(&str)) {
        match std::fs::File::open(path) {
            Err(e) => {
                warn!("jsonl: open {} failed: {e}", path.display());
            }
            Ok(mut file) => {
                // Stat the open file to get its identity and check for replacement.
                let meta = match file.metadata() {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("jsonl: fstat {} failed: {e}", path.display());
                        return;
                    }
                };
                let current_dev = meta.dev();
                let current_ino = meta.ino();

                let entry = self
                    .inner
                    .entry(path.to_path_buf())
                    .or_insert(FileOffsetEntry {
                        offset: 0,
                        dev: current_dev,
                        inode: current_ino,
                    });

                // Reset offset if (dev, inode) changed — file was atomically replaced.
                if entry.dev != current_dev || entry.inode != current_ino {
                    debug!(
                        path = %path.display(),
                        "jsonl: file identity changed (atomic replace?) — resetting offset to 0",
                    );
                    entry.offset = 0;
                    entry.dev = current_dev;
                    entry.inode = current_ino;
                }

                if let Err(e) = file.seek(SeekFrom::Start(entry.offset)) {
                    warn!("jsonl: seek {} failed: {e}", path.display());
                    return;
                }
                let mut reader = BufReader::new(&mut file);
                let mut bytes_read: u64 = 0;
                loop {
                    let mut raw = String::new();
                    match reader.read_line(&mut raw) {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            bytes_read += n as u64;
                            // Strip trailing \r\n or \n before passing to callback.
                            let trimmed = raw.trim_end_matches('\n').trim_end_matches('\r');
                            on_line(trimmed);
                        }
                        Err(e) => {
                            warn!("jsonl: read_line in {} failed: {e}", path.display());
                            break;
                        }
                    }
                }
                entry.offset += bytes_read;
            }
        }
    }
}

/// Recursively walk `dir` and yield paths to `*.jsonl` files.
pub(super) fn walkdir_jsonl(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_dir_inner(dir, &mut out);
    out
}

fn walk_dir_inner(dir: &Path, out: &mut Vec<PathBuf>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_dir_inner(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
}
