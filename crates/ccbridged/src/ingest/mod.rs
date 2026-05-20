// SPDX-License-Identifier: MIT
//! Hook ingest + JSONL tail.
//!
//! `hooks` accepts ccbridge-hook subprocess connections on a unix
//! socket and forwards each event into the aggregator.
//! `jsonl` watches `~/.claude/projects/**/*.jsonl` for new assistant
//! lines and turns their `output_tokens` field into a TokensUpdate
//! flowing into the aggregator.

pub mod hooks;
pub mod jsonl;
