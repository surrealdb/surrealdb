#![recursion_limit = "256"]
// Test-only lint allowances hoisted from the individual test files that were
// consolidated into this single `it` integration-test binary.
#![allow(clippy::unwrap_used)]
#![allow(clippy::regex_creation_in_loops)]
#![allow(clippy::clone_on_ref_ptr)]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

// Shared helpers, compiled once for the whole `it` binary instead of once per
// integration test crate.
mod helpers;

#[macro_use]
mod remove_macros;

mod access;
mod alter;
mod api_scope;
mod api_security;
mod asyncevent;
mod auth_limit;
mod cache;
mod changefeeds;
mod concurrency;
mod create;
mod define;
mod delete;
mod export;
mod field;
mod function;
mod future;
mod index;
mod index_build_shutdown;
mod info;
mod insert;
mod live;
mod merge;
mod param;
mod query;
mod relate;
mod remove;
mod round_trip;
mod select;
mod sequence;
mod slow_log_aborted;
mod statement_count_scaling;
mod table;
mod timeout;
mod update;
mod upsert;
mod use_auth;
mod write_gate_computed;

#[cfg(not(target_family = "wasm"))]
mod complex;

#[cfg(feature = "scripting")]
mod script;
