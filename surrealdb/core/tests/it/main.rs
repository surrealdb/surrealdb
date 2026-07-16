#![recursion_limit = "256"]
// Test-only lint allowances hoisted from the individual test files that were
// consolidated into this single `it` integration-test binary.
#![allow(clippy::unwrap_used)]
#![allow(clippy::regex_creation_in_loops)]
#![allow(clippy::clone_on_ref_ptr)]

// Shared helpers, compiled once for the whole `it` binary instead of once per
// integration test crate.
mod helpers;

#[macro_use]
mod remove_macros;

mod access;
mod alter;
mod api_scope;
mod asyncevent;
mod auth_limit;
mod cache;
mod changefeeds;
mod create;
mod define;
mod delete;
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
mod select;
mod sequence;
mod table;
mod timeout;
mod update;
mod upsert;
mod use_auth;

#[cfg(not(target_family = "wasm"))]
mod complex;

#[cfg(feature = "scripting")]
mod script;
