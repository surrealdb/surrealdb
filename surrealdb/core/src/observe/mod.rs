//! Facade over the leaf [`surrealdb_observe`] crate, plus the core-side residue
//! that cannot live in the leaf.
//!
//! The observability event types, the [`ExecutionObserver`] trait, the fan-out
//! dispatcher, the error-class constants, and the provider trait all live in
//! `surrealdb-observe` and are re-exported here so existing `crate::observe::*`
//! (core) and `surrealdb_core::observe::*` (server) paths keep resolving
//! unchanged.
//!
//! What stays in core:
//!
//! - [`process`]: the process resource snapshot reads `crate::sys`, a private core module.
//! - The `impl`s of the observability provider traits for `CommunityComposer` (a core type, so the
//!   orphan rule pins them here). The trait surface itself is re-exported from the leaf.
//! - Session/expr/error glue that has to read core engine types lives next to those types
//!   (statement classification and `anyhow` error classification in `crate::dbs::executor`;
//!   event-context construction in `crate::dbs` as `From<&Session>`).

pub mod process;

// Core-side provider impls for `CommunityComposer`. No public items — the trait
// surface is re-exported from the leaf below (as `provider`).
mod provider_impls;

pub use process::{ProcessSnapshot, process_snapshot, refresh_process_snapshot};
pub use surrealdb_observe::*;

/// Re-exported at its original path. The function had to move into `dbs` to
/// keep the leaf crate free of `crate::kvs`, but it was the one item of this
/// module's public surface that the move would otherwise have removed, and an
/// observer outside this repo has no way to rebuild it: the sibling
/// `classify_types_error` needs an already-converted `surrealdb_types::Error`.
pub use crate::dbs::executor::classify_anyhow_error;
