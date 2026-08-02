//! Indexing: the engines, and the planner that chooses between them.
//!
//! The engines themselves live one crate down, in [`surrealdb_idx`], and are
//! re-exported here so every `crate::idx::…` path in core keeps resolving. They
//! answer from a transaction and an index-environment handle alone, which is
//! what lets them sit below the executor.
//!
//! What stays here is the half that needs the executor: [`planner`] decides
//! which index a statement should use and iterates it on the statement's
//! behalf, so it reads the query context, the document machinery and the
//! iterator stack — none of which an engine can see.

pub use surrealdb_idx::*;

pub mod planner;
#[cfg(test)]
mod tests;
