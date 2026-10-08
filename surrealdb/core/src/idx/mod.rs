//! Indexing: the engines.
//!
//! The engines themselves live one crate down, in [`surrealdb_idx`]; depend on
//! that crate directly to reach them. They are pulled in here crate-internally
//! so core's own `crate::idx::…` paths resolve. They answer from a transaction
//! and an index-environment handle alone, which is what lets them sit below the
//! executor.

pub(crate) use surrealdb_idx::*;

#[cfg(test)]
mod tests;
