//! Packed graph adjacency: the merged reader over blocks and per-edge keys.
//!
//! A vertex's adjacency lives in two places once its table has folded
//! (`TableDefinition::graph_folded`): packed [`AdjacencyBlock`]s under the
//! `^` key family hold the folded majority, and the per-edge pointer keys
//! under `~` hold the unfolded tail plus tombstones over folded edges.
//! [`merge::MergedAdjacencyCursor`] is the one reader over both: it merges
//! the two streams in pointer-key order, applies delta-wins deduplication
//! and tombstone subtraction, and emits each edge exactly once, in exactly
//! the order — and with exactly the payload — a pre-blocks key scan would
//! have produced.
//!
//! While a table has never folded, the cursor collapses to the plain
//! keys-only scan the engine has always done, at the same cost.
//!
//! [`AdjacencyBlock`]: surrealdb_datastore::values::graph::AdjacencyBlock

pub mod fold;
pub mod merge;
#[cfg(test)]
mod tests;

use anyhow::Result;
use surrealdb_datastore::Transaction;

pub use self::fold::{FoldOutcome, fold_scope};
pub use self::merge::{AdjacencyScope, MergeStats, MergedAdjacencyCursor, MergedAdjacencyEdge};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

/// Whether the given vertex table has folded adjacency, from the
/// transaction-cached catalog.
///
/// A table absent from the catalog has never been defined, so nothing can
/// have folded it.
pub async fn vertex_table_folded(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<bool> {
	Ok(txn.get_tb(ns, db, tb, None).await?.is_some_and(|tb| tb.graph_folded))
}
