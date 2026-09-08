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
pub mod resolve;
#[cfg(test)]
mod tests;

use anyhow::Result;
use surrealdb_datastore::Transaction;

pub use self::fold::{FoldOutcome, fold_scope};
pub use self::merge::{
	AdjacencyScope, MergeStats, MergedAdjacencyCursor, MergedAdjacencyEdge, VisitFlow,
};
pub use self::resolve::{AdjacencyResolveCache, resolve_doc_ids};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
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

/// How a record's own adjacency subspace is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VertexAdjacency {
	/// Physical keys, merged with packed blocks once the table has folded.
	Normal {
		folded: bool,
	},
	/// The record is a lightweight edge: it writes no edge-side keys, so
	/// its adjacency is synthesized from its `[in, out]` id — see
	/// [`MergedAdjacencyCursor::open_lightweight`].
	Lightweight,
}

/// Classifies how a table's records' adjacency is read, from an
/// already-fetched catalog definition.
///
/// A table absent from the catalog has never been defined, so nothing can
/// have folded it and no record can be a lightweight edge. Callers
/// classifying many records should fetch each distinct table once and reuse
/// this — a catalog lookup per record is measurable on hot scans.
pub fn vertex_adjacency_of(tb: Option<&TableDefinition>) -> VertexAdjacency {
	match tb {
		Some(tb) => match &tb.table_type {
			crate::catalog::TableType::Relation(rel) if rel.lightweight => {
				VertexAdjacency::Lightweight
			}
			_ => VertexAdjacency::Normal {
				folded: tb.graph_folded,
			},
		},
		None => VertexAdjacency::Normal {
			folded: false,
		},
	}
}

/// Classifies how the given source table's records' adjacency is read,
/// from the transaction-cached catalog.
pub async fn vertex_adjacency(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<VertexAdjacency> {
	Ok(vertex_adjacency_of(txn.get_tb(ns, db, tb, None).await?.as_deref()))
}
