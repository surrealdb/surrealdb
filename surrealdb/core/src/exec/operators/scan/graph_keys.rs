//! Graph adjacency scan machinery, shared across operators.
//!
//! This module owns the low-level pieces that turn a `(record id, direction,
//! edge tables)` triple into adjacency ranges and enumerate the edges those
//! ranges hold. It was extracted from [`super::graph`] so that operators
//! other than `GraphEdgeScan` (the GQL `Expand` / `PathExpand` operators,
//! which must enumerate edges *per input row* rather than flattening the
//! correlation away) can reuse the exact same range computation and
//! enumeration logic.
//!
//! Enumeration goes through the merged adjacency reader
//! ([`MergedAdjacencyCursor`]), which serves packed blocks and per-edge
//! pointer keys as one stream; on a vertex table that has never folded it
//! collapses to the plain keys-only scan. A [`GraphRange`] also carries a
//! [`DecodedGraphPrefix`] decoder for the few call sites that read the
//! vertex-side range directly instead of through the merged reader —
//! currently the bitmap-fusion reachability leaf and the legacy-format
//! fallback scan of an edge's own (never-folded) adjacency.
//!
//! `GraphEdgeScan` is layered unchanged on top of these helpers.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use surrealdb_datastore::values::graph::GraphFoldScope;

use super::common::evaluate_bound_key;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{ControlFlowExt, ExecutionContext, PhysicalExpr};
use crate::expr::{ControlFlow, Dir};
use crate::idx::adjacency::{
	AdjacencyScope, MergeStats, MergedAdjacencyCursor, MergedAdjacencyEdge, VertexAdjacency,
	vertex_adjacency,
};
/// The decoder re-exported so scan and expand operators don't need to reach
/// into the `key::schema` module directly for the range-decode call sites.
pub(crate) use crate::key::schema::DecodedGraphPrefix;
use crate::key::schema::{GraphDirPrefix, GraphForeignTablePrefix};
use crate::key::{KVSubspace, RawRange};
use crate::kvs::Transaction;
use crate::val::{RecordId, RecordIdKey, TableName};

/// Specification for an edge table to scan, optionally with ID range bounds.
///
/// When range bounds are present, the scan is restricted to edges whose IDs fall
/// within the specified range instead of scanning the entire table.
#[derive(Debug, Clone)]
pub struct EdgeTableSpec {
	/// The edge table name (e.g., `edge`, `knows`)
	pub table: TableName,
	/// Range start bound. When `Unbounded`, starts from the table prefix.
	pub range_start: Bound<Arc<dyn PhysicalExpr>>,
	/// Range end bound. When `Unbounded`, ends at the table suffix.
	pub range_end: Bound<Arc<dyn PhysicalExpr>>,
}

/// One adjacency slice to scan for a single record + direction: the
/// vertex-side key range, the decoder for keys read directly from it, and
/// the scope fields the merged reader needs to mirror it onto the
/// packed-block side.
///
/// The decoder's stripped prefix covers every route field the range's bound
/// fixes — through the direction for a wildcard scan, through the edge table
/// for a per-table scan — so per-key decoding starts at the first field that
/// varies inside this range. It is unused by callers that read through
/// [`MergedAdjacencyCursor`], which decodes internally.
pub(crate) struct GraphRange {
	/// The edge table the range is restricted to; `None` spans all of them.
	pub(crate) edge_table: Option<TableName>,
	/// The evaluated lower foreign-key bound, when the spec had one — the
	/// block side's chunk-routing hint.
	pub(crate) fk_lower: Option<RecordIdKey>,
	/// The vertex-side key range, bounds applied.
	pub(crate) range: RawRange,
	/// The decoder for every key this range returns.
	pub(crate) decoder: DecodedGraphPrefix,
}

impl GraphRange {
	/// The merged reader's borrowed view of this range.
	pub(crate) fn as_adjacency_scope<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		vertex: &'a RecordId,
		dir: Dir,
	) -> AdjacencyScope<'a> {
		AdjacencyScope {
			ns,
			db,
			vertex,
			dir: Some(dir),
			edge_table: self.edge_table.as_ref(),
			fk_lower: self.fk_lower.as_ref(),
			delta_range: self.range.clone(),
		}
	}
}

/// Compute all KV key ranges to scan for a single record + direction, each
/// paired with the decoder for its keys and the scope fields the merged
/// reader needs.
///
/// When `edge_tables` is empty, returns a single wildcard range covering all
/// edges in the given direction. Otherwise returns one range per edge table,
/// respecting any range bounds on each [`EdgeTableSpec`].
pub(crate) async fn compute_graph_ranges(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	edge_tables: &[EdgeTableSpec],
	ctx: &ExecutionContext,
) -> Result<Vec<GraphRange>, ControlFlow> {
	if edge_tables.is_empty() {
		// Scan all edges in this direction. The bound is encoded once and
		// serves both sides of the pair: everything strictly beneath it is
		// the range, and its bytes are the prefix every returned key repeats.
		let prefix = GraphDirPrefix {
			ns: ns_id,
			db: db_id,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			dir,
		};
		let bound = prefix.encode_bound()?;
		let range = prefix.raw(bound.clone().prefix_expect());
		Ok(vec![GraphRange {
			edge_table: None,
			fk_lower: None,
			range,
			decoder: DecodedGraphPrefix::from_dir_bound(bound),
		}])
	} else {
		let mut ranges = Vec::with_capacity(edge_tables.len());
		for spec in edge_tables {
			// The spec's bounds constrain the edge's foreign key, the field that
			// follows the foreign table in the adjacency layout, so the range is
			// cut on that field of the foreign-table bound — and every key in it
			// shares the bound's bytes through the edge table, which the decoder
			// therefore supplies instead of re-decoding per key.
			let start = eval_fk_bound(&spec.range_start, ctx).await?;
			let end = eval_fk_bound(&spec.range_end, ctx).await?;
			let fk_lower = match &start {
				Bound::Included(key) | Bound::Excluded(key) => Some(key.clone()),
				Bound::Unbounded => None,
			};

			let prefix = GraphForeignTablePrefix {
				ns: ns_id,
				db: db_id,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir,
				foreign_table: Cow::Borrowed(&spec.table),
			};
			let decoder = DecodedGraphPrefix::from_foreign_table_bound(
				prefix.encode_bound()?,
				spec.table.clone(),
			);
			let range = prefix.range_where((as_cow_bound(&start), as_cow_bound(&end)))?;

			ranges.push(GraphRange {
				edge_table: Some(spec.table.clone()),
				fk_lower,
				range,
				decoder,
			});
		}
		Ok(ranges)
	}
}

fn as_cow_bound<'a>(bound: &'a Bound<RecordIdKey>) -> Bound<Cow<'a, RecordIdKey>> {
	match bound {
		Bound::Included(key) => Bound::Included(Cow::Borrowed(key)),
		Bound::Excluded(key) => Bound::Excluded(Cow::Borrowed(key)),
		Bound::Unbounded => Bound::Unbounded,
	}
}

/// Enumerate every edge of one record + direction through the merged
/// adjacency reader, handing each edge to `visit` as it is produced.
///
/// This is the whole-source enumeration the GQL expand operators do per
/// input row; `GraphEdgeScan` drives the ranges and cursor itself for its
/// batching, LIMIT and resume machinery. The visitor keeps the fan-out
/// streaming — only what it chooses to retain is buffered, never the whole
/// unfiltered adjacency — and must not hold the transaction borrow (store
/// plain values; the cursor is dropped before the caller can use `txn`
/// again). `adjacency_memo` caches the per-table adjacency classification
/// across calls within one operator execution; `get_tb` is
/// transaction-cache-stable within a statement, so the memo returns what each
/// per-row lookup would. The returned stats accumulate across all ranges.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn collect_adjacency(
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	edge_tables: &[EdgeTableSpec],
	ctx: &ExecutionContext,
	version: Option<u64>,
	adjacency_memo: &mut HashMap<TableName, VertexAdjacency>,
	mut visit: impl FnMut(MergedAdjacencyEdge) -> Result<(), ControlFlow>,
) -> Result<MergeStats, ControlFlow> {
	let ranges = compute_graph_ranges(ns_id, db_id, rid, dir, edge_tables, ctx).await?;
	let adjacency = vertex_adjacency_memo(txn, ns_id, db_id, &rid.table, adjacency_memo)
		.await
		.map_err(ControlFlow::Err)?;
	// A lightweight edge's synthesized adjacency derives from its id alone;
	// gate on the edge existing at this snapshot so a deleted or
	// never-created id expands to nothing.
	if adjacency == VertexAdjacency::Lightweight
		&& !txn
			.record_exists(ns_id, db_id, &rid.table, &rid.key, version)
			.await
			.map_err(ControlFlow::Err)?
	{
		return Ok(MergeStats::default());
	}
	let fold_threshold = ctx.root().ctx.config.idx.graph_fold_threshold;
	let mut stats = MergeStats::default();
	for range in &ranges {
		let mut cursor = match adjacency {
			VertexAdjacency::Lightweight => MergedAdjacencyCursor::open_lightweight(
				txn,
				&range.as_adjacency_scope(ns_id, db_id, rid, dir),
			)
			.context("Failed to open graph cursor")?,
			VertexAdjacency::Normal {
				folded,
			} => MergedAdjacencyCursor::open(
				txn,
				range.as_adjacency_scope(ns_id, db_id, rid, dir),
				folded,
				version,
				Some(ctx.root().ctx.get_index_stores().adjacency_resolve()),
			)
			.await
			.context("Failed to open graph cursor")?,
		};
		loop {
			crate::exec::operators::check_cancelled(ctx)?;
			// The expand operators consume edge/target identities only, so
			// the scan variant skips the per-edge key copies.
			let batch = cursor
				.next_batch_scan(crate::kvs::NORMAL_BATCH_SIZE)
				.await
				.context("Failed to scan graph edge")?;
			if batch.is_empty() {
				break;
			}
			for item in batch {
				visit(item)?;
			}
		}
		let range_stats = cursor.stats();
		stats.block_hits += range_stats.block_hits;
		stats.delta_hits += range_stats.delta_hits;
	}
	observe_fold_candidate(txn, ns_id, db_id, rid, dir, stats.delta_hits, fold_threshold);
	Ok(stats)
}

/// As [`vertex_adjacency`], memoized per [`TableName`] for the lifetime of
/// one operator execution. The answer is invariant per table within a
/// statement (`get_tb` is transaction-cache-stable), so the memo exists
/// only to skip the boxed catalog lookup the plain call pays per row.
pub(crate) async fn vertex_adjacency_memo(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	memo: &mut HashMap<TableName, VertexAdjacency>,
) -> AnyResult<VertexAdjacency> {
	if let Some(adjacency) = memo.get(table) {
		return Ok(*adjacency);
	}
	let adjacency = vertex_adjacency(txn, ns, db, table).await?;
	memo.insert(table.clone(), adjacency);
	Ok(adjacency)
}

/// Observe a (vertex, direction) range for a background fold when a scan
/// found it carrying at least `threshold` unfolded per-edge keys. A zero
/// threshold disables fold initiation — no new observations — but not
/// debt repayment: the background drain keeps consuming durable triggers
/// queued by edge deletes on already-folded tables. Keys a fold cannot
/// pack — legacy-format keys, and values carrying a section this build
/// does not interpret — count toward the tally but are never drained by
/// one; the observation set's drop-on-full bound keeps re-observing such
/// ranges harmless.
pub(crate) fn observe_fold_candidate(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	rid: &RecordId,
	dir: Dir,
	delta_hits: u64,
	threshold: usize,
) {
	if threshold == 0 || (delta_hits as usize) < threshold {
		return;
	}
	txn.observe_graph_fold(GraphFoldScope {
		ns,
		db,
		tb: rid.table.clone(),
		id: rid.key.clone(),
		dir,
	});
}

/// Evaluate one [`EdgeTableSpec`] bound into the foreign-key bound a graph range
/// is cut on.
///
/// The bound kind carries over unchanged; only the expression inside it is
/// evaluated, so an inclusive spec bound stays inclusive on the key.
async fn eval_fk_bound(
	bound: &Bound<Arc<dyn PhysicalExpr>>,
	ctx: &ExecutionContext,
) -> Result<Bound<RecordIdKey>, ControlFlow> {
	Ok(match bound {
		Bound::Included(expr) => Bound::Included(evaluate_bound_key(expr, ctx).await?),
		Bound::Excluded(expr) => Bound::Excluded(evaluate_bound_key(expr, ctx).await?),
		Bound::Unbounded => Bound::Unbounded,
	})
}
