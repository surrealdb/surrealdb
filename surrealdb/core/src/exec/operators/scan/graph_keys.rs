//! Graph adjacency key-scan machinery, shared across operators.
//!
//! This module owns the low-level pieces that turn a `(record id, direction,
//! edge tables)` triple into KV key ranges and decode the adjacency keys those
//! ranges return. It was extracted from [`super::graph`] so that operators
//! other than `GraphEdgeScan` (the GQL `Expand` / `PathExpand` operators, which
//! must enumerate edges *per input row* rather than flattening the correlation
//! away) can reuse the exact same range computation and decode logic.
//!
//! `GraphEdgeScan` is layered unchanged on top of these helpers.

use std::borrow::Cow;
use std::ops::Bound;
use std::sync::Arc;

use super::common::evaluate_bound_key;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{ControlFlowExt, ExecutionContext, PhysicalExpr};
use crate::expr::{ControlFlow, Dir};
use crate::key::TypedRange;
/// Adjacency key decode result, re-exported so callers of [`decode_graph_edge`]
/// don't need to reach into the `key::graph` module directly.
pub(crate) use crate::key::schema::DecodedGraph;
use crate::key::schema::{GraphDirPrefix, GraphForeignTablePrefix};
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

/// Compute all KV key ranges to scan for a single record + direction.
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
) -> Result<Vec<TypedRange<()>>, ControlFlow> {
	if edge_tables.is_empty() {
		// Scan all edges in this direction
		Ok(vec![
			GraphDirPrefix {
				ns: ns_id,
				db: db_id,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir,
			}
			.range()?,
		])
	} else {
		let mut ranges = Vec::with_capacity(edge_tables.len());
		for spec in edge_tables {
			// The spec's bounds constrain the edge's foreign key, the field that
			// follows the foreign table in the adjacency layout, so the range is
			// cut on that field of the foreign-table bound.
			let start = eval_fk_bound(&spec.range_start, ctx).await?;
			let end = eval_fk_bound(&spec.range_end, ctx).await?;

			ranges.push(
				GraphForeignTablePrefix {
					ns: ns_id,
					db: db_id,
					tb: Cow::Borrowed(&rid.table),
					id: Cow::Borrowed(&rid.key),
					dir,
					foreign_table: Cow::Borrowed(&spec.table),
				}
				.range_where((start.map(Cow::Owned), end.map(Cow::Owned)))?,
			);
		}
		Ok(ranges)
	}
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

/// Decode a graph key. For legacy keys, returns the edge id; for new-format
/// keys, also returns the embedded target vertex.
///
/// Thin wrapper over [`DecodedGraph::decode`] that converts the anyhow error
/// into a [`ControlFlow`] with a consistent context string, so call sites in the
/// scan pipeline can stay on `?`.
pub(crate) fn decode_graph_edge(key: &[u8]) -> Result<DecodedGraph, ControlFlow> {
	DecodedGraph::decode(key).context("Failed to decode graph key")
}
