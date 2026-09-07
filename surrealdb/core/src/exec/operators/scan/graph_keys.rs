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
use crate::exec::{ExecutionContext, PhysicalExpr};
use crate::expr::{ControlFlow, Dir};
/// Adjacency key decode types, re-exported so scan and expand operators don't
/// need to reach into the `key::schema` module directly.
pub(crate) use crate::key::schema::{DecodedGraphPrefix, DecodedGraphTarget};
use crate::key::schema::{GraphDirPrefix, GraphForeignTablePrefix};
use crate::key::{KVRange, KVSubspace, TypedRange};
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

/// One scannable adjacency range paired with the decoder for the keys it
/// returns.
///
/// The decoder's stripped prefix covers every route field the range's bound
/// fixes — through the direction for a wildcard scan, through the edge table
/// for a per-table scan — so per-key decoding starts at the first field that
/// varies inside this range. Keeping the pair together is also what upholds
/// the decoder's contract that it is only fed keys from its own adjacency.
pub(crate) struct GraphRange {
	/// The KV range to open a cursor over.
	pub range: TypedRange<()>,
	/// The decoder for every key that range returns.
	pub decoder: DecodedGraphPrefix,
}

/// Compute all KV key ranges to scan for a single record + direction, each
/// paired with the decoder for its keys.
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
		let range = prefix.typed(bound.clone().prefix_expect());
		Ok(vec![GraphRange {
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
			ranges.push(GraphRange {
				range: prefix.range_where((start.map(Cow::Owned), end.map(Cow::Owned)))?,
				decoder,
			});
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
