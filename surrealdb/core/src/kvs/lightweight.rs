//! Scanning a lightweight relation's edges from its endpoints' adjacency.
//!
//! A lightweight relation has no record range to scan: every edge lives as
//! the two vertex-side pointer keys of its endpoints, under the *vertex*
//! tables' subspaces. Enumerating the relation therefore means walking the
//! graph subspace of every table its `IN` clause names — through the
//! merged (blocks ∪ delta − tombstones) reader, so folded edges are
//! included — and keeping the `Out`-direction entries whose edge table is
//! the relation. Each surviving edge appears exactly once, because each
//! edge has exactly one `(in vertex, Out)` pointer and an `in` vertex
//! belongs to exactly one table.
//!
//! The `IN` tables are what makes this complete: lightweight DDL requires
//! a non-empty, grow-only `IN` clause, and the auto `in` field's type
//! check rejects endpoints outside it, so no edge can exist under a table
//! the scan does not visit.

use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::Direction;
use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Relation, TableType};
use crate::expr::Dir;
use crate::idx::adjacency::{AdjacencyResolveCache, MergedAdjacencyCursor};
use crate::val::{RecordId, TableName};

/// The smallest fill requested from the merged cursor per round trip. The
/// cursor filters to this relation's edges, so a fill may walk many
/// non-matching adjacency keys per match; flooring it keeps that walk
/// batched even when the caller wants a single edge.
const FILL_FLOOR: u32 = 64;

/// A pull-based scan of one lightweight relation's edges, in `IN`-table
/// order and, within each table, vertex-key order — both walked in the
/// scan's direction, so emission is ascending canonical-id order forward
/// and descending backward.
pub(crate) struct LightweightEdgeScanner<'a> {
	txn: &'a Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	edge_table: TableName,
	/// The relation's `IN` tables in scan order, not yet exhausted; scanned
	/// one merged cursor at a time.
	tables: Vec<TableName>,
	at: usize,
	cursor: Option<MergedAdjacencyCursor<'a>>,
	direction: Direction,
	version: Option<u64>,
	resolve: Option<&'a AdjacencyResolveCache>,
}

impl<'a> LightweightEdgeScanner<'a> {
	/// Opens the scan. `rel` must be the (lightweight) relation definition
	/// of `edge_table`.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn new(
		txn: &'a Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		edge_table: &TableName,
		rel: &Relation,
		direction: Direction,
		version: Option<u64>,
		resolve: Option<&'a AdjacencyResolveCache>,
	) -> Self {
		let mut tables = rel.from.clone();
		tables.sort();
		tables.dedup();
		if direction == Direction::Backward {
			tables.reverse();
		}
		Self {
			txn,
			ns,
			db,
			edge_table: edge_table.clone(),
			tables,
			at: 0,
			cursor: None,
			direction,
			version,
			resolve,
		}
	}

	/// The next run of edge ids, each the relation's canonical `[in, out]`
	/// record id. A run holds at least `limit` edges while any remain, and
	/// can overshoot: the cursor fill is floored (see [`FILL_FLOOR`]) and
	/// every edge a fill returns is kept, so callers must consume whole
	/// runs. Empty means exhausted.
	pub(crate) async fn next_batch(&mut self, limit: u32) -> Result<Vec<RecordId>> {
		let limit = limit.max(1) as usize;
		let mut out = Vec::new();
		while out.len() < limit {
			if self.cursor.is_none() {
				let Some(table) = self.tables.get(self.at) else {
					break;
				};
				let folded = self
					.txn
					.get_tb(self.ns, self.db, table, self.version)
					.await?
					.is_some_and(|tb| tb.graph_folded);
				self.cursor = Some(
					MergedAdjacencyCursor::open_table_wide(
						self.txn,
						self.ns,
						self.db,
						table,
						folded,
						self.direction,
						// The table-wide scan spans both directions and every
						// edge table; this relation's edges are exactly the
						// `(in vertex, Out)` pointers naming it, each of which
						// exists once.
						Some((Dir::Out, self.edge_table.clone())),
						self.version,
						self.resolve,
					)
					.await?,
				);
			}
			let cursor = self.cursor.as_mut().expect("a cursor was just installed");
			let fill = ((limit - out.len()) as u32).clamp(FILL_FLOOR, NORMAL_BATCH_SIZE);
			let batch = cursor.next_batch(fill).await?;
			if batch.is_empty() {
				self.cursor = None;
				self.at += 1;
				continue;
			}
			out.extend(batch.into_iter().map(|edge| edge.edge));
		}
		Ok(out)
	}
}

/// Whether the lightweight relation currently holds any edge. Used by the
/// DDL paths that must refuse to run over live edges (`REMOVE TABLE`, and
/// narrowing a lightweight relation's `IN`/`OUT`).
pub(crate) async fn lightweight_relation_is_empty(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	edge_table: &TableName,
	rel: &Relation,
) -> Result<bool> {
	let mut scanner =
		LightweightEdgeScanner::new(txn, ns, db, edge_table, rel, Direction::Forward, None, None);
	Ok(scanner.next_batch(1).await?.is_empty())
}

/// The lightweight relation definition of a table, if it is one.
pub(crate) fn lightweight_relation(table_type: &TableType) -> Option<&Relation> {
	match table_type {
		TableType::Relation(rel) if rel.lightweight => Some(rel),
		_ => None,
	}
}

/// The two endpoints of a lightweight edge id — re-exported so core paths
/// split canonical ids the same way record synthesis does.
pub(crate) use surrealdb_datastore::tx::lightweight_edge_parts as edge_parts;
