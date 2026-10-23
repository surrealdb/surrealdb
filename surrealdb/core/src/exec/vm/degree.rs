//! Cheap degree counting for `count(->edge)`-shaped projections.
//!
//! A degree question is answered from the storage that already knows the
//! answer, cheapest first: a live inline adjacency cache is one point read
//! and a filtered length; folded scopes with a clean delta tail sum the
//! chunk headers' entry counts; everything else counts keys or drains the
//! merge without materializing values (see
//! [`crate::idx::adjacency::scope_degree`]).
//!
//! The count stands in for `count` of a bare graph lookup only where its
//! answer is the raw adjacency cardinality: the lowering emits
//! [`Instr::Degree`](super::program::Instr::Degree) only for a caller whose
//! reads go unchecked, and the host counts only for a run whose reads go
//! unchecked and read the current state. There the lookup answers the record
//! id of each adjacency entry without reading its edge, so the number of
//! entries is its count. Every value shape or storage state the counter does
//! not cover falls through to the call over the lookup.

use surrealdb_datastore::values::inline_cache::CacheValue;

use crate::exec::EvalContext;
use crate::exec::operators::scan::EdgeTableSpec;
use crate::exec::operators::scan::graph_keys::compute_graph_ranges;
use crate::exec::parts::LookupDirection;
use crate::expr::Dir;
use crate::idx::adjacency::{VertexAdjacency, scope_degree, vertex_adjacency_of};
use crate::key::schema::EdgeCacheKey;
use crate::val::{RecordId, TableName, Value};

/// The degree fast path of a `count(->edge)`-shaped call over the record the
/// call is evaluated on: what it counts, and the per-table data it derives
/// from that once.
pub(crate) struct DegreeCounter {
	/// The traversal direction; `Both` sums the two directions.
	direction: LookupDirection,
	/// The edge tables as storekey prefixes, for filtering inline-cache
	/// entries — an entry's identity bytes start with its encoded table.
	/// Empty accepts every table. Plan-constant: projections evaluate once
	/// per row, so per-table data is derived once at construction.
	prefixes: Vec<Vec<u8>>,
	/// The edge tables as unbounded scan specs for scope computation.
	/// Direction-independent, so both directions of a `<->` count share
	/// them; empty means the wildcard scope.
	specs: Vec<EdgeTableSpec>,
	/// Per-source-table facts (adjacency classification, cache cap),
	/// memoized for the expression's lifetime.
	table_info:
		std::sync::Mutex<std::collections::HashMap<TableName, (VertexAdjacency, Option<u32>)>>,
}

/// The degree count's cancellation probe: the engine's
/// cancellation token plus the context's deadline, so both a cancelled
/// query and a `TIMEOUT` clause interrupt a long count.
struct DegreeCancel<'a> {
	exec: &'a crate::exec::ExecutionContext,
}

impl crate::catalog::providers::CancellationProbe for DegreeCancel<'_> {
	fn expect_not_timedout(
		&self,
	) -> crate::catalog::providers::BoxProviderFut<'_, anyhow::Result<()>> {
		Box::pin(async move {
			if self.exec.cancellation().is_cancelled() {
				return Err(anyhow::anyhow!(crate::err::EngineError::QueryCancelled));
			}
			self.exec.ctx().expect_not_timedout().await
		})
	}
}

impl DegreeCounter {
	/// The fast path counting `edge_tables` (every table when empty) along
	/// `direction`, deriving the per-table prefixes and scan specs once. A
	/// table named more than once is counted once for each time it is named,
	/// as a lookup of the same tables reads it.
	pub(crate) fn new(
		direction: LookupDirection,
		edge_tables: &[TableName],
	) -> anyhow::Result<Self> {
		let prefixes = edge_tables
			.iter()
			.map(|t| storekey::encode_vec(t).map_err(anyhow::Error::from_boxed))
			.collect::<anyhow::Result<_>>()?;
		let specs = edge_tables
			.iter()
			.map(|t| EdgeTableSpec {
				table: t.clone(),
				range_start: std::ops::Bound::Unbounded,
				range_end: std::ops::Bound::Unbounded,
			})
			.collect();
		Ok(Self {
			direction,
			prefixes,
			specs,
			table_info: Default::default(),
		})
	}

	/// The source record a value the call is evaluated on names: a record
	/// id, or an object's `id` that is one. Arrays, `NONE` and every other
	/// value are left to the evaluator's flatten-and-count semantics.
	pub(crate) fn source(current: Option<&Value>) -> Option<RecordId> {
		match current {
			Some(Value::RecordId(rid)) => Some(rid.clone()),
			Some(Value::Object(obj)) => match obj.get("id") {
				Some(Value::RecordId(rid)) => Some(rid.clone()),
				_ => None,
			},
			_ => None,
		}
	}

	/// The degree of one source record, or `None` when the source's state
	/// is a shape the fast path leaves to the evaluator.
	pub(crate) async fn degree(
		&self,
		ctx: &EvalContext<'_>,
		rid: &RecordId,
	) -> anyhow::Result<Option<u64>> {
		let exec = ctx.exec_ctx;
		let db_ctx = exec.database()?;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let txn = exec.txn();

		let info = self.table_info.lock().expect("degree memo poisoned").get(&rid.table).copied();
		let (adjacency, edges_cap) = match info {
			Some(info) => info,
			None => {
				let tb = txn.layout_tb(ns_id, db_id, &rid.table, None).await?;
				let info = (
					vertex_adjacency_of(tb.as_deref()),
					crate::idx::inline_cache::effective_edges_cap(tb.as_deref()),
				);
				self.table_info
					.lock()
					.expect("degree memo poisoned")
					.insert(rid.table.clone(), info);
				info
			}
		};
		// A lightweight edge's own adjacency is synthesized from its id;
		// rare as a degree source — leave it to the evaluator.
		let VertexAdjacency::Normal {
			folded,
		} = adjacency
		else {
			return Ok(None);
		};

		let dirs: &[Dir] = match self.direction {
			LookupDirection::Out => &[Dir::Out],
			LookupDirection::In => &[Dir::In],
			LookupDirection::Both => &[Dir::In, Dir::Out],
		};

		let cancel = DegreeCancel {
			exec,
		};
		let mut degree = 0u64;
		for &dir in dirs {
			// A live inline cache is the scope's complete adjacency.
			if edges_cap.is_some_and(|cap| cap > 0) {
				let key = EdgeCacheKey {
					ns: ns_id,
					db: db_id,
					tb: std::borrow::Cow::Borrowed(&rid.table),
					id: std::borrow::Cow::Borrowed(&rid.key),
					dir,
				};
				if let Some(CacheValue::Live(entries)) = txn.get_key(&key, None).await? {
					// Table by table, as the ranges below are: a table named twice counts its
					// entries twice.
					degree += if self.prefixes.is_empty() {
						entries.len() as u64
					} else {
						self.prefixes
							.iter()
							.map(|p| {
								entries.iter().filter(|e| e.edge.starts_with(p)).count() as u64
							})
							.sum()
					};
					continue;
				}
			}
			let ranges = compute_graph_ranges(ns_id, db_id, rid, dir, &self.specs, exec)
				.await
				.map_err(|cf| match cf {
					crate::expr::ControlFlow::Err(e) => e,
					// The specs' bounds are all `Unbounded`, so no bound
					// expression is ever evaluated and no other control
					// flow can surface.
					crate::expr::ControlFlow::Break
					| crate::expr::ControlFlow::Continue
					| crate::expr::ControlFlow::Return(_) => {
						anyhow::anyhow!("unexpected control flow while computing degree ranges")
					}
				})?;
			let resolve = exec.root().ctx.get_index_stores().adjacency_resolve();
			let mut dir_degree = 0u64;
			for range in ranges {
				dir_degree += scope_degree(
					&txn,
					range.as_adjacency_scope(ns_id, db_id, rid, dir),
					folded,
					None,
					Some(resolve),
					Some(&cancel),
				)
				.await?;
			}
			// A degree read over an unfolded scope is a read like any
			// other: a hot scope observed here folds in the background,
			// and the next degree question is a header sum.
			if !folded {
				crate::exec::operators::scan::graph_keys::observe_fold_candidate(
					&txn,
					ns_id,
					db_id,
					rid,
					dir,
					dir_degree,
					exec.root().ctx.config.idx.graph_fold_threshold,
				);
			}
			degree += dir_degree;
		}
		Ok(Some(degree))
	}
}
