//! Cheap degree counting for `count(->edge)`-shaped projections.
//!
//! A degree question is answered from the storage that already knows the
//! answer, cheapest first: a live inline adjacency cache is one point read
//! and a filtered length; folded scopes with a clean delta tail sum the
//! chunk headers' entry counts; everything else counts keys or drains the
//! merge without materializing values (see
//! [`crate::idx::adjacency::scope_degree`]).
//!
//! The planner builds this expression only when the count's answer is
//! provably the raw adjacency cardinality — an owner session (permission
//! filtering off, where today's path takes the id-only resolve that never
//! fetches records either), reading current data, over a pure graph
//! lookup. Every value shape or storage state the fast path does not
//! cover delegates to `fallback`, which is the exact expression the
//! planner would otherwise have produced.

use std::sync::Arc;

use surrealdb_datastore::values::inline_cache::CacheValue;

use crate::catalog::providers::TableProvider;
use crate::exec::operators::scan::EdgeTableSpec;
use crate::exec::operators::scan::graph_keys::compute_graph_ranges;
use crate::exec::parts::LookupDirection;
use crate::exec::physical_expr::{BoxFut, EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel, FlowResult};
use crate::expr::Dir;
use crate::idx::adjacency::{VertexAdjacency, scope_degree, vertex_adjacency_of};
use crate::key::schema::EdgeCacheKey;
use crate::val::{RecordId, TableName, Value};

/// See the module docs.
pub(crate) struct GraphDegreeExpr {
	/// The traversal direction; `Both` sums the two directions.
	direction: LookupDirection,
	/// The edge tables to count; empty counts every table.
	edge_tables: Vec<TableName>,
	/// The expression the planner would otherwise have produced — the
	/// authority for every shape the fast path declines at runtime.
	fallback: Arc<dyn PhysicalExpr>,
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

impl std::fmt::Debug for GraphDegreeExpr {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("GraphDegreeExpr")
			.field("direction", &self.direction)
			.field("edge_tables", &self.edge_tables)
			.finish_non_exhaustive()
	}
}

impl surrealdb_types::ToSql for GraphDegreeExpr {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		self.fallback.fmt_sql(f, fmt);
	}
}

impl PhysicalExpr for GraphDegreeExpr {
	fn name(&self) -> &'static str {
		"GraphDegree"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// The fast path covers exactly one row shape: a single source
			// record. Arrays, NONE and exotic values keep the evaluator's
			// flatten-and-count semantics.
			let rid = match ctx.current_value {
				Some(Value::RecordId(rid)) => rid.clone(),
				Some(Value::Object(obj)) => match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => return self.fallback.evaluate(ctx).await,
				},
				_ => return self.fallback.evaluate(ctx).await,
			};
			match self.degree(&ctx, &rid).await? {
				Some(degree) => Ok(Value::Number(degree.into())),
				None => self.fallback.evaluate(ctx).await,
			}
		})
	}
}

/// The degree count's cancellation probe: the streaming executor's
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

impl GraphDegreeExpr {
	/// Builds the expression, deriving the plan-constant per-table prefixes
	/// and scan specs from `edge_tables` once.
	pub(crate) fn new(
		direction: LookupDirection,
		edge_tables: Vec<TableName>,
		fallback: Arc<dyn PhysicalExpr>,
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
			edge_tables,
			fallback,
			prefixes,
			specs,
			table_info: Default::default(),
		})
	}

	/// The degree of one source record, or `None` when the source's state
	/// is a shape the fast path leaves to the evaluator.
	async fn degree(&self, ctx: &EvalContext<'_>, rid: &RecordId) -> anyhow::Result<Option<u64>> {
		let exec = ctx.exec_ctx;
		let db_ctx = exec.database()?;
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_id = db_ctx.db.database_id;
		let txn = exec.txn();

		let info = self.table_info.lock().expect("degree memo poisoned").get(&rid.table).copied();
		let (adjacency, edges_cap) = match info {
			Some(info) => info,
			None => {
				let tb = txn.get_tb(ns_id, db_id, &rid.table, None).await?;
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
			LookupDirection::Reference => return Ok(None),
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
					degree += entries
						.iter()
						.filter(|e| {
							self.prefixes.is_empty()
								|| self.prefixes.iter().any(|p| e.edge.starts_with(p))
						})
						.count() as u64;
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
					_ => anyhow::anyhow!("unexpected control flow while computing degree ranges"),
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
