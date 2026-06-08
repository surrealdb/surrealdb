//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::sync::Arc;

use super::Planner;
use super::util::{extract_table_from_context, key_lit_to_expr};
use crate::err::Error;
use crate::exec::ExecOperator;
use crate::exec::operators::{
	CurrentValueSource, EdgeTableSpec, Filter, GraphEdgeScan, GraphScanOutput, Limit, OrderByField,
	RandomShuffle, ReferenceScan, ReferenceScanOutput, SortDirection,
};
use crate::exec::parts::LookupDirection;
use crate::exec::planner::select::SelectPipelineConfig;
use crate::expr::{Expr, Literal};
use crate::val::TableName;

/// Planned representation of a `->edge->vertex` fast-path collapse.
pub(crate) struct TargetVertexPlan {
	pub direction: LookupDirection,
	pub edge_tables: Vec<EdgeTableSpec>,
	pub target_tables: Vec<TableName>,
}

// ============================================================================
// impl Planner — Source Planning
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Plan an index function call.
	///
	/// Dispatches generically based on [`IndexContextKind`] declared by the
	/// function -- no hardcoded function names. The function declares what kind
	/// of index context it needs, and the planner resolves it:
	///
	/// - **FullText**: extracts the index_ref argument, resolves via MATCHES context
	/// - **Knn**: retrieves the KNN context from the planning context
	pub(crate) async fn plan_index_function(
		&self,
		name: &str,
		mut ast_args: Vec<Expr>,
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
		use crate::exec::function::{IndexContext, IndexContextKind};
		use crate::exec::physical_expr::function::IndexFunctionExec;

		let registry = self.function_registry();
		let func = registry.get_index_function(name).ok_or_else(|| Error::Query {
			message: format!("Index function '{}' not found in registry", name),
		})?;

		// Resolve the appropriate index context based on the function's declared kind
		let index_ctx = match func.index_context_kind() {
			IndexContextKind::FullText => {
				// FullText functions must declare which argument is the index ref
				let ref_idx = func.index_ref_arg_index().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': FullText functions must declare an index_ref_arg_index",
						name
					),
				})?;

				if ref_idx >= ast_args.len() {
					return Err(Error::Query {
						message: format!(
							"Index function '{}' requires at least {} arguments",
							name,
							ref_idx + 1
						),
					});
				}

				// Extract the match_ref argument at plan time (not passed at runtime)
				let match_ref_ast = ast_args.remove(ref_idx);
				let match_ref = match match_ref_ast {
					Expr::Literal(Literal::Integer(n)) if (0..=255).contains(&n) => n as u8,
					Expr::Literal(Literal::Float(n))
						if (0.0..=255.0).contains(&n) && n.fract() == 0.0 =>
					{
						n as u8
					}
					_ => {
						return Err(Error::Query {
							message: format!(
								"Index function '{}': index_ref argument must be a literal integer in range 0..255",
								name
							),
						});
					}
				};

				// Resolve the MatchContext from the MATCHES context
				let matches_ctx = self.ctx.get_matches_context().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': no MATCHES clause found in WHERE condition",
						name
					),
				})?;

				let match_ctx = matches_ctx
					.resolve(match_ref, extract_table_from_context(self.ctx))
					.map_err(|e| Error::Query {
						message: format!("Index function '{}': {}", name, e),
					})?;

				IndexContext::FullText(match_ctx)
			}
			IndexContextKind::Knn => {
				// KNN functions: retrieve the KNN context from the planning context.
				// If there's a ref argument, extract it (currently unused for single-KNN queries).
				if let Some(ref_idx) = func.index_ref_arg_index()
					&& ref_idx < ast_args.len()
				{
					ast_args.remove(ref_idx);
				}

				let knn_ctx = self.ctx.get_knn_context().ok_or_else(|| Error::Query {
					message: format!(
						"Index function '{}': no KNN operator found in WHERE condition",
						name
					),
				})?;
				IndexContext::Knn(Arc::clone(knn_ctx))
			}
		};

		// Compile remaining arguments to physical expressions
		let mut phys_args = Vec::with_capacity(ast_args.len());
		for arg in ast_args {
			phys_args.push(self.physical_expr(arg).await?);
		}

		let func_ctx = func.required_context();

		Ok(Arc::new(IndexFunctionExec {
			name: name.to_string(),
			arguments: phys_args,
			index_ctx,
			func_required_context: func_ctx,
		}))
	}

	/// Convert an `OrderList` to a `Vec<OrderByField>`.
	pub(crate) async fn convert_order_list(
		&self,
		order_list: crate::expr::order::OrderList,
	) -> Result<Vec<OrderByField>, Error> {
		let mut fields = Vec::with_capacity(order_list.len());
		for order_field in order_list {
			let expr: Arc<dyn crate::exec::PhysicalExpr> =
				self.convert_idiom(order_field.value).await?;

			let direction = if order_field.direction {
				SortDirection::Asc
			} else {
				SortDirection::Desc
			};

			fields.push(OrderByField {
				expr,
				direction,
				collate: order_field.collate,
				numeric: order_field.numeric,
			});
		}
		Ok(fields)
	}

	/// Convert a `Bound<RecordIdKeyLit>` to a `Bound<Arc<dyn PhysicalExpr>>`.
	pub(crate) async fn convert_range_bound(
		&self,
		bound: &std::ops::Bound<crate::expr::RecordIdKeyLit>,
	) -> Result<std::ops::Bound<Arc<dyn crate::exec::PhysicalExpr>>, Error> {
		match bound {
			std::ops::Bound::Unbounded => Ok(std::ops::Bound::Unbounded),
			std::ops::Bound::Included(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr).await?;
				Ok(std::ops::Bound::Included(phys))
			}
			std::ops::Bound::Excluded(lit) => {
				let expr = key_lit_to_expr(lit)?;
				let phys = self.physical_expr(expr).await?;
				Ok(std::ops::Bound::Excluded(phys))
			}
		}
	}

	/// Plan a Lookup operation (graph edge or reference traversal).
	///
	/// Builds a streaming operator chain rooted at `CurrentValueSource`.
	/// At execution time, `LookupPart` sets `current_value` on the
	/// `ExecutionContext` before executing this chain, so `CurrentValueSource`
	/// yields the appropriate RecordId into the stream.
	pub(crate) async fn plan_lookup(
		&self,
		lookup: crate::expr::lookup::Lookup,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let input: Arc<dyn ExecOperator> = Arc::new(CurrentValueSource::new());
		self.plan_lookup_with_input(input, lookup).await
	}

	/// Plan a Lookup operation with a specific input operator.
	///
	/// This is the core of lookup planning. When fusing consecutive lookups
	/// into a single operator chain, the planner passes the output of one
	/// lookup as the `input` to the next, instead of always creating a fresh
	/// `CurrentValueSource`.
	pub(crate) async fn plan_lookup_with_input(
		&self,
		input: Arc<dyn ExecOperator>,
		crate::expr::lookup::Lookup {
			kind,
			expr,
			only: _,
			what,
			cond,
			split,
			group,
			order,
			limit,
			start,
			alias: _,
		}: crate::expr::lookup::Lookup,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let needs_full_pipeline = expr.is_some() || group.is_some();
		let needs_full_records = needs_full_pipeline || cond.is_some() || split.is_some();
		let output_mode = if needs_full_records {
			GraphScanOutput::FullEdge
		} else {
			GraphScanOutput::TargetId
		};

		let base_scan: Arc<dyn ExecOperator> = match &kind {
			crate::expr::lookup::LookupKind::Graph(dir) => {
				let mut edge_tables: Vec<EdgeTableSpec> = Vec::with_capacity(what.len());
				for s in what {
					let spec = match s {
						crate::expr::lookup::LookupSubject::Table {
							table,
							..
						} => EdgeTableSpec {
							table,
							range_start: std::ops::Bound::Unbounded,
							range_end: std::ops::Bound::Unbounded,
						},
						crate::expr::lookup::LookupSubject::Range {
							table,
							range,
							..
						} => {
							let range_start = self.convert_range_bound(&range.start).await?;
							let range_end = self.convert_range_bound(&range.end).await?;
							EdgeTableSpec {
								table,
								range_start,
								range_end,
							}
						}
					};
					edge_tables.push(spec);
				}

				let scan = GraphEdgeScan::new(
					input,
					LookupDirection::from(dir),
					edge_tables,
					output_mode,
					self.version.clone(),
				);
				// Push limit into the scan when no filter/sort/split would
				// change the result count. This avoids scanning all edges
				// when only a few are needed. When START is present, add
				// the offset so the scan fetches enough rows for the skip.
				let scan =
					if cond.is_none() && split.is_none() && order.is_none() && group.is_none() {
						if let Some(crate::expr::limit::Limit(crate::expr::Expr::Literal(
							crate::expr::Literal::Integer(n),
						))) = &limit
						{
							let offset = match &start {
								Some(crate::expr::start::Start(crate::expr::Expr::Literal(
									crate::expr::Literal::Integer(s),
								))) => *s as usize,
								_ => 0,
							};
							scan.with_limit(*n as usize + offset)
						} else {
							scan
						}
					} else {
						scan
					};
				Arc::new(scan)
			}
			crate::expr::lookup::LookupKind::Reference => {
				let (referencing_table, referencing_field, range_start, range_end) =
					if let Some(subject) = what.first() {
						match subject {
							crate::expr::lookup::LookupSubject::Table {
								table,
								referencing_field,
							} => (
								Some(table.clone()),
								referencing_field.clone(),
								std::ops::Bound::Unbounded,
								std::ops::Bound::Unbounded,
							),
							crate::expr::lookup::LookupSubject::Range {
								table,
								referencing_field,
								range,
							} => {
								let rs = self.convert_range_bound(&range.start).await?;
								let re = self.convert_range_bound(&range.end).await?;
								(Some(table.clone()), referencing_field.clone(), rs, re)
							}
						}
					} else {
						(None, None, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
					};

				let ref_output_mode = if needs_full_records {
					ReferenceScanOutput::FullRecord
				} else {
					ReferenceScanOutput::RecordId
				};

				Arc::new(ReferenceScan::new(
					input,
					referencing_table,
					referencing_field,
					ref_output_mode,
					range_start,
					range_end,
					self.version.clone(),
				))
			}
		};

		if needs_full_pipeline {
			let config = SelectPipelineConfig {
				where_clause: match cond {
					Some(c) => crate::exec::planner::select::WhereClauseState::Original(c),
					None => crate::exec::planner::select::WhereClauseState::None,
				},
				split,
				group,
				order,
				limit,
				start,
				omit: vec![],
				tempfiles: false,
			};
			self.plan_pipeline(base_scan, expr, config).await
		} else {
			let filtered: Arc<dyn ExecOperator> = if let Some(cond) = cond {
				let predicate = self.physical_expr(cond.0).await?;
				Arc::new(Filter::new(base_scan, predicate))
			} else {
				base_scan
			};

			let split_op: Arc<dyn ExecOperator> = if let Some(splits) = split {
				Arc::new(crate::exec::operators::Split::new(
					filtered,
					splits.into_iter().map(|s| s.0).collect(),
				))
			} else {
				filtered
			};

			let sorted: Arc<dyn ExecOperator> = match order {
				Some(crate::expr::order::Ordering::Order(order_list)) => {
					let order_by = self.convert_order_list(order_list).await?;
					Arc::new(crate::exec::operators::Sort::new(split_op, order_by))
				}
				Some(crate::expr::order::Ordering::Random) => {
					Arc::new(RandomShuffle::new(split_op, None))
				}
				None => split_op,
			};

			let limited: Arc<dyn ExecOperator> = if limit.is_some() || start.is_some() {
				let limit_expr = match limit {
					Some(l) => Some(self.physical_expr(l.0).await?),
					None => None,
				};
				let offset_expr = match start {
					Some(s) => Some(self.physical_expr(s.0).await?),
					None => None,
				};
				Arc::new(Limit::new(sorted, limit_expr, offset_expr))
			} else {
				sorted
			};

			Ok(limited)
		}
	}

	/// Decide whether a consecutive `(edge_lookup, vertex_lookup)` pair can
	/// be served by a single `GraphEdgeScan` in `TargetVertex` mode.
	///
	/// Returns the collapsed scan parameters when eligible. The pair must:
	///
	/// - Both be `Graph(dir)` lookups in the same direction.
	/// - Have no edge/vertex-side filtering, projection, ordering, alias, sub-range, or `ONLY`
	///   markers — the new layout only optimises the pure "walk through the edge to the target
	///   table" case.
	/// - Reference only edge tables whose `SELECT` permission is `Permission::Full`. `Specific`
	///   permissions would otherwise be bypassed by reading the target directly from the adjacency
	///   key.
	pub(crate) async fn try_fast_path_pair(
		&self,
		edge: &crate::expr::lookup::Lookup,
		vertex: &crate::expr::lookup::Lookup,
	) -> Result<Option<TargetVertexPlan>, Error> {
		use crate::expr::lookup::{LookupKind, LookupSubject};

		// Both hops must be graph traversal in the same direction. The
		// `d1 == d2` guard means the vertex-hop direction is exactly
		// `edge_dir`, so we only bind the one we use.
		let edge_dir = match (&edge.kind, &vertex.kind) {
			(LookupKind::Graph(d1), LookupKind::Graph(d2)) if d1 == d2 => d1,
			_ => return Ok(None),
		};
		// SECURITY: time-travel queries (`VERSION <ts>`) must enforce the
		// edge table's SELECT permissions as they stood at the requested
		// version, not the current catalog. The eligibility check below
		// inspects the *current* `Permission::Full` state, so a query that
		// targets a historical version where the edge had a row-level WHERE
		// could be admitted to the fast path -- which then skips the edge
		// record entirely and bypasses that WHERE. Always take the slow
		// path when a VERSION expression is present so the historical
		// permission gate is honoured.
		if self.version.is_some() {
			return Ok(None);
		}
		// Bidirectional (`<->`) traversal has the slow path scan the edge's
		// own adjacency in *both* directions on the second hop, so each
		// edge contributes both endpoint vertices to the result -- the
		// embedded target plus the originating source vertex. The fast
		// path emits only the embedded target, dropping the source-side
		// duplicate. Force the slow path on `<->` to preserve semantics;
		// `->` and `<-` keep the optimisation.
		if matches!(edge_dir, crate::expr::Dir::Both) {
			return Ok(None);
		}

		// Neither hop may carry edge-side filtering, projection, or other
		// clauses that depend on reading the edge / vertex record.
		let lookup_is_plain = |l: &crate::expr::lookup::Lookup| {
			l.expr.is_none()
				&& l.cond.is_none()
				&& l.split.is_none()
				&& l.group.is_none()
				&& l.order.is_none()
				&& l.limit.is_none()
				&& l.start.is_none()
				&& l.alias.is_none()
				&& !l.only
		};
		if !lookup_is_plain(edge) || !lookup_is_plain(vertex) {
			return Ok(None);
		}

		// We only support unbounded `LookupSubject::Table` subjects on both
		// hops. Range subjects on the edge would change the bound semantics
		// (and would need the trailing-target-aware bound construction);
		// keep them on the slow path for now.
		let mut edge_tables: Vec<EdgeTableSpec> = Vec::with_capacity(edge.what.len());
		for s in &edge.what {
			let LookupSubject::Table {
				table,
				..
			} = s
			else {
				return Ok(None);
			};
			edge_tables.push(EdgeTableSpec {
				table: table.clone(),
				range_start: std::ops::Bound::Unbounded,
				range_end: std::ops::Bound::Unbounded,
			});
		}
		// The edge `what` list must be non-empty so we can resolve each
		// edge table's permissions. The unbounded "any edge table" case
		// stays on the slow path because we don't know which tables to
		// permission-check at plan time.
		if edge_tables.is_empty() {
			return Ok(None);
		}

		let mut target_tables: Vec<TableName> = Vec::with_capacity(vertex.what.len());
		for s in &vertex.what {
			let LookupSubject::Table {
				table,
				..
			} = s
			else {
				return Ok(None);
			};
			target_tables.push(table.clone());
		}
		// Mirror the edge `what` check above: the unbounded "any vertex
		// table" case (empty `what`) stays on the slow path. The scan
		// operator would otherwise accept every embedded target without a
		// table filter, leaving the per-target permission check as the
		// only gate; keep the eligibility contract symmetric so plan-time
		// reasoning matches the edge side.
		if target_tables.is_empty() {
			return Ok(None);
		}

		// Verify every edge table has `PERMISSIONS FULL` for SELECT.
		// Without a plan-time transaction we cannot check, so fall back.
		let Some(txn) = self.txn() else {
			return Ok(None);
		};
		let (Some(ns), Some(db)) = (self.ns(), self.db()) else {
			return Ok(None);
		};
		// SECURITY: the fast path emits the embedded target vertex without
		// consulting the edge record, so any per-row WHERE on the edge
		// table would be silently bypassed. We require either:
		//   1. The edge table's SELECT permission is `Full` (no per-row gate exists), OR
		//   2. The runtime would skip permission evaluation altogether for this session (root/owner
		//      with the right level, or auth-disabled anonymous). In that case the slow path's
		//      WHERE wouldn't run either, so the bypass is equivalent.
		// (2) requires the calling auth principal; when it isn't wired
		// through (txn-less or deeply-nested planner) we stay conservative.
		if !self.should_check_perms_for_view(ns, db) {
			return Ok(Some(TargetVertexPlan {
				direction: LookupDirection::from(edge_dir),
				edge_tables,
				target_tables,
			}));
		}
		for spec in &edge_tables {
			use crate::catalog::providers::TableProvider;
			let td = match txn.get_tb_by_name(ns, db, &spec.table, None).await {
				Ok(Some(td)) => td,
				// Missing edge table or any catalog error → take the slow
				// path; this is purely an optimisation, never a correctness
				// gate.
				_ => return Ok(None),
			};
			if !matches!(td.permissions.select, crate::catalog::Permission::Full) {
				return Ok(None);
			}
		}

		Ok(Some(TargetVertexPlan {
			direction: LookupDirection::from(edge_dir),
			edge_tables,
			target_tables,
		}))
	}

	/// Build the operator that implements a fast-path `->edge->vertex`
	/// collapse, returning a `GraphEdgeScan` in `TargetVertex` mode.
	pub(crate) async fn plan_target_vertex_scan(
		&self,
		input: Arc<dyn ExecOperator>,
		plan: TargetVertexPlan,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let scan = GraphEdgeScan::new(
			input,
			plan.direction,
			plan.edge_tables,
			GraphScanOutput::TargetVertex,
			self.version.clone(),
		)
		.with_target_tables(plan.target_tables);
		Ok(Arc::new(scan))
	}
}
