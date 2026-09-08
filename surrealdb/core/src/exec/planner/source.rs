//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::sync::Arc;

use surrealdb_strand::TableName;

use super::Planner;
use super::util::{extract_table_from_matches, get_effective_limit_literal, key_lit_to_expr};
use crate::err::Error;
use crate::exec::operators::{
	CurrentValueSource, EdgeTableSpec, Filter, GraphEdgeScan, GraphScanOutput, Limit, OrderByField,
	ReferenceScan, ReferenceScanOutput, SortDirection,
};
use crate::exec::parts::LookupDirection;
use crate::exec::planner::select::SelectPipelineConfig;
use crate::exec::{Error as ExecError, ExecOperator};
use crate::expr::{Expr, Literal};

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
		let func = registry.get_index_function(name).ok_or_else(|| ExecError::Query {
			message: format!("Index function '{}' not found in registry", name),
		})?;

		// Resolve the appropriate index context based on the function's declared kind
		let index_ctx = match func.index_context_kind() {
			IndexContextKind::FullText => {
				// FullText functions must declare which argument is the index ref
				let ref_idx = func.index_ref_arg_index().ok_or_else(|| ExecError::Query {
					message: format!(
						"Index function '{}': FullText functions must declare an index_ref_arg_index",
						name
					),
				})?;

				if ref_idx >= ast_args.len() {
					return Err(ExecError::Query {
						message: format!(
							"Index function '{}' requires at least {} arguments",
							name,
							ref_idx + 1
						),
					}
					.into());
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
						return Err(ExecError::Query {
							message: format!(
								"Index function '{}': index_ref argument must be a literal integer in range 0..255",
								name
							),
						}
						.into());
					}
				};

				// Resolve the MatchContext from the MATCHES context
				let matches_ctx =
					self.matches_context.as_ref().ok_or_else(|| ExecError::Query {
						message: format!(
							"Index function '{}': no MATCHES clause found in WHERE condition",
							name
						),
					})?;

				let match_ctx = matches_ctx
					.resolve(match_ref, extract_table_from_matches(matches_ctx))
					.map_err(|e| ExecError::Query {
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

				let knn_ctx = self.knn_context.as_ref().ok_or_else(|| ExecError::Query {
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
		let is_graph = matches!(&kind, crate::expr::lookup::LookupKind::Graph(_));
		// For a graph lookup outside the full (expr/group) pipeline, compile
		// the `cond` once and classify it so it can be pushed into the scan
		// instead of a downstream `Filter`. A key-resident predicate (see
		// `graph_predicate_is_key_resident`) is evaluated from the adjacency
		// key alone, so the scan stays in `TargetId` mode and skips fetching
		// non-matching edge records; a record-referencing predicate needs the
		// full edge record.
		let graph_pushdown: Option<(
			Arc<dyn crate::exec::PhysicalExpr>,
			bool,
			Option<crate::exec::operators::scan::PayloadPredicateSpec>,
		)> = if is_graph && !needs_full_pipeline {
			match &cond {
				Some(c) => {
					let pred = self.physical_expr(c.0.clone()).await?;
					let key_resident = Self::graph_predicate_is_key_resident(&pred);
					// A predicate that is not key-resident may still be
					// payload-resident: every field it references inlined
					// into the adjacency payloads of every named edge table.
					let direction = match &kind {
						crate::expr::lookup::LookupKind::Graph(dir) => (*dir).into(),
						_ => LookupDirection::Both,
					};
					let payload = if key_resident {
						None
					} else {
						self.graph_predicate_payload_spec(&c.0, &pred, &what, direction).await
					};
					Some((pred, key_resident, payload))
				}
				None => None,
			}
		} else {
			None
		};
		let cond_pushed_pre_fetch = graph_pushdown
			.as_ref()
			.map(|(_, kr, payload)| *kr || payload.is_some())
			.unwrap_or(false);
		// A `cond` still forces full records unless it is evaluated before
		// the fetch (key- or payload-resident); `SPLIT` and the expr/group
		// pipeline force full as before.
		let needs_full_records =
			needs_full_pipeline || (cond.is_some() && !cond_pushed_pre_fetch) || split.is_some();
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

				// An ORDER BY normally blocks the limit push below (the sort
				// needs every row), with one exception: a single plain
				// ascending `id` key over a single edge table in one fixed
				// direction is exactly the scan's own emission order — one
				// adjacency scope, whose pointer keys sort by edge id — so
				// the first `limit + start` rows the scan yields are the
				// rows the sort would keep. Multiple edge tables or a `<->`
				// traversal concatenate separately-sorted scopes, and a
				// descending key would need a reverse scan, so those keep
				// the full scan. A field list that aliases a different
				// field to the literal name `id` also keeps the full scan:
				// `ORDER BY id` then resolves against the aliased
				// expression (see `order_by_id_is_source_id`), not the
				// edge's own id, so the scan's emission order no longer
				// matches the sort.
				let ordered_scan_cap = !matches!(dir, crate::expr::Dir::Both)
					&& edge_tables.len() == 1
					&& order.as_ref().is_some_and(order_is_plain_id_asc)
					&& order_by_id_is_source_id(expr.as_ref());
				// Push limit into the scan when no filter/sort/split would
				// change the result count. This avoids scanning all edges
				// when only a few are needed. `get_effective_limit_literal`
				// returns the combined `start + limit` only when both are
				// literal non-negative integers (absent START counts as
				// zero); a parameter, expression, or negative value yields
				// `None`, declining the push so the downstream Limit
				// operator applies the real value at execution time instead.
				// Only push the limit into the scan when the scan's output is the
				// final filtered set: no downstream clause changes the row count. A
				// `cond` is safe only when it was pushed into the scan
				// (`graph_pushdown`); when it flows through the expr/group pipeline
				// or a downstream Filter, limiting the scan first would drop rows the
				// filter has not yet seen. An ORDER BY blocks the push unless
				// it matches the scan's own emission order (`ordered_scan_cap`
				// above).
				//
				// The cap must count *visible* rows: with a pushed predicate
				// the scan counts post-resolve at flush time — after
				// permission filtering — so the count is always exact, but
				// without one it counts at buffer time, before the batched
				// resolve applies table SELECT permissions. That shape is
				// admitted only when no counted row can be permission-dropped
				// (`graph_limit_count_is_exact`).
				let pushed_limit = if (cond.is_none() || graph_pushdown.is_some())
					&& split.is_none()
					&& (order.is_none() || ordered_scan_cap)
					&& group.is_none()
				{
					match get_effective_limit_literal(&start, &limit) {
						Some(effective_limit)
							if graph_pushdown.is_some()
								|| self.graph_limit_count_is_exact(&edge_tables).await =>
						{
							Some(effective_limit)
						}
						_ => None,
					}
				} else {
					None
				};
				let scan = GraphEdgeScan::new(
					input,
					LookupDirection::from(dir),
					edge_tables,
					output_mode,
					self.version.clone(),
				);
				let scan = match pushed_limit {
					Some(effective_limit) if ordered_scan_cap => {
						scan.with_ordered_limit(effective_limit)
					}
					Some(effective_limit) => scan.with_limit(effective_limit),
					None => scan,
				};
				// Push the compiled predicate into the scan, replacing the
				// downstream Filter for graph lookups.
				let scan = match &graph_pushdown {
					Some((pred, _, Some(payload))) => {
						scan.with_payload_predicate(Arc::clone(pred), payload.clone())
					}
					Some((pred, key_resident, None)) => {
						scan.with_predicate(Arc::clone(pred), *key_resident)
					}
					None => scan,
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
				topk_pushdown: None,
			};
			self.plan_pipeline(base_scan, expr, config).await
		} else {
			let filtered: Arc<dyn ExecOperator> = match cond {
				// Graph lookups pushed the predicate into the scan above.
				Some(_) if graph_pushdown.is_some() => base_scan,
				Some(cond) => Arc::new(Filter::new(base_scan, self.physical_expr(cond.0).await?)),
				None => base_scan,
			};

			let split_op: Arc<dyn ExecOperator> = if let Some(splits) = split {
				Arc::new(crate::exec::operators::Split::new(
					filtered,
					splits.into_iter().map(|s| s.0).collect(),
				))
			} else {
				filtered
			};

			// `plan_sort` picks the sort operator the same way the SELECT
			// pipeline does: a bounded literal LIMIT gets a top-k heap
			// instead of a full blocking sort.
			let sorted: Arc<dyn ExecOperator> = match order {
				Some(order) => self.plan_sort(split_op, order, &start, &limit, false).await?,
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

	/// Whether a compiled graph-lookup predicate is "key-resident":
	/// evaluable from the adjacency key alone (currently only the edge
	/// `id`), free of subqueries, functions, and side effects. Key-resident
	/// predicates are applied before fetching the edge record. Conservative
	/// — anything not provably key-resident is treated as
	/// record-referencing, which only costs a fetch. Milestone E implements
	/// the id-only recognizer; until then every predicate is
	/// record-referencing.
	fn graph_predicate_is_key_resident(_pred: &Arc<dyn crate::exec::PhysicalExpr>) -> bool {
		false
	}

	/// Whether a graph-lookup predicate is "payload-resident": evaluable
	/// against the inline property payload the adjacency values carry,
	/// before fetching any edge record.
	///
	/// Admission is deliberately conservative — anything not provably
	/// payload-resident stays record-referencing, which only costs a fetch:
	///
	/// * the traversal must name its edge tables and read current data (`VERSION` reads take the
	///   record path);
	/// * static analysis of the predicate must fully resolve the fields it reads (a subquery,
	///   parameter or traversal makes it opaque), each of which — beyond the always-synthesized
	///   `id`/`in`/`out` — must be inlined on *every* named table;
	/// * the predicate must not mutate;
	/// * every named table must be a non-lightweight relation whose SELECT permission is `Full`,
	///   and each referenced inline field's SELECT permission must be `Full` too — payload
	///   evaluation reads raw stored values, so a row- or field-level clause would otherwise let
	///   hidden values steer the visible result set.
	///
	/// Returns the per-table `(generation, field order)` snapshot the
	/// evaluation trusts at read time.
	async fn graph_predicate_payload_spec(
		&self,
		cond: &crate::expr::Expr,
		pred: &Arc<dyn crate::exec::PhysicalExpr>,
		what: &[crate::expr::lookup::LookupSubject],
		direction: LookupDirection,
	) -> Option<crate::exec::operators::scan::PayloadPredicateSpec> {
		use crate::catalog::providers::TableProvider;
		use crate::exec::operators::scan::{PayloadPredicateSpec, PayloadTableSpec};

		let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db) else {
			return None;
		};
		if self.version.is_some() || what.is_empty() {
			return None;
		}
		if pred.access_mode() != crate::exec::AccessMode::ReadOnly {
			return None;
		}
		let deps = crate::expr::computed_deps::extract_computed_deps(cond);
		if !deps.is_complete {
			return None;
		}
		let needs_id = deps.fields.iter().any(|f| f == "id");
		let needs_in = deps.fields.iter().any(|f| f == "in");
		let needs_out = deps.fields.iter().any(|f| f == "out");
		let needed: Vec<&String> =
			deps.fields.iter().filter(|f| !matches!(f.as_str(), "id" | "in" | "out")).collect();
		if needed.is_empty() {
			// A pure id/in/out predicate is key-resident: every field it
			// reads is carried by the adjacency entry itself, so it prunes
			// before any fetch, on any table, with no inline payload, no
			// per-table snapshot and no permission precondition — an edge it
			// drops would have been dropped by the same predicate after the
			// fetch, and every survivor still resolves through the
			// permission-checked record path.
			// A comparison-shaped conjunction additionally compiles to a
			// synchronous matcher the scan runs in place of the evaluator.
			let matcher = crate::exec::operators::scan::key_matcher::KeyPredicateMatcher::compile(
				cond, direction,
			)
			.map(Arc::new);
			return Some(PayloadPredicateSpec {
				tables: std::collections::HashMap::new(),
				needs_id,
				needs_in,
				needs_out,
				key_only: true,
				matcher,
			});
		}
		// When the actor's runtime permission checks are provably skipped,
		// the permission gates below don't apply; otherwise every involved
		// permission must be `Full`, because payload evaluation reads raw
		// stored values.
		let perms_skipped = !self.should_check_perms_for_view(ns, db);
		let mut tables = std::collections::HashMap::new();
		for subject in what {
			let table = match subject {
				crate::expr::lookup::LookupSubject::Table {
					table,
					..
				}
				| crate::expr::lookup::LookupSubject::Range {
					table,
					..
				} => table,
			};
			let Ok(Some(def)) = txn.get_tb_by_name(ns, db, table, None).await else {
				return None;
			};
			if !perms_skipped && !matches!(def.permissions.select, crate::catalog::Permission::Full)
			{
				return None;
			}
			match &def.table_type {
				crate::catalog::TableType::Relation(rel) if !rel.lightweight => {}
				_ => return None,
			}
			let Ok(fields) =
				txn.all_tb_fields(def.namespace_id, def.database_id, &def.name, None).await
			else {
				return None;
			};
			let inline = crate::doc::inline_fields(&fields);
			let names: Vec<String> =
				inline.iter().map(|f| crate::doc::inline_field_name(f)).collect();
			for field in &needed {
				names.iter().position(|n| &n == field)?;
			}
			// Payload evaluation reads the referenced fields' raw stored
			// values — including everything nested inside them — so every
			// field definition rooted at a referenced field must be
			// SELECT-Full: a `meta.secret` clause under an inline `meta`
			// would otherwise let hidden sub-values steer the results.
			if !perms_skipped {
				for fd in fields.iter() {
					let Some(crate::expr::Part::Field(root)) = fd.name.0.first() else {
						continue;
					};
					if needed.iter().any(|n| n.as_str() == root.as_str())
						&& !matches!(fd.select_permission, crate::catalog::Permission::Full)
					{
						return None;
					}
				}
			}
			let referenced = names.iter().map(|n| needed.contains(&n)).collect::<Vec<_>>();
			tables.insert(
				def.name.clone(),
				PayloadTableSpec {
					generation: def.graph_inline_gen,
					fields: names,
					referenced,
				},
			);
		}
		Some(PayloadPredicateSpec {
			tables,
			needs_id,
			needs_in,
			needs_out,
			key_only: false,
			matcher: None,
		})
	}

	/// Whether a LIMIT pushed into a graph scan with no pushed predicate
	/// counts exactly the rows the query returns.
	///
	/// Without a pushed predicate the scan counts rows toward its cap at
	/// buffer time, before `resolve_record_batch` applies each edge table's
	/// SELECT permission — a permission-dropped row would consume cap budget
	/// and the capped lookup would return fewer than `limit` visible rows.
	/// The count is therefore exact only when no counted row can be dropped:
	/// the runtime provably skips permission evaluation for this session, or
	/// every named edge table's SELECT permission is `Full`. Everything else
	/// declines conservatively — a missing plan-time txn/ns/db or auth
	/// principal, an unbounded traversal (no named edge tables, so nothing to
	/// permission-check), a versioned read (which enforces the historical
	/// permissions, not the current catalog's), or any catalog miss or error.
	/// Declining only loses the cap: the downstream Limit operator still
	/// applies the real bound.
	async fn graph_limit_count_is_exact(&self, edge_tables: &[EdgeTableSpec]) -> bool {
		use crate::catalog::providers::TableProvider;
		let (Some(ns), Some(db)) = (self.ns(), self.db()) else {
			return false;
		};
		if !self.should_check_perms_for_view(ns, db) {
			return true;
		}
		if self.version.is_some() || edge_tables.is_empty() {
			return false;
		}
		let Some(txn) = self.txn() else {
			return false;
		};
		for spec in edge_tables {
			match txn.get_tb_by_name(ns, db, &spec.table, None).await {
				Ok(Some(td))
					if matches!(td.permissions.select, crate::catalog::Permission::Full) => {}
				_ => return false,
			}
		}
		true
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
		// A missing edge table or catalog error falls back to the slow path
		// too; this whole check is purely an optimisation, never a
		// correctness gate.
		if self.should_check_perms_for_view(ns, db) {
			let edge_table_names: Vec<TableName> =
				edge_tables.iter().map(|spec| spec.table.clone()).collect();
			if !crate::exec::permission::tables_have_full_select_permission(
				txn,
				ns,
				db,
				&edge_table_names,
			)
			.await
			{
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

/// Whether an ORDER clause is exactly one plain ascending `id` key — no
/// collation, no numeric coercion, no nested path. This is the ordering a
/// single adjacency scope's pointer keys already emit, which is what lets
/// a LIMIT be capped at the scan beneath the sort.
fn order_is_plain_id_asc(order: &crate::expr::order::Ordering) -> bool {
	let crate::expr::order::Ordering::Order(list) = order else {
		return false;
	};
	let [key] = list.0.as_slice() else {
		return false;
	};
	key.direction
		&& !key.collate
		&& !key.numeric
		&& matches!(key.value.0.as_slice(), [crate::expr::Part::Field(f)] if f.as_str() == "id")
}

/// Whether an `ORDER BY id` key (already confirmed plain-ascending by
/// `order_is_plain_id_asc`) still refers to the edge's own id once the
/// lookup's field list is taken into account.
///
/// The guard invokes `resolve_order_by_alias` — the same resolver the sort
/// planner (`plan_sort_consolidated`) applies to each ORDER BY key — and
/// admits only the resolutions that planner maps to the row's own id: no
/// resolution at all, or a bare single-part `id` idiom (the `SELECT id`
/// projection naming the field it sorts by). Any other resolution means a
/// field list aliased a different expression to the literal name `id`, so
/// the sort orders by that expression and the scan's ascending-edge-id
/// emission order no longer matches it. This admission can drift from the
/// sort's behaviour only if the sort planner adopts an alias mechanism
/// other than `resolve_order_by_alias`; the alias-shadowing cases in
/// `kvs::tests::graph_ordered_limit_test` pin the two together end to end.
fn order_by_id_is_source_id(fields: Option<&crate::expr::Fields>) -> bool {
	let Some(fields) = fields else {
		return true;
	};
	let id_idiom = crate::expr::Idiom(crate::expr::paths::ID.to_vec());
	match crate::exec::expression_registry::resolve_order_by_alias(&id_idiom, fields) {
		None => true,
		Some((resolved, _)) => matches!(
			&resolved,
			crate::expr::Expr::Idiom(idiom)
				if matches!(idiom.0.as_slice(), [crate::expr::Part::Field(f)] if f.as_str() == "id")
		),
	}
}
