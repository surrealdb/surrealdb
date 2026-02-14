//! SELECT statement planning for the planner.
//!
//! Handles the full SELECT pipeline: source → filter → split → aggregate →
//! sort → limit → fetch → project → timeout.

use std::sync::Arc;

use super::Planner;
use super::util::{
	all_value_sources, can_push_limit_to_scan, check_forbidden_group_by_params, derive_field_name,
	extract_bruteforce_knn, extract_count_field_names, extract_matches_context, extract_version,
	get_effective_limit_literal, has_knn_operator, idiom_to_field_name, idiom_to_field_path,
	is_count_all_eligible, strip_knn_from_condition,
};
use crate::cnf::MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE;
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::field_path::FieldPath;
#[cfg(all(storage, not(target_family = "wasm")))]
use crate::exec::operators::ExternalSort;
use crate::exec::operators::{
	Aggregate, AnalyzePlan, Compute, ExplainPlan, Fetch, FieldSelection, Filter, Limit, Project,
	ProjectValue, Projection, RandomShuffle, Scan, SelectProject, Sort, SortByKey, SortDirection,
	SortKey, SortTopK, SortTopKByKey, SourceExpr, Split, Timeout, Union, UnwrapExactlyOne,
};
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::field::{Field, Fields};
use crate::expr::{Cond, Expr, Idiom, Literal};

/// Configuration for the SELECT pipeline.
///
/// Bundles optional clauses from a SELECT statement to reduce parameter counts.
#[derive(Debug, Default)]
pub(crate) struct SelectPipelineConfig {
	pub cond: Option<crate::expr::cond::Cond>,
	pub split: Option<crate::expr::split::Splits>,
	pub group: Option<crate::expr::group::Groups>,
	pub order: Option<crate::expr::order::Ordering>,
	pub limit: Option<crate::expr::limit::Limit>,
	pub start: Option<crate::expr::start::Start>,
	pub omit: Vec<Expr>,
	pub is_value_source: bool,
	pub tempfiles: bool,
	/// True when the WHERE predicate has been pushed into the Scan operator.
	/// Currently informational; the actual guard is `cond: None` in the config.
	#[allow(dead_code)]
	pub filter_pushed: bool,
}

impl<'ctx> Planner<'ctx> {
	/// Plan a SELECT statement.
	pub(crate) fn plan_select_statement(
		&self,
		select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let crate::expr::statements::SelectStatement {
			fields,
			omit,
			only,
			what,
			with,
			cond,
			split,
			group,
			order,
			limit,
			start,
			fetch,
			version,
			timeout,
			explain,
			tempfiles,
		} = select;

		let version = extract_version(version, self)?;

		// ── COUNT fast-path ─────────────────────────────────────────
		// Detect `SELECT count() FROM <table> GROUP ALL` (no WHERE,
		// SPLIT, ORDER, FETCH, OMIT) and replace the entire pipeline
		// with a single CountScan operator that calls txn.count().
		if is_count_all_eligible(&fields, &group, &cond, &split, &order, &fetch, &omit, &what) {
			use crate::exec::operators::CountScan;
			// SAFETY: is_count_all_eligible verifies what.len() == 1
			let table_expr =
				self.physical_expr(what.into_iter().next().expect("what verified non-empty"))?;
			// Extract output field names (respecting AS aliases).
			let field_names = extract_count_field_names(&fields);
			let count_scan: Arc<dyn ExecOperator> =
				Arc::new(CountScan::new(table_expr, version, field_names));

			let timed = match timeout {
				Expr::Literal(Literal::None) => count_scan,
				timeout_expr => {
					let timeout_phys = self.physical_expr(timeout_expr)?;
					Arc::new(Timeout::new(count_scan, Some(timeout_phys))) as Arc<dyn ExecOperator>
				}
			};

			return if only {
				Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
			} else {
				Ok(timed)
			};
		}
		// ── end COUNT fast-path ─────────────────────────────────────

		// NOTE: IndexCountScan is available for `SELECT count() FROM <table>
		// WHERE <cond> GROUP ALL` when a matching COUNT index exists.
		// However, automatic detection is deferred until the planner can
		// verify COUNT index existence at plan time (requires transaction
		// access or catalog snapshot). The fallback path (full scan +
		// filter) is slower than the regular pipeline that can use index
		// scans, so auto-detection would regress non-COUNT-index cases.
		// See `is_indexed_count_eligible` in util.rs and `IndexCountScan`
		// in operators/index_count_scan.rs for the ready-to-use components.

		let is_value_source = all_value_sources(&what);

		let primary_table = what.iter().find_map(|expr| {
			if let Expr::Table(table_name) = expr {
				Some(table_name.clone())
			} else {
				None
			}
		});

		// Analyze WHERE clause for MATCHES operators used by index functions
		let planning_ctx: std::borrow::Cow<'_, crate::ctx::FrozenContext> =
			if let Some(ref c) = cond {
				let mut matches_ctx = extract_matches_context(c);
				if !matches_ctx.is_empty() {
					if let Some(ref table) = primary_table {
						matches_ctx.set_table(table.clone());
					}
					let mut child = crate::ctx::Context::new(self.ctx);
					child.set_matches_context(matches_ctx);
					std::borrow::Cow::Owned(child.freeze())
				} else {
					std::borrow::Cow::Borrowed(self.ctx)
				}
			} else {
				std::borrow::Cow::Borrowed(self.ctx)
			};

		// Create the planning planner early so we can compile the predicate
		// before creating sources (needed for filter pushdown into Scan).
		let planning_planner = Planner::new(&planning_ctx);

		// Compute which fields are needed by the query for selective computed field evaluation
		let needed_fields = Self::extract_needed_fields(
			&fields,
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group.as_ref(),
			split.as_ref(),
		);

		// Determine if the source will be a single Scan operator.
		// Filter pushdown: always push the WHERE predicate into Scan when
		// the source is a single Scan. The cond (AST) stays for index selection.
		let source_is_single_scan = what.len() == 1
			&& matches!(what[0], Expr::Table(_) | Expr::FunctionCall(_) | Expr::Postfix { .. })
			|| (what.len() == 1
				&& matches!(&what[0], Expr::Param(p) if {
					matches!(self.ctx.value(p.as_str()), Some(crate::val::Value::Table(_)))
				}));

		// ── KNN handling ───────────────────────────────────────────
		// KNN operators cannot be evaluated as boolean predicates in a
		// filter. We handle them in two ways:
		//
		// 1. Brute-force KNN (`<|k, DIST|>`): extract parameters and wrap the source in a KnnTopK
		//    operator. Only works when the query vector is a literal (not a parameter).
		//
		// 2. HNSW KNN (`<|k, ef|>`): handled by index analysis at scan time. The scan dispatch
		//    routes to KnnScan when an HNSW index exists.
		//
		// KNN operators are always stripped from the filter predicate (they produce distances,
		// not booleans). If unsupported KNN variants remain after stripping, an error is
		// returned.
		let has_knn = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));
		let brute_force_knn = if has_knn {
			cond.as_ref().and_then(extract_bruteforce_knn)
		} else {
			None
		};

		// Determine separate conditions for index analysis and filtering.
		// - cond_for_index: passed to Scan for index analysis (includes KNN for HNSW detection)
		// - cond_for_filter: used for scan predicate / pipeline filter (KNN always stripped)
		let (cond_for_index, cond_for_filter) = if has_knn {
			let stripped = cond.as_ref().and_then(strip_knn_from_condition);
			// Safety check: if the residual still contains KNN operators
			// (e.g. KNN nested under OR, or unhandled KTree variants),
			// return a proper error.
			if stripped.as_ref().is_some_and(|c| has_knn_operator(&c.0)) {
				return Err(Error::Query {
					message: "KNN operators nested in OR/NOT expressions or mixed with \
					 unsupported KNN variants are not supported"
						.to_string(),
				});
			}
			if brute_force_knn.is_some() {
				// Brute-force KNN: index analysis doesn't need to see KNN
				(stripped.clone(), stripped)
			} else {
				// HNSW KNN: keep original condition for index analysis so
				// it can detect HNSW indexes and route to KnnScan.
				(cond, stripped)
			}
		} else {
			// No KNN: use original condition for both
			let c = cond;
			(c.clone(), c)
		};

		// Compile predicate for pushdown into Scan
		let scan_predicate = if source_is_single_scan {
			cond_for_filter
				.as_ref()
				.map(|c| planning_planner.physical_expr(c.0.clone()))
				.transpose()?
		} else {
			None
		};

		// Determine limit pushdown eligibility:
		// No SPLIT, no GROUP BY, and ORDER BY must be scan-compatible (or absent).
		// WHERE does NOT block limit pushdown since the filter is inside Scan.
		let push_limit = source_is_single_scan
			&& limit.is_some()
			&& can_push_limit_to_scan(&split, &group, &order);

		// Compile limit/start for pushdown
		let (scan_limit, scan_start) = if push_limit {
			(
				limit.as_ref().map(|l| planning_planner.physical_expr(l.0.clone())).transpose()?,
				start.as_ref().map(|s| planning_planner.physical_expr(s.0.clone())).transpose()?,
			)
		} else {
			(None, None)
		};

		let source = planning_planner.plan_sources(
			what,
			version,
			// Pass cond_for_index which includes KNN operators for HNSW
			// index detection. For brute-force KNN, this is already stripped
			// since KnnTopK handles it without index support.
			cond_for_index.as_ref(),
			order.as_ref(),
			with.as_ref(),
			needed_fields,
			scan_predicate,
			scan_limit,
			scan_start,
		)?;

		// Wrap source with KnnTopK if brute-force KNN was detected
		let had_bruteforce_knn = brute_force_knn.is_some();
		let source = if let Some(knn_params) = brute_force_knn {
			use crate::exec::operators::KnnTopK;
			// For multi-source queries (source_is_single_scan == false), apply
			// the residual filter BEFORE KnnTopK so that top-K is computed only
			// over rows satisfying the non-KNN predicates. Without this, nearer
			// rows that fail the filter can displace valid rows in the heap.
			let input = if !source_is_single_scan {
				if let Some(ref cond) = cond_for_filter {
					let predicate = planning_planner.physical_expr(cond.0.clone())?;
					Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
				} else {
					source
				}
			} else {
				source // single scan: filter already pushed into Scan
			};
			Arc::new(KnnTopK::new(
				input,
				knn_params.field,
				knn_params.vector,
				knn_params.k as usize,
				knn_params.distance,
			)) as Arc<dyn ExecOperator>
		} else {
			source
		};
		// ── end KNN ────────────────────────────────────────────────

		// Build pipeline config. When pushdown is active, clear the corresponding
		// fields so plan_pipeline does not create redundant operators.
		let filter_pushed = source_is_single_scan && cond_for_filter.is_some();
		let config = SelectPipelineConfig {
			cond: if source_is_single_scan || had_bruteforce_knn {
				// single scan: filter pushed into Scan
				// brute-force KNN: filter applied before KnnTopK (multi-source)
				//   or pushed into Scan (single source)
				None
			} else {
				cond_for_filter
			},
			split,
			group,
			// Clear order when limit is pushed and order matches scan direction
			// (Sort would be redundant since Scan already scans in the right order)
			order: if push_limit {
				None
			} else {
				order
			},
			limit: if push_limit {
				None
			} else {
				limit
			},
			start: if push_limit {
				None
			} else {
				start
			},
			omit,
			is_value_source,
			tempfiles,
			filter_pushed,
		};

		let projected = planning_planner.plan_pipeline(source, Some(fields), config)?;

		let fetched = planning_planner.plan_fetch(fetch, projected)?;

		let timed = match timeout {
			Expr::Literal(Literal::None) => fetched,
			timeout_expr => {
				let timeout_phys = planning_planner.physical_expr(timeout_expr)?;
				Arc::new(Timeout::new(fetched, Some(timeout_phys))) as Arc<dyn ExecOperator>
			}
		};

		let result: Arc<dyn ExecOperator> = if only {
			Arc::new(UnwrapExactlyOne::new(timed, !is_value_source))
		} else {
			timed
		};

		// ── EXPLAIN rewriting ──────────────────────────────────────
		// SELECT ... EXPLAIN      → EXPLAIN SELECT ...       (ExplainPlan)
		// SELECT ... EXPLAIN FULL → EXPLAIN ANALYZE SELECT . (AnalyzePlan)
		//
		// Uses JSON format for consistency with the old executor's structured
		// output format (`[{ detail: ..., operation: ... }]`).
		match explain {
			Some(crate::expr::explain::Explain(full)) => {
				if full {
					Ok(Arc::new(AnalyzePlan {
						plan: result,
						format: crate::expr::ExplainFormat::Json,
						redact_duration: self.ctx.redact_duration(),
					}))
				} else {
					Ok(Arc::new(ExplainPlan {
						plan: result,
						format: crate::expr::ExplainFormat::Json,
					}))
				}
			}
			None => Ok(result),
		}
	}

	/// Plan the SELECT pipeline after the source is determined.
	pub(crate) fn plan_pipeline(
		&self,
		source: Arc<dyn ExecOperator>,
		fields: Option<Fields>,
		config: SelectPipelineConfig,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let SelectPipelineConfig {
			cond,
			split,
			group,
			order,
			limit,
			start,
			omit,
			is_value_source,
			tempfiles,
			filter_pushed: _,
		} = config;

		let filtered = if let Some(cond) = cond {
			let predicate = self.physical_expr(cond.0)?;
			Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
		} else {
			source
		};

		let split_op = if let Some(splits) = split {
			let idioms: Vec<_> = splits.into_iter().map(|s| s.0).collect();
			Arc::new(Split {
				input: filtered,
				idioms,
				metrics: Arc::new(OperatorMetrics::new()),
			}) as Arc<dyn ExecOperator>
		} else {
			filtered
		};

		let fields = fields.unwrap_or_else(Fields::all);

		let (grouped, skip_projections) = if let Some(groups) = group {
			let group_by: Vec<_> = groups.0.into_iter().map(|g| g.0).collect();
			check_forbidden_group_by_params(&fields)?;

			let (aggregates, group_by_exprs) = self.plan_aggregation(&fields, &group_by)?;

			(
				Arc::new(Aggregate::new(split_op, group_by, group_by_exprs, aggregates))
					as Arc<dyn ExecOperator>,
				true,
			)
		} else {
			(split_op, false)
		};

		let (sorted, sort_only_omits) = if let Some(order) = order {
			if skip_projections {
				(self.plan_sort(grouped, order, &start, &limit, tempfiles)?, vec![])
			} else {
				self.plan_sort_consolidated(grouped, order, &fields, &start, &limit, tempfiles)?
			}
		} else {
			(grouped, vec![])
		};

		let limited = if limit.is_some() || start.is_some() {
			let limit_expr = limit.map(|l| self.physical_expr(l.0)).transpose()?;
			let offset_expr = start.map(|s| self.physical_expr(s.0)).transpose()?;
			Arc::new(Limit::new(sorted, limit_expr, offset_expr)) as Arc<dyn ExecOperator>
		} else {
			sorted
		};

		let mut all_omit = omit;
		for field_name in sort_only_omits {
			all_omit.push(Expr::Idiom(Idiom::field(field_name)));
		}

		let projected = if skip_projections {
			if !all_omit.is_empty() {
				let omit_fields = self.plan_omit(all_omit)?;
				Arc::new(Project::new(limited, vec![], omit_fields, true)) as Arc<dyn ExecOperator>
			} else {
				limited
			}
		} else {
			self.plan_projections(fields, all_omit, limited, is_value_source)?
		};

		Ok(projected)
	}

	/// Plan the FROM sources — handles multiple targets with Union.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn plan_sources(
		&self,
		what: Vec<Expr>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
		needed_fields: Option<std::collections::HashSet<String>>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		if what.is_empty() {
			return Err(Error::Query {
				message: "SELECT requires at least one source".to_string(),
			});
		}

		let mut source_plans = Vec::with_capacity(what.len());
		for expr in what {
			let plan = self.plan_source(
				expr,
				version.clone(),
				cond,
				order,
				with,
				needed_fields.clone(),
				scan_predicate.clone(),
				scan_limit.clone(),
				scan_start.clone(),
			)?;
			source_plans.push(plan);
		}

		if source_plans.len() == 1 {
			Ok(source_plans.pop().expect("source_plans verified non-empty"))
		} else {
			Ok(Arc::new(Union::new(source_plans)))
		}
	}

	/// Plan a single FROM source.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn plan_source(
		&self,
		expr: Expr,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
		needed_fields: Option<std::collections::HashSet<String>>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::val::Value;

		match expr {
			Expr::Table(_) => {
				let table_expr = self.physical_expr(expr)?;
				Ok(Arc::new(Scan::new(
					table_expr,
					version,
					cond.cloned(),
					order.cloned(),
					with.cloned(),
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
				)) as Arc<dyn ExecOperator>)
			}

			Expr::Literal(crate::expr::literal::Literal::RecordId(record_id_lit)) => {
				let table_expr = self.physical_expr(Expr::Literal(
					crate::expr::literal::Literal::RecordId(record_id_lit),
				))?;
				Ok(Arc::new(Scan::new(
					table_expr,
					version,
					None,
					None,
					None,
					needed_fields,
					None,
					None,
					None,
				)) as Arc<dyn ExecOperator>)
			}

			Expr::Select(inner_select) => {
				if version.is_some() {
					return Err(Error::Query {
						message: "VERSION clause cannot be used with a subquery source. \
								  Place the VERSION clause inside the subquery instead."
							.to_string(),
					});
				}
				self.plan_select_statement(*inner_select)
			}

			Expr::Literal(crate::expr::literal::Literal::Array(_)) => {
				let phys_expr = self.physical_expr(expr)?;
				Ok(Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>)
			}

			Expr::Param(param) => match self.ctx.value(param.as_str()) {
				Some(Value::Table(_)) => {
					let table_expr = self.physical_expr(Expr::Param(param.clone()))?;
					Ok(Arc::new(Scan::new(
						table_expr,
						version,
						cond.cloned(),
						order.cloned(),
						with.cloned(),
						needed_fields,
						scan_predicate,
						scan_limit,
						scan_start,
					)) as Arc<dyn ExecOperator>)
				}
				Some(Value::RecordId(_)) => {
					let table_expr = self.physical_expr(Expr::Param(param.clone()))?;
					Ok(Arc::new(Scan::new(
						table_expr,
						version,
						None,
						None,
						None,
						needed_fields,
						None,
						None,
						None,
					)) as Arc<dyn ExecOperator>)
				}
				Some(_) | None => {
					let phys_expr = self.physical_expr(Expr::Param(param))?;
					Ok(Arc::new(SourceExpr {
						expr: phys_expr,
						metrics: Arc::new(OperatorMetrics::new()),
					}) as Arc<dyn ExecOperator>)
				}
			},

			Expr::FunctionCall(_) => {
				let source_expr = self.physical_expr(expr)?;
				Ok(Arc::new(Scan::new(
					source_expr,
					version,
					cond.cloned(),
					order.cloned(),
					with.cloned(),
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
				)) as Arc<dyn ExecOperator>)
			}

			Expr::Postfix {
				..
			} => {
				let source_expr = self.physical_expr(expr)?;
				Ok(Arc::new(Scan::new(
					source_expr,
					version,
					cond.cloned(),
					order.cloned(),
					with.cloned(),
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
				)) as Arc<dyn ExecOperator>)
			}

			other => {
				let phys_expr = self.physical_expr(other)?;
				Ok(Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan projections (SELECT fields or SELECT VALUE).
	pub(crate) fn plan_projections(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
		is_value_source: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let expr = self.physical_expr(selector.expr)?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					let omit_fields = if !omit.is_empty() {
						self.plan_omit(omit)?
					} else {
						vec![]
					};
					return Ok(Arc::new(Project::new(input, vec![], omit_fields, true))
						as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				if is_value_source
					&& !has_wildcard
					&& field_list.len() == 1
					&& let Some(Field::Single(selector)) = field_list.first()
					&& selector.alias.is_none()
					&& let Expr::Param(_) = &selector.expr
				{
					let expr = self.physical_expr(selector.expr.clone())?;
					return Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>);
				}

				let mut field_selections = Vec::with_capacity(field_list.len());

				for field in field_list {
					if let Field::Single(selector) = field {
						let field_selection = if let Some(alias) = &selector.alias {
							let output_name = idiom_to_field_name(alias);
							let expr = self.physical_expr(selector.expr)?;
							FieldSelection::with_alias(output_name, expr)
						} else {
							let output_name_or_path = match &selector.expr {
								Expr::Idiom(idiom) => Ok(idiom_to_field_path(idiom)),
								_ => Err(derive_field_name(&selector.expr)),
							};
							let expr = self.physical_expr(selector.expr)?;
							match output_name_or_path {
								Ok(output_path) => {
									FieldSelection::from_field_path(output_path, expr)
								}
								Err(output_name) => FieldSelection::new(output_name, expr),
							}
						};

						field_selections.push(field_selection);
					}
				}

				let omit_fields = if !omit.is_empty() {
					self.plan_omit(omit)?
				} else {
					vec![]
				};

				Ok(Arc::new(Project::new(input, field_selections, omit_fields, has_wildcard))
					as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan OMIT fields — convert expressions to idioms.
	pub(crate) fn plan_omit(
		&self,
		omit: Vec<Expr>,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		let mut fields = Vec::with_capacity(omit.len());
		for expr in omit {
			let mut idioms = self.resolve_field_idioms(expr)?;
			fields.append(&mut idioms);
		}
		Ok(fields)
	}

	/// Plan FETCH clause.
	pub(crate) fn plan_fetch(
		&self,
		fetch: Option<crate::expr::fetch::Fetchs>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let Some(fetchs) = fetch else {
			return Ok(input);
		};

		let mut fields = Vec::with_capacity(fetchs.len());
		for fetch_item in fetchs {
			let mut idioms = self.resolve_field_idioms(fetch_item.0)?;
			fields.append(&mut idioms);
		}

		Ok(Arc::new(Fetch {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}) as Arc<dyn ExecOperator>)
	}

	/// Plan ORDER BY.
	pub(crate) fn plan_sort(
		&self,
		input: Arc<dyn ExecOperator>,
		order: crate::expr::order::Ordering,
		start: &Option<crate::expr::start::Start>,
		limit: &Option<crate::expr::limit::Limit>,
		#[allow(unused)] tempfiles: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::expr::order::Ordering;

		match order {
			Ordering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok(Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>)
			}
			Ordering::Order(order_list) => {
				let order_by = self.convert_order_list(order_list)?;

				#[cfg(all(storage, not(target_family = "wasm")))]
				if tempfiles && let Some(temp_dir) = self.ctx.temporary_directory() {
					return Ok(
						Arc::new(ExternalSort::new(input, order_by, temp_dir.to_path_buf()))
							as Arc<dyn ExecOperator>,
					);
				}

				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize
				{
					return Ok(Arc::new(SortTopK::new(input, order_by, effective_limit))
						as Arc<dyn ExecOperator>);
				}

				Ok(Arc::new(Sort::new(input, order_by)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan ORDER BY with consolidated expression evaluation.
	pub(crate) fn plan_sort_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		order: crate::expr::order::Ordering,
		fields: &Fields,
		start: &Option<crate::expr::start::Start>,
		limit: &Option<crate::expr::limit::Limit>,
		#[allow(unused)] tempfiles: bool,
	) -> Result<(Arc<dyn ExecOperator>, Vec<String>), Error> {
		use crate::expr::order::Ordering;
		use crate::expr::part::Part;

		match order {
			Ordering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok((
					Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>,
					vec![],
				))
			}
			Ordering::Order(order_list) => {
				let mut registry = ExpressionRegistry::new();
				let mut sort_keys = Vec::with_capacity(order_list.len());
				let mut sort_only_fields: Vec<String> = Vec::new();

				for order_field in order_list.iter() {
					let idiom = &order_field.value;

					let field_path = if let Some((resolved_expr, alias)) =
						resolve_order_by_alias(idiom, fields)
					{
						match &resolved_expr {
							Expr::Idiom(inner_idiom) => {
								let has_lookups =
									inner_idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

								if has_lookups {
									let name = registry.register(
										&resolved_expr,
										ComputePoint::Sort,
										Some(alias.clone()),
										self.ctx,
									)?;
									FieldPath::field(name)
								} else {
									match FieldPath::try_from(inner_idiom) {
										Ok(path) => path,
										Err(_) => {
											let name = registry.register(
												&resolved_expr,
												ComputePoint::Sort,
												Some(alias.clone()),
												self.ctx,
											)?;
											FieldPath::field(name)
										}
									}
								}
							}
							_ => {
								let name = registry.register(
									&resolved_expr,
									ComputePoint::Sort,
									Some(alias.clone()),
									self.ctx,
								)?;
								FieldPath::field(name)
							}
						}
					} else {
						match FieldPath::try_from(idiom) {
							Ok(path) => path,
							Err(_) => {
								let expr = Expr::Idiom(idiom.clone());
								let name =
									registry.register(&expr, ComputePoint::Sort, None, self.ctx)?;
								sort_only_fields.push(name.clone());
								FieldPath::field(name)
							}
						}
					};

					let direction = if order_field.direction {
						SortDirection::Asc
					} else {
						SortDirection::Desc
					};

					let mut key = SortKey::new(field_path);
					key.direction = direction;
					key.collate = order_field.collate;
					key.numeric = order_field.numeric;
					sort_keys.push(key);
				}

				let computed = if registry.has_expressions_for_point(ComputePoint::Sort) {
					let compute_fields = registry.get_expressions_for_point(ComputePoint::Sort);
					Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
				} else {
					input
				};

				// Use heap-based TopK when the effective limit is small.
				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize
				{
					return Ok((
						Arc::new(SortTopKByKey::new(computed, sort_keys, effective_limit))
							as Arc<dyn ExecOperator>,
						sort_only_fields,
					));
				}

				Ok((
					Arc::new(SortByKey::new(computed, sort_keys)) as Arc<dyn ExecOperator>,
					sort_only_fields,
				))
			}
		}
	}

	/// Plan SELECT projections with consolidated approach.
	#[allow(dead_code)]
	pub(crate) fn plan_projections_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		fields: &Fields,
		omit: &[Expr],
		computed_fields: &[(String, String)],
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				if !omit.is_empty() {
					return Err(Error::Query {
						message: "OMIT clause with SELECT VALUE not supported".to_string(),
					});
				}
				let expr = self.physical_expr(selector.expr.clone())?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					let omit_names: Vec<String> = omit
						.iter()
						.filter_map(|e| {
							if let Expr::Idiom(idiom) = e {
								Some(idiom_to_field_name(idiom))
							} else {
								None
							}
						})
						.collect();
					let projections: Vec<Projection> = std::iter::once(Projection::All)
						.chain(omit_names.into_iter().map(Projection::Omit))
						.collect();
					return Ok(Arc::new(SelectProject::new(
						input,
						projections,
						Arc::new(OperatorMetrics::new()),
					)) as Arc<dyn ExecOperator>);
				}

				let mut projections = Vec::with_capacity(field_list.len());
				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				if has_wildcard {
					projections.push(Projection::All);
				}

				for field in field_list {
					match field {
						Field::All => {}
						Field::Single(selector) => {
							let output_name = if let Some(alias) = &selector.alias {
								idiom_to_field_name(alias)
							} else {
								derive_field_name(&selector.expr)
							};

							let maybe_computed =
								computed_fields.iter().find(|(_, out)| out == &output_name);

							if let Some((internal_name, _)) = maybe_computed {
								if internal_name != &output_name {
									projections.push(Projection::Rename {
										from: internal_name.clone(),
										to: output_name,
									});
								} else {
									projections.push(Projection::Include(output_name));
								}
							} else {
								projections.push(Projection::Include(output_name));
							}
						}
					}
				}

				if !omit.is_empty() {
					for e in omit {
						if let Expr::Idiom(idiom) = e {
							projections.push(Projection::Omit(idiom_to_field_name(idiom)));
						}
					}
				}

				Ok(Arc::new(SelectProject::new(
					input,
					projections,
					Arc::new(OperatorMetrics::new()),
				)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Convert a LET statement to an execution plan.
	pub(crate) fn plan_let_statement(
		&self,
		let_stmt: crate::expr::statements::SetStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::exec::operators::{ExprPlan, LetPlan};

		let crate::expr::statements::SetStatement {
			name,
			what,
			kind: _,
		} = let_stmt;

		let value: Arc<dyn ExecOperator> = match what {
			Expr::Select(select) => self.plan_select_statement(*select)?,
			Expr::Create(_) => {
				return Err(Error::PlannerUnsupported(
					"CREATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Update(_) => {
				return Err(Error::PlannerUnsupported(
					"UPDATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Upsert(_) => {
				return Err(Error::PlannerUnsupported(
					"UPSERT statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Delete(_) => {
				return Err(Error::PlannerUnsupported(
					"DELETE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Insert(_) => {
				return Err(Error::PlannerUnsupported(
					"INSERT statements in LET not yet supported in execution plans".to_string(),
				));
			}
			Expr::Relate(_) => {
				return Err(Error::PlannerUnsupported(
					"RELATE statements in LET not yet supported in execution plans".to_string(),
				));
			}
			other => {
				let expr = self.physical_expr(other)?;
				if expr.references_current_value() {
					return Err(Error::Query {
						message: "LET expression cannot reference current row context".to_string(),
					});
				}
				Arc::new(ExprPlan::new(expr)) as Arc<dyn ExecOperator>
			}
		};

		Ok(Arc::new(LetPlan::new(name, value)) as Arc<dyn ExecOperator>)
	}

	// ========================================================================
	// Field Resolution Helpers
	// ========================================================================

	/// Resolve a field expression to one or more idioms.
	pub(crate) fn resolve_field_idioms(
		&self,
		expr: Expr,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		use crate::expr::Function;

		match expr {
			Expr::Idiom(idiom) => Ok(vec![idiom]),
			Expr::Param(ref param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				let s = value.clone().coerce_to::<String>().map_err(|_| Error::InvalidFetch {
					value: value.into_literal(),
				})?;
				let idiom: Idiom = crate::syn::idiom(&s)
					.map_err(|_| Error::InvalidFetch {
						value: expr,
					})?
					.into();
				Ok(vec![idiom])
			}
			Expr::FunctionCall(ref call) => match &call.receiver {
				Function::Normal(name) if self.function_registry().is_projection(name) => {
					// Generic projection function handling: resolve each argument
					// as a string (single field) or array of strings (multiple fields)
					// and parse each as an idiom.
					let mut idioms = Vec::new();
					for arg in &call.arguments {
						match self.resolve_expr_to_string(arg) {
							Ok(s) => {
								let idiom: Idiom = crate::syn::idiom(&s)
									.map_err(|e| Error::Query {
										message: format!(
											"Failed to parse field path '{}': {}",
											s, e
										),
									})?
									.into();
								idioms.push(idiom);
							}
							Err(_) => {
								// Try resolving as an array of strings
								let strings =
									self.resolve_expr_to_string_array(arg).map_err(|_| {
										Error::Query {
											message: format!(
												"Projection function '{}' argument could not \
												 be resolved to a field path",
												name
											),
										}
									})?;
								for s in strings {
									let idiom: Idiom = crate::syn::idiom(&s)
										.map_err(|e| Error::Query {
											message: format!(
												"Failed to parse field path '{}': {}",
												s, e
											),
										})?
										.into();
									idioms.push(idiom);
								}
							}
						}
					}
					if idioms.is_empty() {
						return Err(Error::Query {
							message: format!(
								"Projection function '{}' requires at least one argument",
								name
							),
						});
					}
					Ok(idioms)
				}
				_ => Err(Error::InvalidFetch {
					value: expr,
				}),
			},
			other => Err(Error::InvalidFetch {
				value: other,
			}),
		}
	}

	fn resolve_expr_to_string(&self, expr: &Expr) -> Result<String, Error> {
		match expr {
			Expr::Literal(Literal::String(s)) => Ok(s.clone()),
			Expr::Param(param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				value.coerce_to::<String>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	fn resolve_expr_to_string_array(&self, expr: &Expr) -> Result<Vec<String>, Error> {
		match expr {
			Expr::Literal(Literal::Array(items)) => {
				items.iter().map(|item| self.resolve_expr_to_string(item)).collect()
			}
			Expr::Param(param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				value.coerce_to::<Vec<String>>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to an array of strings"
						.to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	/// Extract the set of field names needed by a SELECT statement.
	///
	/// Returns `None` if all fields are needed (SELECT *, wildcard present, or
	/// opaque expressions prevent static analysis). Returns `Some(set)` with
	/// the root field names needed by projections, WHERE, ORDER, GROUP, SPLIT.
	pub(crate) fn extract_needed_fields(
		fields: &Fields,
		omit: &[Expr],
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		group: Option<&crate::expr::group::Groups>,
		split: Option<&crate::expr::split::Splits>,
	) -> Option<std::collections::HashSet<String>> {
		use crate::expr::Part;
		use crate::expr::visit::{Visit, Visitor};

		// Check for SELECT * (wildcard) -- need all fields
		match fields {
			Fields::Select(field_list) => {
				if field_list.iter().any(|f| matches!(f, Field::All)) {
					return None;
				}
			}
			Fields::Value(_) => {
				// SELECT VALUE expr -- still selective
			}
		}

		/// Visitor that collects root field names from idioms and detects opaque expressions.
		struct NeededFieldExtractor {
			fields: std::collections::HashSet<String>,
			has_opaque: bool,
		}

		impl Visitor for NeededFieldExtractor {
			type Error = std::convert::Infallible;

			fn visit_idiom(&mut self, idiom: &crate::expr::Idiom) -> Result<(), Self::Error> {
				if let Some(Part::Field(name)) = idiom.0.first() {
					self.fields.insert(name.clone());
				}
				// Walk nested parts for embedded expressions
				for p in idiom.0.iter() {
					self.visit_part(p)?;
				}
				Ok(())
			}

			fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
				match expr {
					// Parameters could reference any field
					Expr::Param(_) => {
						self.has_opaque = true;
					}
					_ => {
						expr.visit(self)?;
					}
				}
				Ok(())
			}
		}

		let mut extractor = NeededFieldExtractor {
			fields: std::collections::HashSet::new(),
			has_opaque: false,
		};

		// Walk projection expressions
		match fields {
			Fields::Value(selector) => {
				let _ = extractor.visit_expr(&selector.expr);
			}
			Fields::Select(field_list) => {
				for field in field_list {
					if let Field::Single(selector) = field {
						let _ = extractor.visit_expr(&selector.expr);
						if let Some(alias) = &selector.alias {
							let _ = extractor.visit_idiom(alias);
						}
					}
				}
			}
		}

		// Walk OMIT fields (they may reference computed fields that need evaluation)
		for expr in omit {
			let _ = extractor.visit_expr(expr);
		}

		// Walk WHERE condition
		if let Some(cond) = cond {
			let _ = extractor.visit_expr(&cond.0);
		}

		// Walk ORDER BY
		if let Some(ordering) = order {
			match ordering {
				crate::expr::order::Ordering::Random => {}
				crate::expr::order::Ordering::Order(order_list) => {
					for order in order_list.iter() {
						let _ = extractor.visit_idiom(&order.value);
					}
				}
			}
		}

		// Walk GROUP BY
		if let Some(groups) = group {
			for group in groups.0.iter() {
				let _ = extractor.visit_idiom(&group.0);
			}
		}

		// Walk SPLIT
		if let Some(splits) = split {
			for split in splits.iter() {
				let _ = extractor.visit_idiom(&split.0);
			}
		}

		if extractor.has_opaque {
			None
		} else {
			Some(extractor.fields)
		}
	}
}
