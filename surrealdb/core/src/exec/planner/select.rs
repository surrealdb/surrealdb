//! SELECT statement planning for the planner.
//!
//! Handles the full SELECT pipeline: source → filter → split → aggregate →
//! sort → limit → project → fetch → timeout.
//!
//! Projection uses a fast path that classifies SELECT fields at plan time:
//! - **Simple field paths** (e.g. `name`, `age`): handled by `SelectProject` with synchronous field
//!   selection — zero async/expression overhead.
//! - **Complex expressions** (e.g. `math::sum(scores) AS total`): pre-evaluated by a `Compute`
//!   operator, then picked by `SelectProject`.
//! - **Projection functions** or **nested output paths**: fall back to the full `Project` operator.
//!
//! An `ExpressionRegistry` is shared between ORDER BY and projection planning
//! to deduplicate expressions that appear in both clauses.

use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::Planner;
use super::util::{
	SELECT_ITERATION_PARAMS, all_value_sources, check_forbidden_group_by_params, derive_field_name,
	extract_bruteforce_knn, extract_count_field_names, extract_matches_context,
	extract_record_id_point_lookup, extract_version, fold_condition_expressions,
	get_effective_limit_literal, has_knn_k_operator, has_knn_operator, has_top_level_or,
	idiom_to_field_name, idiom_to_field_path, index_covers_ordering, is_count_all_eligible,
	is_indexed_count_eligible, order_is_scan_compatible, resolve_condition_params,
	resolve_param_value, resolve_projection_field_idioms, strip_fts_condition,
	strip_index_conditions, strip_knn_from_condition,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::cnf::MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE;
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::field_path::FieldPath;
use crate::exec::index::access_path::{AccessPath, BTreeAccess, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
#[cfg(all(storage, not(target_family = "wasm")))]
use crate::exec::operators::ExternalSort;
use crate::exec::operators::scan::determine_scan_direction;
use crate::exec::operators::scan::resolved::{ResolvedTableContext, resolve_table_context};
use crate::exec::operators::{
	Aggregate, AnalyzePlan, Compute, DynamicScan, ExplainPlan, Fetch, FieldSelection, Filter,
	KnnTopK, Limit, Project, ProjectValue, Projection, RandomShuffle, RecordIdScan, SelectProject,
	Sort, SortByKey, SortDirection, SortKey, SortTopK, SortTopKByKey, SourceExpr, Split, TableScan,
	Timeout, Union, UnionIndexScan, UnwrapExactlyOne,
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
	pub tempfiles: bool,
	/// True when the WHERE predicate has been pushed into the Scan operator.
	/// Currently informational; the actual guard is `cond: None` in the config.
	#[allow(dead_code)]
	pub filter_pushed: bool,
	/// Pre-compiled predicate. When set, `plan_pipeline` uses this directly
	/// instead of re-compiling `cond` into a new PhysicalExpr. This avoids
	/// duplicate compilation when the same predicate was already compiled
	/// for scan pushdown but ended up not being consumed by the source.
	pub precompiled_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
}

/// Describes how the WHERE predicate should be handled after source planning.
pub(crate) enum FilterAction {
	/// Source did not analyze the predicate. Use the original `cond_for_filter`.
	UseOriginal,
	/// All conditions consumed by the source. No Filter needed.
	FullyConsumed,
	/// Partial residual remains. Create a Filter with this condition only.
	Residual(Cond),
}

/// Result of planning FROM sources.
///
/// Tracks how the WHERE predicate and limit/start were handled by the
/// source operator, so the caller can avoid duplicating them in the
/// outer pipeline.
pub(crate) struct PlannedSource {
	operator: Arc<dyn ExecOperator>,
	/// How the WHERE predicate was handled by the source.
	filter_action: FilterAction,
	/// The limit and start values were consumed by the source operator.
	limit_pushed: bool,
}

/// Determine `FilterAction` when a scan predicate has been compiled.
///
/// When the planner compiled a `scan_predicate` (physical WHERE expression),
/// the source operator is expected to apply it internally, so the outer
/// pipeline needs no additional Filter. Otherwise the original condition
/// must be used.
fn filter_action_for_predicate(
	scan_predicate: &Option<Arc<dyn crate::exec::PhysicalExpr>>,
) -> FilterAction {
	if scan_predicate.is_some() {
		FilterAction::FullyConsumed
	} else {
		FilterAction::UseOriginal
	}
}

impl<'ctx> Planner<'ctx> {
	/// Resolve a parameter to its value at plan time.
	///
	/// Delegates to [`resolve_param_value`] with the planner's context and
	/// namespace/database IDs (looked up from the transaction when available).
	async fn resolve_param(&self, name: &str) -> Option<crate::val::Value> {
		let ns_db = self.ns_db_ids().await;
		resolve_param_value(name, self.ctx, ns_db, SELECT_ITERATION_PARAMS).await
	}

	/// Look up (NamespaceId, DatabaseId) from the planner's transaction.
	///
	/// Mirrors `Context::try_ns_db_ids` but uses the planner's stored ns/db
	/// strings instead of `Options` (which the planner doesn't have).
	/// Returns None when the transaction or namespace/database is unavailable.
	async fn ns_db_ids(&self) -> Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
		let (txn, ns, db) = match (&self.txn, &self.ns, &self.db) {
			(Some(txn), Some(ns), Some(db)) => (txn, ns, db),
			_ => return None,
		};
		let db_def = txn.get_db_by_name(ns, db).await.ok()??;
		Some((db_def.namespace_id, db_def.database_id))
	}

	/// Try to evaluate a source expression to a concrete `Value` at plan time.
	///
	/// Recursively resolves parameters and evaluates synchronous built-in
	/// function calls when all arguments are known. Returns `None` when any
	/// part of the expression cannot be resolved (e.g. row-scoped variables,
	/// async functions, or unknown parameters).
	async fn try_resolve_expr_value(&self, expr: &Expr) -> Option<crate::val::Value> {
		use crate::expr::function::Function;
		use crate::val::Value;

		match expr {
			Expr::Param(param) => self.resolve_param(param.as_str()).await,
			Expr::Literal(lit) => super::util::try_literal_to_value(lit),
			Expr::Table(name) => Some(Value::Table(name.clone())),
			Expr::FunctionCall(fc) => {
				let Function::Normal(ref name) = fc.receiver else {
					return None;
				};
				self.ctx.check_allowed_function(name).ok()?;
				let mut args = Vec::with_capacity(fc.arguments.len());
				for arg in &fc.arguments {
					args.push(Box::pin(self.try_resolve_expr_value(arg)).await?);
				}
				crate::fnc::synchronous(self.ctx, None, name, args).ok()
			}
			_ => None,
		}
	}

	/// Try to resolve FROM source expressions at plan time.
	///
	/// Walks each source in the `what` vector and attempts to evaluate it
	/// to a concrete value. When a source resolves to `Value::Table`, it is
	/// rewritten to `Expr::Table` so that downstream planning (index
	/// resolution, limit pushdown, sort elimination) works identically to
	/// literal table names.
	async fn resolve_source_exprs(&self, what: &mut [Expr]) {
		for expr in what.iter_mut() {
			match expr {
				Expr::Table(_) | Expr::Literal(_) | Expr::Select(_) => continue,
				_ => {}
			}
			if let Some(value) = self.try_resolve_expr_value(expr).await {
				match value {
					crate::val::Value::Table(t) => *expr = Expr::Table(t),
					crate::val::Value::RecordId(rid) => {
						*expr = crate::val::Value::RecordId(rid).into_literal();
					}
					_ => *expr = value.into_literal(),
				}
			}
		}
	}

	/// Plan the SELECT pipeline after the source is determined.
	pub(crate) async fn plan_pipeline(
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
			tempfiles,
			filter_pushed: _,
			precompiled_predicate,
		} = config;

		let filtered = if let Some(predicate) = precompiled_predicate {
			// Use the pre-compiled predicate to avoid duplicate compilation
			Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
		} else if let Some(cond) = cond {
			let predicate = self.physical_expr(cond.0).await?;
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

			let (aggregates, group_by_exprs) = self.plan_aggregation(&fields, &group_by).await?;

			(
				Arc::new(Aggregate::new(split_op, group_by, group_by_exprs, aggregates))
					as Arc<dyn ExecOperator>,
				true,
			)
		} else {
			(split_op, false)
		};

		// Shared expression registry for deduplication across sort and projection.
		// Expressions computed for ORDER BY are reused by the projection step.
		// Reserve the SELECT field names so that synthetic `_eN` names never
		// collide with fields the user explicitly selected.
		let mut registry = ExpressionRegistry::with_reserved_names(collect_field_names(&fields));

		let (sorted, sort_only_omits) = if let Some(order) = order {
			// Sort elimination: if the input is already sorted in the required
			// order, skip creating a Sort operator entirely.
			if self.can_eliminate_sort(&grouped, &order) {
				(grouped, vec![])
			} else if skip_projections {
				// GROUP BY queries use the legacy sort path because the
				// consolidated approach's Compute operator would try to
				// evaluate aggregate expressions (e.g., math::sum) on
				// individual rows rather than grouped arrays.
				(self.plan_sort(grouped, order, &start, &limit, tempfiles).await?, vec![])
			} else {
				self.plan_sort_consolidated(
					grouped,
					order,
					&fields,
					&start,
					&limit,
					tempfiles,
					&mut registry,
				)
				.await?
			}
		} else {
			(grouped, vec![])
		};

		let limited = if limit.is_some() || start.is_some() {
			let limit_expr = match limit {
				Some(l) => Some(self.physical_expr(l.0).await?),
				None => None,
			};
			let offset_expr = match start {
				Some(s) => Some(self.physical_expr(s.0).await?),
				None => None,
			};
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
				let omit_fields = self.plan_omit(all_omit).await?;
				Arc::new(Project::new(limited, vec![], omit_fields, true)) as Arc<dyn ExecOperator>
			} else {
				limited
			}
		} else {
			self.plan_projections_fast(fields, all_omit, limited, &mut registry).await?
		};

		Ok(projected)
	}

	/// Check whether the input operator's output ordering already satisfies
	/// the requested ORDER BY, allowing the Sort operator to be eliminated.
	fn can_eliminate_sort(
		&self,
		input: &Arc<dyn ExecOperator>,
		order: &crate::expr::order::Ordering,
	) -> bool {
		use crate::exec::operators::SortDirection;
		use crate::exec::ordering::SortProperty;
		use crate::expr::order::Ordering;

		let Ordering::Order(order_list) = order else {
			return false; // Random ordering can't be eliminated
		};

		// Convert the ORDER BY clause to SortProperty requirements,
		// including collate/numeric modifiers so that the satisfies
		// check rejects mismatches against raw key ordering.
		let required: Vec<SortProperty> = order_list
			.iter()
			.filter_map(|field| {
				// Only simple field paths can be matched
				crate::exec::field_path::FieldPath::try_from(&field.value).ok().map(|path| {
					let direction = if field.direction {
						SortDirection::Asc
					} else {
						SortDirection::Desc
					};
					SortProperty {
						path,
						direction,
						collate: field.collate,
						numeric: field.numeric,
					}
				})
			})
			.collect();

		// If we couldn't convert all fields, can't eliminate
		if required.len() != order_list.len() {
			return false;
		}

		// Strip leading ORDER BY fields that reference constant
		// (equality-pinned) columns in the input.  These columns have a
		// single value, so any direction trivially satisfies the ordering.
		let constant_fields = input.constant_output_fields();
		let required: Vec<SortProperty> =
			required.into_iter().skip_while(|prop| constant_fields.contains(&prop.path)).collect();

		// If all required fields were constant, the ordering is trivially satisfied.
		if required.is_empty() {
			return true;
		}

		// Check if the input's output ordering satisfies the requirement
		input.output_ordering().satisfies(&required)
	}

	/// Plan projections (SELECT fields or SELECT VALUE).
	pub(crate) async fn plan_projections(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let expr = self.physical_expr(selector.expr).await?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					let omit_fields = if !omit.is_empty() {
						self.plan_omit(omit).await?
					} else {
						vec![]
					};
					return Ok(Arc::new(Project::new(input, vec![], omit_fields, true))
						as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				let mut field_selections = Vec::with_capacity(field_list.len());

				for field in field_list {
					if let Field::Single(selector) = field {
						let field_selection = if let Some(alias) = &selector.alias {
							let output_name = idiom_to_field_name(alias);
							let expr = self.physical_expr(selector.expr).await?;
							FieldSelection::with_alias(output_name, expr)
						} else {
							let output_name_or_path = match &selector.expr {
								Expr::Idiom(idiom) => Ok(idiom_to_field_path(idiom)),
								_ => Err(derive_field_name(&selector.expr)),
							};
							let expr = self.physical_expr(selector.expr).await?;
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
					self.plan_omit(omit).await?
				} else {
					vec![]
				};

				Ok(Arc::new(Project::new(input, field_selections, omit_fields, has_wildcard))
					as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan projections with the fast path: use SelectProject for simple field
	/// selection and Compute for complex expressions, avoiding the full
	/// IdiomExpr/PhysicalExpr/async evaluation chain in Project.
	///
	/// Falls back to `plan_projections` when projection functions or nested
	/// output paths are present, as those require the full Project operator.
	pub(crate) async fn plan_projections_fast(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
		registry: &mut ExpressionRegistry,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let expr = self.physical_expr(selector.expr).await?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(ref field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					if Self::has_complex_omit(&omit) {
						return self.plan_projections(fields, omit, input).await;
					}

					let mut projections = vec![Projection::All];
					for expr in &omit {
						if let Expr::Idiom(idiom) = expr {
							projections.push(Projection::Omit(idiom_to_field_name(idiom)));
						}
					}
					return Ok(Arc::new(SelectProject::new(
						input,
						projections,
						Arc::new(OperatorMetrics::new()),
					)) as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				// Bail out early if OMIT contains complex expressions (nested
				// paths, function calls, params) — the fast SelectProject path
				// can't handle them, and checking now avoids compiling physical
				// expressions we'd throw away.
				if Self::has_complex_omit(&omit) {
					return self.plan_projections(fields, omit, input).await;
				}

				// Classify each field. If any field requires the full Project
				// operator (projection functions, nested output paths), fall back.
				let mut projections = Vec::with_capacity(field_list.len());
				let mut needs_fallback = false;
				// Track source field names read by simple Include/Rename projections.
				// Used post-loop to detect shadowing by Compute internal names.
				let mut simple_source_fields: HashSet<String> = HashSet::new();

				if has_wildcard {
					projections.push(Projection::All);
				}

				for field in field_list {
					match field {
						Field::All => {} // Already handled via has_wildcard
						Field::Single(selector) => {
							let physical = self.physical_expr(selector.expr.clone()).await?;

							// Projection functions produce dynamic field bindings
							// and require the full Project operator.
							if physical.is_projection_function() {
								needs_fallback = true;
								break;
							}

							if let Some(alias) = &selector.alias {
								let output_name = idiom_to_field_name(alias);

								// Dotted aliases (e.g. `AS status.events`) require
								// nested object construction, which SelectProject
								// doesn't support. Fall back to the full Project
								// operator which handles this via
								// parse_output_path + set_field_on_object.
								if output_name.contains('.')
									&& !output_name.contains(['[', '(', ' '])
								{
									needs_fallback = true;
									break;
								}

								if let Some(field_name) = physical.try_simple_field() {
									// Simple aliased field: rename
									simple_source_fields.insert(field_name.to_string());
									if field_name == output_name {
										projections.push(Projection::Include(output_name));
									} else {
										projections.push(Projection::Rename {
											from: field_name.to_string(),
											to: output_name,
										});
									}
								} else {
									// Complex expression with alias: compute it
									Self::register_and_push_projection(
										&mut projections,
										registry,
										selector.expr.to_sql(),
										physical,
										output_name,
									);
								}
							} else {
								// No alias
								if let Some(field_name) = physical.try_simple_field() {
									// Simple field: include directly
									simple_source_fields.insert(field_name.to_string());
									projections.push(Projection::Include(field_name.to_string()));
								} else if let Expr::Idiom(idiom) = &selector.expr {
									let path = idiom_to_field_path(idiom);
									if path.len() > 1 {
										// Multi-part idiom → nested output path.
										// SelectProject doesn't support nested paths,
										// so fall back to Project.
										needs_fallback = true;
										break;
									}
									// Single-part idiom that didn't match
									// try_simple_field (e.g. graph traversal).
									// Register in Compute.
									Self::register_and_push_projection(
										&mut projections,
										registry,
										selector.expr.to_sql(),
										physical,
										idiom_to_field_name(idiom),
									);
								} else {
									// Non-idiom expression without alias (e.g. function call)
									Self::register_and_push_projection(
										&mut projections,
										registry,
										selector.expr.to_sql(),
										physical,
										derive_field_name(&selector.expr),
									);
								}
							}
						}
					}
				}

				// A Compute expression whose internal name matches a simple
				// projection's source field will overwrite that field in the
				// per-row object, causing the simple projection to read the
				// computed value instead of the original. Fall back to the
				// full Project operator which evaluates every field against
				// the original row values. Check both Sort and Project
				// compute points since Sort Compute also feeds into
				// SelectProject.
				if !needs_fallback && !simple_source_fields.is_empty() {
					let has_shadow =
						[ComputePoint::Sort, ComputePoint::Project].iter().any(|point| {
							registry
								.get_expressions_for_point(*point)
								.iter()
								.any(|(name, _)| simple_source_fields.contains(name))
						});
					if has_shadow {
						needs_fallback = true;
					}
				}

				if needs_fallback {
					return self.plan_projections(fields, omit, input).await;
				}

				// Add OMIT projections (all simple / top-level)
				for expr in &omit {
					if let Expr::Idiom(idiom) = expr {
						projections.push(Projection::Omit(idiom_to_field_name(idiom)));
					}
				}

				// Create Compute operator if any complex expressions were registered
				let computed = if registry.has_expressions_for_point(ComputePoint::Project) {
					let compute_fields = registry.get_expressions_for_point(ComputePoint::Project);
					Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
				} else {
					input
				};

				Ok(Arc::new(SelectProject::new(
					computed,
					projections,
					Arc::new(OperatorMetrics::new()),
				)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Register a complex expression in the `ExpressionRegistry` and push the
	/// corresponding `Include` or `Rename` projection.
	///
	/// Deduplicates the identical pattern that appeared three times in
	/// `plan_projections_fast` (aliased expr, unaliased idiom, unaliased
	/// non-idiom).
	fn register_and_push_projection(
		projections: &mut Vec<Projection>,
		registry: &mut ExpressionRegistry,
		expr_key: String,
		physical: Arc<dyn crate::exec::PhysicalExpr>,
		output_name: String,
	) {
		let internal_name = registry.register_physical(
			expr_key,
			physical,
			ComputePoint::Project,
			Some(output_name.clone()),
		);
		if internal_name == output_name {
			projections.push(Projection::Include(output_name));
		} else {
			projections.push(Projection::Rename {
				from: internal_name,
				to: output_name,
			});
		}
	}

	/// Check whether any OMIT expression requires the full `Project` operator.
	///
	/// `SelectProject` only handles flat `Projection::Omit` with simple idioms.
	/// Nested paths like `opts.age`, function calls like `type::field(...)`, and
	/// parameters all require the full `Project` operator via `plan_omit`.
	fn has_complex_omit(omit: &[Expr]) -> bool {
		omit.iter().any(|e| {
			if let Expr::Idiom(idiom) = e {
				idiom.len() > 1
			} else {
				true
			}
		})
	}

	/// Plan OMIT fields — convert expressions to idioms.
	pub(crate) async fn plan_omit(
		&self,
		omit: Vec<Expr>,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		let mut fields = Vec::with_capacity(omit.len());
		for expr in omit {
			let mut idioms = self.resolve_field_idioms(expr).await?;
			fields.append(&mut idioms);
		}
		Ok(fields)
	}

	/// Plan FETCH clause.
	pub(crate) async fn plan_fetch(
		&self,
		fetch: Option<crate::expr::fetch::Fetchs>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let Some(fetchs) = fetch else {
			return Ok(input);
		};

		let mut fields = Vec::with_capacity(fetchs.len());
		for fetch_item in fetchs {
			let mut idioms = self.resolve_field_idioms(fetch_item.0).await?;
			fields.append(&mut idioms);
		}

		Ok(Arc::new(Fetch {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}) as Arc<dyn ExecOperator>)
	}

	/// Plan ORDER BY.
	pub(crate) async fn plan_sort(
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
				let order_by = self.convert_order_list(order_list).await?;

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
	///
	/// Uses a shared `ExpressionRegistry` so that expressions computed for sort
	/// can be reused by downstream projection (avoiding duplicate computation).
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_sort_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		order: crate::expr::order::Ordering,
		fields: &Fields,
		start: &Option<crate::expr::start::Start>,
		limit: &Option<crate::expr::limit::Limit>,
		#[allow(unused)] tempfiles: bool,
		registry: &mut ExpressionRegistry,
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
				let mut sort_keys = Vec::with_capacity(order_list.len());
				let mut sort_only_fields: Vec<String> = Vec::new();

				for order_field in order_list.iter() {
					let idiom = &order_field.value;

					let field_path = if let Some((resolved_expr, alias)) =
						resolve_order_by_alias(idiom, fields)
					{
						match &resolved_expr {
							Expr::Idiom(inner_idiom) => {
								// Multi-part idioms or lookups require the
								// Compute operator for context-aware evaluation
								// (e.g., record-link traversal like
								// `in.creationDate` on edge tables).
								// Single-part idioms can use FieldPath directly.
								if inner_idiom.len() > 1
									|| inner_idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)))
								{
									let name = registry
										.register(
											&resolved_expr,
											ComputePoint::Sort,
											Some(alias.clone()),
											self.ctx,
										)
										.await?;
									FieldPath::field(name)
								} else {
									match FieldPath::try_from(inner_idiom) {
										Ok(path) => path,
										Err(_) => {
											let name = registry
												.register(
													&resolved_expr,
													ComputePoint::Sort,
													Some(alias.clone()),
													self.ctx,
												)
												.await?;
											FieldPath::field(name)
										}
									}
								}
							}
							_ => {
								let name = registry
									.register(
										&resolved_expr,
										ComputePoint::Sort,
										Some(alias.clone()),
										self.ctx,
									)
									.await?;
								FieldPath::field(name)
							}
						}
					} else {
						match FieldPath::try_from(idiom) {
							Ok(path) => path,
							Err(_) => {
								let expr = Expr::Idiom(idiom.clone());
								let name = registry
									.register(&expr, ComputePoint::Sort, None, self.ctx)
									.await?;
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

	// ========================================================================
	// Field Resolution Helpers
	// ========================================================================

	/// Resolve a field expression to one or more idioms.
	pub(crate) async fn resolve_field_idioms(
		&self,
		expr: Expr,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		use crate::expr::Function;

		match expr {
			Expr::Idiom(idiom) => Ok(vec![idiom]),
			Expr::Param(ref param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
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
						match self.resolve_expr_to_string(arg).await {
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
								let strings = self
									.resolve_expr_to_string_array(arg)
									.await
									.map_err(|_| Error::Query {
										message: format!(
											"Projection function '{}' argument could not \
												 be resolved to a field path",
											name
										),
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

	async fn resolve_expr_to_string(&self, expr: &Expr) -> Result<String, Error> {
		match expr {
			Expr::Literal(Literal::String(s)) => Ok(s.clone()),
			Expr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
				value.coerce_to::<String>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	async fn resolve_expr_to_string_array(&self, expr: &Expr) -> Result<Vec<String>, Error> {
		match expr {
			Expr::Literal(Literal::Array(items)) => {
				let mut result = Vec::with_capacity(items.len());
				for item in items {
					result.push(self.resolve_expr_to_string(item).await?);
				}
				Ok(result)
			}
			Expr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
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

	// ========================================================================
	// SELECT planning with plan-time index resolution
	// ========================================================================

	/// Plan a SELECT statement.
	///
	/// Performs plan-time index resolution when a transaction is available,
	/// enabling sort elimination and concrete scan operators.
	pub(crate) async fn plan_select_statement(
		&self,
		mut select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let explain = select.explain.take();
		let plan = Box::pin(self.plan_select_core(select)).await?;
		match explain {
			Some(crate::expr::explain::Explain(full)) => {
				if full {
					Ok(Arc::new(AnalyzePlan {
						plan,
						format: crate::expr::ExplainFormat::Json,
						redact_volatile_explain_attrs: self.ctx.redact_volatile_explain_attrs(),
					}))
				} else {
					Ok(Arc::new(ExplainPlan {
						plan,
						format: crate::expr::ExplainFormat::Json,
					}))
				}
			}
			None => Ok(plan),
		}
	}

	/// Core SELECT planning logic.
	///
	/// Resolves sources (with plan-time index analysis when a transaction is
	/// available), then builds the pipeline: filter → split → aggregate →
	/// sort (with elimination) → limit → project → fetch → timeout.
	async fn plan_select_core(
		&self,
		select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let crate::expr::statements::SelectStatement {
			fields,
			omit,
			only,
			mut what,
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
			explain: _,
			tempfiles,
		} = select;

		let version = extract_version(version, self).await?;

		// COUNT fast-path
		if is_count_all_eligible(&fields, &group, &cond, &split, &order, &fetch, &omit, &what) {
			use crate::exec::operators::CountScan;
			let table_expr = self
				.physical_expr(what.into_iter().next().expect("what verified non-empty"))
				.await?;
			let field_names = extract_count_field_names(&fields);
			let count_scan: Arc<dyn ExecOperator> =
				Arc::new(CountScan::new(table_expr, version, field_names));
			let timed = match timeout {
				Expr::Literal(Literal::None) => count_scan,
				te => {
					let tp = self.physical_expr(te).await?;
					Arc::new(Timeout::new(count_scan, Some(tp))) as Arc<dyn ExecOperator>
				}
			};
			return if only {
				Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
			} else {
				Ok(timed)
			};
		}

		// Indexed COUNT fast-path (COUNT with WHERE + matching COUNT index)
		// Skip when WITH NOINDEX is specified — the user explicitly forbids
		// index-assisted execution.
		if is_indexed_count_eligible(&fields, &group, &cond, &split, &order, &fetch, &omit, &what)
			&& !matches!(with, Some(crate::expr::with::With::NoIndex))
		{
			// Try COUNT index first, then B-tree index for key-only counting.
			let has_count_idx = self.has_matching_count_index(&what, &cond).await;
			let btree_access = if !has_count_idx {
				self.resolve_count_btree_access(&what, &cond, with.as_ref()).await
			} else {
				None
			};

			if has_count_idx || btree_access.is_some() {
				use crate::exec::operators::scan::index_count::IndexCountScan;
				let table_expr = self
					.physical_expr(what.first().cloned().expect("what verified non-empty"))
					.await?;
				let condition = cond.clone().expect("is_indexed_count_eligible requires cond");
				let predicate = self.physical_expr(condition.0.clone()).await?;
				let field_names = extract_count_field_names(&fields);
				let index_count_scan: Arc<dyn ExecOperator> = Arc::new(
					IndexCountScan::new(table_expr, predicate, condition, version, field_names)
						.with_btree_access(btree_access),
				);
				let timed = match timeout {
					Expr::Literal(Literal::None) => index_count_scan,
					te => {
						let tp = self.physical_expr(te).await?;
						Arc::new(Timeout::new(index_count_scan, Some(tp))) as Arc<dyn ExecOperator>
					}
				};
				return if only {
					Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
				} else {
					Ok(timed)
				};
			}
		}

		// Fast path: SELECT [*|fields] FROM <literal RecordId>
		if what.len() == 1
			&& matches!(&what[0], Expr::Literal(Literal::RecordId(_)))
			&& cond.is_none()
			&& order.is_none()
			&& group.is_none()
			&& split.is_none()
			&& fetch.is_none()
			&& with.is_none()
		{
			let needed_fields = Self::extract_needed_fields(
				&fields,
				&omit,
				cond.as_ref(),
				order.as_ref(),
				group.as_ref(),
				split.as_ref(),
			);

			// Extract table name from the literal RecordId for plan-time resolution
			let table_name_for_resolve = match &what[0] {
				Expr::Literal(Literal::RecordId(rid_lit)) => Some(rid_lit.table.clone()),
				_ => None,
			};
			let rid_expr = match what.into_iter().next() {
				Some(e @ Expr::Literal(Literal::RecordId(_))) => self.physical_expr(e).await?,
				_ => unreachable!("verified above"),
			};
			let mut scan = RecordIdScan::new(rid_expr, version, needed_fields, None);
			// Resolve table context at plan time
			if let Some(ref tb) = table_name_for_resolve
				&& let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db)
				&& let Some(tc) = Self::try_resolve_table_ctx(txn, self.ctx, ns, db, tb).await
			{
				scan = scan.with_resolved(tc);
			}
			let scan: Arc<dyn ExecOperator> = Arc::new(scan);
			let limited = if limit.is_some() || start.is_some() {
				let limit_expr = match limit {
					Some(l) => Some(self.physical_expr(l.0).await?),
					None => None,
				};
				let start_expr = match start {
					Some(s) => Some(self.physical_expr(s.0).await?),
					None => None,
				};
				Arc::new(Limit::new(scan, limit_expr, start_expr)) as Arc<dyn ExecOperator>
			} else {
				scan
			};
			let projected = self.plan_projections(fields, omit, limited).await?;
			let timed = match timeout {
				Expr::Literal(Literal::None) => projected,
				te => {
					let tp = self.physical_expr(te).await?;
					Arc::new(Timeout::new(projected, Some(tp))) as Arc<dyn ExecOperator>
				}
			};
			return if only {
				Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
			} else {
				Ok(timed)
			};
		}

		// Capture literal Expr::Table nodes BEFORE resolve_source_exprs so
		// that MATCHES context preferentially binds to tables written in the
		// query rather than param-resolved ones (e.g. FROM $t, article).
		let literal_primary_table = what.iter().find_map(|e| match e {
			Expr::Table(t) => Some(t.clone()),
			_ => None,
		});

		// Pre-resolve FROM sources so that params and function calls like
		// type::table($name) are rewritten to concrete Expr::Table nodes
		// before any downstream checks.
		self.resolve_source_exprs(&mut what).await;

		let is_value_source = all_value_sources(&what);
		let primary_table = literal_primary_table.or_else(|| {
			what.iter().find_map(|e| match e {
				Expr::Table(t) => Some(t.clone()),
				_ => None,
			})
		});
		let has_knn_early = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));

		let planning_ctx: std::borrow::Cow<'_, crate::ctx::FrozenContext> =
			if let Some(ref c) = cond {
				let mc = extract_matches_context(c, Some(self.ctx));
				let hm = !mc.is_empty();
				if hm || has_knn_early {
					let mut child = crate::ctx::Context::new(self.ctx);
					if hm {
						let mut mc = mc;
						if let Some(ref t) = primary_table {
							mc.set_table(t.clone());
						}
						child.set_matches_context(mc);
					}
					if has_knn_early {
						child.set_knn_context(std::sync::Arc::new(
							crate::exec::function::KnnContext::new(),
						));
					}
					std::borrow::Cow::Owned(child.freeze())
				} else {
					std::borrow::Cow::Borrowed(self.ctx)
				}
			} else {
				std::borrow::Cow::Borrowed(self.ctx)
			};

		// Propagate txn to the inner planner
		let pp = if let Some(ref txn) = self.txn {
			Planner::with_txn(&planning_ctx, txn.clone(), self.ns.clone(), self.db.clone())
		} else {
			Planner::new(&planning_ctx)
		};

		let needed_fields = Self::extract_needed_fields(
			&fields,
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group.as_ref(),
			split.as_ref(),
		);
		// After source resolution, params that resolved to tables are now
		// Expr::Table, so we only need to check concrete types here.
		let source_is_single_scan = what.len() == 1
			&& matches!(what[0], Expr::Table(_) | Expr::FunctionCall(_) | Expr::Postfix { .. });

		// Resolve bind-parameter references so that downstream index analysis
		// and KNN extraction see Expr::Literal instead of Expr::Param.
		// This covers LET bindings, client bind params, and DEFINE PARAM.
		let ns_db = self.ns_db_ids().await;
		let cond = match cond.as_ref() {
			Some(c) => {
				Some(resolve_condition_params(c, self.ctx, ns_db, SELECT_ITERATION_PARAMS).await)
			}
			None => None,
		};

		// Fold constant expressions to literals so that index analysis can
		// create proper range access patterns. Handles:
		// - time::now() - 365d → datetime literal
		// - math::floor(20.5) → 20 (any pure function with literal args)
		// - type::int('42') → 42
		let cond = match cond {
			Some(mut c) => {
				fold_condition_expressions(&mut c, self.function_registry());
				Some(c)
			}
			None => None,
		};

		// KNN handling
		let has_knn = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));
		let brute_force_knn = if has_knn {
			cond.as_ref().and_then(extract_bruteforce_knn)
		} else {
			None
		};

		let (cond_for_index, cond_for_filter) = if has_knn {
			let stripped = cond.as_ref().and_then(strip_knn_from_condition);
			if stripped.as_ref().is_some_and(|c| has_knn_operator(&c.0)) {
				return Err(Error::Query {
					message: "KNN operators nested in OR/NOT expressions or mixed with \
					 unsupported KNN variants are not supported"
						.to_string(),
				});
			}
			if brute_force_knn.is_some() {
				(stripped.clone(), stripped)
			} else if cond.as_ref().is_some_and(|c| has_knn_k_operator(&c.0)) {
				return Err(Error::PlannerUnimplemented(
					"Brute-force KNN with parameter-based vectors is not supported \
					 in the streaming executor"
						.to_string(),
				));
			} else {
				(cond, stripped)
			}
		} else {
			let c = cond;
			(c.clone(), c)
		};

		let scan_predicate = if source_is_single_scan {
			match cond_for_filter.as_ref() {
				Some(c) => Some(pp.physical_expr(c.0.clone()).await?),
				None => None,
			}
		} else {
			None
		};

		// Check prerequisites for limit pushdown that don't depend on the
		// access path. The per-access-path decision (whether the scan
		// ordering covers the ORDER BY) is made inside plan_source().
		let can_push_limit = source_is_single_scan
			&& brute_force_knn.is_none()
			&& !has_top_level_or(cond_for_filter.as_ref())
			&& limit.is_some()
			&& split.is_none()
			&& group.is_none();

		let can_soft_push_limit = !can_push_limit
			&& source_is_single_scan
			&& brute_force_knn.is_none()
			&& limit.is_some()
			&& split.is_some()
			&& group.is_none();

		let (scan_limit, scan_start) = if can_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.0.clone()).await?),
					None => None,
				},
				match start.as_ref() {
					Some(s) => Some(pp.physical_expr(s.0.clone()).await?),
					None => None,
				},
			)
		} else if can_soft_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.0.clone()).await?),
					None => None,
				},
				None,
			)
		} else {
			(None, None)
		};

		// Keep a clone of the scan predicate so we can reuse it as a
		// precompiled predicate for the pipeline Filter when the source
		// does not consume it (FilterAction::UseOriginal). This avoids
		// compiling the same AST expression into a PhysicalExpr twice.
		let scan_predicate_for_reuse = scan_predicate.clone();

		// Source resolution with plan-time index analysis.
		// The result tracks whether the predicate and limit/start were
		// consumed by the source operator, so we can avoid duplicating
		// them in the outer pipeline.
		let mut planned = pp
			.plan_sources(
				what,
				version,
				cond_for_index.as_ref(),
				order.as_ref(),
				with.as_ref(),
				needed_fields,
				scan_predicate,
				scan_limit,
				scan_start,
			)
			.await?;

		if can_soft_push_limit {
			planned.limit_pushed = false;
		}

		// Resolve the pipeline condition from the filter action.
		// - FullyConsumed: the source handles the entire predicate, no Filter.
		// - Residual: only the residual part needs a Filter.
		// - UseOriginal: the source did not analyze the predicate, use as-is.
		//
		// When UseOriginal and we already compiled a scan_predicate from the
		// same expression, reuse it as precompiled_predicate to avoid paying
		// the PhysicalExpr compilation cost a second time.
		let (pipeline_cond, precompiled_predicate) = match planned.filter_action {
			FilterAction::FullyConsumed => (None, None),
			FilterAction::Residual(residual) => (Some(residual), None),
			FilterAction::UseOriginal => {
				if scan_predicate_for_reuse.is_some() {
					// Reuse the already-compiled predicate
					(None, scan_predicate_for_reuse)
				} else {
					(cond_for_filter, None)
				}
			}
		};

		// KNN wrapping
		let had_bruteforce_knn = brute_force_knn.is_some();
		let source = if let Some(kp) = brute_force_knn {
			// Residual predicates (non-KNN WHERE conditions) must be applied
			// BEFORE ranking by distance. Otherwise rows that don't satisfy
			// the WHERE clause can consume top-K slots and push out valid rows.
			//
			// When the predicate was fully consumed by the source operator, it
			// is already applied there. Otherwise we add an explicit Filter.
			let input = if let Some(c) = &pipeline_cond {
				let pred = pp.physical_expr(c.0.clone()).await?;
				Arc::new(Filter::new(planned.operator, pred)) as Arc<dyn ExecOperator>
			} else {
				planned.operator
			};
			let knn_ctx = planning_ctx.get_knn_context().cloned();
			Arc::new(
				KnnTopK::new(input, kp.field, kp.vector, kp.k as usize, kp.distance)
					.with_knn_context(knn_ctx),
			) as Arc<dyn ExecOperator>
		} else {
			planned.operator
		};

		// Build pipeline.
		// When the predicate was consumed (fully or partially) by the source,
		// use the computed pipeline_cond (which may be None or a residual).
		// When limit/start were pushed, omit them to avoid double application.
		// ORDER BY is always passed through — sort elimination via
		// `can_eliminate_sort()` in `plan_pipeline()` handles it independently.
		let config = SelectPipelineConfig {
			cond: if had_bruteforce_knn {
				None
			} else {
				pipeline_cond
			},
			split,
			group,
			order,
			limit: if planned.limit_pushed {
				None
			} else {
				limit
			},
			start: if planned.limit_pushed {
				None
			} else {
				start
			},
			omit,
			tempfiles,
			filter_pushed: false,
			precompiled_predicate: if had_bruteforce_knn {
				None
			} else {
				precompiled_predicate
			},
		};

		let projected = pp.plan_pipeline(source, Some(fields), config).await?;
		let fetched = pp.plan_fetch(fetch, projected).await?;
		let timed = match timeout {
			Expr::Literal(Literal::None) => fetched,
			te => {
				let tp = pp.physical_expr(te).await?;
				Arc::new(Timeout::new(fetched, Some(tp))) as Arc<dyn ExecOperator>
			}
		};
		if only {
			Ok(Arc::new(UnwrapExactlyOne::new(timed, !is_value_source)))
		} else {
			Ok(timed)
		}
	}

	/// Plan FROM sources with plan-time index resolution.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_sources(
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
	) -> Result<PlannedSource, Error> {
		if what.is_empty() {
			return Err(Error::Query {
				message: "SELECT requires at least one source".to_string(),
			});
		}
		let mut plans = Vec::with_capacity(what.len());
		for expr in what {
			let p = self
				.plan_source(
					expr,
					version.clone(),
					cond,
					order,
					with,
					needed_fields.clone(),
					scan_predicate.clone(),
					scan_limit.clone(),
					scan_start.clone(),
				)
				.await?;
			plans.push(p);
		}
		if plans.len() == 1 {
			Ok(plans.pop().expect("verified non-empty"))
		} else {
			// Multiple sources are combined via Union; pushdowns are not
			// applicable because source_is_single_scan is false when
			// what.len() > 1, so scan_predicate/scan_limit are always None.
			let operators = plans.into_iter().map(|p| p.operator).collect();
			Ok(PlannedSource {
				operator: Arc::new(Union::new(operators)),
				filter_action: FilterAction::UseOriginal,
				limit_pushed: false,
			})
		}
	}

	/// Plan a single FROM source.
	///
	/// When the planner has a transaction and the source is a table,
	/// resolves the access path at plan time and creates the concrete
	/// operator (IndexScan, FullTextScan, KnnScan) directly. This
	/// avoids redundant index analysis at execution time and enables
	/// sort elimination via `output_ordering()`.
	///
	/// Without a transaction, creates a generic `Scan` that resolves
	/// its access path at execution time.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_source(
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
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::{FullTextScan, IndexScan, KnnScan};

		// Optimisation: WHERE id = <RecordId> -> point lookup.
		// Detects `id = <RecordId literal>` in the top-level AND chain and
		// converts the table scan into a RecordIdScan, avoiding index
		// analysis and full-table iteration entirely.
		//
		// Skipped when the condition contains a KNN operator, because the
		// KNN access path (KnnScan / HNSW) must be resolved by
		// resolve_access_path() to populate KnnContext correctly.
		if let Expr::Table(ref table_name) = expr
			&& !cond.is_some_and(|c| has_knn_operator(&c.0))
			&& let Some(rid_expr) = cond.and_then(|c| extract_record_id_point_lookup(c, table_name))
		{
			let filter_action = filter_action_for_predicate(&scan_predicate);
			let record_id_expr = self.physical_expr(rid_expr).await?;
			// Resolve table context at plan time for the point lookup
			let mut scan =
				RecordIdScan::new(record_id_expr, version, needed_fields, scan_predicate);
			if let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db)
				&& let Some(tc) =
					Self::try_resolve_table_ctx(txn, self.ctx, ns, db, table_name).await
			{
				scan = scan.with_resolved(tc);
			}
			return Ok(PlannedSource {
				operator: Arc::new(scan) as Arc<dyn ExecOperator>,
				filter_action,
				limit_pushed: false,
			});
		}

		// When we have a txn and the source is a table, resolve the
		// access path and table context at plan time.
		if let Expr::Table(ref table_name) = expr
			&& let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db)
		{
			// Resolve table context (table def + field state) at plan time.
			// This eliminates runtime KV lookups in the operator's execute().
			let table_ctx: Option<ResolvedTableContext> =
				Self::try_resolve_table_ctx(txn, self.ctx, ns, db, table_name).await;

			let resolved =
				self.resolve_access_path(txn, ns, db, table_name, cond, order, with).await;
			if let Ok(Some((access_path, direction))) = resolved {
				let table = table_name.clone();
				let knn_ctx = self.ctx.get_knn_context().cloned();
				match access_path {
					AccessPath::BTreeScan {
						index_ref,
						access,
						direction,
					} => {
						// Strip index-covered conditions from the WHERE
						// clause. If all conditions are consumed, no Filter
						// operator will be created in the pipeline.
						let filter_action = if let Some(c) = cond {
							match strip_index_conditions(c, &access, &index_ref.cols) {
								None => FilterAction::FullyConsumed,
								Some(residual) => FilterAction::Residual(residual),
							}
						} else {
							FilterAction::FullyConsumed
						};
						// Push limit to IndexScan when the index ordering
						// covers the ORDER BY (or there is no ORDER BY).
						// IMPORTANT: Only push limit when the filter is fully
						// consumed by the index. When there's a residual
						// filter above the scan, pushing LIMIT causes the
						// scan to return fewer rows than needed because the
						// post-filter may remove some of them.
						let push = scan_limit.is_some()
							&& matches!(filter_action, FilterAction::FullyConsumed)
							&& match order {
								None => true,
								Some(ord) => {
									index_covers_ordering(&index_ref, &access, direction, ord)
								}
							};
						let (idx_limit, idx_start, limit_pushed) = if push {
							(scan_limit.clone(), scan_start.clone(), true)
						} else {
							(None, None, false)
						};
						// When the limit wasn't pushed (residual filter) but
						// the index covers ORDER BY, pass the user's LIMIT
						// as a batch-sizing hint.  This keeps each batch
						// small (~LIMIT entries) so the downstream Limit
						// operator can stop the stream quickly instead of
						// waiting for a full 1000-entry batch.
						let batch_ceiling = if !push
							&& scan_limit.is_some()
							&& matches!(filter_action, FilterAction::Residual(_))
							&& match order {
								None => true,
								Some(ord) => {
									index_covers_ordering(&index_ref, &access, direction, ord)
								}
							} {
							scan_limit.clone()
						} else {
							None
						};
						let mut scan = IndexScan::new(
							index_ref,
							access,
							direction,
							table,
							idx_limit,
							idx_start,
							version.clone(),
						)
						.with_batch_ceiling(batch_ceiling);
						if let Some(ref tc) = table_ctx {
							scan = scan.with_resolved(tc.clone());
						}
						return Ok(PlannedSource {
							operator: Arc::new(scan) as Arc<dyn ExecOperator>,
							filter_action,
							limit_pushed,
						});
					}
					AccessPath::FullTextSearch {
						index_ref,
						query,
						operator,
					} => {
						let mut scan =
							FullTextScan::new(index_ref, query, operator, table, version.clone());
						if let Some(ref tc) = table_ctx {
							scan = scan.with_resolved(tc.clone());
						}
						let filter_action = if let Some(c) = cond {
							match strip_fts_condition(c) {
								None => FilterAction::FullyConsumed,
								Some(residual) => FilterAction::Residual(residual),
							}
						} else {
							FilterAction::FullyConsumed
						};
						return Ok(PlannedSource {
							operator: Arc::new(scan) as Arc<dyn ExecOperator>,
							filter_action,
							limit_pushed: false,
						});
					}
					AccessPath::KnnSearch {
						index_ref,
						vector,
						k,
						ef,
					} => {
						// Strip KNN operators from the condition to get the residual
						// (non-KNN predicates). These are pushed into the HNSW search
						// so that non-matching rows don't consume top-K slots.
						let residual_cond = cond.and_then(strip_knn_from_condition);
						let mut scan = KnnScan::new(
							index_ref,
							vector,
							k,
							ef,
							table,
							version.clone(),
							knn_ctx,
							residual_cond,
						);
						if let Some(ref tc) = table_ctx {
							scan = scan.with_resolved(tc.clone());
						}
						return Ok(PlannedSource {
							operator: Arc::new(scan) as Arc<dyn ExecOperator>,
							filter_action: FilterAction::UseOriginal,
							limit_pushed: false,
						});
					}
					AccessPath::TableScan => {
						let filter_action = filter_action_for_predicate(&scan_predicate);
						// TableScan can only provide ordering for `id ASC/DESC`.
						// Push limit only when ORDER BY is compatible with the
						// natural KV scan direction.
						let push = scan_limit.is_some() && order_is_scan_compatible(order);
						let (tbl_limit, tbl_start, limit_pushed) = if push {
							(scan_limit.clone(), scan_start.clone(), true)
						} else {
							(None, None, false)
						};
						let mut scan = TableScan::new(
							table,
							direction,
							version,
							scan_predicate,
							tbl_limit,
							tbl_start,
							needed_fields,
						);
						if let Some(tc) = table_ctx.clone() {
							scan = scan.with_resolved(tc);
						}
						return Ok(PlannedSource {
							operator: Arc::new(scan) as Arc<dyn ExecOperator>,
							filter_action,
							limit_pushed,
						});
					}
					AccessPath::Union(paths) => {
						// Create a UnionIndexScan with a sub-operator for
						// each OR branch. This is consistent with how
						// BTreeScan/FullTextSearch/KnnSearch create their
						// operators at plan time. The residual WHERE
						// predicate is handled by a Filter above
						// (filter_action = UseOriginal).
						//
						// When ORDER BY is on `id` and every sub-path is
						// an equality B-tree scan, enable merge-sort by
						// record ID.  Each equality scan already produces
						// records in record-ID order, so a k-way merge
						// yields globally sorted output — the Sort
						// operator can be eliminated and Limit terminates
						// the scan early.
						let merge_dir = detect_order_by_id_only(order).filter(|_| {
							paths.iter().all(|p| {
								matches!(
									p,
									AccessPath::BTreeScan {
										access: BTreeAccess::Equality(_),
										..
									}
								)
							})
						});

						// When merge mode is active and a downstream LIMIT
						// exists, pass it as a batch ceiling hint to each
						// sub-scan.  IndexScan applies a 4× multiplier
						// internally to account for filtered rows, keeping
						// batches small so the merge terminates quickly
						// instead of fetching a full 1000-entry batch.
						let merge_batch_ceiling = if merge_dir.is_some() {
							scan_limit.clone()
						} else {
							None
						};

						let mut sub_operators: Vec<Arc<dyn ExecOperator>> =
							Vec::with_capacity(paths.len());
						for path in paths {
							let sub_op: Arc<dyn ExecOperator> = match path {
								AccessPath::BTreeScan {
									index_ref,
									access,
									direction,
								} => {
									let mut scan = IndexScan::new(
										index_ref,
										access,
										direction,
										table.clone(),
										None,
										None,
										version.clone(),
									);
									if let Some(ref ceiling) = merge_batch_ceiling {
										scan = scan.with_batch_ceiling(Some(Arc::clone(ceiling)));
									}
									if let Some(ref tc) = table_ctx {
										scan = scan.with_resolved(tc.clone());
									}
									Arc::new(scan)
								}
								AccessPath::FullTextSearch {
									index_ref,
									query,
									operator,
								} => {
									let mut scan = FullTextScan::new(
										index_ref,
										query,
										operator,
										table.clone(),
										version.clone(),
									);
									if let Some(ref tc) = table_ctx {
										scan = scan.with_resolved(tc.clone());
									}
									Arc::new(scan)
								}
								AccessPath::KnnSearch {
									index_ref,
									vector,
									k,
									ef,
								} => {
									let residual_cond = cond.and_then(strip_knn_from_condition);
									let mut scan = KnnScan::new(
										index_ref,
										vector,
										k,
										ef,
										table.clone(),
										version.clone(),
										knn_ctx.clone(),
										residual_cond,
									);
									if let Some(ref tc) = table_ctx {
										scan = scan.with_resolved(tc.clone());
									}
									Arc::new(scan)
								}
								// TableScan and nested Union should not
								// appear as sub-paths; fall back safely.
								_ => Arc::new(TableScan::new(
									table.clone(),
									direction,
									None,
									None,
									None,
									None,
									None,
								)),
							};
							sub_operators.push(sub_op);
						}
						// UnionIndexScan handles field-level permissions
						// and computed-field materialization internally
						// (same pattern as TableScan). The outer
						// pipeline handles Filter, Sort, and Limit.
						let mut union_scan =
							UnionIndexScan::new(table, sub_operators, needed_fields);
						if let Some(dir) = merge_dir {
							union_scan = union_scan.with_merge_by_id(dir);
						}
						if let Some(ref tc) = table_ctx {
							union_scan = union_scan.with_resolved(tc.clone());
						}
						return Ok(PlannedSource {
							operator: Arc::new(union_scan) as Arc<dyn ExecOperator>,
							filter_action: FilterAction::UseOriginal,
							limit_pushed: false,
						});
					}
				}
			}
		}

		// Fallback: create the appropriate operator (index resolved at runtime)
		let knn_ctx = self.ctx.get_knn_context().cloned();

		match expr {
			Expr::Literal(crate::expr::literal::Literal::RecordId(rid)) => {
				let record_id_expr = self
					.physical_expr(Expr::Literal(crate::expr::literal::Literal::RecordId(rid)))
					.await?;
				Ok(PlannedSource {
					operator: Arc::new(RecordIdScan::new(
						record_id_expr,
						version,
						needed_fields,
						None,
					)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			Expr::Select(inner_select) => {
				if version.is_some() {
					return Err(Error::Query {
						message: "VERSION clause cannot be used with a subquery source. \
								  Place the VERSION clause inside the subquery instead."
							.to_string(),
					});
				}
				Ok(PlannedSource {
					operator: self.plan_select_statement(*inner_select).await?,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			// Params that could be resolved were already rewritten to
			// Expr::Table / Expr::Literal by resolve_source_exprs().
			// Any remaining Expr::Param is unresolvable at plan time.
			Expr::Param(_) => {
				let phys_expr = self.physical_expr(expr).await?;
				Ok(PlannedSource {
					operator: Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			Expr::Table(_)
			| Expr::FunctionCall(_)
			| Expr::Postfix {
				..
			} => {
				self.plan_dynamic_scan(
					expr,
					version,
					cond,
					order,
					with,
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					knn_ctx,
				)
				.await
			}
			other => {
				let phys_expr = self.physical_expr(other).await?;
				Ok(PlannedSource {
					operator: Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
		}
	}

	/// Plan a `DynamicScan` source that resolves its access path at runtime.
	///
	/// Used for `FROM type::table(...)`, `FROM $param` (when the param holds a
	/// table), and the `FROM tablename` fallback when plan-time catalog context
	/// is unavailable. Handles filter-action, limit pushdown with ORDER BY
	/// compatibility, and KNN context in a single place.
	#[allow(clippy::too_many_arguments)]
	async fn plan_dynamic_scan(
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
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Result<PlannedSource, Error> {
		let filter_action = filter_action_for_predicate(&scan_predicate);
		let push = scan_limit.is_some() && order_is_scan_compatible(order);
		let (dyn_limit, dyn_start, limit_pushed) = if push {
			(scan_limit, scan_start, true)
		} else {
			(None, None, false)
		};
		let source_expr = self.physical_expr(expr).await?;
		Ok(PlannedSource {
			operator: Arc::new(
				DynamicScan::new(
					source_expr,
					version,
					cond.cloned(),
					order.cloned(),
					with.cloned(),
					needed_fields,
					scan_predicate,
					dyn_limit,
					dyn_start,
				)
				.with_knn_context(knn_ctx),
			) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
		})
	}

	/// Try to resolve a `ResolvedTableContext` for the given table.
	///
	/// Returns `None` if namespace/database lookup fails or the table doesn't
	/// exist. Errors in field state resolution are silently ignored (the
	/// operator will fall back to runtime resolution).
	async fn try_resolve_table_ctx(
		txn: &crate::kvs::Transaction,
		ctx: &crate::ctx::FrozenContext,
		ns: &str,
		db: &str,
		table_name: &crate::val::TableName,
	) -> Option<crate::exec::operators::scan::resolved::ResolvedTableContext> {
		let ns_def = txn.get_ns_by_name(ns).await.ok()??;
		let db_def = txn.get_db_by_name(ns, db).await.ok()??;
		resolve_table_context(txn, ctx, ns, db, ns_def.namespace_id, db_def.database_id, table_name)
			.await
			.ok()?
	}

	/// Check at plan time whether a matching COUNT index exists for the query.
	///
	/// Returns `true` when:
	/// - Plan-time catalog access is available (txn, ns, db)
	/// - The source is a single table
	/// - The table has a `DEFINE INDEX ... COUNT WHERE <cond>` whose condition matches the query's
	///   WHERE clause
	async fn has_matching_count_index(&self, what: &[Expr], cond: &Option<Cond>) -> bool {
		let Some(ref txn) = self.txn else {
			return false;
		};
		let Some(ref ns_name) = self.ns else {
			return false;
		};
		let Some(ref db_name) = self.db else {
			return false;
		};
		let table_name = match what.first() {
			Some(Expr::Table(t)) => t,
			_ => return false,
		};
		let cond = match cond {
			Some(c) => c,
			None => return false,
		};

		let Ok(Some(ns_def)) = txn.get_ns_by_name(ns_name).await else {
			return false;
		};
		let Ok(Some(db_def)) = txn.get_db_by_name(ns_name, db_name).await else {
			return false;
		};
		let Ok(indexes) =
			txn.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name).await
		else {
			return false;
		};
		indexes.iter().any(|ix| {
			if let crate::catalog::Index::Count(ref idx_cond) = ix.index {
				idx_cond.as_ref() == Some(cond)
			} else {
				false
			}
		})
	}

	/// Resolve a B-tree index access path covering the WHERE condition for
	/// key-only counting.  Returns `Some((IndexRef, BTreeAccess))` when the
	/// index analysis finds a B-tree index that fully covers the predicate
	/// (no residual filter), allowing `IndexCountScan` to count index keys
	/// instead of deserializing records.
	async fn resolve_count_btree_access(
		&self,
		what: &[Expr],
		cond: &Option<Cond>,
		with: Option<&crate::expr::with::With>,
	) -> Option<(
		crate::exec::index::access_path::IndexRef,
		crate::exec::index::access_path::BTreeAccess,
	)> {
		let txn = self.txn.as_ref()?;
		let ns_name = self.ns.as_ref()?;
		let db_name = self.db.as_ref()?;
		let table_name = match what.first() {
			Some(Expr::Table(t)) => t,
			_ => return None,
		};
		let cond = cond.as_ref()?;

		let ns_def = txn.get_ns_by_name(ns_name).await.ok()??;
		let db_def = txn.get_db_by_name(ns_name, db_name).await.ok()??;
		let indexes =
			txn.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name).await.ok()?;

		if indexes.is_empty() {
			return None;
		}

		let analyzer = IndexAnalyzer::new(indexes, with);
		let candidates = analyzer.analyze(Some(cond), None);

		// Look for a candidate that fully covers the WHERE condition
		// (no residual filter needed).
		for candidate in &candidates {
			// Check: does this index access fully cover the condition?
			// If strip_index_conditions returns None, the index
			// consumed the entire WHERE clause.
			if strip_index_conditions(cond, &candidate.access, &candidate.index_ref.cols).is_none()
			{
				return Some((candidate.index_ref.clone(), candidate.access.clone()));
			}
		}

		None
	}

	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns `None` if the namespace/database/table cannot be resolved.
	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns the selected `AccessPath` and scan direction, or `None` if
	/// the namespace/database cannot be resolved.
	#[allow(clippy::too_many_arguments)]
	async fn resolve_access_path(
		&self,
		txn: &crate::kvs::Transaction,
		ns_name: &str,
		db_name: &str,
		table_name: &crate::val::TableName,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
	) -> Result<Option<(AccessPath, crate::idx::planner::ScanDirection)>, Error> {
		let direction = determine_scan_direction(order);

		if matches!(with, Some(crate::expr::with::With::NoIndex)) {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Look up namespace and database to get IDs
		let ns_def = match txn.get_ns_by_name(ns_name).await {
			Ok(Some(ns)) => ns,
			_ => return Ok(None),
		};
		let db_def = match txn.get_db_by_name(ns_name, db_name).await {
			Ok(Some(db)) => db,
			_ => return Ok(None),
		};

		// Fetch indexes for the table
		let indexes =
			match txn.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name).await {
				Ok(idx) => idx,
				Err(_) => return Ok(None),
			};

		if indexes.is_empty() {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Rewrite projection function calls (e.g. type::field("name")) →
		// Idiom in a cloned condition so the index analyzer can match
		// against indexed columns.
		let rewritten_cond = cond.map(|c| {
			let mut c = c.clone();
			resolve_projection_field_idioms(&mut c, self.function_registry());
			c
		});
		let analysis_cond = rewritten_cond.as_ref();

		let analyzer = IndexAnalyzer::new(indexes, with);
		let candidates = analyzer.analyze(analysis_cond, order);

		if candidates.is_empty() {
			if let Some(path) = analyzer.try_or_union(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding IN operators into union of equality lookups
			if let Some(path) = analyzer.try_in_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding CONTAINSALL/CONTAINSANY into union of equality lookups
			if let Some(path) = analyzer.try_containment_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		let path = select_access_path(candidates, with, direction);

		// When the chosen index covers ORDER BY, derive the correct scan
		// direction from the ORDER BY clause rather than the default
		// `determine_scan_direction` (which only handles ORDER BY id).
		// This enables LIMIT pushdown and sort elimination for queries like
		// `ORDER BY metadata.payload_metadata.modified DESC LIMIT 25`.
		let (path, direction) = adjust_direction_for_order(path, order, direction);

		// When the best single-index path is a full-range scan (ORDER BY
		// only, no WHERE selectivity), also try a multi-index union for
		// OR conditions. The union reads only matching rows from each
		// branch, which is typically far better than scanning every row
		// in the index. The outer pipeline adds a Sort when the union
		// does not satisfy ORDER BY.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_or_union(analysis_cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}
		// NOTE: We intentionally do NOT try try_in_expansion() here.
		// The full-range scan covers ORDER BY, enabling sort elimination
		// and early termination with the batch ceiling.  Replacing it
		// with a Union of prefix scans would require an expensive Sort
		// of ALL matching records, which is far worse for ORDER BY +
		// LIMIT queries.  IN expansion is only helpful in the
		// candidates.is_empty() fallback above when no index covers
		// ORDER BY at all.

		Ok(Some((path, direction)))
	}
}

/// Adjust the scan direction and access path when the chosen index covers
/// the ORDER BY clause.
///
/// `determine_scan_direction` only flips to `Backward` for `ORDER BY id DESC`.
/// When an index covers a non-`id` ORDER BY (e.g. a nested field like
/// `metadata.payload_metadata.modified DESC`), we must derive the correct
/// direction from the ORDER BY clause so that:
///
/// 1. `index_covers_ordering()` succeeds → LIMIT is pushed to the IndexScan
/// 2. `can_eliminate_sort()` succeeds → the Sort operator is eliminated
///
/// Without this fix, the index is scanned forward, LIMIT cannot be pushed
/// (direction mismatch), and all rows are read + sorted in memory.
fn adjust_direction_for_order(
	path: AccessPath,
	order: Option<&crate::expr::order::Ordering>,
	default_direction: crate::idx::planner::ScanDirection,
) -> (AccessPath, crate::idx::planner::ScanDirection) {
	use crate::exec::field_path::FieldPath;
	use crate::exec::index::access_path::BTreeAccess;
	use crate::expr::order::Ordering;
	use crate::idx::planner::ScanDirection;

	// Only adjust for BTreeScan paths that cover ORDER BY
	let AccessPath::BTreeScan {
		ref index_ref,
		ref access,
		..
	} = path
	else {
		return (path, default_direction);
	};

	// Need an ORDER BY clause to determine direction
	let Some(Ordering::Order(order_list)) = order else {
		return (path, default_direction);
	};

	let ix_def = index_ref.definition();

	// Collect equality-pinned column paths so we can skip ORDER BY fields
	// that reference them (those columns have a single constant value,
	// so any direction trivially satisfies the requirement).
	let equality_col_paths: Vec<FieldPath> = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => ix_def
			.cols
			.iter()
			.take(prefix.len())
			.filter_map(|idiom| FieldPath::try_from(idiom).ok())
			.collect(),
		BTreeAccess::Equality(_) => {
			ix_def.cols.iter().filter_map(|idiom| FieldPath::try_from(idiom).ok()).collect()
		}
		_ => vec![],
	};

	// Skip leading ORDER BY fields that match equality-pinned columns.
	let mut order_idx = 0;
	for field in order_list.0.iter() {
		if let Ok(fp) = FieldPath::try_from(&field.value)
			&& equality_col_paths.contains(&fp)
		{
			order_idx += 1;
			continue;
		}
		break;
	}

	// Get the first non-constant ORDER BY field
	let Some(first_order) = order_list.0.get(order_idx) else {
		// All ORDER BY fields are constant — direction doesn't matter,
		// keep the default.
		return (path, default_direction);
	};

	let Ok(order_path) = FieldPath::try_from(&first_order.value) else {
		return (path, default_direction);
	};

	// Determine which index column to match against.
	// For compound access with an equality prefix, match the column
	// immediately after the prefix.  For Equality access on a
	// single-column index, all index columns are skipped.
	let target_col_index = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => ix_def.cols.len(),
		_ => 0,
	};

	// If all index columns are equality-pinned, the effective ordering
	// is by record ID.  Check if the ORDER BY field is `id`.
	if target_col_index >= ix_def.cols.len() {
		// All columns are equality-pinned.  Match `ORDER BY id`.
		if order_path == FieldPath::field("id") {
			let new_direction = if first_order.direction {
				ScanDirection::Forward // ASC
			} else {
				ScanDirection::Backward // DESC
			};
			let new_path = AccessPath::BTreeScan {
				index_ref: index_ref.clone(),
				access: access.clone(),
				direction: new_direction,
			};
			return (new_path, new_direction);
		}
		return (path, default_direction);
	}

	let Some(target_col) = ix_def.cols.get(target_col_index) else {
		return (path, default_direction);
	};

	let Ok(col_path) = FieldPath::try_from(target_col) else {
		return (path, default_direction);
	};

	// If the target column matches the ORDER BY field,
	// set the direction based on the ORDER BY direction
	if order_path == col_path {
		let new_direction = if first_order.direction {
			ScanDirection::Forward // ASC
		} else {
			ScanDirection::Backward // DESC
		};

		let new_path = AccessPath::BTreeScan {
			index_ref: index_ref.clone(),
			access: access.clone(),
			direction: new_direction,
		};

		(new_path, new_direction)
	} else {
		(path, default_direction)
	}
}

/// Collect output field names from a SELECT field list.
///
/// These names are passed to `ExpressionRegistry::with_reserved_names` so that
/// synthetic internal names (`_e0`, `_e1`, ...) do not collide with fields the
/// user explicitly selected.
fn collect_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(_) => vec![], // SELECT VALUE has no object fields
		Fields::Select(field_list) => {
			let mut names = Vec::with_capacity(field_list.len());
			for field in field_list {
				if let Field::Single(selector) = field {
					let name = if let Some(alias) = &selector.alias {
						idiom_to_field_name(alias)
					} else {
						derive_field_name(&selector.expr)
					};
					names.push(name);
				}
			}
			names
		}
	}
}

/// Check whether the ORDER BY clause is exactly `ORDER BY id ASC` or
/// `ORDER BY id DESC` with no additional columns.
///
/// Returns `Some(SortDirection)` when the condition is met, allowing
/// callers to enable optimisations that rely on record-ID ordering
/// (e.g. merge-sort in `UnionIndexScan`).
fn detect_order_by_id_only(order: Option<&crate::expr::order::Ordering>) -> Option<SortDirection> {
	use crate::expr::order::Ordering;
	if let Some(Ordering::Order(order_list)) = order
		&& order_list.len() == 1
		&& let Some(first) = order_list.0.first()
		&& first.value.is_id()
		&& !first.collate
		&& !first.numeric
	{
		Some(if first.direction {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		})
	} else {
		None
	}
}
