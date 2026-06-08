//! SELECT pipeline assembly: WHERE → SPLIT → GROUP → ORDER → LIMIT → projection.
//!
//! Owns the pipeline-state types ([`WhereClauseState`], [`FilterAction`],
//! [`PlannedSource`], [`SelectPipelineConfig`]) and the orchestration
//! method [`Planner::plan_pipeline`] that wires the operator chain
//! together. Source planning hands off to this module via
//! [`SelectPipelineConfig`]; projection planning is delegated to
//! [`super::projection`].
//!
//! Sort planning lives here too — both the legacy [`Planner::plan_sort`]
//! (for grouped queries that must use the simpler legacy sort path) and
//! [`Planner::plan_sort_consolidated`] (which shares an
//! [`ExpressionRegistry`] with projection so an expression appearing in
//! both ORDER BY and SELECT is computed only once). The
//! [`Planner::can_eliminate_sort`] helper is consulted before either,
//! short-circuiting Sort entirely when the input's `OutputOrdering`
//! already satisfies the requirement.

use std::sync::Arc;

use super::super::Planner;
use super::super::util::{check_forbidden_group_by_params, get_effective_limit_literal};
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::field_path::FieldPath;
use crate::exec::operators::{
	Aggregate, Compute, Filter, Limit, RandomShuffle, Sort, SortByKey, SortDirection, SortKey,
	SortTopK, SortTopKByKey, Split,
};
#[cfg(all(storage, not(target_family = "wasm")))]
use crate::exec::operators::{ExternalSort, ExternalSortByKey};
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::field::Fields;
use crate::expr::{Cond, Expr, Idiom};

// ============================================================================
// Pipeline-state types
// ============================================================================

/// State of the WHERE clause after source planning has had a chance to
/// push it into the Scan operator.
///
/// Single source of truth for the pipeline's WHERE handling: the type
/// system enforces that exactly one representation is in flight, so
/// callers can't accidentally provide both an AST condition and a
/// precompiled predicate.
#[derive(Default)]
pub(crate) enum WhereClauseState {
	/// No predicate to apply at the pipeline level. Either the source
	/// consumed the WHERE clause fully, the query has no WHERE at all,
	/// or the predicate has been wrapped into the source upstream (e.g.
	/// the brute-force KNN path applies a pre-filter before ranking).
	#[default]
	None,
	/// The original AST condition. `plan_pipeline` will compile it into a
	/// `PhysicalExpr` before wrapping the source in a `Filter` operator.
	Original(crate::expr::cond::Cond),
	/// A predicate already compiled (typically by source planning for
	/// scan pushdown) that ended up not being consumed by the source.
	/// Reusing it avoids paying the compilation cost twice.
	Precompiled(Arc<dyn crate::exec::PhysicalExpr>),
}

/// Configuration for the SELECT pipeline.
///
/// Bundles optional clauses from a SELECT statement to reduce parameter counts.
#[derive(Default)]
pub(crate) struct SelectPipelineConfig {
	pub where_clause: WhereClauseState,
	pub split: Option<crate::expr::split::Splits>,
	pub group: Option<crate::expr::group::Groups>,
	pub order: Option<crate::expr::order::Ordering>,
	pub limit: Option<crate::expr::limit::Limit>,
	pub start: Option<crate::expr::start::Start>,
	pub omit: Vec<Expr>,
	pub tempfiles: bool,
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
	pub(crate) operator: Arc<dyn ExecOperator>,
	/// How the WHERE predicate was handled by the source.
	pub(crate) filter_action: FilterAction,
	/// The limit and start values were consumed by the source operator.
	pub(crate) limit_pushed: bool,
}

/// Determine `FilterAction` when a scan predicate has been compiled.
///
/// When the planner compiled a `scan_predicate` (physical WHERE expression),
/// the source operator is expected to apply it internally, so the outer
/// pipeline needs no additional Filter. Otherwise the original condition
/// must be used.
pub(crate) fn filter_action_for_predicate(
	scan_predicate: &Option<Arc<dyn crate::exec::PhysicalExpr>>,
) -> FilterAction {
	if scan_predicate.is_some() {
		FilterAction::FullyConsumed
	} else {
		FilterAction::UseOriginal
	}
}

// ============================================================================
// Pipeline orchestration
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Plan the SELECT pipeline after the source is determined.
	pub(crate) async fn plan_pipeline(
		&self,
		source: Arc<dyn ExecOperator>,
		fields: Option<Fields>,
		config: SelectPipelineConfig,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let SelectPipelineConfig {
			where_clause,
			split,
			group,
			order,
			limit,
			start,
			omit,
			tempfiles,
		} = config;

		let filtered = match where_clause {
			WhereClauseState::None => source,
			WhereClauseState::Precompiled(predicate) => {
				Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
			}
			WhereClauseState::Original(cond) => {
				let predicate = self.physical_expr(cond.0).await?;
				Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
			}
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
		let mut registry = ExpressionRegistry::with_reserved_and_protected_names(
			super::collect_field_names(&fields),
			super::collect_simple_source_field_names(&fields),
		);

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
				Arc::new(crate::exec::operators::Project::new(limited, vec![], omit_fields, true))
					as Arc<dyn ExecOperator>
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
	pub(crate) fn can_eliminate_sort(
		&self,
		input: &Arc<dyn ExecOperator>,
		order: &crate::expr::order::Ordering,
	) -> bool {
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

	/// Plan ORDER BY (legacy path, used by grouped queries).
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
					&& effective_limit
						<= self.ctx.config.max_order_limit_priority_queue_size as usize
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
											self,
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
													self,
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
										self,
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
									.register(&expr, ComputePoint::Sort, None, self)
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
					let compute_fields = registry
						.get_expressions_for_point(ComputePoint::Sort)
						.into_iter()
						.map(|(name, expr)| (crate::val::Strand::new(name), expr))
						.collect();
					Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
				} else {
					input
				};

				// Honour TEMPFILES before LIMIT-based heap selection: the user
				// explicitly opted in to disk-backed sort, and a small LIMIT
				// shouldn't silently swap them back to an in-memory heap.
				#[cfg(all(storage, not(target_family = "wasm")))]
				if tempfiles && let Some(temp_dir) = self.ctx.temporary_directory() {
					return Ok((
						Arc::new(ExternalSortByKey::new(
							computed,
							sort_keys,
							temp_dir.to_path_buf(),
						)) as Arc<dyn ExecOperator>,
						sort_only_fields,
					));
				}

				// Use heap-based TopK when the effective limit is small.
				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit
						<= self.ctx.config.max_order_limit_priority_queue_size as usize
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
}
