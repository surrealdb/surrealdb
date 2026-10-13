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
use crate::exec::topk_pushdown::{TopKPushdownHandle, TopKPushdownReason};
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
	/// TopK threshold pushdown handle created by source planning; installed
	/// into the sort operator by [`Planner::plan_sort_consolidated`] when the
	/// sort plan matches the probe the scan was compiled against.
	pub topk_pushdown: Option<TopKPushdownHandle>,
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
	/// TopK threshold pushdown handle when the source built a scan-side
	/// probe (TableScan only); the pipeline hands it to sort planning.
	pub(crate) topk_pushdown: Option<TopKPushdownHandle>,
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
// TopK threshold pushdown request
// ============================================================================

/// First ORDER BY key analysis for TopK threshold pushdown, computed by
/// [`compute_topk_pushdown_request`] before source planning.
pub(crate) struct TopKFirstKeySpec {
	/// The first ORDER BY key exactly as [`Planner::plan_sort_consolidated`]
	/// will build it — the install guard compares against this.
	pub(crate) first_key: SortKey,
	/// Total ORDER BY key count; `1` lets the scan probe reject ties.
	pub(crate) key_count: usize,
}

/// Whether (and why not) the SELECT's ORDER BY + LIMIT shape allows the scan
/// to reject rows against the TopK heap's threshold before record decode.
///
/// Computed once in `plan_select` and threaded to `plan_table_scan_source`,
/// which builds the probe ([`crate::exec::topk_pushdown`]) for `Eligible`
/// and surfaces `Ineligible` reasons in EXPLAIN.
pub(crate) enum TopKPushdownRequest {
	/// No pushdown opportunity at all (no ORDER BY, `ORDER BY RAND()`,
	/// first key `id`, SPLIT/GROUP/KNN shapes, feature disabled) — the
	/// `topk_pushdown` EXPLAIN attribute is omitted.
	NotApplicable,
	/// An ORDER BY + LIMIT opportunity exists but the shape disqualifies it;
	/// surfaced as `topk_pushdown: no (…)` in EXPLAIN.
	Ineligible(TopKPushdownReason),
	/// The first key is a raw field path the scan can probe on wire bytes.
	Eligible(TopKFirstKeySpec),
}

/// Analyse the SELECT's ORDER BY / LIMIT / START for TopK threshold pushdown.
///
/// Mirrors [`Planner::plan_sort_consolidated`]'s first-key resolution: any
/// shape that would route the first key through the expression registry
/// (alias to expression, multi-part alias idiom, lookup, unconvertible idiom)
/// produces a synthetic post-decode sort key that cannot be probed on raw
/// bytes. The mirror need not be perfect — the install guard in
/// `plan_sort_consolidated` re-checks the actually-built sort keys against
/// [`TopKFirstKeySpec::first_key`], so a divergence only leaves the probe
/// dormant.
///
/// `disqualified` covers plan shapes where rows are duplicated, regrouped, or
/// ranked between scan and sort (SPLIT, GROUP BY, brute-force KNN).
pub(crate) fn compute_topk_pushdown_request(
	order: Option<&crate::expr::order::Ordering>,
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
	fields: &Fields,
	tempfiles: bool,
	disqualified: bool,
	max_priority_queue_size: usize,
) -> TopKPushdownRequest {
	use crate::expr::order::Ordering;
	use crate::expr::part::Part;

	if disqualified {
		return TopKPushdownRequest::NotApplicable;
	}
	// `Random` uses RandomShuffle; no heap, no threshold.
	let Some(Ordering::Order(order_list)) = order else {
		return TopKPushdownRequest::NotApplicable;
	};
	let Some(first) = order_list.0.first() else {
		return TopKPushdownRequest::NotApplicable;
	};
	// No LIMIT clause means no top-K at all — not an opportunity that was
	// missed, so the EXPLAIN attribute is omitted rather than surfaced as a
	// bail reason.
	if limit.is_none() {
		return TopKPushdownRequest::NotApplicable;
	}
	// Without a literal effective limit within the priority-queue bound
	// (parameter / expression limits, or START pushing the effective limit
	// over the cap), the plan uses a full sort — no heap threshold exists.
	match get_effective_limit_literal(start, limit) {
		Some(effective_limit) if effective_limit <= max_priority_queue_size => {}
		_ => return TopKPushdownRequest::Ineligible(TopKPushdownReason::LimitTooLarge),
	}
	// TEMPFILES routes to disk-backed sort before the LIMIT-based heap
	// selection (see plan_sort_consolidated).
	if tempfiles {
		return TopKPushdownRequest::Ineligible(TopKPushdownReason::Tempfiles);
	}
	// COLLATE / NUMERIC comparison modes are not replicated by the wire probe.
	if first.collate || first.numeric {
		return TopKPushdownRequest::Ineligible(TopKPushdownReason::UnsupportedOrder);
	}
	// Resolve the first key the way plan_sort_consolidated will: alias
	// resolution first, then FieldPath conversion. Registry-computed shapes
	// (the `registry.register` branches there) are synthetic post-decode
	// fields — not probeable.
	let idiom = &first.value;
	let field_path = if let Some((resolved_expr, _alias)) = resolve_order_by_alias(idiom, fields) {
		match &resolved_expr {
			Expr::Idiom(inner_idiom)
				if inner_idiom.len() == 1
					&& !inner_idiom.0.iter().any(|p| matches!(p, Part::Lookup(_))) =>
			{
				match crate::exec::field_path_convert::field_path_from_idiom(inner_idiom) {
					Ok(path) => path,
					Err(_) => {
						return TopKPushdownRequest::Ineligible(
							TopKPushdownReason::UnsupportedOrder,
						);
					}
				}
			}
			_ => return TopKPushdownRequest::Ineligible(TopKPushdownReason::UnsupportedOrder),
		}
	} else {
		match crate::exec::field_path_convert::field_path_from_idiom(idiom) {
			Ok(path) => path,
			Err(_) => return TopKPushdownRequest::Ineligible(TopKPushdownReason::UnsupportedOrder),
		}
	};
	// Non-Field parts (indices, lookups) cannot be walked on wire bytes, and
	// `id` is synthetic (derived from the KV key, owned by the scan-direction
	// optimisation) — `field_path_wire_segments` re-checks this when the
	// probe is built, but `id` specifically is NotApplicable rather than a
	// surfaced bail.
	use crate::exec::field_path::FieldPathPart;
	if matches!(field_path.0.first(), Some(FieldPathPart::Field(f)) if f == "id") {
		return TopKPushdownRequest::NotApplicable;
	}
	if crate::exec::topk_pushdown::field_path_wire_segments(&field_path).is_none() {
		return TopKPushdownRequest::Ineligible(TopKPushdownReason::UnsupportedOrder);
	}
	let mut first_key = SortKey::new(field_path);
	first_key.direction = if first.direction {
		SortDirection::Asc
	} else {
		SortDirection::Desc
	};
	TopKPushdownRequest::Eligible(TopKFirstKeySpec {
		first_key,
		key_count: order_list.0.len(),
	})
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
			topk_pushdown,
		} = config;

		// Defensive: the pushdown request is NotApplicable for SPLIT/GROUP
		// shapes, so the handle should already be None here — but row
		// duplication between scan and sort would be unsound, so drop it
		// structurally rather than rely on the upstream gate alone.
		let topk_pushdown = if split.is_some() || group.is_some() {
			None
		} else {
			topk_pushdown
		};

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
					topk_pushdown,
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
				crate::exec::field_path_convert::field_path_from_idiom(&field.value).ok().map(
					|path| {
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
					},
				)
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
						<= self.ctx.config.exec.max_order_limit_priority_queue_size as usize
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
		topk_pushdown: Option<TopKPushdownHandle>,
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
									match crate::exec::field_path_convert::field_path_from_idiom(
										inner_idiom,
									) {
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
						match crate::exec::field_path_convert::field_path_from_idiom(idiom) {
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
						<= self.ctx.config.exec.max_order_limit_priority_queue_size as usize
				{
					let mut topk = SortTopKByKey::new(computed, sort_keys, effective_limit);
					// Install the TopK threshold publish side only when the
					// sort keys actually built here match what the scan-side
					// probe was compiled against (path, direction, modifiers,
					// key count). Any divergence — alias resolution or
					// registry compute differing from the request-time
					// analysis — leaves the cell unpublished, keeping the
					// scan probe dormant rather than wrong.
					if let Some(handle) = topk_pushdown
						&& topk.sort_keys.first() == Some(&handle.expected_first_key)
						&& topk.sort_keys.len() == handle.expected_key_count
					{
						topk = topk.with_threshold_cell(handle.cell);
					}
					return Ok((Arc::new(topk) as Arc<dyn ExecOperator>, sort_only_fields));
				}

				Ok((
					Arc::new(SortByKey::new(computed, sort_keys)) as Arc<dyn ExecOperator>,
					sort_only_fields,
				))
			}
		}
	}
}
