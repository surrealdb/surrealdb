//! SELECT pipeline assembly: WHERE → SPLIT → GROUP → ORDER → LIMIT → projection.
//!
//! Owns the pipeline-state types ([`WhereClauseState`], [`FilterAction`],
//! [`PlannedSource`], [`SelectPipelineConfig`]) and the orchestration
//! method [`Planner::plan_pipeline`] that wires the operator chain
//! together over an already-planned source. The SELECT compiler hands off to
//! this module once it has chosen an access path, and lookup planning
//! (`source.rs`) reuses it for a lookup's own clauses; projection planning is
//! delegated to [`super::projection`].
//!
//! Sort planning lives here too — both [`Planner::plan_sort`] (for grouped
//! queries, whose projection is evaluated by the aggregation) and
//! [`Planner::plan_sort_consolidated`] (which shares an
//! [`ExpressionRegistry`] with projection so an expression appearing in
//! both ORDER BY and SELECT is computed only once). The
//! [`Planner::can_eliminate_sort`] helper is consulted before either,
//! short-circuiting Sort entirely when the input's `OutputOrdering`
//! already satisfies the requirement.

use std::sync::Arc;

use super::super::Planner;
use super::super::util::{
	check_forbidden_group_by_params, field_path_from_expr, get_effective_limit_literal,
};
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::field_path::FieldPath;
use crate::exec::logical::{BatchRetry, ColumnEffects, columns_may_run_column_major};
use crate::exec::operators::{
	Aggregate, Compute, Filter, Limit, RandomShuffle, Sort, SortByKey, SortDirection, SortKey,
	SortTopK, SortTopKByKey, Split,
};
#[cfg(all(storage, not(target_family = "wasm")))]
use crate::exec::operators::{ExternalSort, ExternalSortByKey};
use crate::exec::planner::aggregate::aggregate_may_run_column_major;
use crate::exec::topk_pushdown::TopKPushdownHandle;
#[cfg(test)]
use crate::exec::topk_pushdown::TopKPushdownReason;
use crate::exec::{ExecOperator, PhysicalExpr};
use crate::expr::logical::{LogicalExpr, LogicalFields, LogicalOrdering};

// ============================================================================
// Pipeline-state types
// ============================================================================

/// State of the WHERE clause after source planning has had a chance to
/// push it into the Scan operator.
///
/// Single source of truth for the pipeline's WHERE handling: the type
/// system enforces that exactly one representation is in flight, so
/// callers can't accidentally provide both a predicate to compile and a
/// precompiled one.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Default)]
pub(crate) enum WhereClauseState {
	/// No predicate to apply at the pipeline level. Either the source
	/// consumed the WHERE clause fully, the query has no WHERE at all,
	/// or the predicate has been wrapped into the source upstream (e.g.
	/// the brute-force KNN path applies a pre-filter before ranking).
	#[default]
	None,
	/// The predicate to compile. `plan_pipeline` turns it into a
	/// `PhysicalExpr` before wrapping the source in a `Filter` operator.
	Original(LogicalExpr),
	/// A predicate already compiled (typically by source planning for
	/// scan pushdown) that ended up not being consumed by the source.
	/// Reusing it avoids paying the compilation cost twice.
	Precompiled(Arc<dyn PhysicalExpr>),
}

/// The clauses [`Planner::plan_pipeline`] applies over a planned source.
///
/// Bundles the optional clauses so the pipeline is configured in one place.
#[derive(Default)]
pub(crate) struct SelectPipelineConfig {
	pub where_clause: WhereClauseState,
	pub split: Option<Vec<LogicalExpr>>,
	/// `GROUP BY` keys; an empty list is `GROUP ALL`.
	pub group: Option<Vec<LogicalExpr>>,
	pub order: Option<LogicalOrdering>,
	pub limit: Option<LogicalExpr>,
	pub start: Option<LogicalExpr>,
	pub omit: Vec<LogicalExpr>,
	pub tempfiles: bool,
	/// TopK threshold pushdown handle created by source planning; installed
	/// into the sort operator by [`Planner::plan_sort_consolidated`] when the
	/// sort plan matches the probe the scan was compiled against.
	pub topk_pushdown: Option<TopKPushdownHandle>,
}

#[cfg(test)]
/// Describes how the WHERE predicate should be handled after source planning.
pub(crate) enum FilterAction {
	/// Source did not analyze the predicate. Use the original `cond_for_filter`.
	UseOriginal,
	/// All conditions consumed by the source. No Filter needed.
	FullyConsumed,
	/// Partial residual remains. Create a Filter with this predicate only.
	Residual(LogicalExpr),
}

#[cfg(test)]
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

#[cfg(test)]
impl PlannedSource {
	#[cfg(test)]
	/// A source the pipeline treats as opaque: nothing was pushed into it, so
	/// every clause above it applies in full.
	pub(crate) fn opaque(operator: Arc<dyn ExecOperator>) -> Self {
		Self {
			operator,
			filter_action: FilterAction::UseOriginal,
			limit_pushed: false,
			topk_pushdown: None,
		}
	}
}

#[cfg(test)]
/// Determine `FilterAction` when a scan predicate has been compiled.
///
/// When the planner compiled a `scan_predicate` (physical WHERE expression),
/// the source operator is expected to apply it internally, so the outer
/// pipeline needs no additional Filter. Otherwise the original condition
/// must be used.
pub(crate) fn filter_action_for_predicate(
	scan_predicate: &Option<Arc<dyn PhysicalExpr>>,
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

#[cfg(test)]
/// First ORDER BY key analysis for TopK threshold pushdown, computed by
/// [`compute_topk_pushdown_request`] before source planning.
pub(crate) struct TopKFirstKeySpec {
	/// The first ORDER BY key exactly as [`Planner::plan_sort_consolidated`]
	/// will build it — the install guard compares against this.
	pub(crate) first_key: SortKey,
	/// Total ORDER BY key count; `1` lets the scan probe reject ties.
	pub(crate) key_count: usize,
}

#[cfg(test)]
/// Whether (and why not) the SELECT's ORDER BY + LIMIT shape allows the scan
/// to reject rows against the TopK heap's threshold before record decode.
///
/// Computed once when the pipeline is compiled and threaded to
/// `plan_table_scan_source`, which builds the probe
/// ([`crate::exec::topk_pushdown`]) for `Eligible` and surfaces `Ineligible`
/// reasons in EXPLAIN.
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

#[cfg(test)]
/// Analyse the SELECT's ORDER BY / LIMIT / START for TopK threshold pushdown.
///
/// Mirrors [`Planner::plan_sort_consolidated`]'s first-key resolution: any
/// shape that would route the first key through the expression registry
/// (alias to expression, multi-step path, lookup, unconvertible path)
/// produces a synthetic post-decode sort key that cannot be probed on raw
/// bytes. The mirror need not be perfect — the install guard in
/// `plan_sort_consolidated` re-checks the actually-built sort keys against
/// [`TopKFirstKeySpec::first_key`], so a divergence only leaves the probe
/// dormant.
///
/// `fields` is the projection the ORDER BY keys may alias into; `None` when
/// the pipeline has no projection, so no alias can resolve. `disqualified`
/// covers plan shapes where rows are duplicated, regrouped, or ranked between
/// scan and sort (SPLIT, GROUP BY, brute-force KNN).
pub(crate) fn compute_topk_pushdown_request(
	order: Option<&LogicalOrdering>,
	start: Option<&LogicalExpr>,
	limit: Option<&LogicalExpr>,
	fields: Option<&LogicalFields>,
	tempfiles: bool,
	disqualified: bool,
	max_priority_queue_size: usize,
) -> TopKPushdownRequest {
	if disqualified {
		return TopKPushdownRequest::NotApplicable;
	}
	// `Random` uses RandomShuffle; no heap, no threshold.
	let Some(LogicalOrdering::Order(order_list)) = order else {
		return TopKPushdownRequest::NotApplicable;
	};
	let Some(first) = order_list.first() else {
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
	let key = &first.value;
	let field_path = if let Some((resolved_expr, _alias)) =
		fields.and_then(|fields| resolve_order_by_alias(key, fields))
	{
		match &resolved_expr {
			LogicalExpr::Field(name) => FieldPath::field(name.as_str()),
			_ => return TopKPushdownRequest::Ineligible(TopKPushdownReason::UnsupportedOrder),
		}
	} else {
		match field_path_from_expr(key) {
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
		key_count: order_list.len(),
	})
}

// ============================================================================
// Pipeline orchestration
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Apply the pipeline's clauses over a planned source.
	///
	/// `fields` is the projection to apply; `None` builds no projection
	/// operator, so the source's rows are returned as they are (apart from
	/// any `OMIT`, and the fields a sort computed for itself). A grouped
	/// pipeline projects inside its aggregation, so `fields` is consumed there
	/// and only `OMIT` applies above it.
	pub(crate) async fn plan_pipeline(
		&self,
		source: Arc<dyn ExecOperator>,
		fields: Option<LogicalFields>,
		config: SelectPipelineConfig,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let SelectPipelineConfig {
			where_clause,
			split,
			group,
			order,
			limit,
			start,
			mut omit,
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
				let predicate = self.physical_expr(cond).await?;
				Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
			}
		};

		// Shared expression registry for deduplication across sort and
		// projection. Expressions computed for ORDER BY are reused by the
		// projection step. Reserve the SELECT field names so that synthetic
		// `_eN` names never collide with fields the user explicitly selected.
		let mut registry = ExpressionRegistry::with_reserved_and_protected_names(
			fields.as_ref().map(super::collect_field_names).unwrap_or_default(),
			fields.as_ref().map(super::collect_simple_source_field_names).unwrap_or_default(),
		);

		// A `SPLIT` names a field of the row the statement returns, so the row
		// it divides is the projected one: an alias names a field the source
		// row does not have, a path through a record link exists only once the
		// projection has built it, a name the projection reuses holds a
		// different value on either side, and a field reading the split one is
		// owed the whole of it. The pipeline projects last for every other
		// shape, because narrow rows sort and page more cheaply; a statement
		// with a `SPLIT` projects here instead and the tail leaves its rows
		// alone.
		//
		// `SPLIT` and `GROUP` are mutually exclusive in one statement, which
		// the parser enforces, so the aggregation below never receives rows
		// this projected for the statement.
		let (split_op, split_projected) = if let Some(exprs) = split {
			let projected = match fields.clone() {
				Some(fields) => {
					let omit = std::mem::take(&mut omit);
					self.plan_projections_fast(fields, omit, filtered, &mut registry).await?
				}
				None => filtered,
			};
			let mut keys = Vec::with_capacity(exprs.len());
			for expr in &exprs {
				keys.push(self.physical_expr(expr.clone()).await?);
			}
			(Arc::new(Split::new(projected, exprs, keys)?) as Arc<dyn ExecOperator>, true)
		} else {
			(filtered, false)
		};

		let (grouped, skip_projections) = if let Some(group_by) = group {
			let agg_fields = fields.clone().unwrap_or_else(LogicalFields::all);
			check_forbidden_group_by_params(&agg_fields)?;

			let column_major =
				aggregate_may_run_column_major(&agg_fields, &group_by, self.function_registry());

			let (aggregates, group_by_exprs) =
				self.plan_aggregation(&agg_fields, &group_by).await?;

			let mut aggregate = Aggregate::new(split_op, group_by, group_by_exprs, aggregates);
			if column_major {
				aggregate = aggregate.with_column_major();
			}

			(Arc::new(aggregate) as Arc<dyn ExecOperator>, true)
		} else {
			(split_op, split_projected)
		};

		let (sorted, sort_only_omits) = if let Some(order) = order {
			// Sort elimination: if the input is already sorted in the required
			// order, skip creating a Sort operator entirely.
			// A grouped query's rows are named by the aggregation, so its
			// ORDER BY keys are already the projected names and resolve
			// against nothing.
			let alias_scope = if skip_projections {
				None
			} else {
				fields.as_ref()
			};
			if self.can_eliminate_sort(&grouped, &order, alias_scope) {
				(grouped, vec![])
			} else if skip_projections {
				// A grouped query's projection is evaluated by the aggregation,
				// so its sort keys are read from the aggregated rows: the
				// consolidated path's Compute operator would evaluate an
				// aggregate expression (e.g. math::sum) per input row instead.
				(
					self.plan_sort(grouped, order, start.as_ref(), limit.as_ref(), tempfiles)
						.await?,
					vec![],
				)
			} else {
				self.plan_sort_consolidated(
					grouped,
					order,
					fields.as_ref(),
					start.as_ref(),
					limit.as_ref(),
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
				Some(l) => Some(self.physical_expr(l).await?),
				None => None,
			};
			let offset_expr = match start {
				Some(s) => Some(self.physical_expr(s).await?),
				None => None,
			};
			Arc::new(Limit::new(sorted, limit_expr, offset_expr)) as Arc<dyn ExecOperator>
		} else {
			sorted
		};

		let mut all_omit = omit;
		for field_name in sort_only_omits {
			all_omit.push(LogicalExpr::Field(field_name.into()));
		}

		let projected = match fields {
			Some(fields) if !skip_projections => {
				self.plan_projections_fast(fields, all_omit, limited, &mut registry).await?
			}
			// Grouped (the aggregation projected), or no projection at all:
			// only an `OMIT` is left to apply.
			Some(_) | None => {
				if !all_omit.is_empty() {
					let omit_fields = self.plan_omit(all_omit).await?;
					Arc::new(
						crate::exec::operators::Project::new(limited, vec![], omit_fields, true)
							.with_column_major(),
					) as Arc<dyn ExecOperator>
				} else {
					limited
				}
			}
		};

		Ok(projected)
	}

	/// Check whether the input operator's output ordering already satisfies
	/// the requested ORDER BY, allowing the Sort operator to be eliminated.
	///
	/// `fields` is the projection an ORDER BY key may alias into, and must be
	/// `None` wherever the sort reads rows the projection already produced —
	/// a grouped query, whose aggregation names its rows by their aliases.
	pub(crate) fn can_eliminate_sort(
		&self,
		input: &Arc<dyn ExecOperator>,
		order: &LogicalOrdering,
		fields: Option<&LogicalFields>,
	) -> bool {
		use crate::exec::ordering::SortProperty;

		let LogicalOrdering::Order(order_list) = order else {
			return false; // Random ordering can't be eliminated
		};

		// Convert the ORDER BY clause to SortProperty requirements,
		// including collate/numeric modifiers so that the satisfies
		// check rejects mismatches against raw key ordering.
		let required: Vec<SortProperty> = order_list
			.iter()
			.filter_map(|field| {
				// Resolve the key the way `plan_sort_consolidated` will:
				// alias resolution first, then FieldPath conversion. An
				// ORDER BY naming a projection output sorts by what that
				// output holds, so the path an input ordering has to provide
				// is the aliased expression's — `b AS a … ORDER BY a` needs
				// `b` ordered, and an index over the record's own `a` does
				// not supply it. A key resolving to anything but a plain
				// field read is computed above the scan, which no scan
				// ordering can satisfy.
				let path =
					match fields.and_then(|fields| resolve_order_by_alias(&field.value, fields)) {
						Some((LogicalExpr::Field(name), _)) => FieldPath::field(name.as_str()),
						Some(_) => return None,
						None => field_path_from_expr(&field.value).ok()?,
					};
				let direction = if field.direction {
					SortDirection::Asc
				} else {
					SortDirection::Desc
				};
				Some(SortProperty {
					path,
					direction,
					collate: field.collate,
					numeric: field.numeric,
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

	/// Plan ORDER BY over rows whose sort keys are read as written (grouped
	/// queries, and lookups without a projection to consolidate with).
	pub(crate) async fn plan_sort(
		&self,
		input: Arc<dyn ExecOperator>,
		order: LogicalOrdering,
		start: Option<&LogicalExpr>,
		limit: Option<&LogicalExpr>,
		#[allow(unused)] tempfiles: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match order {
			LogicalOrdering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok(Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>)
			}
			LogicalOrdering::Order(order_list) => {
				// These operators evaluate the keys themselves, so the shape
				// of that evaluation is judged here rather than at a `Compute`
				// the way `plan_sort_consolidated`'s keys are; see
				// `Sort::column_major`. A sort passes a failed key column on
				// as the batch's, so it repeats nothing.
				let key_effects: Vec<ColumnEffects> = order_list
					.iter()
					.map(|field| ColumnEffects::of(&field.value, self.function_registry()))
					.collect();
				let column_major = columns_may_run_column_major(&key_effects, BatchRetry::None);
				let order_by = self.convert_order_list(order_list).await?;

				#[cfg(all(storage, not(target_family = "wasm")))]
				if tempfiles && let Some(temp_dir) = self.ctx.temporary_directory() {
					let mut sort = ExternalSort::new(input, order_by, temp_dir.to_path_buf());
					if column_major {
						sort = sort.with_column_major();
					}
					return Ok(Arc::new(sort) as Arc<dyn ExecOperator>);
				}

				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit
						<= self.ctx.config.exec.max_order_limit_priority_queue_size as usize
				{
					let mut topk = SortTopK::new(input, order_by, effective_limit);
					if column_major {
						topk = topk.with_column_major();
					}
					return Ok(Arc::new(topk) as Arc<dyn ExecOperator>);
				}

				let mut sort = Sort::new(input, order_by);
				if column_major {
					sort = sort.with_column_major();
				}
				Ok(Arc::new(sort) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan ORDER BY with consolidated expression evaluation.
	///
	/// Uses a shared `ExpressionRegistry` so that expressions computed for sort
	/// can be reused by downstream projection (avoiding duplicate computation).
	/// `fields` is the projection an ORDER BY key may alias into, when there is
	/// one.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_sort_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		order: LogicalOrdering,
		fields: Option<&LogicalFields>,
		start: Option<&LogicalExpr>,
		limit: Option<&LogicalExpr>,
		#[allow(unused)] tempfiles: bool,
		registry: &mut ExpressionRegistry,
		topk_pushdown: Option<TopKPushdownHandle>,
	) -> Result<(Arc<dyn ExecOperator>, Vec<String>), Error> {
		match order {
			LogicalOrdering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok((
					Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>,
					vec![],
				))
			}
			LogicalOrdering::Order(order_list) => {
				let mut sort_keys = Vec::with_capacity(order_list.len());
				let mut sort_only_fields: Vec<String> = Vec::new();

				for order_field in order_list.iter() {
					let key = &order_field.value;

					let field_path = if let Some((resolved_expr, alias)) =
						fields.and_then(|fields| resolve_order_by_alias(key, fields))
					{
						match &resolved_expr {
							// A bare field read is read as written. Any other
							// expression — a multi-step path or a lookup
							// included, which need context-aware evaluation
							// (record-link traversal like `in.creationDate` on
							// edge tables) — is computed before the sort.
							LogicalExpr::Field(name) => FieldPath::field(name.as_str()),
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
						match field_path_from_expr(key) {
							Ok(path) => path,
							Err(_) => {
								let name =
									registry.register(key, ComputePoint::Sort, None, self).await?;
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
					let mut compute = Compute::new(input, compute_fields);
					if registry.point_may_run_column_major(ComputePoint::Sort) {
						compute = compute.with_column_major();
					}
					Arc::new(compute) as Arc<dyn ExecOperator>
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
					// Install the TopK threshold pushdown only when the sort
					// keys actually built here match what the scan-side probe
					// was compiled against (path, direction, modifiers, key
					// count). Any divergence — alias resolution or registry
					// compute differing from the request-time analysis —
					// leaves the pushdown without a publisher: the scan finds
					// no threshold and reads every row rather than the wrong
					// ones.
					if let Some(handle) = topk_pushdown
						&& topk.sort_keys.first() == Some(&handle.expected_first_key)
						&& topk.sort_keys.len() == handle.expected_key_count
					{
						topk = topk.with_topk_pushdown(handle.pushdown);
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::planner::fold::collect_pipeline;
	use crate::expr::logical::lower_select;
	use crate::syn;

	/// The `SPLIT` targets and selection of a `SELECT`, as the pipeline sees
	/// them.
	fn split_and_fields(sql: &str) -> (Vec<LogicalExpr>, Option<LogicalFields>) {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Select(select)) = exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		let (clauses, _) = collect_pipeline(lower_select(*select).expect("lowers"));
		let split = clauses.split.expect("the statement has a SPLIT");
		(split, clauses.project.map(|project| project.fields))
	}

	/// Every `SPLIT` shape reaches the pipeline with its targets and its
	/// selection intact, whatever the projection does to the split field.
	/// The pipeline projects before it splits, so no shape is turned away
	/// for naming a field only the projected row has.
	#[test]
	fn every_split_shape_carries_its_targets_and_selection() {
		for sql in [
			// The field is carried through under its own name.
			"SELECT id, own FROM t SPLIT own",
			"SELECT * FROM t SPLIT own",
			"SELECT id, own, other FROM t SPLIT own, other",
			// The name exists only after the projection.
			"SELECT id, own AS x FROM t SPLIT x",
			"SELECT id, [1, 2, 3] AS x FROM t SPLIT x",
			"SELECT id, link.tags AS x FROM t SPLIT x",
			// The target is a path the projection builds.
			"SELECT id, link.tags FROM t SPLIT link.tags",
			// The name holds a different value on either side.
			"SELECT id, other AS own FROM t SPLIT own",
			"SELECT id, array::distinct(own) AS own FROM t SPLIT own",
			// Another field is owed the whole of the split value.
			"SELECT id, own AS a, own FROM t SPLIT own",
		] {
			let (targets, fields) = split_and_fields(sql);
			assert!(!targets.is_empty(), "`{sql}` carries its SPLIT targets");
			assert!(fields.is_some(), "`{sql}` carries its selection");
		}
	}
}
