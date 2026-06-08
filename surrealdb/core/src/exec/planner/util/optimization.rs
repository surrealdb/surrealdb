//! Plan-time optimisation eligibility checks.
//!
//! Predicates that decide whether a SELECT can take a faster path:
//! `CountScan`/`IndexCountScan` for `SELECT count() ... GROUP ALL`, ORDER BY
//! pushdown into the scan operator (sort elimination), index-covers-ordering
//! for limit pushdown, and the `extract_count_field_names` helper that names
//! the count() output for the fast path. Plus the small helpers for VERSION
//! extraction and effective-LIMIT computation.

use super::fields::{derive_field_name, idiom_to_field_name};
use crate::err::Error;
use crate::expr::field::{Field, Fields};
use crate::expr::{Expr, Literal};

// ============================================================================
// Pushdown Eligibility
// ============================================================================

/// Check if ORDER BY is compatible with the natural KV scan direction.
///
/// Returns `true` when ORDER BY is absent, or is exactly `id ASC` or `id DESC`
/// with no COLLATE/NUMERIC modifiers. In these cases the scan already produces
/// rows in the requested order and no separate Sort operator is needed.
pub(crate) fn order_is_scan_compatible(order: Option<&crate::expr::order::Ordering>) -> bool {
	use crate::expr::order::Ordering;
	match order {
		None => true,
		Some(Ordering::Random) => false,
		Some(Ordering::Order(list)) => {
			list.0.len() == 1 && list.0[0].value.is_id() && !list.0[0].collate && !list.0[0].numeric
		}
	}
}

/// Check if an index + scan direction satisfies the given ORDER BY.
///
/// Builds the same `SortProperty` vector that `IndexScan::output_ordering()`
/// would produce and checks whether it satisfies the ORDER BY requirements.
/// This allows the planner to decide on limit pushdown before the IndexScan
/// operator is created.
///
/// For compound access with an equality prefix, the prefix columns all have
/// the same value and do not define ordering. They are skipped so that the
/// effective ordering starts from the first non-equality column.
///
/// For single-column Equality access (`WHERE col = val`), ALL index columns
/// are constant, so they are all skipped.  Leading ORDER BY fields that
/// reference constant (equality-pinned) columns are also stripped from the
/// requirement because any direction trivially matches a single-valued column.
pub(crate) fn index_covers_ordering(
	index_ref: &crate::exec::index::access_path::IndexRef,
	access: &crate::exec::index::access_path::BTreeAccess,
	direction: crate::idx::planner::ScanDirection,
	order: &crate::expr::order::Ordering,
) -> bool {
	use crate::exec::index::access_path::BTreeAccess;
	use crate::exec::operators::SortDirection;
	use crate::exec::ordering::{OutputOrdering, SortProperty};
	use crate::expr::order::Ordering;

	let Ordering::Order(order_list) = order else {
		return false; // Random ordering can't be satisfied by an index
	};

	// Convert ORDER BY to required SortProperty
	let required: Vec<SortProperty> = order_list
		.iter()
		.filter_map(|field| {
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

	if required.len() != order_list.len() {
		return false;
	}

	// Determine which index columns are equality-pinned (constant value).
	let ix_def = index_ref.definition();
	let (skip_cols, equality_field_paths) = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => {
			let paths: Vec<_> = ix_def
				.cols
				.iter()
				.take(prefix.len())
				.filter_map(|idiom| crate::exec::field_path::FieldPath::try_from(idiom).ok())
				.collect();
			(prefix.len(), paths)
		}
		BTreeAccess::Equality(_) => {
			let paths: Vec<_> = ix_def
				.cols
				.iter()
				.filter_map(|idiom| crate::exec::field_path::FieldPath::try_from(idiom).ok())
				.collect();
			(ix_def.cols.len(), paths)
		}
		_ => (0, vec![]),
	};

	// Strip leading ORDER BY fields that reference equality-pinned columns.
	// These columns have a single constant value, so any direction trivially
	// satisfies the ordering requirement for them.
	let required: Vec<SortProperty> =
		required.into_iter().skip_while(|prop| equality_field_paths.contains(&prop.path)).collect();

	// Build the index ordering (same as IndexScan::output_ordering())
	let dir = match direction {
		crate::idx::planner::ScanDirection::Forward => SortDirection::Asc,
		crate::idx::planner::ScanDirection::Backward => SortDirection::Desc,
	};
	let mut cols: Vec<SortProperty> = ix_def
		.cols
		.iter()
		.skip(skip_cols)
		.filter_map(|idiom| {
			crate::exec::field_path::FieldPath::try_from(idiom).ok().map(|path| SortProperty {
				path,
				direction: dir,
				collate: false,
				numeric: false,
			})
		})
		.collect();

	// For non-unique indexes (Idx), the record ID is stored in the BTree
	// key after the field values.  Entries are implicitly sorted by record
	// ID, so we append an `id` property to the effective ordering.  This
	// allows `ORDER BY col DESC, id DESC` to be satisfied by a backward
	// compound index scan.
	//
	// When all index columns are skipped (Equality), the ordering is
	// *only* by record ID — we still append it.
	if !index_ref.is_unique() && !ix_def.cols.is_empty() {
		cols.push(SortProperty {
			path: crate::exec::field_path::FieldPath::field("id"),
			direction: dir,
			collate: false,
			numeric: false,
		});
	}

	// If all required fields were stripped (all constant), the ordering
	// is trivially satisfied.
	if required.is_empty() {
		return true;
	}

	if cols.is_empty() {
		return false;
	}

	OutputOrdering::Sorted(cols).satisfies(&required)
}

// ============================================================================
// LIMIT helpers
// ============================================================================

/// Try to get the effective limit (start + limit) if both are literals.
pub(crate) fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	let limit_val = limit_expr_as_usize(limit.as_ref().map(|l| &l.0))?;
	let start_val = start.as_ref().map(|s| limit_expr_as_usize(Some(&s.0))).unwrap_or(Some(0))?;

	start_val.checked_add(limit_val)
}

/// Resolve an expression to a non-negative `usize` if it is a non-negative
/// integer or float literal.  Returns `None` for any other shape — including
/// parameters, function calls, and negative numbers.
fn limit_expr_as_usize(expr: Option<&Expr>) -> Option<usize> {
	match expr? {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	}
}

/// Returns `true` when the SELECT pipeline's ORDER BY + LIMIT will be served
/// by a bounded top-k sort (heap of size ≤ `max_order_limit_priority_queue_size`).
///
/// Used by source planning to drop eager per-sub-stream prefetch in
/// upstream scans — the heap discards most rows, so prefetching only wastes
/// memory under high concurrency.
///
/// Returns `false` when:
/// - no ORDER BY, or ORDER BY RANDOM
/// - no LIMIT, or LIMIT/START aren't literal non-negative integers
/// - `start + limit` exceeds the priority-queue threshold
/// - TEMPFILES is set (disk-backed sort is preferred, top-k path is skipped)
pub(crate) fn is_bounded_topk_downstream(
	order: Option<&crate::expr::order::Ordering>,
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
	tempfiles: bool,
	threshold: usize,
) -> bool {
	use crate::expr::order::Ordering;
	if tempfiles {
		return false;
	}
	match order {
		Some(Ordering::Order(_)) => match get_effective_limit_literal(start, limit) {
			Some(n) => n <= threshold,
			None => false,
		},
		_ => false,
	}
}

// ============================================================================
// VERSION
// ============================================================================

/// Extract version expression from VERSION clause.
///
/// Returns a physical expression that, when evaluated at execution time,
/// produces the version timestamp (u64).
pub(crate) async fn extract_version(
	version_expr: Expr,
	planner: &super::super::Planner<'_>,
) -> Result<Option<std::sync::Arc<dyn crate::exec::PhysicalExpr>>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		_ => {
			let expr = planner.physical_expr(version_expr).await?;
			Ok(Some(expr))
		}
	}
}

// ============================================================================
// COUNT() Fast-Path Detection
// ============================================================================

/// Check if a SELECT statement is eligible for the CountScan optimisation.
///
/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table> WHERE <cond> GROUP ALL`
/// with a WHERE clause and no SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// When this returns `true`, the `IndexCountScan` operator can be used.
/// At execution time it will look up the table's indexes and, if a COUNT
/// index with a matching condition exists, sum delta counts instead of
/// scanning all records.
#[allow(clippy::too_many_arguments)]
pub(crate) fn is_indexed_count_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields.
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL.
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// Must have a WHERE clause (the no-WHERE case is handled by `is_count_all_eligible`).
	if cond.is_none() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table (no record-id ranges for indexed counts).
	//
	// Note: this is a deliberately narrower set than `is_count_all_eligible`
	// below, which also accepts `Literal::RecordId(_)` and `Postfix { .. }`.
	// `IndexCountScan` walks a B-tree index, which only makes sense on a
	// whole-table source — a single record-id literal or a record-id range
	// has no index slice to walk. A `Param` is allowed because it resolves
	// to a table at runtime (the post-resolution `what[0]` is then a `Table`).
	if what.len() != 1 {
		return false;
	}
	matches!(&what[0], Expr::Table(_) | Expr::Param(_))
}

/// Returns `true` when the query matches:
///   `SELECT count() FROM <single-table-or-range> GROUP ALL`
/// with no WHERE, SPLIT, ORDER BY, FETCH, or OMIT clauses.
///
/// The CountScan operator replaces the entire Scan -> Aggregate -> Project
/// pipeline with a single `txn.count()` call on the KV key range.
#[allow(clippy::too_many_arguments)]
pub(crate) fn is_count_all_eligible(
	fields: &Fields,
	group: &Option<crate::expr::group::Groups>,
	cond: &Option<crate::expr::cond::Cond>,
	split: &Option<crate::expr::split::Splits>,
	order: &Option<crate::expr::order::Ordering>,
	fetch: &Option<crate::expr::fetch::Fetchs>,
	omit: &[Expr],
	what: &[Expr],
) -> bool {
	// Must be count()-only fields (no arguments, no other fields).
	if !fields.is_count_all_only() {
		return false;
	}
	// Must have GROUP ALL (explicit `GROUP ALL` in the AST = Some(Groups(vec![]))).
	let Some(groups) = group else {
		return false;
	};
	if !groups.is_group_all_only() {
		return false;
	}
	// No WHERE clause (index-accelerated WHERE is a follow-up).
	if cond.is_some() {
		return false;
	}
	// No SPLIT, ORDER BY, FETCH, or OMIT.
	if split.is_some() || order.is_some() || fetch.is_some() || !omit.is_empty() {
		return false;
	}
	// Source must be a single table, record-id, or param (resolving to table).
	if what.len() != 1 {
		return false;
	}
	matches!(
		&what[0],
		Expr::Table(_)
			| Expr::Literal(crate::expr::literal::Literal::RecordId(_))
			| Expr::Param(_)
			| Expr::Postfix { .. }
	)
}

/// Extract the output field names for a CountScan fast-path query.
///
/// For each `count()` field in the SELECT list, this returns the alias name
/// (if `AS alias` is present) or the default derived name (`"count"`).
///
/// Pre-condition: `is_count_all_eligible` returned `true`, so every field is
/// a `count()` function call.
pub(crate) fn extract_count_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(selector) => {
			if let Some(alias) = &selector.alias {
				vec![idiom_to_field_name(alias)]
			} else {
				vec![derive_field_name(&selector.expr)]
			}
		}
		Fields::Select(field_list) => field_list
			.iter()
			.filter_map(|f| match f {
				Field::Single(selector) => {
					if let Some(alias) = &selector.alias {
						Some(idiom_to_field_name(alias))
					} else {
						Some(derive_field_name(&selector.expr))
					}
				}
				_ => None,
			})
			.collect(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::limit::Limit;
	use crate::expr::order::{Order, OrderList, Ordering};
	use crate::expr::start::Start;
	use crate::expr::{Idiom, Part};

	fn int_lit(n: i64) -> Expr {
		Expr::Literal(Literal::Integer(n))
	}

	fn order_by_created_at_desc() -> Ordering {
		Ordering::Order(OrderList(vec![Order {
			value: Idiom(vec![Part::Field(crate::val::Strand::new("created_at"))]),
			collate: false,
			numeric: false,
			direction: false,
		}]))
	}

	#[test]
	fn effective_limit_handles_literals_and_rejects_params() {
		assert_eq!(get_effective_limit_literal(&None, &Some(Limit(int_lit(100)))), Some(100));
		assert_eq!(
			get_effective_limit_literal(&Some(Start(int_lit(20))), &Some(Limit(int_lit(100)))),
			Some(120)
		);
		// Negative integer literal is not accepted as a valid LIMIT.
		assert_eq!(get_effective_limit_literal(&None, &Some(Limit(int_lit(-1)))), None);
		// Non-literal LIMIT (param, function call, etc.) returns None so the
		// caller falls back to the full sort.
		let param = Expr::Param(crate::expr::param::Param::default());
		assert_eq!(get_effective_limit_literal(&None, &Some(Limit(param))), None);
	}

	#[test]
	fn topk_downstream_predicate_matches_design_intent() {
		let order = order_by_created_at_desc();
		// Standard case: literal LIMIT under threshold → top-k engages.
		assert!(is_bounded_topk_downstream(
			Some(&order),
			&None,
			&Some(Limit(int_lit(1000))),
			false,
			1000,
		));
		// START + LIMIT exactly at threshold still qualifies.
		assert!(is_bounded_topk_downstream(
			Some(&order),
			&Some(Start(int_lit(500))),
			&Some(Limit(int_lit(500))),
			false,
			1000,
		));
		// Over the threshold → falls back to full sort.
		assert!(!is_bounded_topk_downstream(
			Some(&order),
			&None,
			&Some(Limit(int_lit(2000))),
			false,
			1000,
		));
		// No ORDER BY → no sort to bound.
		assert!(!is_bounded_topk_downstream(None, &None, &Some(Limit(int_lit(10))), false, 1000));
		// ORDER BY RANDOM → no top-k path.
		assert!(!is_bounded_topk_downstream(
			Some(&Ordering::Random),
			&None,
			&Some(Limit(int_lit(10))),
			false,
			1000,
		));
		// No LIMIT → no top-k.
		assert!(!is_bounded_topk_downstream(Some(&order), &None, &None, false, 1000));
		// TEMPFILES → user opted into disk-backed sort.
		assert!(!is_bounded_topk_downstream(
			Some(&order),
			&None,
			&Some(Limit(int_lit(10))),
			true,
			1000,
		));
	}
}
