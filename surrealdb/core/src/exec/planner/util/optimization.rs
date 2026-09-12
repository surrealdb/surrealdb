//! Plan-time optimisation eligibility checks.
//!
//! Predicates that decide whether a SELECT can take a faster path: ORDER BY
//! pushdown into the scan operator (sort elimination) and index-covers-ordering
//! for limit pushdown, plus the small helpers for VERSION extraction and
//! effective-LIMIT computation.
//!
//! The whole-plan shapes that replace the pipeline with a single scan are
//! matched in `planner::select::fast_path`, against the plan rather than
//! clause by clause.

use super::fields::{field_path_from_expr, idiom_is_id};
use crate::err::Error;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalOrdering};

// ============================================================================
// Pushdown Eligibility
// ============================================================================

/// Check if ORDER BY is compatible with the natural KV scan direction.
///
/// Returns `true` when ORDER BY is absent, or is exactly `id ASC` or `id DESC`
/// with no COLLATE/NUMERIC modifiers. In these cases the scan already produces
/// rows in the requested order and no separate Sort operator is needed.
pub(crate) fn order_is_scan_compatible(order: Option<&LogicalOrdering>) -> bool {
	match order {
		None => true,
		Some(LogicalOrdering::Random) => false,
		Some(LogicalOrdering::Order(list)) => {
			matches!(list.as_slice(), [key] if idiom_is_id(&key.value) && !key.collate && !key.numeric)
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
	direction: crate::kvs::Direction,
	order: &LogicalOrdering,
) -> bool {
	use crate::exec::field_path_convert::field_path_from_idiom as column_path;
	use crate::exec::index::access_path::BTreeAccess;
	use crate::exec::operators::SortDirection;
	use crate::exec::ordering::{OutputOrdering, SortProperty};

	let LogicalOrdering::Order(order_list) = order else {
		return false; // Random ordering can't be satisfied by an index
	};

	// Convert ORDER BY to required SortProperty
	let required: Vec<SortProperty> = order_list
		.iter()
		.filter_map(|field| {
			field_path_from_expr(&field.value).ok().map(|path| {
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
			let paths: Vec<_> =
				ix_def.cols.iter().take(prefix.len()).filter_map(|s| column_path(s).ok()).collect();
			(prefix.len(), paths)
		}
		BTreeAccess::Equality(_) => {
			let paths: Vec<_> = ix_def.cols.iter().filter_map(|s| column_path(s).ok()).collect();
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
		crate::kvs::Direction::Forward => SortDirection::Asc,
		crate::kvs::Direction::Backward => SortDirection::Desc,
	};
	let mut cols: Vec<SortProperty> = ix_def
		.cols
		.iter()
		.skip(skip_cols)
		.filter_map(|s| {
			column_path(s).ok().map(|path| SortProperty {
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
	start: Option<&LogicalExpr>,
	limit: Option<&LogicalExpr>,
) -> Option<usize> {
	let limit_val = limit_expr_as_usize(limit?)?;
	let start_val = match start {
		Some(start) => limit_expr_as_usize(start)?,
		None => 0,
	};

	start_val.checked_add(limit_val)
}

/// Resolve an expression to a non-negative `usize` if it is a non-negative
/// integer or float literal.  Returns `None` for any other shape — including
/// parameters, function calls, and negative numbers.
fn limit_expr_as_usize(expr: &LogicalExpr) -> Option<usize> {
	match expr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n)) if *n >= 0 => Some(*n as usize),
		LogicalExpr::Literal(LogicalLiteral::Float(n)) if *n >= 0.0 => Some(*n as usize),
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
	order: Option<&LogicalOrdering>,
	start: Option<&LogicalExpr>,
	limit: Option<&LogicalExpr>,
	tempfiles: bool,
	threshold: usize,
) -> bool {
	if tempfiles {
		return false;
	}
	match order {
		Some(LogicalOrdering::Order(_)) => match get_effective_limit_literal(start, limit) {
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
/// produces the version timestamp (u64). A `NONE` literal is no version.
pub(crate) async fn extract_version(
	version_expr: LogicalExpr,
	planner: &super::super::Planner<'_>,
) -> Result<Option<std::sync::Arc<dyn crate::exec::PhysicalExpr>>, Error> {
	match version_expr {
		LogicalExpr::Literal(LogicalLiteral::None) => Ok(None),
		other => Ok(Some(planner.physical_expr(other).await?)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::LogicalOrder;

	fn int_lit(n: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(n))
	}

	fn order_by_created_at_desc() -> LogicalOrdering {
		LogicalOrdering::Order(vec![LogicalOrder {
			value: LogicalExpr::Field("created_at".into()),
			collate: false,
			numeric: false,
			direction: false,
		}])
	}

	#[test]
	fn effective_limit_handles_literals_and_rejects_params() {
		assert_eq!(get_effective_limit_literal(None, Some(&int_lit(100))), Some(100));
		assert_eq!(get_effective_limit_literal(Some(&int_lit(20)), Some(&int_lit(100))), Some(120));
		// Negative integer literal is not accepted as a valid LIMIT.
		assert_eq!(get_effective_limit_literal(None, Some(&int_lit(-1))), None);
		// Non-literal LIMIT (param, function call, etc.) returns None so the
		// caller falls back to the full sort.
		let param = LogicalExpr::Param(crate::expr::Param::default());
		assert_eq!(get_effective_limit_literal(None, Some(&param)), None);
	}

	#[test]
	fn scan_compatible_order_is_id_without_modifiers() {
		assert!(order_is_scan_compatible(None));
		assert!(!order_is_scan_compatible(Some(&LogicalOrdering::Random)));
		assert!(!order_is_scan_compatible(Some(&order_by_created_at_desc())));
		let id = |collate: bool| {
			LogicalOrdering::Order(vec![LogicalOrder {
				value: LogicalExpr::Field("id".into()),
				collate,
				numeric: false,
				direction: false,
			}])
		};
		assert!(order_is_scan_compatible(Some(&id(false))));
		assert!(!order_is_scan_compatible(Some(&id(true))));
	}

	#[test]
	fn topk_downstream_predicate_matches_design_intent() {
		let order = order_by_created_at_desc();
		// Standard case: literal LIMIT under threshold → top-k engages.
		assert!(is_bounded_topk_downstream(Some(&order), None, Some(&int_lit(1000)), false, 1000));
		// START + LIMIT exactly at threshold still qualifies.
		assert!(is_bounded_topk_downstream(
			Some(&order),
			Some(&int_lit(500)),
			Some(&int_lit(500)),
			false,
			1000,
		));
		// Over the threshold → falls back to full sort.
		assert!(!is_bounded_topk_downstream(Some(&order), None, Some(&int_lit(2000)), false, 1000));
		// No ORDER BY → no sort to bound.
		assert!(!is_bounded_topk_downstream(None, None, Some(&int_lit(10)), false, 1000));
		// ORDER BY RANDOM → no top-k path.
		assert!(!is_bounded_topk_downstream(
			Some(&LogicalOrdering::Random),
			None,
			Some(&int_lit(10)),
			false,
			1000,
		));
		// No LIMIT → no top-k.
		assert!(!is_bounded_topk_downstream(Some(&order), None, None, false, 1000));
		// TEMPFILES → user opted into disk-backed sort.
		assert!(!is_bounded_topk_downstream(Some(&order), None, Some(&int_lit(10)), true, 1000));
	}
}
