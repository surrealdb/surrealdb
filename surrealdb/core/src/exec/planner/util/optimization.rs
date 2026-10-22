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
use crate::expr::logical::LogicalOrdering;

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
/// Matches the ORDER BY against
/// [`scan_ordering`](crate::exec::index::access_path::scan_ordering), the
/// ordering `IndexScan::output_ordering()` claims, so the two cannot disagree.
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
	let equality_field_paths: Vec<_> = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => ix_def.cols.iter().take(prefix.len()).filter_map(|s| column_path(s).ok()).collect(),
		BTreeAccess::Equality(_) => {
			ix_def.cols.iter().filter_map(|s| column_path(s).ok()).collect()
		}
		BTreeAccess::Range {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => {
			vec![]
		}
	};

	// Strip leading ORDER BY fields that reference equality-pinned columns.
	// These columns have a single constant value, so any direction trivially
	// satisfies the ordering requirement for them.
	let required: Vec<SortProperty> =
		required.into_iter().skip_while(|prop| equality_field_paths.contains(&prop.path)).collect();

	// The ordering the scan itself will claim.
	let cols = crate::exec::index::access_path::scan_ordering(index_ref, access, direction);

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
// VERSION
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::{LogicalExpr, LogicalOrder};

	fn order_by_created_at_desc() -> LogicalOrdering {
		LogicalOrdering::Order(vec![LogicalOrder {
			value: LogicalExpr::Field("created_at".into()),
			collate: false,
			numeric: false,
			direction: false,
		}])
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
}
