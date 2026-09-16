//! Shared types and utilities for sort operators.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::exec::PhysicalExpr;
use crate::exec::field_path::FieldPath;
use crate::val::Value;

/// Sort direction for ORDER BY
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
	/// Ascending order (default)
	#[default]
	Asc,
	/// Descending order
	Desc,
}

/// A single field in an ORDER BY clause that evaluates an expression.
///
/// This is the legacy/original approach where Sort evaluates expressions.
/// For the new consolidated approach, use `SortKey` instead.
#[derive(Debug, Clone)]
pub struct OrderByField {
	/// Expression to evaluate for each row
	pub expr: Arc<dyn PhysicalExpr>,
	/// Sort direction
	pub direction: SortDirection,
	/// Whether to use collation-aware string comparison
	pub collate: bool,
	/// Whether to use numeric string comparison
	pub numeric: bool,
}

/// A single field in an ORDER BY clause that references a field path.
///
/// This is the new consolidated approach where:
/// - Simple field paths (a.b.c) are extracted directly using `FieldPath`
/// - Complex expressions are pre-computed by a Compute operator
/// - Sort becomes a pure comparison operation
///
/// Benefits:
/// - No duplicate expression evaluation
/// - Cleaner separation of concerns
/// - Type-safe field path extraction (no execution required)
///
/// `PartialEq`/`Eq` compare the full specification (path, direction, collate,
/// numeric); the TopK threshold pushdown install guard relies on this to
/// verify the sort plan matches the probe built at scan-planning time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
	/// Path to extract for sorting
	pub path: FieldPath,
	/// Sort direction
	pub direction: SortDirection,
	/// Whether to use collation-aware string comparison
	pub collate: bool,
	/// Whether to use numeric string comparison
	pub numeric: bool,
}

impl SortKey {
	/// Create a new SortKey with default options (ASC, no collate, no numeric).
	pub fn new(path: FieldPath) -> Self {
		Self {
			path,
			direction: SortDirection::Asc,
			collate: false,
			numeric: false,
		}
	}
}

/// Compare two sort key values, respecting collate and numeric modes.
///
/// This delegates to `Value::compare` which handles type coercion
/// and null ordering consistently.
#[inline]
pub fn compare_values(a: &Value, b: &Value, collate: bool, numeric: bool) -> Ordering {
	// Use Value::compare with empty path since we're comparing direct values
	a.compare(b, &[], collate, numeric).unwrap_or(Ordering::Equal)
}

/// Compare two sets of sort keys according to the order-by specification.
///
/// This compares each key pair in order, applying direction (ASC/DESC) and
/// collate/numeric modes as specified in each field.
pub fn compare_keys(keys_a: &[Value], keys_b: &[Value], order_by: &[OrderByField]) -> Ordering {
	for (i, field) in order_by.iter().enumerate() {
		let a = &keys_a[i];
		let b = &keys_b[i];

		let ordering = compare_values(a, b, field.collate, field.numeric);
		let ordering = match field.direction {
			SortDirection::Asc => ordering,
			SortDirection::Desc => ordering.reverse(),
		};

		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	Ordering::Equal
}

/// Compare two pre-extracted key tuples using `SortKey` directions / modes.
///
/// Mirrors [`compare_keys`] for the `SortKey`-keyed sort path. All `ByKey`
/// sort operators extract each row's keys exactly once and compare the
/// cached tuples with this function: `SortTopKByKey` / `SortByKey` keep them
/// in memory, `ExternalSortByKey` serialises them to disk alongside each row.
///
/// Generic over [`Borrow<Value>`] so callers can compare freshly extracted
/// `Cow<Value>` keys against cached owned keys without cloning first.
pub fn compare_keys_by_sort_key<A: Borrow<Value>, B: Borrow<Value>>(
	keys_a: &[A],
	keys_b: &[B],
	sort_keys: &[SortKey],
) -> Ordering {
	for (i, key) in sort_keys.iter().enumerate() {
		let a = keys_a[i].borrow();
		let b = keys_b[i].borrow();

		let ordering = compare_values(a, b, key.collate, key.numeric);
		let ordering = match key.direction {
			SortDirection::Asc => ordering,
			SortDirection::Desc => ordering.reverse(),
		};

		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	Ordering::Equal
}

/// The ORDER BY keys of `values`, as one column per field.
///
/// `column_major` chooses the shape, exactly as it does in the
/// projection operators: where no key has an effect to reorder, each key is
/// evaluated for the whole batch in one call, which lets a key reading through
/// a lookup overlap its fetches. Where one may, the keys of a row are evaluated
/// together, in the order the statement wrote them, so that two keys drawing
/// from the same sequence draw in written order rather than a column at a time.
///
/// A sort is where this is easiest to miss: the keys are not in the output, so
/// a reordering shows only in which rows the comparison puts first. The keys a
/// `Compute` pre-computes are judged at that operator instead; these are the
/// ones the sort evaluates itself.
pub(crate) async fn evaluate_key_columns(
	order_by: &[OrderByField],
	values: &[Value],
	ctx: crate::exec::EvalContext<'_>,
	column_major: bool,
) -> crate::exec::FlowResult<Vec<Vec<Value>>> {
	let mut key_columns: Vec<Vec<Value>> = Vec::with_capacity(order_by.len());
	if column_major {
		for field in order_by {
			key_columns.push(field.expr.evaluate_batch(ctx.clone(), values).await?);
		}
		return Ok(key_columns);
	}
	key_columns.resize_with(order_by.len(), || Vec::with_capacity(values.len()));
	for value in values {
		let row_ctx = ctx.with_value_and_doc(value);
		for (field, column) in order_by.iter().zip(key_columns.iter_mut()) {
			column.push(field.expr.evaluate(row_ctx.clone()).await?);
		}
	}
	Ok(key_columns)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compare_values_integers() {
		let a = Value::from(1);
		let b = Value::from(2);
		assert_eq!(compare_values(&a, &b, false, false), Ordering::Less);
		assert_eq!(compare_values(&b, &a, false, false), Ordering::Greater);
		assert_eq!(compare_values(&a, &a, false, false), Ordering::Equal);
	}

	#[test]
	fn test_compare_values_strings() {
		let a = Value::from("apple");
		let b = Value::from("banana");
		assert_eq!(compare_values(&a, &b, false, false), Ordering::Less);
	}

	#[test]
	fn test_compare_values_nulls() {
		let a = Value::None;
		let b = Value::from(1);
		// None is less than any value
		assert_eq!(compare_values(&a, &b, false, false), Ordering::Less);
		assert_eq!(compare_values(&b, &a, false, false), Ordering::Greater);
	}
}
