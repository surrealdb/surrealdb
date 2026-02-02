//! Shared types and utilities for sort operators.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::exec::field_path::FieldPath;
use crate::exec::PhysicalExpr;
use crate::val::Value;

/// Sort direction for ORDER BY
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
	/// Ascending order (default)
	Asc,
	/// Descending order
	Desc,
}

impl Default for SortDirection {
	fn default() -> Self {
		Self::Asc
	}
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
#[derive(Debug, Clone)]
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

	/// Create a SortKey from a simple field name.
	pub fn from_field(name: impl Into<String>) -> Self {
		Self::new(FieldPath::field(name))
	}

	/// Set the sort direction to descending.
	pub fn desc(mut self) -> Self {
		self.direction = SortDirection::Desc;
		self
	}

	/// Enable collation-aware string comparison.
	pub fn with_collate(mut self) -> Self {
		self.collate = true;
		self
	}

	/// Enable numeric string comparison.
	pub fn with_numeric(mut self) -> Self {
		self.numeric = true;
		self
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

/// Compare two records using SortKey specifications.
///
/// This extracts values from records using FieldPath (which supports
/// nested field access like `user.address.city`) and compares them.
pub fn compare_records_by_keys(
	record_a: &Value,
	record_b: &Value,
	sort_keys: &[SortKey],
) -> Ordering {
	for key in sort_keys {
		let a = key.path.extract(record_a);
		let b = key.path.extract(record_b);

		let ordering = compare_values(&a, &b, key.collate, key.numeric);
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
