//! Shared types and utilities for sort operators.

use std::cmp::Ordering;
use std::sync::Arc;

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

/// A single field in an ORDER BY clause
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
