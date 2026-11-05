//! Shared types and utilities for sort operators.

use std::cmp::Ordering;

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

/// Compare two sort key values, respecting collate and numeric modes.
///
/// This delegates to `Value::compare` which handles type coercion
/// and null ordering consistently.
#[inline]
pub fn compare_values(a: &Value, b: &Value, collate: bool, numeric: bool) -> Ordering {
	// Use Value::compare with empty path since we're comparing direct values
	a.compare(b, &[], collate, numeric).unwrap_or(Ordering::Equal)
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
