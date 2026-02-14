//! Output ordering metadata for execution operators.
//!
//! Each operator can declare the ordering of its output stream via
//! [`OutputOrdering`]. The planner uses this to eliminate redundant
//! Sort operators when the input already produces data in the
//! required order.

use crate::exec::field_path::FieldPath;
use crate::exec::operators::SortDirection;

/// Describes the sort property of a single column in an output ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortProperty {
	/// The field path that the data is sorted by.
	pub path: FieldPath,
	/// The direction of the sort (ascending or descending).
	pub direction: SortDirection,
}

/// Describes the ordering guarantee of an operator's output stream.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OutputOrdering {
	/// No ordering guarantee -- the output may arrive in any order.
	#[default]
	Unordered,
	/// The output is sorted by the given columns, in the given order.
	/// The first element is the primary sort key, the second is the
	/// secondary sort key, and so on.
	Sorted(Vec<SortProperty>),
}

impl OutputOrdering {
	/// Returns `true` if this ordering satisfies the `required` ordering.
	///
	/// An ordering satisfies a requirement when the output ordering is a
	/// prefix-or-equal match of the required ordering. For example, if the
	/// output is sorted by `[a ASC, b ASC, c ASC]` and the requirement is
	/// `[a ASC, b ASC]`, the requirement is satisfied because the output
	/// is sorted by a superset of the required columns.
	///
	/// An `Unordered` output never satisfies a non-empty requirement.
	/// Any ordering satisfies an empty requirement.
	pub fn satisfies(&self, required: &[SortProperty]) -> bool {
		if required.is_empty() {
			return true;
		}
		match self {
			OutputOrdering::Unordered => false,
			OutputOrdering::Sorted(provided) => {
				if provided.len() < required.len() {
					return false;
				}
				provided.iter().zip(required.iter()).all(|(p, r)| p == r)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn prop(name: &str, dir: SortDirection) -> SortProperty {
		SortProperty {
			path: FieldPath::field(name),
			direction: dir,
		}
	}

	#[test]
	fn test_empty_requirement_always_satisfied() {
		assert!(OutputOrdering::Unordered.satisfies(&[]));
		assert!(OutputOrdering::Sorted(vec![prop("a", SortDirection::Asc)]).satisfies(&[]));
	}

	#[test]
	fn test_unordered_never_satisfies_nonempty() {
		let req = vec![prop("a", SortDirection::Asc)];
		assert!(!OutputOrdering::Unordered.satisfies(&req));
	}

	#[test]
	fn test_exact_match() {
		let ordering = OutputOrdering::Sorted(vec![
			prop("a", SortDirection::Asc),
			prop("b", SortDirection::Desc),
		]);
		let req = vec![prop("a", SortDirection::Asc), prop("b", SortDirection::Desc)];
		assert!(ordering.satisfies(&req));
	}

	#[test]
	fn test_superset_satisfies_prefix() {
		let ordering = OutputOrdering::Sorted(vec![
			prop("a", SortDirection::Asc),
			prop("b", SortDirection::Asc),
			prop("c", SortDirection::Asc),
		]);
		let req = vec![prop("a", SortDirection::Asc), prop("b", SortDirection::Asc)];
		assert!(ordering.satisfies(&req));
	}

	#[test]
	fn test_subset_does_not_satisfy() {
		let ordering = OutputOrdering::Sorted(vec![prop("a", SortDirection::Asc)]);
		let req = vec![prop("a", SortDirection::Asc), prop("b", SortDirection::Asc)];
		assert!(!ordering.satisfies(&req));
	}

	#[test]
	fn test_direction_mismatch() {
		let ordering = OutputOrdering::Sorted(vec![prop("a", SortDirection::Asc)]);
		let req = vec![prop("a", SortDirection::Desc)];
		assert!(!ordering.satisfies(&req));
	}

	#[test]
	fn test_path_mismatch() {
		let ordering = OutputOrdering::Sorted(vec![prop("a", SortDirection::Asc)]);
		let req = vec![prop("b", SortDirection::Asc)];
		assert!(!ordering.satisfies(&req));
	}
}
