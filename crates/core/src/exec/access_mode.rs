//! Access mode classification for execution plans and expressions.
//!
//! The access mode determines whether a plan or expression performs mutations.
//! This is critical for dependency analysis: a `SELECT` containing a mutation
//! subquery must be treated as a barrier, not a pure read.
//!
//! Example: `SELECT *, (UPSERT person) FROM person` is syntactically a SELECT
//! but has `AccessMode::ReadWrite` because the subquery mutates data.

/// Access mode for a plan or expression.
///
/// Used to determine if an operation performs mutations, which affects:
/// - Dependency ordering (mutations are barriers)
/// - Transaction mode selection (read-only vs read-write)
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum AccessMode {
	/// Only reads data, no side effects.
	/// Can run in parallel with other reads.
	#[default]
	ReadOnly,
	/// May write data or have side effects.
	/// Acts as a barrier in dependency ordering.
	ReadWrite,
}

impl AccessMode {
	/// Combine two access modes, taking the more restrictive.
	///
	/// Used to propagate access mode up the plan tree:
	/// - If any child is ReadWrite, the parent is ReadWrite
	/// - Only if all children are ReadOnly is the parent ReadOnly
	///
	/// # Examples
	///
	/// ```ignore
	/// use crate::exec::AccessMode;
	///
	/// assert_eq!(AccessMode::ReadOnly.combine(AccessMode::ReadOnly), AccessMode::ReadOnly);
	/// assert_eq!(AccessMode::ReadOnly.combine(AccessMode::ReadWrite), AccessMode::ReadWrite);
	/// assert_eq!(AccessMode::ReadWrite.combine(AccessMode::ReadOnly), AccessMode::ReadWrite);
	/// ```
	#[inline]
	pub fn combine(self, other: Self) -> Self {
		match (self, other) {
			(Self::ReadOnly, Self::ReadOnly) => Self::ReadOnly,
			_ => Self::ReadWrite,
		}
	}

	/// Returns true if this is a read-only access mode.
	#[inline]
	pub fn is_read_only(self) -> bool {
		matches!(self, Self::ReadOnly)
	}

	/// Returns true if this mode may perform writes.
	#[inline]
	pub fn is_read_write(self) -> bool {
		matches!(self, Self::ReadWrite)
	}
}

/// Helper trait for combining access modes from iterators.
pub trait CombineAccessModes: Iterator<Item = AccessMode> {
	/// Combine all access modes in the iterator.
	///
	/// Returns `ReadOnly` if all items are read-only, otherwise `ReadWrite`.
	fn combine_all(self) -> AccessMode;
}

impl<I: Iterator<Item = AccessMode>> CombineAccessModes for I {
	fn combine_all(self) -> AccessMode {
		self.fold(AccessMode::ReadOnly, AccessMode::combine)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_combine_read_only() {
		assert_eq!(AccessMode::ReadOnly.combine(AccessMode::ReadOnly), AccessMode::ReadOnly);
	}

	#[test]
	fn test_combine_read_write() {
		assert_eq!(AccessMode::ReadOnly.combine(AccessMode::ReadWrite), AccessMode::ReadWrite);
		assert_eq!(AccessMode::ReadWrite.combine(AccessMode::ReadOnly), AccessMode::ReadWrite);
		assert_eq!(AccessMode::ReadWrite.combine(AccessMode::ReadWrite), AccessMode::ReadWrite);
	}

	#[test]
	fn test_default() {
		assert_eq!(AccessMode::default(), AccessMode::ReadOnly);
	}

	#[test]
	fn test_is_read_only() {
		assert!(AccessMode::ReadOnly.is_read_only());
		assert!(!AccessMode::ReadWrite.is_read_only());
	}

	#[test]
	fn test_is_read_write() {
		assert!(!AccessMode::ReadOnly.is_read_write());
		assert!(AccessMode::ReadWrite.is_read_write());
	}

	#[test]
	fn test_combine_all() {
		let modes = vec![AccessMode::ReadOnly, AccessMode::ReadOnly];
		assert_eq!(modes.into_iter().combine_all(), AccessMode::ReadOnly);

		let modes = vec![AccessMode::ReadOnly, AccessMode::ReadWrite, AccessMode::ReadOnly];
		assert_eq!(modes.into_iter().combine_all(), AccessMode::ReadWrite);

		let modes: Vec<AccessMode> = vec![];
		assert_eq!(modes.into_iter().combine_all(), AccessMode::ReadOnly);
	}
}
