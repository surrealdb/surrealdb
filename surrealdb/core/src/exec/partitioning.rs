//! How an operator's output is divided across parallel streams.
//!
//! An operator's [`Partitioning`] describes how many independent streams its
//! output arrives on, and what determines which rows land on which. Every
//! operator declares [`Single`](Partitioning::Single) unless it says otherwise,
//! so this is metadata the planner can consult without any operator producing
//! more than one stream.
//!
//! The property exists ahead of the operators that will produce it so that
//! partition-aware planning can be added without a change at every
//! [`ExecOperator`](super::ExecOperator) implementation. Two rules constrain
//! what may ever exploit it:
//!
//! - Only a subtree whose [`AccessMode`](super::AccessMode) is `ReadOnly` may be partitioned. A
//!   write is serialised per transaction, because the transaction's savepoints are a stack with no
//!   handle, so two writers on one transaction roll back each other's work.
//! - An operator that consumes more than one partition and needs to see all rows together (a sort,
//!   a final aggregation, a `LIMIT`) must declare `Single` and merge its input, so downstream
//!   operators cannot silently observe a partial view.

/// How an operator's output rows are divided across streams.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum Partitioning {
	/// One stream carrying every row. The default, and what every operator
	/// produces today.
	#[default]
	Single,
	/// `n` streams whose union is the operator's output, with no guarantee
	/// about which rows land on which stream. Produced by splitting a scan's
	/// key range, and consumable by anything that treats rows independently.
	///
	/// Declared ahead of the operators that will construct it, so that
	/// partition-aware planning can be added without a change at every
	/// [`ExecOperator`](crate::exec::ExecOperator).
	#[allow(dead_code)]
	Arbitrary(usize),
}

impl Partitioning {
	/// The number of streams the output arrives on.
	pub(crate) fn count(&self) -> usize {
		match self {
			Self::Single => 1,
			Self::Arbitrary(n) => *n,
		}
	}

	/// Whether the output arrives on a single stream, which is the condition an
	/// operator that must see every row together requires of its input.
	pub(crate) fn is_single(&self) -> bool {
		self.count() <= 1
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn single_is_the_default() {
		assert_eq!(Partitioning::default(), Partitioning::Single);
	}

	#[test]
	fn count_reports_the_number_of_streams() {
		assert_eq!(Partitioning::Single.count(), 1);
		assert_eq!(Partitioning::Arbitrary(8).count(), 8);
	}

	#[test]
	fn a_degenerate_split_still_counts_as_single() {
		// A planner that divides a range into one piece has not partitioned
		// anything, so an operator requiring a merged input must accept it.
		assert!(Partitioning::Arbitrary(1).is_single());
		assert!(!Partitioning::Arbitrary(2).is_single());
	}
}
