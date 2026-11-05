//! Dialects, phases and legality targets: what a pattern is typed by, what a phase boundary
//! requires, and why an op was eliminated.
//!
//! A pattern is typed by the dialect of the op it roots at, so a logical pattern cannot root at a
//! physical op. A [`LegalityTarget`] names the dialects a phase may leave in the container;
//! conversion stops only when no op outside the target remains.

use std::fmt::Debug;
use std::hash::Hash;

use super::ids::OpId;
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysTag;

/// A dialect marker, the type a pattern set is indexed by.
pub(crate) trait Dialect: 'static {
	/// The dialect's name as the printer writes it: `expr`, `logical` or `phys`.
	const NAME: &'static str;
	/// The payload-free identity of the dialect's ops.
	type Tag: Copy + Eq + Hash + Debug + 'static;

	/// The dialect's tag of `kind`, or `None` when `kind` belongs to another dialect.
	fn tag_of(kind: &OpKind) -> Option<Self::Tag>;
}

/// The expression dialect.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprDialect;

/// The logical dialect.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogicalDialect;

/// The physical dialect.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PhysDialect;

impl Dialect for ExprDialect {
	const NAME: &'static str = "expr";
	type Tag = ExprTag;

	fn tag_of(kind: &OpKind) -> Option<ExprTag> {
		match kind {
			OpKind::Expr(op) => Some(op.tag()),
			OpKind::Logical(_) | OpKind::Phys(_) => None,
		}
	}
}

impl Dialect for LogicalDialect {
	const NAME: &'static str = "logical";
	type Tag = LogTag;

	fn tag_of(kind: &OpKind) -> Option<LogTag> {
		match kind {
			OpKind::Logical(op) => Some(op.tag()),
			OpKind::Expr(_) | OpKind::Phys(_) => None,
		}
	}
}

impl Dialect for PhysDialect {
	const NAME: &'static str = "phys";
	type Tag = PhysTag;

	fn tag_of(kind: &OpKind) -> Option<PhysTag> {
		match kind {
			OpKind::Phys(op) => Some(op.tag()),
			OpKind::Expr(_) | OpKind::Logical(_) => None,
		}
	}
}

/// A set of dialects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DialectSet(u8);

impl DialectSet {
	/// The expression dialect alone.
	pub(crate) const EXPR: Self = Self(0b001);
	/// The logical dialect alone.
	pub(crate) const LOGICAL: Self = Self(0b010);
	/// The physical dialect alone.
	pub(crate) const PHYS: Self = Self(0b100);

	/// The dialects in either set.
	pub(crate) const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	/// Whether every dialect of `other` is in this set.
	pub(crate) const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}
}

/// The dialects a phase may leave in a container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LegalityTarget {
	pub(crate) name: &'static str,
	pub(crate) legal: DialectSet,
}

/// After the logical phase: expression and logical ops.
pub(crate) const LOGICAL_PHASE: LegalityTarget = LegalityTarget {
	name: "logical",
	legal: DialectSet::EXPR.union(DialectSet::LOGICAL),
};

/// After the physical phase: expression and physical ops.
pub(crate) const PHYSICAL_PHASE: LegalityTarget = LegalityTarget {
	name: "physical",
	legal: DialectSet::EXPR.union(DialectSet::PHYS),
};

/// How far a container has progressed. Ordered: a later phase is greater.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Phase {
	/// Built from the bound plan, before any rewrite.
	Bound,
	/// Rewritten in the logical dialect.
	Logical,
	/// Converted to the physical dialect and placed.
	Physical,
	/// Lowered to a program.
	Lowered,
}

/// Why an op left the container without a replacement computing its results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum EliminationReason {
	/// Its results were unused and it had no effect.
	DeadPure,
	/// An access path answers what the op computed.
	AnsweredByAccessPath,
	/// A plan-time fold replaced it with a constant.
	FoldedConstant,
	/// It stands after an op certain to raise, and the raise is the statement's outcome.
	DeadBeforeCertainError,
	/// It computes the value of another op, which stands for it.
	MergedInto(OpId),
	/// A read the principal's permissions make invisible.
	InvisibleToPrincipal,
	/// A computed field no read of the statement refers to.
	Unreferenced,
	/// A column of a projection layer that no op reads, or a layer left with no column.
	DeadLayerColumn,
	/// It follows a terminator in its block and never runs.
	Unreachable,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_phases_are_ordered_by_progress() {
		assert!(Phase::Bound < Phase::Logical);
		assert!(Phase::Logical < Phase::Physical);
		assert!(Phase::Physical < Phase::Lowered);
	}

	#[test]
	fn each_phase_target_admits_its_dialects_only() {
		assert!(LOGICAL_PHASE.legal.contains(DialectSet::EXPR));
		assert!(LOGICAL_PHASE.legal.contains(DialectSet::LOGICAL));
		assert!(!LOGICAL_PHASE.legal.contains(DialectSet::PHYS));
		assert!(PHYSICAL_PHASE.legal.contains(DialectSet::EXPR));
		assert!(PHYSICAL_PHASE.legal.contains(DialectSet::PHYS));
		assert!(!PHYSICAL_PHASE.legal.contains(DialectSet::LOGICAL));
	}
}
