//! The kind of an operation: its dialect, its variant and that variant's attributes.
//!
//! [`OpKind`] is the closed union of the three dialects. [`OpTag`] is its payload-free twin:
//! patterns index on it and the printer names an op by its mnemonic. The shapes the interface
//! methods of an op answer with, [`Arity`], [`IsaCaps`], [`NativeLowering`] and [`Terminator`],
//! are defined here so every dialect shares one definition.

#![deny(clippy::wildcard_enum_match_arm)]

use super::analysis::cx::structural_cx;
use super::analysis::{fallibility, phys_effects};
use super::core::facts::FactCache;
use super::core::ir::Ir;
use super::expr::ops::{ExprOp, ExprTag};
use super::iface::catalog::EffectCx;
use super::iface::effects::OpEffects;
use super::iface::fallibility::Fallibility;
use super::iface::identity::AttrDigest;
use super::iface::ids::{OpId, RegionId};
use super::iface::region_kind::{RegionKind, RegionKinds, RegionProperties};
use super::logical::ops::{LogOp, LogTag};
use super::phys::ops::{PhysOp, PhysTag};
pub(crate) use crate::exec::vm::LeafClass;

/// One operation's kind: the dialect it belongs to, its variant and its attributes.
///
/// Closed: there are exactly three dialects. Carries no `Eq` or `Hash`, because some payloads
/// embed types of the language and the planner that implement neither; identity is the
/// structural digest of the attributes, confirmed by [`OpKind::attrs_eq`].
#[derive(Clone, Debug)]
pub(crate) enum OpKind {
	Expr(ExprOp),
	Logical(LogOp),
	Phys(PhysOp),
}

impl OpKind {
	/// Returns a reference to the expression op if `self` is [`OpKind::Expr`].
	#[inline]
	pub(crate) fn as_expr(&self) -> Option<&ExprOp> {
		if let OpKind::Expr(op) = self {
			Some(op)
		} else {
			None
		}
	}

	/// Returns a reference to the logical op if `self` is [`OpKind::Logical`].
	#[inline]
	pub(crate) fn as_logical(&self) -> Option<&LogOp> {
		if let OpKind::Logical(op) = self {
			Some(op)
		} else {
			None
		}
	}

	/// Returns a reference to the physical op if `self` is [`OpKind::Phys`].
	#[inline]
	pub(crate) fn as_phys(&self) -> Option<&PhysOp> {
		if let OpKind::Phys(op) = self {
			Some(op)
		} else {
			None
		}
	}

	/// The op ids the attributes of the op hold, which name other ops of its container.
	pub(crate) fn attr_op_ids_mut(&mut self) -> impl Iterator<Item = &mut OpId> {
		match self {
			OpKind::Phys(PhysOp::Pipeline(attrs)) => attrs.logical.as_mut(),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		}
		.into_iter()
	}

	/// The payload-free identity of the op. Total.
	pub(crate) fn tag(&self) -> OpTag {
		match self {
			OpKind::Expr(op) => OpTag::Expr(op.tag()),
			OpKind::Logical(op) => OpTag::Logical(op.tag()),
			OpKind::Phys(op) => OpTag::Phys(op.tag()),
		}
	}

	/// The op's own effects, excluding the ops of its regions.
	///
	/// A call answers by the call rule of its callee kind, never by the callee's body. A physical
	/// op answers through the logical ops it stands for.
	pub(crate) fn local_effects(&self, cx: &EffectCx<'_>) -> OpEffects {
		match self {
			OpKind::Expr(op) => op.local_effects(cx),
			OpKind::Logical(op) => op.local_effects(cx),
			OpKind::Phys(op) => phys_effects::of(op, cx),
		}
	}

	/// The op's own failure classes. Total by type: no op is without an answer.
	pub(crate) fn fallibility(&self, cx: &EffectCx<'_>) -> Fallibility {
		match self {
			OpKind::Expr(op) => op.fallibility(cx),
			OpKind::Logical(op) => op.fallibility(cx),
			OpKind::Phys(op) => phys_effects::of(op, cx).fallibility,
		}
	}

	/// The structural digest of the op's identity-bearing attributes: never labels, locations,
	/// provenance or placement.
	pub(crate) fn attr_digest(&self) -> AttrDigest {
		match self {
			OpKind::Expr(op) => op.attr_digest(),
			OpKind::Logical(op) => op.attr_digest(),
			OpKind::Phys(op) => op.attr_digest(),
		}
	}

	/// Whether `self` and `other` have equal identity-bearing attributes, by the dialect's own
	/// comparison of every attribute ([`ExprOp::attrs_eq`], [`LogOp::attrs_eq`],
	/// [`PhysOp::attrs_eq`]). Ops of two dialects never do. Equal digests are a candidate match;
	/// whatever treats two ops as one value confirms it with this.
	pub(crate) fn attrs_eq(&self, other: &OpKind) -> bool {
		match (self, other) {
			(OpKind::Expr(a), OpKind::Expr(b)) => a.attrs_eq(b),
			(OpKind::Logical(a), OpKind::Logical(b)) => a.attrs_eq(b),
			(OpKind::Phys(a), OpKind::Phys(b)) => a.attrs_eq(b),
			(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_), _) => false,
		}
	}

	/// Whether the op ends the block it stands last in.
	pub(crate) fn is_terminator(&self) -> bool {
		match self {
			OpKind::Expr(op) => op.is_terminator(),
			OpKind::Logical(op) => op.is_terminator(),
			OpKind::Phys(op) => op.is_terminator(),
		}
	}

	/// How the op ends its block, or `None` for an op that is not a terminator.
	pub(crate) fn terminator(&self) -> Option<Terminator> {
		match self {
			OpKind::Expr(op) => op.terminator(),
			OpKind::Logical(op) => op.terminator(),
			OpKind::Phys(op) => op.terminator(),
		}
	}

	/// The kind of each of the op's regions, in position order.
	pub(crate) fn region_kinds(&self) -> RegionKinds {
		match self {
			OpKind::Expr(op) => op.region_kinds(),
			OpKind::Logical(op) => op.region_kinds(),
			OpKind::Phys(op) => op.region_kinds(),
		}
	}

	/// Whether each region the op owns must be of exactly the kind [`OpKind::region_kinds`] names,
	/// with no position twin standing in. Holds for an op whose attributes state which twin its
	/// regions are (`value` on a physical `If` and `Loop`), so the regions cannot disagree with
	/// them.
	pub(crate) fn region_kinds_exact(&self) -> bool {
		matches!(self, OpKind::Phys(PhysOp::If { .. } | PhysOp::Loop { .. }))
	}

	/// The kinds the op may own after the regions [`OpKind::region_kinds`] names: the clause
	/// regions a write stage, an assignment or a live query hosts, and the body of a user-defined
	/// function a call holds until the inlining pass runs the body in the caller or drops it. The
	/// op's own verifier checks their order.
	pub(crate) fn tail_region_kinds(&self) -> &'static [RegionKind] {
		match self {
			OpKind::Expr(ExprOp::Call(_)) => &[RegionKind::FunctionBody],
			OpKind::Expr(_) => &[],
			OpKind::Logical(op) => crate::exec::ir::logical::verify::tail_kinds(op),
			OpKind::Phys(op) => op.tail_region_kinds(),
		}
	}

	/// The position in [`OpKind::region_kinds`] of the entry that stands for any number of regions
	/// of its kind, none included, for an op whose attributes do not count those regions.
	pub(crate) fn open_region_position(&self) -> Option<usize> {
		match self {
			OpKind::Expr(_) => None,
			OpKind::Logical(op) => crate::exec::ir::logical::verify::open_position(op),
			OpKind::Phys(op) => op.open_region_position(),
		}
	}

	/// The operand and result counts the op accepts.
	pub(crate) fn arity(&self) -> Arity {
		match self {
			OpKind::Expr(op) => op.arity(),
			OpKind::Logical(op) => op.arity(),
			OpKind::Phys(op) => op.arity(),
		}
	}

	/// How the op reaches the interpreter on the target `isa` describes, with the callees of a
	/// call resolved against `registry`.
	pub(crate) fn lowers_natively(
		&self,
		isa: IsaCaps,
		registry: &crate::exec::function::FunctionRegistry,
	) -> NativeLowering {
		match self {
			OpKind::Expr(op) => op.lowers_natively(isa, registry),
			OpKind::Logical(op) => op.lowers_natively(isa),
			OpKind::Phys(op) => op.lowers_natively(isa),
		}
	}

	/// Whether the region at position `region` is isolated from above: a property of the
	/// region's kind. A position the op has no region at is not isolated.
	pub(crate) fn isolated_from_above(&self, region: usize) -> bool {
		self.region_kinds().get(region).is_some_and(|kind| kind.isolated_from_above())
	}

	/// Whether `op`, an op of `ir`, may raise.
	///
	/// The answer is the transitive one, over the ops of the op's regions as well: never the local
	/// declaration, which on a region-owning op omits them. Calls are judged against `registry`.
	pub(crate) fn may_raise(
		&self,
		op: OpId,
		ir: &Ir,
		facts: &mut FactCache,
		registry: &crate::exec::function::FunctionRegistry,
	) -> bool {
		!fallibility::of_with(ir, facts, &structural_cx(registry, ir), op).is_infallible()
	}
}

/// The payload-free identity of an op kind.
///
/// Ordered by dialect (expression, logical, physical), then by the dialect's own tag order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum OpTag {
	Expr(ExprTag),
	Logical(LogTag),
	Phys(PhysTag),
}

const ALL_LEN: usize = ExprTag::ALL.len() + LogTag::ALL.len() + PhysTag::ALL.len();

/// The concatenation of the three dialects' tag lists, built in a constant context.
const ALL_TAGS: [OpTag; ALL_LEN] = {
	let mut out = [OpTag::Expr(ExprTag::ALL[0]); ALL_LEN];
	let mut next = 0;
	let mut i = 0;
	while i < ExprTag::ALL.len() {
		out[next] = OpTag::Expr(ExprTag::ALL[i]);
		next += 1;
		i += 1;
	}
	i = 0;
	while i < LogTag::ALL.len() {
		out[next] = OpTag::Logical(LogTag::ALL[i]);
		next += 1;
		i += 1;
	}
	i = 0;
	while i < PhysTag::ALL.len() {
		out[next] = OpTag::Phys(PhysTag::ALL[i]);
		next += 1;
		i += 1;
	}
	out
};

impl OpTag {
	/// Every tag of every dialect, in tag order.
	pub(crate) const ALL: &'static [OpTag] = &ALL_TAGS;

	/// The name the printer writes and the parser reads: `<dialect>.<name>`.
	pub(crate) const fn mnemonic(self) -> &'static str {
		match self {
			OpTag::Expr(tag) => tag.mnemonic(),
			OpTag::Logical(tag) => tag.mnemonic(),
			OpTag::Phys(tag) => tag.mnemonic(),
		}
	}
}

/// How many operands and results an op has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Arity {
	pub(crate) operands: OperandRule,
	pub(crate) results: u16,
}

/// The rule an op's operand count follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OperandRule {
	/// Exactly this many operands.
	Exactly(u16),
	/// At least this many operands.
	AtLeast(u16),
	/// Operands in groups, each group of one of the listed sizes.
	Grouped(&'static [u16]),
}

/// The instruction-set capabilities of the target a program is lowered for.
///
/// One capability function answers, per op, whether the op lowers natively on this target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct IsaCaps(pub(crate) u32);

/// How an op reaches the interpreter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeLowering {
	/// The op lowers to instructions the interpreter runs, as a leaf of this class.
	Native {
		leaf_class: LeafClass,
	},
	/// The op is a call of a unit the program holds.
	UnitCall,
}

/// How an op ends the block it stands last in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Terminator {
	/// Ends the region with the terminator op's operands: the results of the region's owning op,
	/// or one loop iteration's contribution to the sink.
	Yield,
	/// `RETURN`: ends `target`, the nearest enclosing region that catches a return. Never a region
	/// above the enclosing statement.
	Return {
		target: RegionId,
	},
	/// `BREAK`: leaves the loop region `target`.
	Break {
		target: RegionId,
	},
	/// `CONTINUE`: ends the current iteration of the loop region `target`.
	Next {
		target: RegionId,
	},
	/// A non-continuing raise: `THROW`, a folded error, a barrier-stopped `BREAK` or `CONTINUE`,
	/// a misplaced transaction statement.
	Raise,
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::{ExprTag, LogTag, OpKind, OpTag, PhysTag};
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::logical::ops::LogOp;
	use crate::exec::ir::phys::ops::PhysOp;

	#[test]
	fn all_is_the_concatenation_of_the_dialects() {
		let expected = ExprTag::ALL.len() + LogTag::ALL.len() + PhysTag::ALL.len();
		assert_eq!(OpTag::ALL.len(), expected);
		let expr = OpTag::ALL.iter().filter(|t| matches!(t, OpTag::Expr(_))).count();
		let logical = OpTag::ALL.iter().filter(|t| matches!(t, OpTag::Logical(_))).count();
		let phys = OpTag::ALL.iter().filter(|t| matches!(t, OpTag::Phys(_))).count();
		assert_eq!(
			(expr, logical, phys),
			(ExprTag::ALL.len(), LogTag::ALL.len(), PhysTag::ALL.len())
		);
	}

	#[test]
	fn all_is_sorted_and_mnemonics_are_unique_across_dialects() {
		assert!(OpTag::ALL.windows(2).all(|pair| pair[0] < pair[1]));
		let mut seen = HashSet::new();
		for tag in OpTag::ALL {
			assert!(seen.insert(tag.mnemonic()), "{} names two tags", tag.mnemonic());
		}
	}

	#[test]
	fn a_kind_is_tagged_with_its_dialect() {
		assert_eq!(OpKind::Logical(LogOp::Yield).tag(), OpTag::Logical(LogTag::Yield));
		assert_eq!(OpKind::Phys(PhysOp::Yield).tag(), OpTag::Phys(PhysTag::Yield));
		assert_eq!(OpKind::Expr(ExprOp::Throw).tag(), OpTag::Expr(ExprTag::Throw));
	}
}
