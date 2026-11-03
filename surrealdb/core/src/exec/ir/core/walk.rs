//! Traversal helpers over a container.

use std::iter::successors;

use super::location::LocOrigin;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId};
use crate::exec::ir::iface::region_kind::{
	LoopRule, RegionKind, RegionProperties, ReturnRule, statement_passes_return,
};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpTag;
use crate::exec::ir::phys::ops::PhysTag;

/// The ops that stand directly in `region`, in block order then op order. Ops in the regions
/// those ops own are not visited. A region that does not exist holds no ops.
pub(crate) fn ops_in(ir: &Ir, region: RegionId) -> OpsIn<'_> {
	OpsIn {
		ir,
		blocks: ir.region(region).map(|region| region.blocks.as_slice()).unwrap_or(&[]),
		ops: &[],
	}
}

/// The ops that stand directly in a region; see [`ops_in`].
pub(crate) struct OpsIn<'a> {
	ir: &'a Ir,
	blocks: &'a [BlockId],
	ops: &'a [OpId],
}

impl Iterator for OpsIn<'_> {
	type Item = OpId;

	fn next(&mut self) -> Option<OpId> {
		loop {
			if let Some((first, rest)) = self.ops.split_first() {
				self.ops = rest;
				return Some(*first);
			}
			let (block, rest) = self.blocks.split_first()?;
			self.blocks = rest;
			self.ops = self.ir.block(*block).map(|data| data.ops.as_slice()).unwrap_or(&[]);
		}
	}
}

/// The regions that enclose `op`, innermost first, each paired with the op that owns it. The top
/// region is last and has no owner. A dangling `op` has no enclosing region. Allocates nothing.
pub(crate) fn ancestors(ir: &Ir, op: OpId) -> impl Iterator<Item = (RegionId, Option<OpId>)> {
	let first = ir.op(op).and_then(|data| ir.block(data.parent)).map(|block| block.parent);
	successors(first, |region| {
		let owner = ir.parent_op(*region)?;
		let block = ir.block(ir.op(owner)?.parent)?;
		Some(block.parent)
	})
	.map(|region| (region, ir.parent_op(region)))
}

/// The `logical.stmt` op whose `Statement(_)` region encloses `op`, or `None` for an op outside
/// every statement.
///
/// Value keys are compared only within one statement, so a consumer of keys groups ops by this
/// answer.
pub(crate) fn enclosing_statement(ir: &Ir, op: OpId) -> Option<OpId> {
	ancestors(ir, op).find_map(|(region, owner)| {
		if let RegionKind::Statement(_) = ir.region(region)?.kind {
			owner
		} else {
			None
		}
	})
}

/// What a `RETURN` does on reaching `region`: the return rule of its kind, except that a statement
/// of a statement list passes it outward ([`statement_passes_return`]). A region that does not
/// exist passes it.
pub(crate) fn region_return_rule(ir: &Ir, region: RegionId) -> ReturnRule {
	let Some(kind) = ir.region(region).map(|held| held.kind) else {
		return ReturnRule::PassesOutward;
	};
	if let RegionKind::Statement(_) = kind {
		let outer = ir
			.parent_op(region)
			.and_then(|owner| ir.op(owner))
			.and_then(|owner| ir.block(owner.parent))
			.and_then(|block| ir.region(block.parent))
			.map(|outer| outer.kind);
		if statement_passes_return(outer) {
			return ReturnRule::PassesOutward;
		}
	}
	kind.return_rule()
}

/// Where a `RETURN`, `BREAK` or `CONTINUE` lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlTarget {
	/// The region the op ends, or whose loop it leaves or continues.
	Region(RegionId),
	/// A `RETURN` that reaches a region no return reaches before it finds one that catches.
	Escapes(RegionId),
	/// A `BREAK` or `CONTINUE` that reaches code which is not written in the user's loop body.
	Barrier(RegionId),
	/// The op is not a `RETURN`, `BREAK` or `CONTINUE`, or is not in the container.
	NotControl,
}

/// Where the control-flow op `op` lands, derived from the regions that enclose it.
///
/// This is the only derivation of a target: the binder and the verifier both read it here.
///
/// - A `return` ends the nearest enclosing region whose return rule catches it
///   ([`region_return_rule`]). A region no return reaches, found first, answers
///   [`ControlTarget::Escapes`].
/// - A `break` or `next` leaves, or continues, the nearest enclosing region whose loop rule is a
///   target. A barrier found first answers [`ControlTarget::Barrier`].
/// - A `break` or `next` whose location is [`LocOrigin::Synthesized`] is a loop exit a pass made.
///   It lands in the nearest `Loop`, `StatementFor` or `ValueFor` region, and barriers are not
///   consulted.
///
/// A `return` that leaves a top region that passes it outward lands on that region: the container
/// is a unit, and the run that holds it takes the `return`. Any other walk that leaves the
/// container's top region without a landing answers `Escapes` for a `return` and `Barrier` for a
/// loop exit, naming the top region. Any other op answers
/// [`ControlTarget::NotControl`].
pub(crate) fn control_target(ir: &Ir, op: OpId) -> ControlTarget {
	let Some(data) = ir.op(op) else {
		return ControlTarget::NotControl;
	};
	let top = ir.top();
	match data.kind.tag() {
		OpTag::Logical(LogTag::Return) | OpTag::Phys(PhysTag::Return) => {
			for (region, _) in ancestors(ir, op) {
				if ir.region(region).is_none() {
					continue;
				}
				match region_return_rule(ir, region) {
					ReturnRule::CatchesReturn => return ControlTarget::Region(region),
					ReturnRule::NeverReached => return ControlTarget::Escapes(region),
					ReturnRule::PassesOutward => {}
				}
			}
			// A unit whose top region passes a `RETURN` outward (a parenthesised statement or a
			// block run as a unit of its own) ends with it: the run that holds the unit takes it.
			match ir.region(top).map(|held| held.kind.return_rule()) {
				Some(ReturnRule::PassesOutward) => ControlTarget::Region(top),
				Some(ReturnRule::CatchesReturn | ReturnRule::NeverReached) | None => {
					ControlTarget::Escapes(top)
				}
			}
		}
		OpTag::Logical(LogTag::Break | LogTag::Next)
		| OpTag::Phys(PhysTag::Break | PhysTag::Next) => {
			let synthesized = matches!(data.loc.origin, LocOrigin::Synthesized);
			for (region, _) in ancestors(ir, op) {
				let Some(kind) = ir.region(region).map(|held| held.kind) else {
					continue;
				};
				if synthesized {
					if matches!(
						kind,
						RegionKind::Loop | RegionKind::StatementFor | RegionKind::ValueFor
					) {
						return ControlTarget::Region(region);
					}
					continue;
				}
				match kind.loop_rule() {
					LoopRule::Target => return ControlTarget::Region(region),
					LoopRule::Barrier => return ControlTarget::Barrier(region),
					LoopRule::Passes => {}
				}
			}
			ControlTarget::Barrier(top)
		}
		OpTag::Expr(_) | OpTag::Logical(_) | OpTag::Phys(_) => ControlTarget::NotControl,
	}
}
