//! The region-stack walks that decide where a `RETURN`, a `BREAK` or a `CONTINUE` goes.
//!
//! A stack lists the regions around a point of the program, outermost first, so its last entry is
//! the region the construct is written in. The walks read only the [`RegionProperties`] of each
//! kind, never an op or an attribute. The binder can therefore ask before the op exists, and the
//! answer matches what the verifier computes from the finished container.

#![deny(clippy::wildcard_enum_match_arm)]

use crate::exec::ir::iface::region_kind::{
	LoopRule, RegionKind, RegionProperties, ReturnRule, statement_passes_return,
};

/// The index in `stack` of the region a `RETURN` written at the top of the stack ends: the
/// innermost region whose return rule catches.
///
/// A `RETURN` that passes every region of a stack whose outermost region passes it lands on that
/// region, the top of a unit. `None` when a region that no `RETURN` walk reaches is met first, or
/// when the stack is empty. Either case breaks an invariant: every op stands inside a statement or
/// a unit.
pub(crate) fn return_target(stack: &[RegionKind]) -> Option<usize> {
	for (index, kind) in stack.iter().enumerate().rev() {
		if let RegionKind::Statement(_) = kind {
			let outer = index.checked_sub(1).and_then(|outer| stack.get(outer)).copied();
			if statement_passes_return(outer) {
				continue;
			}
		}
		match kind.return_rule() {
			ReturnRule::CatchesReturn => return Some(index),
			ReturnRule::NeverReached => return None,
			ReturnRule::PassesOutward => {}
		}
	}
	// A unit whose top region passes a `RETURN` outward (a parenthesised statement or a block
	// compiled as a unit of its own) ends with it: the run that holds the unit takes it.
	stack.first().is_some_and(|top| top.return_rule() == ReturnRule::PassesOutward).then_some(0)
}

/// Where a `BREAK` or `CONTINUE` written at the top of a stack goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopExit {
	/// The index in the stack of the `FOR` body the walk reaches through regions that pass it.
	Target(usize),
	/// Stored code, a function or closure body, or the enclosing statement is met before any
	/// `FOR`: the construct is an error where it is written, and no terminator is built.
	Barrier,
}

/// Where a `BREAK` or `CONTINUE` written at the top of `stack` goes: the innermost region whose
/// loop rule is a target, provided every region between passes it.
///
/// A barrier ends the walk. So does a top-level statement, one written directly under the request
/// or a transaction: it holds no loop of its own, so a `BREAK` that reaches it has no `FOR` around
/// it. A statement inside a block's statement list is passed through, so the loop that encloses
/// the block takes the `BREAK`.
pub(crate) fn loop_exit(stack: &[RegionKind]) -> LoopExit {
	for (index, kind) in stack.iter().enumerate().rev() {
		match kind.loop_rule() {
			LoopRule::Target => return LoopExit::Target(index),
			LoopRule::Barrier => return LoopExit::Barrier,
			LoopRule::Passes => {
				if let RegionKind::Statement(_) = kind {
					let in_list = index
						.checked_sub(1)
						.and_then(|outer| stack.get(outer))
						.is_some_and(|outer| {
							!matches!(outer, RegionKind::Request | RegionKind::Transaction(_))
						});
					if !in_list {
						return LoopExit::Barrier;
					}
				}
			}
		}
	}
	LoopExit::Barrier
}
