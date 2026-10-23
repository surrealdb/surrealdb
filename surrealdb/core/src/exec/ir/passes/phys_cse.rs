//! Common-subexpression elimination and dead-code elimination over the physical dialect, for the
//! expression ops inside the regions of physical ops: projections, filters and key operands.
//!
//! Equal values are merged with the keys and guards of [`super::cse`]. An expression op that
//! nothing reads and whose removal no run can observe is erased with `DeadPure`: its effects are
//! reads of the snapshot, the catalog, the row, the session or the clock, it cannot raise, it owns
//! no region, and it is no terminator, depth charge or computed read. Physical ops are never
//! created, merged or erased here, and the facts of a merged op come from the fact functions that
//! own them; this pass derives none.
//!
//! A container that is not in the physical phase is left as it is.
#![deny(clippy::wildcard_enum_match_arm)]

use super::cse::{READS, Scope, in_physical_region, in_stored_clause, merge_equal_values};
use super::driver::PassReport;
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, Limit};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::{EliminationReason, Phase};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::OpKind;

/// How many times the dead-code sweep is repeated while it keeps erasing.
const MAX_ROUNDS: u8 = GREEDY_DEFAULT.max_passes;

/// Merges equal expression values and removes dead pure expression ops inside the physical regions
/// of `ir`, which must be in [`Phase::Physical`]; any other container answers the default report.
///
/// The rewrites reported are the merges plus the erases. A sweep that still erases at the bound
/// reports [`Limit::Passes`] in `stopped_by`, and leaves a container that verifies.
pub(crate) fn run(
	ir: &mut Ir,
	facts: &mut FactCache,
	effects: &EffectCx<'_>,
) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	if ir.phase() != Phase::Physical {
		return Ok(report);
	}
	report.rewrites = merge_equal_values(ir, facts, effects, Scope::Physical)?;
	for round in 0..MAX_ROUNDS {
		let erased = erase_dead_pure(ir, facts, effects)?;
		report.rewrites = report.rewrites.saturating_add(erased);
		if erased == 0 {
			break;
		}
		if round + 1 == MAX_ROUNDS {
			report.stopped_by = Some(Limit::Passes);
		}
	}
	Ok(report)
}

/// The expression ops inside physical regions, in program order.
fn physical_expressions(ir: &Ir) -> Vec<OpId> {
	let mut ops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let is_expression = ir.op(op).is_some_and(|data| matches!(data.kind, OpKind::Expr(_)));
		if is_expression && in_physical_region(ir, op) && !in_stored_clause(ir, op) {
			ops.push(op);
		}
		Walk::Continue
	});
	ops
}

/// Erases the dead pure expression ops, in reverse program order so that an op's readers go before
/// it is judged, and answers how many went.
fn erase_dead_pure(ir: &mut Ir, facts: &mut FactCache, cx: &EffectCx<'_>) -> Result<u32, IrError> {
	let mut erased = 0;
	let mut gone: FxHashSet<OpId> = FxHashSet::default();
	for op in physical_expressions(ir).into_iter().rev() {
		if gone.contains(&op) || !is_dead_pure(ir, facts, cx, op) {
			continue;
		}
		let mut rw = Rewriter::new(&mut *ir, &mut *facts);
		match rw.erase(op, EliminationReason::DeadPure) {
			Ok(()) => {
				erased += 1;
				gone.insert(op);
			}
			Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(erased)
}

/// Whether `op` can be erased: it has results and none of them is read, it has no region and no
/// control-flow role, its effects are reads only, and it cannot raise.
fn is_dead_pure(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let OpKind::Expr(kind) = &data.kind else {
		return false;
	};
	let pinned = data.results.is_empty()
		|| !data.regions.is_empty()
		|| data.kind.is_terminator()
		|| matches!(kind, ExprOp::DepthCharge { .. } | ExprOp::ComputedRead(_));
	if pinned || data.results.iter().any(|result| !ir.uses(*result).is_empty()) {
		return false;
	}
	let answer = effects::of_with(ir, facts, cx, op);
	let class = answer.fallibility.union(fallibility::floor(answer.set));
	guards::infallible(class) && READS.contains(answer.set)
}
