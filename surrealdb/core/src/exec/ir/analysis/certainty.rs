//! Read certainty: whether a read in a loop body runs on every successful path.
//!
//! Certainty is judged on successful paths: an earlier position's possible error does not make a
//! later read conditional.

#![deny(clippy::wildcard_enum_match_arm)]

use super::{effects, fallibility, loop_legality, multiplicity, slice};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::r#loop::Exit;
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{LogOp, LogTag};
use crate::exec::ir::op_kind::{OpKind, OpTag};

/// The most regions followed on the way from a read up to the loop body, and the most operators
/// followed on the way from a loop down to the `LIMIT` that consumes it. Nesting is finite by
/// construction, so a well-formed container never meets the bound.
const MAX_HOPS: usize = 1024;

/// How certain a read of a loop body is to run.
///
/// Ordered from the strongest answer to the weakest; the weakest is the top of the lattice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReadCertainty {
	/// The read runs in every iteration that completes.
	Certain,
	/// The read runs only when a condition holds: an arm of an `IF`, the right side of a
	/// short-circuit operator, a later conjunct of a filter, or an iteration that follows a
	/// write stage that may skip it.
	Conditional,
	/// Some iteration may not run at all: the loop may exit early or a `LIMIT` stops it.
	PastExit,
}

/// How certain the read `read`, an op of the body of the loop `loop_op`, is to run.
///
/// - [`ReadCertainty::PastExit`] when the loop may exit before its source is exhausted
///   ([`loop_legality::exit_of`]) or a `LIMIT` consumes its rows, since the stream stops being
///   pulled. The answer is the same for every iteration, including the first. A read that stands
///   outside the loop's body, and an id that names no live op, answer it too.
/// - [`ReadCertainty::Conditional`] when the read stands in an arm of an `IF`, a lazily evaluated
///   operand, a choice arm, the body of a nested loop, a closure or function body, or an error
///   scope below the loop body, in a `WHERE` clause that is not the first of its operator, or when
///   the loop skips failing rows and a write stage that may fail precedes the read in body order.
/// - [`ReadCertainty::Certain`] otherwise.
pub(crate) fn of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
	read: OpId,
) -> ReadCertainty {
	if loop_legality::exit_of(ir, loop_op) != Exit::None || consumed_by_limit(ir, loop_op) {
		return ReadCertainty::PastExit;
	}
	let Some(body) = multiplicity::loop_body(ir, loop_op) else {
		return ReadCertainty::PastExit;
	};
	match conditional_below(ir, read, body) {
		None => return ReadCertainty::PastExit,
		Some(true) => return ReadCertainty::Conditional,
		Some(false) => {}
	}
	if slice::on_failure(ir, loop_op) == slice::OnFailure::SkipRow
		&& precedes_by_failing_stage(ir, facts, cx, body, read)
	{
		return ReadCertainty::Conditional;
	}
	ReadCertainty::Certain
}

/// Whether a region between `read` and `body` makes the read run only on some paths, or `None`
/// when `read` does not stand inside `body`.
fn conditional_below(ir: &Ir, read: OpId, body: RegionId) -> Option<bool> {
	ir.op(read)?;
	let mut conditional = false;
	let mut region = ir.enclosing_region(read);
	for _ in 0..MAX_HOPS {
		if region == body {
			return Some(conditional);
		}
		let info = ir.region(region)?;
		let owner = ir.parent_op(region)?;
		conditional |= may_not_run(info.kind) || later_where(ir, owner, region, info.kind);
		region = ir.enclosing_region(owner);
	}
	None
}

/// Whether the ops of a region of this kind may run zero times in one iteration of an enclosing
/// loop: an arm that a condition selects, the body of a loop nested in it (its source may be
/// empty), a closure or function body (it runs only if invoked), and a region in which an error
/// is caught or reshaped.
fn may_not_run(kind: RegionKind) -> bool {
	kind == RegionKind::StatementIf
		|| kind == RegionKind::ValueIf
		|| kind == RegionKind::LazyOperand
		|| kind == RegionKind::ChooseArm
		|| kind == RegionKind::JoinLookupArm
		|| effects::is_loop_body(kind)
		|| kind == RegionKind::ClosureBody
		|| kind == RegionKind::FunctionBody
		|| matches!(kind, RegionKind::ErrorScope(_))
}

/// Whether `region`, owned by `owner`, is a `WHERE` clause other than the first the operator holds.
fn later_where(ir: &Ir, owner: OpId, region: RegionId, kind: RegionKind) -> bool {
	if kind != RegionKind::Clause(ClauseRole::Where) {
		return false;
	}
	let Some(data) = ir.op(owner) else {
		return false;
	};
	data.regions
		.iter()
		.copied()
		.find(|held| ir.region(*held).is_some_and(|info| info.kind == kind))
		.is_some_and(|first| first != region)
}

/// Whether a `LIMIT` consumes the rows of the loop `loop_op`, directly or through operators that
/// hand each row on as soon as they have it, so that the loop stops being pulled once the limit
/// is met.
fn consumed_by_limit(ir: &Ir, loop_op: OpId) -> bool {
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut pending = vec![loop_op];
	let mut visited = 0;
	while let Some(op) = pending.pop() {
		visited += 1;
		if visited > MAX_HOPS {
			return true;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		for result in data.results.iter().copied() {
			for used in ir.uses(result) {
				let Some(user) = ir.op(used.user) else {
					continue;
				};
				if let OpKind::Logical(LogOp::Limit {
					..
				}) = &user.kind
				{
					return true;
				}
				if streams_rows(user.kind.tag()) && seen.insert(used.user) {
					pending.push(used.user);
				}
			}
		}
	}
	false
}

/// Whether an op of this kind delivers each row of its input as soon as it has it.
fn streams_rows(tag: OpTag) -> bool {
	[
		LogTag::Filter,
		LogTag::PermissionFilter,
		LogTag::Project,
		LogTag::Split,
		LogTag::Fetch,
		LogTag::Only,
	]
	.into_iter()
	.any(|streaming| tag == OpTag::Logical(streaming))
}

/// Whether a write stage that may fail comes before `read` in body order.
fn precedes_by_failing_stage(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	body: RegionId,
	read: OpId,
) -> bool {
	let mut stages = Vec::new();
	let mut found_read = false;
	scan(ir, body, read, &mut found_read, &mut stages);
	stages.into_iter().any(|stage| !fallibility::of_with(ir, facts, cx, stage).is_infallible())
}

/// Collects, in body order, the write stages that come before `read`; stops at `read`.
fn scan(ir: &Ir, region: RegionId, read: OpId, done: &mut bool, stages: &mut Vec<OpId>) {
	for op in ops_in(ir, region) {
		if *done {
			return;
		}
		if op == read {
			*done = true;
			return;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		if data.kind.tag() == OpTag::Logical(LogTag::WriteStage) {
			stages.push(op);
		}
		for inner in data.regions.iter().copied() {
			scan(ir, inner, read, done, stages);
		}
	}
}
