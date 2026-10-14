//! The compute slice of a loop and its apply residue.
//!
//! The slice holds the ops of an iteration that may run before the iteration's effects are
//! applied in order; the residue holds the rest. An op outside the slice records why.
//!
//! Ops are judged in body order, which is pre-order over the body region: an op, then the ops of
//! the regions it owns. An op that owns regions is judged on its transitive effects and each op
//! of its regions is judged on its own; code a region stores and never runs, and a closure body
//! that runs only where the closure is invoked, are not part of the body. The rules apply in this
//! order, and the first that excludes an op is the one recorded:
//!
//! 1. an op that writes, or a write stage marked as apply, belongs to the residue;
//! 2. when the loop has an apply residue, an op that advances state or draws entropy does too,
//!    since its answers depend on how many ran before it. A clock read stays in the slice;
//! 3. a read of stored data belongs to the residue when it overlaps a write of the body that
//!    another iteration may make, or one an earlier op of the same iteration makes;
//! 4. a read that locks records does;
//! 5. an op that uses a residue op's result, or that runs only if a residue op decides it, does,
//!    and so does an op that uses a block argument of the body of a residue loop, since the element
//!    and position of such a loop are produced in apply order. This repeats until no further op is
//!    excluded.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{HashMap, VecDeque};

use smallvec::SmallVec;

use super::{effects, footprint, loop_legality, multiplicity, types};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{ElemPath, IrFootprint, IrResource, Multiplicity};
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::iface::r#loop::ApplyCut;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{LogOp, WriteMark};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::pipelines::ChainRefusal;
use crate::expr::logical::EffectSet;

/// The effects an op may have on state other than the loop's own values: any op that holds one
/// leaves the compute slice.
const WRITES: EffectSet = EffectSet::WRITES_DATA
	.union(EffectSet::WRITES_EXTERNAL)
	.union(EffectSet::NOTIFY)
	.union(EffectSet::DEFERS)
	.union(EffectSet::CHANGES_CATALOG)
	.union(EffectSet::WRITES_SESSION);

/// The effects whose answers depend on how many ran before them.
const ORDERED: EffectSet = EffectSet::ADVANCES_STATE.union(EffectSet::ENTROPY);

/// Why an op is outside the compute slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SliceExclusion {
	/// The op writes data, something outside the database, a session variable, the catalog or an
	/// event queue, or notifies.
	Writes,
	/// The op's effects depend on how many ran before it; the refusal names which.
	Effect(ChainRefusal),
	/// The op reads what another iteration may write.
	CrossIteration,
	/// The op reads what an earlier op of the same iteration may write.
	SameIterationWrite,
	/// The op uses the result of an apply op, or runs only if one decides it.
	DependsOnApply,
	/// The op reads records under a lock.
	Locked,
	/// The op is a write stage marked as apply.
	StageMark,
}

/// What a failure in an iteration does to the iterations around it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnFailure {
	/// The failure ends the statement.
	Raise,
	/// The failing record is skipped and the other iterations continue.
	SkipRow,
}

/// The ops of a loop body that may run before the iteration's effects are applied, and the rest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Slice {
	/// The body's ops that may run before the iteration's effects are applied, in body order.
	pub(crate) compute: SmallVec<[OpId; 8]>,
	/// The rest, in body order, each with the first rule that excluded it.
	pub(crate) residue: SmallVec<[(OpId, SliceExclusion); 8]>,
}

/// The compute slice and the apply residue of the loop `loop_op`.
///
/// Every op of the body is in exactly one of the two lists, each in body order. An id that names
/// no live logical loop, and a loop without a body, answer an empty slice.
pub(crate) fn of(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> Slice {
	if multiplicity::loop_kind(ir, loop_op).is_none() {
		return Slice::default();
	}
	let Some(body) = multiplicity::loop_body(ir, loop_op) else {
		return Slice::default();
	};
	let mut ops = Body::default();
	ops.collect(ir, body);
	let cut = loop_legality::apply_cut(ir, facts, cx, loop_op);
	let (stream, key_path) = multiplicity::of_stream(ir, loop_op);

	let judged: Vec<EffectSet> =
		ops.ops.iter().map(|op| effects::of_with(ir, facts, cx, *op).set).collect();
	let touches = EffectSet::READS_DATA.union(WRITES);
	let prints: Vec<IrFootprint> = ops
		.ops
		.iter()
		.zip(&judged)
		.map(|(op, set)| {
			if set.intersects(touches) {
				footprint::relative_to(ir, facts, cx, *op, loop_op)
			} else {
				IrFootprint {
					reads: SmallVec::new(),
					writes: SmallVec::new(),
				}
			}
		})
		.collect();
	let body_writes: Vec<&IrResource> =
		prints.iter().flat_map(|print| print.writes.iter()).collect();

	let mut reason: Vec<Option<SliceExclusion>> = vec![None; ops.ops.len()];
	for at in 0..ops.ops.len() {
		let op = ops.ops[at];
		let set = judged[at];
		reason[at] = if is_apply_stage(ir, op) {
			Some(SliceExclusion::StageMark)
		} else if set.intersects(WRITES) {
			Some(SliceExclusion::Writes)
		} else if cut == ApplyCut::Ordered && set.intersects(ORDERED) {
			Some(SliceExclusion::Effect(ordered_refusal(set)))
		} else if set.contains(EffectSet::READS_DATA) {
			memory_dependence(&ops, &prints, &body_writes, at, key_path, stream)
		} else {
			None
		};
		if reason[at].is_none() && locks(ir, op) {
			reason[at] = Some(SliceExclusion::Locked);
		}
	}

	propagate(ir, &ops, &mut reason);

	let mut out = Slice::default();
	for (at, op) in ops.ops.iter().enumerate() {
		match reason[at] {
			Some(why) => out.residue.push((*op, why)),
			None => out.compute.push(*op),
		}
	}
	out
}

/// What a failure in an iteration of the loop `loop_op` does to the iterations around it:
/// [`OnFailure::SkipRow`] exactly when the loop is the subject loop of a write that ignores
/// failures (`INSERT IGNORE`), and [`OnFailure::Raise`] otherwise.
pub(crate) fn on_failure(ir: &Ir, loop_op: OpId) -> OnFailure {
	let Some(data) = ir.op(loop_op) else {
		return OnFailure::Raise;
	};
	for result in data.results.iter().copied() {
		for used in ir.uses(result) {
			let Some(user) = ir.op(used.user) else {
				continue;
			};
			if let OpKind::Logical(LogOp::Write(attrs)) = &user.kind
				&& attrs.flags.ignore
				&& types::subject_loop(ir, used.user) == Some(loop_op)
			{
				return OnFailure::SkipRow;
			}
		}
	}
	OnFailure::Raise
}

/// The ops of a loop body in body order, with where each op's subtree ends.
#[derive(Default)]
struct Body {
	ops: Vec<OpId>,
	/// The index one past the last op of the subtree of the op at the same index.
	end: Vec<usize>,
	index: HashMap<OpId, usize>,
}

impl Body {
	fn collect(&mut self, ir: &Ir, region: RegionId) {
		for op in ops_in(ir, region) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			let at = self.ops.len();
			self.ops.push(op);
			self.end.push(at + 1);
			self.index.insert(op, at);
			for inner in data.regions.iter().copied() {
				let Some(info) = ir.region(inner) else {
					continue;
				};
				if effects::runs(&data.kind, info.kind) {
					self.collect(ir, inner);
				}
			}
			self.end[at] = self.ops.len();
		}
	}
}

/// The refusal an op whose effects hold [`ORDERED`] names: a state advance before entropy, as
/// [`loop_legality::refusal_of`] orders them.
fn ordered_refusal(set: EffectSet) -> ChainRefusal {
	if set.intersects(EffectSet::ADVANCES_STATE) {
		ChainRefusal::AdvancesState
	} else {
		ChainRefusal::Entropy
	}
}

/// Whether `op` is a write stage the plan marks as apply work.
fn is_apply_stage(ir: &Ir, op: OpId) -> bool {
	matches!(
		ir.op(op).map(|data| &data.kind),
		Some(OpKind::Logical(LogOp::WriteStage {
			mark: WriteMark::Apply,
			..
		}))
	)
}

/// Whether `op` reads records under a lock.
fn locks(ir: &Ir, op: OpId) -> bool {
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Logical(LogOp::Source(attrs))) => attrs.for_update,
		Some(OpKind::Logical(LogOp::Values {
			for_update,
			..
		})) => *for_update,
		Some(_) | None => false,
	}
}

/// Why the read at `at` may not run before the writes of the body, if it may not.
///
/// A read is cross-iteration when any resource it reads is not disjoint, across iterations, from
/// a resource any op of the body writes. It is a same-iteration read when it overlaps a write of
/// an op that finishes before it begins.
fn memory_dependence(
	body: &Body,
	prints: &[IrFootprint],
	body_writes: &[&IrResource],
	at: usize,
	key_path: ElemPath,
	multiplicity: Multiplicity,
) -> Option<SliceExclusion> {
	let reads = &prints[at].reads;
	let across = reads.iter().any(|read| {
		body_writes.iter().any(|write| {
			!footprint::disjoint_across_iterations(read, write, key_path, multiplicity)
		})
	});
	if across {
		return Some(SliceExclusion::CrossIteration);
	}
	let same = (0..at).filter(|earlier| body.end[*earlier] <= at).any(|earlier| {
		prints[earlier]
			.writes
			.iter()
			.any(|write| reads.iter().any(|read| footprint::overlaps(read, write)))
	});
	same.then_some(SliceExclusion::SameIterationWrite)
}

/// Moves into the residue every op that uses the result of a residue op, or whose execution a
/// residue op decides, until no op moves.
fn propagate(ir: &Ir, body: &Body, reason: &mut [Option<SliceExclusion>]) {
	let mut queue: VecDeque<usize> = (0..reason.len()).filter(|at| reason[*at].is_some()).collect();
	while let Some(at) = queue.pop_front() {
		let op = body.ops[at];
		let Some(data) = ir.op(op) else {
			continue;
		};
		let mut dependents: SmallVec<[OpId; 8]> = SmallVec::new();
		for result in data.results.iter().copied() {
			dependents.extend(ir.uses(result).iter().map(|used| used.user));
		}
		for region in data.regions.iter().copied() {
			let Some(info) = ir.region(region) else {
				continue;
			};
			if decides_execution(info.kind) {
				collect_subtree(ir, region, &mut dependents);
			}
			if effects::is_loop(&data.kind) && effects::is_loop_body(info.kind) {
				collect_body_arg_users(ir, region, &mut dependents);
			}
		}
		for dependent in dependents {
			if let Some(&to) = body.index.get(&dependent)
				&& reason[to].is_none()
			{
				reason[to] = Some(SliceExclusion::DependsOnApply);
				queue.push_back(to);
			}
		}
	}
}

/// Whether the ops of a region of this kind run only when the op that owns the region decides it.
fn decides_execution(kind: RegionKind) -> bool {
	kind == RegionKind::StatementIf
		|| kind == RegionKind::ValueIf
		|| kind == RegionKind::LazyOperand
		|| kind == RegionKind::ChooseArm
		|| kind == RegionKind::JoinLookupArm
}

/// Every op that uses an argument of a block of the loop body `region`.
fn collect_body_arg_users(ir: &Ir, region: RegionId, out: &mut SmallVec<[OpId; 8]>) {
	let Some(info) = ir.region(region) else {
		return;
	};
	for block in info.blocks.iter().copied() {
		let Some(data) = ir.block(block) else {
			continue;
		};
		for arg in data.args.iter().copied() {
			out.extend(ir.uses(arg).iter().map(|used| used.user));
		}
	}
}

/// Every op in `region` and in the regions beneath it.
fn collect_subtree(ir: &Ir, region: RegionId, out: &mut SmallVec<[OpId; 8]>) {
	for op in ops_in(ir, region) {
		out.push(op);
		if let Some(data) = ir.op(op) {
			for inner in data.regions.iter().copied() {
				collect_subtree(ir, inner, out);
			}
		}
	}
}
