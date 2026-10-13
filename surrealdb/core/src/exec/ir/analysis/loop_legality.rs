//! Loop legality: whether a loop's iterations are independent, and what each leaves to apply.
//!
//! The verdict is derived from the loop-carried values, the loop's exits, the footprints across
//! iterations and the order-sensitive effects of its body. It is computed when the physical plan
//! asks whether a loop may fan out or batch, and the plan reads it from there, never recomputes it.
//!
//! Exits are read lexically. A `BREAK` or `CONTINUE` belongs to the nearest enclosing `FOR` body
//! and a `RETURN` to the nearest region that catches it; a loop body that is not a `FOR` passes
//! both outward. The code a body stores or runs under another authority (function and closure
//! bodies, stored clauses, write stages, permission, event, field and subscriber frames) is
//! never entered, since a terminator there cannot leave the loop.
//!
//! [`LoopLegality`] is the full verdict. Its projections are [`LoopLegality::facts`], the
//! per-loop facts a placement decision reads, and [`LoopLegality::legality`], the pipeline
//! legality the planner combines with them. Where the analysis cannot decide, a field takes its
//! most conservative value.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{HashSet, VecDeque};

use super::cx::default_cx;
use super::slice::{self, OnFailure, Slice};
use super::{effects, fallibility, footprint, multiplicity, types};
use crate::exec::ir::core::facts::{FactCache, LegalitySnapshotSlot};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Provenance;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{ElemPath, IrResource, KeyOf, Multiplicity, OpEffects};
use crate::exec::ir::iface::fallibility::{Fallibility, FallibilityClass};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::r#loop::{
	ApplyCut, BarrierCause, Exit, ExitCause, LoopMode, LoopRefusal, SourceDelivery,
};
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::iface::region_kind::{LoopRule, RegionKind, RegionProperties, ReturnRule};
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind, LoopSink, SourceRefShape};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::logical::pipelines::{
	AggregateCombine, ChainLegality, ChainRefusal, ErrorOrder, ForeachLegality, Legality, Paging,
	SinkCombine, SourceSplit,
};
use crate::expr::logical::EffectSet;
use crate::val::Value;

/// The most regions followed when resolving where a terminator lands, and the deepest region
/// nesting searched for one. Nesting is finite by construction, so a well-formed container never
/// meets the bound; a search that does answers as an exit.
const MAX_HOPS: usize = 1024;

/// What a loop's apply residue may not hold, each with the refusal it names, in the order
/// [`apply_refusal_of`] answers them. A deferred event queue is work the plan cannot see.
const APPLY_REFUSALS: [(EffectSet, ChainRefusal); 5] = [
	(EffectSet::OPAQUE_BODY, ChainRefusal::Opaque),
	(EffectSet::BLOCKING, ChainRefusal::Blocks),
	(EffectSet::DEFERS, ChainRefusal::Opaque),
	(EffectSet::WRITES_EXTERNAL, ChainRefusal::Writes),
	(EffectSet::READS_EXTERNAL, ChainRefusal::External),
];

/// The operators that hand a row on as soon as they have it, so that the loop's rows reach the
/// operator above without a pause.
const STREAMING: [LogTag; 6] = [
	LogTag::Filter,
	LogTag::PermissionFilter,
	LogTag::Project,
	LogTag::Split,
	LogTag::Fetch,
	LogTag::Only,
];

/// The operators that read their whole input before delivering a row.
const BREAKERS: [LogTag; 3] = [LogTag::Sort, LogTag::Aggregate, LogTag::Distinct];

/// The operators between a scan and a loop that leave the stream's split as it is.
const PASSES: [LogTag; 8] = [
	LogTag::Filter,
	LogTag::PermissionFilter,
	LogTag::Limit,
	LogTag::Project,
	LogTag::Fetch,
	LogTag::Only,
	LogTag::Timeout,
	LogTag::VersionScope,
];

/// A value a loop carries from one iteration to the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Carried {
	/// No iteration reads what another leaves.
	None,
	/// A running accumulator, threaded through the iterations. `combine` is how partial
	/// accumulators merge, when the accumulator has a combine.
	Accumulator {
		combine: Option<AggregateCombine>,
	},
}

/// Whether the apply residue of a loop may be run by the coordinator in iteration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyEffects {
	/// Nothing in the residue keeps it from running in order.
	Admitted,
	/// The residue holds an effect that cannot be ordered, for the reason named.
	Refused(ChainRefusal),
}

/// The full legality verdict of one loop, from which the projections to the planner's legality
/// types are taken.
#[derive(Clone, Debug)]
pub(crate) struct LoopLegality {
	/// How the source is cut into parts.
	pub(crate) source: SourceSplit,
	/// Whether the delivered stream holds an identity at most once.
	pub(crate) multiplicity: Multiplicity,
	/// The path along which the delivered stream's identity holds.
	pub(crate) key_path: ElemPath,
	/// How the source reaches the body.
	pub(crate) delivery: SourceDelivery,
	/// What an iteration leaves to the next.
	pub(crate) carried: Carried,
	/// How an iteration may leave the loop.
	pub(crate) exit: Exit,
	/// Whether the compute slice is row-local, and the first refusal when it is not.
	pub(crate) effects: ChainLegality,
	/// Whether the apply residue may run in iteration order.
	pub(crate) apply_effects: ApplyEffects,
	/// Whether the body holds a write.
	pub(crate) apply: ApplyCut,
	/// What a failing iteration does to the others.
	pub(crate) on_failure: OnFailure,
	/// The first thing in the body that makes its effects unknowable or order-dependent.
	pub(crate) barrier: Option<BarrierCause>,
	/// What the loop does with its iterations' results.
	pub(crate) sink: LoopSink,
	/// The `LIMIT` and `START` operators that consume the loop's rows.
	pub(crate) paging: Paging,
	/// Which failure a split run raises.
	pub(crate) error_order: ErrorOrder,
	/// The strongest failure class of the compute slice.
	pub(crate) compute_fallibility: FallibilityClass,
	/// The ops that may run before the iteration's effects are applied, and the rest.
	pub(crate) slice: Slice,
	/// Whether every write of the body that is keyed along the loop's element is keyed along the
	/// stream's key path. A write whose key is not a path of the element is not counted: the
	/// reads that could meet it are in the residue.
	pub(crate) writes_follow_key_path: bool,
	/// How the partial results of the rows combine when the sink concatenates them: the combine of
	/// the first sort, aggregate or distinct set that reads the loop's rows, else concatenation.
	pub(crate) consumer_combine: SinkCombine,
}

impl LoopLegality {
	/// Whether the iterations may run in any order and at once, and when they may not, the first
	/// condition that failed.
	///
	/// The conditions are checked in the order the refusals are declared: an unsplittable source,
	/// a carried value, an ordered exit, an effect in the compute slice or the apply residue, a
	/// barrier, a sink without a combine, and a write that two iterations may make to one key. A
	/// loop with an apply residue writes in iteration order, which is safe only for a stream that
	/// holds each identity once and whose element-keyed writes follow its key path.
	pub(crate) fn mode(&self) -> LoopMode {
		if self.source == SourceSplit::Whole {
			return LoopMode::Serial(LoopRefusal::Unsplittable);
		}
		if self.carried != Carried::None {
			return LoopMode::Serial(LoopRefusal::Carried);
		}
		if self.exit != Exit::None {
			return LoopMode::Serial(LoopRefusal::OrderedExit);
		}
		if let ChainLegality::Refused(refusal) = self.effects {
			return LoopMode::Serial(LoopRefusal::Effect(refusal));
		}
		if let ApplyEffects::Refused(refusal) = self.apply_effects {
			return LoopMode::Serial(LoopRefusal::Effect(refusal));
		}
		if let Some(cause) = self.barrier {
			return LoopMode::Serial(LoopRefusal::Barrier(cause));
		}
		let has_combine = match &self.sink {
			LoopSink::Fold | LoopSink::Decisive(_) => false,
			LoopSink::Output | LoopSink::Filter | LoopSink::Discard | LoopSink::Write(_) => true,
		};
		if !has_combine {
			return LoopMode::Serial(LoopRefusal::NoCombine);
		}
		if self.apply == ApplyCut::Ordered
			&& !(self.multiplicity == Multiplicity::Distinct && self.writes_follow_key_path)
		{
			return LoopMode::Serial(LoopRefusal::MayRepeatWrite);
		}
		LoopMode::Independent
	}

	/// Whether the loop's mode is anything but serial.
	pub(crate) fn may_fan_out(&self) -> bool {
		!matches!(self.mode(), LoopMode::Serial(_))
	}

	/// The per-loop facts a placement decision reads.
	pub(crate) fn facts(&self) -> LoopFacts {
		LoopFacts {
			mode: self.mode(),
			apply: self.apply,
			exit: self.exit,
			sink: self.sink.clone(),
			delivery: self.delivery,
			multiplicity: self.multiplicity,
			key_path: self.key_path,
		}
	}

	/// The pipeline legality of the loop.
	///
	/// The chain refuses as the compute slice does, as the apply residue does, and as a barrier
	/// does (an opaque body), by the first refusal of those. A sink that concatenates its rows
	/// combines as the first breaker that reads them does. A decisive, folding or writing sink
	/// holds state a part cannot own, so it is not splittable. The pipeline may raise whenever
	/// the compute slice may.
	pub(crate) fn legality(&self) -> Legality {
		let mut chain = self.effects;
		if let ApplyEffects::Refused(refusal) = self.apply_effects {
			chain = chain.both(ChainLegality::Refused(refusal));
		}
		if self.barrier.is_some() {
			chain = chain.both(ChainLegality::Refused(ChainRefusal::Opaque));
		}
		let sink = match &self.sink {
			LoopSink::Output | LoopSink::Filter | LoopSink::Discard => {
				self.consumer_combine.clone()
			}
			LoopSink::Decisive(_) | LoopSink::Fold | LoopSink::Write(_) => {
				SinkCombine::NotSplittable
			}
		};
		Legality {
			source: self.source,
			chain,
			sink,
			paging: self.paging,
			error_order: self.error_order,
			may_raise: self.compute_fallibility != FallibilityClass::Infallible,
		}
	}

	/// What the body of a `FOR` loop allows: the iterations are independent when the loop's mode
	/// is independent and it has no apply residue, and each `IF` arm of the body is judged by
	/// `arms` ([`if_arms`]).
	pub(crate) fn foreach(&self, arms: Vec<ChainLegality>) -> ForeachLegality {
		ForeachLegality {
			iterations_independent: self.mode() == LoopMode::Independent
				&& self.apply == ApplyCut::None,
			arms,
		}
	}
}

/// Files in `slot` the facts of every live logical loop under `region`, keyed by the logical ids
/// the loop stands for: the ids its provenance lists, or its own id when it stands for itself.
/// An entry of an id no such loop stands for is kept.
pub(crate) fn snapshot_into(
	ir: &Ir,
	facts: &mut FactCache,
	region: RegionId,
	slot: &mut LegalitySnapshotSlot,
) -> Result<(), IrError> {
	let mut loops = Vec::new();
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Logical(LogOp::Loop { .. }))) {
			loops.push(op);
		}
		Walk::Continue
	});
	for op in loops {
		let loop_facts = of(ir, facts, op)?;
		match ir.op(op).map(|data| &data.provenance) {
			Some(Provenance::From(ids)) => {
				for id in ids {
					slot.insert(*id, loop_facts.clone());
				}
			}
			Some(Provenance::Bound) => {
				slot.insert(op, loop_facts);
			}
			None => {}
		}
	}
	Ok(())
}

/// The facts of the loop `loop_op`: its mode, apply cut, exit, sink, source delivery,
/// multiplicity and key path.
///
/// Answers under the default effect context. An id that names no live logical loop is
/// [`IrError::Stale`].
pub(crate) fn of(ir: &Ir, facts: &mut FactCache, loop_op: OpId) -> Result<LoopFacts, IrError> {
	of_with(ir, facts, &default_cx(ir), loop_op).map(|legality| legality.facts())
}

/// The full legality verdict of the loop `loop_op` under `cx`.
///
/// Never memoised: the verdict reads siblings of the loop, which the loop's own generation does
/// not cover. An id that names no live op, or an op that is not a logical loop, is
/// [`IrError::Stale`].
pub(crate) fn of_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
) -> Result<LoopLegality, IrError> {
	let (kind, sink) = match ir.op(loop_op).map(|data| &data.kind) {
		Some(OpKind::Logical(LogOp::Loop {
			kind,
			sink,
			..
		})) => (*kind, sink.clone()),
		Some(OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_)) | None => {
			return Err(IrError::Stale("analysis::loop_legality: not a live logical loop"));
		}
	};
	let (multiplicity, key_path) = multiplicity::of_stream(ir, loop_op);
	let held = slice::of(ir, facts, cx, loop_op);
	let mut body_ops = Vec::new();
	if let Some(body) = multiplicity::loop_body(ir, loop_op) {
		body_order(ir, body, &mut body_ops, 0);
	}

	let compute = transitive_set(ir, facts, cx, held.compute.iter().copied());
	let residue = transitive_set(ir, facts, cx, held.residue.iter().map(|(op, _)| *op));
	let compute_fallibility = held
		.compute
		.iter()
		.fold(Fallibility::INFALLIBLE, |all, op| {
			all.union(fallibility::of_with(ir, facts, cx, *op))
		})
		.class();
	let consumers = consumers_of(ir, loop_op);
	let carried = if sink == LoopSink::Fold || kind == LoopKind::Recurse {
		Carried::Accumulator {
			combine: None,
		}
	} else {
		Carried::None
	};

	Ok(LoopLegality {
		source: source_split(ir, facts, cx, loop_op),
		multiplicity,
		key_path,
		delivery: delivery_of(ir, facts, cx, loop_op),
		carried,
		exit: exit_of(ir, loop_op),
		effects: chain_of(compute),
		apply_effects: apply_refusal_of(residue)
			.map_or(ApplyEffects::Admitted, ApplyEffects::Refused),
		apply: apply_cut(ir, facts, cx, loop_op),
		on_failure: slice::on_failure(ir, loop_op),
		barrier: barrier_of(ir, facts, cx, &body_ops),
		sink,
		paging: paging_of(consumers.limits),
		error_order: ErrorOrder::FirstByInputPosition,
		compute_fallibility,
		slice: held,
		writes_follow_key_path: writes_follow_key_path(ir, facts, cx, loop_op, &body_ops, key_path),
		consumer_combine: consumer_combine(ir, loop_op, &consumers),
	})
}

/// The legality of every loop of the container, keyed by its op, in pre-order.
///
/// A loop nested in another's body follows it, and a loop that stands in a closure body or a
/// stored clause is listed like any other.
pub(crate) fn all_loops(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
) -> Vec<(OpId, LoopLegality)> {
	let mut loops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if multiplicity::loop_kind(ir, op).is_some() {
			loops.push(op);
		}
		Walk::Continue
	});
	loops
		.into_iter()
		.filter_map(|op| of_with(ir, facts, cx, op).ok().map(|legality| (op, legality)))
		.collect()
}

/// The legality of each arm of each `IF` statement of the body of the loop `loop_op`, in body
/// order, with the conditions evaluated to reach the arm.
///
/// The `IF` statements counted are those written directly in the body or in blocks inside it. An
/// `ELSE IF` is the lone `IF` of its enclosing else arm and contributes its own arms after the
/// arms above it, each judged with every condition tested on the way. An arm refuses as the
/// effects of its conditions and of its ops do. A loop without a body, and an id that names no
/// live logical loop, hold no arm.
pub(crate) fn if_arms(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
) -> Vec<ChainLegality> {
	let mut arms = Vec::new();
	if multiplicity::loop_kind(ir, loop_op).is_none() {
		return arms;
	}
	let Some(body) = multiplicity::loop_body(ir, loop_op) else {
		return arms;
	};
	let mut statements = Vec::new();
	statement_ifs(ir, body, &mut statements, 0);
	for statement in statements {
		arms_of(ir, facts, cx, statement, EffectSet::PURE, &mut arms, 0);
	}
	arms
}

/// The `IF` statements that stand directly in `region` or in the blocks written in it.
fn statement_ifs(ir: &Ir, region: RegionId, out: &mut Vec<OpId>, depth: usize) {
	if depth > MAX_HOPS {
		return;
	}
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let tag = data.kind.tag();
		if tag == OpTag::Logical(LogTag::If) {
			out.push(op);
		} else if tag == OpTag::Logical(LogTag::Block) {
			for inner in data.regions.iter().copied() {
				statement_ifs(ir, inner, out, depth + 1);
			}
		}
	}
}

/// Pushes the legality of each arm of the `IF` op `statement`, whose enclosing arms and
/// conditions held `before`.
fn arms_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	statement: OpId,
	before: EffectSet,
	out: &mut Vec<ChainLegality>,
	depth: usize,
) {
	if depth > MAX_HOPS {
		out.push(ChainLegality::Refused(ChainRefusal::Opaque));
		return;
	}
	let Some(data) = ir.op(statement) else {
		return;
	};
	let condition = match data.operands.first() {
		Some(value) => value_effects(ir, facts, cx, *value).set,
		None => EffectSet::PURE,
	};
	let conditions = before.union(condition);
	for (at, region) in data.regions.iter().copied().enumerate() {
		let is_arm = ir.region(region).is_some_and(|info| info.kind == RegionKind::StatementIf);
		if !is_arm {
			continue;
		}
		if at > 0
			&& let Some(next) = lone_if(ir, region)
		{
			arms_of(ir, facts, cx, next, conditions, out, depth + 1);
			continue;
		}
		let set = ops_in(ir, region)
			.fold(conditions, |all, op| all.union(effects::of_with(ir, facts, cx, op).set));
		out.push(chain_of(set));
	}
}

/// The `IF` op that is the only statement of the arm `region`, which is how an `ELSE IF` is
/// written.
fn lone_if(ir: &Ir, region: RegionId) -> Option<OpId> {
	let mut found = None;
	for op in ops_in(ir, region) {
		let tag = ir.op(op)?.kind.tag();
		if tag == OpTag::Logical(LogTag::Yield) || tag == OpTag::Expr(ExprTag::Yield) {
			continue;
		}
		if tag != OpTag::Logical(LogTag::If) || found.is_some() {
			return None;
		}
		found = Some(op);
	}
	found
}

/// The ops of the region `region` in body order: an op, then the ops of the regions it owns that
/// run as part of it.
fn body_order(ir: &Ir, region: RegionId, out: &mut Vec<OpId>, depth: usize) {
	if depth > MAX_HOPS {
		return;
	}
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		out.push(op);
		for inner in data.regions.iter().copied() {
			let Some(info) = ir.region(inner) else {
				continue;
			};
			if effects::runs(&data.kind, info.kind) {
				body_order(ir, inner, out, depth + 1);
			}
		}
	}
}

/// The union of the transitive effects of `ops`.
fn transitive_set(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	ops: impl Iterator<Item = OpId>,
) -> EffectSet {
	ops.fold(EffectSet::PURE, |all, op| all.union(effects::of_with(ir, facts, cx, op).set))
}

/// What a chain that evaluates `set` makes of its legality: row-local, or the first refusal.
fn chain_of(set: EffectSet) -> ChainLegality {
	refusal_of(set).map_or(ChainLegality::RowLocal, ChainLegality::Refused)
}

/// The first refusal an apply residue holding `set` names: an opaque body, blocking, deferred
/// work, a write outside the database, a read outside the transaction, in that order.
fn apply_refusal_of(set: EffectSet) -> Option<ChainRefusal> {
	APPLY_REFUSALS.iter().find(|(effects, _)| set.intersects(*effects)).map(|(_, refusal)| *refusal)
}

/// The effects of the op that defines `value` and of every op that op reads, joined. A value
/// that is not the result of an op contributes nothing; a chain longer than the bound answers
/// [`OpEffects::top`].
fn value_effects(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, value: ValueId) -> OpEffects {
	let mut out = effects::bottom();
	let mut seen: HashSet<OpId> = HashSet::new();
	let mut pending = vec![value];
	while let Some(next) = pending.pop() {
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(next).map(|data| data.def)
		else {
			continue;
		};
		if !seen.insert(op) {
			continue;
		}
		if seen.len() > MAX_HOPS {
			return OpEffects::top();
		}
		out = effects::join(&out, &effects::of_with(ir, facts, cx, op));
		if let Some(data) = ir.op(op) {
			pending.extend(data.operands.iter().copied());
		}
	}
	out
}

/// The source operand of the loop `loop_op`.
fn source_operand(ir: &Ir, loop_op: OpId) -> Option<ValueId> {
	ir.op(loop_op).and_then(|data| data.operands.first().copied())
}

/// How the source of the loop `loop_op` is cut into parts.
///
/// - A scan of a table or of a record-id range is [`SourceSplit::Range`].
/// - A union of sources, several sources, and a value (an array, a set, a range, a parameter, a
///   call result, the result of a `LET`, a statement written in place that writes nothing) is
///   [`SourceSplit::Ordinal`]: the coordinator holds it and hands it on in batches. A list of
///   values whose evaluation draws entropy is not, since the draws depend on how many were made.
/// - A statement written in place that writes, a mock set, a parameter or expression that resolves
///   to a source when the statement runs, an input a breaker has read whole (a sort, a grouping, a
///   distinct set, a split, a join) and anything else is [`SourceSplit::Whole`].
///
/// Operators that keep each row and its place (a filter, a limit, a projection, a fetch) leave
/// the split of their input as it is.
fn source_split(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> SourceSplit {
	match source_operand(ir, loop_op) {
		Some(value) => split_of(ir, facts, cx, value, 0),
		None => SourceSplit::Whole,
	}
}

fn split_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
	depth: usize,
) -> SourceSplit {
	if depth > multiplicity::MAX_DEPTH {
		return SourceSplit::Whole;
	}
	let Some(def) = ir.value(value).map(|data| data.def) else {
		return SourceSplit::Whole;
	};
	let ValueDef::OpResult {
		op,
		..
	} = def
	else {
		return SourceSplit::Ordinal;
	};
	let Some(data) = ir.op(op) else {
		return SourceSplit::Whole;
	};
	let tag = data.kind.tag();
	match &data.kind {
		OpKind::Phys(_) => SourceSplit::Whole,
		OpKind::Expr(_) => {
			if tag == OpTag::Expr(ExprTag::Mock)
				|| (tag == OpTag::Expr(ExprTag::Subquery) && writes(ir, facts, cx, op))
			{
				SourceSplit::Whole
			} else {
				SourceSplit::Ordinal
			}
		}
		OpKind::Logical(log) => {
			if let LogOp::Source(attrs) = log {
				return match &attrs.source_ref {
					SourceRefShape::Table(_) | SourceRefShape::RecordId => SourceSplit::Range,
					SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => {
						SourceSplit::Whole
					}
				};
			}
			if PASSES.iter().any(|pass| tag == OpTag::Logical(*pass)) {
				return match data.operands.first() {
					Some(input) => split_of(ir, facts, cx, *input, depth + 1),
					None => SourceSplit::Whole,
				};
			}
			if tag == OpTag::Logical(LogTag::UnionAll) || tag == OpTag::Logical(LogTag::Let) {
				SourceSplit::Ordinal
			} else if tag == OpTag::Logical(LogTag::Values) {
				let drawn = value_effects_of_op(ir, facts, cx, op).set;
				if drawn.contains(EffectSet::ENTROPY) {
					SourceSplit::Whole
				} else {
					SourceSplit::Ordinal
				}
			} else if (tag == OpTag::Logical(LogTag::Subquery)
				|| tag == OpTag::Logical(LogTag::Loop))
				&& !writes(ir, facts, cx, op)
			{
				SourceSplit::Ordinal
			} else {
				SourceSplit::Whole
			}
		}
	}
}

/// The effects of `op` and of every op it reads, joined.
fn value_effects_of_op(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> OpEffects {
	let mut out = effects::of_with(ir, facts, cx, op);
	if let Some(data) = ir.op(op) {
		for operand in data.operands.iter().copied() {
			out = effects::join(&out, &value_effects(ir, facts, cx, operand));
		}
	}
	out
}

/// Whether `op` writes data or something outside the database.
fn writes(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> bool {
	effects::of_with(ir, facts, cx, op)
		.set
		.intersects(EffectSet::WRITES_DATA.union(EffectSet::WRITES_EXTERNAL))
}

/// How the source of the loop `loop_op` reaches its body.
///
/// [`SourceDelivery::Stream`] only when the source cannot be disturbed by the body and cannot
/// raise: its reads are disjoint from every write of the body, its transitive fallibility is
/// empty, and its effects are reads of the transaction, the catalog, the session or the row, and
/// the clock. A table scan may fail on stored data, so it is read whole, and every source error
/// then precedes every body error. A loop with no source operand is read whole.
fn delivery_of(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> SourceDelivery {
	const ADMITTED: EffectSet = EffectSet::READS_DATA
		.union(EffectSet::READS_CATALOG)
		.union(EffectSet::READS_SESSION)
		.union(EffectSet::READS_ROW)
		.union(EffectSet::CLOCK);
	let Some(source) = source_operand(ir, loop_op) else {
		return SourceDelivery::ReadWhole;
	};
	let read = value_effects(ir, facts, cx, source);
	let raises = !read.fallibility.union(fallibility::floor(read.set)).is_infallible();
	if raises || !ADMITTED.contains(read.set) {
		return SourceDelivery::ReadWhole;
	}
	let body_writes = footprint::of_with(ir, facts, cx, loop_op).writes;
	let disturbed =
		read.footprint.reads.iter().any(|source_read| {
			body_writes.iter().any(|write| footprint::overlaps(source_read, write))
		});
	if disturbed {
		SourceDelivery::ReadWhole
	} else {
		SourceDelivery::Stream
	}
}

/// The first thing in the body of a loop, in body order, that makes the loop's effects
/// unknowable or order-dependent. `body_ops` is the body in body order.
///
/// Each op is judged by its own effects, in this order: a catalog change by a schema statement,
/// or by any op that is not a write, is a catalog change, and one by a write defines its table
/// implicitly; `USE`; a session write; `SLEEP`; an opaque body; deferred work; a write whose
/// target neither provenance nor a static table pins.
fn barrier_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	body_ops: &[OpId],
) -> Option<BarrierCause> {
	const DDL: [LogTag; 4] = [LogTag::Define, LogTag::Remove, LogTag::Alter, LogTag::Rebuild];
	for op in body_ops.iter().copied() {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let tag = data.kind.tag();
		let set = effects::local(ir, facts, cx, op).set;
		if set.contains(EffectSet::CHANGES_CATALOG) {
			if DDL.iter().any(|ddl| tag == OpTag::Logical(*ddl)) {
				return Some(BarrierCause::CatalogChange);
			}
			return Some(if effects::is_write(&data.kind) {
				BarrierCause::ImplicitTable
			} else {
				BarrierCause::CatalogChange
			});
		}
		if tag == OpTag::Logical(LogTag::Use) {
			return Some(BarrierCause::Use);
		}
		if set.contains(EffectSet::WRITES_SESSION) {
			return Some(BarrierCause::SessionWrite);
		}
		if tag == OpTag::Logical(LogTag::Sleep) {
			return Some(BarrierCause::Sleep);
		}
		if set.contains(EffectSet::OPAQUE_BODY) {
			return Some(BarrierCause::Opaque);
		}
		if set.contains(EffectSet::DEFERS) {
			return Some(BarrierCause::AsyncEvent);
		}
		if tag == OpTag::Logical(LogTag::Write) && unpinned_target(ir, facts, cx, op) {
			return Some(BarrierCause::UnpinnedDynamicTarget);
		}
	}
	None
}

/// Whether the write `write_op` names a target that neither provenance pins to one table nor is
/// written as a table or a record id.
fn unpinned_target(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, write_op: OpId) -> bool {
	if types::write_target_table(ir, facts, cx, write_op).is_some() {
		return false;
	}
	let Some(subject) = types::subject_loop(ir, write_op) else {
		return true;
	};
	let targets = types::loop_targets(ir, subject);
	targets.is_empty() || !targets.iter().all(|target| static_target(ir, *target))
}

/// Whether `value` is a table or a record id written as such: a scan of a named table, a record
/// id (an expression or a literal), or a table name.
fn static_target(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	let Some(data) = ir.op(op) else {
		return false;
	};
	if let OpKind::Logical(LogOp::Source(attrs)) = &data.kind {
		return matches!(attrs.source_ref, SourceRefShape::Table(_));
	}
	if let OpKind::Expr(ExprOp::Literal(literal)) = &data.kind {
		return matches!(&**literal, Value::RecordId(_));
	}
	let tag = data.kind.tag();
	tag == OpTag::Expr(ExprTag::RecordId) || tag == OpTag::Expr(ExprTag::Table)
}

/// Whether every write of the body of `loop_op` that is keyed along the loop's element is keyed
/// along `key_path`.
fn writes_follow_key_path(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
	body_ops: &[OpId],
	key_path: ElemPath,
) -> bool {
	for op in body_ops.iter().copied() {
		if !effects::local(ir, facts, cx, op).set.contains(EffectSet::WRITES_DATA) {
			continue;
		}
		let print = footprint::relative_to(ir, facts, cx, op, loop_op);
		for write in &print.writes {
			if let IrResource::Data {
				key: KeyOf::Elem(path),
				..
			} = write && *path != key_path
			{
				return false;
			}
		}
	}
	true
}

/// What reads the rows of a loop after it: the `LIMIT` and `START` operators that consume them,
/// and the first breaker.
struct Consumers {
	/// The number of `LIMIT`/`START` operators the rows reach before any breaker.
	limits: usize,
	/// The first sort, aggregate or distinct set that reads the rows.
	breaker: Option<OpId>,
}

/// The operators that read the results of `start`, directly or through operators that hand each
/// row on as soon as they have it. A breaker ends the search along its path: what stands above it
/// reads the breaker's output.
fn consumers_of(ir: &Ir, start: OpId) -> Consumers {
	let mut out = Consumers {
		limits: 0,
		breaker: None,
	};
	let mut seen: HashSet<OpId> = HashSet::new();
	let mut pending: VecDeque<OpId> = VecDeque::from([start]);
	let mut visited = 0usize;
	while let Some(op) = pending.pop_front() {
		visited += 1;
		if visited > MAX_HOPS {
			out.limits = 2;
			break;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		for result in data.results.iter().copied() {
			for used in ir.uses(result) {
				let Some(user) = ir.op(used.user) else {
					continue;
				};
				if !seen.insert(used.user) {
					continue;
				}
				let tag = user.kind.tag();
				if tag == OpTag::Logical(LogTag::Limit) {
					out.limits += 1;
					pending.push_back(used.user);
				} else if BREAKERS.iter().any(|breaker| tag == OpTag::Logical(*breaker)) {
					if out.breaker.is_none() {
						out.breaker = Some(used.user);
					}
				} else if STREAMING.iter().any(|streaming| tag == OpTag::Logical(*streaming)) {
					pending.push_back(used.user);
				}
			}
		}
	}
	out
}

/// The paging of `limits` `LIMIT`/`START` operators.
fn paging_of(limits: usize) -> Paging {
	match limits {
		0 => Paging::None,
		1 => Paging::One,
		_ => Paging::Many,
	}
}

/// How the partial results of the rows of `loop_op` combine when its sink concatenates them.
///
/// The first breaker that reads the rows decides: a sort is a stable merge, and a top-k when a
/// `LIMIT` reads its rows; an aggregate combines per aggregate function it folds; a distinct set
/// keeps the first occurrence. An aggregate written in the body of the loop decides when no
/// breaker reads the loop. Without a breaker the rows are concatenated.
fn consumer_combine(ir: &Ir, loop_op: OpId, consumers: &Consumers) -> SinkCombine {
	if let Some(breaker) = consumers.breaker {
		let tag = ir.op(breaker).map(|data| data.kind.tag());
		if tag == Some(OpTag::Logical(LogTag::Sort)) {
			return if consumers_of(ir, breaker).limits > 0 {
				SinkCombine::TopK
			} else {
				SinkCombine::StableMerge
			};
		}
		if tag == Some(OpTag::Logical(LogTag::Aggregate)) {
			return SinkCombine::Aggregate(aggregate_combines(ir, breaker));
		}
		if tag == Some(OpTag::Logical(LogTag::Distinct)) {
			return SinkCombine::Distinct;
		}
	}
	if let Some(body) = multiplicity::loop_body(ir, loop_op) {
		let aggregate = ops_in(ir, body).find(|op| {
			ir.op(*op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Aggregate))
		});
		if let Some(aggregate) = aggregate {
			return SinkCombine::Aggregate(aggregate_combines(ir, aggregate));
		}
	}
	SinkCombine::Concat
}

/// The combine of each aggregate function the grouping `aggregate` folds, in the order written.
fn aggregate_combines(ir: &Ir, aggregate: OpId) -> Vec<AggregateCombine> {
	let mut out = Vec::new();
	let regions = ir.op(aggregate).map(|data| data.regions.to_vec()).unwrap_or_default();
	for region in regions {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Expr(ExprOp::Agg {
				combine,
				..
			})) = ir.op(op).map(|data| &data.kind)
			{
				out.push(*combine);
			}
			Walk::Continue
		});
	}
	out
}

/// How an iteration of the loop `loop_op` may leave it before its source is exhausted.
///
/// The body is searched in body order through every region written inside it, nested loops
/// included, and the first cause met is the answer:
///
/// - a `BREAK` that lands on this loop's own body is [`ExitCause::Break`];
/// - a `BREAK` or `CONTINUE` that lands outside the body is [`ExitCause::Outer`], and one that
///   lands inside a nested `FOR` is absorbed;
/// - a `CONTINUE` that lands on this loop's own body ends the iteration, not the loop, and is no
///   exit;
/// - a `RETURN` that lands outside the body is [`ExitCause::Return`], and one a region inside the
///   body catches is absorbed.
///
/// With no exit in the body, a decisive sink still makes the loop an exit, since it can stop
/// early. An id that names no live logical loop answers an exit, the most conservative answer.
pub(crate) fn exit_of(ir: &Ir, loop_op: OpId) -> Exit {
	if multiplicity::loop_kind(ir, loop_op).is_none() {
		return Exit::Ordered(ExitCause::Outer);
	}
	let Some(body) = multiplicity::loop_body(ir, loop_op) else {
		return Exit::Ordered(ExitCause::Outer);
	};
	if let Some(cause) = exit_in(ir, body, body, 0) {
		return Exit::Ordered(cause);
	}
	let decisive = matches!(
		ir.op(loop_op).map(|data| &data.kind),
		Some(OpKind::Logical(LogOp::Loop {
			sink: LoopSink::Decisive(_),
			..
		}))
	);
	if decisive {
		Exit::Ordered(ExitCause::Decisive)
	} else {
		Exit::None
	}
}

/// The first cause in `region`, in body order, by which a terminator leaves the loop whose body is
/// `body`. Regions that cannot be left by a terminator are not entered.
fn exit_in(ir: &Ir, region: RegionId, body: RegionId, depth: usize) -> Option<ExitCause> {
	if depth > MAX_HOPS {
		return Some(ExitCause::Outer);
	}
	for op in ops_in(ir, region) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		if let Some(cause) = terminator_exit(ir, op, data.kind.tag(), body) {
			return Some(cause);
		}
		for inner in data.regions.iter().copied() {
			let Some(info) = ir.region(inner) else {
				continue;
			};
			if info.kind.loop_rule() == LoopRule::Barrier {
				continue;
			}
			if let Some(cause) = exit_in(ir, inner, body, depth + 1) {
				return Some(cause);
			}
		}
	}
	None
}

/// The cause by which the op `op` leaves the loop whose body is `body`, when it is a terminator
/// that does.
fn terminator_exit(ir: &Ir, op: OpId, tag: OpTag, body: RegionId) -> Option<ExitCause> {
	let is_break = tag == OpTag::Logical(LogTag::Break);
	let is_next = tag == OpTag::Logical(LogTag::Next);
	let is_return = tag == OpTag::Logical(LogTag::Return);
	if !(is_break || is_next || is_return) {
		return None;
	}
	match terminator_target(ir, op, is_return) {
		Some(target) if target == body => is_break.then_some(ExitCause::Break),
		Some(target) if within(ir, target, body) => None,
		Some(_) | None => Some(if is_return {
			ExitCause::Return
		} else {
			ExitCause::Outer
		}),
	}
}

/// The region the terminator `op` lands on: the nearest region that catches a `RETURN` (when
/// `is_return`), or the nearest `FOR` body for a `BREAK` or `CONTINUE`. `None` when no region
/// catches it before the code it stands in stops it.
fn terminator_target(ir: &Ir, op: OpId, is_return: bool) -> Option<RegionId> {
	let mut region = ir.enclosing_region(op);
	for _ in 0..MAX_HOPS {
		let info = ir.region(region)?;
		if is_return {
			match info.kind.return_rule() {
				ReturnRule::CatchesReturn => return Some(region),
				ReturnRule::PassesOutward => {}
				ReturnRule::NeverReached => return None,
			}
		} else {
			match info.kind.loop_rule() {
				LoopRule::Target => return Some(region),
				LoopRule::Passes => {}
				LoopRule::Barrier => return None,
			}
		}
		let owner = ir.parent_op(region)?;
		region = ir.enclosing_region(owner);
	}
	None
}

/// Whether `region` is `ancestor` or stands inside it.
fn within(ir: &Ir, region: RegionId, ancestor: RegionId) -> bool {
	let mut current = region;
	for _ in 0..MAX_HOPS {
		if current == ancestor {
			return true;
		}
		let Some(owner) = ir.parent_op(current) else {
			return false;
		};
		current = ir.enclosing_region(owner);
	}
	false
}

/// Whether the body of the loop `loop_op` holds a write, triggered writes included.
///
/// [`ApplyCut::Ordered`] exactly when the transitive effects of the loop hold `WRITES_DATA`: the
/// residue the compute slice leaves behind then runs in iteration order. The slice and the
/// verdict both read the cut here, so it is computed one way. An id that names no live logical
/// loop answers `Ordered`, the most conservative value.
pub(crate) fn apply_cut(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
) -> ApplyCut {
	if multiplicity::loop_kind(ir, loop_op).is_none() {
		return ApplyCut::Ordered;
	}
	if effects::of_with(ir, facts, cx, loop_op).set.contains(EffectSet::WRITES_DATA) {
		ApplyCut::Ordered
	} else {
		ApplyCut::None
	}
}

/// The effects that stop a call from running on several rows at once, each with the refusal it
/// names, in the order [`refusal_of`] answers them. The clock is not among them.
const REFUSALS: [(EffectSet, ChainRefusal); 6] = [
	(EffectSet::WRITES_DATA.union(EffectSet::WRITES_EXTERNAL), ChainRefusal::Writes),
	(EffectSet::ADVANCES_STATE, ChainRefusal::AdvancesState),
	(EffectSet::ENTROPY, ChainRefusal::Entropy),
	(EffectSet::BLOCKING, ChainRefusal::Blocks),
	(EffectSet::OPAQUE_BODY, ChainRefusal::Opaque),
	(EffectSet::READS_EXTERNAL, ChainRefusal::External),
];

/// The first refusal `set` names: writes, state advance, entropy, blocking, an opaque body, an
/// external read, in that order. A set holding none of them, as one that only reads the clock,
/// names none.
pub(crate) fn refusal_of(set: EffectSet) -> Option<ChainRefusal> {
	REFUSALS.iter().find(|(effects, _)| set.intersects(*effects)).map(|(_, refusal)| *refusal)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::logical::effects_of_expr;
	use crate::exec::logical::pipelines::chain_legality;
	use crate::expr::logical::LogicalExpr;

	fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(sql).expect("parses");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
			panic!("a bare expression is expected: {sql}");
		};
		crate::expr::logical::lower_expr(expr).expect("lowers")
	}

	#[test]
	fn refusal_of_names_the_first_refusal() {
		assert_eq!(refusal_of(EffectSet::PURE), None);
		assert_eq!(refusal_of(EffectSet::CLOCK), None);
		assert_eq!(refusal_of(EffectSet::WRITES_EXTERNAL), Some(ChainRefusal::Writes));
		assert_eq!(refusal_of(EffectSet::ADVANCES_STATE), Some(ChainRefusal::AdvancesState));
		assert_eq!(refusal_of(EffectSet::BLOCKING), Some(ChainRefusal::Blocks));
		assert_eq!(refusal_of(EffectSet::READS_EXTERNAL), Some(ChainRefusal::External));
		assert_eq!(
			refusal_of(EffectSet::ENTROPY.union(EffectSet::OPAQUE_BODY)),
			Some(ChainRefusal::Entropy),
			"the earlier refusal wins"
		);
	}

	#[test]
	fn refusal_order_matches_pipelines() {
		let registry = FunctionRegistry::with_builtins();
		let cases: [(&str, Option<ChainRefusal>); 8] = [
			("age + 1", None),
			("time::now()", None),
			("(CREATE log:1)", Some(ChainRefusal::Writes)),
			("sequence::nextval('s')", Some(ChainRefusal::AdvancesState)),
			("rand()", Some(ChainRefusal::Entropy)),
			("sleep(1ms)", Some(ChainRefusal::Blocks)),
			("fn::f()", Some(ChainRefusal::Opaque)),
			("http::get('http://localhost/')", Some(ChainRefusal::External)),
		];
		for (sql, expected) in cases {
			let expression = expr(sql);
			let set = effects_of_expr(&expression, &registry).set;
			assert_eq!(refusal_of(set), expected, "{sql}");
			assert_eq!(chain_of(set), chain_legality([&expression], &registry), "{sql}");
		}
	}

	#[test]
	fn the_refusal_set_is_non_determinism_but_the_clock() {
		let refused = REFUSALS.iter().fold(EffectSet::PURE, |all, (set, _)| all.union(*set));
		assert_eq!(refused, EffectSet::NON_DETERMINISTIC_BUT_FOR_CLOCK);
	}
}
