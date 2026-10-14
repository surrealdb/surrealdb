//! Transitive effects, footprint, rollback behaviour and determinism of an op.
//!
//! The answer for an op is the least upper bound of its local answer and the answers of every op
//! in the regions it owns, with these exceptions:
//!
//! - a `StoredClause` region holds code the statement stores and never runs, so it contributes
//!   nothing;
//! - the `ClosureBody` of an `expr.closure_create` is not part of the creating op. It belongs to
//!   each op that takes the closure as an operand, directly or through the result of a
//!   `logical.let` or an invocation's dispatch: a closure loop, `expr.invoke`, `expr.call` or
//!   `expr.method`. A closure that reaches an op in any other way escapes, and that op's own call
//!   rule answers for it.
//!
//! The arms of a lazy operand, an `if` and a choice are all folded: an arm that may not run still
//! contributes its effects. A write shows the stored code it runs when a write-stage op with a
//! non-empty region sits under it or, for a logical write, under the subject loop whose result it
//! consumes. A write that shows no such code may trigger code the container does not show, so its
//! footprint holds `IrResource::Unknown`.
//!
//! The only memo is the fact cache entry of an op at its generation. It holds the region-local
//! answer: the op's local answer joined with the region-local answers of the ops of its own
//! regions, never anything read from outside the op's subtree. The closure contribution and the
//! `Unknown` of a logical write whose stage code is not shown are read across subtrees and are
//! added on every call. A footprint folded out of a loop body has every
//! element key widened to `KeyOf::Any`, so a loop and its ancestors never state a key relative to
//! an iteration.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use super::cx::default_cx;
use super::{phys_effects, types};
use crate::exec::ir::core::facts::{FactCache, OpFacts};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{DispatchTarget, ExprOp, ExprTag};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{
	Determinism, IrFootprint, IrResource, KeyOf, OpEffects, Rollback,
};
use crate::exec::ir::iface::fallibility::{ContextDep, Fallibility};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::PhysTag;
use crate::expr::logical::EffectSet;

/// The most hops followed from an operand to the closure creation it carries. A longer chain is
/// not a closure the analysis attributes, and answers as an escape.
const MAX_CLOSURE_HOPS: usize = 64;

/// The effects of `op` and of every op in its regions, memoised in `facts`, under the default
/// context.
///
/// An op nothing is known about answers [`OpEffects::top`].
pub(crate) fn of(ir: &Ir, facts: &mut FactCache, op: OpId) -> OpEffects {
	of_with(ir, facts, &default_cx(ir), op)
}

/// The effects of `op` and of every op in its regions under `cx`, memoised in `facts`.
///
/// The answer meets every floor the effect set implies. A dangling id answers
/// [`OpEffects::top`].
pub(crate) fn of_with(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> OpEffects {
	if ir.op(op).is_none() {
		return OpEffects::top();
	}
	let mut out = regional(ir, facts, cx, op);
	let mut invoked: SmallVec<[OpId; 2]> = SmallVec::new();
	collect_invoked(ir, op, &mut invoked);
	for creator in invoked {
		out = join(&out, &closure_body(ir, facts, cx, creator));
	}
	if hides_stage_code(ir, op) {
		out.footprint.reads = smallvec![IrResource::Unknown];
		out.footprint.writes = smallvec![IrResource::Unknown];
	}
	settle(out)
}

/// The local answer of one op: its kind's own effects, never those of its regions.
///
/// A physical op answers by [`phys_effects::of_op`], the fold of the logical ops its provenance
/// names; every other op answers by its kind. A dangling id answers [`OpEffects::top`].
pub(crate) fn local(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> OpEffects {
	match ir.op(op) {
		Some(data) => match &data.kind {
			OpKind::Phys(_) => phys_effects::of_op(ir, facts, cx, op),
			OpKind::Expr(_) | OpKind::Logical(_) => data.kind.local_effects(cx),
		},
		None => OpEffects::top(),
	}
}

/// The least upper bound of two answers.
///
/// Commutative, associative and idempotent, with [`bottom`] as identity: the sets and failure
/// classes are unioned, context dependencies are unioned without repeats, footprints are
/// concatenated without repeats (a list holding `IrResource::Unknown` is that resource alone),
/// and rollback and determinism each take the greater value of their order.
pub(crate) fn join(a: &OpEffects, b: &OpEffects) -> OpEffects {
	OpEffects {
		set: a.set.union(b.set),
		fallibility: a.fallibility.union(b.fallibility),
		context_deps: join_deps(&a.context_deps, &b.context_deps),
		footprint: IrFootprint {
			reads: join_resources(&a.footprint.reads, &b.footprint.reads),
			writes: join_resources(&a.footprint.writes, &b.footprint.writes),
		},
		rollback: join_rollback(a.rollback, b.rollback),
		determinism: join_determinism(a.determinism, b.determinism),
	}
}

/// The weaker guarantee of two: `Deterministic < PerStatement < WithinStatementUnlessWritten <
/// NonDeterministic`.
///
/// A clock is constant within a statement, so a clock read together with a data read is no
/// stronger than the data read.
pub(crate) fn join_determinism(a: Determinism, b: Determinism) -> Determinism {
	if determinism_rank(a) >= determinism_rank(b) {
		a
	} else {
		b
	}
}

/// The stronger rollback behaviour of two: `NoEffects < Undone < Survives`.
pub(crate) fn join_rollback(a: Rollback, b: Rollback) -> Rollback {
	if rollback_rank(a) >= rollback_rank(b) {
		a
	} else {
		b
	}
}

/// The identity of [`join`]: no effect, infallible, an empty footprint, `Rollback::NoEffects` and
/// `Determinism::Deterministic`.
pub(crate) fn bottom() -> OpEffects {
	OpEffects {
		set: EffectSet::PURE,
		fallibility: Fallibility::INFALLIBLE,
		context_deps: SmallVec::new(),
		footprint: IrFootprint {
			reads: SmallVec::new(),
			writes: SmallVec::new(),
		},
		rollback: Rollback::NoEffects,
		determinism: Determinism::Deterministic,
	}
}

fn determinism_rank(d: Determinism) -> u8 {
	match d {
		Determinism::Deterministic => 0,
		Determinism::PerStatement => 1,
		Determinism::WithinStatementUnlessWritten => 2,
		Determinism::NonDeterministic => 3,
	}
}

fn rollback_rank(r: Rollback) -> u8 {
	match r {
		Rollback::NoEffects => 0,
		Rollback::Undone => 1,
		Rollback::Survives => 2,
	}
}

fn join_deps(a: &[ContextDep], b: &[ContextDep]) -> SmallVec<[ContextDep; 1]> {
	let mut out: SmallVec<[ContextDep; 1]> = SmallVec::new();
	for dep in a.iter().chain(b) {
		if !out.contains(dep) {
			out.push(dep.clone());
		}
	}
	out
}

fn join_resources(a: &[IrResource], b: &[IrResource]) -> SmallVec<[IrResource; 2]> {
	if a.contains(&IrResource::Unknown) || b.contains(&IrResource::Unknown) {
		return smallvec![IrResource::Unknown];
	}
	let mut out: SmallVec<[IrResource; 2]> = SmallVec::new();
	for resource in a.iter().chain(b) {
		if !out.contains(resource) {
			out.push(resource.clone());
		}
	}
	out
}

/// `effects` raised to every floor its set implies.
///
/// - A data read is at least `WithinStatementUnlessWritten`.
/// - Any effect that keeps two evaluations apart, the clock excepted, is `NonDeterministic`; a
///   clock alone is at least `PerStatement`.
/// - An effect outside the transaction, a body nobody can see, a notification and a counter the
///   transaction does not snapshot survive a rollback.
/// - A data or catalog write with no such effect is undone by a rollback.
fn settle(mut effects: OpEffects) -> OpEffects {
	const SURVIVES: EffectSet = EffectSet::WRITES_EXTERNAL
		.union(EffectSet::READS_EXTERNAL)
		.union(EffectSet::OPAQUE_BODY)
		.union(EffectSet::NOTIFY)
		.union(EffectSet::ADVANCES_STATE);
	const UNDONE: EffectSet = EffectSet::WRITES_DATA.union(EffectSet::CHANGES_CATALOG);

	let set = effects.set;
	if set.contains(EffectSet::READS_DATA) {
		effects.determinism =
			join_determinism(effects.determinism, Determinism::WithinStatementUnlessWritten);
	}
	if set.contains(EffectSet::CLOCK) {
		effects.determinism = join_determinism(effects.determinism, Determinism::PerStatement);
	}
	if set.intersects(EffectSet::NON_DETERMINISTIC_BUT_FOR_CLOCK) {
		effects.determinism = Determinism::NonDeterministic;
	}
	if set.intersects(SURVIVES) {
		effects.rollback = Rollback::Survives;
	} else if set.intersects(UNDONE) {
		effects.rollback = join_rollback(effects.rollback, Rollback::Undone);
	}
	effects
}

/// The region-local answer of `op`, memoised at the op's generation: its local answer joined with
/// the region-local answers of the ops of its regions, the write rule applied and the floors met.
///
/// Reads nothing outside the subtree of `op`.
fn regional(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> OpEffects {
	let generation = ir.generation(op);
	let key = cx.memo_key();
	if let Some(hit) = facts.get(op, generation, &key) {
		return hit.effects.clone();
	}
	let Some(data) = ir.op(op) else {
		return OpEffects::top();
	};
	let mut out = local(ir, facts, cx, op);
	let loop_owner = is_loop(&data.kind);
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			out = join(&out, &OpEffects::top());
			continue;
		};
		if !runs(&data.kind, info.kind) {
			continue;
		}
		let mut folded = bottom();
		for child in ops_in(ir, region) {
			folded = join(&folded, &regional(ir, facts, cx, child));
		}
		if loop_owner && is_loop_body(info.kind) {
			folded = widen_keys(folded);
		}
		out = join(&out, &folded);
	}
	if is_write(&data.kind) && !is_logical_write(&data.kind) && !holds_write_stage(ir, op) {
		out.footprint.reads = smallvec![IrResource::Unknown];
		out.footprint.writes = smallvec![IrResource::Unknown];
	}
	let out = settle(out);
	facts.insert(
		op,
		generation,
		key,
		OpFacts {
			effects: out.clone(),
		},
	);
	out
}

/// The effects of the body of the closure `creator` creates: the transitive answers of the ops of
/// its `ClosureBody` region, with every element key widened, since no iteration of an enclosing
/// loop is the closure's element.
///
/// Never memoised: it is attributed to the ops that invoke the closure, which stand outside the
/// closure's subtree.
pub(super) fn closure_body(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	creator: OpId,
) -> OpEffects {
	let Some(data) = ir.op(creator) else {
		return OpEffects::top();
	};
	let mut out = bottom();
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			out = join(&out, &OpEffects::top());
			continue;
		};
		if info.kind != RegionKind::ClosureBody {
			continue;
		}
		for child in ops_in(ir, region) {
			out = join(&out, &of_with(ir, facts, cx, child));
		}
	}
	settle(widen_keys(out))
}

/// Every closure creation whose body belongs to an op of the subtree of `root`, once each, in the
/// order they are met.
///
/// The `ClosureBody` of a creation is not entered: what runs inside it runs only where the
/// closure is invoked, which is a different op.
pub(super) fn collect_invoked(ir: &Ir, root: OpId, out: &mut SmallVec<[OpId; 2]>) {
	let Some(data) = ir.op(root) else {
		return;
	};
	if takes_closures(&data.kind) {
		for operand in data.operands.iter().copied() {
			if let Some(creator) = closure_of(ir, operand)
				&& !out.contains(&creator)
			{
				out.push(creator);
			}
		}
	}
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			continue;
		};
		if !runs(&data.kind, info.kind) {
			continue;
		}
		for child in ops_in(ir, region) {
			collect_invoked(ir, child, out);
		}
	}
}

/// The `expr.closure_create` that `value` carries, following the result of a `logical.let` to the
/// value it binds and the dispatch of an invocation to its target. `None` when the value is
/// anything else, which makes it an escape.
fn closure_of(ir: &Ir, value: ValueId) -> Option<OpId> {
	let mut current = value;
	for _ in 0..MAX_CLOSURE_HOPS {
		let ValueDef::OpResult {
			op,
			..
		} = ir.value(current)?.def
		else {
			return None;
		};
		let data = ir.op(op)?;
		let tag = data.kind.tag();
		if tag == OpTag::Expr(ExprTag::ClosureCreate) {
			return Some(op);
		}
		let through = if tag == OpTag::Logical(LogTag::Let) {
			true
		} else if let OpKind::Expr(ExprOp::Dispatch(target)) = &data.kind {
			**target == DispatchTarget::Invoke
		} else {
			false
		};
		if !through {
			return None;
		}
		current = *data.operands.first()?;
	}
	None
}

/// Whether an op of `kind` runs the closures among its operands: a closure loop, an invocation, a
/// call or a method call.
fn takes_closures(kind: &OpKind) -> bool {
	if let OpKind::Logical(LogOp::Loop {
		kind: loop_kind,
		..
	}) = kind
	{
		return *loop_kind == LoopKind::Closure;
	}
	let tag = kind.tag();
	tag == OpTag::Expr(ExprTag::Invoke)
		|| tag == OpTag::Expr(ExprTag::Call)
		|| tag == OpTag::Expr(ExprTag::Method)
}

/// Whether the region of kind `region` owned by an op of kind `owner` runs as part of the owner:
/// a stored clause never runs, and a closure body runs only where the closure is invoked.
pub(super) fn runs(owner: &OpKind, region: RegionKind) -> bool {
	if let RegionKind::StoredClause(_) = region {
		return false;
	}
	!(region == RegionKind::ClosureBody && owner.tag() == OpTag::Expr(ExprTag::ClosureCreate))
}

pub(super) fn is_loop(kind: &OpKind) -> bool {
	let tag = kind.tag();
	tag == OpTag::Logical(LogTag::Loop) || tag == OpTag::Phys(PhysTag::Loop)
}

/// Whether `kind` is the region of a loop that runs once per element.
pub(super) fn is_loop_body(kind: RegionKind) -> bool {
	kind == RegionKind::Loop || kind == RegionKind::StatementFor || kind == RegionKind::ValueFor
}

/// Whether `kind` is a write statement, logical or physical.
pub(crate) fn is_write(kind: &OpKind) -> bool {
	let tag = kind.tag();
	tag == OpTag::Logical(LogTag::Write) || tag == OpTag::Phys(PhysTag::Write)
}

/// Whether `kind` is a `logical.write`, whose stage ops stand in the subject loop beside it.
pub(super) fn is_logical_write(kind: &OpKind) -> bool {
	kind.tag() == OpTag::Logical(LogTag::Write)
}

/// Whether the write `write` shows the stored code it runs: a write-stage op with a non-empty
/// region under the write, or, for a logical write, under the subject loop whose result it
/// consumes.
pub(super) fn shows_write_stage(ir: &Ir, write: OpId) -> bool {
	holds_write_stage(ir, write)
		|| types::subject_loop(ir, write).is_some_and(|subject| holds_write_stage(ir, subject))
}

/// Whether the subtree of `root`, the regions that run as part of it, holds a logical write that
/// does not show its stage code. Reads the subject loop of each such write, which stands outside
/// the write's own subtree.
fn hides_stage_code(ir: &Ir, root: OpId) -> bool {
	let Some(data) = ir.op(root) else {
		return false;
	};
	if is_logical_write(&data.kind) && !shows_write_stage(ir, root) {
		return true;
	}
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			continue;
		};
		if !runs(&data.kind, info.kind) {
			continue;
		}
		if ops_in(ir, region).any(|child| hides_stage_code(ir, child)) {
			return true;
		}
	}
	false
}

/// Whether the subtree of `root` shows the stored code a write runs: a write-stage op whose region
/// holds ops.
///
/// A stage op with an empty region shows no code, so it neither shows the code a table's events,
/// views and references would run nor that the table has none.
pub(super) fn holds_write_stage(ir: &Ir, root: OpId) -> bool {
	let Some(data) = ir.op(root) else {
		return false;
	};
	for region in data.regions.iter().copied() {
		for child in ops_in(ir, region) {
			let Some(child_data) = ir.op(child) else {
				continue;
			};
			let tag = child_data.kind.tag();
			let is_stage = tag == OpTag::Logical(LogTag::WriteStage)
				|| tag == OpTag::Phys(PhysTag::WriteStage);
			if is_stage && shows_code(ir, child_data.regions.iter().copied()) {
				return true;
			}
			if holds_write_stage(ir, child) {
				return true;
			}
		}
	}
	false
}

/// Whether any of `regions` holds an op.
fn shows_code(ir: &Ir, mut regions: impl Iterator<Item = RegionId>) -> bool {
	regions.any(|region| ops_in(ir, region).next().is_some())
}

/// `effects` with every element key of its footprint widened to `KeyOf::Any`.
fn widen_keys(mut effects: OpEffects) -> OpEffects {
	effects.footprint.reads = widen_list(&effects.footprint.reads);
	effects.footprint.writes = widen_list(&effects.footprint.writes);
	effects
}

fn widen_list(list: &[IrResource]) -> SmallVec<[IrResource; 2]> {
	let mut out: SmallVec<[IrResource; 2]> = SmallVec::new();
	for resource in list {
		let widened = match resource {
			IrResource::Data {
				table,
				fields,
				key: KeyOf::Elem(_),
			} => IrResource::Data {
				table: table.clone(),
				fields: fields.clone(),
				key: KeyOf::Any,
			},
			IrResource::Data {
				key: KeyOf::Any,
				..
			}
			| IrResource::Catalog {
				..
			}
			| IrResource::Snapshot(_)
			| IrResource::Cascade {
				..
			}
			| IrResource::Reject {
				..
			}
			| IrResource::Queue {
				..
			}
			| IrResource::Session {
				..
			}
			| IrResource::Unknown => resource.clone(),
		};
		if !out.contains(&widened) {
			out.push(widened);
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::iface::effects::{ElemPath, FieldSet};

	fn data(table: &str, key: KeyOf) -> IrResource {
		IrResource::Data {
			table: Some(TableName::new(table)),
			fields: FieldSet::All,
			key,
		}
	}

	/// A spread of answers over every lattice the join ranges over.
	fn samples() -> Vec<OpEffects> {
		let mut reading = bottom();
		reading.set = EffectSet::READS_DATA;
		reading.fallibility = Fallibility::DATA;
		reading.footprint.reads = smallvec![data("a", KeyOf::Elem(ElemPath::Id))];
		reading.determinism = Determinism::WithinStatementUnlessWritten;

		let mut writing = bottom();
		writing.set = EffectSet::WRITES_DATA;
		writing.fallibility = Fallibility::VALUE;
		writing.footprint.writes = smallvec![data("a", KeyOf::Any), data("b", KeyOf::Any)];
		writing.rollback = Rollback::Undone;
		writing.determinism = Determinism::NonDeterministic;

		let mut clock = bottom();
		clock.set = EffectSet::CLOCK;
		clock.context_deps = smallvec![ContextDep::Capabilities];
		clock.determinism = Determinism::PerStatement;

		let mut external = bottom();
		external.set = EffectSet::READS_EXTERNAL;
		external.context_deps = smallvec![ContextDep::Capabilities, ContextDep::PrincipalClass];
		external.rollback = Rollback::Survives;

		vec![bottom(), reading, writing, clock, external, OpEffects::top()]
	}

	#[test]
	fn join_is_commutative_and_bottom_is_identity() {
		let samples = samples();
		for a in &samples {
			assert_eq!(&join(a, &bottom()), a, "bottom is the identity");
			assert_eq!(&join(&bottom(), a), a, "bottom is the identity");
			assert_eq!(&join(a, a), a, "join is idempotent");
			for b in &samples {
				assert_eq!(join(a, b), join(b, a), "join is commutative");
				for c in &samples {
					assert_eq!(join(&join(a, b), c), join(a, &join(b, c)), "join is associative");
				}
			}
		}
	}

	#[test]
	fn join_unions_deduplicates_and_collapses_unknown() {
		let mut a = bottom();
		a.context_deps = smallvec![ContextDep::Capabilities];
		a.footprint.reads = smallvec![data("t", KeyOf::Any)];
		let mut b = bottom();
		b.context_deps = smallvec![ContextDep::PrincipalClass, ContextDep::Capabilities];
		b.footprint.reads = smallvec![data("t", KeyOf::Any), data("u", KeyOf::Any)];
		let joined = join(&a, &b);
		assert_eq!(
			joined.context_deps.as_slice(),
			&[ContextDep::Capabilities, ContextDep::PrincipalClass]
		);
		assert_eq!(
			joined.footprint.reads.as_slice(),
			&[data("t", KeyOf::Any), data("u", KeyOf::Any)]
		);
		let mut unknown = bottom();
		unknown.footprint.reads = smallvec![IrResource::Unknown];
		assert_eq!(join(&a, &unknown).footprint.reads.as_slice(), &[IrResource::Unknown]);
	}

	#[test]
	fn determinism_and_rollback_order() {
		let determinism = [
			Determinism::Deterministic,
			Determinism::PerStatement,
			Determinism::WithinStatementUnlessWritten,
			Determinism::NonDeterministic,
		];
		for (i, a) in determinism.iter().enumerate() {
			for (j, b) in determinism.iter().enumerate() {
				assert_eq!(join_determinism(*a, *b), determinism[i.max(j)]);
			}
		}
		let rollback = [Rollback::NoEffects, Rollback::Undone, Rollback::Survives];
		for (i, a) in rollback.iter().enumerate() {
			for (j, b) in rollback.iter().enumerate() {
				assert_eq!(join_rollback(*a, *b), rollback[i.max(j)]);
			}
		}
	}

	#[test]
	fn floors_follow_the_set() {
		let floored = |set: EffectSet| {
			let mut effects = bottom();
			effects.set = set;
			settle(effects)
		};
		assert_eq!(
			floored(EffectSet::READS_DATA).determinism,
			Determinism::WithinStatementUnlessWritten
		);
		assert_eq!(floored(EffectSet::CLOCK).determinism, Determinism::PerStatement);
		assert_eq!(
			floored(EffectSet::CLOCK | EffectSet::READS_DATA).determinism,
			Determinism::WithinStatementUnlessWritten
		);
		assert_eq!(floored(EffectSet::ENTROPY).determinism, Determinism::NonDeterministic);
		assert_eq!(floored(EffectSet::WRITES_DATA).rollback, Rollback::Undone);
		assert_eq!(floored(EffectSet::CHANGES_CATALOG).rollback, Rollback::Undone);
		for survivor in [
			EffectSet::WRITES_EXTERNAL,
			EffectSet::READS_EXTERNAL,
			EffectSet::OPAQUE_BODY,
			EffectSet::NOTIFY,
			EffectSet::ADVANCES_STATE,
		] {
			assert_eq!(floored(survivor).rollback, Rollback::Survives);
			assert_eq!(floored(survivor | EffectSet::WRITES_DATA).rollback, Rollback::Survives);
		}
		assert_eq!(floored(EffectSet::PURE), bottom());
	}

	#[test]
	fn widening_replaces_element_keys_only() {
		let mut effects = bottom();
		effects.footprint.reads = smallvec![
			data("t", KeyOf::Elem(ElemPath::Id)),
			data("t", KeyOf::Elem(ElemPath::Elem)),
			IrResource::Session {
				name: Strand::new("s"),
			},
		];
		let widened = widen_keys(effects);
		assert_eq!(
			widened.footprint.reads.as_slice(),
			&[
				data("t", KeyOf::Any),
				IrResource::Session {
					name: Strand::new("s"),
				},
			]
		);
	}
}
