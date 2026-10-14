//! Row dependence and loop invariance: which values vary with the row or element a loop visits.
//!
//! A value that does not vary with the row is invariant in the loop and may be computed once. The
//! answer for a value asked against a loop `scope` is [`Invariance::Invariant`] when the value
//! answers the same at every reach within one execution of `scope`, and otherwise names the first
//! cause found.
//!
//! The answer is built from three layers, in this order:
//!
//! - **Structure.** A value defined outside the body of `scope` is invariant. A block argument of a
//!   region that lies inside the body and encloses the asked value varies per iteration. An op is
//!   invariant when every operand it takes from outside its own subtree is invariant. A region the
//!   op owns is rebound on every evaluation of the op, so a block argument of one is internal to
//!   the op. A closure body is never entered: a closure creation with invariant captures is
//!   invariant. `expr.now` is invariant when its timestamp is the argument of a statement.
//! - **Position.** The regions between the value and `scope` are only those of the statement's own
//!   inlined code, and no `Return`, `Break` or `Next` inside the op leaves it.
//! - **Effects.** The transitive effects of the op allow one evaluation: nothing writes or is
//!   non-deterministic, every read of stored data is out of reach of a write of the statement, and
//!   a catalog read is not followed by a catalog change of the same statement.
//!
//! Answers are not kept between queries. An answer depends on ops outside the asked op's subtree,
//! and a rewrite of a sibling does not change the generation of the asked op, so a kept answer
//! could go stale.

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::{HashMap, HashSet};

use smallvec::SmallVec;

use super::cx::default_cx;
use super::{effects, footprint, multiplicity, write_epoch};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{Determinism, IrResource};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId, WriteEpoch};
use crate::exec::ir::iface::region_kind::{LoopRule, RegionKind, RegionProperties, ReturnRule};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpTag;
use crate::expr::logical::EffectSet;

/// The most regions followed on the way up from an op. Nesting is finite by construction, so a
/// well-formed container never meets the bound.
const MAX_HOPS: usize = 1024;

/// The deepest chain of defining ops followed from an operand. An answer that meets the bound is
/// not invariant.
const MAX_DEPTH: usize = 256;

/// The flags whose presence forbids one evaluation because the op changes something.
const WRITE_FLAGS: [EffectSet; 6] = [
	EffectSet::WRITES_DATA,
	EffectSet::WRITES_EXTERNAL,
	EffectSet::NOTIFY,
	EffectSet::DEFERS,
	EffectSet::CHANGES_CATALOG,
	EffectSet::WRITES_SESSION,
];

/// The flags that make two evaluations of an op differ, the clock excepted.
const NON_DETERMINISTIC_FLAGS: [EffectSet; 11] = [
	EffectSet::WRITES_DATA,
	EffectSet::WRITES_EXTERNAL,
	EffectSet::READS_EXTERNAL,
	EffectSet::ADVANCES_STATE,
	EffectSet::ENTROPY,
	EffectSet::BLOCKING,
	EffectSet::OPAQUE_BODY,
	EffectSet::WRITES_SESSION,
	EffectSet::NOTIFY,
	EffectSet::DEFERS,
	EffectSet::CHANGES_CATALOG,
];

/// Whether a value answers the same at every reach within one execution of a loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Invariance {
	Invariant,
	Dependent(DepCause),
}

/// Why a value is not invariant in a loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DepCause {
	/// A block argument of a region that lies between the value and the loop, the loop's own body
	/// included.
	BlockArg(ValueId),
	/// The flags of the op's effects that forbid one evaluation.
	Effect(EffectSet),
	/// A read of stored data that a write of the statement may reach.
	ReachedByWrite(OpId),
	/// A region kind a value may not be carried across.
	Barrier(RegionKind),
	/// A `Return`, `Break` or `Next` inside the op whose target lies outside it.
	Terminator(OpId),
	/// A body nobody can see, or an op the analysis cannot answer for.
	Opaque(OpId),
	/// A catalog read in a statement that also changes the catalog; names the change.
	CatalogChange(OpId),
}

/// Whether `value` answers the same at every reach within one execution of the loop `scope`, under
/// the default context.
pub(crate) fn invariant_in(
	ir: &Ir,
	facts: &mut FactCache,
	value: ValueId,
	scope: OpId,
) -> Invariance {
	invariant_in_with(ir, facts, &default_cx(ir), value, scope)
}

/// Whether `value` answers the same at every reach within one execution of the loop `scope`,
/// under `cx`.
///
/// A value defined outside the body of `scope` is always invariant. A dangling value or scope, and
/// a scope with no per-element body, answer `Dependent(Opaque(scope))`. The memo inside one query
/// holds only ops judged in that query.
pub(crate) fn invariant_in_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
	scope: OpId,
) -> Invariance {
	let body = body_regions(ir, scope);
	if body.is_empty() {
		return Invariance::Dependent(DepCause::Opaque(scope));
	}
	let mut query = Query {
		ir,
		facts,
		cx,
		scope,
		body,
		memo: HashMap::new(),
		catalog_change: None,
		writes: write_epoch::StatementWrites::default(),
	};
	query.value(value, 0)
}

/// The outermost enclosing loop of `value`'s position for which the value is invariant, under the
/// default context.
pub(crate) fn outermost_scope(ir: &Ir, facts: &mut FactCache, value: ValueId) -> Option<OpId> {
	outermost_scope_with(ir, facts, &default_cx(ir), value)
}

/// The outermost enclosing loop of `value`'s position for which the value is invariant, under
/// `cx`.
///
/// Loops are tried from the innermost outward and the walk ends at the first loop the value
/// depends on, so the answer is the last of an unbroken run of loops the value is invariant in.
/// `None` when the value is invariant in none, and for a dangling value.
pub(crate) fn outermost_scope_with(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
) -> Option<OpId> {
	let start = match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).map(|_| ir.enclosing_region(op))?,
		ValueDef::BlockArg {
			block,
			..
		} => ir.block(block)?.parent,
	};
	let mut best = None;
	let mut next = loop_containing(ir, start);
	while let Some(scope) = next {
		match invariant_in_with(ir, facts, cx, value, scope) {
			Invariance::Invariant => best = Some(scope),
			Invariance::Dependent(_) => break,
		}
		next = loop_containing(ir, ir.enclosing_region(scope));
	}
	best
}

/// The nearest logical loop whose body contains `region`, the region itself included.
fn loop_containing(ir: &Ir, region: RegionId) -> Option<OpId> {
	let mut current = region;
	for _ in 0..MAX_HOPS {
		let info = ir.region(current)?;
		let owner = ir.parent_op(current)?;
		if multiplicity::is_loop_body(info.kind)
			&& ir.op(owner)?.kind.tag() == OpTag::Logical(LogTag::Loop)
		{
			return Some(owner);
		}
		current = ir.enclosing_region(owner);
	}
	None
}

/// The per-element body regions of the loop `scope`.
fn body_regions(ir: &Ir, scope: OpId) -> SmallVec<[RegionId; 2]> {
	let mut out = SmallVec::new();
	if let Some(data) = ir.op(scope) {
		for region in data.regions.iter().copied() {
			if ir.region(region).is_some_and(|info| multiplicity::is_loop_body(info.kind)) {
				out.push(region);
			}
		}
	}
	out
}

/// Whether a value may be carried across a region of `kind`: the regions of a statement's own
/// inlined code. Every other region runs on its own, once per invocation, and is not the body of
/// the loop it stands in.
fn crossable(kind: RegionKind) -> bool {
	match kind {
		RegionKind::Loop
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Clause(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::Subquery
		| RegionKind::LazyOperand
		| RegionKind::ErrorScope(_)
		| RegionKind::Deadline
		| RegionKind::ChooseArm
		| RegionKind::PermissionFrame
		| RegionKind::FieldFrame(_) => true,
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::StoredClause(_) => false,
	}
}

/// The flags of `flags` that `set` holds.
fn present(set: EffectSet, flags: &[EffectSet]) -> EffectSet {
	flags.iter().fold(EffectSet::PURE, |found, flag| {
		if set.contains(*flag) {
			found.union(*flag)
		} else {
			found
		}
	})
}

/// Whether `value` is the timestamp argument of a statement.
fn is_statement_timestamp(ir: &Ir, value: ValueId) -> bool {
	let Some(data) = ir.value(value) else {
		return false;
	};
	let ValueDef::BlockArg {
		block,
		..
	} = data.def
	else {
		return false;
	};
	ir.block(block)
		.and_then(|block| ir.region(block.parent))
		.is_some_and(|region| matches!(region.kind, RegionKind::Statement(_)))
}

/// The ops of the subtree of `root`, `root` first, and the regions those ops own.
///
/// A closure body is not entered: what runs in it runs where the closure is invoked.
fn collect(ir: &Ir, root: OpId, ops: &mut Vec<OpId>, regions: &mut HashSet<RegionId>) {
	let Some(data) = ir.op(root) else {
		return;
	};
	ops.push(root);
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			continue;
		};
		regions.insert(region);
		if !effects::runs(&data.kind, info.kind) {
			continue;
		}
		for child in ops_in(ir, region) {
			collect(ir, child, ops, regions);
		}
	}
}

/// Whether the terminator `op` leaves the subtree whose owned regions are `inside`.
///
/// A `Return` is caught by the nearest region that catches it, a `Break` or `Next` by the nearest
/// loop body; a region a loop-control walk may not leave, and the top of the container, are
/// outside.
fn escapes(ir: &Ir, op: OpId, inside: &HashSet<RegionId>) -> bool {
	let Some(data) = ir.op(op) else {
		return true;
	};
	let is_return = data.kind.tag() == OpTag::Logical(LogTag::Return);
	let mut region = ir.enclosing_region(op);
	for _ in 0..MAX_HOPS {
		let Some(info) = ir.region(region) else {
			return true;
		};
		if is_return {
			match info.kind.return_rule() {
				ReturnRule::CatchesReturn => return !inside.contains(&region),
				ReturnRule::PassesOutward => {}
				ReturnRule::NeverReached => return true,
			}
		} else {
			match info.kind.loop_rule() {
				LoopRule::Target => return !inside.contains(&region),
				LoopRule::Passes => {}
				LoopRule::Barrier => return true,
			}
		}
		let Some(owner) = ir.parent_op(region) else {
			return true;
		};
		region = ir.enclosing_region(owner);
	}
	true
}

/// Whether `tag` ends the iteration or the loop, or returns from the statement.
fn is_control_terminator(tag: OpTag) -> bool {
	tag == OpTag::Logical(LogTag::Return)
		|| tag == OpTag::Logical(LogTag::Break)
		|| tag == OpTag::Logical(LogTag::Next)
}

/// The outermost region of kind `Statement` that holds `op`, else the top region.
fn enclosing_statement(ir: &Ir, op: OpId) -> RegionId {
	let mut region = ir.enclosing_region(op);
	let mut found = None;
	for _ in 0..MAX_HOPS {
		if ir.region(region).is_some_and(|info| matches!(info.kind, RegionKind::Statement(_))) {
			found = Some(region);
		}
		let Some(owner) = ir.parent_op(region) else {
			break;
		};
		region = ir.enclosing_region(owner);
	}
	found.unwrap_or_else(|| ir.top())
}

/// Whether `value` is defined by an op of `ops` or is an argument of a region of `regions`.
fn defined_within(
	ir: &Ir,
	value: ValueId,
	ops: &HashSet<OpId>,
	regions: &HashSet<RegionId>,
) -> bool {
	let Some(data) = ir.value(value) else {
		return false;
	};
	match data.def {
		ValueDef::OpResult {
			op,
			..
		} => ops.contains(&op),
		ValueDef::BlockArg {
			block,
			..
		} => ir.block(block).is_some_and(|block| regions.contains(&block.parent)),
	}
}

/// One invariance query: the scope, the ops already judged and the context.
struct Query<'a, 'b> {
	ir: &'a Ir,
	facts: &'a mut FactCache,
	cx: &'a EffectCx<'b>,
	scope: OpId,
	body: SmallVec<[RegionId; 2]>,
	memo: HashMap<OpId, Invariance>,
	/// The catalog change of the statement holding the scope, found on first use.
	catalog_change: Option<Option<OpId>>,
	/// The write set of each statement a candidate read stands in, computed on first use.
	writes: write_epoch::StatementWrites,
}

impl Query<'_, '_> {
	/// Whether `region` is a body of the scope or lies inside one.
	fn in_body(&self, region: RegionId) -> bool {
		let ir = self.ir;
		let mut current = region;
		for _ in 0..MAX_HOPS {
			if self.body.contains(&current) {
				return true;
			}
			let Some(owner) = ir.parent_op(current) else {
				return false;
			};
			current = ir.enclosing_region(owner);
		}
		false
	}

	/// The invariance of `value`.
	fn value(&mut self, value: ValueId, depth: usize) -> Invariance {
		let ir = self.ir;
		let opaque = Invariance::Dependent(DepCause::Opaque(self.scope));
		let Some(data) = ir.value(value) else {
			return opaque;
		};
		match data.def {
			ValueDef::BlockArg {
				block,
				..
			} => {
				let Some(info) = ir.block(block) else {
					return opaque;
				};
				if self.in_body(info.parent) {
					Invariance::Dependent(DepCause::BlockArg(value))
				} else {
					Invariance::Invariant
				}
			}
			ValueDef::OpResult {
				op,
				..
			} => {
				if ir.op(op).is_none() {
					return opaque;
				}
				if !self.in_body(ir.enclosing_region(op)) {
					return Invariance::Invariant;
				}
				self.op(op, depth)
			}
		}
	}

	/// The invariance of the op `op`, which stands in the body of the scope.
	fn op(&mut self, op: OpId, depth: usize) -> Invariance {
		if let Some(known) = self.memo.get(&op) {
			return known.clone();
		}
		if depth > MAX_DEPTH {
			return Invariance::Dependent(DepCause::Opaque(op));
		}
		let answer = self.judge(op, depth);
		self.memo.insert(op, answer.clone());
		answer
	}

	fn judge(&mut self, op: OpId, depth: usize) -> Invariance {
		let ir = self.ir;
		if let Some(kind) = self.barrier_on_path(op) {
			return Invariance::Dependent(DepCause::Barrier(kind));
		}
		let mut ops = Vec::new();
		let mut regions = HashSet::new();
		collect(ir, op, &mut ops, &mut regions);
		let inside: HashSet<OpId> = ops.iter().copied().collect();
		for inner in ops.iter().copied() {
			let Some(data) = ir.op(inner) else {
				return Invariance::Dependent(DepCause::Opaque(inner));
			};
			let tag = data.kind.tag();
			if is_control_terminator(tag) && escapes(ir, inner, &regions) {
				return Invariance::Dependent(DepCause::Terminator(inner));
			}
			let clock = tag == OpTag::Expr(ExprTag::Now);
			for operand in data.operands.iter().copied() {
				if clock && is_statement_timestamp(ir, operand) {
					continue;
				}
				if defined_within(ir, operand, &inside, &regions) {
					continue;
				}
				if let Invariance::Dependent(cause) = self.value(operand, depth + 1) {
					return Invariance::Dependent(cause);
				}
			}
		}
		self.effect_conditions(op, &ops)
	}

	/// The first region on the way from `op` up through the body of the scope that a value may
	/// not be carried across.
	fn barrier_on_path(&self, op: OpId) -> Option<RegionKind> {
		let ir = self.ir;
		let mut region = ir.enclosing_region(op);
		for _ in 0..MAX_HOPS {
			let kind = ir.region(region)?.kind;
			if !crossable(kind) {
				return Some(kind);
			}
			if self.body.contains(&region) {
				return None;
			}
			let owner = ir.parent_op(region)?;
			region = ir.enclosing_region(owner);
		}
		None
	}

	/// The conditions the transitive effects of `op` must meet for one evaluation to serve every
	/// reach, in the order a cause is reported. `ops` is the subtree of `op`.
	///
	/// An `expr.computed_read` is judged here like any op: its own transitive answer decides.
	fn effect_conditions(&mut self, op: OpId, ops: &[OpId]) -> Invariance {
		let (ir, cx) = (self.ir, self.cx);
		let answer = effects::of_with(ir, self.facts, cx, op);
		let writes = present(answer.set, &WRITE_FLAGS);
		if writes != EffectSet::PURE {
			return Invariance::Dependent(DepCause::Effect(writes));
		}
		if answer.set.contains(EffectSet::OPAQUE_BODY) {
			let at = self.locate(op, ops, EffectSet::OPAQUE_BODY);
			return Invariance::Dependent(DepCause::Opaque(at));
		}
		if answer.determinism == Determinism::NonDeterministic {
			let flags = present(answer.set, &NON_DETERMINISTIC_FLAGS);
			let cause = if flags == EffectSet::PURE {
				answer.set
			} else {
				flags
			};
			return Invariance::Dependent(DepCause::Effect(cause));
		}
		if answer.set.contains(EffectSet::READS_DATA)
			&& let Some(read) = self.reached_by_write(op, ops, &answer.footprint.reads)
		{
			return Invariance::Dependent(DepCause::ReachedByWrite(read));
		}
		if answer.set.contains(EffectSet::READS_CATALOG)
			&& let Some(change) = self.catalog_change()
		{
			return Invariance::Dependent(DepCause::CatalogChange(change));
		}
		Invariance::Invariant
	}

	/// Every op that runs as part of the subtree `ops`: the subtree and the bodies of the
	/// closures the subtree invokes.
	fn running_ops(&self, ops: &[OpId]) -> Vec<OpId> {
		let ir = self.ir;
		let mut out: Vec<OpId> = ops.to_vec();
		let mut creators: SmallVec<[OpId; 2]> = SmallVec::new();
		if let Some(root) = ops.first() {
			effects::collect_invoked(ir, *root, &mut creators);
		}
		let mut regions = HashSet::new();
		for creator in creators {
			let Some(data) = ir.op(creator) else {
				continue;
			};
			for region in data.regions.iter().copied() {
				if ir.region(region).is_some_and(|info| info.kind == RegionKind::ClosureBody) {
					for child in ops_in(ir, region) {
						collect(ir, child, &mut out, &mut regions);
					}
				}
			}
		}
		out
	}

	/// The first op that runs as part of the subtree `ops` and carries `flag` in its local
	/// effects, else `op`.
	fn locate(&mut self, op: OpId, ops: &[OpId], flag: EffectSet) -> OpId {
		let (ir, cx) = (self.ir, self.cx);
		for candidate in self.running_ops(ops) {
			if effects::local(ir, self.facts, cx, candidate).set.contains(flag) {
				return candidate;
			}
		}
		op
	}

	/// A read of stored data inside `op` that a write of the statement may reach.
	///
	/// Every op that reads stored data by its own effects must hold a stable epoch. A read the
	/// per-op test does not isolate is still caught when the transitive reads of `op` meet a write
	/// of the statement, and then `op` itself is named.
	fn reached_by_write(&mut self, op: OpId, ops: &[OpId], reads: &[IrResource]) -> Option<OpId> {
		let (ir, cx) = (self.ir, self.cx);
		for candidate in self.running_ops(ops) {
			if !effects::local(ir, self.facts, cx, candidate).set.contains(EffectSet::READS_DATA) {
				continue;
			}
			if let WriteEpoch::Unique(_) =
				write_epoch::assign_scratch(ir, self.facts, cx, candidate, &mut self.writes)
			{
				return Some(candidate);
			}
		}
		let writes = self.writes.of(ir, self.facts, cx, op);
		let unknown = [IrResource::Unknown];
		let reads = if reads.is_empty() {
			&unknown[..]
		} else {
			reads
		};
		if reads.iter().any(|read| writes.iter().any(|write| footprint::overlaps(read, write))) {
			Some(op)
		} else {
			None
		}
	}

	/// An op that changes the catalog anywhere in the outermost statement holding the scope: the
	/// statements nested in it, loop bodies, projections and the bodies of functions the container
	/// holds included.
	fn catalog_change(&mut self) -> Option<OpId> {
		if let Some(known) = self.catalog_change {
			return known;
		}
		let (ir, cx, scope) = (self.ir, self.cx, self.scope);
		let region = enclosing_statement(ir, scope);
		let mut found = None;
		for child in ops_in(ir, region) {
			let answer = effects::of_with(ir, self.facts, cx, child);
			if answer.set.contains(EffectSet::CHANGES_CATALOG) {
				let mut ops = Vec::new();
				let mut regions = HashSet::new();
				collect(ir, child, &mut ops, &mut regions);
				found = Some(self.locate(child, &ops, EffectSet::CHANGES_CATALOG));
				break;
			}
		}
		self.catalog_change = Some(found);
		found
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::iface::region_kind::{
		ClauseRole, ErrorScope, FieldRole, StatementCommit, StoredRole, TxnKind,
	};

	#[test]
	fn a_value_crosses_only_the_regions_of_the_statements_own_code() {
		for kind in [
			RegionKind::Loop,
			RegionKind::StatementFor,
			RegionKind::ValueFor,
			RegionKind::Clause(ClauseRole::Where),
			RegionKind::Block,
			RegionKind::ValueBlock,
			RegionKind::StatementIf,
			RegionKind::ValueIf,
			RegionKind::Subquery,
			RegionKind::LazyOperand,
			RegionKind::ErrorScope(ErrorScope::ShapeToNone),
			RegionKind::Deadline,
			RegionKind::ChooseArm,
			RegionKind::PermissionFrame,
			RegionKind::FieldFrame(FieldRole::Computed),
		] {
			assert!(crossable(kind), "{kind:?} is the statement's own code");
		}
		for kind in [
			RegionKind::Request,
			RegionKind::Transaction(TxnKind::Explicit),
			RegionKind::Statement(StatementCommit::Auto),
			RegionKind::FunctionBody,
			RegionKind::ClosureBody,
			RegionKind::EventFrame,
			RegionKind::SubscriberFrame,
			RegionKind::Pipeline,
			RegionKind::JoinLookupArm,
			RegionKind::WriteStage,
			RegionKind::CatalogUnit,
			RegionKind::StoredClause(StoredRole::Value),
		] {
			assert!(!crossable(kind), "{kind:?} runs on its own");
		}
	}

	#[test]
	fn present_names_only_the_flags_the_set_holds() {
		let set = EffectSet::ENTROPY | EffectSet::READS_DATA | EffectSet::NOTIFY;
		assert_eq!(present(set, &WRITE_FLAGS), EffectSet::NOTIFY);
		assert_eq!(present(set, &NON_DETERMINISTIC_FLAGS), EffectSet::ENTROPY | EffectSet::NOTIFY);
		assert_eq!(present(EffectSet::READS_DATA, &WRITE_FLAGS), EffectSet::PURE);
	}

	#[test]
	fn every_write_flag_is_non_deterministic() {
		for flag in WRITE_FLAGS {
			assert!(NON_DETERMINISTIC_FLAGS.contains(&flag));
		}
	}
}
