//! Row dependence and loop invariance: which values vary with the row or element a loop visits.
//!
//! A value that does not vary with the row is invariant in the loop and may be computed once. The
//! answer for a value asked against a loop `scope` is [`Invariance::Invariant`] when the value
//! answers the same at every reach within one execution of `scope`, and otherwise names the first
//! cause found.
//!
//! The answer is pure dependence, in three parts:
//!
//! - **Structure.** A value defined outside the body of `scope` is invariant. A block argument of a
//!   region that lies inside the body and encloses the asked value varies per iteration. An op is
//!   invariant when every operand it takes from outside its own subtree, effect tokens included, is
//!   invariant. A region the op owns is rebound on every evaluation of the op, so a block argument
//!   of one is internal to the op. A closure body is never entered: a closure creation with
//!   invariant captures is invariant.
//! - **Effects.** An effect is a token operand. An op that writes a class of state takes and
//!   produces the token of the class, and a loop whose body produces a class carries it in a body
//!   argument, so what reads or changes the state the loop changes depends on a value the body
//!   defines. A read in a loop that produces nothing of its class reads a token defined before the
//!   loop. A token argument of a region isolated from above stands for the token its owner passes
//!   in.
//! - **Position.** The regions between the value and `scope` are only those of the statement's own
//!   inlined code, and no `Return`, `Break` or `Next` inside the op leaves it.
//!
//! Dependence reads the tokens of a threaded container. An unthreaded container answers that no
//! value is invariant, and so does a value whose subtree holds an op created after the threading:
//! such an op holds no token, so nothing says what state it orders itself against.
//!
//! Answers are not kept between queries. An answer depends on ops outside the asked op's subtree,
//! and a rewrite of a sibling does not change the generation of the asked op, so a kept answer
//! could go stale.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::{effects, multiplicity};
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ops_in, region_return_rule};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{LoopRule, RegionKind, RegionProperties, ReturnRule};
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpTag;

/// The most regions followed on the way up from an op. Nesting is finite by construction, so a
/// well-formed container never meets the bound.
const MAX_HOPS: usize = 1024;

/// The deepest chain of defining ops followed from an operand. An answer that meets the bound is
/// not invariant.
const MAX_DEPTH: usize = 256;

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
	/// A region kind a value may not be carried across.
	Barrier(RegionKind),
	/// A `Return`, `Break` or `Next` inside the op whose target lies outside it.
	Terminator(OpId),
	/// A body nobody can see, an op the analysis cannot answer for, an op created after the
	/// container was threaded, or a container no token is threaded through.
	Opaque(OpId),
}

/// Whether `value` answers the same at every reach within one execution of the loop `scope`.
///
/// A value defined outside the body of `scope` is always invariant. A dangling value or scope, a
/// scope with no per-element body, an unthreaded container and a scope inside a stored clause,
/// which holds no token, answer `Dependent(Opaque(scope))`. The memo inside one query holds only
/// ops judged in that query.
pub(crate) fn invariant_in(ir: &Ir, value: ValueId, scope: OpId) -> Invariance {
	let body = body_regions(ir, scope);
	if body.is_empty() || !ir.is_threaded() || in_stored_clause(ir, scope) {
		return Invariance::Dependent(DepCause::Opaque(scope));
	}
	let mut query = Query {
		ir,
		scope,
		body,
		memo: FxHashMap::default(),
	};
	query.value(value, 0)
}

/// The outermost enclosing loop of `value`'s position for which the value is invariant.
///
/// Loops are tried from the innermost outward and the walk ends at the first loop the value
/// depends on, so the answer is the last of an unbroken run of loops the value is invariant in.
/// `None` when the value is invariant in none, and for a dangling value.
pub(crate) fn outermost_scope(ir: &Ir, value: ValueId) -> Option<OpId> {
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
		match invariant_in(ir, value, scope) {
			Invariance::Invariant => best = Some(scope),
			Invariance::Dependent(_) => break,
		}
		next = loop_containing(ir, ir.enclosing_region(scope));
	}
	best
}

/// Whether `op` stands inside a stored clause: code the statement stores and never runs, which no
/// token is threaded through.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	let mut region = ir.enclosing_region(op);
	for _ in 0..MAX_HOPS {
		let Some(info) = ir.region(region) else {
			return false;
		};
		if matches!(info.kind, RegionKind::StoredClause(_)) {
			return true;
		}
		let Some(owner) = ir.parent_op(region) else {
			return false;
		};
		region = ir.enclosing_region(owner);
	}
	false
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

/// The ops of the subtree of `root`, `root` first, and the regions those ops own.
///
/// A closure body is not entered: what runs in it runs where the closure is invoked.
fn collect(ir: &Ir, root: OpId, ops: &mut Vec<OpId>, regions: &mut FxHashSet<RegionId>) {
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
fn escapes(ir: &Ir, op: OpId, inside: &FxHashSet<RegionId>) -> bool {
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
			match region_return_rule(ir, region) {
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

/// Whether `value` is defined by an op of `ops` or is an argument of a region of `regions`.
fn defined_within(
	ir: &Ir,
	value: ValueId,
	ops: &FxHashSet<OpId>,
	regions: &FxHashSet<RegionId>,
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
struct Query<'a> {
	ir: &'a Ir,
	scope: OpId,
	body: SmallVec<[RegionId; 2]>,
	memo: FxHashMap<OpId, Invariance>,
}

impl Query<'_> {
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
				if !self.in_body(info.parent) {
					return Invariance::Invariant;
				}
				match self.passed_in(value, info.parent) {
					Some(passed) => self.value(passed, depth + 1),
					None => Invariance::Dependent(DepCause::BlockArg(value)),
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
		let mut regions = FxHashSet::default();
		collect(ir, op, &mut ops, &mut regions);
		let inside: FxHashSet<OpId> = ops.iter().copied().collect();
		for inner in ops.iter().copied() {
			let Some(data) = ir.op(inner) else {
				return Invariance::Dependent(DepCause::Opaque(inner));
			};
			let tag = data.kind.tag();
			if is_control_terminator(tag) && escapes(ir, inner, &regions) {
				return Invariance::Dependent(DepCause::Terminator(inner));
			}
			if !data.kind.is_terminator() && ir.holds_no_tokens(inner) {
				return Invariance::Dependent(DepCause::Opaque(inner));
			}
			for operand in data.operands.iter().chain(ir.token_operands(inner)).copied() {
				if defined_within(ir, operand, &inside, &regions) {
					continue;
				}
				if let Invariance::Dependent(cause) = self.value(operand, depth + 1) {
					return Invariance::Dependent(cause);
				}
			}
		}
		Invariance::Invariant
	}

	/// The value the owner of a region passes in for the token argument `arg` of `region`: for a
	/// region isolated from above, the token the owner takes of the argument's class, or the token
	/// it produces of the class when the region runs after the owner's effect. `None` for any other
	/// argument, including a loop body's own, and for an owner that holds no token of the class.
	fn passed_in(&self, arg: ValueId, region: RegionId) -> Option<ValueId> {
		let ir = self.ir;
		let class = ir.token_class(arg)?;
		let kind = ir.region(region)?.kind;
		let owner = ir.parent_op(region)?;
		let follows = effects::follows_effect(&ir.op(owner)?.kind);
		let after = follows && !multiplicity::is_loop_body(kind);
		if !kind.isolated_from_above() && !after {
			return None;
		}
		if follows && let Some(produced) = ir.token_of_class(ir.token_results(owner), class) {
			return Some(produced);
		}
		ir.token_of_class(ir.token_operands(owner), class)
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
}
