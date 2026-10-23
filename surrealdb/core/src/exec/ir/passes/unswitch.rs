//! Loop unswitching: a branch on a loop-invariant condition moves out of the loop, each arm
//! keeping its own copy of the loop.
//!
//! [`Unswitch`] rewrites a `SELECT` loop whose `WHERE` or table permission predicate holds a
//! conjunct that is the same for every row into `logical.if(condition)` with two arms. Each arm
//! holds a copy of the loop and of the source only it reads, and in each copy the conjunct is the
//! literal the arm stands for, so each arm is a loop over a predicate the planner can specialise.
//! The condition is evaluated once, before the source is read, so it runs even when the source
//! holds no row; only a condition that cannot raise and has no effect beyond reads qualifies.
#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::rehome::{RehomeMode, and_chain, can_rehome, rehome_tree};
use crate::exec::ir::analysis::multiplicity::is_loop_body;
use crate::exec::ir::analysis::row_dep::{Invariance, invariant_in};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::census::Census;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{
	ControlTarget, ancestors, control_target, enclosing_statement, ops_in,
};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind};
use crate::exec::ir::op_kind::{OpKind, OpTag, Terminator};
use crate::val::Value;

/// The most ops one copy of a loop may hold: the loop, the source ops only it reads, and every op
/// inside their regions. A loop over the bound is left as it is.
pub(crate) const UNSWITCH_MAX_OPS: usize = 2048;

/// The most regions followed on the way up from an op. Nesting is finite by construction, so a
/// well-formed container never meets the bound.
const MAX_HOPS: usize = 1024;

/// Moves a branch on a loop-invariant condition out of a `SELECT` loop.
///
/// A loop qualifies when all of these hold:
///
/// - it is a `SELECT` loop with one result, never the subject loop of a write;
/// - no `RETURN`, `BREAK` or `CONTINUE` inside it lands outside it;
/// - one copy of the loop and of the source only it reads holds at most [`UNSWITCH_MAX_OPS`] ops,
///   and every region of the copy reads only values defined inside that region, so the copy can be
///   cloned;
/// - a conjunct of the `WHERE` filter or of the table permission predicate standing directly in the
///   loop's body is defined inside the loop, is read only as a boolean (an operand of `AND` or
///   `truthy`, or the value that closes the predicate), is invariant in the loop under the
///   row-dependence analysis, and cannot raise: no op that computes it is fallible. The conjunct is
///   not a literal, a named constant, a table or a yield, and every value its ops read from outside
///   them is defined outside every loop.
///
/// A condition defined outside the loop is read by the loop's regions, which cannot be cloned, so
/// such a loop is left as it is.
///
/// The first qualifying conjunct, in body order, is the condition. The loop is replaced by a
/// `logical.if` on a copy of the conjunct's ops placed before the loop; the true arm holds a copy
/// of the loop in which the conjunct is `true`, and the false arm one in which it is `false`. The
/// `if` yields the loop's value and stands in the loop's position, a statement branch where the
/// loop stood as a statement and a value branch otherwise. A loop is unswitched at most once per
/// run, outermost first, and the loops a run creates are not visited by it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Unswitch;

impl LoPass for Unswitch {
	fn name(&self) -> &'static str {
		"unswitch"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.unswitch
	}

	/// [`run_unswitch`] visits the ops of [`live_ops`] that [`is_select_loop`] accepts: a
	/// `logical.loop` outside stored code. With none it reads no fact, rewrites nothing and
	/// reports the default.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Loop)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_unswitch(cx))
	}
}

// ---------------------------------------------------------------------------------------------
// Container walks
// ---------------------------------------------------------------------------------------------

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every op outside stored clauses, in program order: an op before the ops of its regions.
fn live_ops(ir: &Ir) -> Vec<OpId> {
	let mut ops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if !in_stored_clause(ir, op) {
			ops.push(op);
		}
		Walk::Continue
	});
	ops
}

/// The op that defines `value`, or `None` for a block argument and for a dangling id.
fn def_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).map(|_| op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// Whether `op` is a `logical.loop`.
fn is_loop(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Loop))
}

/// Whether `op` is a `SELECT` loop.
fn is_select_loop(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| {
		matches!(
			data.kind,
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Select,
				..
			})
		)
	})
}

/// The per-row body regions of the loop `loop_op`.
fn body_regions(ir: &Ir, loop_op: OpId) -> Vec<RegionId> {
	let Some(data) = ir.op(loop_op) else {
		return Vec::new();
	};
	data.regions
		.iter()
		.copied()
		.filter(|region| ir.region(*region).is_some_and(|held| is_loop_body(held.kind)))
		.collect()
}

/// Whether `op` stands in a region of `ancestor`, at any depth.
fn stands_in(ir: &Ir, op: OpId, ancestor: OpId) -> bool {
	ancestors(ir, op).any(|(_, owner)| owner == Some(ancestor))
}

/// Whether `region` is a region of `loop_op` or lies inside one.
fn region_inside(ir: &Ir, region: RegionId, loop_op: OpId) -> bool {
	let mut current = region;
	for _ in 0..MAX_HOPS {
		let Some(owner) = ir.parent_op(current) else {
			return false;
		};
		if owner == loop_op {
			return true;
		}
		current = ir.enclosing_region(owner);
	}
	false
}

/// Whether `region` is the per-row body of a loop, or lies inside one.
fn inside_loop_body(ir: &Ir, region: RegionId) -> bool {
	let mut current = region;
	for _ in 0..MAX_HOPS {
		let (Some(info), Some(owner)) = (ir.region(current), ir.parent_op(current)) else {
			return false;
		};
		if is_loop_body(info.kind) && is_loop(ir, owner) {
			return true;
		}
		current = ir.enclosing_region(owner);
	}
	false
}

/// Whether a `RETURN`, `BREAK` or `CONTINUE` inside `loop_op` lands outside it, or lands nowhere
/// a region catches it.
fn leaves_loop(ir: &Ir, loop_op: OpId) -> bool {
	let Some(data) = ir.op(loop_op) else {
		return true;
	};
	let mut leaves = false;
	for region in data.regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			let escapes = match control_target(ir, op) {
				ControlTarget::NotControl => false,
				ControlTarget::Region(target) => !region_inside(ir, target, loop_op),
				ControlTarget::Escapes(_) | ControlTarget::Barrier(_) => true,
			};
			if escapes {
				leaves = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	leaves
}

// ---------------------------------------------------------------------------------------------
// Copyable trees
// ---------------------------------------------------------------------------------------------

/// The ops a copy of the tree rooted at `root` carries, in definition order: the root and the ops
/// of its operand subtree that stand in its block and are read only inside the tree.
fn copy_tree(ir: &Ir, root: OpId) -> Vec<OpId> {
	let Some(block) = ir.op(root).map(|data| data.parent) else {
		return Vec::new();
	};
	let mut members: FxHashSet<OpId> = FxHashSet::default();
	members.insert(root);
	let mut pending = vec![root];
	while let Some(op) = pending.pop() {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let mut reads: Vec<ValueId> = data.operands.to_vec();
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
				if let Some(held) = ir.op(inner) {
					reads.extend_from_slice(&held.operands);
				}
				Walk::Continue
			});
		}
		for operand in reads {
			if let Some(defining) = def_op(ir, operand)
				&& ir.op(defining).is_some_and(|held| held.parent == block)
				&& members.insert(defining)
			{
				pending.push(defining);
			}
		}
	}
	loop {
		let outside: Vec<OpId> = members
			.iter()
			.copied()
			.filter(|op| *op != root && read_outside(ir, *op, &members))
			.collect();
		if outside.is_empty() {
			break;
		}
		for op in outside {
			members.remove(&op);
		}
	}
	let Some(held) = ir.block(block) else {
		return Vec::new();
	};
	held.ops.iter().copied().filter(|op| members.contains(op)).collect()
}

/// Whether `op` is a member or stands inside the regions of one.
fn within(ir: &Ir, op: OpId, members: &FxHashSet<OpId>) -> bool {
	members.contains(&op)
		|| ancestors(ir, op).any(|(_, owner)| owner.is_some_and(|held| members.contains(&held)))
}

/// Whether a result of `op` is read by an op that is neither a member nor inside one.
fn read_outside(ir: &Ir, op: OpId, members: &FxHashSet<OpId>) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	data.results
		.iter()
		.any(|result| ir.uses(*result).iter().any(|held| !within(ir, held.user, members)))
}

/// Whether `value` is defined by an op of the tree, an op inside one, or a block argument of a
/// region one owns.
fn defined_within(ir: &Ir, value: ValueId, members: &FxHashSet<OpId>) -> bool {
	match ir.value(value).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => within(ir, op, members),
		Some(ValueDef::BlockArg {
			block,
			..
		}) => ir
			.block(block)
			.and_then(|held| ir.parent_op(held.parent))
			.is_some_and(|owner| within(ir, owner, members)),
		None => false,
	}
}

/// The ops inside the regions of `op`, at any depth.
fn nested_ops(ir: &Ir, op: OpId) -> usize {
	let mut count = 0;
	if let Some(data) = ir.op(op) {
		for region in data.regions.iter().copied() {
			ir.walk(region, WalkOrder::PreOrder, &mut |_| {
				count += 1;
				Walk::Continue
			});
		}
	}
	count
}

/// The ops one copy of `tree` holds: its members and every op inside their regions.
fn tree_size(ir: &Ir, tree: &[OpId]) -> usize {
	tree.iter().map(|op| 1 + nested_ops(ir, *op)).sum()
}

/// Whether `value` is defined by an op inside `region` or is an argument of a block inside it.
fn value_inside(ir: &Ir, value: ValueId, region: RegionId) -> bool {
	let held = match ir.value(value).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => ir.op(op).and_then(|data| ir.block(data.parent)).map(|block| block.parent),
		Some(ValueDef::BlockArg {
			block,
			..
		}) => ir.block(block).map(|held| held.parent),
		None => None,
	};
	let Some(mut current) = held else {
		return false;
	};
	for _ in 0..MAX_HOPS {
		if current == region {
			return true;
		}
		let Some(owner) = ir.parent_op(current) else {
			return false;
		};
		current = ir.enclosing_region(owner);
	}
	false
}

/// Whether every op inside `region` reads only values defined inside it.
fn region_closed(ir: &Ir, region: RegionId) -> bool {
	let mut closed = true;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		if data.operands.iter().any(|read| !value_inside(ir, *read, region)) {
			closed = false;
			return Walk::Stop;
		}
		Walk::Continue
	});
	closed
}

/// Whether every region of every op of `tree` is closed, which is what lets the regions be cloned.
fn regions_closed(ir: &Ir, tree: &[OpId]) -> bool {
	tree.iter().all(|op| {
		ir.op(*op).is_some_and(|data| data.regions.iter().all(|region| region_closed(ir, *region)))
	})
}

/// Whether every value the ops of `tree` read from outside the tree, those inside their regions
/// included, is defined outside every loop body.
fn inputs_outside_loops(ir: &Ir, tree: &[OpId]) -> bool {
	let members: FxHashSet<OpId> = tree.iter().copied().collect();
	let mut reads: Vec<ValueId> = Vec::new();
	for op in tree {
		let Some(data) = ir.op(*op) else {
			return false;
		};
		reads.extend(data.operands.iter().copied());
		for region in data.regions.iter().copied() {
			ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
				if let Some(held) = ir.op(inner) {
					reads.extend(held.operands.iter().copied());
				}
				Walk::Continue
			});
		}
	}
	reads.into_iter().all(|value| {
		if defined_within(ir, value, &members) {
			return true;
		}
		let region = match ir.value(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => ir.op(op).map(|_| ir.enclosing_region(op)),
			Some(ValueDef::BlockArg {
				block,
				..
			}) => ir.block(block).map(|held| held.parent),
			None => None,
		};
		region.is_some_and(|region| !inside_loop_body(ir, region))
	})
}

// ---------------------------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------------------------

/// One step down from an op to an op inside it: which region of the op, which block of the
/// region, and which op of the block.
#[derive(Clone, Copy, Debug)]
struct Step {
	region: usize,
	block: usize,
	op: usize,
}

/// A loop and the condition it is unswitched on.
struct Plan {
	loop_op: OpId,
	/// The op that defines the condition, and the tag a copy of it must have.
	cond: OpId,
	cond_tag: OpTag,
	/// The result of `cond` that is the conjunct.
	cond_index: usize,
	/// How to reach a copy of `cond` from a copy of the loop.
	path: Vec<Step>,
	/// The loop and the source ops only it reads, in definition order.
	tree: Vec<OpId>,
}

/// The steps that lead from `loop_op` down to `target`, or `None` when `target` does not stand
/// inside it.
fn path_to(ir: &Ir, loop_op: OpId, target: OpId) -> Option<Vec<Step>> {
	let mut steps = Vec::new();
	let mut current = target;
	for _ in 0..MAX_HOPS {
		let block_id = ir.op(current)?.parent;
		let block = ir.block(block_id)?;
		let region_id = block.parent;
		let region = ir.region(region_id)?;
		let owner = ir.parent_op(region_id)?;
		steps.push(Step {
			region: ir.op(owner)?.regions.iter().position(|held| *held == region_id)?,
			block: region.blocks.iter().position(|held| *held == block_id)?,
			op: block.ops.iter().position(|held| *held == current)?,
		});
		if owner == loop_op {
			steps.reverse();
			return Some(steps);
		}
		current = owner;
	}
	None
}

/// The op `path` leads to from `root`.
fn follow(ir: &Ir, root: OpId, path: &[Step]) -> Result<OpId, IrError> {
	let stale = || IrError::Stale("unswitch path");
	let mut at = root;
	for step in path {
		let region =
			*ir.op(at).ok_or(IrError::Stale("op"))?.regions.get(step.region).ok_or_else(stale)?;
		let block = *ir
			.region(region)
			.ok_or(IrError::Stale("region"))?
			.blocks
			.get(step.block)
			.ok_or_else(stale)?;
		at = *ir.block(block).ok_or(IrError::Stale("block"))?.ops.get(step.op).ok_or_else(stale)?;
	}
	Ok(at)
}

/// The value that closes the clause region `region`, when it is a lone block ending in a yield of
/// one value.
fn closing_predicate(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	let last = ir.op(*ir.block(*block)?.ops.last()?)?;
	let (OpKind::Expr(ExprOp::Yield), [predicate]) = (&last.kind, last.operands.as_slice()) else {
		return None;
	};
	Some(*predicate)
}

/// The predicates of the `WHERE` filters and of the table permission filters that stand directly
/// in the per-row body of `loop_op`, in body order.
///
/// A permission filter that derives computed fields lists their bodies after its predicate, and
/// the derivation runs for every row before the predicate. The predicate is used only when every
/// body is infallible with effects within the row alone, so an arm the condition makes `false`
/// cannot skip a derivation that raises.
fn predicates(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> Vec<ValueId> {
	let mut found = Vec::new();
	for region in body_regions(ir, loop_op) {
		for op in ops_in(ir, region) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			let wanted = match &data.kind {
				OpKind::Logical(LogOp::Filter) => RegionKind::Clause(ClauseRole::Where),
				OpKind::Logical(LogOp::PermissionFilter {
					..
				}) => RegionKind::PermissionFrame,
				OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_) => continue,
			};
			let Some((held, derivations)) = data.regions.split_first() else {
				continue;
			};
			if ir.region(*held).map(|info| info.kind) != Some(wanted) {
				continue;
			}
			let derives_totally = derivations
				.iter()
				.flat_map(|region| ops_in(ir, *region))
				.all(|member| total_in_row(ir, facts, cx, member));
			if !derives_totally {
				continue;
			}
			if let Some(predicate) = closing_predicate(ir, *held) {
				found.push(predicate);
			}
		}
	}
	found
}

/// Whether `op`, regions included, is infallible and has effects within the row alone.
fn total_in_row(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, op: OpId) -> bool {
	let answer = effects::of_with(ir, facts, cx, op);
	let class = answer.fallibility.union(fallibility::floor(answer.set));
	guards::infallible(class) && guards::pure(answer.set)
}

/// Whether the value the yield `yielded` closes with is read as a conjunct: the yield closes a
/// predicate clause, or the right operand of an `AND`.
fn closes_a_conjunct(ir: &Ir, yielded: OpId) -> bool {
	let region = ir.enclosing_region(yielded);
	let Some(info) = ir.region(region) else {
		return false;
	};
	match info.kind {
		RegionKind::Clause(ClauseRole::Where) | RegionKind::PermissionFrame => true,
		RegionKind::LazyOperand => {
			info.parent.and_then(|owner| ir.op(owner)).is_some_and(|owner| {
				matches!(
					owner.kind,
					OpKind::Expr(ExprOp::ShortCircuit {
						op: ShortCircuitOp::And
					})
				)
			})
		}
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::ValueIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::FieldFrame(_)
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::Clause(_)
		| RegionKind::StoredClause(_)
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline => false,
	}
}

/// Whether `value` has one reader and that reader takes only its truthiness: the left operand of
/// an `AND`, a `truthy`, or a yield that closes a conjunct. The conjunct then stands in a chain
/// whose value only a filter reads as a boolean, so a boolean literal of the same truthiness
/// takes its place without a change of answer.
fn consumed_as_boolean(ir: &Ir, value: ValueId) -> bool {
	let [held] = ir.uses(value) else {
		return false;
	};
	let Some(user) = ir.op(held.user) else {
		return false;
	};
	match &user.kind {
		OpKind::Expr(ExprOp::Truthy) => true,
		OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		}) => held.operand == 0,
		OpKind::Expr(ExprOp::Yield) => closes_a_conjunct(ir, held.user),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether an op of this kind is a constant already: a literal, a named constant or a table name,
/// or a yield, which only closes a region.
fn is_constant_kind(kind: &ExprOp) -> bool {
	matches!(
		kind,
		ExprOp::Literal(_) | ExprOp::Constant { .. } | ExprOp::Table { .. } | ExprOp::Yield
	)
}

/// The op that defines the conjunct `value` and the index of the result it is, when `value` is a
/// condition `loop_op` may be unswitched on.
fn condition_of(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	loop_op: OpId,
	value: ValueId,
) -> Option<(OpId, usize)> {
	let ValueDef::OpResult {
		op,
		index,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	if data.results.len() != 1 || is_constant_kind(kind) || !stands_in(ir, op, loop_op) {
		return None;
	}
	if !consumed_as_boolean(ir, value) {
		return None;
	}
	if !matches!(invariant_in(ir, value, loop_op), Invariance::Invariant) {
		return None;
	}
	let tree = copy_tree(ir, op);
	if !tree.iter().all(|member| total_in_row(ir, facts, cx, *member)) {
		return None;
	}
	if !inputs_outside_loops(ir, &tree)
		|| !regions_closed(ir, &tree)
		|| !can_rehome(ir, op, InsertPoint::Before(loop_op), &[])
	{
		return None;
	}
	Some((op, usize::from(index)))
}

/// The unswitch `loop_op` admits, or `None`.
///
/// Everything that can refuse the rewrite is decided here, before the container is touched.
fn plan_for(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> Option<Plan> {
	if !is_select_loop(ir, loop_op) || ir.op(loop_op)?.results.len() != 1 {
		return None;
	}
	if leaves_loop(ir, loop_op) {
		return None;
	}
	let tree = copy_tree(ir, loop_op);
	if tree_size(ir, &tree) > UNSWITCH_MAX_OPS {
		return None;
	}
	let (cond, cond_index) = predicates(ir, facts, cx, loop_op)
		.into_iter()
		.flat_map(|predicate| and_chain(ir, predicate))
		.find_map(|conjunct| condition_of(ir, facts, cx, loop_op, conjunct))?;
	let statement = enclosing_statement(ir, loop_op)?;
	if ir.copy_statement(statement).is_err()
		|| !regions_closed(ir, &tree)
		|| !can_rehome(ir, loop_op, InsertPoint::Before(loop_op), &[])
	{
		return None;
	}
	Some(Plan {
		loop_op,
		cond,
		cond_tag: ir.op(cond)?.kind.tag(),
		cond_index,
		path: path_to(ir, loop_op, cond)?,
		tree,
	})
}

// ---------------------------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------------------------

/// The kind of the arms of the `if` that takes the place of `loop_op`: a statement branch when the
/// loop stands as a statement of a statement list, which is when its value feeds only the yield
/// closing its own block, and a value branch otherwise.
fn arm_kind(ir: &Ir, loop_op: OpId) -> RegionKind {
	let Some(data) = ir.op(loop_op) else {
		return RegionKind::ValueIf;
	};
	let in_list = ir.holds_statement_list(ir.enclosing_region(loop_op));
	let only_closes = data.results.iter().all(|result| {
		ir.uses(*result).iter().all(|used| {
			ir.op(used.user).is_some_and(|user| {
				user.parent == data.parent && user.kind.terminator() == Some(Terminator::Yield)
			})
		})
	});
	if in_list && only_closes {
		RegionKind::StatementIf
	} else {
		RegionKind::ValueIf
	}
}

/// Makes the conjunct of the copy of the loop rooted at `copy_loop` the literal `truth`.
fn substitute(
	rw: &mut Rewriter<'_>,
	plan: &Plan,
	copy_loop: OpId,
	truth: bool,
) -> Result<(), IrError> {
	let target = follow(rw.ir(), copy_loop, &plan.path)?;
	let data = rw.ir().op(target).ok_or(IrError::Stale("op"))?.clone();
	if data.kind.tag() != plan.cond_tag {
		return Err(IrError::Stale("unswitch path"));
	}
	let old = *data.results.get(plan.cond_index).ok_or(IrError::Stale("value"))?;
	let literal = ExprOp::Literal(Box::new(Value::Bool(truth)));
	let spec = OpSpec {
		results: smallvec![result_type(&literal, &[])],
		kind: OpKind::Expr(literal),
		operands: SmallVec::new(),
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc,
	};
	let made = rw.derived(&[plan.loop_op], |rw| rw.create(InsertPoint::Before(target), spec))?;
	let new = rw
		.ir()
		.op(made)
		.and_then(|held| held.results.first().copied())
		.ok_or(IrError::Stale("value"))?;
	rw.replace_all_uses(old, new)?;
	match rw.erase(target, EliminationReason::FoldedConstant) {
		Ok(()) | Err(IrError::HasUses(_)) => Ok(()),
		Err(error) => Err(error),
	}
}

/// Replaces the loop of `plan` by the `if` on its condition.
fn apply(rw: &mut Rewriter<'_>, plan: &Plan) -> Result<(), IrError> {
	let loop_op = plan.loop_op;
	let data = rw.ir().op(loop_op).ok_or(IrError::Stale("op"))?.clone();
	let result = *data.results.first().ok_or(IrError::Stale("value"))?;
	let ty = rw.ir().value(result).ok_or(IrError::Stale("value"))?.ty.clone();
	let arms = arm_kind(rw.ir(), loop_op);
	let here = InsertPoint::Before(loop_op);

	let moved = rw.derived(&[loop_op], |rw| {
		rehome_tree(rw, plan.cond, here, RehomeMode::Copy, EliminationReason::DeadPure)
	})?;
	let condition = moved.get(plan.cond_index).copied().ok_or(IrError::Stale("value"))?;
	let branch_spec = OpSpec {
		kind: OpKind::Logical(LogOp::If),
		operands: smallvec![condition],
		results: smallvec![ty],
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc.clone(),
	};
	let branch = rw.derived(&[loop_op], |rw| rw.create(here, branch_spec))?;

	for truth in [true, false] {
		let (_, block) = rw.create_region(branch, arms, &[])?;
		let copied = rw.derived(&[loop_op], |rw| {
			rehome_tree(
				rw,
				loop_op,
				InsertPoint::End(block),
				RehomeMode::Copy,
				EliminationReason::DeadPure,
			)
		})?;
		let value = *copied.first().ok_or(IrError::Stale("value"))?;
		let Some(ValueDef::OpResult {
			op: copy_loop,
			..
		}) = rw.ir().value(value).map(|held| held.def)
		else {
			return Err(IrError::Stale("op"));
		};
		substitute(rw, plan, copy_loop, truth)?;
		let closing = OpSpec {
			kind: OpKind::Logical(LogOp::Yield),
			operands: smallvec![value],
			results: SmallVec::new(),
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		};
		rw.derived(&[loop_op], |rw| rw.create(InsertPoint::End(block), closing))?;
	}

	let answer = rw
		.ir()
		.op(branch)
		.and_then(|held| held.results.first().copied())
		.ok_or(IrError::Stale("value"))?;
	rw.replace_op(loop_op, &[answer])?;
	for source in plan.tree.iter().rev().filter(|op| **op != loop_op) {
		if rw.ir().op(*source).is_none() {
			continue;
		}
		match rw.erase(*source, EliminationReason::MergedInto(branch)) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(())
}

fn run_unswitch(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	let loops: Vec<OpId> =
		live_ops(cx.ir).into_iter().filter(|op| is_select_loop(cx.ir, *op)).collect();
	for loop_op in loops {
		if cx.ir.op(loop_op).is_none() {
			continue;
		}
		let Some(plan) = plan_for(&*cx.ir, &mut *cx.facts, cx.effects, loop_op) else {
			continue;
		};
		let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
		apply(&mut rw, &plan)?;
		report.rewrites = report.rewrites.saturating_add(1);
	}
	Ok(report)
}
