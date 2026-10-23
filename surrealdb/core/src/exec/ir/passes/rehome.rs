//! Moving and copying op trees, and rebuilding `AND` chains, with the frozen mutators.
//!
//! Every op these helpers create is created under [`Rewriter::derived`] over the op it replaces,
//! so provenance keeps the logical id and an EXPLAIN of the result names the original. An op keeps
//! the frame and snapshot of the op it re-creates. Each helper checks everything it can refuse
//! before it changes anything; once it has begun, the only failures left are a full table or a
//! broken container invariant, and the caller's verification reports those.
#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use crate::exec::ir::core::copy::StatementCopy;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, enclosing_statement};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;

/// Whether a re-created tree replaces the original or stands beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RehomeMode {
	/// The regions of the tree's ops are transferred, every use of the original results is
	/// redirected to the new results, and the original ops are erased.
	Move,
	/// The regions of the tree's ops are cloned and the original stays where it is.
	Copy,
}

/// Re-creates the op tree rooted at `root` at `at`, in definition order, and answers the new
/// root's results.
///
/// The tree is `root` and every op of its operand subtree that stands in `root`'s block and whose
/// results are read only inside the tree; an operand defined elsewhere or read outside the tree is
/// shared, not copied. Regions of a re-created op are transferred with `move_region` (Move) or
/// cloned (Copy). In Move mode every use of the old results is redirected and the old ops are
/// erased in reverse definition order with `reason`; the caller guarantees that `at` dominates
/// every use of the root's results.
///
/// Refused before any mutation:
/// - `IrError::Refused("rehome escapes")`: the tree reads a value that is not defined inside it and
///   is not visible at `at`;
/// - `IrError::Refused("rehome insert point")`: `at` lies inside one of the tree's regions;
/// - `IrError::Refused("rehome terminator")`: the tree holds a terminator;
/// - `IrError::Refused("rehome copy")` (Copy only): the container holds no statement around `root`,
///   or a region of the tree reads a value defined outside the region and so cannot be cloned.
///
/// The ops a cloned region holds are `Bound` to this container, which is how a region clone
/// records them.
pub(crate) fn rehome_tree(
	rw: &mut Rewriter<'_>,
	root: OpId,
	at: InsertPoint,
	mode: RehomeMode,
	reason: EliminationReason,
) -> Result<SmallVec<[ValueId; 2]>, IrError> {
	rehome_tree_bound(rw, root, at, mode, reason, &[])
}

/// [`rehome_tree`] for a tree that reads values only its old home can see, such as the row
/// argument of the region it stands in.
///
/// Each `(from, to)` of `bound` makes every read of `from` by the re-created tree, the reads inside
/// its regions included, read `to` instead. `to` must be visible at `at`, and `from` must not be
/// defined inside the tree; either failing is refused before any mutation with
/// `IrError::Refused("rehome bind")` or `IrError::Refused("rehome escapes")`. The original ops keep
/// reading `from` until they are erased, so a Copy-mode caller that keeps them sees no change in
/// them. A cloned region cannot be re-bound: one that reads a bound value is refused like any
/// region that reads outside itself.
pub(crate) fn rehome_tree_bound(
	rw: &mut Rewriter<'_>,
	root: OpId,
	at: InsertPoint,
	mode: RehomeMode,
	reason: EliminationReason,
	bound: &[(ValueId, ValueId)],
) -> Result<SmallVec<[ValueId; 2]>, IrError> {
	let tree = collect_tree(rw.ir(), root)?;
	let members: FxHashSet<OpId> = tree.iter().copied().collect();
	let (block, index) = resolve(rw.ir(), at)?;
	check_placement(rw.ir(), &tree, &members, block, index, bound)?;
	let clones = match mode {
		RehomeMode::Move => None,
		RehomeMode::Copy => Some(clone_source(rw.ir(), root, &tree)?),
	};

	let point = anchored(rw.ir(), at)?;
	let mut renamed: FxHashMap<ValueId, ValueId> = bound.iter().copied().collect();
	let mut created: Vec<OpId> = Vec::with_capacity(tree.len());
	for old in &tree {
		let data = rw.ir().op(*old).ok_or(IrError::Stale("op"))?.clone();
		let spec = respec(rw.ir(), &data, &renamed)?;
		let new = rw.derived(&[*old], |rw| rw.create(point, spec))?;
		let results = rw.ir().op(new).ok_or(IrError::Stale("op"))?.results.clone();
		for (before, after) in data.results.iter().zip(&results) {
			renamed.insert(*before, *after);
		}
		match &clones {
			None => {
				for region in &data.regions {
					rw.move_region(*region, new)?;
				}
			}
			Some((source, map)) => {
				for region in &data.regions {
					let copy = map.map_region(*region).ok_or(IrError::Refused("rehome copy"))?;
					rw.clone_region_from(source, copy, new)?;
				}
			}
		}
		created.push(new);
	}

	if mode == RehomeMode::Move {
		rebind_nested(rw, &created, bound)?;
		for old in &tree {
			let results = rw.ir().op(*old).ok_or(IrError::Stale("op"))?.results.clone();
			for result in results {
				if let Some(new) = renamed.get(&result) {
					rw.replace_all_uses(result, *new)?;
				}
			}
		}
		for old in tree.iter().rev() {
			rw.erase(*old, reason)?;
		}
	}

	let at_root = tree.iter().position(|op| *op == root).ok_or(IrError::Stale("op"))?;
	let new_root = created.get(at_root).ok_or(IrError::Stale("op"))?;
	Ok(rw.ir().op(*new_root).ok_or(IrError::Stale("op"))?.results.clone())
}

/// Re-creates a copy of the op tree rooted at `root` at `at` and answers the copy's first result;
/// the original stays where it is and keeps reading `from`.
///
/// The tree is the one [`rehome_tree`] moves. Each `(from, to)` of `bound` makes every read of
/// `from` by the copy, the reads inside its regions included, read `to`. Each region of a copied op
/// is rebuilt op by op, so a region may read a bound value, which a cloned region cannot.
/// Refusals are those of [`can_rehome`], checked before any mutation.
pub(crate) fn copy_tree_bound(
	rw: &mut Rewriter<'_>,
	root: OpId,
	at: InsertPoint,
	bound: &[(ValueId, ValueId)],
) -> Result<ValueId, IrError> {
	let tree = collect_tree(rw.ir(), root)?;
	let members: FxHashSet<OpId> = tree.iter().copied().collect();
	let (block, index) = resolve(rw.ir(), at)?;
	check_placement(rw.ir(), &tree, &members, block, index, bound)?;
	let point = anchored(rw.ir(), at)?;
	let mut renamed: FxHashMap<ValueId, ValueId> = bound.iter().copied().collect();
	let mut copy_of_root = None;
	for old in &tree {
		let new = copy_op(rw, *old, point, &mut renamed)?;
		if *old == root {
			copy_of_root = Some(new);
		}
	}
	let new_root = copy_of_root.ok_or(IrError::Stale("op"))?;
	rw.ir()
		.op(new_root)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("value"))
}

/// Creates a copy of `old` at `point` reading the renamed values, records each copied result in
/// `renamed`, rebuilds the regions of `old` inside the copy, and answers the copy.
fn copy_op(
	rw: &mut Rewriter<'_>,
	old: OpId,
	point: InsertPoint,
	renamed: &mut FxHashMap<ValueId, ValueId>,
) -> Result<OpId, IrError> {
	let data = rw.ir().op(old).ok_or(IrError::Stale("op"))?.clone();
	let spec = respec(rw.ir(), &data, renamed)?;
	let new = rw.derived(&[old], |rw| rw.create(point, spec))?;
	let results = rw.ir().op(new).ok_or(IrError::Stale("op"))?.results.clone();
	for (before, after) in data.results.iter().zip(&results) {
		renamed.insert(*before, *after);
	}
	for region in &data.regions {
		copy_region(rw, *region, new, renamed)?;
	}
	Ok(new)
}

/// Rebuilds the one-block region `region` as a region of `owner`: the block arguments are new,
/// and every op of the block is copied in order.
fn copy_region(
	rw: &mut Rewriter<'_>,
	region: RegionId,
	owner: OpId,
	renamed: &mut FxHashMap<ValueId, ValueId>,
) -> Result<(), IrError> {
	let held = rw.ir().region(region).ok_or(IrError::Stale("region"))?;
	let kind = held.kind;
	let [block] = held.blocks.as_slice() else {
		return Err(IrError::Refused("rehome copy"));
	};
	let block = rw.ir().block(*block).ok_or(IrError::Stale("block"))?;
	let (args, ops) = (block.args.clone(), block.ops.clone());
	let mut types: SmallVec<[TypeKnowledge; 2]> = SmallVec::new();
	for arg in &args {
		types.push(rw.ir().value(*arg).ok_or(IrError::Stale("value"))?.ty.clone());
	}
	let (_, copied) = rw.create_region(owner, kind, &types)?;
	let fresh = rw.ir().block(copied).ok_or(IrError::Stale("block"))?.args.clone();
	for (before, after) in args.iter().zip(&fresh) {
		renamed.insert(*before, *after);
	}
	for op in ops {
		copy_op(rw, op, InsertPoint::End(copied), renamed)?;
	}
	Ok(())
}

/// Whether [`rehome_tree_bound`] over `root`, `at` and `bound` passes every check it makes before
/// it mutates anything. Reads nothing but the container.
pub(crate) fn can_rehome(
	ir: &Ir,
	root: OpId,
	at: InsertPoint,
	bound: &[(ValueId, ValueId)],
) -> bool {
	let Ok(tree) = collect_tree(ir, root) else {
		return false;
	};
	let members: FxHashSet<OpId> = tree.iter().copied().collect();
	let Ok((block, index)) = resolve(ir, at) else {
		return false;
	};
	check_placement(ir, &tree, &members, block, index, bound).is_ok()
}

/// Points every read of a bound `from` inside the regions of the re-created ops `created` at its
/// `to`.
fn rebind_nested(
	rw: &mut Rewriter<'_>,
	created: &[OpId],
	bound: &[(ValueId, ValueId)],
) -> Result<(), IrError> {
	if bound.is_empty() {
		return Ok(());
	}
	let members: FxHashSet<OpId> = created.iter().copied().collect();
	for (from, to) in bound {
		let readers: Vec<(OpId, u16)> = rw
			.ir()
			.uses(*from)
			.iter()
			.filter(|held| within(rw.ir(), held.user, &members))
			.map(|held| (held.user, held.operand))
			.collect();
		for (user, operand) in readers {
			rw.set_operand(user, operand, *to)?;
		}
	}
	Ok(())
}

/// The conjuncts of an `AND` chain, left to right: `a AND b AND c` as `[a, b, c]`.
///
/// A nested `AND` is dissolved into the chain only when its result has no other reader, and a right
/// operand only when it is defined inside the right region of the chain; any other value is one
/// conjunct. A value that is not the result of an `AND` is the chain's only conjunct.
pub(crate) fn and_chain(ir: &Ir, root: ValueId) -> SmallVec<[ValueId; 4]> {
	let mut out: SmallVec<[ValueId; 4]> = SmallVec::new();
	let mut pending: SmallVec<[(ValueId, bool); 8]> = smallvec![(root, true)];
	while let Some((value, owned)) = pending.pop() {
		match owned.then(|| and_parts(ir, value)).flatten() {
			Some(parts) => {
				pending.push((parts.right, parts.right_owned));
				pending.push((parts.left, ir.uses(parts.left).len() == 1));
			}
			None => out.push(value),
		}
	}
	out
}

/// Rebuilds an `AND` chain over `conjuncts` in the given order at `at` and answers its value.
///
/// The first conjunct is the left operand of the innermost `AND`, and each later one is the value
/// a `LazyOperand` region of its own yields. A conjunct that is visible at `at` is read where it
/// is. Any other conjunct is moved into its place with [`rehome_tree`], so a conjunct that was
/// lazy stays lazy. The old chain is not touched: the caller redirects the uses of its value to
/// the answer and erases it.
///
/// The new ops take the frame, snapshot and location of the op `at` stands beside, and one
/// conjunct answers that conjunct itself. An empty list answers `IrError::Stale`.
pub(crate) fn rebuild_and_chain(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	conjuncts: &[ValueId],
) -> Result<ValueId, IrError> {
	rebuild_and_chain_bound(rw, at, conjuncts, &[])
}

/// [`rebuild_and_chain`] for conjuncts that read values only their old home can see: every
/// conjunct that has to move is moved with [`rehome_tree_bound`] under `bound`.
pub(crate) fn rebuild_and_chain_bound(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	conjuncts: &[ValueId],
	bound: &[(ValueId, ValueId)],
) -> Result<ValueId, IrError> {
	rebuild_chain(rw, at, conjuncts, bound, false)
}

/// [`rebuild_and_chain`] for a chain whose every conjunct after the first is evaluated lazily,
/// whether or not it is visible at `at`.
///
/// A conjunct that stands in the block of `at` is read there by [`rebuild_and_chain`], so it stays
/// evaluated before the chain whatever its place in the chain. Here the tree that computes each
/// later conjunct is moved into the `LazyOperand` region of its own, so it is evaluated only for a
/// row the conjuncts before it keep. The value of a later conjunct must be read by its chain alone:
/// its tree is moved and every other reader is redirected to the moved value, which that reader
/// may not see. A conjunct that is a block argument is read where it is. Refusals are those of
/// [`rehome_tree`].
///
/// A conjunct in the block of `at` is evaluated for every row when it is read in place. Here it is
/// evaluated only for the rows the conjuncts before it keep. The caller must ensure that this is
/// unobservable: the conjunct raises nothing and has no effect.
pub(crate) fn rebuild_lazy_and_chain(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	conjuncts: &[ValueId],
) -> Result<ValueId, IrError> {
	rebuild_chain(rw, at, conjuncts, &[], true)
}

/// The chain [`rebuild_and_chain`] and [`rebuild_lazy_and_chain`] build; `lazy` makes every
/// conjunct after the first move into its own region.
fn rebuild_chain(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	conjuncts: &[ValueId],
	bound: &[(ValueId, ValueId)],
	lazy: bool,
) -> Result<ValueId, IrError> {
	let (first, rest) = conjuncts.split_first().ok_or(IrError::Stale("rebuild_and_chain empty"))?;
	let beside = neighbour(rw.ir(), at)?;
	let template = rw.ir().op(beside).ok_or(IrError::Stale("op"))?.clone();
	let point = anchored(rw.ir(), at)?;
	let and = OpKind::Expr(ExprOp::ShortCircuit {
		op: ShortCircuitOp::And,
	});
	let mut chain = home(rw, point, *first, bound)?;
	for conjunct in rest {
		let ty = result_type(
			&ExprOp::ShortCircuit {
				op: ShortCircuitOp::And,
			},
			&[],
		);
		let spec = OpSpec {
			kind: and.clone(),
			operands: smallvec![chain],
			results: smallvec![ty],
			frame: template.frame,
			snapshot: template.snapshot,
			loc: template.loc.clone(),
		};
		let gate = rw.derived(&[beside], |rw| rw.create(point, spec))?;
		let (_, body) = rw.create_region(gate, RegionKind::LazyOperand, &[])?;
		let right = if lazy {
			lazily(rw, InsertPoint::End(body), *conjunct)?
		} else {
			home(rw, InsertPoint::End(body), *conjunct, bound)?
		};
		let yielded = OpSpec {
			kind: OpKind::Expr(ExprOp::Yield),
			operands: smallvec![right],
			results: SmallVec::new(),
			frame: template.frame,
			snapshot: template.snapshot,
			loc: template.loc.clone(),
		};
		rw.derived(&[beside], |rw| rw.create(InsertPoint::End(body), yielded))?;
		chain = rw
			.ir()
			.op(gate)
			.and_then(|data| data.results.first().copied())
			.ok_or(IrError::Stale("op"))?;
	}
	Ok(chain)
}

/// The two operands of an `AND`, and whether the right one may be dissolved into a chain.
struct AndParts {
	left: ValueId,
	right: ValueId,
	right_owned: bool,
}

/// The parts of `value` when it is the result of an `AND`, or `None`.
fn and_parts(ir: &Ir, value: ValueId) -> Option<AndParts> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let OpKind::Expr(ExprOp::ShortCircuit {
		op: ShortCircuitOp::And,
	}) = &data.kind
	else {
		return None;
	};
	let [left] = data.operands.as_slice() else {
		return None;
	};
	let [region] = data.regions.as_slice() else {
		return None;
	};
	let [body] = ir.region(*region)?.blocks.as_slice() else {
		return None;
	};
	let block = ir.block(*body)?;
	let last = ir.op(*block.ops.last()?)?;
	let OpKind::Expr(ExprOp::Yield) = &last.kind else {
		return None;
	};
	let [right] = last.operands.as_slice() else {
		return None;
	};
	let defined_inside = match ir.value(*right)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).is_some_and(|defining| defining.parent == *body),
		ValueDef::BlockArg {
			..
		} => false,
	};
	Some(AndParts {
		left: *left,
		right: *right,
		right_owned: defined_inside && ir.uses(*right).len() == 1,
	})
}

/// `value`, visible at `at`: itself when it is already visible there, and otherwise the result of
/// the moved tree that defines it.
fn home(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	value: ValueId,
	bound: &[(ValueId, ValueId)],
) -> Result<ValueId, IrError> {
	let (block, index) = resolve(rw.ir(), at)?;
	if visible_at(rw.ir(), value, block, index) {
		return Ok(value);
	}
	let ValueDef::OpResult {
		op,
		index: result,
	} = rw.ir().value(value).ok_or(IrError::Stale("value"))?.def
	else {
		return Err(IrError::Refused("rehome escapes"));
	};
	let moved =
		rehome_tree_bound(rw, op, at, RehomeMode::Move, EliminationReason::DeadPure, bound)?;
	moved.get(usize::from(result)).copied().ok_or(IrError::Stale("value"))
}

/// `value`, computed by a tree moved to `at` even when `value` is already visible there; a block
/// argument is read where it is.
fn lazily(rw: &mut Rewriter<'_>, at: InsertPoint, value: ValueId) -> Result<ValueId, IrError> {
	let ValueDef::OpResult {
		op,
		index: result,
	} = rw.ir().value(value).ok_or(IrError::Stale("value"))?.def
	else {
		return Ok(value);
	};
	let moved = rehome_tree_bound(rw, op, at, RehomeMode::Move, EliminationReason::DeadPure, &[])?;
	moved.get(usize::from(result)).copied().ok_or(IrError::Stale("value"))
}

/// The ops of the tree rooted at `root`, in definition order.
fn collect_tree(ir: &Ir, root: OpId) -> Result<Vec<OpId>, IrError> {
	let block = ir.op(root).ok_or(IrError::Stale("op"))?.parent;
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
			let Some(ValueDef::OpResult {
				op: defining,
				..
			}) = ir.value(operand).map(|value| value.def)
			else {
				continue;
			};
			if ir.op(defining).is_some_and(|held| held.parent == block) && members.insert(defining)
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
	let held = ir.block(block).ok_or(IrError::Stale("block"))?;
	Ok(held.ops.iter().copied().filter(|op| members.contains(op)).collect())
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

/// Whether `op` is a member or stands inside the regions of one.
fn within(ir: &Ir, op: OpId, members: &FxHashSet<OpId>) -> bool {
	members.contains(&op)
		|| ancestors(ir, op).any(|(_, owner)| owner.is_some_and(|held| members.contains(&held)))
}

/// Whether the value is defined by an op of the tree, an op inside one, or a block argument of a
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

/// The block and position `at` names.
fn resolve(ir: &Ir, at: InsertPoint) -> Result<(BlockId, usize), IrError> {
	let beside = |op: OpId, after: usize| {
		let block = ir.op(op).ok_or(IrError::Stale("insert point"))?.parent;
		let held = ir.block(block).ok_or(IrError::Stale("insert point"))?;
		let position = held
			.ops
			.iter()
			.position(|candidate| *candidate == op)
			.ok_or(IrError::Stale("insert point"))?;
		Ok((block, position + after))
	};
	match at {
		InsertPoint::Before(op) => beside(op, 0),
		InsertPoint::After(op) => beside(op, 1),
		InsertPoint::End(block) => {
			let held = ir.block(block).ok_or(IrError::Stale("block"))?;
			Ok((block, held.ops.len()))
		}
	}
}

/// `at` as a point that stays put while ops are created at it: ops created one after another at
/// the answer stand in creation order.
fn anchored(ir: &Ir, at: InsertPoint) -> Result<InsertPoint, IrError> {
	let (block, index) = resolve(ir, at)?;
	let held = ir.block(block).ok_or(IrError::Stale("block"))?;
	Ok(match held.ops.get(index) {
		Some(next) => InsertPoint::Before(*next),
		None => InsertPoint::End(block),
	})
}

/// The op `at` stands beside, whose frame, snapshot and location a new op there takes: the op it
/// names, the last op of a block, or the owner of an empty block's region.
fn neighbour(ir: &Ir, at: InsertPoint) -> Result<OpId, IrError> {
	match at {
		InsertPoint::Before(op) | InsertPoint::After(op) => Ok(op),
		InsertPoint::End(block) => {
			let held = ir.block(block).ok_or(IrError::Stale("block"))?;
			match held.ops.last() {
				Some(last) => Ok(*last),
				None => ir.parent_op(held.parent).ok_or(IrError::Stale("insert point")),
			}
		}
	}
}

/// The description of a re-created `data`, reading the renamed values where `renamed` has them.
fn respec(
	ir: &Ir,
	data: &OpData,
	renamed: &FxHashMap<ValueId, ValueId>,
) -> Result<OpSpec, IrError> {
	let operands = data
		.operands
		.iter()
		.map(|operand| renamed.get(operand).copied().unwrap_or(*operand))
		.collect();
	let mut results: SmallVec<[TypeKnowledge; 2]> = SmallVec::new();
	for result in &data.results {
		results.push(ir.value(*result).ok_or(IrError::Stale("value"))?.ty.clone());
	}
	Ok(OpSpec {
		kind: data.kind.clone(),
		operands,
		results,
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc.clone(),
	})
}

/// Refuses a placement that would break the tree: inside it, past a terminator, or where a value
/// it reads is not visible.
fn check_placement(
	ir: &Ir,
	tree: &[OpId],
	members: &FxHashSet<OpId>,
	block: BlockId,
	index: usize,
	bound: &[(ValueId, ValueId)],
) -> Result<(), IrError> {
	if block_inside(ir, block, members) {
		return Err(IrError::Refused("rehome insert point"));
	}
	for (from, to) in bound {
		if defined_within(ir, *from, members) {
			return Err(IrError::Refused("rehome bind"));
		}
		if !visible_at(ir, *to, block, index) {
			return Err(IrError::Refused("rehome escapes"));
		}
	}
	for op in tree {
		let data = ir.op(*op).ok_or(IrError::Stale("op"))?;
		if is_terminator(&data.kind) {
			return Err(IrError::Refused("rehome terminator"));
		}
		let mut reads: Vec<ValueId> = data.operands.to_vec();
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut |nested| {
				if let Some(inner) = ir.op(nested) {
					reads.extend(inner.operands.iter().copied());
				}
				Walk::Continue
			});
		}
		for read in reads {
			let rebound = bound.iter().any(|(from, _)| *from == read);
			if !rebound && !defined_within(ir, read, members) && !visible_at(ir, read, block, index)
			{
				return Err(IrError::Refused("rehome escapes"));
			}
		}
	}
	Ok(())
}

/// Whether `kind` ends a block.
fn is_terminator(kind: &OpKind) -> bool {
	if kind.terminator().is_some() {
		return true;
	}
	let OpKind::Expr(ExprOp::Yield) = kind else {
		return false;
	};
	true
}

/// Whether `block` is a region of a member, or lies inside one.
fn block_inside(ir: &Ir, block: BlockId, members: &FxHashSet<OpId>) -> bool {
	let mut current = block;
	loop {
		let Some(owner) = ir.block(current).and_then(|held| ir.parent_op(held.parent)) else {
			return false;
		};
		if members.contains(&owner) {
			return true;
		}
		match ir.op(owner) {
			Some(data) => current = data.parent,
			None => return false,
		}
	}
}

/// Whether `value` is visible to an op created at position `index` of `block`: a block argument of
/// `block` or of a block around it, or the result of an op that stands before `index`, or before
/// the op around it. A value never reaches into a region that is isolated from above.
fn visible_at(ir: &Ir, value: ValueId, block: BlockId, index: usize) -> bool {
	let Some(def) = ir.value(value).map(|data| data.def) else {
		return false;
	};
	let mut current = block;
	let mut limit = index;
	loop {
		match def {
			ValueDef::BlockArg {
				block: held,
				..
			} if held == current => return true,
			ValueDef::OpResult {
				op,
				..
			} if ir.op(op).is_some_and(|data| data.parent == current) => {
				return ir.block(current).is_some_and(|held| {
					held.ops
						.iter()
						.position(|candidate| *candidate == op)
						.is_some_and(|at| at < limit)
				});
			}
			ValueDef::BlockArg {
				..
			}
			| ValueDef::OpResult {
				..
			} => {}
		}
		let Some(region) = ir.block(current).map(|held| held.parent) else {
			return false;
		};
		if !encloses_value(ir, region) {
			return false;
		}
		let Some(owner) = ir.parent_op(region) else {
			return false;
		};
		let Some(parent) = ir.op(owner).map(|data| data.parent) else {
			return false;
		};
		let Some(position) =
			ir.block(parent).and_then(|held| held.ops.iter().position(|op| *op == owner))
		else {
			return false;
		};
		current = parent;
		limit = position;
	}
}

/// Whether a value defined outside `region` can be read inside it.
fn encloses_value(ir: &Ir, region: RegionId) -> bool {
	ir.region(region).is_some_and(|held| !held.kind.isolated_from_above())
}

/// The statement copy a Copy-mode rehome clones the tree's regions from, with the correspondence
/// of ids. Refuses with `Refused("rehome copy")` when there is nothing to clone from, or a region
/// cannot be cloned.
fn clone_source(ir: &Ir, root: OpId, tree: &[OpId]) -> Result<(Ir, StatementCopy), IrError> {
	let refused = || IrError::Refused("rehome copy");
	let mut owned: Vec<RegionId> = Vec::new();
	for op in tree {
		owned.extend(ir.op(*op).ok_or(IrError::Stale("op"))?.regions.iter().copied());
	}
	for region in &owned {
		if !self_contained(ir, *region) {
			return Err(refused());
		}
	}
	let Some(statement) = enclosing_statement(ir, root) else {
		return Err(refused());
	};
	ir.copy_statement(statement)
}

/// Whether every op inside `region` reads only values defined inside it.
fn self_contained(ir: &Ir, region: RegionId) -> bool {
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
	loop {
		if current == region {
			return true;
		}
		let Some(owner) = ir.parent_op(current) else {
			return false;
		};
		current = ir.enclosing_region(owner);
	}
}
