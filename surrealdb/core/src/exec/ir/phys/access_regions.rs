//! The condition regions of an access op, built from the conjuncts of a filter.
//!
//! A scan that judges part of a statement's predicate itself, such as a `KNN` search, holds those
//! conjuncts as regions of its own. [`conjunct_region`] builds one: a `Clause(Where)` region under
//! the scan whose body is a copy of the cones of the chosen conjuncts of the filter's region,
//! joined by `AND` in the order the filter states them. The filter's own ops are never moved, so
//! the filter keeps the whole predicate.
//!
//! A copy reads what the original reads. A value the filter's region defines is copied; a value
//! defined outside it is shared. A filter's clause reads its own row argument and values defined
//! around the statement, so every shared value is visible where the scan stands; one that is not
//! is an [`IrError::Invariant`].

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;

use super::access_expr::conjunct_values;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;
use crate::val::Value;

/// The copy of the ops of one region into another.
struct Copy {
	/// The region copied from: a value defined inside it is copied.
	from: RegionId,
	/// The region the copy stands in, which a shared value must be visible from.
	into: RegionId,
	/// The value each copied value became.
	values: FxHashMap<ValueId, ValueId>,
	/// The rows of `from` a rowless copy stands with no value for ([`rowless_value_copy`]): its
	/// block arguments. Empty for any other copy.
	rowless: SmallVec<[ValueId; 2]>,
}

/// Builds a `Clause(Where)` region under `owner` that yields the conjunction of the conjuncts of
/// the `Clause(Where)` region `src` at the positions `selected`, counted from the first conjunct
/// of `src`.
///
/// `owner` stands where `visible_at` stands: a value `src` reads that it does not define must be
/// visible there. The region yields the conjunction under a truthiness coercion where `src` does.
pub(super) fn conjunct_region(
	rw: &mut Rewriter<'_>,
	owner: OpId,
	visible_at: OpId,
	src: RegionId,
	selected: &[usize],
) -> Result<RegionId, IrError> {
	conjunct_region_across(rw, owner, visible_at, &[(src, selected)])
}

/// Builds a `Clause(Where)` region under `owner` that yields the conjunction of the conjuncts the
/// `parts` select, each part a `Clause(Where)` region of a filter and the positions of its
/// conjuncts to take, counted from the first conjunct of that region.
///
/// The parts are the filters of one chain, each judging the rows the one before it passed on, so
/// the conjunction takes the parts in order and a conjunct of a later part is judged only for a
/// row the earlier ones hold, as the filters judge it. Every part's row argument is the new
/// region's. The region yields the conjunction under a truthiness coercion where any part does.
/// The coercion leaves a part that already yields a boolean unchanged.
///
/// `owner` stands where `visible_at` stands: a value a part reads that it does not define is
/// visible there, and one that is not is an [`IrError::Invariant`]. The parts must take the same
/// block arguments, and select at least one conjunct between them.
pub(super) fn conjunct_region_across(
	rw: &mut Rewriter<'_>,
	owner: OpId,
	visible_at: OpId,
	parts: &[(RegionId, &[usize])],
) -> Result<RegionId, IrError> {
	let ir = rw.ir();
	let mut chosen: Vec<(usize, ValueId)> = Vec::new();
	let mut truthy = false;
	for (part, (src, selected)) in parts.iter().enumerate() {
		let (leaves, coerced) = conjunct_values(ir, *src)
			.ok_or(IrError::Stale("a where clause that yields no value"))?;
		truthy |= coerced;
		for at in *selected {
			chosen.push((
				part,
				leaves.get(*at).copied().ok_or(IrError::Stale("a conjunct of the clause"))?,
			));
		}
	}
	let (_, first) = *chosen.first().ok_or(IrError::Stale("a region of no conjunct"))?;
	let (lead, _) = *parts.first().ok_or(IrError::Stale("a region of no clause"))?;
	let lead_block = single_block(ir, lead)?;
	let arg_types: Vec<TypeKnowledge> = ir
		.block(lead_block)
		.map(|block| {
			block
				.args
				.iter()
				.filter_map(|arg| ir.value(*arg).map(|value| value.ty.clone()))
				.collect()
		})
		.unwrap_or_default();
	let mut source_args: Vec<SmallVec<[ValueId; 2]>> = Vec::with_capacity(parts.len());
	for (src, _) in parts {
		let block = single_block(ir, *src)?;
		let args: SmallVec<[ValueId; 2]> =
			ir.block(block).map(|held| held.args.iter().copied().collect()).unwrap_or_default();
		if args.len() != arg_types.len() {
			return Err(IrError::Invariant {
				what: "access_regions::conjunct_region_across: clauses that take different arguments",
			});
		}
		source_args.push(args);
	}
	let frame_of = yield_op(ir, lead).ok_or(IrError::Stale("the yield of a where clause"))?;
	let (frame, snapshot, loc) = {
		let data = ir.op(frame_of).ok_or(IrError::Stale("op"))?;
		(data.frame, data.snapshot, data.loc.clone())
	};
	let into = ir.enclosing_region(visible_at);
	let (region, block) =
		rw.create_region(owner, RegionKind::Clause(ClauseRole::Where), &arg_types)?;
	let args: SmallVec<[ValueId; 2]> =
		rw.ir().block(block).map(|held| held.args.iter().copied().collect()).unwrap_or_default();
	let mut copies: Vec<Copy> = Vec::with_capacity(parts.len());
	for ((src, _), old_args) in parts.iter().zip(&source_args) {
		let mut copy = Copy {
			from: *src,
			into,
			values: FxHashMap::default(),
			rowless: SmallVec::new(),
		};
		for (old, new) in old_args.iter().zip(args.iter()) {
			copy.values.insert(*old, *new);
		}
		copies.push(copy);
	}
	let make = |kind: ExprOp, operands: SmallVec<[ValueId; 4]>| OpSpec {
		kind: OpKind::Expr(kind),
		operands,
		results: smallvec![TypeKnowledge::Unknown],
		frame,
		snapshot,
		loc: loc.clone(),
	};
	let mut acc = copy_value(rw, &mut copies[chosen[0].0], first, InsertPoint::End(block))?;
	for (part, value) in chosen.iter().skip(1) {
		let joined = rw.create(
			InsertPoint::End(block),
			make(
				ExprOp::ShortCircuit {
					op: ShortCircuitOp::And,
				},
				smallvec![acc],
			),
		)?;
		let (_, lazy) = rw.create_region(joined, RegionKind::LazyOperand, &[])?;
		let right = copy_value(rw, &mut copies[*part], *value, InsertPoint::End(lazy))?;
		rw.create(
			InsertPoint::End(lazy),
			OpSpec {
				kind: OpKind::Expr(ExprOp::Yield),
				operands: smallvec![right],
				results: SmallVec::new(),
				frame,
				snapshot,
				loc: loc.clone(),
			},
		)?;
		acc = result_of(rw.ir(), joined)?;
	}
	if truthy {
		let coerced = rw.create(InsertPoint::End(block), make(ExprOp::Truthy, smallvec![acc]))?;
		acc = result_of(rw.ir(), coerced)?;
	}
	rw.create(
		InsertPoint::End(block),
		OpSpec {
			kind: OpKind::Expr(ExprOp::Yield),
			operands: smallvec![acc],
			results: SmallVec::new(),
			frame,
			snapshot,
			loc,
		},
	)?;
	Ok(region)
}

/// What the values of a source region stand for in a region built from its cone.
pub(super) struct Stands<'a> {
	/// The values that read the new region's one argument.
	pub(super) document: &'a [ValueId],
	/// The values that read `NONE`.
	pub(super) absent: &'a [ValueId],
}

/// Builds a region of kind `kind` under `owner` that takes one argument and yields a copy of the
/// cone of `value`, which the region `src`, or a region inside it, defines.
///
/// A value of `src` that `stands` names is replaced in the copy by the value `stands` gives; every
/// other value of `src` the cone reads is copied with the ops that define it. `owner` stands where
/// `visible_at` stands: a value the cone reads that `src` does not define is visible there, and one
/// that is not is an [`IrError::Invariant`]. A block argument of `src` the cone reads that `stands`
/// does not name has no copy ([`IrError::Stale`]).
pub(super) fn value_region(
	rw: &mut Rewriter<'_>,
	owner: OpId,
	visible_at: OpId,
	kind: RegionKind,
	src: RegionId,
	stands: &Stands<'_>,
	value: ValueId,
) -> Result<RegionId, IrError> {
	let ir = rw.ir();
	let (frame, snapshot, loc) = {
		let at = match ir.value(value).ok_or(IrError::Stale("value"))?.def {
			ValueDef::OpResult {
				op,
				..
			} => op,
			ValueDef::BlockArg {
				..
			} => ops_in(ir, src).last().ok_or(IrError::Stale("a region of no op"))?,
		};
		let data = ir.op(at).ok_or(IrError::Stale("op"))?;
		(data.frame, data.snapshot, data.loc.clone())
	};
	let into = ir.enclosing_region(visible_at);
	let (region, block) = rw.create_region(owner, kind, &[TypeKnowledge::Unknown])?;
	let arg = rw
		.ir()
		.block(block)
		.and_then(|held| held.args.first().copied())
		.ok_or(IrError::Stale("a region argument"))?;
	let mut copy = Copy {
		from: src,
		into,
		values: FxHashMap::default(),
		rowless: SmallVec::new(),
	};
	for old in stands.document {
		copy.values.insert(*old, arg);
	}
	for old in stands.absent {
		let none = rw.create(
			InsertPoint::End(block),
			OpSpec {
				kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::None))),
				operands: SmallVec::new(),
				results: smallvec![TypeKnowledge::Unknown],
				frame,
				snapshot,
				loc: loc.clone(),
			},
		)?;
		copy.values.insert(*old, result_of(rw.ir(), none)?);
	}
	let copied = copy_value(rw, &mut copy, value, InsertPoint::End(block))?;
	rw.create(
		InsertPoint::End(block),
		OpSpec {
			kind: OpKind::Expr(ExprOp::Yield),
			operands: smallvec![copied],
			results: SmallVec::new(),
			frame,
			snapshot,
			loc,
		},
	)?;
	Ok(region)
}

/// Copies the region `src` whole as a region of `owner`: its kind, its block arguments and every
/// op of its body, in order, each in the frame it held.
///
/// `owner` stands where `visible_at` stands: a value `src` reads that it does not define is
/// visible there, and one that is not is an [`IrError::Invariant`].
pub(super) fn region_copy(
	rw: &mut Rewriter<'_>,
	owner: OpId,
	visible_at: OpId,
	src: RegionId,
) -> Result<(), IrError> {
	let mut copy = Copy {
		from: src,
		into: rw.ir().enclosing_region(visible_at),
		values: FxHashMap::default(),
		rowless: SmallVec::new(),
	};
	copy_region(rw, &mut copy, src, owner)
}

/// Copies the cone of `value`, a `KNN` query vector the region `src` computes, to stand before `at`
/// as [`value_copy`] does, with each block argument of `src`, the row being scanned, read as
/// `$this` where the copy stands, and returns the copy of `value`. The read is left out where no
/// op of the copy takes the row as an operand.
///
/// The vector is evaluated once, with no row in scope and the row being scanned forbidden to it
/// ([`ForbiddenRow`](crate::exec::context::ForbiddenRow)), so a read of that `$this` fails with the
/// vector's refusal; a call's row names, which the call is lowered without, are never read. A
/// closure literal that captures the row by a name is copied to read that name from the frame it
/// is made in instead, as a literal written where no row is in scope does: the frame binds it to
/// the row being scanned, and the closure carries the refusal to an invocation that reads it, so
/// making the closure and passing it along read nothing.
pub(super) fn rowless_value_copy(
	rw: &mut Rewriter<'_>,
	src: RegionId,
	value: ValueId,
	at: OpId,
) -> Result<ValueId, IrError> {
	let args: Vec<ValueId> = rw
		.ir()
		.region(src)
		.and_then(|data| data.blocks.first())
		.and_then(|block| rw.ir().block(*block))
		.map(|block| block.args.to_vec())
		.unwrap_or_default();
	let mut copy = Copy {
		from: src,
		into: rw.ir().enclosing_region(at),
		values: FxHashMap::default(),
		rowless: args.iter().copied().collect(),
	};
	if args.is_empty() || !reads_region_args(rw.ir(), src, value) {
		return copy_value(rw, &mut copy, value, InsertPoint::Before(at));
	}
	let data = rw.ir().op(at).ok_or(IrError::Stale("op"))?;
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let read = rw.create(
		InsertPoint::Before(at),
		OpSpec {
			kind: OpKind::Expr(ExprOp::FreeParam {
				name: Strand::new("this"),
			}),
			operands: SmallVec::new(),
			results: smallvec![TypeKnowledge::Unknown],
			frame,
			snapshot,
			loc,
		},
	)?;
	let this = result_of(rw.ir(), read)?;
	for arg in args {
		copy.values.insert(arg, this);
	}
	let copied = copy_value(rw, &mut copy, value, InsertPoint::Before(at))?;
	// A read nothing takes would run where it stands and refuse the vector.
	if rw.ir().uses(this).is_empty() {
		rw.erase(read, EliminationReason::DeadPure)?;
	}
	Ok(copied)
}

/// Copies the cone of `value`, which the region `src` defines, to stand before `at`, and returns
/// the copy of `value`.
///
/// Every op of the cone that `src` defines is copied in data-flow order, with the regions it owns;
/// a value defined outside `src` is shared. A clause reads values defined around its statement, so
/// every shared value is visible where `at` stands; one that is not is an [`IrError::Invariant`].
/// The cone must read no block argument of `src`: such a value has no copy outside the region
/// ([`IrError::Stale`]).
pub(super) fn value_copy(
	rw: &mut Rewriter<'_>,
	src: RegionId,
	value: ValueId,
	at: OpId,
) -> Result<ValueId, IrError> {
	let mut copy = Copy {
		from: src,
		into: rw.ir().enclosing_region(at),
		values: FxHashMap::default(),
		rowless: SmallVec::new(),
	};
	copy_value(rw, &mut copy, value, InsertPoint::Before(at))
}

/// Whether the cone of `value` reads a block argument of the region `src`, looking into the
/// regions of the ops of the cone that `src` defines. A cone that does not can be copied out of
/// `src` ([`value_copy`]).
pub(super) fn reads_region_args(ir: &Ir, src: RegionId, value: ValueId) -> bool {
	let args: Vec<ValueId> = ir
		.region(src)
		.and_then(|data| data.blocks.first())
		.and_then(|block| ir.block(*block))
		.map(|block| block.args.to_vec())
		.unwrap_or_default();
	let mut seen = FxHashSet::default();
	let mut pending = vec![value];
	while let Some(at) = pending.pop() {
		if !seen.insert(at) {
			continue;
		}
		if args.contains(&at) {
			return true;
		}
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(at).map(|data| data.def)
		else {
			continue;
		};
		if !region_of(ir, at).is_some_and(|defined| encloses(ir, src, defined)) {
			continue;
		}
		// The op and every op of the regions it owns, at any depth.
		let mut owners = vec![op];
		while let Some(owner) = owners.pop() {
			let Some(data) = ir.op(owner) else {
				continue;
			};
			pending.extend(data.operands.iter().copied());
			for region in &data.regions {
				owners.extend(ops_in(ir, *region));
			}
		}
	}
	false
}

/// Whether every op of the cone of `value` that the region `src` defines, with the ops of the
/// regions they own at any depth, is an expression op: a cone a conversion can copy, which holds
/// no op the conversion has yet to convert.
pub(super) fn cone_is_expr(ir: &Ir, src: RegionId, value: ValueId) -> bool {
	let mut seen = FxHashSet::default();
	let mut pending = vec![value];
	while let Some(at) = pending.pop() {
		if !seen.insert(at) {
			continue;
		}
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(at).map(|data| data.def)
		else {
			continue;
		};
		if !region_of(ir, at).is_some_and(|defined| encloses(ir, src, defined)) {
			continue;
		}
		let mut owners = vec![op];
		while let Some(owner) = owners.pop() {
			let Some(data) = ir.op(owner) else {
				continue;
			};
			if !matches!(data.kind, OpKind::Expr(_)) {
				return false;
			}
			pending.extend(data.operands.iter().copied());
			for region in &data.regions {
				owners.extend(ops_in(ir, *region));
			}
		}
	}
	true
}

/// Whether the region `src`, or a region inside it, defines `value`.
pub(super) fn defined_in(ir: &Ir, src: RegionId, value: ValueId) -> bool {
	region_of(ir, value).is_some_and(|defined| encloses(ir, src, defined))
}

/// The single block of `region`.
fn single_block(ir: &Ir, region: RegionId) -> Result<BlockId, IrError> {
	match ir.region(region).map(|data| data.blocks.as_slice()) {
		Some([block]) => Ok(*block),
		_ => Err(IrError::Stale("a region of one block")),
	}
}

/// The terminator of the body of `region`, when it ends in a yield.
fn yield_op(ir: &Ir, region: RegionId) -> Option<OpId> {
	let last = ops_in(ir, region).last()?;
	matches!(ir.op(last)?.kind, OpKind::Expr(ExprOp::Yield)).then_some(last)
}

/// The first result of `op`.
fn result_of(ir: &Ir, op: OpId) -> Result<ValueId, IrError> {
	ir.op(op).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("result"))
}

/// The region `value` is defined in.
fn region_of(ir: &Ir, value: ValueId) -> Option<RegionId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(ir.enclosing_region(op)),
		ValueDef::BlockArg {
			block,
			..
		} => ir.block(block).map(|held| held.parent),
	}
}

/// Whether `outer` is `inner` or a region around it.
fn encloses(ir: &Ir, outer: RegionId, inner: RegionId) -> bool {
	let mut at = inner;
	loop {
		if at == outer {
			return true;
		}
		let Some(owner) = ir.region(at).and_then(|data| data.parent) else {
			return false;
		};
		at = ir.enclosing_region(owner);
	}
}

/// The copy of `value` at `at`, copying its defining op and the cone of what it reads that the
/// source region defines. A value the source region does not define is shared.
fn copy_value(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	value: ValueId,
	at: InsertPoint,
) -> Result<ValueId, IrError> {
	if let Some(held) = copy.values.get(&value) {
		return Ok(*held);
	}
	let ir = rw.ir();
	let defined = region_of(ir, value).ok_or(IrError::Stale("value"))?;
	if !encloses(ir, copy.from, defined) {
		if !encloses(ir, defined, copy.into) {
			return Err(IrError::Invariant {
				what: "access_regions::copy_value: a condition that reads a value its scan cannot see",
			});
		}
		return Ok(value);
	}
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value).ok_or(IrError::Stale("value"))?.def
	else {
		return Err(IrError::Stale("a block argument the copy does not map"));
	};
	copy_op(rw, copy, op, at)?;
	copy.values.get(&value).copied().ok_or(IrError::Stale("a copied value"))
}

/// The positions among the operands of `data`, an `expr.closure_create` of a rowless copy
/// ([`rowless_value_copy`]), of the captures that are a row of the copy; empty for any other op.
fn rowless_captures(copy: &Copy, data: &OpData) -> SmallVec<[usize; 2]> {
	if copy.rowless.is_empty() || !matches!(&data.kind, OpKind::Expr(ExprOp::ClosureCreate(_))) {
		return SmallVec::new();
	}
	data.operands
		.iter()
		.enumerate()
		.filter(|(_, operand)| copy.rowless.contains(operand))
		.map(|(at, _)| at)
		.collect()
}

/// The positions among the operands of `data`, the op `op` of a rowless copy
/// ([`rowless_value_copy`]), of the row names of an `expr.call` that are a row of the copy and
/// that the call's lowering does not read; empty for any other op. The lowering reads none of
/// them but the first, and that one only where the call stands in a loop's body, which it
/// compares with the loop's element. The copy passes each as an `expr.free_param` of `$parent`
/// made beside the call, which nothing reads either, rather than as the read of the row the copy
/// makes where it stands.
fn rowless_unread_row_names(ir: &Ir, copy: &Copy, op: OpId, data: &OpData) -> SmallVec<[usize; 2]> {
	if copy.rowless.is_empty() {
		return SmallVec::new();
	}
	let OpKind::Expr(ExprOp::Call(call)) = &data.kind else {
		return SmallVec::new();
	};
	let first_row_name = data.operands.len().saturating_sub(usize::from(call.row_names));
	let in_loop = ancestors(ir, op)
		.take_while(|(region, _)| *region != copy.from)
		.any(|(region, _)| ir.region(region).is_some_and(|held| held.kind == RegionKind::Loop));
	let unread_from = if in_loop {
		first_row_name + 1
	} else {
		first_row_name
	};
	data.operands
		.iter()
		.enumerate()
		.skip(unread_from)
		.filter(|(_, operand)| copy.rowless.contains(operand))
		.map(|(at, _)| at)
		.collect()
}

/// An `expr.free_param` of `name`, made at `at` with the frame, snapshot and location of `data`.
fn free_param_before(
	rw: &mut Rewriter<'_>,
	data: &OpData,
	name: &str,
	at: InsertPoint,
) -> Result<ValueId, IrError> {
	let read = rw.create(
		at,
		OpSpec {
			kind: OpKind::Expr(ExprOp::FreeParam {
				name: Strand::new(name),
			}),
			operands: SmallVec::new(),
			results: smallvec![TypeKnowledge::Unknown],
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
	)?;
	result_of(rw.ir(), read)
}

/// Copies `data`, an `expr.closure_create` whose captures at `dropped` are a row of a rowless
/// copy, to `at` without those captures: its body reads each such name from the frame the closure
/// is made in, as an `expr.free_param` at the head of the body.
fn copy_closure_reading_rows_by_name(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	op: OpId,
	data: &OpData,
	dropped: &[usize],
	at: InsertPoint,
) -> Result<OpId, IrError> {
	let OpKind::Expr(ExprOp::ClosureCreate(attrs)) = &data.kind else {
		return Err(IrError::Stale("a closure literal"));
	};
	let mut kept = attrs.as_ref().clone();
	kept.captures = attrs
		.captures
		.iter()
		.enumerate()
		.filter(|(position, _)| !dropped.contains(position))
		.map(|(_, name)| name.clone())
		.collect();
	let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
	for (position, operand) in data.operands.iter().enumerate() {
		if !dropped.contains(&position) {
			operands.push(copy_value(rw, copy, *operand, at)?);
		}
	}
	let results: SmallVec<[TypeKnowledge; 2]> = data
		.results
		.iter()
		.map(|result| {
			rw.ir()
				.value(*result)
				.map(|value| value.ty.clone())
				.ok_or(IrError::Stale("result value"))
		})
		.collect::<Result<_, _>>()?;
	let new = rw.create(
		at,
		OpSpec {
			kind: OpKind::Expr(ExprOp::ClosureCreate(Box::new(kept))),
			operands,
			results,
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
	)?;
	let made: SmallVec<[ValueId; 2]> =
		rw.ir().op(new).map(|held| held.results.clone()).unwrap_or_default();
	for (old, now) in data.results.iter().zip(made.iter()) {
		copy.values.insert(*old, *now);
	}
	let [body] = data.regions.as_slice() else {
		return Err(IrError::Stale("a closure literal's body"));
	};
	// The body's arguments are the timestamp, the parameters, the captures in operand order and
	// the rows it takes; a dropped capture's argument becomes a read of its name.
	let first_capture = 1 + attrs.params.len();
	let names: Vec<(usize, Strand)> = dropped
		.iter()
		.map(|position| {
			let name = attrs
				.captures
				.get(*position)
				.cloned()
				.ok_or(IrError::Stale("a closure capture's name"))?;
			Ok((first_capture + position, name))
		})
		.collect::<Result<_, IrError>>()?;
	copy_region_reading_by_name(rw, copy, *body, new, &names)?;
	rw.carry_decisions(op, new);
	Ok(new)
}

/// Copies the op `op` to `at`, with the regions it owns.
fn copy_op(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	op: OpId,
	at: InsertPoint,
) -> Result<OpId, IrError> {
	let data = rw.ir().op(op).ok_or(IrError::Stale("op"))?.clone();
	let dropped = rowless_captures(copy, &data);
	if !dropped.is_empty() {
		return copy_closure_reading_rows_by_name(rw, copy, op, &data, &dropped, at);
	}
	let unread = rowless_unread_row_names(rw.ir(), copy, op, &data);
	let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
	for (position, operand) in data.operands.iter().enumerate() {
		let copied = match unread.contains(&position) {
			true => free_param_before(rw, &data, "parent", at)?,
			false => copy_value(rw, copy, *operand, at)?,
		};
		operands.push(copied);
	}
	let results: SmallVec<[TypeKnowledge; 2]> = data
		.results
		.iter()
		.map(|result| {
			rw.ir()
				.value(*result)
				.map(|value| value.ty.clone())
				.ok_or(IrError::Stale("result value"))
		})
		.collect::<Result<_, _>>()?;
	let new = rw.create(
		at,
		OpSpec {
			kind: data.kind.clone(),
			operands,
			results,
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
	)?;
	let made: SmallVec<[ValueId; 2]> =
		rw.ir().op(new).map(|held| held.results.clone()).unwrap_or_default();
	for (old, now) in data.results.iter().zip(made.iter()) {
		copy.values.insert(*old, *now);
	}
	for region in &data.regions {
		copy_region(rw, copy, *region, new)?;
	}
	rw.carry_decisions(op, new);
	Ok(new)
}

/// Copies the region `region` as a region of `owner`: its kind, its block arguments and every op
/// of its body, in order.
fn copy_region(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	region: RegionId,
	owner: OpId,
) -> Result<(), IrError> {
	copy_region_reading_by_name(rw, copy, region, owner, &[])
}

/// [`copy_region`], with each block argument of `region` at a position `by_name` lists left out
/// of the copy's arguments and read instead by an `expr.free_param` of the name it lists, at the
/// head of the copy's block.
fn copy_region_reading_by_name(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	region: RegionId,
	owner: OpId,
	by_name: &[(usize, Strand)],
) -> Result<(), IrError> {
	let ir = rw.ir();
	let kind = ir.region(region).ok_or(IrError::Stale("region"))?.kind;
	let block = single_block(ir, region)?;
	let old_args: SmallVec<[ValueId; 2]> =
		ir.block(block).map(|held| held.args.iter().copied().collect()).unwrap_or_default();
	let named = |position: usize| by_name.iter().find(|(at, _)| *at == position);
	let kept: SmallVec<[ValueId; 2]> = old_args
		.iter()
		.enumerate()
		.filter(|(position, _)| named(*position).is_none())
		.map(|(_, arg)| *arg)
		.collect();
	let types: Vec<TypeKnowledge> = kept
		.iter()
		.map(|arg| ir.value(*arg).map(|value| value.ty.clone()).ok_or(IrError::Stale("value")))
		.collect::<Result<_, _>>()?;
	let body: Vec<OpId> = ops_in(ir, region).collect();
	let frame = rw.ir().op(owner).map(|data| (data.frame, data.snapshot, data.loc.clone()));
	let (_, new_block) = rw.create_region(owner, kind, &types)?;
	let new_args: SmallVec<[ValueId; 2]> = rw
		.ir()
		.block(new_block)
		.map(|held| held.args.iter().copied().collect())
		.unwrap_or_default();
	for (old, now) in kept.iter().zip(new_args.iter()) {
		copy.values.insert(*old, *now);
	}
	if !by_name.is_empty() {
		let (frame, snapshot, loc) = frame.ok_or(IrError::Stale("op"))?;
		for (position, name) in by_name {
			let old = *old_args.get(*position).ok_or(IrError::Stale("a region argument"))?;
			let read = rw.create(
				InsertPoint::End(new_block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::FreeParam {
						name: name.clone(),
					}),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame,
					snapshot,
					loc: loc.clone(),
				},
			)?;
			copy.values.insert(old, result_of(rw.ir(), read)?);
		}
	}
	for member in body {
		copy_op(rw, copy, member, InsertPoint::End(new_block))?;
	}
	Ok(())
}
