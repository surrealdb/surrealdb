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

use std::collections::HashMap;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;

use super::access_expr::conjunct_values;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
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
	values: HashMap<ValueId, ValueId>,
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
			values: HashMap::new(),
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
		values: HashMap::new(),
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
		values: HashMap::new(),
	};
	copy_region(rw, &mut copy, src, owner)
}

/// Copies the cone of `value`, a `KNN` query vector the region `src` computes, to stand before `at`
/// as [`value_copy`] does, with each block argument of `src`, the row being scanned, read as
/// `$this` where the copy stands, and returns the copy of `value`.
///
/// The vector is evaluated once, with no row in scope and the row being scanned forbidden to it
/// ([`ForbiddenRow`](crate::exec::context::ForbiddenRow)), so a read of that `$this` fails with the
/// vector's refusal; a call's row names, which the call is lowered without, are never read.
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
		values: HashMap::new(),
	};
	if !args.is_empty() && reads_region_args(rw.ir(), src, value) {
		let data = rw.ir().op(at).ok_or(IrError::Stale("op"))?;
		let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
		let this = rw.create(
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
		let this = result_of(rw.ir(), this)?;
		for arg in args {
			copy.values.insert(arg, this);
		}
	}
	copy_value(rw, &mut copy, value, InsertPoint::Before(at))
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
		values: HashMap::new(),
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
	let mut seen = std::collections::HashSet::new();
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
	let mut seen = std::collections::HashSet::new();
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

/// Copies the op `op` to `at`, with the regions it owns.
fn copy_op(
	rw: &mut Rewriter<'_>,
	copy: &mut Copy,
	op: OpId,
	at: InsertPoint,
) -> Result<OpId, IrError> {
	let data = rw.ir().op(op).ok_or(IrError::Stale("op"))?.clone();
	let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
	for operand in &data.operands {
		operands.push(copy_value(rw, copy, *operand, at)?);
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
	let ir = rw.ir();
	let kind = ir.region(region).ok_or(IrError::Stale("region"))?.kind;
	let block = single_block(ir, region)?;
	let old_args: SmallVec<[ValueId; 2]> =
		ir.block(block).map(|held| held.args.iter().copied().collect()).unwrap_or_default();
	let types: Vec<TypeKnowledge> = old_args
		.iter()
		.map(|arg| ir.value(*arg).map(|value| value.ty.clone()).ok_or(IrError::Stale("value")))
		.collect::<Result<_, _>>()?;
	let body: Vec<OpId> = ops_in(ir, region).collect();
	let (_, new_block) = rw.create_region(owner, kind, &types)?;
	let new_args: SmallVec<[ValueId; 2]> = rw
		.ir()
		.block(new_block)
		.map(|held| held.args.iter().copied().collect())
		.unwrap_or_default();
	for (old, now) in old_args.iter().zip(new_args.iter()) {
		copy.values.insert(*old, *now);
	}
	for member in body {
		copy_op(rw, copy, member, InsertPoint::End(new_block))?;
	}
	Ok(())
}
