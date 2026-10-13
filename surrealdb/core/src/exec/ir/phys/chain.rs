//! Reading a chain of row ops through the containers it is wrapped in.
//!
//! The binder states the per-row ops of a `SELECT` (a predicate, a permission, a layer, a fetch, a
//! projection) as the body of a `phys.loop` of kind select, and [`super::pipeline::build`] wraps
//! each such loop in a `phys.pipeline`. The ops that break a pipeline (a sort, a limit, an
//! aggregate) and the source that feeds it stand outside. A rule that reads the chain of ops
//! beneath one of them follows the chain as one: the rows a sort reads are the rows the pipeline
//! delivers, which are the rows the select loop delivers, which are the rows its body's last op
//! delivers, and an op of the body reads the rows its source delivers through the loop's element.
//!
//! [`enter`] is the one step of that walk. It never judges an op: whether a rule may cross the ops
//! it meets, and whether the value it stands at has another reader, stay with the rule.
//!
//! A select loop is read through only when its iterations are the rows of its source one by one: no
//! body for the first row, and no step. A loop of another kind, or one that states a step, is an
//! op like any other for the walk, and the rules decline at it.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::PhysOp;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;

/// The source and the body of `op` when it is a select loop that a chain is read through.
pub(super) fn select_loop(ir: &Ir, op: OpId) -> Option<(ValueId, RegionId)> {
	let data = ir.op(op)?;
	let OpKind::Phys(PhysOp::Loop {
		kind: LoopKind::Select,
		first: false,
		step: None,
		..
	}) = &data.kind
	else {
		return None;
	};
	let ([source], [body], [_]) =
		(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
	else {
		return None;
	};
	Some((*source, *body))
}

/// The value the single block of `region` delivers: the operand of its closing `phys.yield`.
fn delivered(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	let last = ir.op(*ir.block(*block)?.ops.last()?)?;
	match (&last.kind, last.operands.as_slice()) {
		(OpKind::Phys(PhysOp::Yield), [value]) => Some(*value),
		_ => None,
	}
}

/// The value a walk that stands at `value` continues from when `value` is the edge of a container
/// the walk reads through, and `None` when it is not.
///
/// - The result of a `phys.pipeline` is the value its region delivers.
/// - The result of a select loop is the value its body delivers.
/// - The element of a select loop's body is the loop's source.
///
/// The answer is always a value of the same rows: the container is transparent to a chain, and
/// each step only names the value on its other side.
pub(super) fn enter(ir: &Ir, value: ValueId) -> Option<ValueId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			index: 0,
		} => {
			let data = ir.op(op)?;
			match &data.kind {
				OpKind::Phys(PhysOp::Pipeline(_)) => {
					let [region] = data.regions.as_slice() else {
						return None;
					};
					delivered(ir, *region)
				}
				OpKind::Phys(PhysOp::Loop {
					..
				}) => delivered(ir, select_loop(ir, op)?.1),
				OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_) => None,
			}
		}
		ValueDef::BlockArg {
			block,
			index: 0,
		} => {
			let region = ir.block(block)?.parent;
			let (source, body) = select_loop(ir, ir.region(region)?.parent?)?;
			(body == region).then_some(source)
		}
		ValueDef::OpResult {
			..
		}
		| ValueDef::BlockArg {
			..
		} => None,
	}
}

/// The select loop whose body `value` is the element of, when it is one.
pub(super) fn element_loop(ir: &Ir, value: ValueId) -> Option<OpId> {
	let ValueDef::BlockArg {
		block,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let region = ir.block(block)?.parent;
	let owner = ir.region(region)?.parent?;
	let (_, body) = select_loop(ir, owner)?;
	(body == region).then_some(owner)
}

/// Whether `op` stands in the body of a select loop that a chain is read through.
pub(super) fn in_select_body(ir: &Ir, op: OpId) -> bool {
	let region = ir.enclosing_region(op);
	ir.region(region)
		.and_then(|held| held.parent)
		.and_then(|owner| select_loop(ir, owner))
		.is_some_and(|(_, body)| body == region)
}

/// The op that stands for the select loop `select` in the block around it: the `phys.pipeline`
/// whose first op is the loop, and the loop itself when no pipeline holds it first.
///
/// The loop's source is defined before this op, so an op that must read the rows of the source
/// before the loop does goes in front of it, in the block it stands in.
pub(super) fn container_of(ir: &Ir, select: OpId) -> OpId {
	let region = ir.enclosing_region(select);
	let Some(held) = ir.region(region) else {
		return select;
	};
	let Some(owner) = held.parent else {
		return select;
	};
	let wraps = held.kind == RegionKind::Pipeline
		&& matches!(ir.op(owner).map(|data| &data.kind), Some(OpKind::Phys(PhysOp::Pipeline(_))))
		&& held
			.blocks
			.first()
			.and_then(|block| ir.block(*block))
			.is_some_and(|block| block.ops.first() == Some(&select));
	if wraps {
		owner
	} else {
		select
	}
}
