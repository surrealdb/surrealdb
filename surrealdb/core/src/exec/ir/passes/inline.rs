//! The decision to inline a user-defined function body at its call site.
//!
//! A body may run in the caller when it is a function of its arguments alone, wherever it is
//! evaluated, and small enough that the inlined ops are the smaller program. [`inline_admits`]
//! answers that over the body's region; it rewrites nothing, and an inlining pass calls it.
#![deny(clippy::wildcard_enum_match_arm)]

use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::{BlockId, RegionId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::op_kind::OpKind;
use crate::expr::Function;
use crate::expr::logical::EffectSet;

/// The most ops a body inlined may have: past it, a call is the smaller program.
pub(crate) const MAX_NODES: usize = 64;

/// Whether a call of the function whose body region is `body` may run the body inlined in the
/// caller.
///
/// The body is one block of expression ops closed by a yield of one value, the function's value. It
/// has at most [`MAX_NODES`] ops, and its transitive effects are [`EffectSet::PURE`]. It reads no
/// parameter but its arguments, which are block arguments, and no row and no record. Every call in
/// it names a builtin that is pure and synchronous and is neither an index function nor a
/// projection. Any other region, or a body with a different shape, answers `false`. A body that
/// refers to row names or binds statement ops is refused too.
pub(crate) fn inline_admits(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	body: RegionId,
) -> bool {
	let Some(region) = ir.region(body) else {
		return false;
	};
	if region.kind != RegionKind::FunctionBody
		|| !closes_with_one_value(ir, region.blocks.as_slice())
	{
		return false;
	}
	let mut nodes = 0;
	let mut admitted = true;
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		nodes += 1;
		let Some(data) = ir.op(op) else {
			admitted = false;
			return Walk::Stop;
		};
		if nodes > MAX_NODES || !admits(&data.kind, cx) {
			admitted = false;
			return Walk::Stop;
		}
		Walk::Continue
	});
	admitted
		&& ops_in(ir, body).all(|op| effects::of_with(ir, facts, cx, op).set == EffectSet::PURE)
}

/// Whether `blocks` is one block whose last op yields one value.
fn closes_with_one_value(ir: &Ir, blocks: &[BlockId]) -> bool {
	let [block] = blocks else {
		return false;
	};
	let Some(last) = ir.block(*block).and_then(|held| held.ops.last()).and_then(|op| ir.op(*op))
	else {
		return false;
	};
	matches!(last.kind, OpKind::Expr(ExprOp::Yield)) && last.operands.len() == 1
}

/// Whether `kind`, on its own, is a function of its operands alone.
///
/// A parameter, a row read, a record read, a statement, a closure and a lookup are refused; a call
/// is admitted when it names a pure, synchronous builtin.
fn admits(kind: &OpKind, cx: &EffectCx<'_>) -> bool {
	match kind {
		OpKind::Expr(ExprOp::Call(function)) => match &function.function {
			Function::Normal(name) => cx.registry.get(name).is_some_and(|builtin| {
				builtin.is_pure()
					&& !builtin.is_async()
					&& !cx.registry.is_index_function(name)
					&& !cx.registry.is_projection(name)
			}),
			Function::Custom(_)
			| Function::Script(..)
			| Function::Model(..)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		OpKind::Expr(op) => admits_tag(op.tag()),
		OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether the expression op `tag` is a value of its operands alone.
fn admits_tag(tag: ExprTag) -> bool {
	match tag {
		ExprTag::Literal
		| ExprTag::Array
		| ExprTag::Set
		| ExprTag::Object
		| ExprTag::RecordId
		| ExprTag::Table
		| ExprTag::Constant
		| ExprTag::First
		| ExprTag::Last
		| ExprTag::Flatten
		| ExprTag::Index
		| ExprTag::Prefix
		| ExprTag::Postfix
		| ExprTag::Binary
		| ExprTag::ShortCircuit
		| ExprTag::If
		| ExprTag::Truthy
		| ExprTag::Yield => true,
		ExprTag::FreeParam
		| ExprTag::SessionParam
		| ExprTag::CatalogParam
		| ExprTag::Mock
		| ExprTag::Field
		| ExprTag::ComputedRead
		| ExprTag::Doc
		| ExprTag::Access
		| ExprTag::Deref
		| ExprTag::Each
		| ExprTag::Dispatch
		| ExprTag::Method
		| ExprTag::Destructure
		| ExprTag::Optional
		| ExprTag::RepeatRecurse
		| ExprTag::SetField
		| ExprTag::Convert
		| ExprTag::Invoke
		| ExprTag::Call
		| ExprTag::ClosureCreate
		| ExprTag::Throw
		| ExprTag::Subquery
		| ExprTag::Agg
		| ExprTag::Now
		| ExprTag::Raise
		| ExprTag::DepthCharge
		| ExprTag::TupleRow => false,
	}
}
