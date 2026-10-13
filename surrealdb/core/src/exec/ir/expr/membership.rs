//! Whether a statement in value position is read as a membership set.
//!
//! A statement whose only readers test membership in its rows can keep those rows as an index when
//! it runs once ([`NestedStrategy::Once`](super::ops::NestedStrategy::Once)). A statement read any
//! other way cannot. The fact is computed from the uses of the result at the point it is read, so a
//! rewrite that moves or merges a reader cannot leave it stale.

use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::OpKind;
use crate::expr::BinaryOperator;

/// Whether `subquery`, an `expr.subquery`, has a result that is read, and read only as the right
/// operand of an `INSIDE` or a `NOT INSIDE`. `false` for any other op.
pub(crate) fn is_membership_set(ir: &Ir, subquery: OpId) -> bool {
	let Some(data) = ir.op(subquery) else {
		return false;
	};
	if !matches!(data.kind, OpKind::Expr(ExprOp::Subquery(_))) {
		return false;
	}
	let Some(result) = data.results.first().copied() else {
		return false;
	};
	let uses = ir.uses(result);
	!uses.is_empty()
		&& uses.iter().all(|read| {
			read.operand == 1
				&& matches!(
					ir.op(read.user).map(|user| &user.kind),
					Some(OpKind::Expr(ExprOp::Binary {
						op: BinaryOperator::Inside | BinaryOperator::NotInside,
					}))
				)
		})
}
