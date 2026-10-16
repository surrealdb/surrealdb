//! The existence short-circuit: a statement in value position that only a truthiness test reads
//! stops after its first row.
//!
//! `WHERE (SELECT id FROM big WHERE s = 'a')` asks whether the subquery has a row, and an array is
//! truthy exactly when it is not empty, so one row of the result answers. [`offer`] gives such a
//! statement a `LIMIT 1`, which the access decision reads as any written `LIMIT 1` and the
//! pipeline stops at.
//!
//! The decision is made once, before the conversion of the statement, from the logical dialect:
//!
//! - what the value is read for, and the shape of the statement, are facts of the logical dialect
//!   ([`existence_only`], [`plain_select`]): no `ORDER BY`, `LIMIT`, `START`, `GROUP`, `SPLIT`,
//!   `FETCH`, `ONLY`, `TIMEOUT`, `VERSION` or `WITH`, and a plain read of a table;
//! - the work the loop does for each row never raises and has no effect ([`per_row_total`]), so no
//!   row that is not read could have raised an error the statement owes;
//! - the physical catalog answers what the logical dialect cannot: the table is defined, is read at
//!   the current version, holds no computed field, and the read enforces nothing at run time that
//!   the ops of the statement do not state (run-time enforcement evaluates permission and computed
//!   field code the dialect does not show).
//!
//! A statement under an `EXPLAIN` is left as written, so that the plan it renders is the plan of
//! the statement. The strategy of the limited statement (`Once`, `Hash`, `Lookup` or `Apply`) is
//! decided afterwards, as for any other, and reads past the limit.

use smallvec::smallvec;

use super::access::{ReadVersion, RestrictedSelect, source_key};
use super::count::in_stored_clause;
use super::edit::{Stamp, first_result, limit_over, require_untokened};
use crate::exec::ir::analysis::existence::{
	PlainSelect, existence_only, per_row_total, plain_select,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::OpKind;
use crate::val::{Number, Value};

/// Gives every statement of `ir` that is read only for existence a `LIMIT 1`, and answers how
/// many it limited.
///
/// Called once per container, before [`convert`](crate::exec::ir::core::convert::convert), after
/// the counts are decided. An answer the physical catalog does not hold is
/// [`IrError::NotLoaded`]; one it knows to be unknowable (a table read after a catalog change)
/// leaves the statement as it is.
pub(super) fn offer(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	require_untokened(ir)?;
	let mut candidates = Vec::new();
	ir.walk(ir.top(), WalkOrder::PostOrder, &mut |op| {
		if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Subquery(_))))
			&& !in_stored_clause(ir, op)
			&& !under_explain(ir, op)
		{
			candidates.push(op);
		}
		Walk::Continue
	});
	let mut decided = Vec::new();
	for subquery in candidates {
		if let Some(plain) = decide(ir, subquery, cx)? {
			decided.push((subquery, plain));
		}
	}
	let limited = decided.len() as u32;
	for (subquery, plain) in decided {
		limit_to_one(ir, facts, subquery, plain)?;
	}
	Ok(limited)
}

/// Whether `op` stands in a statement an `EXPLAIN` renders: an `explain` op follows the
/// statement's rows in a region that encloses `op`.
pub(super) fn under_explain(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ops_in(ir, region).any(|held| {
			matches!(
				ir.op(held).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Explain { .. }))
			)
		})
	})
}

/// The plain select the statement of `subquery` is, when it may stop after its first row.
fn decide(ir: &Ir, subquery: OpId, cx: &PhysCx<'_>) -> Result<Option<PlainSelect>, IrError> {
	let Some(data) = ir.op(subquery) else {
		return Ok(None);
	};
	let ([region], [result]) = (data.regions.as_slice(), data.results.as_slice()) else {
		return Ok(None);
	};
	if !existence_only(ir, *result) {
		return Ok(None);
	}
	let Some(plain) = plain_select(ir, *region) else {
		return Ok(None);
	};
	if !per_row_total(ir, plain.select_loop) {
		return Ok(None);
	}
	let Some(OpKind::Logical(LogOp::Source(attrs))) = ir.op(plain.source).map(|data| &data.kind)
	else {
		return Ok(None);
	};
	if attrs.may_meet_change {
		return Ok(None);
	}
	let Some(key) = source_key(ir, plain.source, cx.effects.catalog, cx.physical)? else {
		return Ok(None);
	};
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::Unknown | CatalogAnswer::Absent => return Ok(None),
		CatalogAnswer::NotLoaded => {
			return Err(IrError::NotLoaded {
				what: "existence::offer: the answers of a table an existence read names",
			});
		}
	};
	// A resolved source has what its table declares stated as the permission and layer ops of the
	// statement, which the allow-list has judged. A table whose permission context is unresolved
	// is enforced as it is met when the statement runs, which the ops do not show. A computed
	// field is derived by code the read need not state.
	let enforced_by_ops =
		attrs.resolved && !matches!(answers.restricted, RestrictedSelect::Unresolved);
	let stated = answers.defined
		&& answers.version == ReadVersion::Current
		&& !answers.fields.iter().any(|field| field.computed.is_some())
		&& (!answers.enforces || enforced_by_ops);
	Ok(stated.then_some(plain))
}

/// Limits the statement of `subquery` to its first row: the written form of `LIMIT 1`, a limit
/// over the rows of the loop with a literal `1`, which the region yields in place of the loop's
/// rows.
fn limit_to_one(
	ir: &mut Ir,
	facts: &mut FactCache,
	subquery: OpId,
	plain: PlainSelect,
) -> Result<(), IrError> {
	let region = ir
		.op(subquery)
		.and_then(|data| data.regions.first().copied())
		.ok_or(IrError::Stale("existence: the statement of a subquery"))?;
	let closing = ops_in(ir, region).last().ok_or(IrError::Stale("existence: a closing yield"))?;
	let loop_data = ir.op(plain.select_loop).ok_or(IrError::Stale("existence: a select loop"))?;
	let rows =
		*loop_data.results.first().ok_or(IrError::Stale("existence: the rows of a select loop"))?;
	let stamp = Stamp::of(loop_data);
	let mut rw = Rewriter::new(ir, facts);
	rw.derived(&[plain.select_loop], |rw| {
		let one = ExprOp::Literal(Box::new(Value::Number(Number::Int(1))));
		let mut spec = stamp.spec(OpKind::Expr(one.clone()), &[], 0);
		spec.results = smallvec![result_type(&one, &[])];
		let literal = rw.create(InsertPoint::Before(closing), spec)?;
		let literal = first_result(rw, literal)?;
		let limited = limit_over(rw, &stamp, InsertPoint::Before(closing), rows, literal)?;
		rw.set_operand(closing, 0, limited)
	})
}
