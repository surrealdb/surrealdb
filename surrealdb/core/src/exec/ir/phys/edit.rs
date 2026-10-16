//! The logical ops an edit before the conversion makes.
//!
//! [`existence`](super::existence) and [`extreme`](super::extreme) add ops to a container that is
//! still in the logical dialect: a limit over rows, a sort, a filter, a projection, a
//! conditional. Each constructor here makes one such op through the layout functions the binder
//! builds the written statement's op with ([`limit_layout`], [`sort_attrs`], [`project_attrs`],
//! the clause regions and [`row_region_args`]), so an op an edit makes has the operand order,
//! attributes, clause roles and region arguments of the op the binder makes for the same
//! statement written out. An op is made at an [`InsertPoint`] with the frame, snapshot and
//! location of the op the edit stands for ([`Stamp`]).
//!
//! The ops are made in a container that holds no effect token: the physical planning that runs
//! the edits follows [`tokens::strip`](crate::exec::ir::analysis::tokens::strip). None of them
//! orders itself against a class of world state, so none takes or produces a token.

use smallvec::smallvec;
use surrealdb_strand::Strand;

use crate::exec::ir::build::logical::{
	ORDER_KEY, PROJECTION_CLAUSE, WHERE_CLAUSE, limit_layout, project_attrs, row_region_args,
	sort_attrs,
};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::{BlockId, OpId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::{LogicalExpr, LogicalFields, LogicalOrder, LogicalOrdering};
use crate::val::Value;

/// Refuses a container that holds effect tokens. The ops an edit makes take none, and a copy of a
/// region that reads storage would be left without the tokens it orders itself by, so an edit
/// runs only after the tokens are stripped.
pub(super) fn require_untokened(ir: &Ir) -> Result<(), IrError> {
	if ir.is_threaded() {
		return Err(IrError::Invariant {
			what: "an edit before the conversion runs in a container that holds effect tokens",
		});
	}
	Ok(())
}

/// The frame, snapshot and location every op of an edit carries: those of the op it stands for.
pub(super) struct Stamp {
	proto: OpData,
}

impl Stamp {
	/// The stamp of the op `data`.
	pub(super) fn of(data: &OpData) -> Self {
		Self {
			proto: data.clone(),
		}
	}

	/// The specification of an op of `kind` over `operands` with `results` results of unknown type.
	pub(super) fn spec(&self, kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: std::iter::repeat_n(TypeKnowledge::Unknown, results).collect(),
			frame: self.proto.frame,
			snapshot: self.proto.snapshot,
			loc: self.proto.loc.clone(),
		}
	}

	/// An expression op of `kind` over `operands` at `at`, answering its one value.
	pub(super) fn value(
		&self,
		rw: &mut Rewriter<'_>,
		at: InsertPoint,
		kind: ExprOp,
		operands: &[ValueId],
	) -> Result<ValueId, IrError> {
		let op = rw.create(at, self.spec(OpKind::Expr(kind), operands, 1))?;
		first_result(rw, op)
	}

	/// A literal.
	pub(super) fn literal(
		&self,
		rw: &mut Rewriter<'_>,
		at: InsertPoint,
		value: Value,
	) -> Result<ValueId, IrError> {
		self.value(rw, at, ExprOp::Literal(Box::new(value)), &[])
	}

	/// The closing yield of the expression region whose block is `block`.
	pub(super) fn close(
		&self,
		rw: &mut Rewriter<'_>,
		block: BlockId,
		value: ValueId,
	) -> Result<(), IrError> {
		rw.create(InsertPoint::End(block), self.spec(OpKind::Expr(ExprOp::Yield), &[value], 0))?;
		Ok(())
	}

	/// The closing yield of the statement region whose block is `block`.
	pub(super) fn close_statement(
		&self,
		rw: &mut Rewriter<'_>,
		block: BlockId,
		value: ValueId,
	) -> Result<(), IrError> {
		rw.create(InsertPoint::End(block), self.spec(OpKind::Logical(LogOp::Yield), &[value], 0))?;
		Ok(())
	}
}

/// The first result of `op`.
pub(super) fn first_result(rw: &Rewriter<'_>, op: OpId) -> Result<ValueId, IrError> {
	rw.ir()
		.op(op)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("edit: a created op without a result"))
}

/// The first `LIMIT` rows of `rows`, the limit a value `count`, as the written `LIMIT` binds it.
pub(super) fn limit_over(
	rw: &mut Rewriter<'_>,
	stamp: &Stamp,
	at: InsertPoint,
	rows: ValueId,
	count: ValueId,
) -> Result<ValueId, IrError> {
	let (kind, operands) = limit_layout(rows, Some(count), None);
	let limit = rw.create(at, stamp.spec(OpKind::Logical(kind), &operands, 1))?;
	first_result(rw, limit)
}

/// One key of a sort: the field of the row it reads, and its direction.
#[derive(Clone, Debug)]
pub(super) struct SortBy {
	pub(super) field: Strand,
	pub(super) ascending: bool,
}

/// `rows` in the order of `keys`, as the written `ORDER BY` of those fields binds: each key is a
/// path read of one field, with no collation and no numeric ordering.
pub(super) fn sort_over(
	rw: &mut Rewriter<'_>,
	stamp: &Stamp,
	at: InsertPoint,
	rows: ValueId,
	keys: &[SortBy],
) -> Result<ValueId, IrError> {
	let ordering = LogicalOrdering::Order(
		keys.iter()
			.map(|key| LogicalOrder {
				value: LogicalExpr::Field(key.field.clone()),
				collate: false,
				numeric: false,
				direction: key.ascending,
			})
			.collect(),
	);
	let attrs = sort_attrs(&ordering, |_| false, false);
	let sort =
		rw.create(at, stamp.spec(OpKind::Logical(LogOp::Sort(Box::new(attrs))), &[rows], 1))?;
	for key in keys {
		let (_, block) = rw.create_region(sort, ORDER_KEY, &row_region_args(ORDER_KEY))?;
		let argument = *rw
			.ir()
			.block(block)
			.and_then(|held| held.args.first())
			.ok_or(IrError::Stale("edit: the row of a sort key"))?;
		let value = stamp.value(
			rw,
			InsertPoint::End(block),
			ExprOp::Field {
				name: key.field.clone(),
			},
			&[argument],
		)?;
		stamp.close(rw, block, value)?;
	}
	first_result(rw, sort)
}

/// A `SELECT VALUE field` of `rows` at `at`: the value of `field` of each row, which later ops read
/// in place of the rows.
pub(super) fn project_field_over(
	rw: &mut Rewriter<'_>,
	stamp: &Stamp,
	at: InsertPoint,
	rows: ValueId,
	field: &Strand,
) -> Result<ValueId, IrError> {
	let fields = LogicalFields::Value {
		expr: LogicalExpr::Field(field.clone()),
		alias: None,
	};
	let attrs = project_attrs(&fields, 0, false, Vec::new(), Vec::new());
	let project =
		rw.create(at, stamp.spec(OpKind::Logical(LogOp::Project(Box::new(attrs))), &[rows], 1))?;
	let (_, clause) =
		rw.create_region(project, PROJECTION_CLAUSE, &row_region_args(PROJECTION_CLAUSE))?;
	let row = *rw
		.ir()
		.block(clause)
		.and_then(|held| held.args.first())
		.ok_or(IrError::Stale("edit: the row of a projection"))?;
	let value = stamp.value(
		rw,
		InsertPoint::End(clause),
		ExprOp::Field {
			name: field.clone(),
		},
		&[row],
	)?;
	stamp.close(rw, clause, value)?;
	first_result(rw, project)
}

/// A filter of the row `row` placed before the op `before`, whose predicate `predicate` makes from
/// the row at the end of the filter's clause. Answers the rows the filter keeps.
pub(super) fn filter_over(
	rw: &mut Rewriter<'_>,
	stamp: &Stamp,
	before: OpId,
	row: ValueId,
	predicate: impl FnOnce(&mut Rewriter<'_>, InsertPoint, ValueId) -> Result<ValueId, IrError>,
) -> Result<ValueId, IrError> {
	let filter = rw.create(
		InsertPoint::Before(before),
		stamp.spec(OpKind::Logical(LogOp::Filter), &[row], 1),
	)?;
	let (_, clause) = rw.create_region(filter, WHERE_CLAUSE, &row_region_args(WHERE_CLAUSE))?;
	let argument = *rw
		.ir()
		.block(clause)
		.and_then(|held| held.args.first())
		.ok_or(IrError::Stale("edit: the row of a filter"))?;
	let value = predicate(rw, InsertPoint::End(clause), argument)?;
	stamp.close(rw, clause, value)?;
	first_result(rw, filter)
}

/// A conditional on `condition` at `at`, answering one value of type `ty`: a statement `IF` when
/// `statement` holds, a value `IF` otherwise. Answers the op and the kind of its two arms, which
/// the caller creates and closes.
pub(super) fn if_over(
	rw: &mut Rewriter<'_>,
	stamp: &Stamp,
	at: InsertPoint,
	condition: ValueId,
	ty: &TypeKnowledge,
	statement: bool,
) -> Result<(OpId, RegionKind), IrError> {
	let (kind, arms) = if statement {
		(OpKind::Logical(LogOp::If), RegionKind::StatementIf)
	} else {
		(OpKind::Expr(ExprOp::If), RegionKind::ValueIf)
	};
	let mut spec = stamp.spec(kind, &[condition], 0);
	spec.results = smallvec![ty.clone()];
	let branch = rw.create(at, spec)?;
	Ok((branch, arms))
}
