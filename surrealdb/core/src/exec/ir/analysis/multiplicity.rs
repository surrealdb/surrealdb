//! Stream multiplicity: whether the stream a loop iterates holds an identity at most once.
//!
//! The answer describes the stream the loop's source operand delivers, after every operator that
//! stands between the scan and the loop. It is read by walking from the operand to its defining
//! op and is recomputed on every call.
//!
//! A table scan, a record-id range and a set deliver each identity once, and so does an array
//! whose one member is a mock that names the ids of a range. Any other array, a parameter, a
//! stored subquery result, a call result, a `SPLIT`, a second source, a join, a graph step and an
//! aggregate may repeat one. A `SELECT` written directly as the source delivers its scan's stream
//! when its projection keeps `id` as itself: the identity then holds along the record's `id`;
//! under `SELECT VALUE id` it holds along the element itself. A source nothing is known about
//! answers [`Multiplicity::MayRepeat`], the conservative answer.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;

use smallvec::SmallVec;
use surrealdb_strand::TableName;

use super::types;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::effects::{ElemPath, Multiplicity};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{
	FieldShape, FieldsShape, LogOp, LogTag, LoopKind, ProjectAttrs, SourceRefShape,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::logical::OutputPathPart;
use crate::expr::{BinaryOperator, Function, Kind, Mock};
use crate::val::{Number, Range, Value};

/// The most defining ops followed from a source operand. A longer chain is not a source the
/// analysis can read and answers [`Multiplicity::MayRepeat`].
pub(super) const MAX_DEPTH: usize = 64;

/// The multiplicity of the stream the loop `loop_op` iterates.
///
/// A table, an index, a range and a set deliver each element once along their elements; an array
/// may repeat. A projection that keeps `id` keeps the multiplicity of its source; a `SPLIT`, a
/// self-join and a graph step may repeat. An unknown stream answers [`Multiplicity::MayRepeat`].
pub(crate) fn of(ir: &Ir, loop_op: OpId) -> Multiplicity {
	stream_fact(ir, loop_op).multiplicity
}

/// The path along which the delivered stream's identity holds.
///
/// A stream that may repeat answers [`ElemPath::Elem`].
pub(crate) fn key_path(ir: &Ir, loop_op: OpId) -> ElemPath {
	stream_fact(ir, loop_op).key_path
}

/// The multiplicity and the identity path of the stream the loop iterates, from one walk of the
/// source.
pub(crate) fn of_stream(ir: &Ir, loop_op: OpId) -> (Multiplicity, ElemPath) {
	let fact = stream_fact(ir, loop_op);
	(fact.multiplicity, fact.key_path)
}

/// What one walk of a loop's source learns about its delivered stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamFact {
	pub(crate) multiplicity: Multiplicity,
	pub(crate) key_path: ElemPath,
	/// The one table every record of the stream belongs to, when the walk reaches a scan of a
	/// single table. `None` for a stream that spans several tables or whose source is a value.
	pub(crate) table: Option<TableName>,
}

impl StreamFact {
	fn repeating() -> Self {
		Self {
			multiplicity: Multiplicity::MayRepeat,
			key_path: ElemPath::Elem,
			table: None,
		}
	}

	fn distinct(key_path: ElemPath) -> Self {
		Self {
			multiplicity: Multiplicity::Distinct,
			key_path,
			table: None,
		}
	}

	fn scan(table: Option<TableName>) -> Self {
		Self {
			multiplicity: Multiplicity::Distinct,
			key_path: ElemPath::Id,
			table,
		}
	}
}

/// The fact about the stream `loop_op` iterates. A dangling id and an op that is not a logical
/// loop answer a stream that may repeat.
pub(crate) fn stream_fact(ir: &Ir, loop_op: OpId) -> StreamFact {
	loop_stream(ir, loop_op, 0)
}

/// The table of a stream that is distinct along `id`: the table whose records the loop iterates.
/// `None` when the stream may repeat, is keyed along its element or has no single table.
pub(crate) fn stream_table(ir: &Ir, loop_op: OpId) -> Option<TableName> {
	let fact = stream_fact(ir, loop_op);
	if fact.multiplicity == Multiplicity::Distinct && fact.key_path == ElemPath::Id {
		fact.table
	} else {
		None
	}
}

/// The one table whose records `source` delivers as the source of a loop: the table of the rows a
/// `SELECT` over `source` reads before it projects them.
pub(super) fn source_table(ir: &Ir, source: ValueId, depth: usize) -> Option<TableName> {
	value_stream(ir, source, depth).table
}

/// The kind of the loop `loop_op`, or `None` when it is not a live logical loop.
pub(crate) fn loop_kind(ir: &Ir, loop_op: OpId) -> Option<LoopKind> {
	if let OpKind::Logical(LogOp::Loop {
		kind,
		..
	}) = &ir.op(loop_op)?.kind
	{
		Some(*kind)
	} else {
		None
	}
}

/// Whether `kind` is the per-element body of a loop.
pub(crate) fn is_loop_body(kind: RegionKind) -> bool {
	kind == RegionKind::Loop || kind == RegionKind::StatementFor || kind == RegionKind::ValueFor
}

/// The per-element body region of the loop `loop_op`.
pub(crate) fn loop_body(ir: &Ir, loop_op: OpId) -> Option<RegionId> {
	let data = ir.op(loop_op)?;
	data.regions
		.iter()
		.copied()
		.find(|region| ir.region(*region).is_some_and(|info| is_loop_body(info.kind)))
}

/// The element of the loop `loop_op`: the first argument of its body block.
pub(crate) fn loop_element(ir: &Ir, loop_op: OpId) -> Option<ValueId> {
	let body = loop_body(ir, loop_op)?;
	let block = ir.block(*ir.region(body)?.blocks.first()?)?;
	block.args.first().copied()
}

/// The loop whose body block holds the argument `value`, with the argument's position: `0` is the
/// element and `1` the position. `None` for any other value.
pub(crate) fn loop_arg_owner(ir: &Ir, value: ValueId) -> Option<(OpId, u16)> {
	let ValueDef::BlockArg {
		block,
		index,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let region = ir.block(block)?.parent;
	let info = ir.region(region)?;
	if !is_loop_body(info.kind) || info.blocks.first() != Some(&block) {
		return None;
	}
	let owner = ir.parent_op(region)?;
	if ir.op(owner)?.kind.tag() == OpTag::Logical(LogTag::Loop) {
		Some((owner, index))
	} else {
		None
	}
}

/// The value a region delivers: the operand of the terminator that ends its block, when that
/// terminator yields or returns one.
pub(crate) fn region_result(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let block = ir.block(*ir.region(region)?.blocks.last()?)?;
	let last = ir.op(*block.ops.last()?)?;
	let tag = last.kind.tag();
	if tag == OpTag::Expr(ExprTag::Yield)
		|| tag == OpTag::Logical(LogTag::Yield)
		|| tag == OpTag::Logical(LogTag::Return)
	{
		last.operands.first().copied()
	} else {
		None
	}
}

/// The base of a read of the field `id`: the first operand of an `expr.field` or `expr.access`
/// named `id` that defines `value`.
pub(crate) fn id_read_base(ir: &Ir, value: ValueId) -> Option<ValueId> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	let named_id = match &data.kind {
		OpKind::Expr(ExprOp::Field {
			name,
		}) => *name == "id",
		OpKind::Expr(ExprOp::Access {
			field,
		}) => *field == "id",
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	};
	if named_id {
		data.operands.first().copied()
	} else {
		None
	}
}

/// The stream the loop delivers: its source operand's stream, which for a `SELECT` is the stream
/// of its output rows.
fn loop_stream(ir: &Ir, loop_op: OpId, depth: usize) -> StreamFact {
	let Some(data) = ir.op(loop_op) else {
		return StreamFact::repeating();
	};
	let OpKind::Logical(LogOp::Loop {
		kind,
		..
	}) = &data.kind
	else {
		return StreamFact::repeating();
	};
	let Some(source) = data.operands.first().copied() else {
		return StreamFact::repeating();
	};
	match kind {
		LoopKind::GraphStep | LoopKind::Recurse => StreamFact::repeating(),
		LoopKind::Select => {
			let input = value_stream(ir, source, depth + 1);
			select_output(ir, loop_op, input)
		}
		LoopKind::Write
		| LoopKind::For
		| LoopKind::Closure
		| LoopKind::ElementFilter
		| LoopKind::Broadcast => value_stream(ir, source, depth + 1),
	}
}

/// The stream `value` delivers when it is a loop's source.
///
/// A value is followed to the op that defines it and no further through a `LET`: a stored result
/// is an array.
pub(super) fn value_stream(ir: &Ir, value: ValueId, depth: usize) -> StreamFact {
	if depth > MAX_DEPTH {
		return StreamFact::repeating();
	}
	let Some(data) = ir.value(value) else {
		return StreamFact::repeating();
	};
	if let TypeKnowledge::Fact {
		kind: Kind::Set(..),
		..
	} = &data.ty
	{
		return StreamFact::distinct(ElemPath::Elem);
	}
	match data.def {
		ValueDef::BlockArg {
			..
		} => StreamFact::repeating(),
		ValueDef::OpResult {
			op,
			index,
		} => match ir.op(op) {
			Some(producer) if index == 0 => op_stream(ir, op, producer, depth),
			Some(_) | None => StreamFact::repeating(),
		},
	}
}

/// The stream the result of `op` delivers.
fn op_stream(ir: &Ir, op: OpId, data: &OpData, depth: usize) -> StreamFact {
	match &data.kind {
		OpKind::Logical(log) => logical_stream(ir, op, data, log, depth),
		OpKind::Expr(expr) => expr_stream(ir, data, expr, depth),
		OpKind::Phys(_) => StreamFact::repeating(),
	}
}

fn logical_stream(ir: &Ir, op: OpId, data: &OpData, log: &LogOp, depth: usize) -> StreamFact {
	let input = || match data.operands.first() {
		Some(first) => value_stream(ir, *first, depth + 1),
		None => StreamFact::repeating(),
	};
	match log {
		LogOp::Source(attrs) => match &attrs.source_ref {
			SourceRefShape::Table(table) => StreamFact::scan(Some(table.clone())),
			SourceRefShape::RecordId => {
				StreamFact::scan(data.operands.first().and_then(|id| record_table(ir, *id)))
			}
			SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => {
				StreamFact::repeating()
			}
		},
		LogOp::Values {
			written,
			..
		} => {
			if *written {
				StreamFact::repeating()
			} else {
				input()
			}
		}
		LogOp::Subquery => subquery_stream(ir, data, depth),
		LogOp::Loop {
			kind,
			..
		} => match kind {
			LoopKind::Select => loop_stream(ir, op, depth + 1),
			LoopKind::Write
			| LoopKind::For
			| LoopKind::Closure
			| LoopKind::ElementFilter
			| LoopKind::GraphStep
			| LoopKind::Recurse
			| LoopKind::Broadcast => StreamFact::repeating(),
		},
		// Operators that drop, reorder or hide rows never add a repeat.
		LogOp::Filter
		| LogOp::PermissionFilter {
			..
		}
		| LogOp::Limit {
			..
		}
		| LogOp::Sort(_)
		| LogOp::Window(_) => input(),
		LogOp::Distinct => {
			let fact = input();
			if fact.key_path == ElemPath::Elem {
				StreamFact {
					multiplicity: Multiplicity::Distinct,
					..fact
				}
			} else {
				fact
			}
		}
		LogOp::Request {
			..
		}
		| LogOp::Txn {
			..
		}
		| LogOp::Stmt {
			..
		}
		| LogOp::Statement {
			..
		}
		| LogOp::Match {
			..
		}
		| LogOp::MatchRead {
			..
		}
		| LogOp::MatchUpdate(_)
		| LogOp::MatchDelete {
			..
		}
		| LogOp::MatchInsert(_)
		| LogOp::MatchOutput(_)
		| LogOp::BindStep(_)
		| LogOp::Write(_)
		| LogOp::Assign(_)
		| LogOp::Unset {
			..
		}
		| LogOp::Content {
			..
		}
		| LogOp::DdlGuard {
			..
		}
		| LogOp::Define(_)
		| LogOp::Remove(_)
		| LogOp::Alter(_)
		| LogOp::Rebuild(_)
		| LogOp::UnionAll
		| LogOp::Join {
			..
		}
		| LogOp::Split {
			..
		}
		| LogOp::Aggregate(_)
		| LogOp::Project(_)
		| LogOp::Fetch
		| LogOp::Timeout
		| LogOp::VersionScope
		| LogOp::Only
		| LogOp::Explain {
			..
		}
		| LogOp::Eval
		| LogOp::Block
		| LogOp::Let(_)
		| LogOp::Return
		| LogOp::If
		| LogOp::Info {
			..
		}
		| LogOp::Sleep
		| LogOp::Use {
			..
		}
		| LogOp::Option {
			..
		}
		| LogOp::Access(_)
		| LogOp::Kill
		| LogOp::Live(_)
		| LogOp::Subscriber(_)
		| LogOp::Show(_)
		| LogOp::Choose(_)
		| LogOp::Events(_)
		| LogOp::WriteStage {
			..
		}
		| LogOp::Yield
		| LogOp::Next
		| LogOp::Break => StreamFact::repeating(),
	}
}

/// Whether `value` is a mock that names the ids of a range, each once.
fn is_id_range_mock(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::OpResult {
		op,
		index: 0,
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	matches!(
		ir.op(op).map(|data| &data.kind),
		Some(OpKind::Expr(ExprOp::Mock(mock))) if matches!(**mock, Mock::Range(..))
	)
}

fn expr_stream(ir: &Ir, data: &OpData, expr: &ExprOp, depth: usize) -> StreamFact {
	match expr {
		ExprOp::Set => StreamFact::distinct(ElemPath::Elem),
		ExprOp::Literal(value) => literal_stream(value),
		ExprOp::Call(function) => {
			if let Function::Normal(name) = &function.function
				&& name == "array::distinct"
			{
				StreamFact::distinct(ElemPath::Elem)
			} else {
				StreamFact::repeating()
			}
		}
		ExprOp::Method {
			name,
		} => {
			if *name == "distinct" {
				StreamFact::distinct(ElemPath::Elem)
			} else {
				StreamFact::repeating()
			}
		}
		ExprOp::Convert(attrs) => {
			if let Kind::Set(..) = &attrs.kind {
				StreamFact::distinct(ElemPath::Elem)
			} else {
				StreamFact::repeating()
			}
		}
		ExprOp::Binary {
			op,
		} => {
			let is_range = *op == BinaryOperator::Range || *op == BinaryOperator::RangeInclusive;
			let integers = data.operands.len() == 2
				&& data.operands.iter().all(|operand| types::is_int_at(ir, *operand, depth + 1));
			if is_range && integers {
				StreamFact::distinct(ElemPath::Elem)
			} else {
				StreamFact::repeating()
			}
		}
		ExprOp::Subquery(_) => subquery_stream(ir, data, depth),
		ExprOp::Array => {
			if data.operands.len() == 1 && is_id_range_mock(ir, data.operands[0]) {
				StreamFact::distinct(ElemPath::Elem)
			} else {
				StreamFact::repeating()
			}
		}
		ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::SetField {
			..
		}
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Prefix(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::ClosureCreate(_)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Yield
		| ExprOp::Truthy
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => StreamFact::repeating(),
	}
}

/// The stream of a statement written in place: the stream of the value its region delivers.
fn subquery_stream(ir: &Ir, data: &OpData, depth: usize) -> StreamFact {
	for region in data.regions.iter().copied() {
		let Some(info) = ir.region(region) else {
			continue;
		};
		if info.kind == RegionKind::Subquery {
			return match region_result(ir, region) {
				Some(result) => value_stream(ir, result, depth + 1),
				None => StreamFact::repeating(),
			};
		}
	}
	StreamFact::repeating()
}

/// The stream a literal value delivers: a set and a range of integers hold each element once;
/// every other literal is an array or a scalar.
fn literal_stream(value: &Value) -> StreamFact {
	if let Value::Set(_) = value {
		StreamFact::distinct(ElemPath::Elem)
	} else if let Value::Range(range) = value
		&& integer_range(range)
	{
		StreamFact::distinct(ElemPath::Elem)
	} else {
		StreamFact::repeating()
	}
}

/// Whether both ends of `range` are integers.
fn integer_range(range: &Range) -> bool {
	let integer = |bound: &Bound<Value>| match bound {
		Bound::Included(Value::Number(Number::Int(_)))
		| Bound::Excluded(Value::Number(Number::Int(_))) => true,
		Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded => false,
	};
	integer(&range.start) && integer(&range.end)
}

/// The table a record id operand names: the table of an `expr.record_id` or of a record-id
/// literal.
fn record_table(ir: &Ir, value: ValueId) -> Option<TableName> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(ExprOp::RecordId(attrs)) => Some(attrs.table.clone()),
		OpKind::Expr(ExprOp::Literal(literal)) => {
			if let Value::RecordId(id) = &**literal {
				Some(id.table.clone())
			} else {
				None
			}
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// What a `SELECT` body does to the identity of the rows it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
	/// Rows that keep `id` as itself: the identity holds along `id`.
	Rows,
	/// The ids themselves, from `SELECT VALUE id`: the identity holds along the element.
	IdValues,
	/// Elements of a value stream, passed through untouched.
	Elements,
	/// An output that no longer carries the identity.
	Lost,
}

/// The stream a `SELECT` delivers, given the stream of its source.
///
/// The body decides the answer: a `SPLIT` repeats every row, an aggregate collapses them, and a
/// projection that drops, overwrites or renames `id` leaves an output that carries no identity.
fn select_output(ir: &Ir, loop_op: OpId, input: StreamFact) -> StreamFact {
	if input.multiplicity == Multiplicity::MayRepeat {
		return StreamFact::repeating();
	}
	let Some(body) = loop_body(ir, loop_op) else {
		return StreamFact::repeating();
	};
	let mut shape = match input.key_path {
		ElemPath::Id => Shape::Rows,
		ElemPath::Elem => Shape::Elements,
	};
	for child in ops_in(ir, body) {
		let Some(data) = ir.op(child) else {
			return StreamFact::repeating();
		};
		match &data.kind {
			OpKind::Logical(LogOp::Split {
				..
			})
			| OpKind::Logical(LogOp::Aggregate(_)) => {
				return StreamFact::repeating();
			}
			OpKind::Logical(LogOp::Project(attrs)) => {
				shape = project_shape(ir, loop_op, child, attrs, shape);
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {}
		}
	}
	match shape {
		Shape::Rows => StreamFact {
			multiplicity: Multiplicity::Distinct,
			key_path: ElemPath::Id,
			table: input.table,
		},
		Shape::IdValues => StreamFact {
			multiplicity: Multiplicity::Distinct,
			key_path: ElemPath::Elem,
			table: input.table,
		},
		Shape::Elements => StreamFact::distinct(ElemPath::Elem),
		Shape::Lost => StreamFact::repeating(),
	}
}

/// The shape of the rows after the projection `project`, given their shape before it.
fn project_shape(
	ir: &Ir,
	loop_op: OpId,
	project: OpId,
	attrs: &ProjectAttrs,
	before: Shape,
) -> Shape {
	let rows = projected_rows(ir, loop_op, project);
	match before {
		Shape::Lost => Shape::Lost,
		Shape::Rows => match &attrs.fields {
			FieldsShape::Value {
				..
			} => {
				if projection_regions(ir, project).first().is_some_and(|r| yields_id(ir, *r, &rows))
				{
					Shape::IdValues
				} else {
					Shape::Lost
				}
			}
			FieldsShape::Select(fields) => {
				if keeps_id(ir, project, attrs, fields, &rows) {
					Shape::Rows
				} else {
					Shape::Lost
				}
			}
		},
		Shape::IdValues | Shape::Elements => match &attrs.fields {
			FieldsShape::Select(fields)
				if attrs.omit == 0 && fields.len() == 1 && fields[0] == FieldShape::All =>
			{
				before
			}
			FieldsShape::Select(_)
			| FieldsShape::Value {
				..
			} => Shape::Lost,
		},
	}
}

/// The `Clause(Projection)` regions of a projection op, one per field that is not `*`, in order.
fn projection_regions(ir: &Ir, project: OpId) -> SmallVec<[RegionId; 4]> {
	let mut out = SmallVec::new();
	if let Some(data) = ir.op(project) {
		for region in data.regions.iter().copied() {
			let projecting = ir
				.region(region)
				.is_some_and(|info| info.kind == RegionKind::Clause(ClauseRole::Projection));
			if projecting {
				out.push(region);
			}
		}
	}
	out
}

/// The values that stand for the row a projection reads: the projection's input, the element of
/// the `SELECT` loop that holds it, and the row argument of each of its projection regions, which
/// the projection binds to its input row.
fn projected_rows(ir: &Ir, loop_op: OpId, project: OpId) -> SmallVec<[ValueId; 2]> {
	let mut rows = SmallVec::new();
	if let Some(input) = ir.op(project).and_then(|data| data.operands.first().copied()) {
		rows.push(input);
	}
	if let Some(element) = loop_element(ir, loop_op) {
		rows.push(element);
	}
	for region in projection_regions(ir, project) {
		let row = ir
			.region(region)
			.and_then(|info| info.blocks.first())
			.and_then(|block| ir.block(*block))
			.and_then(|block| block.args.first().copied());
		rows.extend(row);
	}
	rows
}

/// Whether the region delivers the row's own `id`: an `id` read whose base is one of `rows`. An
/// `id` read off any other base is the id of some other record.
fn yields_id(ir: &Ir, region: RegionId, rows: &[ValueId]) -> bool {
	region_result(ir, region)
		.and_then(|value| id_read_base(ir, value))
		.is_some_and(|base| rows.contains(&base))
}

/// Whether a projection with `fields` leaves the row's `id` in place: it lists `*` or the row's
/// own `id`, omits nothing, and no field lands at `id` with any other expression. An unaliased
/// field lands at `id` only when it is the row's own `id` read.
fn keeps_id(
	ir: &Ir,
	project: OpId,
	attrs: &ProjectAttrs,
	fields: &[FieldShape],
	rows: &[ValueId],
) -> bool {
	if attrs.omit > 0 {
		return false;
	}
	let regions = projection_regions(ir, project);
	let mut listed = false;
	let mut next = 0usize;
	for field in fields {
		match field {
			FieldShape::All => listed = true,
			FieldShape::Single {
				alias,
				..
			} => {
				let identity = regions.get(next).is_some_and(|r| yields_id(ir, *r, rows));
				next += 1;
				match alias {
					None => listed |= identity,
					Some(path) => {
						let at_id = if let Some(OutputPathPart::Field(name)) = path.parts().first()
						{
							name == "id"
						} else {
							false
						};
						if at_id && !identity {
							return false;
						}
						listed |= at_id;
					}
				}
			}
		}
	}
	listed
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;

	fn int(value: i64) -> Value {
		Value::Number(Number::Int(value))
	}

	#[test]
	fn literal_streams() {
		let range = |start: Bound<Value>, end: Bound<Value>| {
			Value::Range(Box::new(Range {
				start,
				end,
			}))
		};
		let bounded = range(Bound::Included(int(1)), Bound::Excluded(int(3)));
		assert_eq!(literal_stream(&bounded), StreamFact::distinct(ElemPath::Elem));
		let open = range(Bound::Included(int(1)), Bound::Unbounded);
		assert_eq!(literal_stream(&open), StreamFact::repeating(), "an open range is no source");
		let floats = range(
			Bound::Included(Value::Number(Number::Float(1.0))),
			Bound::Included(Value::Number(Number::Float(2.0))),
		);
		assert_eq!(literal_stream(&floats), StreamFact::repeating());
		assert_eq!(literal_stream(&int(3)), StreamFact::repeating());
		assert_eq!(
			literal_stream(&Value::Set(Default::default())),
			StreamFact::distinct(ElemPath::Elem)
		);
		assert_eq!(literal_stream(&Value::Array(Default::default())), StreamFact::repeating());
	}

	#[test]
	fn a_missing_loop_answers_a_stream_that_may_repeat() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let missing = OpId::default();
		assert_eq!(of_stream(&ir, missing), (Multiplicity::MayRepeat, ElemPath::Elem));
		assert_eq!(of(&ir, missing), Multiplicity::MayRepeat);
		assert_eq!(key_path(&ir, missing), ElemPath::Elem);
		assert_eq!(stream_table(&ir, missing), None);
	}
}
