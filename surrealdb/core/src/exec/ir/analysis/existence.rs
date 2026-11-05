//! Whether a statement in value position is read only for whether it has a row.
//!
//! A statement whose value only a truthiness test reads, as a `WHERE` does, needs one row of its
//! result to answer: an array is truthy exactly when it is not empty. [`existence_only`] states
//! that fact of a value. [`per_row_total`] states the other half of the rewrite that stops such a
//! statement after its first row: that no row it would not read could have raised, so reading
//! fewer rows changes no answer and no error. [`plain_select`] is the statement shape the rewrite
//! applies to. [`extreme_shape`] is the statement shape of a `math::max` or `math::min` over one
//! stored field of a whole table, which a read of that field's index in order can answer.
//!
//! Every function is a logical fact: it reads the logical ops of the container and no catalog,
//! and is recomputed on each call in a time linear in what it reads. A construct it does not name
//! answers `false`, so a consumer built on it is conservative and never wrong.
//!
//! # Contract
//!
//! - **Truthiness readers.** A value is read only for existence when it has a use and every use is
//!   one of: the operand of `expr.truthy`; the operand of `NOT`; the condition of an `IF`; the
//!   closing yield of the predicate of a `logical.filter` or the permission predicate of a
//!   `logical.permission_filter`, which keep a row by its truthiness; the left operand of `AND` or
//!   `OR`, or the yield of its right operand, where the result of the operator is itself read only
//!   for existence. `AND` and `OR` answer an operand and not a boolean, so only the truthiness of
//!   their result is the truthiness of their operands. A membership test, a count, a comparison, a
//!   projection, a `RETURN`, a `LET` and a function argument read the value.
//! - **Per-row work is total.** The ops of a select loop's body are an allow-list of those that
//!   answer for every row without raising and without an effect: literals, reads of stored fields
//!   of the loop's own row, comparisons, `AND`, `OR`, `NOT`, the parameters of the session, one
//!   field of the row of an enclosing select loop over a table, and the filters and plain
//!   projections that carry them. Any other op, among them a call, a cast, a nested statement, a
//!   stored parameter (whose permission may raise), a computed field and a layer, is not total. The
//!   loop's own source is not judged: not reading rows past the first raises nothing the language
//!   raises.

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::{Use, ValueDef};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{
	FieldShape, FieldsShape, LogOp, LoopKind, LoopSink, ProjectAttrs, SourceRefShape,
};
use crate::exec::ir::op_kind::{OpKind, Terminator};
use crate::expr::{BinaryOperator, PrefixOperator};

/// How deep `AND` and `OR` nest before the answer is `false`.
const MAX_DEPTH: usize = 64;

/// Whether every reader of `value` reads only whether it is truthy, and it has one.
pub(crate) fn existence_only(ir: &Ir, value: ValueId) -> bool {
	existence_only_at(ir, value, 0)
}

fn existence_only_at(ir: &Ir, value: ValueId, depth: usize) -> bool {
	if depth > MAX_DEPTH {
		return false;
	}
	let uses = ir.uses(value);
	!uses.is_empty() && uses.iter().all(|read| reads_truthiness(ir, *read, depth))
}

/// Whether the use `read` of a value reads only its truthiness.
fn reads_truthiness(ir: &Ir, read: Use, depth: usize) -> bool {
	let Some(user) = ir.op(read.user) else {
		return false;
	};
	match &user.kind {
		OpKind::Expr(ExprOp::Truthy) => true,
		OpKind::Expr(ExprOp::Prefix(op)) => matches!(**op, PrefixOperator::Not),
		OpKind::Expr(ExprOp::If) | OpKind::Logical(LogOp::If) => read.operand == 0,
		OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And | ShortCircuitOp::Or,
		}) => {
			read.operand == 0
				&& user
					.results
					.first()
					.is_some_and(|result| existence_only_at(ir, *result, depth + 1))
		}
		OpKind::Expr(ExprOp::Yield) => yields_truthiness(ir, read.user, depth),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether the yield `op` ends a region whose value is read only for its truthiness.
fn yields_truthiness(ir: &Ir, op: OpId, depth: usize) -> bool {
	let region = ir.enclosing_region(op);
	let (Some(held), Some(owner)) = (ir.region(region), ir.parent_op(region)) else {
		return false;
	};
	let Some(owner_data) = ir.op(owner) else {
		return false;
	};
	match (held.kind, &owner_data.kind) {
		(RegionKind::Clause(ClauseRole::Where), OpKind::Logical(LogOp::Filter))
		| (
			RegionKind::PermissionFrame,
			OpKind::Logical(LogOp::PermissionFilter {
				..
			}),
		) => true,
		(
			RegionKind::LazyOperand,
			OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::And | ShortCircuitOp::Or,
			}),
		) => owner_data
			.results
			.first()
			.is_some_and(|result| existence_only_at(ir, *result, depth + 1)),
		_ => false,
	}
}

/// The select statement an existence read may stop after its first row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlainSelect {
	/// The select loop the statement yields.
	pub(crate) select_loop: OpId,
	/// The source the loop reads.
	pub(crate) source: OpId,
}

/// The plain select `region` holds, when it is exactly a table source, a select loop over it and
/// the yield of the loop's rows.
///
/// The source reads a table, locks nothing and states no `WITH` hint. The loop is a select loop
/// with no step that outputs its rows. A `LIMIT`, `START`, `ORDER BY`, `GROUP`, `SPLIT`, `FETCH`,
/// `ONLY`, `TIMEOUT`, `VERSION` or `EXPLAIN` stands as an op between the loop and the yield, or
/// around the statement, so it is not a plain select.
pub(crate) fn plain_select(ir: &Ir, region: RegionId) -> Option<PlainSelect> {
	let ops: Vec<OpId> = ops_in(ir, region).collect();
	let [source, select_loop, closing] = ops.as_slice() else {
		return None;
	};
	let (source_data, loop_data, closing_data) =
		(ir.op(*source)?, ir.op(*select_loop)?, ir.op(*closing)?);
	let OpKind::Logical(LogOp::Source(attrs)) = &source_data.kind else {
		return None;
	};
	if !matches!(attrs.source_ref, SourceRefShape::Table(_))
		|| attrs.for_update
		|| attrs.with.is_some()
	{
		return None;
	}
	let OpKind::Logical(LogOp::Loop {
		kind: LoopKind::Select,
		sink: LoopSink::Output,
		step: None,
		..
	}) = &loop_data.kind
	else {
		return None;
	};
	let [rows] = source_data.results.as_slice() else {
		return None;
	};
	let [loop_rows] = loop_data.results.as_slice() else {
		return None;
	};
	let [body] = loop_data.regions.as_slice() else {
		return None;
	};
	let plain_body = ops_in(ir, *body).all(|op| {
		matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Logical(
				LogOp::Filter | LogOp::PermissionFilter { .. } | LogOp::Project(_) | LogOp::Yield
			))
		)
	});
	let reads_source = loop_data.operands.as_slice() == [*rows] && ir.uses(*rows).len() == 1;
	let yields_loop = matches!(closing_data.kind, OpKind::Expr(ExprOp::Yield))
		&& closing_data.operands.as_slice() == [*loop_rows]
		&& ir.uses(*loop_rows).len() == 1;
	(plain_body && reads_source && yields_loop).then_some(PlainSelect {
		select_loop: *select_loop,
		source: *source,
	})
}

/// Which extreme of a field an aggregate folds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Extreme {
	/// `math::max`.
	Max,
	/// `math::min`.
	Min,
}

/// Where an extreme statement stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtremeHome {
	/// The aggregate is a value of a top-level statement's own region.
	Statement,
	/// The aggregate is the value of the statement in value position `subquery`, which owns the
	/// region the aggregate stands in.
	Value {
		subquery: OpId,
	},
}

/// The statement of a single `math::max` or `math::min` over one stored field of a whole table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExtremeShape {
	/// Where the statement stands.
	pub(crate) home: ExtremeHome,
	/// The `logical.aggregate`.
	pub(crate) aggregate: OpId,
	/// The select loop the aggregate folds.
	pub(crate) select_loop: OpId,
	/// The table source the loop reads.
	pub(crate) source: OpId,
	/// Which extreme the aggregate folds.
	pub(crate) extreme: Extreme,
	/// The stored field the aggregate reads of each row, a single path segment.
	pub(crate) field: Strand,
}

/// The extreme statement the aggregate `aggregate` is, when it is exactly one `math::max` or
/// `math::min` of one stored field over every row of a table.
///
/// The aggregate has no group key and one column, a bare call of the function over `field` of the
/// row, with any alias. Its rows come from one select loop over a table source that locks nothing
/// and states no `WITH` hint. The loop's body holds permission filters and nothing else, no
/// `WHERE`, and its work is total ([`per_row_total`]). The aggregate's value is read only by a
/// chain of `LIMIT` and `ONLY` that ends in the yield of its region, so a `VERSION` or an
/// `EXPLAIN` around the statement is not this shape. The region is the region of a top-level
/// statement or of a statement in value position: a `TIMEOUT` region around the statement, or a
/// statement of a block, is not.
pub(crate) fn extreme_shape(ir: &Ir, aggregate: OpId) -> Option<ExtremeShape> {
	let data = ir.op(aggregate)?;
	let OpKind::Logical(LogOp::Aggregate(shape)) = &data.kind else {
		return None;
	};
	let one_column = match &shape.fields {
		FieldsShape::Value {
			..
		} => true,
		FieldsShape::Select(fields) => {
			let mut fields = fields.iter();
			matches!(
				(fields.next(), fields.next()),
				(
					Some(FieldShape::Single {
						kind: None,
						..
					}),
					None
				)
			)
		}
	};
	if shape.groups != 0 || !one_column {
		return None;
	}
	let ([rows], [column], [result]) =
		(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
	else {
		return None;
	};
	let (extreme, field) = extreme_column(ir, *column)?;
	if !paged_to_yield(ir, *result) || ir.uses(*rows).len() != 1 {
		return None;
	}
	let home = home_of(ir, aggregate)?;
	let select_loop = defining_op(ir, *rows)?;
	let loop_data = ir.op(select_loop)?;
	let OpKind::Logical(LogOp::Loop {
		kind: LoopKind::Select,
		sink: LoopSink::Output,
		step: None,
		..
	}) = &loop_data.kind
	else {
		return None;
	};
	let ([source_rows], [body]) = (loop_data.operands.as_slice(), loop_data.regions.as_slice())
	else {
		return None;
	};
	let source = defining_op(ir, *source_rows)?;
	let OpKind::Logical(LogOp::Source(attrs)) = &ir.op(source)?.kind else {
		return None;
	};
	if !matches!(attrs.source_ref, SourceRefShape::Table(_))
		|| attrs.for_update
		|| attrs.with.is_some()
		|| ir.uses(*source_rows).len() != 1
	{
		return None;
	}
	let only_permissions = ops_in(ir, *body).all(|op| {
		matches!(
			ir.op(op).map(|held| &held.kind),
			Some(OpKind::Logical(LogOp::PermissionFilter { .. } | LogOp::Yield))
		)
	});
	(only_permissions && per_row_total(ir, select_loop)).then_some(ExtremeShape {
		home,
		aggregate,
		select_loop,
		source,
		extreme,
		field,
	})
}

/// Where the aggregate `aggregate` stands, when that is the region of a top-level statement or of
/// a statement in value position.
fn home_of(ir: &Ir, aggregate: OpId) -> Option<ExtremeHome> {
	let region = ir.enclosing_region(aggregate);
	let owner = ir.parent_op(region)?;
	match (ir.region(region)?.kind, &ir.op(owner)?.kind) {
		(
			RegionKind::Statement(_),
			OpKind::Logical(LogOp::Stmt {
				..
			}),
		) => Some(ExtremeHome::Statement),
		(RegionKind::Subquery, OpKind::Expr(ExprOp::Subquery(_))) => Some(ExtremeHome::Value {
			subquery: owner,
		}),
		_ => None,
	}
}

/// The extreme and the field of the column region `column`, when it yields one `math::max` or
/// `math::min` whose one argument is the field of the row.
fn extreme_column(ir: &Ir, column: RegionId) -> Option<(Extreme, Strand)> {
	if ir.region(column)?.kind != RegionKind::Clause(ClauseRole::Projection) {
		return None;
	}
	let row = *ir.block(*ir.region(column)?.blocks.first()?)?.args.first()?;
	let ops: SmallVec<[OpId; 2]> = ops_in(ir, column).collect();
	let [call, closing] = ops.as_slice() else {
		return None;
	};
	let (call_data, closing_data) = (ir.op(*call)?, ir.op(*closing)?);
	let OpKind::Expr(ExprOp::Agg {
		kind,
		..
	}) = &call_data.kind
	else {
		return None;
	};
	let extreme = match *kind {
		"math::max" => Extreme::Max,
		"math::min" => Extreme::Min,
		_ => return None,
	};
	let [result] = call_data.results.as_slice() else {
		return None;
	};
	let yields_call = closing_data.kind.terminator() == Some(Terminator::Yield)
		&& closing_data.operands.as_slice() == [*result];
	let [argument] = call_data.regions.as_slice() else {
		return None;
	};
	if !yields_call
		|| !call_data.operands.is_empty()
		|| !matches!(ir.region(*argument)?.kind, RegionKind::ErrorScope(_))
	{
		return None;
	}
	let held: SmallVec<[OpId; 2]> = ops_in(ir, *argument).collect();
	let [read, yielded] = held.as_slice() else {
		return None;
	};
	let (read_data, yielded_data) = (ir.op(*read)?, ir.op(*yielded)?);
	let OpKind::Expr(ExprOp::Field {
		name,
	}) = &read_data.kind
	else {
		return None;
	};
	let [value] = read_data.results.as_slice() else {
		return None;
	};
	let reads_row = read_data.operands.as_slice() == [row];
	let yields_read = yielded_data.kind.terminator() == Some(Terminator::Yield)
		&& yielded_data.operands.as_slice() == [*value];
	(reads_row && yields_read).then(|| (extreme, name.clone()))
}

/// Whether `value` is read only by a chain of `LIMIT` and `ONLY` that ends in a yield.
fn paged_to_yield(ir: &Ir, value: ValueId) -> bool {
	let mut at = value;
	for _ in 0..MAX_DEPTH {
		let [used] = ir.uses(at) else {
			return false;
		};
		let Some(user) = ir.op(used.user) else {
			return false;
		};
		if user.kind.terminator() == Some(Terminator::Yield) {
			return true;
		}
		let pages = matches!(user.kind, OpKind::Logical(LogOp::Limit { .. } | LogOp::Only));
		match user.results.first() {
			Some(next) if pages && used.operand == 0 => at = *next,
			_ => return false,
		}
	}
	false
}

/// The op that defines `value`, when an op does.
fn defining_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// Whether every op of the body of the select loop `select_loop` answers for each row without
/// raising and without an effect. See the module docs for the allow-list.
pub(crate) fn per_row_total(ir: &Ir, select_loop: OpId) -> bool {
	let Some(data) = ir.op(select_loop) else {
		return false;
	};
	let [body] = data.regions.as_slice() else {
		return false;
	};
	region_total(ir, *body, select_loop, 0)
}

/// Whether every op in `region` and the regions its ops own is on the allow-list, for a loop
/// `select_loop` whose row the ops read.
fn region_total(ir: &Ir, region: RegionId, select_loop: OpId, depth: usize) -> bool {
	if depth > MAX_DEPTH {
		return false;
	}
	ops_in(ir, region).all(|op| {
		let Some(data) = ir.op(op) else {
			return false;
		};
		op_total(ir, op, select_loop)
			&& data.regions.iter().all(|nested| region_total(ir, *nested, select_loop, depth + 1))
	})
}

/// Whether `op` alone is on the allow-list.
fn op_total(ir: &Ir, op: OpId, select_loop: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let reads_loop_row = |at: usize| {
		data.operands.get(at).is_some_and(|row| row_loop(ir, *row) == Some(select_loop))
	};
	match &data.kind {
		OpKind::Expr(expr) => match expr {
			ExprOp::Literal(_)
			| ExprOp::SessionParam {
				..
			}
			| ExprOp::Truthy => true,
			ExprOp::Field {
				..
			}
			| ExprOp::Doc => reads_loop_row(0),
			ExprOp::Access {
				..
			} => data.operands.first().is_some_and(|base| {
				row_loop(ir, *base).is_some_and(|row| {
					row != select_loop
						&& loop_reads_table(ir, row)
						&& encloses(ir, row, select_loop)
				})
			}),
			ExprOp::Binary {
				op,
			} => matches!(
				op,
				BinaryOperator::Equal
					| BinaryOperator::ExactEqual
					| BinaryOperator::NotEqual
					| BinaryOperator::LessThan
					| BinaryOperator::LessThanEqual
					| BinaryOperator::MoreThan
					| BinaryOperator::MoreThanEqual
			),
			ExprOp::ShortCircuit {
				op,
			} => {
				matches!(op, ShortCircuitOp::And | ShortCircuitOp::Or)
			}
			ExprOp::Prefix(op) => matches!(**op, PrefixOperator::Not),
			ExprOp::Yield => true,
			ExprOp::Array
			| ExprOp::Set
			| ExprOp::Object {
				..
			}
			| ExprOp::RecordId(_)
			| ExprOp::FreeParam {
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
			| ExprOp::ComputedRead(_)
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
			| ExprOp::Method {
				..
			}
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
			| ExprOp::Convert(_)
			| ExprOp::Postfix {
				..
			}
			| ExprOp::Invoke
			| ExprOp::Call(_)
			| ExprOp::ClosureCreate(_)
			| ExprOp::If
			| ExprOp::Throw
			| ExprOp::Subquery(_)
			| ExprOp::Agg {
				..
			}
			| ExprOp::Now {
				..
			}
			| ExprOp::Raise(_)
			| ExprOp::DepthCharge {
				..
			} => false,
		},
		OpKind::Logical(log) => match log {
			LogOp::Filter | LogOp::Yield => true,
			LogOp::PermissionFilter {
				computed,
				..
			} => computed.is_none(),
			LogOp::Project(attrs) => plain_projection(attrs),
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
			| LogOp::Source(_)
			| LogOp::Values {
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
			| LogOp::Subquery
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
			| LogOp::Distinct
			| LogOp::Sort(_)
			| LogOp::Window(_)
			| LogOp::Limit {
				..
			}
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
			| LogOp::Loop {
				..
			}
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
			| LogOp::WriteStage {
				..
			}
			| LogOp::Events(_)
			| LogOp::Next
			| LogOp::Break => false,
		},
		OpKind::Phys(_) => false,
	}
}

/// Whether a projection is `*` and bare fields: no layer, no `OMIT`, and no column coerced to a
/// kind.
fn plain_projection(attrs: &ProjectAttrs) -> bool {
	!attrs.layer
		&& attrs.omit == 0
		&& match &attrs.fields {
			FieldsShape::Value {
				..
			} => true,
			FieldsShape::Select(fields) => fields.iter().all(|field| {
				matches!(
					field,
					FieldShape::All
						| FieldShape::Single {
							kind: None,
							..
						}
				)
			}),
		}
}

/// The select loop whose element row `value` is: the row itself, or the row as it passes through
/// the filters and the projection that read it.
fn row_loop(ir: &Ir, value: ValueId) -> Option<OpId> {
	let mut at = value;
	for _ in 0..MAX_DEPTH {
		match ir.value(at)?.def {
			ValueDef::OpResult {
				op,
				..
			} => match &ir.op(op)?.kind {
				OpKind::Logical(
					LogOp::Filter
					| LogOp::PermissionFilter {
						..
					},
				) => {
					at = *ir.op(op)?.operands.first()?;
				}
				OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return None,
			},
			ValueDef::BlockArg {
				block,
				index,
			} => {
				if index != 0 {
					return None;
				}
				let region = ir.block(block)?.parent;
				let owner = ir.parent_op(region)?;
				match (ir.region(region)?.kind, &ir.op(owner)?.kind) {
					(
						RegionKind::Loop,
						OpKind::Logical(LogOp::Loop {
							kind: LoopKind::Select,
							..
						}),
					) => return Some(owner),
					(RegionKind::Clause(ClauseRole::Where), OpKind::Logical(LogOp::Filter))
					| (
						RegionKind::PermissionFrame,
						OpKind::Logical(LogOp::PermissionFilter {
							..
						}),
					)
					| (
						RegionKind::Clause(ClauseRole::Projection),
						OpKind::Logical(LogOp::Project(_)),
					) => at = *ir.op(owner)?.operands.first()?,
					_ => return None,
				}
			}
		}
	}
	None
}

/// Whether the select loop `select_loop` reads the rows of a table: its rows are documents,
/// objects that a step into a field cannot fetch from.
fn loop_reads_table(ir: &Ir, select_loop: OpId) -> bool {
	let Some(rows) = ir.op(select_loop).and_then(|data| data.operands.first().copied()) else {
		return false;
	};
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(rows).map(|value| value.def)
	else {
		return false;
	};
	matches!(
		ir.op(op).map(|data| &data.kind),
		Some(OpKind::Logical(LogOp::Source(attrs))) if matches!(attrs.source_ref, SourceRefShape::Table(_))
	)
}

/// Whether the body of the select loop `outer` holds the select loop `inner`.
fn encloses(ir: &Ir, outer: OpId, inner: OpId) -> bool {
	crate::exec::ir::core::walk::ancestors(ir, inner).any(|(_, owner)| owner == Some(outer))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::ir::build::logical::bind_request;
	use crate::exec::ir::core::ir::{IrScope, Walk, WalkOrder};
	use crate::exec::ir::iface::catalog::{EffectCx, SemanticCatalog};
	use crate::exec::ir::iface::frame::FrameTable;
	use crate::exec::ir::{RequestInput, RequestStatement};

	struct NoCatalog;

	impl SemanticCatalog for NoCatalog {}

	/// Binds `sql` without converting it.
	async fn bound(sql: &str) -> Ir {
		let registry = FunctionRegistry::with_builtins();
		let frames = FrameTable::default();
		let effects = EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		};
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		bind_request(&input, &effects).await.expect("the request binds")
	}

	/// The `expr.subquery` ops of `ir`, in pre-order.
	fn subqueries(ir: &Ir) -> Vec<OpId> {
		let mut found = Vec::new();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Subquery(_)))) {
				found.push(op);
			}
			Walk::Continue
		});
		found
	}

	/// Whether the value of the first subquery of `sql` is read only for existence.
	async fn existence(sql: &str) -> bool {
		let ir = bound(sql).await;
		let subquery = *subqueries(&ir).first().unwrap_or_else(|| panic!("no subquery in {sql}"));
		let result = ir.op(subquery).expect("the op exists").results[0];
		existence_only(&ir, result)
	}

	/// Whether the per-row work of the plain select that is the first subquery of `sql` is total.
	async fn total(sql: &str) -> bool {
		let ir = bound(sql).await;
		let subquery = *subqueries(&ir).first().expect("a subquery");
		let region = ir.op(subquery).expect("the op exists").regions[0];
		let plain = plain_select(&ir, region).expect("a plain select");
		per_row_total(&ir, plain.select_loop)
	}

	#[tokio::test]
	async fn a_predicate_reads_only_whether_its_subquery_has_a_row() {
		for sql in [
			"SELECT * FROM small WHERE (SELECT id FROM big)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s)",
			"SELECT * FROM small WHERE !(SELECT id FROM big)",
			"SELECT * FROM small WHERE !(SELECT id FROM big) AND n > 1",
			"SELECT * FROM small WHERE (SELECT id FROM big) AND n > 1",
			"SELECT * FROM small WHERE n > 1 AND (SELECT id FROM big)",
			"SELECT * FROM small WHERE n > 1 OR (SELECT id FROM big)",
			"SELECT * FROM small WHERE (SELECT id FROM big) OR n > 1",
			"SELECT * FROM small WHERE n > 1 AND (n < 9 OR (SELECT id FROM big))",
			"SELECT * FROM small WHERE ((SELECT id FROM big) OR 0)",
			"SELECT IF (SELECT id FROM big) { 1 } ELSE { 2 } AS v FROM small",
			"RETURN IF (SELECT id FROM big) { 1 } ELSE { 2 }",
		] {
			assert!(existence(sql).await, "{sql}");
		}
	}

	/// The binder states the predicate of a `WHERE` as the value it yields, with no truthiness op
	/// between, so a clause's closing yield is a reader of truthiness by itself.
	#[tokio::test]
	async fn the_binder_wraps_no_predicate_in_a_truthiness_op() {
		let ir = bound("SELECT * FROM small WHERE (SELECT id FROM big)").await;
		let mut truthy = 0;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Truthy))) {
				truthy += 1;
			}
			Walk::Continue
		});
		assert_eq!(truthy, 0);
	}

	#[tokio::test]
	async fn any_other_reader_reads_the_value() {
		for sql in [
			"SELECT (SELECT id FROM big) AS v FROM small",
			"SELECT ((SELECT id FROM big) OR 0) AS v FROM small",
			"SELECT * FROM small WHERE ((SELECT id FROM big) OR 0) = 1",
			"SELECT * FROM small WHERE (SELECT id FROM big) = []",
			"SELECT * FROM small WHERE array::len((SELECT id FROM big)) > 0",
			"SELECT * FROM small WHERE count((SELECT id FROM big)) > 0",
			"SELECT * FROM small WHERE n IN (SELECT VALUE n FROM big)",
			"SELECT * FROM small WHERE (SELECT id FROM big) CONTAINS small:1",
			"RETURN (SELECT id FROM big) AND [1]",
			"RETURN [] OR (SELECT id FROM big)",
			"RETURN [(SELECT id FROM big)]",
			"RETURN [(SELECT id FROM big), !(SELECT id FROM big)]",
			"RETURN (SELECT id FROM big) ?? 1",
		] {
			assert!(!existence(sql).await, "{sql}");
		}
	}

	#[tokio::test]
	async fn the_work_of_a_plain_select_over_stored_fields_is_total() {
		for sql in [
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = 'a')",
			"SELECT * FROM small WHERE (SELECT id FROM big)",
			"SELECT * FROM small WHERE (SELECT * FROM big WHERE s = 'a' AND (n > 1 OR !(m < 2)))",
			"SELECT * FROM small WHERE (SELECT VALUE s FROM big WHERE s != 'a')",
			"SELECT * FROM small WHERE (SELECT s, n FROM big WHERE n >= 1 AND n <= 5)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s AND n > $parent.n)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE owner = $auth)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent)",
		] {
			assert!(total(sql).await, "{sql}");
		}
	}

	#[tokio::test]
	async fn anything_that_may_raise_is_not_total() {
		for sql in [
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE <int> s > 0)",
			"SELECT * FROM small WHERE (SELECT <int> s FROM big)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE fn::check(n))",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE math::abs(n) > 1)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $p)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE n + 1 > 1)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s.a = 1)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $parent.s.a)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s IN [1, 2])",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE (SELECT id FROM other))",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $auth.id)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $auth.id.x)",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE time::now() > d'2024-01-01')",
			"SELECT * FROM small WHERE (SELECT id FROM big WHERE s = $this.s)",
			"SELECT * FROM [small:1, small:2] WHERE (SELECT id FROM big WHERE s = $parent.s)",
			"SELECT * FROM small:1..3 WHERE (SELECT id FROM big WHERE s = $parent.s)",
			"SELECT * FROM small WHERE (SELECT * OMIT s FROM big)",
		] {
			assert!(!total(sql).await, "{sql}");
		}
	}

	#[tokio::test]
	async fn only_a_plain_select_is_the_shape() {
		let plain = |sql: &'static str| async move {
			let ir = bound(sql).await;
			let subquery = *subqueries(&ir).first().expect("a subquery");
			let region = ir.op(subquery).expect("the op exists").regions[0];
			plain_select(&ir, region).is_some()
		};
		assert!(plain("SELECT * FROM small WHERE (SELECT id FROM big WHERE s = 'a')").await);
		for sql in [
			"SELECT * FROM small WHERE (SELECT id FROM big ORDER BY id)",
			"SELECT * FROM small WHERE (SELECT id FROM big LIMIT 3)",
			"SELECT * FROM small WHERE (SELECT id FROM big START 1)",
			"SELECT * FROM small WHERE (SELECT count() FROM big GROUP ALL)",
			"SELECT * FROM small WHERE (SELECT s FROM big GROUP BY s)",
			"SELECT * FROM small WHERE (SELECT s FROM big SPLIT s)",
			"SELECT * FROM small WHERE (SELECT id FROM big FETCH s)",
			"SELECT * FROM small WHERE (SELECT id FROM ONLY big:1)",
			"SELECT * FROM small WHERE (SELECT id FROM big TIMEOUT 1s)",
			"SELECT * FROM small WHERE (SELECT id FROM big VERSION d'2024-01-01')",
			"SELECT * FROM small WHERE (SELECT id FROM big WITH NOINDEX WHERE s = 'a')",
			"SELECT * FROM small WHERE (SELECT id FROM big FOR UPDATE)",
			"SELECT * FROM small WHERE (SELECT id FROM [1, 2])",
			"SELECT * FROM small WHERE (SELECT id FROM big, other)",
			"SELECT * FROM small WHERE (EXPLAIN SELECT id FROM big)",
			"SELECT * FROM small WHERE (SELECT VALUE id FROM big:1..5)",
		] {
			assert!(!plain(sql).await, "{sql}");
		}
	}

	/// The extreme statement of the first aggregate of `sql`, if it is one.
	async fn extreme(sql: &str) -> Option<ExtremeShape> {
		let ir = bound(sql).await;
		let mut aggregate = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if aggregate.is_none()
				&& matches!(
					ir.op(op).map(|data| &data.kind),
					Some(OpKind::Logical(LogOp::Aggregate(_)))
				) {
				aggregate = Some(op);
			}
			Walk::Continue
		});
		extreme_shape(&ir, aggregate.unwrap_or_else(|| panic!("no aggregate in {sql}")))
	}

	#[tokio::test]
	async fn a_single_max_or_min_of_a_field_over_a_table_is_the_shape() {
		for (sql, kind) in [
			("SELECT math::max(n) AS m FROM t GROUP ALL", Extreme::Max),
			("SELECT math::min(n) AS m FROM t GROUP ALL", Extreme::Min),
			("SELECT math::max(n) FROM t GROUP ALL", Extreme::Max),
			("SELECT VALUE math::min(n) FROM t GROUP ALL", Extreme::Min),
			("SELECT math::max(n) AS a.b FROM t GROUP ALL", Extreme::Max),
			("SELECT math::max(n) AS m FROM t GROUP ALL LIMIT 1 START 0", Extreme::Max),
			("SELECT math::max(n) AS m FROM ONLY t GROUP ALL", Extreme::Max),
			("RETURN (SELECT VALUE math::max(n) FROM t GROUP ALL)[0]", Extreme::Max),
			("SELECT id, (SELECT math::min(n) AS lo FROM t GROUP ALL) AS m FROM t", Extreme::Min),
		] {
			let shape = extreme(sql).await.unwrap_or_else(|| panic!("not the shape: {sql}"));
			assert_eq!(shape.extreme, kind, "{sql}");
			assert_eq!(shape.field.as_str(), "n", "{sql}");
		}
	}

	#[tokio::test]
	async fn anything_else_is_not_the_shape() {
		for sql in [
			"SELECT math::max(n) AS m FROM t WHERE n > 1 GROUP ALL",
			"SELECT math::max(n) AS m FROM t WHERE true GROUP ALL",
			"SELECT math::max(n) AS m, count() AS c FROM t GROUP ALL",
			"SELECT math::max(n) AS m, math::min(n) AS l FROM t GROUP ALL",
			"SELECT n, math::max(n) AS m FROM t GROUP BY n",
			"SELECT math::max(n.x) AS m FROM t GROUP ALL",
			"SELECT math::max(n + 0) AS m FROM t GROUP ALL",
			"SELECT math::max(n, 1) AS m FROM t GROUP ALL",
			"SELECT math::sum(n) AS m FROM t GROUP ALL",
			"SELECT math::mean(n) AS m FROM t GROUP ALL",
			"SELECT math::max(n) AS m FROM t WITH NOINDEX GROUP ALL",
			"SELECT math::max(n) AS m FROM t GROUP ALL VERSION d'2024-01-01'",
			"EXPLAIN SELECT math::max(n) AS m FROM t GROUP ALL",
			"SELECT math::max(n) AS m FROM t, u GROUP ALL",
			"SELECT math::max(n) AS m FROM [1, 2] GROUP ALL",
			"SELECT math::max(n) AS m FROM t:1..5 GROUP ALL",
			"SELECT math::max(n) AS m FROM t GROUP ALL FETCH m",
			"SELECT math::max(n) AS m FROM t GROUP ALL TIMEOUT 1s",
			"SELECT count() AS m FROM t GROUP ALL",
		] {
			assert!(extreme(sql).await.is_none(), "{sql}");
		}
	}
}
