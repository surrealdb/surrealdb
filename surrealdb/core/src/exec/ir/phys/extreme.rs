//! The extreme of an indexed integer field answered from its index: `math::max(n)` and
//! `math::min(n)` over a whole table read the end of the ordered B-tree index over `n` instead of
//! every row.
//!
//! The statement is `SELECT math::max(n) FROM t GROUP ALL` (or `min`), found in the logical
//! dialect by [`extreme_shape`]: one aggregate over one select loop that holds nothing but the
//! caller's permission filters. The rewrite is exact, results and errors included, because the
//! aggregate stays the one place that computes the answer; it is only handed fewer rows. A
//! conditional chooses the rows when the statement runs:
//!
//! - a probe reads the first [`PROBE_ROWS`] rows of `t` in the order of `n`, descending for a `max`
//!   and ascending for a `min`, over the rows whose `n` is a number: the range `-Infinity <= n <=
//!   NaN`, which holds every number (`NaN` orders above `Infinity`) and no `NONE`, `NULL`, boolean,
//!   string or other value. The rows are those the caller may see, because the probe holds the same
//!   permission filters as the statement. It yields the value of `n` of each row and nothing else
//!   of the row;
//! - when the second value of the probe is not equal to the first, the extreme is held by one row,
//!   and the statement runs over that row: the first row of the index order. A value of the probe
//!   that is absent reads as `NONE`, which is equal to nothing but `NONE`, so a table that holds
//!   one number is settled by the same test as any other;
//! - otherwise the statement runs as written: an empty table, and a table that holds no number,
//!   answer as the aggregate over every row does.
//!
//! The aggregate keeps the first of several equal numbers in scan order, which is record id order.
//! The index is read only where it holds no two equal numbers (it is a `UNIQUE` index, which
//! refuses equal numbers of every kind), so the first row of its order is the only row that holds
//! the extreme and no tie is to be broken.
//!
//! A top-level statement carries the conditional as a statement `IF` whose arms are statements, a
//! statement in value position as a value `IF` whose arms are copies of the subquery. What pages
//! the aggregate (`LIMIT`, `START`, `ONLY`) is part of each arm of a top-level statement and
//! stays inside each copy of a subquery.
//!
//! A non-unique index is not read. Its key may be held by many rows, the rows of one key lie in
//! record id order but equal numbers of different kinds lie under different keys, and a backward
//! read of a key held by many rows is dear; the plan carries no estimate of the rows under a key,
//! so the decision cannot tell such a column apart. Only a `UNIQUE` index, whose key is held by one
//! row, is offered.
//!
//! The decision needs what the logical dialect cannot state, the physical catalog's answers, and
//! is made once per container, before the conversion:
//!
//! - the table is defined, is read at the current version and holds no computed field, and the read
//!   enforces nothing at run time that the ops of the statement do not state;
//! - the field is declared exactly `int`, with no `VALUE` clause. The declaration is a cost gate:
//!   it makes the rows the probe reads numbers, so the else arm, which a stored value of another
//!   kind can reach, is the rare path. It does not decide the answer. A value stored before the
//!   declaration may be of any kind, an array included, which an index holds as one entry per
//!   element; the number range of the probe is checked against the stored value after the index
//!   read, so such a row is read as the statement as written reads it;
//! - every B-tree index whose first column is the field is a `UNIQUE` index of that column alone,
//!   and one exists. A non-unique index declines, as does a field that has one beside a unique
//!   index, because the plan may read either;
//! - no field the caller's `SELECT` permission restricts is the field or a path over it.
//!
//! A statement under an `EXPLAIN` is left as written, so that the plan it renders is the plan of
//! the statement.

use std::ops::Deref;

use surrealdb_strand::Strand;

use super::access::{ReadVersion, RestrictedSelect, TableAnswers, source_key};
use super::count::in_stored_clause;
use super::edit::{
	SortBy, Stamp, filter_over, first_result, if_over, limit_over, project_field_over,
	require_untokened, sort_over,
};
use super::existence::under_explain;
use crate::catalog::Index;
use crate::exec::ir::analysis::existence::{Extreme, ExtremeHome, ExtremeShape, extreme_shape};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, NestedStrategy, ShortCircuitOp};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{BlockId, OpId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, SourceRefShape};
use crate::exec::ir::logical::verify::timestamp_arg;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::passes::rehome::{RehomeMode, rehome_tree};
use crate::expr::{BinaryOperator, Idiom, Kind};
use crate::val::{Number, Value};

/// The rows the probe of an extreme reads: the extreme and the number that follows it.
const PROBE_ROWS: u64 = 2;

/// Gives every extreme statement of `ir` that its index answers the conditional of the module
/// docs, and answers how many it rewrote.
///
/// Called once per container, before [`convert`](crate::exec::ir::core::convert::convert). An
/// answer the physical catalog does not hold is [`IrError::NotLoaded`]; one it knows to be
/// unknowable (a table read after a catalog change) leaves the statement as it is.
pub(super) fn offer(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	require_untokened(ir)?;
	let mut candidates = Vec::new();
	ir.walk(ir.top(), WalkOrder::PostOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Aggregate(shape))) if shape.groups == 0
		) && !in_stored_clause(ir, op)
			&& !under_explain(ir, op)
		{
			candidates.push(op);
		}
		Walk::Continue
	});
	let mut decided = Vec::new();
	for aggregate in candidates {
		let Some(shape) = extreme_shape(ir, aggregate) else {
			continue;
		};
		if decide(ir, &shape, cx)? {
			decided.push(shape);
		}
	}
	let mut rewritten = 0;
	for shape in decided {
		if rewrite(ir, facts, &shape)? {
			rewritten += 1;
		}
	}
	Ok(rewritten)
}

// ---------------------------------------------------------------------------------------------
// The decision
// ---------------------------------------------------------------------------------------------

/// Whether the catalog's answers let the index over the field of `shape` answer the statement.
fn decide(ir: &Ir, shape: &ExtremeShape, cx: &PhysCx<'_>) -> Result<bool, IrError> {
	let Some(OpKind::Logical(LogOp::Source(attrs))) = ir.op(shape.source).map(|data| &data.kind)
	else {
		return Ok(false);
	};
	if attrs.may_meet_change || !matches!(attrs.source_ref, SourceRefShape::Table(_)) {
		return Ok(false);
	}
	let Some(key) = source_key(ir, shape.source, cx.effects.catalog, cx.physical)? else {
		return Ok(false);
	};
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::Unknown | CatalogAnswer::Absent => return Ok(false),
		CatalogAnswer::NotLoaded => {
			return Err(IrError::NotLoaded {
				what: "extreme::offer: the answers of a table an extreme reads",
			});
		}
	};
	// A resolved source has what its table declares stated as the permission ops of the
	// statement, which the allow-list has judged. A table whose permission context is unresolved
	// is enforced as it is met when the statement runs, which the ops do not show.
	let enforced_by_ops =
		attrs.resolved && !matches!(answers.restricted, RestrictedSelect::Unresolved);
	let stated = answers.defined
		&& !answers.lightweight
		&& answers.key.is_none()
		&& answers.version == ReadVersion::Current
		&& !answers.fields.iter().any(|field| field.computed.is_some())
		&& (!answers.enforces || enforced_by_ops);
	Ok(stated && field_is_served(&answers, &shape.field))
}

/// Whether `field` is declared exactly `int`, is not restricted, and every B-tree index that
/// leads with it is a `UNIQUE` index of it alone, one at least.
fn field_is_served(answers: &TableAnswers, field: &Strand) -> bool {
	let path = Idiom::field(field.clone());
	let declared_int = answers.fields.iter().any(|definition| {
		definition.name == path
			&& matches!(definition.field_kind, Some(Kind::Int))
			&& definition.value.is_none()
			&& definition.computed.is_none()
	});
	if !declared_int || answers.restricted.prefixes().index_touches(std::slice::from_ref(&path)) {
		return false;
	}
	let mut leading = answers
		.indexes
		.iter()
		.filter(|index| matches!(index.index, Index::Idx | Index::Uniq))
		.filter(|index| index.cols.first() == Some(&path))
		.peekable();
	leading.peek().is_some()
		&& leading.all(|index| matches!(index.index, Index::Uniq) && index.cols.len() == 1)
}

// ---------------------------------------------------------------------------------------------
// The edit
// ---------------------------------------------------------------------------------------------

/// What every op the edit makes shares: the stamp of the aggregate it stands for and the field of
/// the statement.
struct Site {
	stamp: Stamp,
	field: Strand,
}

impl Deref for Site {
	type Target = Stamp;

	fn deref(&self) -> &Stamp {
		&self.stamp
	}
}

/// The op that defines `value`.
fn definer(rw: &Rewriter<'_>, value: ValueId) -> Result<OpId, IrError> {
	match rw.ir().value(value).map(|held| held.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => Ok(op),
		_ => Err(IrError::Stale("extreme: the op that defines a value")),
	}
}

/// Rewrites the statement of `shape` into the conditional of the module docs, and answers whether
/// it did. A statement some region of which cannot be copied stays as it is.
fn rewrite(ir: &mut Ir, facts: &mut FactCache, shape: &ExtremeShape) -> Result<bool, IrError> {
	let home = shape.home;
	let proto = ir.op(shape.aggregate).ok_or(IrError::Stale("extreme: the aggregate"))?.clone();
	let chain = paging_chain(ir, shape.aggregate);
	let replaced = match home {
		ExtremeHome::Statement => *chain.last().ok_or(IrError::Stale("extreme: the aggregate"))?,
		ExtremeHome::Value {
			subquery,
		} => subquery,
	};
	let result = *ir
		.op(replaced)
		.and_then(|data| data.results.first())
		.ok_or(IrError::Stale("extreme: the value the conditional replaces"))?;
	let ty = ir.value(result).ok_or(IrError::Stale("extreme: the value"))?.ty.clone();
	let block = ir.op(replaced).ok_or(IrError::Stale("extreme: the replaced op"))?.parent;
	let held: FxHashSet<OpId> = ir
		.block(block)
		.ok_or(IrError::Stale("extreme: the block of the statement"))?
		.ops
		.iter()
		.copied()
		.collect();
	let site = Site {
		stamp: Stamp::of(&proto),
		field: shape.field.clone(),
	};
	let mut rw = Rewriter::new(ir, facts);
	let consumed = [shape.aggregate, shape.select_loop, shape.source];
	let built = rw.derived(&consumed, |rw| build(rw, &site, shape, home, replaced, &ty));
	let branch = match built {
		Ok(branch) => branch,
		Err(IrError::Capacity(_) | IrError::Refused(_)) => {
			discard(&mut rw, block, &held)?;
			return Ok(false);
		}
		Err(other) => return Err(other),
	};
	let answer = first_result(&rw, branch)?;
	rw.replace_op(replaced, &[answer])?;
	if matches!(home, ExtremeHome::Statement) {
		let rest = chain.iter().rev().skip(1).chain([&shape.select_loop, &shape.source]);
		for op in rest {
			match rw.erase(*op, EliminationReason::MergedInto(branch)) {
				Ok(()) | Err(IrError::HasUses(_)) => {}
				Err(error) => return Err(error),
			}
		}
	}
	Ok(true)
}

/// Erases the ops a refused edit made in `block`, the ops it holds that `held` does not name, last
/// made first. The ops made inside regions go with the op that owns them, and nothing reads a
/// made op but another, so the container is as it was before the edit.
fn discard(rw: &mut Rewriter<'_>, block: BlockId, held: &FxHashSet<OpId>) -> Result<(), IrError> {
	let made: Vec<OpId> = rw
		.ir()
		.block(block)
		.ok_or(IrError::Stale("extreme: the block of the statement"))?
		.ops
		.iter()
		.copied()
		.filter(|op| !held.contains(op))
		.collect();
	for op in made.into_iter().rev() {
		rw.erase(op, EliminationReason::DeadPure)?;
	}
	Ok(())
}

/// Creates the probe, its test and the conditional before `replaced`, and answers the
/// conditional.
fn build(
	rw: &mut Rewriter<'_>,
	site: &Site,
	shape: &ExtremeShape,
	home: ExtremeHome,
	replaced: OpId,
	ty: &TypeKnowledge,
) -> Result<OpId, IrError> {
	let plan = Plan {
		site,
		shape,
		home,
		replaced,
		ty,
	};
	let here = InsertPoint::Before(replaced);
	let ascending = shape.extreme == Extreme::Min;
	// The probe: the first rows of the ordered numbers, a statement of its own.
	let probe =
		rw.create(here, site.spec(OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)), &[], 1))?;
	let (_, block) = rw.create_region(probe, RegionKind::Subquery, &[])?;
	let rows = rehome_tree(
		rw,
		shape.select_loop,
		InsertPoint::End(block),
		RehomeMode::Copy,
		EliminationReason::DeadPure,
	)?
	.first()
	.copied()
	.ok_or(IrError::Stale("extreme: the rows of the probe"))?;
	let probe_loop = definer(rw, rows)?;
	number_filter(rw, site, probe_loop)?;
	let ordered =
		ordered_head(rw, site, InsertPoint::End(block), rows, &site.field, ascending, PROBE_ROWS)?;
	let numbers = project_field_over(rw, site, InsertPoint::End(block), ordered, &site.field)?;
	site.close(rw, block, numbers)?;
	let probed = first_result(rw, probe)?;
	choose(rw, &plan, here, probed)
}

/// What every conditional of an extreme shares: where the statement stands, the statement the
/// arms copy, and the type of its value.
struct Plan<'a> {
	site: &'a Site,
	shape: &'a ExtremeShape,
	home: ExtremeHome,
	replaced: OpId,
	ty: &'a TypeKnowledge,
}

/// The conditional at `here` that runs the statement over the first row of the index when the
/// rows `probed` of the probe show that no other row holds the extreme, and as written otherwise.
fn choose(
	rw: &mut Rewriter<'_>,
	plan: &Plan<'_>,
	here: InsertPoint,
	probed: ValueId,
) -> Result<OpId, IrError> {
	let Plan {
		site,
		shape,
		home,
		replaced,
		ty,
	} = *plan;
	let ascending = shape.extreme == Extreme::Min;
	let first = nth_of(rw, site, here, probed, 0)?;
	let other = nth_of(rw, site, here, probed, 1)?;
	let settled = site.value(
		rw,
		here,
		ExprOp::Binary {
			op: BinaryOperator::NotEqual,
		},
		&[first, other],
	)?;
	let (branch, arms) =
		if_over(rw, site, here, settled, ty, matches!(home, ExtremeHome::Statement))?;
	for narrowed in [true, false] {
		let (_, arm) = rw.create_region(branch, arms, &[])?;
		let (block, answer) = match home {
			ExtremeHome::Statement => {
				let statement = rw.create(
					InsertPoint::End(arm),
					site.spec(
						OpKind::Logical(LogOp::Statement {
							changes_catalog: false,
							served: None,
						}),
						&[],
						1,
					),
				)?;
				let (_, block) = rw.create_region(
					statement,
					RegionKind::Statement(StatementCommit::InTxn),
					&[timestamp_arg()],
				)?;
				let copied = rehome_tree(
					rw,
					replaced,
					InsertPoint::End(block),
					RehomeMode::Copy,
					EliminationReason::DeadPure,
				)?
				.first()
				.copied()
				.ok_or(IrError::Stale("extreme: the aggregate of an arm"))?;
				if narrowed {
					let aggregate = aggregate_of(rw, definer(rw, copied)?)?;
					narrow(rw, site, aggregate, ascending)?;
				}
				site.close_statement(rw, block, copied)?;
				(arm, first_result(rw, statement)?)
			}
			ExtremeHome::Value {
				subquery,
			} => {
				let copied = rehome_tree(
					rw,
					subquery,
					InsertPoint::End(arm),
					RehomeMode::Copy,
					EliminationReason::DeadPure,
				)?
				.first()
				.copied()
				.ok_or(IrError::Stale("extreme: the subquery of an arm"))?;
				if narrowed {
					let held = definer(rw, copied)?;
					let region = *rw
						.ir()
						.op(held)
						.and_then(|data| data.regions.first())
						.ok_or(IrError::Stale("extreme: the region of a copied subquery"))?;
					let aggregate = ops_in(rw.ir(), region)
						.find(|op| {
							matches!(
								rw.ir().op(*op).map(|data| &data.kind),
								Some(OpKind::Logical(LogOp::Aggregate(_)))
							)
						})
						.ok_or(IrError::Stale("extreme: the aggregate of a copied subquery"))?;
					narrow(rw, site, aggregate, ascending)?;
				}
				(arm, copied)
			}
		};
		match home {
			ExtremeHome::Statement => {
				rw.create(
					InsertPoint::End(block),
					site.spec(OpKind::Logical(LogOp::Yield), &[answer], 0),
				)?;
			}
			ExtremeHome::Value {
				..
			} => site.close(rw, block, answer)?,
		}
	}
	Ok(branch)
}

/// The aggregate and the `LIMIT` and `ONLY` that page its value, in order: the ops a statement
/// holds above its select loop.
fn paging_chain(ir: &Ir, aggregate: OpId) -> Vec<OpId> {
	let mut chain = vec![aggregate];
	let mut at = aggregate;
	while let Some(next) = ir
		.op(at)
		.and_then(|data| data.results.first())
		.and_then(|result| ir.uses(*result).first())
		.map(|used| used.user)
		.filter(|user| {
			matches!(
				ir.op(*user).map(|data| &data.kind),
				Some(OpKind::Logical(LogOp::Limit { .. } | LogOp::Only))
			)
		}) {
		chain.push(next);
		at = next;
	}
	chain
}

/// The aggregate under the paging ops that end at `top`, in a copy of the statement.
fn aggregate_of(rw: &Rewriter<'_>, top: OpId) -> Result<OpId, IrError> {
	let mut at = top;
	loop {
		let data = rw.ir().op(at).ok_or(IrError::Stale("extreme: a copied op"))?;
		match &data.kind {
			OpKind::Logical(LogOp::Aggregate(_)) => return Ok(at),
			OpKind::Logical(
				LogOp::Limit {
					..
				}
				| LogOp::Only,
			) => {
				let input = *data.operands.first().ok_or(IrError::Stale("extreme: a paging op"))?;
				at = definer(rw, input)?;
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => {
				return Err(IrError::Stale("extreme: the aggregate of a copied statement"));
			}
		}
	}
}

/// Narrows the statement the copied aggregate `aggregate` closes to the first number of its
/// field in index order: its select loop keeps only numbers, and the ordered head of one row
/// stands between the loop and the aggregate.
fn narrow(
	rw: &mut Rewriter<'_>,
	site: &Site,
	aggregate: OpId,
	ascending: bool,
) -> Result<(), IrError> {
	let input = *rw
		.ir()
		.op(aggregate)
		.and_then(|data| data.operands.first())
		.ok_or(IrError::Stale("extreme: the rows of an aggregate"))?;
	let arm_loop = definer(rw, input)?;
	number_filter(rw, site, arm_loop)?;
	let head =
		ordered_head(rw, site, InsertPoint::Before(aggregate), input, &site.field, ascending, 1)?;
	rw.set_operand(aggregate, 0, head)
}

/// `numbers[position]`: the value of the probe at that position, `NONE` when there is none.
fn nth_of(
	rw: &mut Rewriter<'_>,
	site: &Site,
	at: InsertPoint,
	numbers: ValueId,
	position: u64,
) -> Result<ValueId, IrError> {
	let position = i64::try_from(position).map_err(|_| IrError::Capacity("probe position"))?;
	let index = site.literal(rw, at, Value::Number(Number::Int(position)))?;
	site.value(rw, at, ExprOp::Index, &[numbers, index])
}

/// Gives the body of the copied select loop `select_loop` a filter that keeps the rows whose
/// field is a number: `-Infinity <= field <= NaN`, after the permission filters it holds.
fn number_filter(rw: &mut Rewriter<'_>, site: &Site, select_loop: OpId) -> Result<(), IrError> {
	let body = *rw
		.ir()
		.op(select_loop)
		.and_then(|data| data.regions.first())
		.ok_or(IrError::Stale("extreme: the body of a select loop"))?;
	let block = *rw
		.ir()
		.region(body)
		.and_then(|region| region.blocks.first())
		.ok_or(IrError::Stale("extreme: the block of a select loop"))?;
	let closing = rw
		.ir()
		.block(block)
		.and_then(|held| held.ops.last().copied())
		.ok_or(IrError::Stale("extreme: the yield of a select loop"))?;
	let row = *rw
		.ir()
		.op(closing)
		.and_then(|data| data.operands.first())
		.ok_or(IrError::Stale("extreme: the row a select loop yields"))?;
	let kept = filter_over(rw, site, closing, row, |rw, end, argument| {
		let read = site.value(
			rw,
			end,
			ExprOp::Field {
				name: site.field.clone(),
			},
			&[argument],
		)?;
		let low = site.literal(rw, end, Value::Number(Number::Float(f64::NEG_INFINITY)))?;
		let above = site.value(
			rw,
			end,
			ExprOp::Binary {
				op: BinaryOperator::MoreThanEqual,
			},
			&[read, low],
		)?;
		let both = rw.create(
			end,
			site.spec(
				OpKind::Expr(ExprOp::ShortCircuit {
					op: ShortCircuitOp::And,
				}),
				&[above],
				1,
			),
		)?;
		let (_, lazy) = rw.create_region(both, RegionKind::LazyOperand, &[])?;
		let high =
			site.literal(rw, InsertPoint::End(lazy), Value::Number(Number::Float(f64::NAN)))?;
		let below = site.value(
			rw,
			InsertPoint::End(lazy),
			ExprOp::Binary {
				op: BinaryOperator::LessThanEqual,
			},
			&[read, high],
		)?;
		site.close(rw, lazy, below)?;
		first_result(rw, both)
	})?;
	rw.set_operand(closing, 0, kept)
}

/// The first `count` rows of `rows` in the order of `field`, `ascending` or not, at `at`: a sort by
/// the field and a limit, as the written `ORDER BY field LIMIT count` binds.
fn ordered_head(
	rw: &mut Rewriter<'_>,
	site: &Site,
	at: InsertPoint,
	rows: ValueId,
	field: &Strand,
	ascending: bool,
	count: u64,
) -> Result<ValueId, IrError> {
	let key = SortBy {
		field: field.clone(),
		ascending,
	};
	let sorted = sort_over(rw, site, at, rows, &[key])?;
	let count = i64::try_from(count).map_err(|_| IrError::Capacity("probe rows"))?;
	let limit = site.literal(rw, at, Value::Number(Number::Int(count)))?;
	limit_over(rw, site, at, sorted, limit)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::core::rewriter::OpSpec;
	use crate::exec::ir::iface::ids::{FrameId, SnapshotId};

	fn spec(kind: ExprOp, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind: OpKind::Expr(kind),
			operands: operands.iter().copied().collect(),
			results: std::iter::repeat_n(TypeKnowledge::Unknown, results).collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	/// The ops a refused edit made in a block, those of its regions and those that read each
	/// other and values the edit did not make included, are erased, and the ops the block held
	/// before it stay.
	#[test]
	fn a_refused_edit_leaves_the_block_as_it_was() {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let at = InsertPoint::End(block);
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let literal = |rw: &mut Rewriter<'_>, at: InsertPoint, value: i64| {
			let op = rw
				.create(at, spec(ExprOp::Literal(Box::new(Value::from(value))), &[], 1))
				.expect("the literal is created");
			first_result(rw, op).expect("the literal has a result")
		};
		let held_value = literal(&mut rw, at, 1);
		let held: FxHashSet<OpId> =
			rw.ir().block(block).expect("the block").ops.iter().copied().collect();

		let made = literal(&mut rw, at, 2);
		let nested =
			rw.create(at, spec(ExprOp::Subquery(NestedStrategy::Apply), &[], 1)).expect("the");
		let (_, inner) = rw.create_region(nested, RegionKind::Subquery, &[]).expect("the region");
		let inside = rw
			.create(
				InsertPoint::End(inner),
				spec(
					ExprOp::Binary {
						op: BinaryOperator::Add,
					},
					&[made, held_value],
					1,
				),
			)
			.expect("the op is created");
		let inside = first_result(&rw, inside).expect("the op has a result");
		rw.create(InsertPoint::End(inner), spec(ExprOp::Yield, &[inside], 0)).expect("the yield");
		let nested_value = first_result(&rw, nested).expect("the subquery has a result");
		rw.create(at, spec(ExprOp::Index, &[nested_value, made], 1)).expect("the index is created");

		discard(&mut rw, block, &held).expect("the made ops are erased");
		let after: FxHashSet<OpId> =
			rw.ir().block(block).expect("the block").ops.iter().copied().collect();
		assert_eq!(after, held);
		assert!(rw.ir().uses(held_value).is_empty(), "no erased op still reads a held value");
	}
}
