//! The count fast path: a `count()` of a table's records answered by a count source.
//!
//! A statement `SELECT count() FROM <source> [WHERE <cond>] GROUP ALL` reads its source through one
//! select loop and folds the rows in one aggregate. When the records can be counted without
//! reading them, [`offer`] replaces the source, the loop and the aggregate by one `phys.count`,
//! which stands in for the projection as well as the aggregation: its one row is what the
//! aggregate would have answered, and whatever pages that row (`LIMIT`, `START`, `ONLY`) stays
//! where it is.
//!
//! The decision is made once, before the conversion of the statement: it reads the logical source
//! and the filters of its loop, which the conversion consumes. The shape and the conditions are the
//! planner's own:
//!
//! - the aggregate holds no group key, and every column is a bare `count()`; a column whose alias
//!   is a path rather than a key declines, because the count source builds a flat row;
//! - the source is read by one select loop whose body holds the permission filters the caller is
//!   judged by, at most one filter, and nothing else; a source that locks its records, a union, a
//!   split, a sort, a fetch or an `OMIT` stand in the way;
//! - a table, or a record id naming a key range, is counted whole, unless the table is a
//!   lightweight relation, which stores no record range;
//! - a condition over a table is counted from an index, in this order: a `COUNT` index with that
//!   guard, a B-tree walk that covers the whole condition, an exact bitmap fusion. Never for a read
//!   under a `VERSION`, under `WITH NOINDEX`, or whose condition touches a field that a non-`Full`
//!   `SELECT` permission restricts. An index count would reveal how many records hold values the
//!   caller may not select.
//!
//! The answers are the physical catalog's: [`TableAnswers`] states whether the table is defined,
//! whether it is a lightweight relation, its online indexes and their columns, the restricted
//! fields and what the table's `SELECT` permission does to a count. A table whose answers are not
//! known at plan time (a catalog change earlier in the request) is not counted; the statement is
//! planned again when it is reached.
//!
//! A table whose `SELECT` permission is a predicate holds the plan that answers when the shortcut
//! does not apply as a [`RegionKind::Subquery`] region beside the count, a copy of the statement as
//! the conversion would have met it. The counted shapes that decline hold none.

use smallvec::{SmallVec, smallvec};

use super::access::{ReadVersion, TableAnswers, source_key};
use super::access_expr::{Reconstructed, predicate_of_region};
use super::ops::{CountAttrs, CountShape, PhysOp};
use crate::catalog::{Index, IndexId};
use crate::exec::function::FunctionRegistry;
use crate::exec::index::access_path::{
	BTreeAccess, IndexRef as AccessIndexRef, access_fans_out, range_matches_comparison,
};
use crate::exec::index::derived_columns::{guard_counts, range_is_widened};
use crate::exec::ir::build::phys::{bitmap_shape, index_ref};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape};
use crate::exec::ir::iface::catalog::{CatalogAnswer, PhysCx};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::logical::ops::{
	AggregateShape, FieldShape, FieldsShape, LogOp, LoopKind, LoopSink, SourceRefShape,
};
use crate::exec::ir::op_kind::{OpKind, Terminator};
use crate::exec::operators::scan::CountOutput;
use crate::exec::planner::util::strip_index_conditions_in;
use crate::exec::vm::payload::CountSelect;
use crate::expr::logical::LogicalExpr;
use crate::expr::with::With;

/// The statement shape a count replaces, found in the logical dialect.
struct Matched {
	/// The `logical.aggregate`.
	aggregate: OpId,
	/// The select loop that reads the source and holds the permission filters and the filter.
	select_loop: OpId,
	/// The `logical.source`.
	source: OpId,
	/// The permission filters of the loop's body, innermost first.
	permissions: SmallVec<[OpId; 2]>,
	/// The filter of the loop's body.
	filter: Option<OpId>,
	/// What the source names.
	source_ref: SourceRefShape,
	/// The source's index hint.
	with: Option<With>,
	/// The shape of the counted row.
	output: CountOutput,
}

/// What [`offer`] decided for one statement.
struct Decision {
	shape: CountShape,
	select: CountSelect,
	deltas: Option<IndexId>,
	/// The filter's `Clause(Where)` region, which an indexed count holds.
	condition: Option<RegionId>,
}

/// Replaces every count the statement shapes of `ir` allow by a `phys.count`, and answers how many
/// it replaced.
///
/// Called once per container, before [`convert`](crate::exec::ir::core::convert::convert): it reads
/// the logical source and the filters the conversion consumes. An aggregate nested in the
/// predicate of another is decided first, so the outer count is decided over the inner count
/// that replaces it. An answer the physical catalog does not hold is [`IrError::NotLoaded`]; one it
/// knows to be unknowable (a table read after a catalog change) leaves the statement as it is.
pub(super) fn offer(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<u32, IrError> {
	let mut candidates = Vec::new();
	ir.walk(ir.top(), WalkOrder::PostOrder, &mut |op| {
		if matches!(
			ir.op(op).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Aggregate(shape))) if shape.groups == 0
		) && !in_stored_clause(ir, op)
		{
			candidates.push(op);
		}
		Walk::Continue
	});
	let mut decided = Vec::new();
	for aggregate in candidates {
		let Some(matched) = matched(ir, aggregate) else {
			continue;
		};
		if let Some(decision) = decide(ir, &matched, cx)? {
			decided.push((matched, decision));
		}
	}
	let mut replaced = 0;
	for (matched, decision) in decided {
		if replace(ir, facts, &matched, &decision)? {
			replaced += 1;
		}
	}
	Ok(replaced)
}

/// Whether `op` stands in code a statement stores and never runs.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

// ---------------------------------------------------------------------------------------------
// The shape
// ---------------------------------------------------------------------------------------------

/// The statement shape `aggregate` heads, when a count can replace it.
fn matched(ir: &Ir, aggregate: OpId) -> Option<Matched> {
	let data = ir.op(aggregate)?;
	let OpKind::Logical(LogOp::Aggregate(shape)) = &data.kind else {
		return None;
	};
	let output = counted_output(ir, data, shape)?;
	let [rows] = data.operands.as_slice() else {
		return None;
	};
	if !only_paged(ir, *data.results.first()?) {
		return None;
	}
	let select_loop = producer(ir, *rows)?;
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
	if ir.uses(*rows).len() != 1 {
		return None;
	}
	let ([source_rows], [body]) = (loop_data.operands.as_slice(), loop_data.regions.as_slice())
	else {
		return None;
	};
	let source = producer(ir, *source_rows)?;
	let source_data = ir.op(source)?;
	let OpKind::Logical(LogOp::Source(attrs)) = &source_data.kind else {
		return None;
	};
	if attrs.for_update || ir.uses(*source_rows).len() != 1 {
		return None;
	}
	let (permissions, filter) = body_nodes(ir, *body)?;
	Some(Matched {
		aggregate,
		select_loop,
		source,
		permissions,
		filter,
		source_ref: attrs.source_ref.clone(),
		with: attrs.with.clone(),
		output,
	})
}

/// The op that defines `value`.
fn producer(ir: &Ir, value: ValueId) -> Option<OpId> {
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

/// Whether the one row of `value` is read only by what pages it: a `LIMIT` or `ONLY` chain that
/// ends in the yield of its region, or in the `EXPLAIN` that prints the statement. A projection, a
/// sort or any other reader says the statement holds a clause a count source does not apply.
fn only_paged(ir: &Ir, value: ValueId) -> bool {
	let mut at = value;
	loop {
		let [used] = ir.uses(at) else {
			return false;
		};
		let Some(user) = ir.op(used.user) else {
			return false;
		};
		if user.kind.terminator() == Some(Terminator::Yield)
			|| matches!(user.kind, OpKind::Logical(LogOp::Explain { .. }))
		{
			return true;
		}
		let pages = matches!(user.kind, OpKind::Logical(LogOp::Limit { .. } | LogOp::Only));
		match user.results.first() {
			Some(next) if pages && used.operand == 0 => at = *next,
			_ => return false,
		}
	}
}

/// The permission filters and the filter of a select loop's body, when the body holds nothing
/// else: the nodes of the statement's pipeline that stand over the rows of the source, in the order
/// they apply, then the yield of the last row.
///
/// A permission filter judged as another principal (`AUTH LIMIT`) is not the caller's permission,
/// so the body does not match and the statement keeps its ordinary plan.
fn body_nodes(ir: &Ir, body: RegionId) -> Option<(SmallVec<[OpId; 2]>, Option<OpId>)> {
	let block = *ir.region(body)?.blocks.first()?;
	let mut row = *ir.block(block)?.args.first()?;
	let mut permissions = SmallVec::new();
	let mut filter = None;
	let mut nodes = ops_in(ir, body).peekable();
	while let Some(op) = nodes.next() {
		let data = ir.op(op)?;
		if nodes.peek().is_none() {
			let last = data.kind.terminator() == Some(Terminator::Yield)
				&& data.operands.as_slice() == [row];
			return last.then_some((permissions, filter));
		}
		if data.operands.first() != Some(&row) {
			return None;
		}
		match &data.kind {
			OpKind::Logical(LogOp::PermissionFilter {
				..
			}) if filter.is_none() => {
				let own = ir.frame(data.frame)?;
				if own.eval.auth.is_some() {
					return None;
				}
				permissions.push(op);
			}
			OpKind::Logical(LogOp::Filter) if filter.is_none() => filter = Some(op),
			_ => return None,
		}
		row = *data.results.first()?;
	}
	None
}

/// The shape of the counted row, when every column of the aggregate is a bare `count()`.
///
/// A column's name is its alias, or the name its expression derives; an alias that is a path
/// declines, because the count source builds a flat map of name to value where the aggregation
/// places such an alias as nested objects.
fn counted_output(ir: &Ir, data: &OpData, shape: &AggregateShape) -> Option<CountOutput> {
	let columns: SmallVec<[RegionId; 2]> = data
		.regions
		.iter()
		.copied()
		.filter(|region| {
			ir.region(*region)
				.is_some_and(|held| held.kind == RegionKind::Clause(ClauseRole::Projection))
		})
		.collect();
	if columns.is_empty() || !columns.iter().all(|column| is_bare_count(ir, *column)) {
		return None;
	}
	match &shape.fields {
		FieldsShape::Value {
			..
		} => (columns.len() == 1).then_some(CountOutput::Value),
		FieldsShape::Select(fields) => {
			let mut names = shape.names.iter();
			let mut out = Vec::with_capacity(fields.len());
			for field in fields.iter() {
				let FieldShape::Single {
					alias,
					..
				} = field
				else {
					return None;
				};
				let derived = names.next()?;
				out.push(match alias {
					Some(alias) => (alias.parts().len() == 1).then(|| alias.name())?,
					None => derived.name(),
				});
			}
			(out.len() == columns.len()).then_some(CountOutput::Fields(out))
		}
	}
}

/// Whether the region `column` yields the value of one `count` aggregate with no argument: an
/// `expr.agg` of kind `count` whose argument region yields nothing.
fn is_bare_count(ir: &Ir, column: RegionId) -> bool {
	let ops: SmallVec<[OpId; 2]> = ops_in(ir, column).collect();
	let [agg, ret] = ops.as_slice() else {
		return false;
	};
	let (Some(agg_data), Some(ret_data)) = (ir.op(*agg), ir.op(*ret)) else {
		return false;
	};
	let counts = matches!(
		&agg_data.kind,
		OpKind::Expr(ExprOp::Agg {
			kind: "count",
			..
		})
	);
	let no_argument = match agg_data.regions.as_slice() {
		[] => true,
		[arguments] => {
			let held: SmallVec<[OpId; 2]> = ops_in(ir, *arguments).collect();
			matches!(held.as_slice(), [only] if ir.op(*only).is_some_and(|data| {
				data.kind.terminator() == Some(Terminator::Yield) && data.operands.is_empty()
			}))
		}
		_ => false,
	};
	counts
		&& no_argument
		&& agg_data.operands.is_empty()
		&& ret_data.kind.terminator() == Some(Terminator::Yield)
		&& agg_data.results.first().is_some_and(|result| ret_data.operands.as_slice() == [*result])
}

// ---------------------------------------------------------------------------------------------
// The decision
// ---------------------------------------------------------------------------------------------

/// Which count answers the statement, or `None` when the ordinary plan does.
fn decide(ir: &Ir, m: &Matched, cx: &PhysCx<'_>) -> Result<Option<Decision>, IrError> {
	let Some(key) = source_key(ir, m.source, cx.effects.catalog, cx.physical)? else {
		return Ok(None);
	};
	let answers = match cx.physical.table_answers(&key) {
		CatalogAnswer::Found(answers) => answers,
		CatalogAnswer::Unknown | CatalogAnswer::Absent => return Ok(None),
		CatalogAnswer::NotLoaded => {
			return Err(IrError::NotLoaded {
				what: "count::offer: the answers of a counted table",
			});
		}
	};
	let Some(filter) = m.filter else {
		return Ok(counted_whole(ir, m, &answers));
	};
	let SourceRefShape::Table(table) = &m.source_ref else {
		return Ok(None);
	};
	indexed(ir, m, filter, table, &answers, cx.effects.registry)
}

/// The count of a whole key range, which any source but a lightweight relation takes.
///
/// A table is a key range. A record id is one only where its key is a range: a record id that
/// names one record stays on the ordinary plan, which counts the rows the source yields. A source
/// that is evaluated when the statement runs may name anything, and stays too.
fn counted_whole(ir: &Ir, m: &Matched, answers: &TableAnswers) -> Option<Decision> {
	let proven = match &m.source_ref {
		SourceRefShape::Table(_) => true,
		SourceRefShape::RecordId => {
			let operand = *ir.op(m.source)?.operands.first()?;
			matches!(
				ir.op(producer(ir, operand)?)?.kind,
				OpKind::Expr(ExprOp::RecordId(ref attrs)) if matches!(attrs.key_shape, KeyShape::Range(_))
			)
		}
		SourceRefShape::Mock | SourceRefShape::Param | SourceRefShape::Dynamic => false,
	};
	if !proven || answers.lightweight {
		return None;
	}
	let deltas = answers
		.indexes
		.iter()
		.find(|index| matches!(&index.index, Index::Count(None)))
		.map(|index| index.index_id);
	Some(Decision {
		shape: CountShape::All {
			source: m.source_ref.clone(),
			output: m.output.clone(),
		},
		select: answers.select,
		deltas,
		condition: None,
	})
}

/// The count of the records of `table` a condition admits, from an index that answers it.
///
/// The condition is the filter's as written: every conjunct must be one the index analysis reads,
/// since an index that leaves a conjunct to the rows would count records the condition rejects.
fn indexed(
	ir: &Ir,
	m: &Matched,
	filter: OpId,
	table: &surrealdb_strand::TableName,
	answers: &TableAnswers,
	registry: &FunctionRegistry,
) -> Result<Option<Decision>, IrError> {
	if answers.version != ReadVersion::Current || matches!(m.with, Some(With::NoIndex)) {
		return Ok(None);
	}
	let Some(region) = ir.op(filter).and_then(|data| data.regions.first().copied()) else {
		return Ok(None);
	};
	let mut read = Reconstructed::default();
	if !predicate_of_region(ir, region, &mut read)? || !read.withheld.is_empty() {
		return Ok(None);
	}
	let Some(cond) = read.condition() else {
		return Ok(None);
	};
	if answers.restricted.prefixes().cond_touches(&cond) {
		return Ok(None);
	}
	let deltas = answers
		.indexes
		.iter()
		.find(|index| {
			matches!(&index.index, Index::Count(Some(_)))
				&& index
					.count_cond
					.as_ref()
					.is_some_and(|guard| guard_counts(answers.key.as_deref(), &guard.0, &cond))
		})
		.map(|index| index.index_id);
	let btree = match deltas {
		Some(_) => None,
		None => btree_walk(answers, &cond, m.with.as_ref(), registry),
	};
	let bitmap = match (deltas, &btree) {
		(None, None) => bitmap_fusion(answers, &cond, m.with.as_ref(), registry),
		_ => None,
	};
	if deltas.is_none() && btree.is_none() && bitmap.is_none() {
		return Ok(None);
	}
	Ok(Some(Decision {
		shape: CountShape::Indexed {
			table: table.clone(),
			output: m.output.clone(),
			btree: btree.map(|(index, access)| Box::new((index_ref(&index), access))),
			bitmap: bitmap.as_ref().map(|plan| Box::new(bitmap_shape(plan))),
		},
		select: answers.select,
		deltas,
		condition: Some(region),
	}))
}

/// A B-tree index whose walk of keys is the count of `cond`: it covers the whole condition, and
/// every record owns exactly one entry in the range.
///
/// An array value is indexed once per element, so a record holding `[3, 4]` is listed under `3`
/// although `[3, 4] = 3` is false: the walk is chosen only for an index every column of which holds
/// one entry per record, as recorded by the index as storing no array in it. A range
/// widened to the seconds of its bounds counts records its predicate rejects.
fn btree_walk(
	answers: &TableAnswers,
	cond: &LogicalExpr,
	with: Option<&With>,
	registry: &FunctionRegistry,
) -> Option<(AccessIndexRef, BTreeAccess)> {
	if answers.indexes.is_empty() {
		return None;
	}
	let columns = answers.columns.as_ref()?;
	let one_key_per_record =
		|index: &AccessIndexRef| (0..index.cols.len()).all(|at| index.one_entry_per_record(at));
	let analyzer = columns
		.analyzer(with, registry)
		.with_key(answers.key.clone())
		.with_clock(answers.allows_clock);
	for candidate in analyzer.analyze(Some(cond), None) {
		if !one_key_per_record(&candidate.index_ref)
			|| !range_matches_comparison(&candidate.access)
			|| access_fans_out(&candidate.index_ref.cols, &candidate.access)
			|| range_is_widened(&candidate.index_ref, &candidate.access)
		{
			continue;
		}
		// The index consumed the whole condition. A conjunct that reads a component of the record
		// id as `id[i]` is the column the index holds for it.
		if strip_index_conditions_in(
			cond,
			&candidate.access,
			&candidate.index_ref.cols,
			answers.key.as_deref(),
		)
		.is_none()
		{
			return Some((candidate.index_ref.clone(), candidate.access));
		}
	}
	None
}

/// An exact bitmap fusion for `cond`: an AND of index-backed predicates each exactly represented
/// by a bitmap, whose cardinality is the count with no record fetched.
fn bitmap_fusion(
	answers: &TableAnswers,
	cond: &LogicalExpr,
	with: Option<&With>,
	registry: &FunctionRegistry,
) -> Option<crate::exec::index::access_path::BitmapPlan> {
	if answers.indexes.len() < 2 {
		return None;
	}
	let columns = answers.columns.as_ref()?;
	columns
		.analyzer(with, registry)
		.with_key(answers.key.clone())
		.with_clock(answers.allows_clock)
		.try_bitmap_count_fusion(cond, &|index, col| index.one_entry_per_record(col))
}

// ---------------------------------------------------------------------------------------------
// The replacement
// ---------------------------------------------------------------------------------------------

/// Replaces the statement `m` by the count `decision` states; `false` when the count could not be
/// built, which leaves the statement as it was.
///
/// The count records the provenance of every op it replaces. Its regions are copies, so the
/// original ops go with the loop that holds them: the filter's condition, and for a table whose
/// permission is a predicate, the statement itself as the plan beside the count. A copy that reads
/// a value its region does not hold cannot be made, and the statement keeps its ordinary plan.
fn replace(
	ir: &mut Ir,
	facts: &mut FactCache,
	m: &Matched,
	decision: &Decision,
) -> Result<bool, IrError> {
	let before = ir.clone();
	let (Some(aggregate), Some(source)) = (before.op(m.aggregate), before.op(m.source)) else {
		return Ok(false);
	};
	let Some(result) = aggregate.results.first().and_then(|value| before.value(*value)) else {
		return Ok(false);
	};
	let operands: SmallVec<[ValueId; 4]> = match m.source_ref {
		SourceRefShape::Table(_) => SmallVec::new(),
		_ => source.operands.clone(),
	};
	let spec = OpSpec {
		kind: OpKind::Phys(PhysOp::Count(Box::new(CountAttrs {
			shape: decision.shape.clone(),
			select: decision.select,
			deltas: decision.deltas,
		}))),
		operands,
		results: smallvec![result.ty.clone()],
		frame: source.frame,
		snapshot: source.snapshot,
		loc: aggregate.loc.clone(),
	};
	let mut consumed: SmallVec<[OpId; 8]> = smallvec![m.aggregate, m.select_loop, m.source];
	consumed.extend(m.permissions.iter().copied());
	consumed.extend(m.filter);
	let mut rw = Rewriter::new(ir, facts);
	let built = rw.derived(&consumed, |rw| -> Result<Option<OpId>, IrError> {
		let count = rw.create(InsertPoint::Before(m.aggregate), spec)?;
		match copies(rw, &before, m, decision, count) {
			Ok(()) => Ok(Some(count)),
			Err(IrError::Capacity(_) | IrError::Refused(_)) => {
				rw.erase(count, EliminationReason::DeadPure)?;
				Ok(None)
			}
			Err(other) => Err(other),
		}
	})?;
	let Some(count) = built else {
		return Ok(false);
	};
	let produced: SmallVec<[ValueId; 2]> =
		rw.ir().op(count).map(|data| data.results.clone()).ok_or(IrError::Stale("count"))?;
	rw.derived(&consumed, |rw| {
		rw.replace_op(m.aggregate, &produced)?;
		rw.erase(m.select_loop, EliminationReason::AnsweredByAccessPath)?;
		rw.erase(m.source, EliminationReason::AnsweredByAccessPath)
	})?;
	Ok(true)
}

/// Gives `count` the regions its decision states, copied from `before`: the condition of an
/// indexed count, then the statement as the plan beside a count under a conditional permission.
fn copies(
	rw: &mut Rewriter<'_>,
	before: &Ir,
	m: &Matched,
	decision: &Decision,
	count: OpId,
) -> Result<(), IrError> {
	if let Some(condition) = decision.condition {
		rw.clone_region_from(before, condition, count)?;
	}
	if decision.select != CountSelect::Conditional {
		return Ok(());
	}
	let (region, block) = rw.create_region(count, RegionKind::Subquery, &[])?;
	let source = copy_op(rw, before, m.source, block, &[])?;
	let rows = first_result(rw, source)?;
	let select_loop = copy_op(rw, before, m.select_loop, block, &[rows])?;
	let rows = first_result(rw, select_loop)?;
	let aggregate = copy_op(rw, before, m.aggregate, block, &[rows])?;
	let counted = first_result(rw, aggregate)?;
	rw.create(
		InsertPoint::End(block),
		OpSpec {
			kind: OpKind::Expr(ExprOp::Yield),
			operands: smallvec![counted],
			results: SmallVec::new(),
			frame: before.op(m.aggregate).ok_or(IrError::Stale("count"))?.frame,
			snapshot: before.op(m.aggregate).ok_or(IrError::Stale("count"))?.snapshot,
			loc: before.op(m.aggregate).ok_or(IrError::Stale("count"))?.loc.clone(),
		},
	)?;
	for value in rw.ir().region_inputs(region)? {
		if !read_only_as_clock(rw.ir(), region, value)? {
			return Err(IrError::Refused(
				"count: an ordinary plan that reads a value from outside it",
			));
		}
	}
	Ok(())
}

/// Whether `value` is read inside `region` only as the timestamp operand of `expr.now`.
///
/// The count source runs the plan beside it as a unit with no inputs, so a value from outside
/// reads as `NONE`. The statement clock is the one outside value such a unit may read, and only
/// as a timestamp. A plan that reads any other outside value (a record range's bounds, a `LET`
/// the condition reads) cannot stand beside the count, and the statement keeps its ordinary plan.
/// A `value` the container does not hold is a stale id, never a refusal.
fn read_only_as_clock(ir: &Ir, region: RegionId, value: ValueId) -> Result<bool, IrError> {
	let held =
		ir.value(value).ok_or(IrError::Stale("count: a value read from outside the plan"))?;
	let within = |op: OpId| {
		let mut at = ir.enclosing_region(op);
		loop {
			if at == region {
				return true;
			}
			let Some(owner) = ir.parent_op(at) else {
				return false;
			};
			at = ir.enclosing_region(owner);
		}
	};
	Ok(held.uses.iter().filter(|read| within(read.user)).all(|read| {
		read.operand == 0
			&& ir
				.op(read.user)
				.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Now { .. })))
	}))
}

/// The first result of `op`.
fn first_result(rw: &Rewriter<'_>, op: OpId) -> Result<ValueId, IrError> {
	rw.ir()
		.op(op)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("a copied op without a result"))
}

/// A copy of `original`, an op of `before`, at the end of `block`, with `operands` in place of the
/// operands that come from the copied statement itself (an op whose operands are all defined
/// outside it keeps them), and a copy of each of its regions.
fn copy_op(
	rw: &mut Rewriter<'_>,
	before: &Ir,
	original: OpId,
	block: crate::exec::ir::iface::ids::BlockId,
	operands: &[ValueId],
) -> Result<OpId, IrError> {
	let data = before.op(original).ok_or(IrError::Stale("op to copy"))?;
	let results = data
		.results
		.iter()
		.map(|value| {
			before.value(*value).map(|value| value.ty.clone()).ok_or(IrError::Stale("value"))
		})
		.collect::<Result<_, _>>()?;
	let copy = rw.create(
		InsertPoint::End(block),
		OpSpec {
			kind: data.kind.clone(),
			operands: if operands.is_empty() {
				data.operands.clone()
			} else {
				operands.iter().copied().collect()
			},
			results,
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
	)?;
	for region in &data.regions {
		rw.clone_region_from(before, *region, copy)?;
	}
	Ok(copy)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;

	/// A stale value among the plan's outside reads answers `Stale`, which the rewrite raises,
	/// rather than the refusal that keeps the ordinary plan.
	#[test]
	fn a_stale_outside_read_is_stale_not_refused() {
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		assert!(matches!(
			read_only_as_clock(&ir, ir.top(), ValueId::default()),
			Err(IrError::Stale(_))
		));
	}
}
