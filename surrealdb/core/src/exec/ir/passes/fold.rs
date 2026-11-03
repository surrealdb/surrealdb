//! Constant folding at the decisions that consume a constant, the collapse of an `IF` whose
//! condition is a literal, the algebraic rules, and the erasure of conversions a type fact makes
//! redundant.
//!
//! A value is folded only where a decision consumes it: the condition of an `IF`, a conjunct of a
//! filter or permission predicate, the iterable of a loop that may be unrolled, or the call a
//! source reads its rows from, which names a table when it folds to one. A folded
//! constant replaces the decision it feeds, after the observer has been told which value the
//! decision consumed. A fold that raises is never applied: the op stays and raises at run time
//! where it stands. A value the folder refuses stays as it is.
//!
//! An `IF` arm is collapsed only when its block ends in a yield and no `RETURN`, `BREAK` or
//! `CONTINUE` inside it lands on the arm or beyond it, because a value arm catches a `RETURN` and
//! inlining it would move that landing. An arm that holds a write is not collapsed either: the
//! computation depth the write's events resume from counts the arms that enclose it. A collapse
//! the rewriter refuses is no rewrite.
#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use super::dce::layer_column_names;
use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::rehome::{and_chain, rebuild_and_chain};
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::{effects, fallibility, types};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, Limit, Pattern, PatternSet, apply_greedy};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::{Use, ValueDef};
use crate::exec::ir::core::walk::{ControlTarget, ancestors, control_target};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, KeyShape, RecordIdAttrs, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::{EliminationReason, ExprDialect};
use crate::exec::ir::iface::fold::{FoldConsumer, FoldOutcome, FoldRequest};
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LoopKind, SourceAttrs, SourceRefShape};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::{BinaryOperator, Kind, PrefixOperator};
use crate::val::{RecordIdKey, Value};

/// How many times a sweep is repeated while it keeps rewriting: a collapse can expose a value the
/// next sweep folds.
const MAX_ROUNDS: u8 = GREEDY_DEFAULT.max_passes;

/// How far a value's consumer is followed through short-circuit operators when deciding whether
/// only its truthiness is read.
const MAX_CONSUMER_HOPS: u8 = 4;

/// Folds the values that decisions consume and applies the decisions.
///
/// Each round sweeps the container for decision sites without mutating it, asks the folder for
/// the value each site consumes, applies the outcomes in program order, and runs the algebraic and
/// fact rules to a fixed point. The pass ends when a round rewrites nothing or after
/// [`MAX_ROUNDS`] rounds, which it reports as a stop at the pass bound.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct FoldPass;

impl LoPass for FoldPass {
	fn name(&self) -> &'static str {
		"fold"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.fold
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::Pending(Box::pin(run_fold(cx)))
	}
}

/// The decision sites of a container, in program order.
#[derive(Default)]
struct Sites {
	/// `logical.if` and `expr.if` ops.
	ifs: Vec<OpId>,
	/// A filter or permission op with the region that holds its predicate.
	clauses: Vec<(OpId, RegionId)>,
	/// The iterable of each loop an unroll may consume.
	iterables: Vec<ValueId>,
	/// Each unlocked source whose operand is a call, which names a table when the call folds to
	/// one.
	sources: Vec<OpId>,
	/// Each source read from an expression whose value is a record id literal, which reads that
	/// record.
	record_sources: Vec<OpId>,
	/// Each broadcast loop.
	broadcasts: Vec<OpId>,
	/// The `WHERE` of each write, whose comparisons an access path serves the scan from.
	write_conditions: Vec<RegionId>,
	/// The operands of the comparisons of those predicates, which an access path reads as
	/// literals when they fold to one.
	operands: Vec<ValueId>,
}

async fn run_fold(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let rules = rule_set();
	let mut report = PassReport::default();
	// The filters a plan-time fold decided a conjunct of, which stay filters.
	let mut folded: FxHashSet<OpId> = FxHashSet::default();
	for round in 0..MAX_ROUNDS {
		let sites = sweep(cx.ir, cx.cfg.unroll);
		fold_requests(cx, requests(cx.ir, &sites)).await;
		let mut applied = apply_ifs(cx, &sites.ifs)?;
		applied += apply_clauses(cx, &sites.clauses, &mut folded)?;
		applied += apply_operands(cx, &sites.operands)?;
		applied += apply_sources(cx, &sites.sources)?;
		applied += apply_record_sources(cx, &sites.record_sources)?;
		applied += apply_broadcasts(cx, &sites.broadcasts)?;
		let greedy =
			apply_greedy(cx.ir, cx.facts, &rules, cx.folder, cx.effects.registry, GREEDY_DEFAULT)
				.await?;
		applied += greedy.rewrites;
		report.rewrites = report.rewrites.saturating_add(applied);
		if greedy.stopped_by.is_some() {
			report.stopped_by = greedy.stopped_by;
			return Ok(report);
		}
		if applied == 0 {
			return Ok(report);
		}
		if round + 1 == MAX_ROUNDS {
			report.stopped_by = Some(Limit::Passes);
		}
	}
	Ok(report)
}

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Collects the decision sites outside stored clauses. `unroll` adds the iterables of loops an
/// unroll consumes.
fn sweep(ir: &Ir, unroll: bool) -> Sites {
	let mut sites = Sites::default();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		for region in &data.regions {
			if ir
				.region(*region)
				.is_some_and(|held| matches!(held.kind, RegionKind::Clause(ClauseRole::WriteCond)))
				&& !in_stored_clause(ir, op)
			{
				sites.write_conditions.push(*region);
			}
		}
		if matches!(data.kind, OpKind::Logical(LogOp::If) | OpKind::Expr(ExprOp::If)) {
			if !in_stored_clause(ir, op) {
				sites.ifs.push(op);
			}
		} else if matches!(data.kind, OpKind::Logical(LogOp::Filter)) {
			if !in_stored_clause(ir, op) {
				for region in &data.regions {
					let is_where = ir.region(*region).is_some_and(|held| {
						matches!(held.kind, RegionKind::Clause(ClauseRole::Where))
					});
					if is_where {
						sites.clauses.push((op, *region));
					}
				}
			}
		} else if matches!(data.kind, OpKind::Logical(LogOp::PermissionFilter { .. })) {
			if !in_stored_clause(ir, op) {
				for region in &data.regions {
					let is_predicate = ir
						.region(*region)
						.is_some_and(|held| matches!(held.kind, RegionKind::PermissionFrame));
					if is_predicate {
						sites.clauses.push((op, *region));
					}
				}
			}
		} else if let OpKind::Logical(LogOp::Source(attrs)) = &data.kind
			&& matches!(attrs.source_ref, SourceRefShape::Dynamic)
			&& !attrs.for_update
			&& data.operands.first().is_some_and(|operand| call_of(ir, *operand))
			&& !in_stored_clause(ir, op)
		{
			sites.sources.push(op);
		} else if let OpKind::Logical(LogOp::Source(attrs)) = &data.kind
			&& matches!(attrs.source_ref, SourceRefShape::Dynamic)
			&& data.operands.first().is_some_and(|operand| {
				single_result_def(ir, *operand)
					.and_then(|def| ir.op(def))
					.is_some_and(|def| matches!(def.kind, OpKind::Expr(ExprOp::RecordId(_))))
			}) && !in_stored_clause(ir, op)
		{
			sites.record_sources.push(op);
		} else if matches!(
			data.kind,
			OpKind::Logical(LogOp::Loop {
				kind: LoopKind::Broadcast,
				..
			})
		) && !in_stored_clause(ir, op)
		{
			sites.broadcasts.push(op);
		} else if let OpKind::Logical(LogOp::Loop {
			kind,
			..
		}) = &data.kind
			&& unroll && matches!(
			kind,
			LoopKind::For | LoopKind::Closure | LoopKind::ElementFilter
		) && !in_stored_clause(ir, op)
			&& let Some(source) = data.operands.first()
		{
			sites.iterables.push(*source);
		}
		Walk::Continue
	});
	let regions: Vec<RegionId> = sites
		.clauses
		.iter()
		.map(|(_, region)| *region)
		.chain(sites.write_conditions.iter().copied())
		.collect();
	sites.operands = comparison_operands(ir, &regions);
	sites
}

/// Whether `operator` compares its operands: a value an access path can serve a scan from.
fn is_comparison(operator: &BinaryOperator) -> bool {
	matches!(
		operator,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::Inside
			| BinaryOperator::NotInside
			| BinaryOperator::Contain
			| BinaryOperator::NotContain
			| BinaryOperator::ContainAll
			| BinaryOperator::ContainAny
			| BinaryOperator::ContainNone
			| BinaryOperator::AllInside
			| BinaryOperator::AnyInside
			| BinaryOperator::Intersects
			| BinaryOperator::Matches(_)
	)
}

/// Whether `value` is a range built from its bounds, which an access path reads as the bounds it
/// was written with.
fn is_range_constructor(ir: &Ir, value: ValueId) -> bool {
	single_result_def(ir, value).and_then(|def| ir.op(def)).is_some_and(|data| {
		matches!(
			&data.kind,
			OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::Range
					| BinaryOperator::RangeInclusive
					| BinaryOperator::RangeSkip
					| BinaryOperator::RangeSkipInclusive,
			})
		) || matches!(
			&data.kind,
			OpKind::Expr(ExprOp::Prefix(prefix))
				if matches!(**prefix, PrefixOperator::Range | PrefixOperator::RangeInclusive)
		)
	})
}

/// Push `operand` into `operands` if it is a non-literal single-result value that an access path
/// may read as a literal.
fn push_comparison_operand(ir: &Ir, operand: ValueId, operands: &mut Vec<ValueId>) {
	if literal_of(ir, operand).is_none()
		&& single_result_def(ir, operand).is_some()
		&& !is_range_constructor(ir, operand)
		&& !operands.contains(&operand)
	{
		operands.push(operand);
	}
}

/// The operands, not written as literals, of the comparisons in the predicates of `regions`.
fn comparison_operands(ir: &Ir, regions: &[RegionId]) -> Vec<ValueId> {
	let mut operands: Vec<ValueId> = Vec::new();
	for region in regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			if let Some(data) = ir.op(op)
				&& let OpKind::Expr(ExprOp::Binary {
					op: operator,
				}) = &data.kind
				&& is_comparison(operator)
			{
				for operand in &data.operands {
					push_comparison_operand(ir, *operand, &mut operands);
					if let Some(def) = single_result_def(ir, *operand)
						&& let Some(call_data) = ir.op(def)
						&& let OpKind::Expr(ExprOp::Call(attrs)) = &call_data.kind
						&& matches!(
							&attrs.function,
							crate::expr::Function::Normal(name)
								if name.eq_ignore_ascii_case("geo::distance")
						) {
						for arg in &call_data.operands {
							push_comparison_operand(ir, *arg, &mut operands);
						}
					}
				}
			}
			Walk::Continue
		});
	}
	operands
}

/// The values the sweep asks the folder about, with the decision that consumes each.
///
/// A conjunct that something other than its chain reads is not asked about: replacing it would
/// change what that reader sees.
fn requests(ir: &Ir, sites: &Sites) -> Vec<(ValueId, FoldConsumer)> {
	let mut out = Vec::new();
	for op in &sites.ifs {
		if let Some(cond) = ir.op(*op).and_then(|data| data.operands.first()) {
			out.push((*cond, FoldConsumer::BranchChoice));
		}
	}
	for (_, region) in &sites.clauses {
		let Some((_, cond)) = clause_yield(ir, *region) else {
			continue;
		};
		for conjunct in and_chain(ir, cond) {
			if ir.uses(conjunct).len() == 1 {
				out.push((conjunct, FoldConsumer::BranchChoice));
			}
		}
	}
	for operand in &sites.operands {
		out.push((*operand, FoldConsumer::IndexChoice));
	}
	for source in &sites.iterables {
		out.push((*source, FoldConsumer::Unroll));
	}
	for source in &sites.sources {
		if let Some(operand) = ir.op(*source).and_then(|data| data.operands.first()) {
			out.push((*operand, FoldConsumer::DynamicTargetTable));
		}
	}
	out
}

/// Whether `value` is the result of a call of a function by name.
fn call_of(ir: &Ir, value: ValueId) -> bool {
	single_result_def(ir, value)
		.and_then(|op| ir.op(op))
		.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::Call(_))))
}

/// Replaces each source whose call folded to a table by a source naming that table, and answers
/// the rewrites made.
///
/// The table source reads the table as a literal name does: it takes the access path, ordering and
/// paging of a table read. It is not resolved, so its read enforces what the table declares when
/// it runs. A call that folds to any other value stays a source its call names when the statement
/// runs.
fn apply_sources(cx: &mut LoCx<'_>, sources: &[OpId]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for source in sources {
		let Some(data) = cx.ir.op(*source) else {
			continue;
		};
		let (OpKind::Logical(LogOp::Source(attrs)), [operand], [rows]) =
			(&data.kind, data.operands.as_slice(), data.results.as_slice())
		else {
			continue;
		};
		let (operand, rows) = (*operand, *rows);
		let Some(FoldOutcome::Constant {
			value: Value::Table(table),
			..
		}) = cx.facts.fold_outcome(operand)
		else {
			continue;
		};
		let kind = OpKind::Logical(LogOp::Source(Box::new(SourceAttrs {
			source_ref: SourceRefShape::Table(table.clone()),
			with: attrs.with.clone(),
			for_update: false,
			may_be_defined: attrs.may_be_defined,
			may_meet_change: attrs.may_meet_change,
			resolved: false,
		})));
		let spec = OpSpec {
			kind,
			operands: SmallVec::new(),
			results: smallvec![TypeKnowledge::Unknown],
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		};
		cx.observer.consumed(&*cx.ir, operand, FoldConsumer::DynamicTargetTable);
		let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
		let named = rw.derived(&[*source], |rw| rw.create(InsertPoint::Before(*source), spec))?;
		let named = rw
			.ir()
			.op(named)
			.and_then(|made| made.results.first().copied())
			.ok_or(IrError::Stale("op"))?;
		rw.replace_all_uses(rows, named)?;
		rw.erase(*source, EliminationReason::FoldedConstant)?;
		if rw.ir().uses(operand).is_empty()
			&& let Some(call) = single_result_def(rw.ir(), operand)
		{
			rw.erase(call, EliminationReason::FoldedConstant)?;
		}
		rewrites += 1;
	}
	Ok(rewrites)
}

/// Reads each of `sources`, a source of an expression that is a record id literal, as the source
/// of that record, and answers the rewrites made.
fn apply_record_sources(cx: &mut LoCx<'_>, sources: &[OpId]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	for source in sources {
		if rw.ir().op(*source).is_none() {
			continue;
		}
		rw.update_kind(*source, |kind| {
			if let OpKind::Logical(LogOp::Source(attrs)) = kind {
				attrs.source_ref = SourceRefShape::RecordId;
			}
		})?;
		rewrites += 1;
	}
	Ok(rewrites)
}

/// The block arguments of the body of a broadcast: the element and its position.
const BROADCAST_BODY_ARGS: usize = 2;

/// Replaces each broadcast over a value that is known not to be a collection by its body, and
/// answers the rewrites made.
///
/// A broadcast applies its body to each element of an array and to the value itself otherwise.
/// Where the type facts say the base is no collection (not `any`, an array, a set, an `either` or a
/// literal kind), the body runs once with the base as its element. The body reads the element only
/// through the loop's block argument, so binding that argument to the base keeps every read it
/// makes.
fn apply_broadcasts(cx: &mut LoCx<'_>, broadcasts: &[OpId]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for op in broadcasts {
		if cx.ir.op(*op).is_some() {
			rewrites += resolve_broadcast(cx, *op)?;
		}
	}
	Ok(rewrites)
}

/// Replaces the broadcast `op` by its body when its base is no collection, and answers `1` when it
/// did.
fn resolve_broadcast(cx: &mut LoCx<'_>, op: OpId) -> Result<u32, IrError> {
	let Some(data) = cx.ir.op(op) else {
		return Ok(0);
	};
	let ([base], [body], [result]) =
		(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
	else {
		return Ok(0);
	};
	let (base, body, result) = (*base, *body, *result);
	if !is_no_collection(cx.ir, cx.facts, cx.effects, base)
		|| !collapsible(cx.ir, body, BROADCAST_BODY_ARGS, 1)
	{
		return Ok(0);
	}
	// The body's second argument is the element's position, which only a body that numbers its
	// elements reads.
	let position_unread = cx
		.ir
		.region(body)
		.and_then(|held| held.blocks.first())
		.and_then(|block| cx.ir.block(*block))
		.is_some_and(|block| block.args.iter().skip(1).all(|arg| cx.ir.uses(*arg).is_empty()));
	if !position_unread {
		return Ok(0);
	}
	let body_ops: Vec<OpId> = cx
		.ir
		.region(body)
		.and_then(|held| held.blocks.first())
		.and_then(|block| cx.ir.block(*block))
		.map(|block| block.ops.clone())
		.unwrap_or_default();
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let Ok(yielded) = rw.inline_region(body, InsertPoint::Before(op), &[base; BROADCAST_BODY_ARGS])
	else {
		return Ok(0);
	};
	let [value] = yielded.as_slice() else {
		return Ok(0);
	};
	rw.replace_all_uses(result, *value)?;
	// The value stands for the expression the broadcast was written as, so its op keeps that
	// expression's text.
	if let Some(def) = single_result_def(rw.ir(), *value)
		&& body_ops.contains(&def)
		&& let Some(loc) = rw.ir().op(op).map(|data| data.loc.clone())
	{
		rw.set_location(def, loc)?;
	}
	rw.erase(op, EliminationReason::FoldedConstant)?;
	Ok(1)
}

/// Whether `value` is known not to be a collection: its type fact is a kind no collection has, or
/// it is a record id or a compound literal that is no array or set, whose kind the type analysis
/// does not state.
fn is_no_collection(
	ir: &Ir,
	facts: &mut FactCache,
	effects: &EffectCx<'_>,
	value: ValueId,
) -> bool {
	if let TypeKnowledge::Fact {
		kind,
		..
	} = types::classify_with(ir, facts, effects, value)
	{
		return is_scalar_kind(&kind);
	}
	if let Some(held) = literal_of(ir, value) {
		return matches!(
			held,
			Value::Object(_)
				| Value::Geometry(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::File(_)
				| Value::Range(_)
		);
	}
	single_result_def(ir, value)
		.and_then(|def| ir.op(def))
		.is_some_and(|data| matches!(data.kind, OpKind::Expr(ExprOp::RecordId(_))))
}

/// Whether every value of `kind` is a single value a broadcast does not map over.
fn is_scalar_kind(kind: &Kind) -> bool {
	match kind {
		Kind::Any
		| Kind::Named(_)
		| Kind::Array(..)
		| Kind::Set(..)
		| Kind::Either(_)
		| Kind::Literal(_) => false,
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Enum(_) => true,
	}
}

/// Asks the folder for the values of `wanted` in one batch and stores each answer. A literal
/// needs no fold, a value that already holds an outcome is not asked again, and a value requested
/// twice is asked once, for the consumer that requested it first.
async fn fold_requests(cx: &mut LoCx<'_>, wanted: Vec<(ValueId, FoldConsumer)>) {
	let mut batch: Vec<FoldRequest> = Vec::new();
	let mut held: FxHashSet<ValueId> = FxHashSet::default();
	for (value, consumer) in wanted {
		if cx.ir.value(value).is_none()
			|| literal_of(cx.ir, value).is_some()
			|| cx.facts.fold_outcome(value).is_some()
			|| !held.insert(value)
		{
			continue;
		}
		batch.push(FoldRequest {
			value,
			consumer,
		});
	}
	if batch.is_empty() {
		return;
	}
	let outcomes = cx.folder.fold_all(&*cx.ir, cx.effects, &batch).await;
	for (request, outcome) in batch.iter().zip(outcomes) {
		cx.facts.store_fold(request.value, outcome);
	}
}

/// The literal `value` is defined by, if it is the result of an `expr.literal`.
fn literal_of(ir: &Ir, value: ValueId) -> Option<&Value> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let OpKind::Expr(ExprOp::Literal(held)) = &ir.op(op)?.kind else {
		return None;
	};
	Some(&**held)
}

/// The yield that closes the one block of `region` and the one value it yields.
fn clause_yield(ir: &Ir, region: RegionId) -> Option<(OpId, ValueId)> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	let last = *ir.block(*block)?.ops.last()?;
	let data = ir.op(last)?;
	let OpKind::Expr(ExprOp::Yield) = &data.kind else {
		return None;
	};
	let [value] = data.operands.as_slice() else {
		return None;
	};
	Some((last, *value))
}

/// Applies the outcome of each `IF` condition, in program order, and answers the rewrites made.
fn apply_ifs(cx: &mut LoCx<'_>, ifs: &[OpId]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for op in ifs {
		if cx.ir.op(*op).is_some() {
			rewrites += collapse_if(cx, *op)?;
		}
	}
	Ok(rewrites)
}

/// Replaces `op`, an `IF`, by the arm its condition selects, and answers `1`, or `0` when nothing
/// is decided, the arm may not be collapsed, or the rewriter refuses the inlining.
///
/// An `IF` standing as a value is replaced by the ops of the arm. An `IF` standing as a statement
/// ([`is_statement_if`]) is replaced by what the binder emits for the statement the arm is: the
/// arm as a block of its own, or, when no arm is selected and there is no `ELSE`, an `eval` of
/// `NONE`.
///
/// A literal condition decides without a fold and without telling the observer. A folded constant
/// decides by its truthiness and is reported to the observer once the arm is inlined and before
/// the `IF` and the untaken arm are erased.
fn collapse_if(cx: &mut LoCx<'_>, op: OpId) -> Result<u32, IrError> {
	let Some(data) = cx.ir.op(op) else {
		return Ok(0);
	};
	let (Some(&cond), [then, otherwise]) = (data.operands.first(), data.regions.as_slice()) else {
		return Ok(0);
	};
	let (then, otherwise) = (*then, *otherwise);
	let results = data.results.clone();
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let (truth, folded) = if let Some(literal) = literal_of(cx.ir, cond) {
		(literal.is_truthy(), false)
	} else if let Some(FoldOutcome::Constant {
		value,
		..
	}) = cx.facts.fold_outcome(cond)
	{
		(value.is_truthy(), true)
	} else {
		return Ok(0);
	};
	let taken = if truth {
		then
	} else {
		otherwise
	};
	if !collapsible(cx.ir, taken, 0, results.len()) {
		return Ok(0);
	}
	let statement = is_statement_if(cx.ir, op, then) && results.len() == 1;
	let bare_none = statement && is_bare_none(cx.ir, taken);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let template = |kind: OpKind, operands: SmallVec<[ValueId; 4]>, results: usize| OpSpec {
		kind,
		operands,
		results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
		frame,
		snapshot,
		loc: loc.clone(),
	};
	let replacement = if bare_none {
		let Ok(yielded) = rw.inline_region(taken, InsertPoint::Before(op), &[]) else {
			return Ok(0);
		};
		let eval = rw.derived(&[op], |rw| {
			rw.create(
				InsertPoint::Before(op),
				template(OpKind::Logical(LogOp::Eval), yielded.iter().copied().collect(), 1),
			)
		})?;
		single_result(rw.ir(), eval)?
	} else if statement {
		let block = rw.derived(&[op], |rw| {
			rw.create(
				InsertPoint::Before(op),
				template(OpKind::Logical(LogOp::Block), SmallVec::new(), 1),
			)
		})?;
		let (_, body) = rw.create_region(block, RegionKind::Block, &[])?;
		let yielded = rw.inline_region(taken, InsertPoint::End(body), &[])?;
		rw.derived(&[op], |rw| {
			rw.create(
				InsertPoint::End(body),
				template(OpKind::Logical(LogOp::Yield), yielded.iter().copied().collect(), 0),
			)
		})?;
		single_result(rw.ir(), block)?
	} else {
		let Ok(yielded) = rw.inline_region(taken, InsertPoint::Before(op), &[]) else {
			return Ok(0);
		};
		if folded {
			cx.observer.consumed(rw.ir(), cond, FoldConsumer::BranchChoice);
		}
		for (result, value) in results.iter().zip(&yielded) {
			rw.replace_all_uses(*result, *value)?;
		}
		rw.erase(op, EliminationReason::FoldedConstant)?;
		return Ok(1);
	};
	if folded {
		cx.observer.consumed(rw.ir(), cond, FoldConsumer::BranchChoice);
	}
	for result in &results {
		rw.replace_all_uses(*result, replacement)?;
	}
	rw.erase(op, EliminationReason::FoldedConstant)?;
	Ok(1)
}

/// The only result of `op`.
fn single_result(ir: &Ir, op: OpId) -> Result<ValueId, IrError> {
	match ir.op(op).map(|data| data.results.as_slice()) {
		Some([result]) => Ok(*result),
		Some(_) | None => Err(IrError::Stale("op")),
	}
}

/// Whether the `IF` `op`, whose first arm is `then`, is a statement of a statement list: its arms
/// are bound as statement lists, and it is not the continuation of an `ELSE IF` chain, which stands
/// alone in the `ELSE` arm of the `IF` before it.
fn is_statement_if(ir: &Ir, op: OpId, then: RegionId) -> bool {
	if !ir.region(then).is_some_and(|held| held.kind == RegionKind::StatementIf) {
		return false;
	}
	let Some(parent) = ir.op(op).and_then(|data| ir.block(data.parent)).map(|block| block.parent)
	else {
		return false;
	};
	let Some(owner) = ir.parent_op(parent) else {
		return true;
	};
	let continues_chain = ir.op(owner).is_some_and(|data| {
		matches!(data.kind, OpKind::Logical(LogOp::If))
			&& data.regions.get(1).is_some_and(|held| *held == parent)
	});
	!continues_chain
}

/// Whether the arm `region` is the bare `NONE` the binder gives an `IF` with no `ELSE`: one
/// literal and the yield of it.
fn is_bare_none(ir: &Ir, region: RegionId) -> bool {
	let Some([block]) = ir.region(region).map(|held| held.blocks.as_slice()) else {
		return false;
	};
	ir.block(*block).is_some_and(
		|held| matches!(held.ops.as_slice(), [lit, last] if bare_none_pair(ir, *lit, *last)),
	)
}

/// Whether `lit` is a literal `NONE` and `last` a logical yield of it.
fn bare_none_pair(ir: &Ir, lit: OpId, last: OpId) -> bool {
	let (Some(lit), Some(last)) = (ir.op(lit), ir.op(last)) else {
		return false;
	};
	matches!(&lit.kind, OpKind::Expr(ExprOp::Literal(held)) if matches!(**held, Value::None))
		&& matches!(last.kind, OpKind::Logical(LogOp::Yield))
		&& last.operands.as_slice() == lit.results.as_slice()
}

/// Whether the region `region`, an arm of an `IF` or the body of a loop, may be inlined into the
/// block of its owner: it is one block of `args` arguments that ends in a yield of `arity` values,
/// no `RETURN`, `BREAK` or `CONTINUE` inside it lands on the arm or beyond it, and it holds no
/// write, whose events spend the computation depth of the arms around it.
fn collapsible(ir: &Ir, region: RegionId, args: usize, arity: usize) -> bool {
	let Some(held) = ir.region(region) else {
		return false;
	};
	let Some(owner) = held.parent else {
		return false;
	};
	let [block] = held.blocks.as_slice() else {
		return false;
	};
	let Some(block) = ir.block(*block) else {
		return false;
	};
	let Some(last) = block.ops.last().and_then(|op| ir.op(*op)) else {
		return false;
	};
	let yields = matches!(last.kind, OpKind::Expr(ExprOp::Yield) | OpKind::Logical(LogOp::Yield));
	if block.args.len() != args
		|| !yields
		|| last.operands.len() != arity
		|| last.results.iter().any(|result| !ir.uses(*result).is_empty())
		|| !tokens_collapsible(ir, owner, &block.token_args)
	{
		return false;
	}
	let mut stays = true;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		let writes = ir.op(op).is_some_and(|data| effects::is_write(&data.kind));
		if !writes && control_stays_inside(ir, op, region) {
			Walk::Continue
		} else {
			stays = false;
			Walk::Stop
		}
	});
	stays
}

/// Whether every token argument of `token_args` that is read can be replaced by a token of its
/// class when inlining before `owner`.
fn tokens_collapsible(ir: &Ir, owner: OpId, token_args: &[ValueId]) -> bool {
	let follows = ir.op(owner).is_some_and(|data| effects::follows_effect(&data.kind));
	token_args.iter().all(|formal| {
		if ir.uses(*formal).is_empty() {
			return true;
		}
		let Some(class) = ir.token_class(*formal) else {
			return false;
		};
		if follows && ir.token_of_class(ir.token_results(owner), class).is_some() {
			return false;
		}
		ir.token_of_class(ir.token_operands(owner), class).is_some()
	})
}

/// Whether the control-flow op `op`, inside `arm`, lands strictly inside `arm`. An op that is not
/// control flow stays.
fn control_stays_inside(ir: &Ir, op: OpId, arm: RegionId) -> bool {
	match control_target(ir, op) {
		ControlTarget::NotControl => true,
		ControlTarget::Region(target) => {
			for (region, _) in ancestors(ir, op) {
				if region == arm {
					return false;
				}
				if region == target {
					return true;
				}
			}
			false
		}
		ControlTarget::Escapes(_) | ControlTarget::Barrier(_) => false,
	}
}

/// Applies the folded conjuncts of each filter and permission predicate and answers the rewrites
/// made.
fn apply_clauses(
	cx: &mut LoCx<'_>,
	clauses: &[(OpId, RegionId)],
	folded: &mut FxHashSet<OpId>,
) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for (op, region) in clauses {
		if cx.ir.op(*op).is_some() && cx.ir.region(*region).is_some() {
			let (decided, dropped) = fold_conjuncts(cx, *region)?;
			if decided > 0 {
				folded.insert(*op);
			}
			rewrites += decided + dropped;
			if !folded.contains(op) {
				rewrites += erase_true_filter(cx, *op, *region)?;
			}
		}
	}
	Ok(rewrites)
}

/// Erases the `WHERE` filter `op` when every conjunct of its predicate in `region` is the literal
/// `true`, pointing its readers at its input, and answers `1` when it went.
///
/// Such a filter admits every row. A filter that yields `false` stays: its rows are all hidden,
/// which the access path plans. The caller leaves a filter whose conjunct a plan-time fold decided,
/// which the physical plan keeps as a predicate evaluated once.
fn erase_true_filter(cx: &mut LoCx<'_>, op: OpId, region: RegionId) -> Result<u32, IrError> {
	let Some(data) = cx.ir.op(op) else {
		return Ok(0);
	};
	let (OpKind::Logical(LogOp::Filter), [input], [result], [_]) =
		(&data.kind, data.operands.as_slice(), data.results.as_slice(), data.regions.as_slice())
	else {
		return Ok(0);
	};
	let (input, result) = (*input, *result);
	let Some((_, cond)) = clause_yield(cx.ir, region) else {
		return Ok(0);
	};
	let admits_every_row = and_chain(cx.ir, cond)
		.iter()
		.all(|conjunct| matches!(literal_of(cx.ir, *conjunct), Some(Value::Bool(true))));
	if !admits_every_row {
		return Ok(0);
	}
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	rw.replace_all_uses(result, input)?;
	match rw.erase(op, EliminationReason::FoldedConstant) {
		Ok(()) => Ok(1),
		Err(IrError::HasUses(_)) => Ok(0),
		Err(error) => Err(error),
	}
}

/// The `expr.record_id` op that stands for `value` when it is a record id whose key is a number,
/// a string or a uuid: the form an access path reads a record id as, which a literal of the same
/// value is not.
fn record_id_op(value: &Value) -> Option<ExprOp> {
	let Value::RecordId(id) = value else {
		return None;
	};
	let key_shape = match &id.key {
		RecordIdKey::Number(number) => KeyShape::Number(*number),
		RecordIdKey::String(text) => KeyShape::String(text.clone()),
		RecordIdKey::Uuid(uuid) => KeyShape::Uuid(*uuid),
		RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => return None,
	};
	Some(ExprOp::RecordId(Box::new(RecordIdAttrs {
		table: id.table.clone(),
		key_shape,
	})))
}

/// Whether `def` is already the operand-free record-id op `kind`: replacing it would make the same
/// op again.
fn already_built_as(ir: &Ir, def: OpId, kind: &ExprOp) -> bool {
	let ExprOp::RecordId(wanted) = kind else {
		return false;
	};
	ir.op(def).is_some_and(|data| {
		data.operands.is_empty()
			&& data.regions.is_empty()
			&& matches!(&data.kind, OpKind::Expr(ExprOp::RecordId(held)) if held == wanted)
	})
}

/// Replaces each comparison operand that folded to a constant by a literal of it, and answers the
/// rewrites made. The operand keeps the text it was written as. A fold that raised is not applied.
fn apply_operands(cx: &mut LoCx<'_>, operands: &[ValueId]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for operand in operands {
		if cx.ir.value(*operand).is_none() || literal_of(cx.ir, *operand).is_some() {
			continue;
		}
		let Some(FoldOutcome::Constant {
			value,
			..
		}) = cx.facts.fold_outcome(*operand).cloned()
		else {
			continue;
		};
		let Some(def) = single_result_def(cx.ir, *operand) else {
			continue;
		};
		let replacement = record_id_op(&value);
		if replacement.as_ref().is_some_and(|kind| already_built_as(cx.ir, def, kind)) {
			continue;
		}
		cx.observer.consumed(&*cx.ir, *operand, FoldConsumer::IndexChoice);
		let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
		match replacement {
			Some(kind) => replace_with_expr(&mut rw, def, kind)?,
			None => replace_with_literal(&mut rw, def, value)?,
		};
		rewrites += 1;
	}
	Ok(rewrites)
}

/// Replaces each conjunct of the predicate in `region` that folded to a constant by the literal of
/// its truthiness, then removes the conjuncts that are the literal `true`, written or folded, from
/// the `AND` chain.
///
/// A chain whose every conjunct is `true` keeps its first one, so the predicate is still a value.
/// A conjunct that folded to `false` stays as a literal `false`.
///
/// Answers the conjuncts folded and the chain rewrites made. A written `true` is dropped without
/// being a fold: the filter it stands in is not one a plan-time decision settled.
fn fold_conjuncts(cx: &mut LoCx<'_>, region: RegionId) -> Result<(u32, u32), IrError> {
	let Some((_, cond)) = clause_yield(cx.ir, region) else {
		return Ok((0, 0));
	};
	let mut made_true: SmallVec<[ValueId; 2]> = SmallVec::new();
	let mut folded = 0;
	for conjunct in and_chain(cx.ir, cond) {
		if matches!(literal_of(cx.ir, conjunct), Some(Value::Bool(true))) {
			made_true.push(conjunct);
			continue;
		}
		if literal_of(cx.ir, conjunct).is_some() || cx.ir.uses(conjunct).len() != 1 {
			continue;
		}
		let Some(FoldOutcome::Constant {
			value,
			..
		}) = cx.facts.fold_outcome(conjunct)
		else {
			continue;
		};
		let truth = value.is_truthy();
		let Some(def) = single_result_def(cx.ir, conjunct) else {
			continue;
		};
		cx.observer.consumed(&*cx.ir, conjunct, FoldConsumer::BranchChoice);
		let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
		let literal = replace_with_literal(&mut rw, def, Value::Bool(truth))?;
		if truth {
			made_true.push(literal);
		}
		folded += 1;
	}
	let dropped = if made_true.is_empty() {
		0
	} else {
		drop_true_conjuncts(cx, region, &made_true)?
	};
	Ok((folded, dropped))
}

/// The op that defines `value` when it is that op's only result.
fn single_result_def(ir: &Ir, value: ValueId) -> Option<OpId> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	(ir.op(op)?.results.len() == 1).then_some(op)
}

/// Replaces the single-result op `def` by an `expr.literal` of `value` made beside it, and answers
/// the literal's result.
fn replace_with_literal(
	rw: &mut Rewriter<'_>,
	def: OpId,
	value: Value,
) -> Result<ValueId, IrError> {
	replace_with_expr(rw, def, ExprOp::Literal(Box::new(value)))
}

/// Replaces the single-result op `def` by an `expr.literal` of `value` that an algebraic rule
/// decided, and answers the literal's result. The literal carries no text of the expression it
/// replaces: it is the constant the rule's decision is, shown and judged as one.
fn replace_with_decided(
	rw: &mut Rewriter<'_>,
	def: OpId,
	value: Value,
) -> Result<ValueId, IrError> {
	let mut loc = rw.ir().op(def).ok_or(IrError::Stale("op"))?.loc.clone();
	if matches!(loc.origin, LocOrigin::Written(_)) {
		loc.origin = LocOrigin::Query;
	}
	replace_with(rw, def, ExprOp::Literal(Box::new(value)), loc)
}

/// Replaces the single-result op `def` by the op `kind` made beside it, and answers its result.
fn replace_with_expr(rw: &mut Rewriter<'_>, def: OpId, kind: ExprOp) -> Result<ValueId, IrError> {
	let loc = rw.ir().op(def).ok_or(IrError::Stale("op"))?.loc.clone();
	replace_with(rw, def, kind, loc)
}

/// Replaces the single-result op `def` by the op `kind` made beside it at `loc`, and answers its
/// result.
fn replace_with(
	rw: &mut Rewriter<'_>,
	def: OpId,
	kind: ExprOp,
	loc: Location,
) -> Result<ValueId, IrError> {
	let data = rw.ir().op(def).ok_or(IrError::Stale("op"))?;
	let (frame, snapshot) = (data.frame, data.snapshot);
	let ty = result_type(&kind, &[]);
	let spec = OpSpec {
		kind: OpKind::Expr(kind),
		operands: SmallVec::new(),
		results: smallvec![ty],
		frame,
		snapshot,
		loc,
	};
	let literal = rw.derived(&[def], |rw| rw.create(InsertPoint::Before(def), spec))?;
	let result = rw
		.ir()
		.op(literal)
		.and_then(|made| made.results.first().copied())
		.ok_or(IrError::Stale("op"))?;
	rw.replace_op(def, &[result])?;
	Ok(result)
}

/// Replaces the single-result op `op` by the existing value `value`.
///
/// When `value` is defined by an op in the same error scope as `op` (or is a block argument),
/// [`Rewriter::replace_op`] merges `op`'s provenance into that defining op. When `value` is defined
/// by an op in a different error scope, merging `op`'s provenance into it would move `op`'s bound
/// provenance across an [`RegionKind::ErrorScope`] boundary, so `op`'s uses are replaced by `value`
/// and `op` is erased as [`EliminationReason::FoldedConstant`].
fn replace_with_existing(rw: &mut Rewriter<'_>, op: OpId, value: ValueId) -> Result<(), IrError> {
	if same_error_scope(rw.ir(), op, value) {
		return rw.replace_op(op, &[value]);
	}
	let result = single_result(rw.ir(), op)?;
	rw.replace_all_uses(result, value)?;
	rw.erase(op, EliminationReason::FoldedConstant)
}

/// Whether `value` is defined in the same error scope as `op`, or is not defined by an op.
fn same_error_scope(ir: &Ir, op: OpId, value: ValueId) -> bool {
	let def = match ir.value(value).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op: def,
			..
		}) => def,
		Some(ValueDef::BlockArg {
			..
		})
		| None => return true,
	};
	let (Some(op_block), Some(def_block)) =
		(ir.op(op).map(|data| data.parent), ir.op(def).map(|data| data.parent))
	else {
		return false;
	};
	ir.error_scope_owner(op_block) == ir.error_scope_owner(def_block)
}

/// Rebuilds the `AND` chain of the predicate in `region` without the conjuncts in `dropped`, and
/// answers `1` when it did. A chain of one conjunct is left alone.
fn drop_true_conjuncts(
	cx: &mut LoCx<'_>,
	region: RegionId,
	dropped: &[ValueId],
) -> Result<u32, IrError> {
	let Some((_, root)) = clause_yield(cx.ir, region) else {
		return Ok(0);
	};
	let all = and_chain(cx.ir, root);
	if all.len() < 2 {
		return Ok(0);
	}
	let mut kept: Vec<ValueId> = all.iter().copied().filter(|v| !dropped.contains(v)).collect();
	if kept.len() == all.len() {
		return Ok(0);
	}
	if kept.is_empty() {
		kept.extend(all.first().copied());
	}
	let gates = gate_spine(cx.ir, root);
	let Some(&outer) = gates.first() else {
		return Ok(0);
	};
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let rebuilt = rebuild_and_chain(&mut rw, InsertPoint::Before(outer), &kept)?;
	rw.replace_all_uses(root, rebuilt)?;
	for gate in gates {
		let unused = rw
			.ir()
			.op(gate)
			.is_some_and(|data| data.results.iter().all(|result| rw.ir().uses(*result).is_empty()));
		if unused {
			rw.erase(gate, EliminationReason::DeadPure)?;
		}
	}
	Ok(1)
}

/// The `AND` ops of the chain whose value is `root`, outermost first, following the left operand
/// while it is read by that op alone.
fn gate_spine(ir: &Ir, root: ValueId) -> SmallVec<[OpId; 8]> {
	let mut gates: SmallVec<[OpId; 8]> = SmallVec::new();
	let mut at = root;
	while let Some(gate) = and_gate_of(ir, at) {
		gates.push(gate);
		let Some(&left) = ir.op(gate).and_then(|data| data.operands.first()) else {
			break;
		};
		if ir.uses(left).len() != 1 {
			break;
		}
		at = left;
	}
	gates
}

/// The `AND` op that defines `value`, if any.
fn and_gate_of(ir: &Ir, value: ValueId) -> Option<OpId> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	matches!(
		ir.op(op)?.kind,
		OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And
		})
	)
	.then_some(op)
}

/// The rewrite rules of the pass: [`AlgebraicRules`] and [`FactRules`].
fn rule_set() -> PatternSet<ExprDialect> {
	let mut set = PatternSet::new();
	for pattern in AlgebraicRules::patterns().into_iter().chain(FactRules::patterns()) {
		set.add(pattern);
	}
	set
}

/// Whether dropping `value`, or leaving it unevaluated, is unobservable: no op that computes it can
/// raise and two evaluations are one value. A block argument is a plain value and always
/// qualifies.
///
/// The ops judged are the defining op and, transitively, the ops of the same block that define
/// its operands, each with its regions: an operand computed only for `value` is not evaluated once
/// `value` is not, so a call beneath a comparison is judged as the comparison is. Reads the effects
/// the rewriter reports, computed under the default context. A definition that context does not
/// hold reports the top of every lattice, so the answer is never less conservative than the
/// request's own.
fn unobservable(rw: &mut Rewriter<'_>, value: ValueId, plain_reads: bool) -> bool {
	let Some(def) = rw.ir().value(value).map(|data| data.def) else {
		return false;
	};
	let ValueDef::OpResult {
		op: root,
		..
	} = def
	else {
		return true;
	};
	let Some(block) = rw.ir().op(root).map(|data| data.parent) else {
		return false;
	};
	let mut seen: SmallVec<[OpId; 8]> = SmallVec::new();
	let mut pending: SmallVec<[OpId; 8]> = smallvec![root];
	while let Some(op) = pending.pop() {
		if seen.contains(&op) {
			continue;
		}
		seen.push(op);
		if rw.ir().op(op).is_none() {
			return false;
		}
		if !(plain_reads && inert_row_read(rw.ir(), op)) {
			let effects = rw.facts(op).effects;
			let failures = effects.fallibility.union(fallibility::floor(effects.set));
			if !(guards::infallible(failures) && guards::deterministic(effects.determinism, true)) {
				return false;
			}
		}
		let Some(data) = rw.ir().op(op) else {
			return false;
		};
		for operand in &data.operands {
			if let Some(ValueDef::OpResult {
				op: held,
				..
			}) = rw.ir().value(*operand).map(|value| value.def)
				&& rw.ir().op(held).is_some_and(|held| held.parent == block)
			{
				pending.push(held);
			}
		}
	}
	true
}

/// What the type analysis knows of `value` under the registry `rw` holds. A rewriter with no
/// registry knows nothing.
fn classify(rw: &Rewriter<'_>, value: ValueId) -> TypeKnowledge {
	match rw.registry() {
		Some(registry) => types::classify_with(
			rw.ir(),
			&mut FactCache::new(),
			&structural_cx(registry, rw.ir()),
			value,
		),
		None => TypeKnowledge::Unknown,
	}
}

/// Whether the op `read` is a read of a field off the row of a `WHERE` clause that evaluating
/// reaches nothing through: the rows are the stored rows of a resolved table, and no layer
/// between the table and the clause sets the field, so the read neither derives a computed field
/// nor judges a guard.
fn inert_row_read(ir: &Ir, read: OpId) -> bool {
	let Some(data) = ir.op(read) else {
		return false;
	};
	let (
		OpKind::Expr(ExprOp::Field {
			name,
		}),
		[row],
	) = (&data.kind, data.operands.as_slice())
	else {
		return false;
	};
	let Some(ValueDef::BlockArg {
		block,
		..
	}) = ir.value(*row).map(|value| value.def)
	else {
		return false;
	};
	let Some(region) = ir.block(block).map(|held| held.parent) else {
		return false;
	};
	if !ir.region(region).is_some_and(|held| held.kind == RegionKind::Clause(ClauseRole::Where)) {
		return false;
	}
	let Some(filter) = ir.parent_op(region).and_then(|owner| ir.op(owner)) else {
		return false;
	};
	if !matches!(filter.kind, OpKind::Logical(LogOp::Filter)) {
		return false;
	}
	let Some(mut at) = filter.operands.first().copied() else {
		return false;
	};
	loop {
		let Some(value) = ir.value(at) else {
			return false;
		};
		match value.def {
			ValueDef::BlockArg {
				block,
				index,
			} => {
				// The element of the `SELECT` loop the filter stands in, over sources whose rows
				// are read without a fetch.
				let owner = ir
					.block(block)
					.and_then(|held| ir.parent_op(held.parent))
					.and_then(|owner| ir.op(owner));
				return owner
					.filter(|owner| {
						index == 0
							&& matches!(
								owner.kind,
								OpKind::Logical(LogOp::Loop {
									kind: LoopKind::Select,
									..
								})
							)
					})
					.and_then(|owner| owner.operands.first())
					.is_some_and(|source| rows_are_inert(ir, *source));
			}
			ValueDef::OpResult {
				op,
				..
			} => {
				let Some(held) = ir.op(op) else {
					return false;
				};
				match &held.kind {
					OpKind::Logical(LogOp::Project(attrs)) if attrs.layer => {
						if layer_column_names(attrs).iter().any(|column| column == name.as_str()) {
							return false;
						}
					}
					OpKind::Logical(
						LogOp::Filter
						| LogOp::PermissionFilter {
							..
						},
					) => {}
					OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return false,
				}
				let Some(next) = held.operands.first() else {
					return false;
				};
				at = *next;
			}
		}
	}
}

/// Whether a bare field read off any row the source `source` produces evaluates to a plain value
/// without fetching anything.
///
/// A table's rows are records, and therefore objects, so a field read is a key lookup. A scalar
/// source yields that scalar, and a field read off a scalar is `NONE`. Any other source can yield a
/// record id, directly or inside an array the read maps over, where the same read dereferences it
/// and runs the computed fields and permissions of the target. Every input of a union must qualify.
fn rows_are_inert(ir: &Ir, source: ValueId) -> bool {
	let Some(def) = single_result_def(ir, source).and_then(|def| ir.op(def)) else {
		return false;
	};
	match &def.kind {
		OpKind::Logical(LogOp::Source(attrs)) => match &attrs.source_ref {
			SourceRefShape::Table(_) => attrs.resolved,
			SourceRefShape::Dynamic => {
				def.operands.first().is_some_and(|operand| value_rows_are_inert(ir, *operand))
			}
			SourceRefShape::RecordId | SourceRefShape::Mock | SourceRefShape::Param => false,
		},
		OpKind::Logical(LogOp::Values {
			..
		}) => def.operands.first().is_some_and(|operand| value_rows_are_inert(ir, *operand)),
		OpKind::Logical(LogOp::UnionAll) => {
			!def.operands.is_empty() && def.operands.iter().all(|input| rows_are_inert(ir, *input))
		}
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// [`rows_are_inert`] for a source given as the value `value`: a scalar, an object or a constant.
/// An array can hold record ids, and anything else is resolved where the statement runs.
fn value_rows_are_inert(ir: &Ir, value: ValueId) -> bool {
	let Some(def) = single_result_def(ir, value).and_then(|def| ir.op(def)) else {
		return false;
	};
	match &def.kind {
		OpKind::Expr(ExprOp::Literal(held)) => matches!(
			&**held,
			Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Bytes(_)
				| Value::Regex(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Geometry(_)
				| Value::File(_)
				| Value::Object(_)
				| Value::Table(_)
		),
		OpKind::Expr(
			ExprOp::Object {
				..
			}
			| ExprOp::Constant {
				..
			}
			| ExprOp::Table {
				..
			},
		) => true,
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether the type analysis guarantees that `value` is a boolean.
fn is_bool_fact(rw: &Rewriter<'_>, value: ValueId) -> bool {
	let known = classify(rw, value);
	matches!(
		known,
		TypeKnowledge::Fact {
			kind: Kind::Bool,
			..
		}
	)
}

/// Whether every reader of `value` looks only at its truthiness: an `expr.truthy`, the condition of
/// an `IF`, the predicate of a filter or permission, or an operand of an `AND` or `OR` whose own
/// result is read that way. A value nothing reads answers `false`.
fn consumed_as_boolean(ir: &Ir, value: ValueId, hops: u8) -> bool {
	let uses = ir.uses(value);
	!uses.is_empty() && uses.iter().all(|held| boolean_use(ir, *held, hops))
}

/// Whether the read `held` looks only at the truthiness of the value it reads.
fn boolean_use(ir: &Ir, held: Use, hops: u8) -> bool {
	let Some(user) = ir.op(held.user) else {
		return false;
	};
	if matches!(user.kind, OpKind::Expr(ExprOp::Truthy)) {
		return true;
	}
	if matches!(user.kind, OpKind::Logical(LogOp::If) | OpKind::Expr(ExprOp::If)) {
		return held.operand == 0;
	}
	if matches!(user.kind, OpKind::Expr(ExprOp::Yield)) {
		return yield_is_boolean(ir, user.parent, hops);
	}
	if let OpKind::Expr(ExprOp::ShortCircuit {
		op,
	}) = &user.kind
	{
		return held.operand == 0
			&& matches!(op, ShortCircuitOp::And | ShortCircuitOp::Or)
			&& gate_result_is_boolean(ir, held.user, hops);
	}
	false
}

/// Whether the result of the short-circuit op `gate` is read only for its truthiness.
fn gate_result_is_boolean(ir: &Ir, gate: OpId, hops: u8) -> bool {
	if hops >= MAX_CONSUMER_HOPS {
		return false;
	}
	let Some(&result) = ir.op(gate).and_then(|data| data.results.first()) else {
		return false;
	};
	consumed_as_boolean(ir, result, hops + 1)
}

/// Whether the value a yield in `block` hands to its region's owner is read only for its
/// truthiness: the predicate of a filter or permission, or the right operand of an `AND` or `OR`
/// whose result is.
fn yield_is_boolean(ir: &Ir, block: BlockId, hops: u8) -> bool {
	let Some(region) = ir.block(block).map(|held| held.parent) else {
		return false;
	};
	let (Some(kind), Some(owner)) = (ir.region(region).map(|held| held.kind), ir.parent_op(region))
	else {
		return false;
	};
	let Some(owner_kind) = ir.op(owner).map(|data| &data.kind) else {
		return false;
	};
	if matches!(kind, RegionKind::Clause(ClauseRole::Where)) {
		return matches!(owner_kind, OpKind::Logical(LogOp::Filter));
	}
	if matches!(kind, RegionKind::PermissionFrame) {
		return matches!(owner_kind, OpKind::Logical(LogOp::PermissionFilter { .. }));
	}
	if matches!(kind, RegionKind::LazyOperand)
		&& let OpKind::Expr(ExprOp::ShortCircuit {
			op,
		}) = owner_kind
	{
		return matches!(op, ShortCircuitOp::And | ShortCircuitOp::Or)
			&& gate_result_is_boolean(ir, owner, hops);
	}
	false
}

/// The value of the literal that is the whole of the lazy operand `region`: one `expr.literal` and
/// the yield of it.
fn lazy_literal(ir: &Ir, region: RegionId) -> Option<&Value> {
	let (_, value) = clause_yield(ir, region)?;
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	(ir.block(*block)?.ops.len() == 2).then_some(())?;
	literal_of(ir, value)
}

/// The algebraic rules that the logical rewrite catalogue does not cover.
///
/// Each rule is a pattern over the expression dialect and requests no fold:
/// - `false AND <lazy>` is `false` and `true OR <lazy>` is `true`, because the lazy side never
///   runs;
/// - `true AND <lazy>` is the lazy operand's value and `false OR <lazy>` is the lazy operand's
///   value, which the gate evaluates in every case;
/// - `x AND true` is `x` and `x OR false` is `x`, when only the truthiness of the result is read or
///   `x` is a boolean: `x` is evaluated once either way;
/// - `x AND false` is `false` and `x OR true` is `true`, when only the truthiness of the result is
///   read or `x` is a boolean, and `x` can neither raise nor differ between two evaluations;
/// - `x INSIDE []` is `false` and `x NOTINSIDE []` is `true`, when `x` can neither raise nor differ
///   between two evaluations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AlgebraicRules;

impl AlgebraicRules {
	/// The rules, in the order they are tried.
	pub(crate) fn patterns() -> Vec<Box<dyn Pattern<ExprDialect>>> {
		vec![
			Box::new(LiteralLeft {
				name: "false_and_lazy",
				gate: ShortCircuitOp::And,
				literal: false,
			}),
			Box::new(LiteralLeft {
				name: "true_or_lazy",
				gate: ShortCircuitOp::Or,
				literal: true,
			}),
			Box::new(NeutralLeft {
				name: "true_and_lazy",
				gate: ShortCircuitOp::And,
				neutral: true,
			}),
			Box::new(NeutralLeft {
				name: "false_or_lazy",
				gate: ShortCircuitOp::Or,
				neutral: false,
			}),
			Box::new(NeutralRight {
				name: "and_true",
				gate: ShortCircuitOp::And,
				neutral: true,
			}),
			Box::new(NeutralRight {
				name: "or_false",
				gate: ShortCircuitOp::Or,
				neutral: false,
			}),
			Box::new(DecidedRight {
				name: "and_false",
				gate: ShortCircuitOp::And,
				decided: false,
			}),
			Box::new(DecidedRight {
				name: "or_true",
				gate: ShortCircuitOp::Or,
				decided: true,
			}),
			Box::new(InsideEmpty {
				name: "inside_empty",
				operator: BinaryOperator::Inside,
				answer: false,
			}),
			Box::new(InsideEmpty {
				name: "not_inside_empty",
				operator: BinaryOperator::NotInside,
				answer: true,
			}),
		]
	}
}

/// `false AND <lazy>` and `true OR <lazy>`: the left literal decides, so the result is that
/// literal.
struct LiteralLeft {
	name: &'static str,
	gate: ShortCircuitOp,
	literal: bool,
}

impl Pattern<ExprDialect> for LiteralLeft {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::ShortCircuit
	}

	fn benefit(&self) -> u16 {
		20
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: gate,
		}) = &data.kind
		else {
			return Ok(false);
		};
		let ([left], 1) = (data.operands.as_slice(), data.results.len()) else {
			return Ok(false);
		};
		let left = *left;
		let decided = *gate == self.gate
			&& matches!(literal_of(rw.ir(), left), Some(Value::Bool(held)) if *held == self.literal);
		if !decided {
			return Ok(false);
		}
		replace_with_existing(rw, op, left)?;
		Ok(true)
	}
}

/// `true AND <lazy>` and `false OR <lazy>`: the left literal is neutral, so the gate always
/// evaluates the lazy operand and the result is its value. The operand's ops stand where the gate
/// stood.
struct NeutralLeft {
	name: &'static str,
	gate: ShortCircuitOp,
	neutral: bool,
}

impl Pattern<ExprDialect> for NeutralLeft {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::ShortCircuit
	}

	fn benefit(&self) -> u16 {
		20
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: gate,
		}) = &data.kind
		else {
			return Ok(false);
		};
		let ([left], [region], [_]) =
			(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
		else {
			return Ok(false);
		};
		let (left, region) = (*left, *region);
		let neutral = *gate == self.gate
			&& matches!(literal_of(rw.ir(), left), Some(Value::Bool(held)) if *held == self.neutral);
		if !neutral || !collapsible(rw.ir(), region, 0, 1) {
			return Ok(false);
		}
		let Ok(yielded) = rw.inline_region(region, InsertPoint::Before(op), &[]) else {
			return Ok(false);
		};
		let [value] = yielded.as_slice() else {
			return Err(IrError::Stale("value"));
		};
		replace_with_existing(rw, op, *value)?;
		Ok(true)
	}
}

/// `x AND true` and `x OR false`: the lazy right operand is the neutral literal, so the result is
/// `x` wherever only truthiness is read. `x` is evaluated once before and after, so nothing it
/// does is dropped.
struct NeutralRight {
	name: &'static str,
	gate: ShortCircuitOp,
	neutral: bool,
}

impl Pattern<ExprDialect> for NeutralRight {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::ShortCircuit
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: gate,
		}) = &data.kind
		else {
			return Ok(false);
		};
		let ([x], [region], [result]) =
			(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
		else {
			return Ok(false);
		};
		let (x, region, result) = (*x, *region, *result);
		let neutral = *gate == self.gate
			&& matches!(lazy_literal(rw.ir(), region), Some(Value::Bool(held)) if *held == self.neutral);
		if !neutral {
			return Ok(false);
		}
		if !(consumed_as_boolean(rw.ir(), result, 0) || is_bool_fact(rw, x)) {
			return Ok(false);
		}
		replace_with_existing(rw, op, x)?;
		Ok(true)
	}
}

/// `x AND false` and `x OR true`: the lazy right operand decides, so the result is that literal
/// wherever only truthiness is read.
struct DecidedRight {
	name: &'static str,
	gate: ShortCircuitOp,
	decided: bool,
}

impl Pattern<ExprDialect> for DecidedRight {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::ShortCircuit
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: gate,
		}) = &data.kind
		else {
			return Ok(false);
		};
		let ([x], [region], [result]) =
			(data.operands.as_slice(), data.regions.as_slice(), data.results.as_slice())
		else {
			return Ok(false);
		};
		let (x, region, result) = (*x, *region, *result);
		let decides = *gate == self.gate
			&& matches!(lazy_literal(rw.ir(), region), Some(Value::Bool(held)) if *held == self.decided);
		if !decides {
			return Ok(false);
		}
		if !(consumed_as_boolean(rw.ir(), result, 0) || is_bool_fact(rw, x))
			|| !unobservable(rw, x, true)
		{
			return Ok(false);
		}
		replace_with_decided(rw, op, Value::Bool(self.decided))?;
		Ok(true)
	}
}

/// `x INSIDE []` and `x NOTINSIDE []`: no value is in an empty array, so the answer does not
/// depend on `x`.
struct InsideEmpty {
	name: &'static str,
	operator: BinaryOperator,
	answer: bool,
}

impl Pattern<ExprDialect> for InsideEmpty {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::Binary
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::Binary {
			op: operator,
		}) = &data.kind
		else {
			return Ok(false);
		};
		let ([x, empty], 1) = (data.operands.as_slice(), data.results.len()) else {
			return Ok(false);
		};
		let (x, empty) = (*x, *empty);
		let matched = *operator == self.operator
			&& literal_of(rw.ir(), empty).is_some_and(Value::is_empty_array);
		if !matched || !unobservable(rw, x, !self.answer) {
			return Ok(false);
		}
		replace_with_decided(rw, op, Value::Bool(self.answer))?;
		Ok(true)
	}
}

/// The rules a type fact makes sound without a guard.
///
/// A fact comes from a literal or from derivation, so these rules fire on values derived from
/// literals:
/// - an `expr.convert` to kind `K` of a value known to be of kind `K` is that value;
/// - an `expr.truthy` of a value known to be a boolean is that value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FactRules;

impl FactRules {
	/// The rules, in the order they are tried.
	pub(crate) fn patterns() -> Vec<Box<dyn Pattern<ExprDialect>>> {
		vec![Box::new(ConvertOfFact), Box::new(TruthyOfFact)]
	}
}

/// An `expr.convert` whose operand is already known to be of the kind it converts to.
struct ConvertOfFact;

impl Pattern<ExprDialect> for ConvertOfFact {
	fn name(&self) -> &'static str {
		"convert_of_fact"
	}

	fn root(&self) -> ExprTag {
		ExprTag::Convert
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let OpKind::Expr(ExprOp::Convert(attrs)) = &data.kind else {
			return Ok(false);
		};
		let ([operand], 1) = (data.operands.as_slice(), data.results.len()) else {
			return Ok(false);
		};
		let operand = *operand;
		let known = classify(rw, operand);
		let TypeKnowledge::Fact {
			kind,
			..
		} = known
		else {
			return Ok(false);
		};
		if kind != attrs.kind {
			return Ok(false);
		}
		replace_with_existing(rw, op, operand)?;
		Ok(true)
	}
}

/// An `expr.truthy` whose operand is already known to be a boolean.
struct TruthyOfFact;

impl Pattern<ExprDialect> for TruthyOfFact {
	fn name(&self) -> &'static str {
		"truthy_of_fact"
	}

	fn root(&self) -> ExprTag {
		ExprTag::Truthy
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(data) = rw.ir().op(op) else {
			return Ok(false);
		};
		let ([operand], 1) = (data.operands.as_slice(), data.results.len()) else {
			return Ok(false);
		};
		let operand = *operand;
		if !is_bool_fact(rw, operand) {
			return Ok(false);
		}
		replace_with_existing(rw, op, operand)?;
		Ok(true)
	}
}
