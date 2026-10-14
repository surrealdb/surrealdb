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

use std::future::Future;
use std::pin::Pin;

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport};
use super::rehome::{and_chain, rebuild_and_chain};
use crate::exec::ir::analysis::{effects, fallibility, types};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, Limit, Pattern, PatternSet, apply_greedy};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::{Use, ValueDef};
use crate::exec::ir::core::walk::{ControlTarget, ancestors, control_target};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::conversion::{EliminationReason, ExprDialect};
use crate::exec::ir::iface::fold::{FoldConsumer, FoldOutcome, FoldRequest};
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LoopKind, SourceAttrs, SourceRefShape};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::{BinaryOperator, Kind};
use crate::val::Value;

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

	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>> {
		Box::pin(run_fold(cx))
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
}

async fn run_fold(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let rules = rule_set();
	let mut report = PassReport::default();
	for round in 0..MAX_ROUNDS {
		let sites = sweep(cx.ir, cx.cfg.unroll);
		for (value, consumer) in requests(cx.ir, &sites) {
			fold_value(cx, value, consumer).await;
		}
		let mut applied = apply_ifs(cx, &sites.ifs)?;
		applied += apply_clauses(cx, &sites.clauses)?;
		applied += apply_sources(cx, &sites.sources)?;
		let greedy = apply_greedy(cx.ir, cx.facts, &rules, cx.folder, GREEDY_DEFAULT).await?;
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
	sites
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

/// Asks the folder for `value` and stores the answer. A literal needs no fold, and a value that
/// already holds an outcome is not asked again.
async fn fold_value(cx: &mut LoCx<'_>, value: ValueId, consumer: FoldConsumer) {
	if cx.ir.value(value).is_none()
		|| literal_of(cx.ir, value).is_some()
		|| cx.facts.fold_outcome(value).is_some()
	{
		return;
	}
	let request = FoldRequest {
		value,
		consumer,
	};
	let outcome = cx.folder.fold(&*cx.ir, cx.effects, request).await;
	cx.facts.store_fold(value, outcome);
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

/// Replaces `op`, an `IF`, by the ops of the arm its condition selects, and answers `1`, or `0`
/// when nothing is decided, the arm may not be collapsed, or the rewriter refuses the inlining.
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
	if !collapsible(cx.ir, taken, results.len()) {
		return Ok(0);
	}
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
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
	Ok(1)
}

/// Whether the arm `region` may be inlined into the block of its `IF`: it is one block without
/// arguments that ends in a yield of `arity` values, no `RETURN`, `BREAK` or `CONTINUE` inside
/// it lands on the arm or beyond it, and it holds no write, whose events spend the computation
/// depth of the arms around it.
fn collapsible(ir: &Ir, region: RegionId, arity: usize) -> bool {
	let Some(held) = ir.region(region) else {
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
	if !block.args.is_empty() || !yields || last.operands.len() != arity {
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
fn apply_clauses(cx: &mut LoCx<'_>, clauses: &[(OpId, RegionId)]) -> Result<u32, IrError> {
	let mut rewrites = 0;
	for (op, region) in clauses {
		if cx.ir.op(*op).is_some() && cx.ir.region(*region).is_some() {
			rewrites += fold_conjuncts(cx, *region)?;
		}
	}
	Ok(rewrites)
}

/// Replaces each conjunct of the predicate in `region` that folded to a constant by the literal of
/// its truthiness, then removes the conjuncts that became `true` from the `AND` chain.
///
/// A chain whose every conjunct became `true` keeps its first one, so the predicate is still a
/// value. A conjunct that folded to `false` stays as a literal `false`.
fn fold_conjuncts(cx: &mut LoCx<'_>, region: RegionId) -> Result<u32, IrError> {
	let Some((_, cond)) = clause_yield(cx.ir, region) else {
		return Ok(0);
	};
	let mut made_true: SmallVec<[ValueId; 2]> = SmallVec::new();
	let mut rewrites = 0;
	for conjunct in and_chain(cx.ir, cond) {
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
		rewrites += 1;
	}
	if !made_true.is_empty() {
		rewrites += drop_true_conjuncts(cx, region, &made_true)?;
	}
	Ok(rewrites)
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
	let data = rw.ir().op(def).ok_or(IrError::Stale("op"))?;
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let kind = ExprOp::Literal(Box::new(value));
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
fn gate_spine(ir: &Ir, root: ValueId) -> SmallVec<[OpId; 4]> {
	let mut gates: SmallVec<[OpId; 4]> = SmallVec::new();
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
fn unobservable(rw: &mut Rewriter<'_>, value: ValueId) -> bool {
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
		let effects = rw.facts(op).effects;
		let failures = effects.fallibility.union(fallibility::floor(effects.set));
		if !(guards::infallible(failures) && guards::deterministic(effects.determinism, true)) {
			return false;
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

/// Whether the type analysis guarantees that `value` is a boolean.
fn is_bool_fact(ir: &Ir, value: ValueId) -> bool {
	let known = types::classify(ir, &mut FactCache::new(), value);
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
/// - `x AND true` is `x` and `x OR false` is `x`, when only the truthiness of the result is read or
///   `x` is a boolean, and `x` can neither raise nor differ between two evaluations;
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
		rw.replace_op(op, &[left])?;
		Ok(true)
	}
}

/// `x AND true` and `x OR false`: the lazy right operand is the neutral literal, so the result is
/// `x` wherever only truthiness is read.
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
		if !(consumed_as_boolean(rw.ir(), result, 0) || is_bool_fact(rw.ir(), x)) {
			return Ok(false);
		}
		if !unobservable(rw, x) {
			return Ok(false);
		}
		rw.replace_op(op, &[x])?;
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
		if !matched || !unobservable(rw, x) {
			return Ok(false);
		}
		replace_with_literal(rw, op, Value::Bool(self.answer))?;
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
		let known = types::classify(rw.ir(), &mut FactCache::new(), operand);
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
		rw.replace_op(op, &[operand])?;
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
		if !is_bool_fact(rw.ir(), operand) {
			return Ok(false);
		}
		rw.replace_op(op, &[operand])?;
		Ok(true)
	}
}
