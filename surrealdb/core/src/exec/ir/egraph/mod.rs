//! The e-graph: equality saturation over the logical and physical dialects.
//!
//! The e-graph runs under hard limits on nodes, iterations and time. A wall-clock stop or a rule
//! that reaches its match cap leaves the statement with the plan it had before the e-graph ran
//! (after seeding, with the default conversion's plan), and a node or iteration stop extracts the
//! best term found so far. Every rule is justified by a side condition or by a soundness
//! statement, and a rule registers for one dialect only. Placement is decided after extraction,
//! never by the e-graph, and the cost hook reads no placement policy. Production planning is
//! logical-only, because no implementation rule is registered.
//!
//! [`plan_statement`] is the one entry. It decides through [`gate`] whether the e-graph runs on a
//! statement and in which [`Mode`], and it answers an [`EgraphOutcome`] that names every stop and
//! every fallback. A statement the e-graph does not change is exactly as it was given.

#[cfg(test)]
use std::cell::Cell;
use std::collections::HashMap;

use egg::{EGraph, Id};
use surrealdb_strand::TableName;

use self::analysis::IrAnalysis;
use self::decl::{ClassRef, ENodeSnapshot, MAX_ROUNDS, StmtMap};
use self::elaborate::{
	Basis, SeedTarget, StatementSurgery, apply_mapped, plan, write_logical, write_seeded,
};
use self::extract::{Candidate, PhysChoice, extract_phys, logical_candidates};
use self::import::{ImportStop, Imported, SeedCopy, import, import_seed};
use self::lang::{ENode, identity_of};
use self::rule::RuleSet;
use self::runner::{
	Budget, ROUND_L_ITERATIONS, ROUND_P_ITERATIONS, RoundReport, TierFilter, resaturate_needed,
	saturate,
};
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::egraph::{EGRAPH_DEFAULT, EgraphLimits, RuleDialect};
use crate::exec::ir::iface::ids::{OpId, SnapshotId, ValueId};
use crate::exec::ir::logical::ops::{LogOp, LoopKind, SourceRefShape};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::With;

pub(crate) mod analysis;
pub(crate) mod decl;
pub(crate) mod elaborate;
pub(crate) mod extract;
pub(crate) mod guards;
pub(crate) mod import;
pub(crate) mod lang;
mod outcome;
pub(crate) mod reg;
pub(crate) mod rule;
pub(crate) mod rules_logical;
pub(crate) mod rules_phys;
pub(crate) mod runner;
pub(crate) mod surgery;
#[cfg(test)]
mod tests;

pub(crate) use self::outcome::{
	EgraphOutcome, FallbackReason, Gate, Mode, SeedFuture, SeedHost, SkipReason,
};

// ---------------------------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------------------------

/// Whether `kind` is a choice point: an op whose placement, order or form the e-graph may decide.
fn is_choice_point(kind: &OpKind) -> bool {
	matches!(
		kind,
		OpKind::Logical(
			LogOp::Join { .. }
				| LogOp::Subquery
				| LogOp::Loop {
					kind: LoopKind::GraphStep,
					..
				}
		) | OpKind::Expr(ExprOp::Subquery(_))
	)
}

/// What one walk over a statement finds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Scan {
	/// The statement holds a choice point.
	choice_point: bool,
	/// Bit `i` is set when rule `i` pre-matches some op of the statement.
	mask: u64,
}

/// Walks the ops of `stmt` once, in program order, and answers its choice points and the rules
/// that pre-match. Allocates nothing.
fn scan(ir: &Ir, stmt: OpId, rules: &RuleSet) -> Scan {
	let mut found = Scan::default();
	let Some(data) = ir.op(stmt) else {
		return found;
	};
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			if let Some(held) = ir.op(op) {
				found.choice_point |= is_choice_point(&held.kind);
			}
			found.mask |= rules.pre_match(ir, op);
			Walk::Continue
		});
	}
	found
}

/// The bit mask of the rules of `rules` that are implementation rules.
fn implementation_mask(rules: &RuleSet) -> u64 {
	rules
		.of_dialect(RuleDialect::Implementation)
		.fold(0u64, |mask, (rank, _)| mask | (1u64 << rank))
}

/// The verdict for a statement: it needs a choice point and a rule that pre-matches, and runs
/// seeded exactly when an implementation rule pre-matches.
fn decide(found: Scan, implementation: u64) -> Gate {
	if !found.choice_point {
		Gate::Skip(SkipReason::NoChoicePoint)
	} else if found.mask == 0 {
		Gate::Skip(SkipReason::NoApplicableRule)
	} else if found.mask & implementation != 0 {
		Gate::Run(Mode::Seeded)
	} else {
		Gate::Run(Mode::LogicalOnly)
	}
}

/// Whether the e-graph runs on the statement `stmt`, and in which mode.
///
/// One walk over the statement with a `u64` mask and no allocation. A statement without a choice
/// point (a `logical.join`, a `logical.loop` of kind `GraphStep`, a `logical.subquery` or an
/// `expr.subquery`) has nothing to place, and one no rule pre-matches ([`RuleSet::pre_match`])
/// cannot change. Otherwise the run is seeded exactly when an implementation rule pre-matches, and
/// logical-only when only logical rewrite rules do.
///
/// A program-cache hit is not a verdict this function produces: no program cache exists to
/// consult, so [`SkipReason::CacheHit`] is reserved.
pub(crate) fn gate(ir: &Ir, stmt: OpId, rules: &RuleSet) -> Gate {
	decide(scan(ir, stmt, rules), implementation_mask(rules))
}

// ---------------------------------------------------------------------------------------------
// The test hook
// ---------------------------------------------------------------------------------------------

/// How a test overrides the gate.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EgraphMode {
	/// The gate decides.
	Default,
	/// The e-graph runs logical-only on every statement, whatever the gate would say.
	ForceOnLogicalOnly,
	/// The e-graph runs seeded on every statement.
	ForceOnSeeded,
	/// The e-graph never runs.
	ForceOff,
}

#[cfg(test)]
thread_local! {
	static MODE: Cell<EgraphMode> = const { Cell::new(EgraphMode::Default) };
	static RULES: Cell<Option<&'static RuleSet>> = const { Cell::new(None) };
	static LIMITS: Cell<Option<EgraphLimits>> = const { Cell::new(None) };
}

/// What planning has reached on one thread.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts {
	/// Calls to [`plan_statement`].
	pub(crate) planned: usize,
	/// Seeded runs that reached all-physical extraction.
	pub(crate) extracted: usize,
	/// Statements a write-back adopted a patched copy for.
	pub(crate) adopted: usize,
}

#[cfg(test)]
thread_local! {
	static COUNTS: Cell<Counts> = const {
		Cell::new(Counts { planned: 0, extracted: 0, adopted: 0 })
	};
}

/// The counts of the calling thread's planning so far.
///
/// The counts are per thread, like the mode: a test that forces the e-graph on drives the planner
/// on a current-thread runtime, so planning another test's statements on another thread never
/// advances them. A test that forces the e-graph on asserts the counts it expects moved, so a
/// request path that never reaches the planner, the extraction or the write-back fails the test
/// instead of passing it.
#[cfg(test)]
pub(crate) fn counts() -> Counts {
	COUNTS.with(Cell::get)
}

#[cfg(test)]
fn count(bump: impl FnOnce(&mut Counts)) {
	COUNTS.with(|held| {
		let mut counts = held.get();
		bump(&mut counts);
		held.set(counts);
	});
}

/// Records that a write-back adopted a patched copy.
#[cfg(test)]
fn note_adopted() {
	count(|counts| counts.adopted += 1);
}

/// Records that a write-back adopted a patched copy: never, outside tests.
#[cfg(not(test))]
fn note_adopted() {}

/// Sets the mode the calling thread's planning runs under, and answers the one it replaces.
///
/// The mode is read on the thread that polls [`plan_statement`], so a test that forces a mode
/// drives the planner on a current-thread runtime.
#[cfg(test)]
pub(crate) fn set_mode(mode: EgraphMode) -> EgraphMode {
	MODE.with(|held| held.replace(mode))
}

/// Sets the rule set the calling thread's planning uses in place of the one its [`PlanStmtCx`]
/// names, and answers the one it replaces. `None` leaves the context's set in force.
#[cfg(test)]
pub(crate) fn set_rules(rules: Option<&'static RuleSet>) -> Option<&'static RuleSet> {
	RULES.with(|held| held.replace(rules))
}

/// Sets the limits the calling thread's planning runs under in place of the ones its
/// [`PlanStmtCx`] names, and answers the ones it replaces. `None` leaves the context's limits in
/// force.
#[cfg(test)]
pub(crate) fn set_limits(limits: Option<EgraphLimits>) -> Option<EgraphLimits> {
	LIMITS.with(|held| held.replace(limits))
}

/// A mode, rule set and limits in force on the calling thread until the guard drops, which
/// restores the ones it found.
#[cfg(test)]
pub(crate) struct ForcedPlanning {
	mode: EgraphMode,
	rules: Option<&'static RuleSet>,
	limits: Option<EgraphLimits>,
}

#[cfg(test)]
impl ForcedPlanning {
	pub(crate) fn new(mode: EgraphMode, rules: Option<&'static RuleSet>) -> Self {
		Self {
			mode: set_mode(mode),
			rules: set_rules(rules),
			limits: set_limits(None),
		}
	}

	/// A guard that also runs the planning under `limits`, so a test of a rewrite measures the
	/// rewrite and not the wall clock.
	pub(crate) fn with_limits(self, limits: EgraphLimits) -> Self {
		set_limits(Some(limits));
		self
	}
}

#[cfg(test)]
impl Drop for ForcedPlanning {
	fn drop(&mut self) {
		set_mode(self.mode);
		set_rules(self.rules);
		set_limits(self.limits);
	}
}

/// The rule set a planning call uses.
#[cfg(test)]
fn rules_for<'a>(cx: &'a PlanStmtCx<'_>) -> &'a RuleSet {
	RULES.with(Cell::get).unwrap_or(cx.rules)
}

/// The rule set a planning call uses.
#[cfg(not(test))]
fn rules_for<'a>(cx: &'a PlanStmtCx<'_>) -> &'a RuleSet {
	cx.rules
}

/// The limits a planning call runs under.
#[cfg(test)]
fn limits_for(cx: &PlanStmtCx<'_>) -> EgraphLimits {
	LIMITS.with(Cell::get).unwrap_or(cx.limits)
}

/// The limits a planning call runs under.
#[cfg(not(test))]
fn limits_for(cx: &PlanStmtCx<'_>) -> EgraphLimits {
	cx.limits
}

/// The verdict for `stmt`, and whether a test forced it.
///
/// A forced run bypasses the choice-point and pre-match parts of the gate and the total-order
/// part of seeded mode, so the whole machinery runs on every statement.
#[cfg(test)]
fn verdict(ir: &Ir, stmt: OpId, rules: &RuleSet) -> (Gate, bool) {
	let forced = match MODE.with(Cell::get) {
		EgraphMode::Default => return (gate(ir, stmt, rules), false),
		EgraphMode::ForceOff => return (Gate::Skip(SkipReason::Forced), true),
		EgraphMode::ForceOnLogicalOnly => Mode::LogicalOnly,
		EgraphMode::ForceOnSeeded => Mode::Seeded,
	};
	(Gate::Run(forced), true)
}

/// The verdict for `stmt`, and whether a test forced it: never, outside tests.
#[cfg(not(test))]
fn verdict(ir: &Ir, stmt: OpId, rules: &RuleSet) -> (Gate, bool) {
	(gate(ir, stmt, rules), false)
}

// ---------------------------------------------------------------------------------------------
// Planning one statement
// ---------------------------------------------------------------------------------------------

/// What planning one statement reads: the effect context, the limits, the rules, the physical
/// layer's answers for seeded mode, and the container's statement copies.
pub(crate) struct PlanStmtCx<'a> {
	pub(crate) effects: EffectCx<'a>,
	pub(crate) limits: EgraphLimits,
	pub(crate) rules: &'a RuleSet,
	/// `None` when no physical layer answers for seeded mode. A statement whose verdict is seeded
	/// is then left as it was, with [`SkipReason::NotYet`].
	pub(crate) seed: Option<&'a dyn SeedHost>,
	pub(crate) surgery: &'a dyn StatementSurgery,
}

impl<'a> PlanStmtCx<'a> {
	/// The context a request plans one statement with: the default limits, the registered rules,
	/// the container's statement surgery, and no physical layer for seeded mode. A caller that has
	/// a physical layer answering for seeded mode sets `seed` on the value returned.
	pub(crate) fn for_request(effects: EffectCx<'a>) -> PlanStmtCx<'a> {
		PlanStmtCx {
			effects,
			limits: EGRAPH_DEFAULT,
			rules: rule::registered(),
			seed: None,
			surgery: &surgery::CONTAINER_SURGERY,
		}
	}
}

/// The outcome of a run that ended at `stop` with the graph `eg` and the work `budget` records.
fn outcome(
	mode: Mode,
	budget: &Budget,
	eg: &EGraph<ENode, IrAnalysis>,
	stopped_by: Option<Limit>,
	chose_alternative: bool,
	fallback: Option<FallbackReason>,
) -> EgraphOutcome {
	EgraphOutcome {
		ran: Ok(()),
		stopped_by,
		mode: Some(mode),
		rounds: budget.rounds,
		iterations: budget.iterations,
		nodes: u32::try_from(eg.total_number_of_nodes()).unwrap_or(u32::MAX),
		chose_alternative,
		fallback,
		union_refused: eg.analysis.union_refused,
	}
}

/// The outcome of a run with no graph, and of a run that failed before it had one.
fn without_graph(
	mode: Mode,
	stopped_by: Option<Limit>,
	fallback: Option<FallbackReason>,
) -> EgraphOutcome {
	EgraphOutcome {
		ran: Ok(()),
		stopped_by,
		mode: Some(mode),
		rounds: 0,
		iterations: 0,
		nodes: 0,
		chose_alternative: false,
		fallback,
		union_refused: 0,
	}
}

/// The outcome of an import that produced no graph.
///
/// A node limit leaves the statement as it was. A wall limit is the skip path's, and a statement
/// the container does not hold is not planned.
fn import_stopped(mode: Mode, stop: ImportStop) -> EgraphOutcome {
	match stop {
		ImportStop::Limit(Limit::Wall) => {
			without_graph(mode, Some(Limit::Wall), Some(FallbackReason::WallBeforeSeeding))
		}
		ImportStop::Limit(limit) => without_graph(mode, Some(limit), None),
		ImportStop::Dangling | ImportStop::Seed(_) => EgraphOutcome::skipped(SkipReason::NotYet),
	}
}

/// The fallback a round's stop calls for before any physical plan exists: a rule that reached
/// its match cap has truncated its matches, and a wall stop is the skip path's. A node or an
/// iteration stop calls for none, because the graph as it stands is sound.
fn stop_fallback(report: &RoundReport) -> Option<FallbackReason> {
	if report.cap_hit {
		Some(FallbackReason::MatchCapHit)
	} else if report.stopped_by == Some(Limit::Wall) {
		Some(FallbackReason::WallBeforeSeeding)
	} else {
		None
	}
}

/// Plans the statement `stmt` of `ir` through the e-graph and writes the plan back, or leaves the
/// statement exactly as it was.
///
/// The gate decides whether the e-graph runs and in which mode ([`gate`]). A logical-only run
/// imports the statement, saturates the logical rules and, when the cheapest term differs from
/// the incumbent, writes it back through `cx.surgery`. A seeded run adds the physical layer: it
/// resolves the row sources' facts, converts a copy of each logical candidate, saturates the
/// implementation rules over the logical and physical nodes together, extracts an all-physical
/// plan and patches it into the winning candidate's converted copy. The statement is then wholly
/// logical or wholly physical.
///
/// Stops follow the limits of `cx.limits`. A wall stop and a rule that reaches its match cap answer
/// what the skip path answers (after seeding, the default conversion's plan, which needs nothing
/// written); a node or iteration stop extracts the best plan found. A failed write-back or
/// conversion is reported in the outcome's `fallback` and leaves `ir` and `facts` as they were. No
/// failure is an error.
///
/// Seeded mode needs `cx.seed`; without it the statement is skipped with [`SkipReason::NotYet`].
/// The e-graph is held across the awaits of the physical layer, so the future is `Send`.
pub(crate) async fn plan_statement(
	ir: &mut Ir,
	facts: &mut FactCache,
	stmt: OpId,
	cx: &PlanStmtCx<'_>,
) -> EgraphOutcome {
	#[cfg(test)]
	count(|counts| counts.planned += 1);
	let rules = rules_for(cx);
	let (verdict, forced) = verdict(ir, stmt, rules);
	match verdict {
		Gate::Skip(reason) => EgraphOutcome::skipped(reason),
		Gate::Run(Mode::LogicalOnly) => plan_logical(ir, facts, stmt, cx, rules),
		Gate::Run(Mode::Seeded) => match cx.seed {
			None => EgraphOutcome::skipped(SkipReason::NotYet),
			Some(host) => plan_seeded(ir, facts, stmt, cx, rules, host, forced).await,
		},
	}
}

/// The logical-only run: import, round L, and the write-back of candidate 1 when one exists.
fn plan_logical(
	ir: &mut Ir,
	facts: &mut FactCache,
	stmt: OpId,
	cx: &PlanStmtCx<'_>,
	rules: &RuleSet,
) -> EgraphOutcome {
	let mode = Mode::LogicalOnly;
	let limits = limits_for(cx);
	let mut budget = Budget::new(limits);
	let mut imported = match import(ir, facts, stmt, &cx.effects, limits) {
		Ok(imported) => imported,
		Err(stop) => return import_stopped(mode, stop),
	};
	let round = saturate(
		&mut imported.eg,
		rules,
		RuleDialect::LogicalRewrite,
		TierFilter::Normal,
		ROUND_L_ITERATIONS,
		&mut budget,
		&cx.effects,
	);
	let stopped_by = round.stopped_by;
	if let Some(fallback) = stop_fallback(&round) {
		return outcome(mode, &budget, &imported.eg, stopped_by, false, Some(fallback));
	}
	let found = logical_candidates(&imported.eg, &imported.roots);
	let Some(rewritten) = found.get(1) else {
		return outcome(mode, &budget, &imported.eg, stopped_by, false, None);
	};
	match write_logical(ir, facts, stmt, &imported.eg, rewritten, cx.surgery, &cx.effects) {
		Ok(()) => outcome(mode, &budget, &imported.eg, stopped_by, true, None),
		Err(fallback) => outcome(mode, &budget, &imported.eg, stopped_by, false, Some(fallback)),
	}
}

/// A table a statement reads, with the `WITH` clause and snapshot it reads it under.
type TableSource = (TableName, Option<With>, SnapshotId);

/// The row sources of the statement `stmt`: each source op, and each source of a table with the
/// `WITH` clause and snapshot it reads.
fn row_sources(ir: &Ir, stmt: OpId) -> (Vec<OpId>, Vec<TableSource>) {
	let mut producers = Vec::new();
	let mut tables = Vec::new();
	let Some(data) = ir.op(stmt) else {
		return (producers, tables);
	};
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			if let Some(held) = ir.op(op)
				&& let OpKind::Logical(LogOp::Source(attrs)) = &held.kind
			{
				producers.push(op);
				if let SourceRefShape::Table(table) = &attrs.source_ref {
					tables.push((table.clone(), attrs.with.clone(), held.snapshot));
				}
			}
			Walk::Continue
		});
	}
	(producers, tables)
}

/// The converted copy of one candidate, and what describes it.
struct Converted {
	candidate: u8,
	ir: Ir,
	map: StmtMap,
}

/// A statement copy with a candidate elaborated into it, before its conversion.
struct Prepared {
	ir: Ir,
	map: StmtMap,
	facts: FactCache,
	/// The class each op the elaboration created is a node of, by the op's result value. Such a
	/// value has no counterpart in `map`, so the seeding resolves it here.
	created: HashMap<ValueId, ClassRef>,
}

/// A copy of the statement with candidate `cand` elaborated into it. Candidate 0 is the
/// statement as it is.
fn prepare_copy(
	ir: &Ir,
	stmt: OpId,
	eg: &EGraph<ENode, IrAnalysis>,
	cand: &Candidate,
	surgery: &dyn StatementSurgery,
	effects: &EffectCx<'_>,
) -> Result<Prepared, IrError> {
	let (mut copy, map) = surgery.copy_statement(ir, stmt)?;
	let mut copy_facts = FactCache::new();
	let mut all_ops = Vec::new();
	copy.walk(copy.top(), WalkOrder::PostOrder, &mut |op| {
		all_ops.push(op);
		Walk::Continue
	});
	let structural = structural_cx(effects.registry, &copy);
	for op in all_ops {
		let _ = effects::of_with(&copy, &mut copy_facts, &structural, op);
		let _ = effects::of_with(&copy, &mut copy_facts, effects, op);
	}
	let mut created = HashMap::new();
	if cand.index != 0 {
		let built = plan(eg, &cand.term, &map, &copy, &Basis::LOGICAL)
			.map_err(|_| IrError::Stale("an elaboration plan"))?;
		let mut rw = Rewriter::new(&mut copy, &mut copy_facts);
		created.extend(apply_mapped(&built, &mut rw)?);
	}
	Ok(Prepared {
		ir: copy,
		map,
		facts: copy_facts,
		created,
	})
}

/// The classes of the row producers of a statement, split by whether the physical layer may
/// change the order each delivers rows in.
///
/// The licence belongs to a producer's consumer chain, not to its class: hash-consing joins row
/// sources that are equal as row sets, and one of them may feed an order-sensitive consumer. A
/// class therefore holds the licence only when every producer that maps to it, directly or
/// through a join, is order-free.
#[derive(Debug, Default)]
struct OrderLicence {
	free: Vec<Id>,
	held: Vec<Id>,
}

impl OrderLicence {
	/// The licence of the producers `order_free` lists, by the class `imported` gave each one's
	/// first result. A producer the import gave no class has none to mark.
	fn of(ir: &Ir, imported: &Imported, order_free: &[(OpId, bool)]) -> Self {
		let mut licence = Self::default();
		for (op, ok) in order_free {
			let value = ir.op(*op).and_then(|data| data.results.first().copied());
			if let Some(class) = value.and_then(|value| imported.class(value)) {
				if *ok {
					licence.free.push(class);
				} else {
					licence.held.push(class);
				}
			}
		}
		licence
	}

	/// Marks the classes of the order-free producers, which a union of such a class with a seed or
	/// a rule-built node would otherwise clear, and leaves a class alone that any order-sensitive
	/// producer has joined.
	///
	/// A class join keeps `total_order_ok` only when both sides hold it, and a node a seed or a
	/// rule adds starts without it. The flag is set again on the canonical class wherever a join
	/// can have cleared it, before a rule guard or extraction reads it.
	fn mark(&self, eg: &mut EGraph<ENode, IrAnalysis>) {
		let held: Vec<Id> = self.held.iter().map(|class| eg.find(*class)).collect();
		for class in &self.free {
			let canonical = eg.find(*class);
			if !held.contains(&canonical) {
				eg[canonical].data.total_order_ok = true;
			}
		}
	}
}

/// The seed candidates a chosen node belongs to, as a mask with bit `c` set for candidate `c`.
fn seed_mask(eg: &EGraph<ENode, IrAnalysis>, node: &ENodeSnapshot) -> u8 {
	eg.analysis.seeds.get(&identity_of(node)).map_or(0, |seeds| {
		seeds.iter().fold(0u8, |mask, seed| mask | (1u8 << seed.candidate.min(7)))
	})
}

/// The candidate whose converted copy the physical plan is patched into: the one whose seed
/// stands at more island roots, and candidate 0 when the roots tie or no root stands on a seed.
fn winning_candidate(eg: &EGraph<ENode, IrAnalysis>, choice: &PhysChoice) -> u8 {
	let tally = |classes: &[ClassRef]| {
		let (mut zero, mut one) = (0usize, 0usize);
		for class in classes {
			let Some(node) = choice.term.chosen.get(class) else {
				continue;
			};
			match seed_mask(eg, node) {
				0b01 => zero += 1,
				0b10 => one += 1,
				_ => {}
			}
		}
		(zero, one)
	};
	let (mut zero, mut one) = tally(&choice.term.roots);
	if zero == one {
		(zero, one) = tally(&choice.term.order);
	}
	u8::from(one > zero)
}

/// The seeded run: the physical facts, import, round L, one converted copy per candidate, round
/// P, all-physical extraction and the write-back.
async fn plan_seeded(
	ir: &mut Ir,
	facts: &mut FactCache,
	stmt: OpId,
	cx: &PlanStmtCx<'_>,
	rules: &RuleSet,
	host: &dyn SeedHost,
	forced: bool,
) -> EgraphOutcome {
	let mode = Mode::Seeded;
	let limits = limits_for(cx);
	let mut budget = Budget::new(limits);

	// Reads the physical facts of the row sources, and whether each row producer may change its
	// order. The wall clock counts e-graph work only, so it does not run while the physical layer
	// answers.
	budget.pause();
	let (producers, tables) = row_sources(ir, stmt);
	let table_facts = match host.table_facts(&tables).await {
		Ok(held) => held,
		Err(_) => {
			return without_graph(mode, None, Some(FallbackReason::SeedConversionFailed));
		}
	};
	let order_free: Vec<(OpId, bool)> =
		producers.iter().map(|op| (*op, host.order_change_admissible(ir, facts, *op))).collect();
	if !forced {
		let found = scan(ir, stmt, rules);
		let by_rule = (0..rules.len()).any(|rank| {
			found.mask & (1u64 << rank) != 0
				&& rules.rule(rank).is_some_and(|rule| {
					rule.dialect() == RuleDialect::Implementation && !rule.decl().needs_total_order
				})
		});
		if !by_rule && !order_free.iter().any(|(_, ok)| *ok) {
			return EgraphOutcome::skipped(SkipReason::NeedsTotalOrder);
		}
	}

	// Imports the statement and runs round L.
	budget.resume();
	let mut imported: Imported = match import(ir, facts, stmt, &cx.effects, limits) {
		Ok(imported) => imported,
		Err(stop) => return import_stopped(mode, stop),
	};
	for held in table_facts {
		let tables = &mut imported.eg.analysis.tables;
		let merged = match tables.get(&held.table) {
			Some(earlier) => earlier.meet(&held),
			None => held,
		};
		tables.insert(merged.table.clone(), merged);
	}
	let licence = OrderLicence::of(ir, &imported, &order_free);
	licence.mark(&mut imported.eg);
	let round = saturate(
		&mut imported.eg,
		rules,
		RuleDialect::LogicalRewrite,
		TierFilter::Normal,
		ROUND_L_ITERATIONS,
		&mut budget,
		&cx.effects,
	);
	let mut stopped_by = round.stopped_by;
	if let Some(fallback) = stop_fallback(&round) {
		return outcome(mode, &budget, &imported.eg, stopped_by, false, Some(fallback));
	}
	licence.mark(&mut imported.eg);

	// Converts a copy of each candidate and seeds it into the graph. The wall clock
	// does not run while the physical layer converts.
	let found: Vec<Candidate> = logical_candidates(&imported.eg, &imported.roots).into_vec();
	let mut converted: Vec<Converted> = Vec::with_capacity(found.len());
	for cand in &found {
		let Prepared {
			ir: mut copy,
			map,
			facts: mut copy_facts,
			created,
		} = match prepare_copy(ir, stmt, &imported.eg, cand, cx.surgery, &cx.effects) {
			Ok(prepared) => prepared,
			Err(_) if cand.index != 0 => continue,
			Err(_) => {
				return outcome(
					mode,
					&budget,
					&imported.eg,
					stopped_by,
					false,
					Some(FallbackReason::SeedConversionFailed),
				);
			}
		};
		budget.pause();
		let log = host.convert_copy(&mut copy, &mut copy_facts).await;
		let log = match log {
			Ok(log) => log,
			Err(_) if cand.index != 0 => {
				budget.resume();
				continue;
			}
			Err(_) => {
				budget.resume();
				return outcome(
					mode,
					&budget,
					&imported.eg,
					stopped_by,
					false,
					Some(FallbackReason::SeedConversionFailed),
				);
			}
		};
		// A later candidate whose seeding fails is dropped with every union it made, so the graph
		// the remaining candidates seed into is the one they would have met without it.
		let before = (cand.index != 0).then(|| imported.clone());
		let seeded = import_seed(
			&mut imported,
			&SeedCopy {
				ir: &copy,
				map: &map,
				created: &created,
				log: &log,
				candidate: cand.index,
			},
			&mut copy_facts,
			host,
			&cx.effects,
		);
		budget.resume();
		// A seed copy that names what the container does not hold stops the run, as an import
		// that meets one does. A seed or limit stop drops only its candidate.
		if matches!(seeded, Err(ImportStop::Dangling)) {
			return import_stopped(mode, ImportStop::Dangling);
		}
		if seeded.is_err()
			&& let Some(before) = before
		{
			imported = before;
			continue;
		}
		licence.mark(&mut imported.eg);
		if seeded.is_err() {
			return outcome(
				mode,
				&budget,
				&imported.eg,
				stopped_by,
				false,
				Some(FallbackReason::SeedConversionFailed),
			);
		}
		converted.push(Converted {
			candidate: cand.index,
			ir: copy,
			map,
		});
	}

	// Runs round P, and re-saturates when the physical facts warrant it. A stop here
	// answers the default conversion's plan, which needs nothing written. A pass is a round L
	// followed by a round P: re-saturation runs round L again with the explore rules and then
	// round P over the grown graph, and the candidates seeded in step 7 stay the ones extraction
	// may patch.
	let mut round = saturate(
		&mut imported.eg,
		rules,
		RuleDialect::Implementation,
		TierFilter::Normal,
		ROUND_P_ITERATIONS,
		&mut budget,
		&cx.effects,
	);
	stopped_by = stopped_by.or(round.stopped_by);
	let mut halted = round.stopped_by == Some(Limit::Wall) || round.cap_hit;
	let mut passes = 1;
	while !halted && passes < MAX_ROUNDS && resaturate_needed(&imported.eg, passes) {
		let again = saturate(
			&mut imported.eg,
			rules,
			RuleDialect::LogicalRewrite,
			TierFilter::All,
			ROUND_L_ITERATIONS,
			&mut budget,
			&cx.effects,
		);
		stopped_by = stopped_by.or(again.stopped_by);
		if again.cap_hit {
			return outcome(
				mode,
				&budget,
				&imported.eg,
				stopped_by,
				false,
				Some(FallbackReason::MatchCapHit),
			);
		}
		passes += 1;
		if again.stopped_by == Some(Limit::Wall) {
			halted = true;
			break;
		}
		licence.mark(&mut imported.eg);
		round = saturate(
			&mut imported.eg,
			rules,
			RuleDialect::Implementation,
			TierFilter::Normal,
			ROUND_P_ITERATIONS,
			&mut budget,
			&cx.effects,
		);
		stopped_by = stopped_by.or(round.stopped_by);
		halted = round.stopped_by == Some(Limit::Wall) || round.cap_hit;
	}
	if halted {
		let fallback = round.cap_hit.then_some(FallbackReason::MatchCapHit);
		return outcome(mode, &budget, &imported.eg, stopped_by, false, fallback);
	}

	// Extracts the all-physical plan, and writes it back unless the default plan stands.
	licence.mark(&mut imported.eg);
	#[cfg(test)]
	count(|counts| counts.extracted += 1);
	let choice = match extract_phys(&imported.eg, &imported.roots, host.cost()) {
		Ok(choice) => choice,
		Err(fallback) => {
			return outcome(mode, &budget, &imported.eg, stopped_by, false, Some(fallback));
		}
	};
	let winner = winning_candidate(&imported.eg, &choice);
	let Some(target) = converted.into_iter().find(|held| held.candidate == winner) else {
		return outcome(
			mode,
			&budget,
			&imported.eg,
			stopped_by,
			false,
			Some(FallbackReason::WritebackAborted),
		);
	};
	let basis = Basis {
		seed: Some(winner),
		answered: &[],
	};
	let changes = match plan(&imported.eg, &choice.term, &target.map, &target.ir, &basis) {
		Ok(built) => winner != 0 || !built.is_empty(),
		Err(_) => {
			return outcome(
				mode,
				&budget,
				&imported.eg,
				stopped_by,
				false,
				Some(FallbackReason::WritebackAborted),
			);
		}
	};
	let written = write_seeded(
		ir,
		facts,
		stmt,
		&imported.eg,
		&choice,
		SeedTarget {
			ir: target.ir,
			map: target.map,
			candidate: winner,
		},
		cx.surgery,
	);
	match written {
		Ok(()) => outcome(mode, &budget, &imported.eg, stopped_by, changes, None),
		Err(fallback) => outcome(mode, &budget, &imported.eg, stopped_by, false, Some(fallback)),
	}
}

#[cfg(test)]
mod unit_tests {
	use std::collections::HashMap;

	use egg::EGraph;
	use smallvec::smallvec;

	use super::analysis::{IrAnalysis, SeedInfo};
	use super::decl::{ClassRef, EpochKey, LeafId, NodeIdentity, NodeShape, OrderSig};
	use super::extract::{PhysChoice, Term};
	use super::lang::{ENode, snapshot_of};
	use super::winning_candidate;
	use crate::exec::ir::iface::ids::{FrameId, OpId, SnapshotId};

	fn leaf_node(leaf: u32) -> ENode {
		ENode {
			id: NodeIdentity {
				shape: NodeShape::Opaque {
					leaf: LeafId(leaf),
				},
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				epoch: EpochKey::None,
			},
			kids: smallvec![],
		}
	}

	#[test]
	fn winning_candidate_falls_back_to_term_order_when_roots_tie() {
		let mut eg: EGraph<ENode, IrAnalysis> = EGraph::new(IrAnalysis::new());
		let root_node = leaf_node(0);
		let child_node = leaf_node(1);
		// Root node is unseeded (e.g. a rule-built wrapper at the island root), while the
		// child node inside `term.order` comes exclusively from candidate 1's seed copy.
		eg.analysis.seeds.insert(
			child_node.clone(),
			smallvec![SeedInfo {
				candidate: 1,
				op: OpId::default(),
				order: OrderSig::Unordered,
			}],
		);

		let root_class = ClassRef(0);
		let child_class = ClassRef(1);
		let mut chosen = HashMap::new();
		chosen.insert(root_class, snapshot_of(&root_node));
		chosen.insert(child_class, snapshot_of(&child_node));
		let choice = PhysChoice {
			term: Term {
				roots: smallvec![root_class],
				order: vec![root_class, child_class],
				chosen,
			},
		};
		assert_eq!(winning_candidate(&eg, &choice), 1);
	}
}
