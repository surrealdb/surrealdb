//! The e-graph runner: saturation within the limits of the e-graph's configuration.
//!
//! A run that reaches a limit stops, reports the limit and leaves the graph as it stands, which
//! is sound because every rule preserves equality. A limit is never an error.
//!
//! The runner drives [`CompiledRule::search`] and [`CompiledRule::apply`] and egg's
//! `EGraph::rebuild`. It constructs neither `egg::Runner` nor `egg::Rewrite`: egg's runner reads
//! a clock whatever the limits, truncates a search in an order that depends on class ids, and
//! carries no bound node into an applier. No per-request datum is interned as an `egg::Symbol`.
//!
//! Every limit is checked between rules and never inside one, so a rule's matches are all found
//! before any is applied and the graph a run stops at is the graph after a whole rule. A graph
//! this module returns is rebuilt: its congruence is restored, its class index is current and the
//! keys of [`IrAnalysis::seeds`] are canonical.

use egg::EGraph;
use web_time::{Duration, Instant};

use super::analysis::{IrAnalysis, recanonicalise_seeds};
use super::decl::RuleTier;
use super::lang::ENode;
use super::rule::{CompiledRule, RuleSet};
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::egraph::{EgraphLimits, RuleDialect};

/// The most iterations the logical rewrite round may run.
pub(crate) const ROUND_L_ITERATIONS: usize = 6;

/// The most iterations the implementation round may run.
pub(crate) const ROUND_P_ITERATIONS: usize = 2;

/// The limits of one statement's planning and what has been spent against them.
///
/// The iteration limit is shared by every round of the statement, the node limit applies to the
/// graph as it stands, and the wall limit counts the time the planner spends on the e-graph:
/// the time between [`Budget::pause`] and [`Budget::resume`] is not counted.
#[derive(Clone, Debug)]
pub(crate) struct Budget {
	pub(crate) start: Instant,
	pub(crate) limits: EgraphLimits,
	/// The iterations every round so far has run.
	pub(crate) iterations: usize,
	/// The rounds that have run.
	pub(crate) rounds: usize,
	/// The time spent paused, over every completed pause.
	pub(crate) paused: Duration,
	/// When the pause in force began.
	pause_began: Option<Instant>,
}

impl Budget {
	/// A budget of `limits`, started now.
	pub(crate) fn new(limits: EgraphLimits) -> Self {
		Self {
			start: Instant::now(),
			limits,
			iterations: 0,
			rounds: 0,
			paused: Duration::ZERO,
			pause_began: None,
		}
	}

	/// Stops the wall clock. A pause in force stays as it is.
	pub(crate) fn pause(&mut self) {
		if self.pause_began.is_none() {
			self.pause_began = Some(Instant::now());
		}
	}

	/// Restarts the wall clock, leaving the paused time out of what it counts. Without a pause in
	/// force it does nothing.
	pub(crate) fn resume(&mut self) {
		if let Some(began) = self.pause_began.take() {
			self.paused += Instant::now().saturating_duration_since(began);
		}
	}

	/// The time spent on the e-graph so far: the time since the start, less the time paused. The
	/// count stands still while a pause is in force.
	pub(crate) fn active(&self) -> Duration {
		let now = Instant::now();
		let idle = self.paused
			+ self.pause_began.map_or(Duration::ZERO, |began| now.saturating_duration_since(began));
		now.saturating_duration_since(self.start).saturating_sub(idle)
	}

	/// Whether the wall limit is passed. A limit of zero is passed at every check.
	pub(crate) fn wall_exceeded(&self) -> bool {
		self.limits.wall.is_zero() || self.active() > self.limits.wall
	}

	/// The iterations the statement may still run.
	pub(crate) fn iterations_left(&self) -> usize {
		self.limits.iterations.saturating_sub(self.iterations)
	}
}

/// Which rules of a dialect a round runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TierFilter {
	/// The [`RuleTier::Normal`] rules.
	Normal,
	/// Every rule, [`RuleTier::Explore`] included.
	All,
}

impl TierFilter {
	/// Whether a rule of `tier` runs.
	pub(crate) fn admits(self, tier: RuleTier) -> bool {
		match self {
			Self::Normal => tier == RuleTier::Normal,
			Self::All => true,
		}
	}
}

/// What one round did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RoundReport {
	/// The iterations the round started.
	pub(crate) iterations: usize,
	/// The limit the round stopped at, or `None` when it saturated. A rule that reached its match
	/// cap stops the round with `Some(Limit::Iterations)` and sets `cap_hit`.
	pub(crate) stopped_by: Option<Limit>,
	/// An iteration added no union and no node.
	pub(crate) saturated: bool,
	/// A rule's search stopped at its match cap. The round's matches are then incomplete, so no
	/// alternative it found may be chosen.
	pub(crate) cap_hit: bool,
}

/// The limit `eg` has reached under `budget`: the wall first, then the node count.
///
/// A graph that holds as many nodes as the limit allows has reached it.
fn limit_reached(eg: &EGraph<ENode, IrAnalysis>, budget: &Budget) -> Option<Limit> {
	if budget.wall_exceeded() {
		Some(Limit::Wall)
	} else if eg.total_number_of_nodes() >= budget.limits.nodes {
		Some(Limit::Nodes)
	} else {
		None
	}
}

/// Restores `eg`'s congruence and class index, and the canonical form of its seeds, when an add
/// or a union has made it stale.
fn settle(eg: &mut EGraph<ENode, IrAnalysis>) {
	if !eg.clean {
		eg.rebuild();
		recanonicalise_seeds(eg);
	}
}

/// Runs the rules of `dialect` and `tier` over `eg` until the graph saturates or a limit is
/// reached, and answers what the round did.
///
/// Rules run in registration order. A rule's search finishes before any of its matches is
/// applied, the matches apply in the order found, and the graph is rebuilt before the next rule
/// searches. Before every iteration and after every rule, the wall clock and the node count are
/// checked. A round runs at most `max_iterations` iterations, and no more than the statement's
/// iteration limit leaves: reaching either stops the round with `Limit::Iterations`. The round is
/// saturated when an iteration applies no rule that adds a node or joins two classes.
///
/// A rule whose search reaches its match cap stops the round at once with `cap_hit` set and none
/// of that rule's matches applied: the caller answers as it does for a wall stop.
///
/// `eg` is rebuilt on return, whatever stopped the round. Every union goes through
/// `guarded_union` inside [`CompiledRule::apply`], and the refusals are counted in
/// [`IrAnalysis::union_refused`].
pub(crate) fn saturate(
	eg: &mut EGraph<ENode, IrAnalysis>,
	rules: &RuleSet,
	dialect: RuleDialect,
	tier: TierFilter,
	max_iterations: usize,
	budget: &mut Budget,
	cx: &EffectCx<'_>,
) -> RoundReport {
	let active: Vec<&CompiledRule> = rules
		.of_dialect(dialect)
		.map(|(_, rule)| rule)
		.filter(|rule| tier.admits(rule.tier()))
		.collect();
	let cap = max_iterations.min(budget.iterations_left());
	let mut report = RoundReport {
		iterations: 0,
		stopped_by: None,
		saturated: false,
		cap_hit: false,
	};
	settle(eg);
	'round: loop {
		if let Some(limit) = limit_reached(eg, budget) {
			report.stopped_by = Some(limit);
			break;
		}
		if report.iterations >= cap {
			report.stopped_by = Some(Limit::Iterations);
			break;
		}
		report.iterations += 1;
		budget.iterations += 1;
		let mut changed = false;
		for rule in &active {
			let (matches, capped) = rule.search(eg);
			if capped {
				report.cap_hit = true;
				report.stopped_by = Some(Limit::Iterations);
				break 'round;
			}
			for m in &matches {
				let applied = rule.apply(eg, m, cx);
				changed |= applied.unioned || applied.added;
			}
			settle(eg);
			if let Some(limit) = limit_reached(eg, budget) {
				report.stopped_by = Some(limit);
				break 'round;
			}
		}
		if !changed {
			report.saturated = true;
			break;
		}
	}
	settle(eg);
	budget.rounds += 1;
	report
}

/// Whether the physical facts from a planning pass call for another logical round.
///
/// The answer is `true` when a class carries an estimate, fewer than `MAX_ROUNDS` passes have
/// run, and the cost hook marks that estimate significant. A `true` answer re-runs the logical
/// round with the [`RuleTier::Explore`] rules enabled for those classes, then the implementation
/// round over the grown graph, before extraction. `passes` counts the passes run so far, a pass
/// being the logical round followed by the implementation round. Logical rules never read
/// physical facts: the facts choose which classes are explored and which alternative extraction
/// picks. The answer is read at plan time, after the implementation round, never during a run.
///
/// Returns `false` for every input, since no code path produces an estimate.
pub(crate) fn resaturate_needed(_eg: &EGraph<ENode, IrAnalysis>, _passes: usize) -> bool {
	false
}
