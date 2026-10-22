//! Patterns and the greedy driver.
//!
//! A pattern registered for a dialect is offered only ops whose tag is the dialect's tag: a
//! logical pattern may create expression and logical ops only, a physical pattern expression and
//! physical ops only. A driver that reaches a limit stops, reports it and returns the container
//! as it stands, which is valid because every pattern leaves a valid container. A limit is never
//! an error.

use std::cmp::Reverse;

use super::facts::FactCache;
use super::ir::{Ir, Walk, WalkOrder};
use super::rewriter::Rewriter;
use super::verify::{VerifyError, VerifyRule};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::{Dialect, DialectSet};
use crate::exec::ir::iface::fold::{FoldRequest, PlanTimeFolder};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::op_kind::OpKind;

/// A rewrite rooted at one op tag of dialect `D`.
pub(crate) trait Pattern<D: Dialect>: Send + Sync {
	fn name(&self) -> &'static str;
	/// The tag of the op the pattern is offered.
	fn root(&self) -> D::Tag;
	/// Patterns with a higher benefit are tried first.
	fn benefit(&self) -> u16;
	/// `Ok(true)` when it rewrote.
	///
	/// Reads facts only through the rewriter and never reads stored rows. A pattern that needs a
	/// folded value requests it through the rewriter and answers `Ok(false)`; it reads the outcome
	/// on a later pass.
	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError>;
}

/// The patterns of one dialect, indexed by the tag they root at.
pub(crate) struct PatternSet<D: Dialect> {
	by_root: Vec<(D::Tag, Box<dyn Pattern<D>>)>,
}

impl<D: Dialect> PatternSet<D> {
	/// An empty set.
	pub(crate) fn new() -> Self {
		Self {
			by_root: Vec::new(),
		}
	}

	/// Adds `pattern`, indexed by its root.
	pub(crate) fn add(&mut self, pattern: Box<dyn Pattern<D>>) {
		self.by_root.push((pattern.root(), pattern));
	}

	/// The patterns rooted at `tag`, in the order they were added.
	pub(crate) fn rooted_at(&self, tag: D::Tag) -> impl Iterator<Item = &dyn Pattern<D>> {
		self.by_root.iter().filter(move |(root, _)| *root == tag).map(|(_, pattern)| &**pattern)
	}

	/// The patterns with their roots, highest benefit first and ties in the order they were added.
	fn by_benefit(&self) -> Vec<(D::Tag, &dyn Pattern<D>)> {
		let mut ordered: Vec<_> =
			self.by_root.iter().map(|(root, pattern)| (*root, &**pattern)).collect();
		ordered.sort_by_key(|(_, pattern)| Reverse(pattern.benefit()));
		ordered
	}

	/// How many patterns the set holds.
	pub(crate) fn len(&self) -> usize {
		self.by_root.len()
	}

	/// Whether the set holds no pattern.
	pub(crate) fn is_empty(&self) -> bool {
		self.by_root.is_empty()
	}
}

impl<D: Dialect> Default for PatternSet<D> {
	fn default() -> Self {
		Self::new()
	}
}

/// The bounds of one greedy run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GreedyLimits {
	pub(crate) max_passes: u8,
	pub(crate) max_rewrites: u32,
}

/// The default bounds of one greedy run.
pub(crate) const GREEDY_DEFAULT: GreedyLimits = GreedyLimits {
	max_passes: 8,
	max_rewrites: 10_000,
};

/// The limit a bounded driver stopped at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Limit {
	Passes,
	Rewrites,
	Nodes,
	Iterations,
	Wall,
}

/// What one greedy run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GreedyReport {
	pub(crate) passes: u8,
	pub(crate) rewrites: u32,
	/// The limit the run stopped at, or `None` when it reached a fixed point.
	pub(crate) stopped_by: Option<Limit>,
}

/// Applies the patterns of `set` to `ir` until none rewrites or a limit of `limits` is reached.
///
/// A pass offers each live op, in pre-order, the patterns rooted at its tag: highest benefit
/// first, ties in the order they were added, until one answers `Ok(true)`. Between passes the
/// driver awaits every fold the patterns requested through `folder` and stores the outcomes, so a
/// pattern reads them on the next pass; those passes count toward `limits.max_passes`. After each
/// pass every op the patterns created must belong to the dialects the set's dialect may create,
/// otherwise the run answers a `Legality` verification error. The awaited folds are judged under
/// `registry`, the function registry of the datastore the container is planned for.
///
/// The driver never changes the container's phase and never verifies it; the caller verifies at
/// the phase boundary. A pattern's error is returned as it is.
pub(crate) async fn apply_greedy<D: Dialect>(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &PatternSet<D>,
	folder: &dyn PlanTimeFolder,
	registry: &FunctionRegistry,
	limits: GreedyLimits,
) -> Result<GreedyReport, IrError> {
	let mut report = GreedyReport {
		passes: 0,
		rewrites: 0,
		stopped_by: None,
	};
	if limits.max_passes == 0 {
		report.stopped_by = Some(Limit::Passes);
		return Ok(report);
	}
	loop {
		report.passes += 1;
		let pass = run_pass(
			ir,
			facts,
			set,
			registry,
			limits.max_rewrites.saturating_sub(report.rewrites),
		)?;
		report.rewrites += pass.rewrites;
		if pass.limit_reached {
			report.stopped_by = Some(Limit::Rewrites);
			return Ok(report);
		}
		let pending: Vec<FoldRequest> = facts.pending_folds().collect();
		if pass.rewrites == 0 && pending.is_empty() {
			return Ok(report);
		}
		if report.passes == limits.max_passes {
			report.stopped_by = Some(Limit::Passes);
			return Ok(report);
		}
		await_folds(ir, facts, folder, &structural_cx(registry, ir), pending).await;
	}
}

/// What one pass did.
struct PassOutcome {
	rewrites: u32,
	/// Whether the pass stopped because the rewrite budget was spent.
	limit_reached: bool,
}

/// Runs one pass over the live ops of `ir` with at most `budget` rewrites.
///
/// The ids are collected before the first offer, so an op a pattern creates is offered on the
/// next pass, and an op erased earlier in the pass is skipped. Ops inside a stored-clause region
/// are never offered.
fn run_pass<D: Dialect>(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &PatternSet<D>,
	registry: &FunctionRegistry,
	budget: u32,
) -> Result<PassOutcome, IrError> {
	let mut ids = super::OpList::with_capacity(ir.ops.len());
	ir.walk_outside_stored(ir.top(), WalkOrder::PreOrder, &mut |op| {
		ids.push(op);
		Walk::Continue
	});
	let ordered = set.by_benefit();
	let mut rw = Rewriter::new(ir, facts).with_registry(registry);
	let mut outcome = PassOutcome {
		rewrites: 0,
		limit_reached: false,
	};
	'ops: for id in ids {
		let Some(tag) = rw.ir().op(id).and_then(|data| D::tag_of(&data.kind)) else {
			continue;
		};
		for (_, pattern) in ordered.iter().filter(|(root, _)| *root == tag) {
			if outcome.rewrites >= budget {
				outcome.limit_reached = true;
				break 'ops;
			}
			if pattern.apply(id, &mut rw)? {
				outcome.rewrites += 1;
				if outcome.rewrites >= budget {
					outcome.limit_reached = true;
					break 'ops;
				}
				continue 'ops;
			}
		}
	}
	check_created::<D>(&mut rw)?;
	Ok(outcome)
}

/// The dialects the patterns of the dialect named `name` may create.
fn created_dialects(name: &str) -> DialectSet {
	match name {
		"logical" => DialectSet::EXPR.union(DialectSet::LOGICAL),
		"phys" => DialectSet::EXPR.union(DialectSet::PHYS),
		_ => DialectSet::EXPR,
	}
}

/// The dialect `kind` belongs to.
pub(super) fn dialect_of(kind: &OpKind) -> DialectSet {
	match kind {
		OpKind::Expr(_) => DialectSet::EXPR,
		OpKind::Logical(_) => DialectSet::LOGICAL,
		OpKind::Phys(_) => DialectSet::PHYS,
	}
}

/// Answers a `Legality` violation when a pattern of `D` created an op outside the dialects `D`
/// may create. An op created and erased within the pass is not inspected.
fn check_created<D: Dialect>(rw: &mut Rewriter<'_>) -> Result<(), IrError> {
	let allowed = created_dialects(D::NAME);
	for op in rw.take_created() {
		let Some(data) = rw.ir().op(op) else {
			continue;
		};
		if !allowed.contains(dialect_of(&data.kind)) {
			return Err(IrError::Verify(VerifyError {
				op: Some(op),
				rule: VerifyRule::Legality,
				detail: format!("a {} pattern created an op of another dialect", D::NAME),
			}));
		}
	}
	Ok(())
}

/// Awaits the requests of `pending` through `folder` in one batch and stores each outcome.
pub(super) async fn await_folds(
	ir: &Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	cx: &EffectCx<'_>,
	pending: Vec<FoldRequest>,
) {
	if pending.is_empty() {
		return;
	}
	let outcomes = folder.fold_all(ir, cx, &pending).await;
	for (req, outcome) in pending.iter().zip(outcomes) {
		facts.store_fold(req.value, outcome);
	}
}
