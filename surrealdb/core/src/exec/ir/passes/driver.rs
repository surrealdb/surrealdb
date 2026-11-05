//! The loop optimiser's driver: the pass interface, the per-run configuration and the loop that
//! runs the passes in order.
//!
//! The container stays in the logical phase for the whole run; the caller owns every phase
//! boundary. A pass rewrites only through the rewriter, leaves a container that verifies, and
//! never moves or merges an op across a `Statement` region. A pass that stops at a bound records
//! it in its report and leaves a valid container: a bound is never an error.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::reg::lo_passes;
use crate::exec::ir::analysis::tokens;
use crate::exec::ir::core::census::Census;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::core::verify::{VerifyAt, VerifyError, verify};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::{LOGICAL_PHASE, PHYSICAL_PHASE, Phase};
use crate::exec::ir::iface::fold::{FoldConsumer, FoldRequest, PlanTimeFolder};
use crate::exec::ir::iface::ids::ValueId;

/// Which passes of the loop optimiser run.
///
/// There is no run-time switch: the planner runs [`PassConfig::PRODUCTION`] and tests build a
/// configuration directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PassConfig {
	pub(crate) specialise_guards: bool,
	pub(crate) inline_calls: bool,
	pub(crate) fold: bool,
	pub(crate) dce: bool,
	/// Removal of the predecessors of a raise that is certain.
	pub(crate) dce_certain_error: bool,
	pub(crate) cse: bool,
	pub(crate) push_into_subquery: bool,
	pub(crate) push_below_layer: bool,
	pub(crate) push_below_permission: bool,
	pub(crate) unroll: bool,
	pub(crate) unswitch: bool,
	pub(crate) hoist_leading: bool,
	pub(crate) licm: bool,
	/// Folds that read the planning catalog.
	pub(crate) fold_phase_b: bool,
}

impl PassConfig {
	/// What the planner runs. The passes that move a conjunct across a permission filter, copy
	/// loops, remove what precedes a certain raise or need a catalog the planning session does
	/// not hold are off.
	pub(crate) const RELEASE: Self = Self {
		specialise_guards: true,
		inline_calls: true,
		fold: true,
		dce: true,
		dce_certain_error: false,
		cse: true,
		push_into_subquery: true,
		push_below_layer: true,
		push_below_permission: false,
		unroll: false,
		unswitch: false,
		hoist_leading: true,
		licm: true,
		fold_phase_b: false,
	};

	/// What a production request runs: every pass of [`Self::RELEASE`]. The physical golden plans
	/// check that none of these passes changes a plan the planner builds for the same statement.
	pub(crate) const PRODUCTION: Self = Self::RELEASE;

	/// Every pass on.
	pub(crate) const ALL_ON: Self = Self {
		specialise_guards: true,
		inline_calls: true,
		fold: true,
		dce: true,
		dce_certain_error: true,
		cse: true,
		push_into_subquery: true,
		push_below_layer: true,
		push_below_permission: true,
		unroll: true,
		unswitch: true,
		hoist_leading: true,
		licm: true,
		fold_phase_b: true,
	};

	/// Every pass off: a run leaves the container as it found it.
	pub(crate) const ALL_OFF: Self = Self {
		specialise_guards: false,
		inline_calls: false,
		fold: false,
		dce: false,
		dce_certain_error: false,
		cse: false,
		push_into_subquery: false,
		push_below_layer: false,
		push_below_permission: false,
		unroll: false,
		unswitch: false,
		hoist_leading: false,
		licm: false,
		fold_phase_b: false,
	};
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PassReport {
	/// The rewrites the pass applied.
	pub(crate) rewrites: u32,
	/// The bound the pass stopped at, or `None` when it finished.
	pub(crate) stopped_by: Option<Limit>,
	/// Memo candidates found and not applied, because the container has no memo op.
	pub(crate) memo_candidates_unapplied: u32,
}

/// Told of each folded value a decision consumes.
pub(crate) trait FoldObserver: Send {
	/// Called once per folded value a decision consumes, before the rewrite replaces the value's
	/// defining ops, so a recorder can walk them back to their roots.
	fn consumed(&mut self, ir: &Ir, value: ValueId, consumer: FoldConsumer);
}

/// An observer that records nothing.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoObserver;

impl FoldObserver for NoObserver {
	fn consumed(&mut self, _ir: &Ir, _value: ValueId, _consumer: FoldConsumer) {}
}

/// What a pass reads and rewrites.
pub(crate) struct LoCx<'a> {
	pub(crate) ir: &'a mut Ir,
	pub(crate) facts: &'a mut FactCache,
	pub(crate) folder: &'a dyn PlanTimeFolder,
	pub(crate) effects: &'a EffectCx<'a>,
	pub(crate) cfg: &'a PassConfig,
	pub(crate) observer: &'a mut dyn FoldObserver,
}

/// One pass of the loop optimiser.
pub(crate) trait LoPass: Send + Sync {
	/// The name the pass reports under and a verification failure names.
	fn name(&self) -> &'static str;

	/// Whether `cfg` turns the pass on.
	fn enabled(&self, cfg: &PassConfig) -> bool;

	/// Whether the pass can find a candidate in a container with this `census`. The driver skips,
	/// like a disabled pass, a pass that answers `false`.
	///
	/// A pass answers `false` only when its candidate search matches on tags and flags the census
	/// lacks, and a run that finds no candidate leaves the container and the fact cache untouched
	/// and reports [`PassReport::default`]. The answer is stated with its proof on each override.
	/// The default is `true`: the pass always runs.
	fn needs(&self, _census: &Census) -> bool {
		true
	}

	/// Runs the pass over every statement of the container.
	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a>;
}

/// What [`LoPass::run`] answers: the report of a pass that ran to completion, or the future of a
/// pass that awaits plan-time folds. Awaiting it answers the report either way, and a pass that
/// completes without awaiting is not boxed.
pub(crate) enum PassRun<'a> {
	/// The report, taken by the first poll.
	Done(Option<Result<PassReport, IrError>>),
	Pending(Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>>),
}

impl PassRun<'_> {
	/// The run of a pass that has completed with `report`.
	pub(crate) fn done(report: Result<PassReport, IrError>) -> Self {
		Self::Done(Some(report))
	}
}

impl Future for PassRun<'_> {
	type Output = Result<PassReport, IrError>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.get_mut() {
			Self::Done(report) => Poll::Ready(
				report.take().unwrap_or(Err(IrError::Stale("pass run polled after completion"))),
			),
			Self::Pending(future) => future.as_mut().poll(cx),
		}
	}
}

/// Runs the loop optimiser over every statement of `ir`, in the order of [`lo_passes`], and
/// answers one report per pass.
///
/// A pass `cfg` turns off, or whose [`LoPass::needs`] the census of `ir` refuses, is skipped
/// without touching `ir`, and its report is the default one. The census is built before the first
/// pass that runs and again after each pass that reports a rewrite.
/// After each pass that ran, the folds it left requested are awaited through `folder`, so the next
/// pass reads their outcomes, and a debug build verifies the container, naming the pass in the
/// error. A release build leaves verification to the caller's phase boundary.
pub(crate) async fn run_lo(
	ir: &mut Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	effects: &EffectCx<'_>,
	cfg: &PassConfig,
	observer: &mut dyn FoldObserver,
) -> Result<Vec<(&'static str, PassReport)>, IrError> {
	run_passes(lo_passes(), ir, facts, folder, effects, cfg, observer).await
}

/// [`run_lo`] over the given passes, in the given order.
pub(super) async fn run_passes(
	passes: &[Box<dyn LoPass>],
	ir: &mut Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	effects: &EffectCx<'_>,
	cfg: &PassConfig,
	observer: &mut dyn FoldObserver,
) -> Result<Vec<(&'static str, PassReport)>, IrError> {
	let mut reports = Vec::with_capacity(passes.len());
	let mut census: Option<Census> = None;
	for pass in passes {
		let name = pass.name();
		if !pass.enabled(cfg) {
			reports.push((name, PassReport::default()));
			continue;
		}
		let held = census.get_or_insert_with(|| Census::of(ir));
		if !gated_in(pass.as_ref(), held) {
			reports.push((name, PassReport::default()));
			continue;
		}
		let marks = ir.is_threaded().then(|| tokens::mark(ir));
		let report = {
			let mut cx = LoCx {
				ir: &mut *ir,
				facts: &mut *facts,
				folder,
				effects,
				cfg,
				observer: &mut *observer,
			};
			pass.run(&mut cx).await?
		};
		settle_folds(ir, facts, folder, effects).await;
		if report.rewrites > 0
			&& ir.is_threaded()
			&& let Some(marks) = &marks
		{
			tokens::rethread_changed(ir, facts, effects, marks)?;
		}
		verify_after(ir, facts, effects, name)?;
		// A pass reports every rewrite it applies, so a report of none leaves the census as it
		// was; a debug build checks that no tag or flag appeared.
		if report.rewrites > 0 {
			census = None;
		} else {
			debug_assert!(
				census.as_ref().is_none_or(|held| held.covers(&Census::of(ir))),
				"pass {name} changed the container and reported no rewrite"
			);
		}
		reports.push((name, report));
	}
	Ok(reports)
}

/// Whether `pass` runs over a container with `census`.
fn gated_in(pass: &dyn LoPass, census: &Census) -> bool {
	#[cfg(test)]
	if ungated::forced() {
		return true;
	}
	pass.needs(census)
}

/// Awaits every fold a pass left requested and stores its outcome. A request whose value the pass
/// erased is left alone.
async fn settle_folds(
	ir: &Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	effects: &EffectCx<'_>,
) {
	let pending: Vec<FoldRequest> =
		facts.pending_folds().filter(|req| ir.value(req.value).is_some()).collect();
	if pending.is_empty() {
		return;
	}
	let outcomes = folder.fold_all(ir, effects, &pending).await;
	for (req, outcome) in pending.iter().zip(outcomes) {
		facts.store_fold(req.value, outcome);
	}
}

/// In a debug build, verifies `ir` against the legality target of its phase and names `pass` in the
/// error. A release build answers `Ok`.
fn verify_after(
	ir: &Ir,
	facts: &mut FactCache,
	effects: &EffectCx<'_>,
	pass: &'static str,
) -> Result<(), IrError> {
	if !cfg!(debug_assertions) {
		return Ok(());
	}
	let target = match ir.phase() {
		Phase::Bound | Phase::Logical => &LOGICAL_PHASE,
		Phase::Physical | Phase::Lowered => &PHYSICAL_PHASE,
	};
	verify(ir, facts, VerifyAt::AfterPass(pass), target, effects).map_err(|error| {
		IrError::Verify(VerifyError {
			detail: format!("after pass {pass}: {}", error.detail),
			..error
		})
	})
}

/// A switch that runs every enabled pass whatever the census says, so a test compares the gated
/// run with the ungated one. It holds for the current thread, which runs a test's future.
#[cfg(test)]
pub(super) mod ungated {
	use std::cell::Cell;

	thread_local! {
		static FORCED: Cell<bool> = const { Cell::new(false) };
	}

	pub(super) fn forced() -> bool {
		FORCED.with(Cell::get)
	}

	/// Forces the passes ungated until the guard drops.
	pub(in crate::exec::ir) struct Guard(bool);

	pub(in crate::exec::ir) fn force() -> Guard {
		Guard(FORCED.with(|forced| forced.replace(true)))
	}

	impl Drop for Guard {
		fn drop(&mut self) {
			FORCED.with(|forced| forced.set(self.0));
		}
	}
}
