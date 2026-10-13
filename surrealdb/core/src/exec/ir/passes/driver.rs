//! The loop optimiser's driver: the pass interface, the per-run configuration and the loop that
//! runs the passes in order.
//!
//! The container stays in the logical phase for the whole run; the caller owns every phase
//! boundary. A pass rewrites only through the rewriter, leaves a container that verifies, and
//! never moves or merges an op across a `Statement` region. A pass that stops at a bound records
//! it in its report and leaves a valid container: a bound is never an error.

use std::future::Future;
use std::pin::Pin;

use super::reg::lo_passes;
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
	pub(crate) fold: bool,
	pub(crate) dce: bool,
	/// Removal of the predecessors of a raise that is certain.
	pub(crate) dce_certain_error: bool,
	pub(crate) cse: bool,
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
	/// What the planner runs. Passes that reorder a raise's neighbours, copy loops or need a
	/// catalog the planning session does not hold are off.
	pub(crate) const RELEASE: Self = Self {
		specialise_guards: true,
		fold: true,
		dce: true,
		dce_certain_error: false,
		cse: true,
		push_below_layer: false,
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
		fold: true,
		dce: true,
		dce_certain_error: true,
		cse: true,
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
		fold: false,
		dce: false,
		dce_certain_error: false,
		cse: false,
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

	/// Runs the pass over every statement of the container.
	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>>;
}

/// Runs the loop optimiser over every statement of `ir`, in the order of [`lo_passes`], and
/// answers one report per pass.
///
/// A pass `cfg` turns off is skipped without touching `ir`, and its report is the default one.
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
	run_passes(&lo_passes(), ir, facts, folder, effects, cfg, observer).await
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
	for pass in passes {
		let name = pass.name();
		if !pass.enabled(cfg) {
			reports.push((name, PassReport::default()));
			continue;
		}
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
		verify_after(ir, facts, effects, name)?;
		reports.push((name, report));
	}
	Ok(reports)
}

/// Awaits every fold a pass left requested and stores its outcome. A request whose value the pass
/// erased is left alone.
async fn settle_folds(
	ir: &Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	effects: &EffectCx<'_>,
) {
	let pending: Vec<FoldRequest> = facts.pending_folds().collect();
	for req in pending {
		if ir.value(req.value).is_none() {
			continue;
		}
		let outcome = folder.fold(ir, effects, req).await;
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
