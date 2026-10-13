//! Dialect conversion: logical ops to physical ops.
//!
//! `convert` leaves no op outside the target's dialects, except the ops inside stored-clause
//! regions, which are never lowered, and every logical op id is in some physical op's provenance
//! or in the container's eliminated list. A driver that reaches a limit
//! stops, reports it and returns the container as it stands; a limit is never an error.

use super::facts::FactCache;
use super::ir::{Ir, Walk, WalkOrder};
use super::pattern::{Limit, await_folds, dialect_of};
use super::rewriter::Rewriter;
use super::verify::{VerifyError, VerifyRule};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::conversion::{
	Dialect, EliminationReason, LegalityTarget, LogicalDialect,
};
use crate::exec::ir::iface::fold::{FoldRequest, PlanTimeFolder};
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::logical::ops::LogTag;

/// A conversion of one logical op tag to physical ops.
pub(crate) trait ConversionPattern: Send + Sync {
	fn name(&self) -> &'static str;
	/// The logical tag the pattern converts.
	fn source(&self) -> LogTag;
	/// Converts `op`, consulting the physical catalog through `cx`.
	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		cx: &PhysCx<'_>,
	) -> Result<Converted, IrError>;
}

/// What a conversion did to the op it was offered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Converted {
	/// The op was replaced by physical ops.
	Replaced,
	/// The op was removed without replacement, for the reason.
	Eliminated(EliminationReason),
	/// The pattern does not apply to this op.
	NotApplicable,
}

/// The conversion patterns, indexed by the logical tag they convert.
#[derive(Default)]
pub(crate) struct ConversionSet {
	by_source: Vec<(LogTag, Box<dyn ConversionPattern>)>,
}

impl ConversionSet {
	/// An empty set.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Adds `pattern`, indexed by its source.
	pub(crate) fn add(&mut self, pattern: Box<dyn ConversionPattern>) {
		self.by_source.push((pattern.source(), pattern));
	}

	/// The patterns converting `tag`, in the order they were added.
	pub(crate) fn for_source(&self, tag: LogTag) -> impl Iterator<Item = &dyn ConversionPattern> {
		self.by_source
			.iter()
			.filter(move |(source, _)| *source == tag)
			.map(|(_, pattern)| &**pattern)
	}

	/// How many patterns the set holds.
	pub(crate) fn len(&self) -> usize {
		self.by_source.len()
	}

	/// Whether the set holds no pattern.
	pub(crate) fn is_empty(&self) -> bool {
		self.by_source.is_empty()
	}
}

/// What one conversion run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConversionReport {
	pub(crate) converted: u32,
	pub(crate) eliminated: u32,
	/// The limit the run stopped at, or `None` when no op outside the target's dialects remains
	/// outside stored-clause regions.
	pub(crate) stopped_by: Option<Limit>,
}

/// Converts the ops of `ir` outside `target.legal` with the patterns of `set`.
///
/// A round offers each such logical op, in post-order, the patterns of its tag in the order they
/// were added, until one answers other than `NotApplicable`; the ops of an op's regions and the
/// ops it reads are converted before the op is offered. Each pattern runs inside
/// [`Rewriter::derived`] on the op it converts, so what it creates records that op. The ops a
/// pattern creates must lie in `target.legal`, or the run answers a `Legality` verification error.
/// Each `Replaced` or `Eliminated` answer removes at least one op outside the target, so the run
/// ends.
///
/// A `Replaced` answer requires the op to be gone, and an `Eliminated` answer erases the op with
/// the reason when the pattern has not. Folds the patterns requested are awaited through `folder`
/// in `cx.effects` between rounds. A round that converts nothing while no fold is pending ends
/// the run with `Limit::Iterations`; the container is returned as it stands and the caller's
/// phase-boundary verification reports the ops left outside the target.
///
/// The driver never changes the container's phase. A pattern's error is returned as it is.
pub(crate) async fn convert(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &ConversionSet,
	cx: &PhysCx<'_>,
	folder: &dyn PlanTimeFolder,
	target: &LegalityTarget,
) -> Result<ConversionReport, IrError> {
	drive(ir, facts, set, cx, folder, target, None).await
}

/// [`convert`], also answering the conversion's replacement log: each `(from, to)` is a value of
/// `ir` and the value the conversion replaced it with, in the order the replacements were made.
///
/// The log holds what the patterns replaced. A value a fold replaced with its result is not in it,
/// and neither is a value the conversion left alone.
pub(crate) async fn convert_logged(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &ConversionSet,
	cx: &PhysCx<'_>,
	folder: &dyn PlanTimeFolder,
	target: &LegalityTarget,
) -> Result<(ConversionReport, Vec<(ValueId, ValueId)>), IrError> {
	let mut log = Vec::new();
	let report = drive(ir, facts, set, cx, folder, target, Some(&mut log)).await?;
	Ok((report, log))
}

/// The rounds of [`convert`], appending the replacements the patterns make to `log` when one is
/// given.
async fn drive(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &ConversionSet,
	cx: &PhysCx<'_>,
	folder: &dyn PlanTimeFolder,
	target: &LegalityTarget,
	mut log: Option<&mut Vec<(ValueId, ValueId)>>,
) -> Result<ConversionReport, IrError> {
	let mut report = ConversionReport {
		converted: 0,
		eliminated: 0,
		stopped_by: None,
	};
	let mut remaining = outside_target(ir, target);
	while !remaining.is_empty() {
		let before = report.converted + report.eliminated;
		run_round(ir, facts, set, cx, target, &remaining, &mut report, log.as_deref_mut())?;
		remaining = outside_target(ir, target);
		if remaining.is_empty() {
			break;
		}
		let pending: Vec<FoldRequest> = facts.pending_folds().collect();
		if report.converted + report.eliminated == before && pending.is_empty() {
			report.stopped_by = Some(Limit::Iterations);
			break;
		}
		await_folds(ir, facts, folder, &cx.effects, pending).await;
	}
	Ok(report)
}

/// The live ops of `ir` whose dialect is outside `target.legal`, in post-order. Ops inside a
/// stored-clause region are not listed: stored code is never converted.
fn outside_target(ir: &Ir, target: &LegalityTarget) -> Vec<OpId> {
	let mut ids = Vec::new();
	ir.walk_outside_stored(ir.top(), WalkOrder::PostOrder, &mut |op| {
		if ir.op(op).is_some_and(|data| !target.legal.contains(dialect_of(&data.kind))) {
			ids.push(op);
		}
		Walk::Continue
	});
	ids
}

/// Offers each op of `ids` still live the patterns of its tag, and records what they did in
/// `report`.
#[expect(
	clippy::too_many_arguments,
	reason = "a round reads the container, its facts, the conversion, the catalog, the target and the report"
)]
fn run_round(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &ConversionSet,
	cx: &PhysCx<'_>,
	target: &LegalityTarget,
	ids: &[OpId],
	report: &mut ConversionReport,
	log: Option<&mut Vec<(ValueId, ValueId)>>,
) -> Result<(), IrError> {
	let mut rw = Rewriter::new(ir, facts);
	if log.is_some() {
		rw.record_replacements();
	}
	for &id in ids {
		let Some(tag) = rw.ir().op(id).and_then(|data| LogicalDialect::tag_of(&data.kind)) else {
			continue;
		};
		for pattern in set.for_source(tag) {
			let answer = rw.derived(&[id], |rw| pattern.convert(id, rw, cx))?;
			let created = check_created(&mut rw, target)?;
			match answer {
				Converted::NotApplicable if created => {
					return Err(IrError::Invariant {
						what: "convert: NotApplicable after creating ops",
					});
				}
				Converted::NotApplicable => continue,
				Converted::Replaced => {
					if rw.ir().op(id).is_some() {
						return Err(IrError::Invariant {
							what: "convert: Replaced left the op",
						});
					}
					report.converted += 1;
				}
				Converted::Eliminated(reason) => {
					if rw.ir().op(id).is_some() {
						rw.erase(id, reason)?;
					}
					report.eliminated += 1;
				}
			}
			break;
		}
	}
	if let Some(log) = log {
		log.extend(rw.take_replacements());
	}
	Ok(())
}

/// Answers a `Legality` violation when a conversion created an op outside `target.legal`, and
/// otherwise whether the conversion created any op. An op created and erased within the call is
/// not inspected, but counts as created.
fn check_created(rw: &mut Rewriter<'_>, target: &LegalityTarget) -> Result<bool, IrError> {
	let created = rw.take_created();
	for &op in &created {
		let Some(data) = rw.ir().op(op) else {
			continue;
		};
		if !target.legal.contains(dialect_of(&data.kind)) {
			return Err(IrError::Verify(VerifyError {
				op: Some(op),
				rule: VerifyRule::Legality,
				detail: format!("a conversion created an op outside the {} target", target.name),
			}));
		}
	}
	Ok(!created.is_empty())
}
