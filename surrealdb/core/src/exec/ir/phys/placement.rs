//! The one implementation of the placement boundary.
//!
//! [`Placement`] decides how one pipeline runs from the legality the logical plan allows, the
//! enclosing statement's legality, the loop's facts and the pipeline's physical facts, through the
//! planner's [`Policy`]. [`place_all`] decides every pipeline of a container and [`install`]
//! records the decisions in the container's placement table, the only writer of it.
//!
//! The boundary only narrows. A decision is the logical combine or a weaker one, never a stronger
//! one, and the rules this module adds on top of the policy (a backend that offers no read views,
//! a pipeline nested under one that fans out) only turn a fan-out into a serial run.

#![deny(clippy::wildcard_enum_match_arm)]

use super::facts::{NestedReading, chain_pipelines, facts_reading, phys_of, pipeline_entries};
use super::ops::{Batch, PhysOp};
use super::pipeline::PipelineDag;
use super::placement_attr::{PlacementAttr, PlacementTable};
use super::write_cut::{NO_CUT_STATED, WriteCut, write_cut_of};
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::phys::UNBATCHED;
use crate::exec::ir::core::Provenance;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::{FxHashMap, map_with_capacity};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::BackendCaps;
use crate::exec::ir::iface::effects::{ElemPath, Multiplicity};
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::policy::{LoopFacts, PlacementBoundary};
use crate::exec::ir::iface::{ApplyCut, Exit, LoopMode, LoopRefusal, SourceDelivery};
use crate::exec::ir::logical::ops::{LoopKind, LoopSink};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::pipelines::{ChainRefusal, Legality, StatementLegality};
use crate::exec::planner::placement::{
	Combine, Decision, MergeOrder, MorselInput, NestedUse, PhysicalFacts, Policy,
};

/// The placement decision point of the physical dialect.
///
/// Holds the [`Policy`] every decision is made through, so no threshold lives anywhere else.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Placement {
	policy: Policy,
	/// Whether the datastore's read-only transactions hand out no read view, a static fact of the
	/// datastore ([`IoCaps::read_views`](crate::exec::ir::iface::catalog::IoCaps::read_views)).
	/// `false` when the caller does not say.
	no_read_views: bool,
}

impl Placement {
	/// A placement that decides through `policy`.
	pub(crate) fn new(policy: Policy) -> Self {
		Self {
			policy,
			no_read_views: false,
		}
	}

	/// This placement over a datastore whose read-only transactions hand out read views when
	/// `read_views`.
	pub(crate) fn with_read_views(mut self, read_views: bool) -> Self {
		self.no_read_views = !read_views;
		self
	}

	/// `decision`, as the policy and the datastore let it run. A pipeline that could only run
	/// every morsel on the statement's task runs serially instead, which keeps the reads the plan
	/// gives a serial pipeline (rows read ahead, link and frontier windows):
	/// - under a policy that never splits ([`Policy::may_split_at_all`]), no batch goes to a worker
	///   and no range is cut;
	/// - a key range is cut only where workers read its parts through a view, when the policy says
	///   so ([`Policy::needs_view`]), so on a datastore that gives none a pipeline decided as range
	///   morsels is never cut. Morsels over batches of rows need no view and are kept.
	fn narrowed(&self, decision: Decision) -> Decision {
		let Decision::Morsels {
			input,
			..
		} = decision
		else {
			return decision;
		};
		let ranges_stay_whole =
			input == MorselInput::Range && self.no_read_views && self.policy.needs_view();
		if !self.policy.may_split_at_all() || ranges_stay_whole {
			Decision::Serial
		} else {
			decision
		}
	}

	/// A placement that decides through the policy `config` selects.
	pub(crate) fn from_config(config: &ExecConfig) -> Self {
		Self::new(Policy::from_config(config))
	}

	/// The bounds of one batch of the loop whose facts are `loop_facts`.
	pub(crate) fn loop_batch(&self, loop_facts: &LoopFacts, facts: &PhysicalFacts) -> Batch {
		self.policy.loop_batch(loop_facts, facts)
	}

	/// The bounds of one batch of a loop of `kind`.
	///
	/// A `FOR` runs its iterations in order, each as a statement of its own whose body may write,
	/// so it is never batched: its bounds are [`UNBATCHED`]. Every other kind takes the policy's
	/// bounds ([`Self::loop_batch`]).
	pub(crate) fn batch_of(
		&self,
		kind: LoopKind,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
	) -> Batch {
		match kind {
			LoopKind::For => UNBATCHED,
			LoopKind::Select
			| LoopKind::Write
			| LoopKind::Closure
			| LoopKind::ElementFilter
			| LoopKind::GraphStep
			| LoopKind::Recurse
			| LoopKind::Broadcast => self.loop_batch(loop_facts, facts),
		}
	}

	/// The iterations of the loop's body one execution of its program covers.
	pub(crate) fn unroll(&self, loop_facts: &LoopFacts) -> u8 {
		self.policy.unroll(loop_facts)
	}

	/// What the policy picks, before the check that it is no stronger than the legality.
	///
	/// A pipeline that stands for a loop is judged under that loop's verdict
	/// ([`Policy::plan_loop`]); a pipeline that stands for none is judged on its own source and
	/// sink ([`Policy::judge`]).
	fn pick(
		&self,
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: Option<&LoopFacts>,
		write_cut: Result<&WriteCut, &'static str>,
		facts: &PhysicalFacts,
	) -> Decision {
		match loop_facts {
			Some(loop_facts) => {
				Policy::plan_loop(legality, statement, loop_facts, facts, write_cut)
			}
			None => Policy::judge(legality, statement, facts).unwrap_or(Decision::Serial),
		}
	}
}

/// The placement of the shipped default configuration.
impl Default for Placement {
	fn default() -> Self {
		Self::from_config(&ExecConfig::default())
	}
}

impl PlacementBoundary for Placement {
	/// The policy's pick for the pipeline, or [`Decision::Serial`] when the pick is stronger than
	/// the legality allows. [`place_all`] reports the latter as an error.
	fn place(
		&self,
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
	) -> Decision {
		// A write loop is judged with the cut of its iterations, which only the pipeline that holds
		// it states: this entry answers for a loop on its facts alone, and so runs a write
		// serially.
		let picked = self.pick(legality, statement, Some(loop_facts), Err(NO_CUT_STATED), facts);
		never_stronger(legality, picked).unwrap_or(Decision::Serial)
	}
}

/// A decision whose combine is stronger than the legality the logical plan states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StrongerThanLegality {
	pub(crate) combine: Combine,
}

/// A merge order whose arrival is stronger than the legality the logical plan states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MergeStrongerThanLegality {
	pub(crate) merge: MergeOrder,
}

/// `merge` itself when its arrival is no stronger than the arrival `legality` states: the merge
/// of a pipeline may take its morsels' results in a weaker order than the legality allows, and
/// never in a stronger one.
pub(crate) fn merge_never_stronger(
	legality: &Legality,
	merge: MergeOrder,
) -> Result<MergeOrder, MergeStrongerThanLegality> {
	if merge.as_arrival().weaker_or_equal(legality.arrival) {
		Ok(merge)
	} else {
		Err(MergeStrongerThanLegality {
			merge,
		})
	}
}

/// `decision` itself when no combine it names is stronger than `legality` states: along
/// `TopK`, `StableMerge`, `Concat` and `Serial` a decision may weaken, and an aggregate or a
/// distinct set weakens to `Serial` alone.
pub(crate) fn never_stronger(
	legality: &Legality,
	decision: Decision,
) -> Result<Decision, StrongerThanLegality> {
	match decision {
		Decision::Serial => Ok(decision),
		Decision::Morsels {
			combine,
			..
		} => {
			if legality.weaker_or_equal(&combine.as_sink()) {
				Ok(decision)
			} else {
				Err(StrongerThanLegality {
					combine,
				})
			}
		}
	}
}

/// Why the decision recorded in `attr` is not one the loop whose facts are `loop_facts` admits,
/// or `None` when it is.
///
/// A serial decision is admitted for every loop. A fan-out is admitted when [`Policy::judge_loop`]
/// judges the recorded legality, statement, loop facts, physical facts and write cut to allow one
/// (when there are no loop facts, [`Policy::judge`] judges the pipeline alone), and its combine is
/// no stronger than the legality states ([`never_stronger`]): the rule [`place_all`] applies, so a
/// verifier that calls this holds no rule of its own. A placement that records no decision answers
/// `None`; a missing decision is another rule's failure.
pub(crate) fn admission_gap<'l>(
	attr: &PlacementAttr,
	loop_facts: impl Into<Option<&'l LoopFacts>>,
) -> Option<String> {
	let decision = attr.decision()?;
	if matches!(decision, Decision::Serial) {
		return None;
	}
	let Some(physical) = attr.facts() else {
		return Some("a pipeline fans out and records no physical facts to judge it by".to_owned());
	};
	let judged = match loop_facts.into() {
		Some(loop_facts) => Policy::judge_loop(
			attr.legality(),
			*attr.statement(),
			loop_facts,
			physical,
			attr.write_cut().as_ref().ok_or(NO_CUT_STATED),
		),
		None => Policy::judge(attr.legality(), *attr.statement(), physical),
	};
	match judged {
		Err(reason) => {
			Some(format!("a pipeline fans out where its loop admits only a serial run: {reason}"))
		}
		Ok(_) => never_stronger(attr.legality(), decision)
			.err()
			.map(|stronger| {
				format!(
					"a pipeline fans out with the {:?} combine, which is stronger than its \
					 legality states",
					stronger.combine
				)
			})
			.or_else(|| merge_gap(attr, physical, decision))
			.or_else(|| fold_gap(attr, decision)),
	}
}

/// Why the merge order recorded in `attr` is not one its fan-out `decision` admits, or `None`
/// when it is: no stronger than the arrival the legality states, one the decision's combine has a
/// meaning for ([`MergeOrder::fits`]), and no stronger than the order [`Policy::merge_order`]
/// picks.
fn merge_gap(attr: &PlacementAttr, facts: &PhysicalFacts, decision: Decision) -> Option<String> {
	let merge = attr.merge();
	if let Err(stronger) = merge_never_stronger(attr.legality(), merge) {
		return Some(format!(
			"a pipeline merges its morsels as {:?}, which is stronger than its legality states",
			stronger.merge
		));
	}
	if let Decision::Morsels {
		combine,
		..
	} = decision
		&& !merge.fits(combine)
	{
		return Some(format!(
			"a pipeline merges its morsels as {merge:?}, which its {combine:?} combine has no \
			 meaning for"
		));
	}
	(merge > Policy::merge_order(attr.legality(), facts, decision)).then(|| {
		format!("a pipeline merges its morsels as {merge:?}, which its combine does not allow")
	})
}

/// Why the fold order recorded in `attr` is not the one [`Policy::ordered_fold`] picks for its
/// fan-out `decision`, or `None` when it is. Terms kept for the merge to fold in order are folded
/// by a merge that takes the morsels in the order of the serial run, so a record of them beside
/// another merge order is a gap of its own.
fn fold_gap(attr: &PlacementAttr, decision: Decision) -> Option<String> {
	if !attr.ordered_fold().is_empty() && attr.merge() != MergeOrder::Serial {
		return Some(format!(
			"a pipeline keeps the terms of its float folds for a merge in the order of the serial \
			 run and merges its morsels as {:?}",
			attr.merge()
		));
	}
	(attr.ordered_fold() != Policy::ordered_fold(attr.legality(), decision, attr.merge())).then(
		|| {
			"a pipeline records an order for the terms of its float folds that its legality and \
			 combine do not state"
				.to_owned()
		},
	)
}

/// A backend that offers nothing: no loop other than a `SELECT` fans out.
pub(crate) const NO_BACKEND_CAPS: BackendCaps = BackendCaps {
	offers_read_views: false,
	reads_tracked: false,
	offers_write_views: false,
};

/// The loop op that stands directly in the pipeline `pipeline_op`, and its kind.
pub(super) fn loop_in(ir: &Ir, pipeline_op: OpId) -> Option<(OpId, LoopKind)> {
	pipeline_entries(ir, pipeline_op).into_iter().find_map(|(op, kind)| {
		if let PhysOp::Loop {
			kind,
			..
		} = kind
		{
			Some((op, *kind))
		} else {
			None
		}
	})
}

/// The serial verdict of a loop nothing is known of: one iteration at a time, no apply residue and
/// no exit, over a source read whole whose identities may repeat.
fn serial_loop_facts() -> LoopFacts {
	LoopFacts {
		mode: LoopMode::Serial(LoopRefusal::Unsplittable),
		apply: ApplyCut::None,
		exit: Exit::None,
		sink: LoopSink::Output,
		delivery: SourceDelivery::ReadWhole,
		multiplicity: Multiplicity::MayRepeat,
		key_path: ElemPath::Id,
		compute_reads_data: true,
		cut_at_marks: false,
	}
}

/// The verdict of the loop the pipeline `pipeline_op` stands for, or `None` when it stands for
/// none.
///
/// The verdict is the one the logical boundary recorded for the logical loop the pipeline's
/// attributes name (the legality snapshot). A pipeline that holds a loop op the snapshot says
/// nothing of answers the serial verdict, so a loop the logical boundary did not judge is never
/// split. A pipeline that holds no loop op has no verdict and is judged on its own source and
/// sink.
pub(crate) fn loop_verdict(ir: &Ir, facts: &FactCache, pipeline_op: OpId) -> Option<LoopFacts> {
	let recorded = logical_pipeline_of(ir, pipeline_op)
		.and_then(|logical| facts.legality_snapshot()?.get(&logical))
		.cloned();
	recorded.or_else(|| loop_in(ir, pipeline_op).map(|_| serial_loop_facts()))
}

/// The legality inputs of the pipeline `pipeline_op`: what the logical plan allows it, what its
/// statement allows its pipelines, and the verdict of the loop it stands for.
///
/// Reads the legality inputs from the analysis. The pipeline stands for the logical loop its
/// attributes name, and its statement for the `logical.stmt` its enclosing `phys.stmt` stands
/// for. The legality of that loop's segment and the refusal of that statement are the ones the
/// logical plan stated for those ops
/// ([`LogicalLegalities`](crate::exec::ir::core::facts::LogicalLegalities)), so the inputs are
/// derived from the pipeline's provenance and not from a constant. A pipeline or a statement the
/// plan stated nothing for is one the logical plan does not list ([`Legality::not_listed`]) in a
/// statement that refuses with [`ChainRefusal::Opaque`], and no policy admits either. The loop's
/// verdict is [`loop_verdict`].
pub(crate) fn legality_inputs(
	ir: &Ir,
	facts: &FactCache,
	pipeline_op: OpId,
) -> (Legality, StatementLegality, Option<LoopFacts>) {
	let logical = logical_pipeline_of(ir, pipeline_op);
	let loop_facts = loop_verdict(ir, facts, pipeline_op);
	let stated = facts.logical_legalities();
	let legality = logical
		.and_then(|logical| stated?.pipelines.get(&logical))
		.cloned()
		.unwrap_or_else(Legality::not_listed);
	let statement = logical_statement_of(ir, pipeline_op)
		.and_then(|logical| stated?.statements.get(&logical))
		.copied()
		.unwrap_or(StatementLegality {
			refusal: Some(ChainRefusal::Opaque),
		});
	(legality, statement, loop_facts)
}

/// The logical loop the pipeline `pipeline_op` stands for.
fn logical_pipeline_of(ir: &Ir, pipeline_op: OpId) -> Option<OpId> {
	match phys_of(ir, pipeline_op) {
		Some(PhysOp::Pipeline(attrs)) => attrs.logical,
		Some(_) | None => None,
	}
}

/// The `logical.stmt` the statement around `op` stands for: the first logical op of the provenance
/// of the nearest enclosing `phys.stmt`.
fn logical_statement_of(ir: &Ir, op: OpId) -> Option<OpId> {
	let mut at = op;
	while let Some(owner) = ir.parent_op(ir.enclosing_region(at)) {
		let data = ir.op(owner)?;
		if matches!(data.kind, OpKind::Phys(PhysOp::Stmt { .. })) {
			return match &data.provenance {
				Provenance::From(ids) => ids.first().copied(),
				Provenance::Bound => None,
			};
		}
		at = owner;
	}
	None
}

/// `decision`, as a loop of kind `kind` may run it on a backend with `caps`: a loop that is not a
/// `SELECT` reads through a view of its own per worker, so it runs serially unless the backend
/// offers read views. A pipeline that holds no loop, and a `SELECT`, keep the policy's rule, and so
/// does a write that applies in order: its workers read no stored data, so they need no view.
fn apply_backend(decision: Decision, kind: Option<LoopKind>, caps: BackendCaps) -> Decision {
	let needs_view = kind.is_some_and(|kind| kind != LoopKind::Select);
	if needs_view && !is_ordered_apply(decision) && !caps.offers_read_views {
		Decision::Serial
	} else {
		decision
	}
}

/// Whether `decision` runs a write's iterations as morsels whose effects apply in order.
fn is_ordered_apply(decision: Decision) -> bool {
	matches!(
		decision,
		Decision::Morsels {
			combine: Combine::OrderedApply,
			..
		}
	)
}

/// `decision`, as a pipeline whose enclosing pipelines were decided `ancestors` may run it: at
/// most one pipeline fans out on a path of nested pipelines, the outermost one the policy allows,
/// and an inner pipeline runs serially inside a worker.
fn apply_nesting(decision: Decision, ancestors: &[Decision]) -> Decision {
	let fanned = ancestors.iter().any(|ancestor| matches!(ancestor, Decision::Morsels { .. }));
	if fanned {
		Decision::Serial
	} else {
		decision
	}
}

/// The pipelines whose regions contain the pipeline `op`, innermost first.
fn enclosing_pipelines(ir: &Ir, op: OpId) -> Vec<OpId> {
	let mut found = Vec::new();
	let mut at = op;
	while let Some(owner) = ir.parent_op(ir.enclosing_region(at)) {
		if matches!(phys_of(ir, owner), Some(PhysOp::Pipeline(..))) {
			found.push(owner);
		}
		at = owner;
	}
	found
}

/// The error of a pick stronger than the legality of the pipeline `op`.
fn stronger_than_legality(op: OpId, stronger: StrongerThanLegality) -> IrError {
	IrError::Verify(VerifyError {
		op: Some(op),
		rule: VerifyRule::PerOp,
		detail: format!(
			"the placement of a pipeline picked the {:?} combine, which is stronger than its \
			 legality states",
			stronger.combine
		),
	})
}

/// The error of a merge order stronger than the legality of the pipeline `op`.
fn merge_stronger_than_legality(op: OpId, stronger: MergeStrongerThanLegality) -> IrError {
	IrError::Verify(VerifyError {
		op: Some(op),
		rule: VerifyRule::PerOp,
		detail: format!(
			"the placement of a pipeline merges its morsels as {:?}, which is stronger than its \
			 legality states",
			stronger.merge
		),
	})
}

/// What was decided for one pipeline.
struct Judged {
	legality: Legality,
	statement: StatementLegality,
	decision: Decision,
	facts: PhysicalFacts,
	chain: usize,
	prunes: bool,
	prunes_by_score: bool,
	write_cut: Result<WriteCut, &'static str>,
}

/// Whether the full-text scan or index union of a pipeline that does not prune may still reject a
/// hit by the score threshold of the sort it feeds ([`PlacementAttr::prunes_by_score`]), judged
/// from the container, the pipeline and the pipeline's physical facts.
///
/// The verdict reads no placement decision, so [`place_all_reading`] takes it before it decides
/// the pipeline.
pub(crate) type ScoreVerdict<'v> =
	dyn FnMut(&Ir, &mut FactCache, OpId, &PhysicalFacts) -> bool + 'v;

/// Whether an entry of the pipeline may be rejected before its body runs, by a threshold the
/// serial run would have judged it against: no row the pipeline reads raises.
///
/// The logical plan states it ([`Legality::may_raise`], an allow-list over the pipeline's
/// expressions that knows the catalog's computed fields); a nested statement or a block in an
/// expression is one the allow-list does not name, so the physical facts refuse it too.
fn prunes_of(legality: &Legality, facts: &PhysicalFacts) -> bool {
	!legality.may_raise && !facts.host_blocks && facts.nested == NestedUse::None
}

/// Whether `pipeline` reads a full-text scan, or an index union of full-text branches, that passes
/// hits over by the score threshold of the sort it feeds, which then runs serially.
///
/// The scan does a hit's index and storage work itself, before the row is handed on; the body
/// left takes the score the scan remembered and pushes the row into the sort. Morsels would
/// gather rows into batches before any of them reached the sort, which delays the first
/// threshold and every tightening after it, and so lets the scan read the records of hits the
/// sort would never keep.
///
/// The scan passes hits over only where it holds a key that rejects entries
/// ([`TopKKey::rejects_entries`]) and the pipeline prunes, by its legality or by score alone
/// (`prunes`): the physical rewrites state that the scan of every other pipeline rejects no entry,
/// and its pipeline is left to the policy as any other.
///
/// [`TopKKey::rejects_entries`]: crate::exec::vm::payload::TopKKey::rejects_entries
fn passes_over_by_score(ir: &Ir, pipeline: OpId, prunes: bool) -> bool {
	prunes
		&& super::facts::source_of(ir, pipeline).is_some_and(|source| {
			match ir.op(source).map(|data| &data.kind) {
				Some(OpKind::Phys(PhysOp::FullTextScan(scan))) => {
					scan.topk.as_ref().is_some_and(|key| key.rejects_entries)
				}
				Some(OpKind::Phys(PhysOp::IndexUnion(union))) => {
					union.topk.as_ref().is_some_and(|(key, _)| key.rejects_entries)
				}
				Some(_) | None => false,
			}
		})
}

/// Decides every pipeline of `ir`, in an order that places each after the pipelines that feed it.
///
/// For a pipeline: the legality inputs, the physical facts, the policy's pick (never stronger than
/// the legality, which is an error here), the backend's rule for a loop that needs a view, and
/// then the nesting rule: a pipeline inside a pipeline that fans out runs serially. `prunes` is
/// whether the plan states the pipeline raises for no row ([`prunes_of`]).
///
/// The statements in value position are read under [`NestedReading::Hedged`], and no pipeline is
/// judged to prune by score alone; see [`place_all_reading`].
pub(crate) fn place_all(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	placement: &Placement,
	caps: BackendCaps,
) -> Result<Vec<(OpId, PlacementAttr)>, IrError> {
	place_all_reading(
		ir,
		facts,
		registry,
		placement,
		caps,
		NestedReading::Hedged,
		&mut |_, _, _, _| false,
	)
}

/// [`place_all`] with the statements in value position read as `reading` states, and each pipeline
/// that does not prune judged by `by_score` ([`ScoreVerdict`]).
///
/// A container whose nested strategies are decided is placed under [`NestedReading::Strategy`]:
/// what the container holds is what runs, so a statement left `Apply` is evaluated for each row.
///
/// Whether a pipeline prunes, by its legality or by score alone, is judged before its decision
/// and recorded in its placement ([`PlacementAttr::prunes`], [`PlacementAttr::prunes_by_score`]).
/// Neither verdict reads a decision, so the one pass decides every pipeline, and the nesting rule
/// reads the final decision of each pipeline around it. A full-text scan or index union whose
/// pipeline prunes passes hits over by the score threshold of the sort it feeds, and its pipeline
/// runs serially ([`passes_over_by_score`]); one whose pipeline does not is placed as any other.
pub(crate) fn place_all_reading(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	placement: &Placement,
	caps: BackendCaps,
	reading: NestedReading,
	by_score: &mut ScoreVerdict<'_>,
) -> Result<Vec<(OpId, PlacementAttr)>, IrError> {
	let dag = PipelineDag::of(ir);
	let order = dag.topological_order().map_err(IrError::Verify)?;
	let mut judged: FxHashMap<OpId, Judged> = map_with_capacity(order.len());
	for (_, op) in &order {
		let (legality, statement, loop_facts) = legality_inputs(ir, facts, *op);
		let physical = facts_reading(ir, facts, registry, *op, reading)?;
		let write_cut = write_cut_of(ir, loop_facts.as_ref(), *op);
		let picked = placement.pick(
			&legality,
			statement,
			loop_facts.as_ref(),
			write_cut.as_ref().map_err(|reason| *reason),
			&physical,
		);
		let decision = never_stronger(&legality, picked)
			.map_err(|stronger| stronger_than_legality(*op, stronger))?;
		let decision = apply_backend(decision, loop_in(ir, *op).map(|(_, kind)| kind), caps);
		let decision = placement.narrowed(decision);
		let prunes = prunes_of(&legality, &physical);
		let prunes_by_score = !prunes && by_score(ir, facts, *op, &physical);
		let decision = if passes_over_by_score(ir, *op, prunes || prunes_by_score) {
			Decision::Serial
		} else {
			decision
		};
		judged.insert(
			*op,
			Judged {
				legality,
				statement,
				decision,
				facts: physical,
				chain: chain_pipelines(ir, *op),
				prunes,
				prunes_by_score,
				write_cut,
			},
		);
	}
	// Outer pipelines come first in pre-order, so an inner pipeline sees the final decision of each
	// pipeline around it.
	for (_, op) in &dag.nodes {
		let ancestors: Vec<Decision> = enclosing_pipelines(ir, *op)
			.into_iter()
			.filter_map(|ancestor| judged.get(&ancestor).map(|judged| judged.decision))
			.collect();
		if let Some(entry) = judged.get_mut(op) {
			entry.decision = apply_nesting(entry.decision, &ancestors);
		}
	}
	let mut placed = Vec::with_capacity(order.len());
	for (_, op) in &order {
		let Some(entry) = judged.remove(op) else {
			continue;
		};
		let merge = Policy::merge_order(&entry.legality, &entry.facts, entry.decision);
		let merge = merge_never_stronger(&entry.legality, merge)
			.map_err(|stronger| merge_stronger_than_legality(*op, stronger))?;
		let ordered_fold = Policy::ordered_fold(&entry.legality, entry.decision, merge);
		let mut attr = PlacementAttr::new(entry.legality, entry.statement);
		attr.set_decision(Some(entry.decision));
		attr.set_merge(merge);
		attr.set_ordered_fold(ordered_fold);
		attr.set_facts(Some(entry.facts));
		attr.set_chain(Some(entry.chain));
		attr.set_prunes(entry.prunes);
		attr.set_prunes_by_score(entry.prunes_by_score);
		// Only a write that fans out is cut: a serial run holds the cut in no placement.
		attr.set_write_cut(entry.write_cut.ok().filter(|_| is_ordered_apply(entry.decision)));
		placed.push((*op, attr));
	}
	Ok(placed)
}

/// Records the placement of every pipeline in `placed` in `table`, replacing the placement a
/// pipeline already has.
pub(crate) fn install(table: &mut PlacementTable, placed: Vec<(OpId, PlacementAttr)>) {
	for (op, attr) in placed {
		table.insert(op, attr);
	}
}

/// Records `decision` as the placement of the pipeline `op` of `ir`, judged from `legality` and
/// `facts`, in a statement that refuses nothing.
#[cfg(test)]
pub(crate) fn place_for_test(
	ir: &mut Ir,
	op: OpId,
	legality: Legality,
	decision: Decision,
	facts: PhysicalFacts,
) {
	let mut attr = PlacementAttr::new(
		legality,
		StatementLegality {
			refusal: None,
		},
	);
	attr.set_decision(Some(decision));
	attr.set_facts(Some(facts));
	install(ir.placement_table_mut(), vec![(op, attr)]);
}

#[cfg(test)]
mod merge_tests {
	use super::*;
	use crate::exec::logical::pipelines::{
		AggregateCombine, Arrival, ChainLegality, ErrorOrder, Paging, SinkCombine, SourceSplit,
	};
	use crate::exec::planner::placement::{MorselInput, SinkKind, SourceKind};

	fn facts() -> PhysicalFacts {
		PhysicalFacts {
			source: SourceKind::Scan,
			enforce: false,
			writes: false,
			limits: 0,
			output: true,
			sink: SinkKind::Output,
			binds_row: false,
			fetches_row: false,
			distincts: 0,
			nested: NestedUse::None,
			host_blocks: false,
			paging_replayable: false,
			estimate: None,
		}
	}

	fn legality(sink: SinkCombine, arrival: Arrival) -> Legality {
		Legality {
			source: SourceSplit::Range,
			chain: ChainLegality::RowLocal,
			sink,
			paging: Paging::None,
			arrival,
			ordered_fold: false,
			error_order: ErrorOrder::FirstByInputPosition,
			may_raise: false,
		}
	}

	fn morsels(combine: Combine, paged: bool) -> Decision {
		Decision::Morsels {
			input: MorselInput::Range,
			combine,
			paged,
			prunes: false,
		}
	}

	#[test]
	fn the_merge_is_never_stronger_than_the_arrival_stated() {
		let free = legality(SinkCombine::Concat, Arrival::Free);
		let serial = legality(SinkCombine::Concat, Arrival::Serial);
		assert_eq!(merge_never_stronger(&free, MergeOrder::Free), Ok(MergeOrder::Free));
		assert_eq!(merge_never_stronger(&free, MergeOrder::Serial), Ok(MergeOrder::Serial));
		assert_eq!(merge_never_stronger(&serial, MergeOrder::Serial), Ok(MergeOrder::Serial));
		assert_eq!(
			merge_never_stronger(&serial, MergeOrder::Free),
			Err(MergeStrongerThanLegality {
				merge: MergeOrder::Free
			})
		);
	}

	/// Only the unpaged concatenation of a statement's output and an aggregate of counts merge as
	/// morsels finish, and only where the legality states it.
	#[test]
	fn only_an_unpaged_concat_of_a_free_output_merges_freely() {
		let free = legality(SinkCombine::Concat, Arrival::Free);
		let serial = legality(SinkCombine::Concat, Arrival::Serial);
		let merge = |legality: &Legality, facts: &PhysicalFacts, decision| {
			Policy::merge_order(legality, facts, decision)
		};
		assert_eq!(merge(&free, &facts(), morsels(Combine::Concat, false)), MergeOrder::Free);
		assert_eq!(merge(&serial, &facts(), morsels(Combine::Concat, false)), MergeOrder::Serial);
		assert_eq!(merge(&free, &facts(), Decision::Serial), MergeOrder::Serial);
		assert_eq!(merge(&free, &facts(), morsels(Combine::Concat, true)), MergeOrder::Serial);
		for combine in [
			Combine::StableMerge,
			Combine::TopK,
			Combine::Aggregate,
			Combine::Distinct,
			Combine::OrderedApply,
		] {
			assert_eq!(merge(&free, &facts(), morsels(combine, false)), MergeOrder::Serial);
		}
		let not_output = PhysicalFacts {
			output: false,
			sink: SinkKind::Aggregate,
			..facts()
		};
		assert_eq!(merge(&free, &not_output, morsels(Combine::Concat, false)), MergeOrder::Serial);
		// An aggregate merges freely only where its legality is an aggregate that states it, and
		// is not paged.
		let counts = legality(SinkCombine::Aggregate(vec![AggregateCombine::Exact]), Arrival::Free);
		let folds =
			legality(SinkCombine::Aggregate(vec![AggregateCombine::Exact]), Arrival::Serial);
		assert_eq!(
			merge(&counts, &not_output, morsels(Combine::Aggregate, false)),
			MergeOrder::Free
		);
		assert_eq!(
			merge(&folds, &not_output, morsels(Combine::Aggregate, false)),
			MergeOrder::Serial
		);
		assert_eq!(
			merge(&counts, &not_output, morsels(Combine::Aggregate, true)),
			MergeOrder::Serial
		);
	}

	/// A sort that keeps every row positions the runs it takes, an aggregate combines the partial
	/// states of morsels next to one another, and each only over a key range, where the morsels
	/// are taken as they finish; no other combine takes its morsels out of order.
	#[test]
	fn a_sort_positions_runs_and_an_aggregate_combines_adjacent_partials() {
		let merge = |legality: &Legality, combine: Combine, input: MorselInput, paged: bool| {
			Policy::merge_order(
				legality,
				&facts(),
				Decision::Morsels {
					input,
					combine,
					paged,
					prunes: false,
				},
			)
		};
		let sort = legality(SinkCombine::StableMerge, Arrival::Positioned);
		let folds = legality(
			SinkCombine::Aggregate(vec![AggregateCombine::Exact, AggregateCombine::Appending]),
			Arrival::Adjacent,
		);
		let (ranges, batches) = (MorselInput::Range, MorselInput::Batches);
		assert_eq!(merge(&sort, Combine::StableMerge, ranges, false), MergeOrder::Positioned);
		assert_eq!(merge(&sort, Combine::StableMerge, batches, false), MergeOrder::Serial);
		assert_eq!(merge(&sort, Combine::StableMerge, ranges, true), MergeOrder::Serial);
		assert_eq!(merge(&folds, Combine::Aggregate, ranges, false), MergeOrder::Adjacent);
		assert_eq!(merge(&folds, Combine::Aggregate, batches, false), MergeOrder::Serial);
		assert_eq!(merge(&sort, Combine::TopK, ranges, false), MergeOrder::Serial);
		assert_eq!(merge(&folds, Combine::Concat, ranges, false), MergeOrder::Serial);
		// A legality that states less is merged in the order of the serial run.
		let strict = legality(SinkCombine::StableMerge, Arrival::Serial);
		assert_eq!(merge(&strict, Combine::StableMerge, ranges, false), MergeOrder::Serial);
		// The merge of a combine takes only the orders it has a meaning for.
		for merge in
			[MergeOrder::Serial, MergeOrder::Positioned, MergeOrder::Adjacent, MergeOrder::Free]
		{
			for combine in [
				Combine::Concat,
				Combine::StableMerge,
				Combine::TopK,
				Combine::Aggregate,
				Combine::Distinct,
				Combine::OrderedApply,
			] {
				let expected = match merge {
					MergeOrder::Serial => true,
					MergeOrder::Positioned => combine == Combine::StableMerge,
					MergeOrder::Adjacent => combine == Combine::Aggregate,
					MergeOrder::Free => matches!(combine, Combine::Concat | Combine::Aggregate),
				};
				assert_eq!(merge.fits(combine), expected, "{merge:?} over {combine:?}");
			}
		}
	}

	/// The verifier names a merge order its combine has no meaning for, though the arrival the
	/// legality states would allow it.
	#[test]
	fn the_verifier_names_a_merge_order_the_combine_has_no_meaning_for() {
		let record = |legality: Legality, combine: Combine, sink: SinkKind, merge: MergeOrder| {
			let mut attr = PlacementAttr::new(
				legality,
				StatementLegality {
					refusal: None,
				},
			);
			attr.set_decision(Some(morsels(combine, false)));
			attr.set_facts(Some(PhysicalFacts {
				output: false,
				sink,
				..facts()
			}));
			attr.set_merge(merge);
			attr
		};
		let sorting = SinkKind::Sort {
			bounded: false,
		};
		let sort = legality(SinkCombine::StableMerge, Arrival::Adjacent);
		assert_eq!(
			admission_gap(
				&record(sort.clone(), Combine::StableMerge, sorting, MergeOrder::Positioned),
				None
			),
			None
		);
		let gap =
			admission_gap(&record(sort, Combine::StableMerge, sorting, MergeOrder::Adjacent), None)
				.expect("an aggregate's merge order on a sort");
		assert!(gap.contains("no meaning"), "{gap}");
		let folds =
			legality(SinkCombine::Aggregate(vec![AggregateCombine::Exact]), Arrival::Adjacent);
		assert_eq!(
			admission_gap(
				&record(
					folds.clone(),
					Combine::Aggregate,
					SinkKind::Aggregate,
					MergeOrder::Adjacent
				),
				None
			),
			None
		);
		let gap = admission_gap(
			&record(folds, Combine::Aggregate, SinkKind::Aggregate, MergeOrder::Positioned),
			None,
		)
		.expect("a sort's merge order on an aggregate");
		assert!(gap.contains("no meaning"), "{gap}");
	}

	/// A recorded merge that is stronger than the legality states, or than the combine allows, is
	/// a gap the verifier names.
	#[test]
	fn the_verifier_refuses_a_merge_stronger_than_the_legality_or_the_combine() {
		let record = |legality: Legality, decision: Decision, merge: MergeOrder| {
			let mut attr = PlacementAttr::new(
				legality,
				StatementLegality {
					refusal: None,
				},
			);
			attr.set_decision(Some(decision));
			attr.set_facts(Some(facts()));
			attr.set_merge(merge);
			attr
		};
		let free = legality(SinkCombine::Concat, Arrival::Free);
		let serial = legality(SinkCombine::Concat, Arrival::Serial);
		let concat = morsels(Combine::Concat, false);
		assert_eq!(admission_gap(&record(free.clone(), concat, MergeOrder::Free), None), None);
		assert_eq!(admission_gap(&record(free, concat, MergeOrder::Serial), None), None);
		let gap = admission_gap(&record(serial, concat, MergeOrder::Free), None)
			.expect("a merge stronger than the arrival the legality states");
		assert!(gap.contains("stronger than its legality"), "{gap}");
		// A merge as pages finish of a paged combine: the legality allows it and the combine does
		// not.
		let paged = morsels(Combine::Concat, true);
		let mut paging = legality(SinkCombine::Concat, Arrival::Free);
		paging.paging = Paging::One;
		let mut facts_paged = facts();
		facts_paged.limits = 1;
		facts_paged.paging_replayable = true;
		let mut attr = record(paging, paged, MergeOrder::Free);
		attr.set_facts(Some(facts_paged));
		let gap = admission_gap(&attr, None).expect("a merge its combine does not allow");
		assert!(gap.contains("does not allow"), "{gap}");
	}
	/// A pipeline whose morsels keep the terms of their float folds records it, where the logical
	/// plan states the fold may be replayed in order and the pipeline fans out as an aggregate; a
	/// record that says otherwise is a gap the verifier names.
	#[test]
	fn the_verifier_names_a_fold_order_the_plan_does_not_state() {
		let aggregate = PhysicalFacts {
			output: false,
			sink: SinkKind::Aggregate,
			..facts()
		};
		let record = |ordered: bool, replayable: bool| {
			let mut folding = legality(
				SinkCombine::Aggregate(vec![AggregateCombine::InexactFloat]),
				Arrival::Serial,
			);
			folding.ordered_fold = replayable;
			let mut attr = PlacementAttr::new(
				folding,
				StatementLegality {
					refusal: None,
				},
			);
			attr.set_decision(Some(morsels(Combine::Aggregate, false)));
			attr.set_facts(Some(aggregate.clone()));
			attr.set_ordered_fold(if ordered {
				vec![0]
			} else {
				Vec::new()
			});
			attr
		};
		assert_eq!(admission_gap(&record(true, true), None), None);
		let gap = admission_gap(&record(false, true), None).expect("a fold that keeps no terms");
		assert!(gap.contains("order for the terms"), "{gap}");
		// A fold the plan does not state may be replayed does not fan out at all.
		let gap = admission_gap(&record(true, false), None).expect("a fold with no replay");
		assert!(gap.contains("float fold"), "{gap}");
		// Terms kept for the merge to fold in order are folded by a merge that takes the morsels
		// in the order of the serial run, whatever order the legality would allow.
		let mut counted =
			legality(SinkCombine::Aggregate(vec![AggregateCombine::InexactFloat]), Arrival::Free);
		counted.ordered_fold = true;
		let mut attr = PlacementAttr::new(
			counted,
			StatementLegality {
				refusal: None,
			},
		);
		attr.set_decision(Some(morsels(Combine::Aggregate, false)));
		attr.set_facts(Some(aggregate));
		attr.set_merge(MergeOrder::Free);
		attr.set_ordered_fold(vec![0]);
		let gap = admission_gap(&attr, None).expect("terms kept beside a free merge");
		assert!(gap.contains("keeps the terms of its float folds"), "{gap}");
	}
}
