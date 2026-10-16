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

use std::collections::HashMap;

use super::facts::{NestedReading, facts_reading, phys_of, pipeline_entries};
use super::ops::{Batch, PhysOp};
use super::pipeline::PipelineDag;
use super::placement_attr::{PlacementAttr, PlacementTable};
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::{fallibility, loop_legality};
use crate::exec::ir::build::phys::UNBATCHED;
use crate::exec::ir::core::Provenance;
use crate::exec::ir::core::facts::FactCache;
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
use crate::exec::planner::placement::{Combine, Decision, PhysicalFacts, Policy};

/// The placement decision point of the physical dialect.
///
/// Holds the [`Policy`] every decision is made through, so no threshold lives anywhere else.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Placement {
	policy: Policy,
}

impl Placement {
	/// A placement that decides through `policy`.
	pub(crate) fn new(policy: Policy) -> Self {
		Self {
			policy,
		}
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
	fn pick(
		&self,
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
	) -> Decision {
		Policy::plan_loop(legality, statement, loop_facts, facts)
	}
}

/// The placement of the shipped default configuration.
impl Default for Placement {
	fn default() -> Self {
		Self::new(Policy::from_config(&ExecConfig::default()))
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
		let picked = self.pick(legality, statement, loop_facts, facts);
		never_stronger(legality, picked).unwrap_or(Decision::Serial)
	}
}

/// A decision whose combine is stronger than the legality the logical plan states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StrongerThanLegality {
	pub(crate) combine: Combine,
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
/// judges the recorded legality, statement, loop facts and physical facts to allow one, and its
/// combine is no stronger than the legality states ([`never_stronger`]): the rule [`place_all`]
/// applies, so a verifier that calls this holds no rule of its own. A placement that records no
/// decision answers `None`; a missing decision is another rule's failure.
pub(crate) fn admission_gap(attr: &PlacementAttr, loop_facts: &LoopFacts) -> Option<String> {
	let decision = attr.decision()?;
	if matches!(decision, Decision::Serial) {
		return None;
	}
	let Some(physical) = attr.facts() else {
		return Some("a pipeline fans out and records no physical facts to judge it by".to_owned());
	};
	match Policy::judge_loop(attr.legality(), *attr.statement(), loop_facts, physical) {
		Err(reason) => {
			Some(format!("a pipeline fans out where its loop admits only a serial run: {reason}"))
		}
		Ok(_) => never_stronger(attr.legality(), decision).err().map(|stronger| {
			format!(
				"a pipeline fans out with the {:?} combine, which is stronger than its legality \
				 states",
				stronger.combine
			)
		}),
	}
}

/// A backend that offers nothing: no loop other than a `SELECT` fans out.
pub(crate) const NO_BACKEND_CAPS: BackendCaps = BackendCaps {
	offers_read_views: false,
	reads_tracked: false,
	offers_write_views: false,
};

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
	}
}

/// The loop op that stands directly in the pipeline `pipeline_op`, and its kind.
fn loop_in(ir: &Ir, pipeline_op: OpId) -> Option<(OpId, LoopKind)> {
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

/// The legality inputs of the pipeline `pipeline_op`: what the logical plan allows it, what its
/// statement allows its pipelines, and the verdict of the loop it holds.
///
/// Reads the legality inputs from the analysis. The pipeline stands for the logical loop its
/// attributes name, and its statement for the `logical.stmt` its enclosing `phys.stmt` stands
/// for. The legality of that loop's segment and the refusal of that statement are the ones the
/// logical plan stated for those ops
/// ([`LogicalLegalities`](crate::exec::ir::core::facts::LogicalLegalities)), so the inputs are
/// derived from the pipeline's provenance and not from a constant. A pipeline or a statement the
/// plan stated nothing for is one the logical plan does not list ([`Legality::not_listed`]) in a
/// statement that refuses with [`ChainRefusal::Opaque`], and no policy admits either. The loop's
/// verdict is the analysis of the loop op the pipeline holds; a pipeline that holds none, and a
/// loop the analysis cannot judge, answer the serial verdict.
pub(crate) fn legality_inputs(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	pipeline_op: OpId,
) -> (Legality, StatementLegality, LoopFacts) {
	let loop_facts = loop_in(ir, pipeline_op)
		.and_then(|(loop_op, _)| {
			loop_legality::of_with(ir, facts, &structural_cx(registry, ir), loop_op)
				.map(|legality| legality.facts())
				.ok()
		})
		.unwrap_or_else(serial_loop_facts);
	let stated = facts.logical_legalities();
	let legality = logical_pipeline_of(ir, pipeline_op)
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
/// offers read views. A pipeline that holds no loop, and a `SELECT`, keep the policy's rule.
fn apply_backend(decision: Decision, kind: Option<LoopKind>, caps: BackendCaps) -> Decision {
	let needs_view = kind.is_some_and(|kind| kind != LoopKind::Select);
	if needs_view && !caps.offers_read_views {
		Decision::Serial
	} else {
		decision
	}
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

/// What was decided for one pipeline.
struct Judged {
	legality: Legality,
	statement: StatementLegality,
	decision: Decision,
	facts: PhysicalFacts,
	prunes: bool,
}

/// Decides every pipeline of `ir`, in an order that places each after the pipelines that feed it.
///
/// For a pipeline: the legality inputs, the physical facts, the policy's pick (never stronger than
/// the legality, which is an error here), the backend's rule for a loop that needs a view, and
/// then the nesting rule: a pipeline inside a pipeline that fans out runs serially. `prunes` is
/// whether the pipeline cannot fail.
///
/// The statements in value position are read under [`NestedReading::Hedged`]; see
/// [`place_all_reading`].
pub(crate) fn place_all(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	placement: &Placement,
	caps: BackendCaps,
) -> Result<Vec<(OpId, PlacementAttr)>, IrError> {
	place_all_reading(ir, facts, registry, placement, caps, NestedReading::Hedged)
}

/// [`place_all`] with the statements in value position read as `reading` states.
///
/// A container whose nested strategies are decided is placed under [`NestedReading::Strategy`]:
/// what the container holds is what runs, so a statement left `Apply` is evaluated for each row.
pub(crate) fn place_all_reading(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	placement: &Placement,
	caps: BackendCaps,
	reading: NestedReading,
) -> Result<Vec<(OpId, PlacementAttr)>, IrError> {
	let dag = PipelineDag::of(ir);
	let order = dag.topological_order().map_err(IrError::Verify)?;
	let mut judged: HashMap<OpId, Judged> = HashMap::with_capacity(order.len());
	for (_, op) in &order {
		let (legality, statement, loop_facts) = legality_inputs(ir, facts, registry, *op);
		let physical = facts_reading(ir, facts, registry, *op, reading)?;
		let picked = placement.pick(&legality, statement, &loop_facts, &physical);
		let decision = never_stronger(&legality, picked)
			.map_err(|stronger| stronger_than_legality(*op, stronger))?;
		let decision = apply_backend(decision, loop_in(ir, *op).map(|(_, kind)| kind), caps);
		let prunes =
			fallibility::of_with(ir, facts, &structural_cx(registry, ir), *op).is_infallible();
		judged.insert(
			*op,
			Judged {
				legality,
				statement,
				decision,
				facts: physical,
				prunes,
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
		let mut attr = PlacementAttr::new(entry.legality, entry.statement);
		attr.set_decision(Some(entry.decision));
		attr.set_facts(Some(entry.facts));
		attr.set_prunes(entry.prunes);
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
