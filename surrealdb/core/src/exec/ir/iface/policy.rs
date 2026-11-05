//! The placement boundary: the one place that decides how each pipeline runs, within what the
//! logical plan allows.
//!
//! The boundary only narrows. A pick stronger than the logical combine allows is an internal
//! error. The VM may narrow a decision further at run time, but it never widens a serial one.

use super::effects::{ElemPath, Multiplicity};
use super::r#loop::{ApplyCut, Exit, LoopMode, SourceDelivery};
use crate::exec::ir::logical::ops::LoopSink;
#[allow(unused_imports)]
pub(crate) use crate::exec::ir::phys::placement_attr::PlacementAttr;
use crate::exec::logical::pipelines::{Legality, StatementLegality};
use crate::exec::planner::placement::{Decision, PhysicalFacts};

/// The per-loop facts that the projection to [`Legality`] drops: iteration order, what is left to
/// apply, how the loop may exit, what the sink combines, how the source arrives, how often an
/// identity repeats, and which key path the stream is keyed by.
#[derive(Clone, Debug)]
pub(crate) struct LoopFacts {
	pub(crate) mode: LoopMode,
	pub(crate) apply: ApplyCut,
	pub(crate) exit: Exit,
	pub(crate) sink: LoopSink,
	pub(crate) delivery: SourceDelivery,
	pub(crate) multiplicity: Multiplicity,
	pub(crate) key_path: ElemPath,
	/// For a write loop: whether an op the compute slice holds reads stored data. A compute half
	/// that reads data reads what the apply half of earlier iterations writes.
	pub(crate) compute_reads_data: bool,
	/// For a write loop: whether every stage the plan marks `Compute` is in the compute slice, so
	/// that the plan's marks are where the apply half starts.
	pub(crate) cut_at_marks: bool,
}

/// The single placement decision point.
pub(crate) trait PlacementBoundary {
	/// Decides how one pipeline runs.
	///
	/// The answer is the logical combine or a weaker one: `TopK`, `StableMerge`, `Concat` or
	/// `Serial`. It is made only from the legality the logical plan allows, the enclosing
	/// statement's legality, the loop's facts and the physical facts of the pipeline.
	fn place(
		&self,
		legality: &Legality,
		statement: StatementLegality,
		loop_facts: &LoopFacts,
		facts: &PhysicalFacts,
	) -> Decision;
}
