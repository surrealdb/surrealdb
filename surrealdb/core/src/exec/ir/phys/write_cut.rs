//! Where a write loop's iterations are cut in two.
//!
//! A write that fans out runs the part of each iteration ahead of its cut on any task and the rest
//! in iteration order. The cut is the first stage the plan marks as apply work
//! ([`WriteMark::Apply`]); every stage ahead of it is marked compute, and the logical plan states
//! that every compute-marked stage is wholly in the loop's compute slice
//! ([`LoopFacts::cut_at_marks`]). A write whose iterations cannot be cut this way has no cut.

use super::facts::{pipeline_entries, subjects_are_range_mock};
use super::ops::{PhysOp, PhysWriteAttrs, WriteSubjectsShape};
use super::placement::loop_in;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::logical::ops::{LoopKind, WriteBranching, WriteMark, WriteStageKind};
use crate::exec::ir::logical::stages::stage_mutates_image;
use crate::exec::ir::op_kind::OpKind;

/// The cut of a write loop's iterations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteCut {
	/// The position, among the stage ops of the write loop's body, of the first stage the apply
	/// half runs: the stages at earlier positions of the order that is cut run ahead of the cut.
	pub(crate) stage: u16,
	/// Whether an op of the compute half reads stored data.
	pub(crate) compute_reads_data: bool,
	/// Whether the compute half encodes the record's after-image, because no stage between the cut
	/// and the stage that stores the image changes it.
	pub(crate) encode_at_cut: bool,
}

/// The cut of the write the pipeline `pipeline_op` holds, or the reason its iterations are not cut.
///
/// A write has a cut when it reads the rows of a source, or the ids of one range its statement
/// names, is the first op of its pipeline, runs its stages inline for one table
/// ([`PhysWriteAttrs`] with no statement text) in one order, and the logical plan states that
/// every stage marked compute is in the compute slice. The cut is at the first stage marked apply,
/// and a write whose compute half is empty has none. An order chosen from the record, and an
/// attempt that is rolled back, read or write the record ahead of the cut: such a write has no
/// cut. A write that ops of the rows' chain run ahead of has none either: the body that is cut is
/// the pipeline's first code.
pub(crate) fn write_cut_of(
	ir: &Ir,
	loop_facts: Option<&LoopFacts>,
	pipeline_op: OpId,
) -> Result<WriteCut, &'static str> {
	let loop_facts = loop_facts.ok_or(NOT_A_WRITE)?;
	if !loop_facts.cut_at_marks {
		return Err("a stage the plan marks compute holds an op that only the apply half may run");
	}
	let (loop_op, kind) = loop_in(ir, pipeline_op).ok_or(NOT_A_WRITE)?;
	if kind != LoopKind::Write {
		return Err(NOT_A_WRITE);
	}
	if pipeline_entries(ir, pipeline_op).first().map(|(op, _)| *op) != Some(loop_op) {
		return Err("ops of the rows' chain run ahead of the write");
	}
	let attrs = write_of(ir, pipeline_op).ok_or(NOT_A_WRITE)?;
	let over_rows = match attrs.subjects {
		WriteSubjectsShape::Rows => true,
		WriteSubjectsShape::Targets => subjects_are_range_mock(ir, loop_op),
		WriteSubjectsShape::Guaranteed
		| WriteSubjectsShape::Inserts
		| WriteSubjectsShape::Edges => false,
	};
	if !over_rows || attrs.write.statement.is_some() {
		return Err("the write does not run its stages inline over the rows of one table");
	}
	let body = ir.op(loop_op).and_then(|data| data.regions.first().copied()).ok_or(NOT_A_WRITE)?;
	let stages: Vec<(WriteStageKind, WriteMark)> = ops_in(ir, body)
		.filter_map(|op| match &ir.op(op)?.kind {
			OpKind::Phys(PhysOp::WriteStage {
				stage,
				mark,
			}) => Some((*stage, *mark)),
			OpKind::Phys(_) | OpKind::Logical(_) | OpKind::Expr(_) => None,
		})
		.collect();
	let marks: Vec<WriteMark> = stages.iter().map(|(_, mark)| *mark).collect();
	let order = match attrs.write.body.branching {
		WriteBranching::Sole => &attrs.write.body.first,
		WriteBranching::UpFront | WriteBranching::Retry(_) | WriteBranching::ByAction => {
			return Err("the write's body is not one order of stages");
		}
	};
	// The stage the apply half starts at, and a compute half that is a prefix of the order.
	let at = order
		.iter()
		.position(|position| {
			marks.get(usize::from(*position)).is_none_or(|mark| *mark == WriteMark::Apply)
		})
		.ok_or(NO_COMPUTE_HALF)?;
	let clean = order[at..].iter().all(|position| {
		marks.get(usize::from(*position)).is_none_or(|mark| *mark == WriteMark::Apply)
	});
	if at == 0 {
		return Err(NO_COMPUTE_HALF);
	}
	if !clean {
		return Err("a stage marked compute follows a stage marked apply");
	}
	// The after-image may be encoded ahead of the apply half when the stage that stores it is
	// reached through stages that do not change it.
	let encode_at_cut = order[at..]
		.iter()
		.map(|position| stages.get(usize::from(*position)).map(|(stage, _)| *stage))
		.take_while(|stage| *stage != Some(WriteStageKind::StoreRecordData))
		.all(|stage| stage.is_some_and(|stage| !stage_mutates_image(stage)))
		&& order[at..].iter().any(|position| {
			stages.get(usize::from(*position)).map(|(stage, _)| *stage)
				== Some(WriteStageKind::StoreRecordData)
		});
	Ok(WriteCut {
		stage: order[at],
		compute_reads_data: loop_facts.compute_reads_data,
		encode_at_cut,
	})
}

/// The reason of a write judged without the cut of its iterations.
pub(crate) const NO_CUT_STATED: &str = "the write's cut is not stated to the policy";

/// The reason of a pipeline that holds no write loop over rows.
const NOT_A_WRITE: &str = "the pipeline holds no write over the rows of a source";

/// The reason of a write whose first stage is one the apply half runs.
const NO_COMPUTE_HALF: &str = "the write's iterations have no cut with a compute half ahead of it";

/// The attributes of the write that consumes the rows of the pipeline `pipeline_op`.
fn write_of(ir: &Ir, pipeline_op: OpId) -> Option<&PhysWriteAttrs> {
	let result = ir.op(pipeline_op)?.results.first().copied()?;
	ir.uses(result).iter().find_map(|used| match &ir.op(used.user)?.kind {
		OpKind::Phys(PhysOp::Write(attrs)) => Some(&**attrs),
		OpKind::Phys(_) | OpKind::Logical(_) | OpKind::Expr(_) => None,
	})
}
