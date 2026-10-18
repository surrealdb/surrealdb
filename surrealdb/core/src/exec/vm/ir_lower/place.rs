//! Placement copied from the physical plan, and the provenance of the pipelines emitted.
//!
//! A pipeline's parallelism is the decision the physical plan attached to its op. Lowering copies
//! it and never derives one: the one change it makes is to `prunes`, which it narrows to what the
//! decision and the pipeline's own attribute both allow.

use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::phys::ops::PipelineId;
use crate::exec::planner::placement::{Decision, MergeOrder, Parallel};
use crate::exec::vm::program::Program;

/// Where each pipeline of a lowered program came from.
///
/// Each entry is the pipeline's index in `Program::pipelines`, the physical op it was lowered
/// from, and its pipeline id. Op ids are never reused within one container, so an entry names one
/// op for the life of the container. A pipeline that stands for no `phys.pipeline` op has no
/// entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Provenance {
	pub(crate) pipelines: Vec<(u16, OpId, PipelineId)>,
}

impl Provenance {
	/// Records that the pipeline at index `pipeline` of the program was lowered from `op`, whose
	/// pipeline id is `id`.
	///
	/// A pipeline holds at most one entry: recording an index again answers
	/// [`IrError::Stale`], and an index past the range of a program's pipeline indices answers
	/// [`IrError::Capacity`].
	#[allow(dead_code)] // Called by the `phys.pipeline` arm of `dispatch::lower`, which is not built.
	pub(super) fn record(
		&mut self,
		pipeline: usize,
		op: OpId,
		id: PipelineId,
	) -> Result<(), IrError> {
		let index = u16::try_from(pipeline).map_err(|_| IrError::Capacity("vm pipelines"))?;
		if self.origin(index).is_some() {
			return Err(IrError::Stale("ir_lower::place: pipeline recorded twice"));
		}
		self.pipelines.push((index, op, id));
		Ok(())
	}

	/// The op and the pipeline id the pipeline at index `pipeline` was lowered from, when it has
	/// an entry.
	#[allow(dead_code)] // Read by the prior-run statistics keyed by pipeline, which are not built.
	pub(crate) fn origin(&self, pipeline: u16) -> Option<(OpId, PipelineId)> {
		self.pipelines.iter().find(|(index, ..)| *index == pipeline).map(|(_, op, id)| (*op, *id))
	}
}

/// A program lowered from a container, with the provenance of its pipelines and the units it asks
/// the compile driver for.
#[derive(Debug)]
pub(crate) struct Lowered {
	pub(crate) program: Program,
	pub(crate) provenance: Provenance,
	/// The requests the lowering made for the first time, in the order it made them. The program
	/// names the unit of each that holds a slot by its id, and holds no code for any of them.
	pub(crate) needs: Vec<super::units::Needed>,
}

/// The parallelism of the pipeline `pipeline`, copied from its placement.
///
/// `paging_slot` is the slot of the program holding the paging clause a paged decision replays.
/// [`IrError::Invariant`] when the op has no placement, when the placement holds no decision
/// (the physical plan did not place this pipeline), or when the decision pages and no slot is
/// given. Every pipeline in a container is placed, and a unit copied from a region keeps the
/// placements of its pipelines. A paged decision is only made for a pipeline with one limit, so
/// a valid container never reaches the last case.
#[allow(dead_code)] // Called by the `phys.pipeline` and `phys.loop` arms of `dispatch::lower`.
pub(super) fn parallel_of(
	ir: &Ir,
	pipeline: OpId,
	paging_slot: Option<u16>,
) -> Result<Parallel, IrError> {
	let attr = ir.placement(pipeline).ok_or(IrError::Invariant {
		what: "ir_lower::place::parallel_of: a pipeline with no placement",
	})?;
	copy_decision(
		attr.decision(),
		attr.prunes(),
		attr.merge(),
		attr.ordered_fold().to_vec(),
		paging_slot,
	)
}

/// The parallelism of a pipeline whose placement holds `decision` and whose attribute answers
/// `attr_prunes`, `merge` and `ordered_fold`.
///
/// The decision is copied as it stands except for `prunes`, which holds only when the decision
/// and the attribute both hold it: a morsel is pruned only where neither says its body can raise.
/// The merge order and the fold order are the attribute's.
fn copy_decision(
	decision: Option<Decision>,
	attr_prunes: bool,
	merge: MergeOrder,
	ordered_fold: Vec<usize>,
	paging_slot: Option<u16>,
) -> Result<Parallel, IrError> {
	let mut decision = decision.ok_or(IrError::Invariant {
		what: "ir_lower::place::parallel_of: a pipeline the physical plan did not place",
	})?;
	if let Decision::Morsels {
		prunes,
		..
	} = &mut decision
	{
		*prunes = *prunes && attr_prunes;
	}
	decision.to_parallel(paging_slot, merge, ordered_fold).ok_or(IrError::Invariant {
		what: "ir_lower::place::parallel_of: a paged decision with no paging slot",
	})
}

#[cfg(test)]
mod tests {
	use smallvec::SmallVec;

	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::phys::ops::{PhysOp, PipelineAttrs};
	use crate::exec::ir::phys::placement::{NO_BACKEND_CAPS, Placement, install, place_all};
	use crate::exec::planner::placement::{Combine, MorselInput, MorselPlan};
	use crate::exec::vm::ir_lower::tests::Builder;

	fn morsels(paged: bool, prunes: bool) -> Decision {
		Decision::Morsels {
			input: MorselInput::Range,
			combine: Combine::Concat,
			paged,
			prunes,
		}
	}

	/// A container holding one `phys.pipeline` op and no placement.
	fn one_pipeline() -> (Builder, OpId) {
		let mut b = Builder::new();
		let op = b.add_phys(
			PhysOp::Pipeline(Box::new(PipelineAttrs {
				id: PipelineId(0),
				edges: SmallVec::new(),
				logical: None,
			})),
			&[],
		);
		(b, op)
	}

	#[test]
	fn unplaced_pipeline_is_internal_error() {
		let (b, op) = one_pipeline();
		assert!(matches!(
			parallel_of(&b.ir, op, None),
			Err(IrError::Invariant {
				what: "ir_lower::place::parallel_of: a pipeline with no placement"
			})
		));
		// A placement that holds no decision is the same refusal, worded for what it is.
		assert!(matches!(
			copy_decision(None, true, MergeOrder::Serial, Vec::new(), None),
			Err(IrError::Invariant {
				what: "ir_lower::place::parallel_of: a pipeline the physical plan did not place"
			})
		));
	}

	#[test]
	fn a_placed_pipeline_is_read_from_the_table() {
		let (mut b, op) = one_pipeline();
		let mut facts = FactCache::new();
		let placed = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), placed);
		let attr = b.ir.placement(op).expect("the pipeline is placed");
		let expected = attr.decision().and_then(|decision| {
			decision.to_parallel(None, attr.merge(), attr.ordered_fold().to_vec())
		});
		assert_eq!(parallel_of(&b.ir, op, None).ok(), expected);
	}

	/// The physical plan places every pipeline it holds, a pipeline inside a statement in value
	/// position included, and the unit a statement's region is copied into keeps the placement of
	/// each pipeline it holds: no pipeline reaches lowering without a decision.
	#[test]
	fn every_pipeline_the_plan_holds_is_placed_in_its_unit_too() {
		use crate::exec::ir::expr::ops::{ExprOp, NestedStrategy};
		use crate::exec::ir::iface::region_kind::RegionKind;
		use crate::val::Value;

		let (mut b, outer) = one_pipeline();
		let owner = b.add_with_regions(ExprOp::Subquery(NestedStrategy::Apply), &[]);
		let mut inner = None;
		b.region(owner, RegionKind::Subquery, |b| {
			inner = Some(b.add_phys(
				PhysOp::Pipeline(Box::new(PipelineAttrs {
					id: PipelineId(1),
					edges: SmallVec::new(),
					logical: None,
				})),
				&[],
			));
			b.literal(Value::from(1_i64))
		});
		let inner = inner.expect("the inner pipeline is built");
		let mut facts = FactCache::new();
		let placed = place_all(
			&b.ir,
			&mut facts,
			crate::exec::ir::analysis::cx::builtin_registry(),
			&Placement::default(),
			NO_BACKEND_CAPS,
		)
		.expect("the container is a DAG");
		install(b.ir.placement_table_mut(), placed);
		for op in [outer, inner] {
			assert!(parallel_of(&b.ir, op, None).is_ok(), "{op:?} is placed");
		}
		let region =
			*b.ir.op(owner).expect("the subquery exists").regions.first().expect("a region");
		let unit = b.ir.copy_region(region).expect("the region is copied");
		let copied = unit.map.map_op(inner).expect("the inner pipeline is copied");
		assert_eq!(parallel_of(&unit.ir, copied, None).ok(), parallel_of(&b.ir, inner, None).ok());
	}

	#[test]
	fn decision_copied_verbatim() {
		assert_eq!(
			copy_decision(Some(Decision::Serial), true, MergeOrder::Serial, Vec::new(), None).ok(),
			Some(Parallel::Serial)
		);
		assert_eq!(
			copy_decision(Some(morsels(true, true)), true, MergeOrder::Serial, Vec::new(), Some(3))
				.ok(),
			Some(Parallel::Morsels(MorselPlan {
				input: MorselInput::Range,
				combine: Combine::Concat,
				paged: Some(3),
				prunes: true,
				merge: MergeOrder::Serial,
				ordered_fold: Vec::new(),
			}))
		);
		// An unpaged decision ignores a slot it is given, and carries the order its merge takes
		// results in.
		assert_eq!(
			copy_decision(
				Some(morsels(false, false)),
				false,
				MergeOrder::Free,
				Vec::new(),
				Some(3)
			)
			.ok(),
			Some(Parallel::Morsels(MorselPlan {
				input: MorselInput::Range,
				combine: Combine::Concat,
				paged: None,
				prunes: false,
				merge: MergeOrder::Free,
				ordered_fold: Vec::new(),
			}))
		);
		// The order its float folds take is the attribute's.
		assert!(matches!(
			copy_decision(Some(morsels(false, false)), true, MergeOrder::Serial, vec![1, 3], None).ok(),
			Some(Parallel::Morsels(MorselPlan {
				ordered_fold,
				..
			})) if ordered_fold == [1, 3]
		));
		// A paged decision with no slot has no parallel form.
		assert!(matches!(
			copy_decision(Some(morsels(true, true)), true, MergeOrder::Serial, Vec::new(), None),
			Err(IrError::Invariant {
				what: "ir_lower::place::parallel_of: a paged decision with no paging slot"
			})
		));
	}

	#[test]
	fn prunes_is_the_conjunction() {
		for decision_prunes in [false, true] {
			for attr_prunes in [false, true] {
				let parallel = copy_decision(
					Some(morsels(false, decision_prunes)),
					attr_prunes,
					MergeOrder::Serial,
					Vec::new(),
					None,
				)
				.ok();
				let Some(Parallel::Morsels(plan)) = parallel else {
					panic!("a morsel decision copies to morsels");
				};
				assert_eq!(plan.prunes, decision_prunes && attr_prunes);
			}
		}
	}

	#[test]
	fn provenance_keys_each_pipeline_once() {
		let (_, op) = one_pipeline();
		let mut provenance = Provenance::default();
		assert_eq!(provenance.origin(0), None);
		assert!(provenance.record(0, op, PipelineId(7)).is_ok());
		assert_eq!(provenance.origin(0), Some((op, PipelineId(7))));
		assert_eq!(provenance.origin(1), None);
		assert!(matches!(
			provenance.record(0, op, PipelineId(8)),
			Err(IrError::Stale("ir_lower::place: pipeline recorded twice"))
		));
		assert!(matches!(
			provenance.record(usize::from(u16::MAX) + 1, op, PipelineId(9)),
			Err(IrError::Capacity("vm pipelines"))
		));
	}
}
