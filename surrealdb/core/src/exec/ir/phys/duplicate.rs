//! A copy of a region, or of a run of ops, inside the container that holds it.
//!
//! [`region`] re-creates the ops of a region, and of the regions they own, as a new region of the
//! same kind owned by another op. An operand defined outside the region is read by the copy as the
//! original reads it, so the copy runs wherever the original does; an operand defined inside is
//! read from the copy's own definition. Every `phys.pipeline` the region holds is given an id of
//! its own, and an edge between two of them names the copies.
//!
//! Each op of the copy records the provenance of the op it was copied from, and keeps its frame,
//! snapshot and location. The copy holds no placement: the placement of the container is derived
//! again once the rules that copy have run.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::ops::{PhysOp, PipelineId};
use super::pipeline::{PipelineDag, PipelineIds};
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::OpKind;

/// A copied region and the correspondence of its ops with the original's.
pub(super) struct Copied {
	/// The new region, owned by the op the copy was made for.
	pub(super) region: RegionId,
	/// Each op of the original region, and of the regions it owns, with the op that copies it.
	pub(super) ops: FxHashMap<OpId, OpId>,
}

/// State one copy threads through its ops: new ids for the values, ops and pipelines copied so far.
struct Copy {
	/// The copy of each value defined inside the original region.
	values: FxHashMap<ValueId, ValueId>,
	ops: FxHashMap<OpId, OpId>,
	/// The id of the copy of each pipeline of the original region.
	pipelines: FxHashMap<PipelineId, PipelineId>,
}

/// Copies `region` as a region of the same kind owned by `owner`, after the regions `owner`
/// already owns.
///
/// Answers [`IrError::Stale`] when `region` does not hold exactly one block. No region a rule
/// copies has that shape. On error the ops copied before the failing op stay in the container, so
/// the caller must discard a container whose rewrite failed.
pub(super) fn region(
	rw: &mut Rewriter<'_>,
	region: RegionId,
	owner: OpId,
) -> Result<Copied, IrError> {
	let mut copy = Copy {
		values: FxHashMap::default(),
		ops: FxHashMap::default(),
		pipelines: FxHashMap::default(),
	};
	let mut ids = PipelineIds::following(&PipelineDag::of(rw.ir()))?;
	for old in pipelines_in(rw.ir(), region) {
		copy.pipelines.insert(old, ids.allocate()?);
	}
	let copied = copy_region(rw, region, owner, &mut copy)?;
	Ok(Copied {
		region: copied,
		ops: copy.ops,
	})
}

/// The copy of a run of ops and the correspondence of its values and ops with the original's.
pub(super) struct CopiedOps {
	/// Each op of the original, and of the regions it owns, with the op that copies it.
	pub(super) ops: FxHashMap<OpId, OpId>,
	/// Each value an op of the original defines, with the value its copy defines.
	pub(super) values: FxHashMap<ValueId, ValueId>,
}

/// Copies `ops`, which stand in one block in definition order, to the end of `block`.
///
/// An operand defined outside `ops` is read by the copy as the original reads it, so the copy of
/// an op reads the copy of the op that defines its operand within `ops` and the original value
/// otherwise. Every `phys.pipeline` among `ops`, or in a region they hold, is given an id of its
/// own, and an edge between two of them names the copies. The copy keeps provenance, frame,
/// snapshot and location as [`region`] does.
pub(super) fn ops_into(
	rw: &mut Rewriter<'_>,
	ops: &[OpId],
	block: BlockId,
) -> Result<CopiedOps, IrError> {
	let mut copy = Copy {
		values: FxHashMap::default(),
		ops: FxHashMap::default(),
		pipelines: FxHashMap::default(),
	};
	let mut ids = PipelineIds::following(&PipelineDag::of(rw.ir()))?;
	for op in ops {
		for old in pipelines_of(rw.ir(), *op) {
			copy.pipelines.insert(old, ids.allocate()?);
		}
	}
	for op in ops {
		copy_op(rw, *op, block, &mut copy)?;
	}
	Ok(CopiedOps {
		ops: copy.ops,
		values: copy.values,
	})
}

/// The ids of the pipelines `op` is, and of those its regions hold at any depth.
fn pipelines_of(ir: &Ir, op: OpId) -> Vec<PipelineId> {
	let Some(data) = ir.op(op) else {
		return Vec::new();
	};
	let mut found = Vec::new();
	if let OpKind::Phys(PhysOp::Pipeline(attrs)) = &data.kind {
		found.push(attrs.id);
	}
	for region in &data.regions {
		found.extend(pipelines_in(ir, *region));
	}
	found
}

/// The ids of the pipelines `region` holds, at any depth.
fn pipelines_in(ir: &Ir, region: RegionId) -> Vec<PipelineId> {
	let mut found = Vec::new();
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Phys(PhysOp::Pipeline(attrs))) = ir.op(op).map(|data| &data.kind) {
			found.push(attrs.id);
		}
		Walk::Continue
	});
	found
}

fn copy_region(
	rw: &mut Rewriter<'_>,
	region: RegionId,
	owner: OpId,
	copy: &mut Copy,
) -> Result<RegionId, IrError> {
	let (kind, old_block) = {
		let held = rw.ir().region(region).ok_or(IrError::Stale("region"))?;
		let [block] = held.blocks.as_slice() else {
			return Err(IrError::Stale("a region of one block"));
		};
		(held.kind, *block)
	};
	let args: Vec<ValueId> =
		rw.ir().block(old_block).ok_or(IrError::Stale("block"))?.args.iter().copied().collect();
	let types: Vec<TypeKnowledge> = args
		.iter()
		.map(|arg| rw.ir().value(*arg).map_or(TypeKnowledge::Unknown, |value| value.ty.clone()))
		.collect();
	let (created, block) = rw.create_region(owner, kind, &types)?;
	let fresh: Vec<ValueId> = rw.ir().block(block).ok_or(IrError::Stale("block"))?.args.to_vec();
	for (old, new) in args.iter().zip(&fresh) {
		copy.values.insert(*old, *new);
	}
	let ops: Vec<OpId> = rw.ir().block(old_block).ok_or(IrError::Stale("block"))?.ops.clone();
	for old in ops {
		copy_op(rw, old, block, copy)?;
	}
	Ok(created)
}

fn copy_op(
	rw: &mut Rewriter<'_>,
	old: OpId,
	block: BlockId,
	copy: &mut Copy,
) -> Result<(), IrError> {
	let data = rw.ir().op(old).ok_or(IrError::Stale("op"))?.clone();
	let kind = match &data.kind {
		OpKind::Phys(PhysOp::Pipeline(attrs)) => {
			let mut renamed = attrs.clone();
			renamed.id = *copy.pipelines.get(&attrs.id).ok_or(IrError::Stale("pipeline"))?;
			for (_, other) in &mut renamed.edges {
				if let Some(copied) = copy.pipelines.get(other) {
					*other = *copied;
				}
			}
			OpKind::Phys(PhysOp::Pipeline(renamed))
		}
		other @ (OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) => other.clone(),
	};
	let spec = OpSpec {
		kind,
		operands: data
			.operands
			.iter()
			.map(|operand| copy.values.get(operand).copied().unwrap_or(*operand))
			.collect(),
		results: data
			.results
			.iter()
			.map(|result| {
				rw.ir().value(*result).map_or(TypeKnowledge::Unknown, |value| value.ty.clone())
			})
			.collect(),
		frame: data.frame,
		snapshot: data.snapshot,
		loc: data.loc.clone(),
	};
	let new = rw.derived(&[old], |rw| rw.create(InsertPoint::End(block), spec))?;
	rw.carry_decisions(old, new);
	let results: SmallVec<[ValueId; 2]> =
		rw.ir().op(new).ok_or(IrError::Stale("op"))?.results.clone();
	for (before, after) in data.results.iter().zip(&results) {
		copy.values.insert(*before, *after);
	}
	copy.ops.insert(old, new);
	for held in &data.regions {
		copy_region(rw, *held, new, copy)?;
	}
	Ok(())
}
