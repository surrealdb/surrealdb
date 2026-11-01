//! The pipeline graph of a statement: `phys.pipeline` ops and the edges between them.
//!
//! Each pipeline has an identifier, and an edge between two pipelines says that one feeds the
//! other, builds a structure the other probes, or probes one the other builds. A pipeline stands
//! for the logical ops in its provenance. Pipelines that lowering creates for itself (host code,
//! view and projection bodies, a single-row source) are not ops. Lowering returns them, and each
//! runs serially.
//!
//! Identifiers are unique across the pipelines of a container and never part of an op's identity.
//! The edges of a container form a DAG, and a pipeline runs after the pipelines its edges say
//! precede it: the topological order of [`PipelineDag`].
//!
//! A pipeline is cut where the planner's breakers cut it: a run of streaming ops ([`streams`])
//! above a source, ending below the sort, aggregation, split, join side or write that consumes its
//! rows.

#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};

use smallvec::{SmallVec, smallvec};

use super::ops::{MatchStepAttrs, PhysOp, PipelineAttrs, PipelineEdge, PipelineId, PrefetchKeys};
use super::schedule;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::footprint;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::{FxHashMap, map_with_capacity};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::location::Location;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::effects::IrResource;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::statements::BindStepShape;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LoopKind;
use crate::exec::ir::op_kind::OpKind;

/// The allocator of the pipeline ids of one container.
///
/// Ids are dense from zero in the order they are allocated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PipelineIds {
	next: u32,
}

impl PipelineIds {
	/// An allocator whose first id is zero.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// An allocator whose ids follow every id `dag` holds.
	pub(crate) fn following(dag: &PipelineDag) -> Result<Self, IrError> {
		let mut ids = Self::new();
		if let Some(top) = dag.nodes.iter().map(|(id, _)| id.0).max() {
			ids.next = top.checked_add(1).ok_or(IrError::Capacity("pipeline ids"))?;
		}
		Ok(ids)
	}

	/// The next id. Fails when no id is left to name another pipeline.
	pub(crate) fn allocate(&mut self) -> Result<PipelineId, IrError> {
		let id = PipelineId(self.next);
		self.next = self.next.checked_add(1).ok_or(IrError::Capacity("pipeline ids"))?;
		Ok(id)
	}
}

/// The pipelines of one container and the edges between them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PipelineDag {
	/// Every `phys.pipeline` op with its id, in pre-order.
	pub(crate) nodes: SmallVec<[(PipelineId, OpId); 4]>,
	/// Every edge as the pipeline that holds it, its kind and the pipeline it names, in the order
	/// of the nodes and then of each node's edges.
	pub(crate) edges: SmallVec<[(PipelineId, PipelineEdge, PipelineId); 4]>,
}

/// The pipeline an edge orders first and the one it orders after it, from the pipeline that holds
/// the edge, its kind and the pipeline it names.
fn precedence(
	owner: PipelineId,
	kind: PipelineEdge,
	other: PipelineId,
) -> (PipelineId, PipelineId) {
	match kind {
		// The other pipeline produces the rows, or builds the structure, this one consumes.
		PipelineEdge::Feeds | PipelineEdge::Builds => (other, owner),
		// This pipeline builds the structure the other probes.
		PipelineEdge::Probes => (owner, other),
	}
}

/// A rule of the pipeline graph that a container breaks.
fn fault(op: Option<OpId>, detail: String) -> VerifyError {
	VerifyError {
		op,
		rule: VerifyRule::PerOp,
		detail,
	}
}

impl PipelineDag {
	/// The pipelines of `ir`, found by a pre-order walk, with the edges their attributes hold.
	///
	/// Reads the attributes as they stand and checks nothing: [`PipelineDag::check_dag`] does.
	pub(crate) fn of(ir: &Ir) -> Self {
		let mut dag = Self::default();
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(PhysOp::Pipeline(attrs))) = ir.op(op).map(|data| &data.kind) {
				dag.nodes.push((attrs.id, op));
				for (kind, other) in &attrs.edges {
					dag.edges.push((attrs.id, *kind, *other));
				}
			}
			Walk::Continue
		});
		dag
	}

	/// The pipelines in an order that runs each after the ones its edges say precede it: a
	/// pipeline that feeds another comes first, a join's build side before its probe side.
	/// Pipelines with no order between them keep their pre-order position, so the answer is a
	/// function of the container alone.
	///
	/// Fails where [`PipelineDag::check_dag`] does.
	pub(crate) fn topological_order(
		&self,
	) -> Result<SmallVec<[(PipelineId, OpId); 4]>, VerifyError> {
		Ok(self.order()?.into_iter().map(|at| self.nodes[at]).collect())
	}

	/// Checks that every id names one pipeline, every edge names a pipeline of the container, and
	/// no cycle of edges orders a pipeline after itself.
	pub(crate) fn check_dag(&self) -> Result<(), VerifyError> {
		self.order().map(|_| ())
	}

	/// The indices of the nodes in run order. The one place the three rules of the DAG are
	/// checked.
	fn order(&self) -> Result<SmallVec<[usize; 4]>, VerifyError> {
		// Without edges the run order is pre-order, and only the ids need checking; a short list
		// is checked pairwise so the common plan of a few pipelines allocates nothing.
		if self.edges.is_empty() && self.nodes.len() <= 16 {
			for (at, (id, op)) in self.nodes.iter().enumerate() {
				if self.nodes[..at].iter().any(|(held, _)| held == id) {
					return Err(fault(Some(*op), format!("pipeline id {} is held twice", id.0)));
				}
			}
			return Ok((0..self.nodes.len()).collect());
		}
		let mut at_id: FxHashMap<PipelineId, usize> = map_with_capacity(self.nodes.len());
		for (at, (id, op)) in self.nodes.iter().enumerate() {
			if at_id.insert(*id, at).is_some() {
				return Err(fault(Some(*op), format!("pipeline id {} is held twice", id.0)));
			}
		}
		let mut after: Vec<Vec<usize>> = vec![Vec::new(); self.nodes.len()];
		let mut waiting = vec![0usize; self.nodes.len()];
		let mut seen: BTreeSet<(usize, usize)> = BTreeSet::new();
		for (owner, kind, other) in &self.edges {
			let Some(owner_at) = at_id.get(owner).copied() else {
				return Err(fault(
					None,
					format!("an edge is held by unknown pipeline {}", owner.0),
				));
			};
			let Some(other_at) = at_id.get(other).copied() else {
				return Err(fault(
					Some(self.nodes[owner_at].1),
					format!("pipeline {} has an edge to unknown pipeline {}", owner.0, other.0),
				));
			};
			let (first, second) = if precedence(*owner, *kind, *other).0 == *owner {
				(owner_at, other_at)
			} else {
				(other_at, owner_at)
			};
			if seen.insert((first, second)) {
				after[first].push(second);
				waiting[second] += 1;
			}
		}
		// Kahn's algorithm; of the pipelines ready to run, the one first in pre-order goes first.
		let mut ready: BinaryHeap<Reverse<usize>> =
			(0..self.nodes.len()).filter(|at| waiting[*at] == 0).map(Reverse).collect();
		let mut order = SmallVec::with_capacity(self.nodes.len());
		while let Some(Reverse(at)) = ready.pop() {
			order.push(at);
			for next in &after[at] {
				waiting[*next] -= 1;
				if waiting[*next] == 0 {
					ready.push(Reverse(*next));
				}
			}
		}
		if order.len() < self.nodes.len() {
			return Err(match (0..self.nodes.len()).find(|at| waiting[*at] > 0) {
				Some(at) => {
					let (id, op) = self.nodes[at];
					fault(Some(op), format!("pipeline {} is ordered after itself", id.0))
				}
				None => fault(None, "the pipeline edges contain a cycle".to_owned()),
			});
		}
		Ok(order)
	}
}

/// Collects the pipelines of `ir` into their graph and checks it.
///
/// Every `phys.pipeline` op holds an id unique in the container, every edge names a pipeline of
/// the container, and the edges form a DAG. Ids are allocated by [`wrap`]; this reads them and
/// never assigns one.
pub(crate) fn assign(ir: &Ir, _facts: &mut FactCache) -> Result<PipelineDag, IrError> {
	let dag = PipelineDag::of(ir);
	dag.check_dag().map_err(IrError::Verify)?;
	Ok(dag)
}

/// Whether a loop of `kind` is wrapped in a pipeline, the unit a placement decision is made for.
///
/// Every loop kind but a recursion may be fanned out, so each is a pipeline; a recursion's
/// iterations are serial across depths and the placement never has a decision to make for it.
pub(crate) fn fans_out(kind: LoopKind) -> bool {
	match kind {
		LoopKind::Select
		| LoopKind::Write
		| LoopKind::For
		| LoopKind::Closure
		| LoopKind::ElementFilter
		| LoopKind::GraphStep
		| LoopKind::Broadcast => true,
		LoopKind::Recurse => false,
	}
}

/// Wraps every fan-out `phys.loop` under `root` that no pipeline holds yet in a `phys.pipeline`,
/// and answers how many pipelines it built.
///
/// A loop whose kind [`fans_out`] is a pipeline, the unit a placement decision is made for;
/// conversion creates none, so each plan's pipelines are built once, when placement is decided
/// for that plan. The pipeline's `logical` is the first logical op the loop stands for, and `None`
/// for a loop that stands for itself. A select loop's pipeline also holds the run of streaming ops
/// that read the loop's rows one by one and that nothing else reads ([`trailing_of`]): a paging
/// clause, a distinct or a projection above the loop runs in the pipeline's body, so the placement
/// decision is made for the rows the pipeline delivers. Ids follow every id the container holds
/// and are taken once before the first wrap. A loop that stands in a `Pipeline` region is skipped,
/// so a second call builds none. Wrapping reads nothing physical, so no context is taken.
pub(crate) fn build(ir: &mut Ir, facts: &mut FactCache, root: RegionId) -> Result<usize, IrError> {
	let mut ids = PipelineIds::following(&PipelineDag::of(ir))?;
	let mut loops: Vec<(SmallVec<[OpId; 8]>, Option<OpId>)> = Vec::new();
	ir.walk(root, WalkOrder::PreOrder, &mut |op| {
		if let Some(data) = ir.op(op)
			&& let OpKind::Phys(PhysOp::Loop {
				kind,
				..
			}) = &data.kind
			&& fans_out(*kind)
			&& !in_pipeline(ir, op)
		{
			let logical = match &data.provenance {
				Provenance::From(ids) => ids.first().copied(),
				Provenance::Bound => None,
			};
			let mut chain: SmallVec<[OpId; 8]> = smallvec![op];
			if *kind == LoopKind::Select {
				chain.extend(trailing_of(ir, op));
			}
			loops.push((chain, logical));
		}
		Walk::Continue
	});
	let mut rw = Rewriter::new(ir, facts);
	for (chain, logical) in &loops {
		wrap(&mut rw, chain, *logical, &mut ids, SmallVec::new())?;
	}
	Ok(loops.len())
}

/// The streaming ops that follow the loop `loop_op` in its block, in data-flow order: each reads
/// the rows of the op before it as its first operand, is the only reader of them, and stands in
/// the loop's block with one result.
///
/// The run stops at the first op that is a breaker or that another op also reads, so every op it
/// names may move into the pipeline of the loop without changing another consumer.
fn trailing_of(ir: &Ir, loop_op: OpId) -> SmallVec<[OpId; 8]> {
	let mut trail = SmallVec::new();
	let Some(block) = ir.op(loop_op).map(|data| data.parent) else {
		return trail;
	};
	let mut at = loop_op;
	loop {
		let Some(data) = ir.op(at) else {
			return trail;
		};
		let [result] = data.results.as_slice() else {
			return trail;
		};
		let [read] = ir.uses(*result) else {
			return trail;
		};
		let Some(user) = ir.op(read.user) else {
			return trail;
		};
		let streaming = matches!(&user.kind, OpKind::Phys(kind) if streams(kind));
		if !streaming || read.operand != 0 || user.parent != block || user.results.len() != 1 {
			return trail;
		}
		trail.push(read.user);
		at = read.user;
	}
}

/// Whether the block of `op` belongs to a `Pipeline` region.
fn in_pipeline(ir: &Ir, op: OpId) -> bool {
	ir.region(ir.enclosing_region(op)).is_some_and(|region| region.kind == RegionKind::Pipeline)
}

/// Whether an op passes the rows of its input through one at a time, so that the pipeline it
/// stands in continues above it: a filter, a permission filter, a resolver layer, a limit, a
/// projection, a distinct, a fetch, the enforcement of a table, and the binding of each row of a
/// `MATCH` under a name.
///
/// Every other op is a breaker: it is a source, or it consumes its whole input (a sort, an
/// aggregation, a window, a split, a join side, a write, any other step of a binding table), or it
/// is not part of a row pipeline.
pub(crate) fn streams(op: &PhysOp) -> bool {
	matches!(
		op,
		PhysOp::Filter
			| PhysOp::PermissionFilter(..)
			| PhysOp::Layer(..)
			| PhysOp::Limit { .. }
			| PhysOp::Project(..)
			| PhysOp::Distinct { .. }
			| PhysOp::Fetch
			| PhysOp::Enforced { .. }
	) || binds_row(op)
}

/// Whether `op` is the step of a `MATCH` that wraps each row as `{ name: row }`.
pub(crate) fn binds_row(op: &PhysOp) -> bool {
	matches!(
		op,
		PhysOp::MatchStep(attrs)
			if matches!(attrs.as_ref(), MatchStepAttrs::Bind(BindStepShape::Bind { .. }))
	)
}

/// The ops of the pipeline whose last op is `top`, in data-flow order.
///
/// From `top`, the chain follows the first operand while the op it stands at [`streams`] and takes
/// the op that defines that operand as the next element: the first op that does not stream is the
/// source. The chain stops before an input that is not a physical op's result and before one that
/// has another user, because an op another consumer reads cannot move into this pipeline.
pub(crate) fn chain_of(ir: &Ir, top: OpId) -> Vec<OpId> {
	let mut chain = vec![top];
	let mut at = top;
	while let Some(data) = ir.op(at) {
		let OpKind::Phys(kind) = &data.kind else {
			break;
		};
		if !streams(kind) {
			break;
		}
		let Some(input) = data.operands.first().and_then(|input| ir.value(*input)) else {
			break;
		};
		let ValueDef::OpResult {
			op: producer,
			..
		} = input.def
		else {
			break;
		};
		let physical = matches!(ir.op(producer).map(|data| &data.kind), Some(OpKind::Phys(_)));
		if input.uses.len() != 1 || !physical {
			break;
		}
		chain.push(producer);
		at = producer;
	}
	chain.reverse();
	chain
}

/// What `wrap` reads of a chain it has checked.
struct Chain {
	frame: FrameId,
	snapshot: SnapshotId,
	loc: Location,
	result: TypeKnowledge,
}

/// Checks that `ops` is a chain `wrap` can move into a pipeline: physical ops of one block outside
/// any pipeline, each with one result, where every op after the first streams, reads the result
/// before it as its first operand and is that result's only user. Only the last op's result may be
/// used elsewhere.
fn check_chain(ir: &Ir, ops: &[OpId]) -> Result<Chain, IrError> {
	let broken = |op: OpId, detail: &str| {
		IrError::Verify(fault(Some(op), format!("pipeline chain: {detail}")))
	};
	let (Some(&first), Some(&last)) = (ops.first(), ops.last()) else {
		return Err(IrError::Stale("pipeline chain is empty"));
	};
	let first_data = ir.op(first).ok_or(IrError::Stale("pipeline chain op"))?;
	let mut previous: Option<ValueId> = None;
	for &op in ops {
		let data = ir.op(op).ok_or(IrError::Stale("pipeline chain op"))?;
		let OpKind::Phys(kind) = &data.kind else {
			return Err(broken(op, "an op is not physical"));
		};
		if data.parent != first_data.parent {
			return Err(broken(op, "the ops stand in different blocks"));
		}
		if data.results.len() != 1 {
			return Err(broken(op, "an op does not have exactly one result"));
		}
		if let Some(input) = previous {
			if !streams(kind) {
				return Err(broken(op, "an op after the first does not stream"));
			}
			if data.operands.first() != Some(&input) {
				return Err(broken(op, "an op does not read the result before it"));
			}
			if ir.uses(input).len() != 1 {
				return Err(broken(op, "a result inside the chain has another user"));
			}
		}
		previous = data.results.first().copied();
	}
	let in_pipeline = ir
		.block(first_data.parent)
		.and_then(|block| ir.region(block.parent))
		.is_some_and(|region| region.kind == RegionKind::Pipeline);
	if in_pipeline {
		return Err(broken(first, "the ops are already in a pipeline"));
	}
	let last_data = ir.op(last).ok_or(IrError::Stale("pipeline chain op"))?;
	let result = last_data
		.results
		.first()
		.and_then(|value| ir.value(*value))
		.map(|value| value.ty.clone())
		.ok_or(IrError::Stale("pipeline chain result"))?;
	Ok(Chain {
		frame: last_data.frame,
		snapshot: last_data.snapshot,
		loc: last_data.loc.clone(),
		result,
	})
}

/// Moves the chain `ops` into a new `phys.pipeline` op that stands after it, and answers the
/// pipeline.
///
/// `ops` is in data-flow order, as [`chain_of`] answers it. The pipeline holds a `Pipeline` region
/// with the chain, ends in a `phys.yield` of the last op's result, and takes the place of the last
/// op for every user of its result. It carries `edges` and `logical`, the logical op it stands
/// for. Every op it creates records the provenance of the chain and of `logical`, so `EXPLAIN` and
/// the legality snapshot find the logical plan from the pipeline.
///
/// The rewriter has no move of an op into a block, so the ops of the chain are re-created inside
/// the region with the same attributes, operands, frame, snapshot and location, their regions
/// moved across, and the originals are erased. A chain that fails its checks is left unchanged and
/// consumes no id. Pipelines lowering creates for its own host code are never wrapped.
pub(crate) fn wrap(
	rw: &mut Rewriter<'_>,
	ops: &[OpId],
	logical: Option<OpId>,
	ids: &mut PipelineIds,
	edges: SmallVec<[(PipelineEdge, PipelineId); 2]>,
) -> Result<OpId, IrError> {
	let chain = check_chain(rw.ir(), ops)?;
	let Some(&last) = ops.last() else {
		return Err(IrError::Stale("pipeline chain is empty"));
	};
	let attrs = PipelineAttrs {
		id: ids.allocate()?,
		edges,
		logical,
	};
	let mut from: SmallVec<[OpId; 8]> = ops.iter().copied().collect();
	from.extend(logical);

	let (pipeline, clones) = rw.derived(&from, |rw| -> Result<(OpId, Vec<OpId>), IrError> {
		let pipeline = rw.create(
			InsertPoint::After(last),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Pipeline(Box::new(attrs))),
				operands: SmallVec::new(),
				results: smallvec![chain.result.clone()],
				frame: chain.frame,
				snapshot: chain.snapshot,
				loc: chain.loc.clone(),
			},
		)?;
		let (_region, block) = rw.create_region(pipeline, RegionKind::Pipeline, &[])?;
		let mut clones: Vec<OpId> = Vec::with_capacity(ops.len());
		let mut produced: Option<ValueId> = None;
		for &old in ops {
			let (spec, regions) = clone_spec(rw.ir(), old, produced)?;
			let clone = rw.create(InsertPoint::End(block), spec)?;
			for region in regions {
				rw.move_region(region, clone)?;
			}
			produced = first_result(rw.ir(), clone);
			clones.push(clone);
		}
		let yielded = produced.ok_or(IrError::Stale("pipeline chain result"))?;
		rw.create(
			InsertPoint::End(block),
			OpSpec {
				kind: OpKind::Phys(PhysOp::Yield),
				operands: smallvec![yielded],
				results: SmallVec::new(),
				frame: chain.frame,
				snapshot: chain.snapshot,
				loc: chain.loc.clone(),
			},
		)?;
		Ok((pipeline, clones))
	})?;

	// Retire the originals from the last to the first, so that no op is erased while a user of its
	// result remains. The users of the last op's result read the pipeline.
	let pipeline_result =
		first_result(rw.ir(), pipeline).ok_or(IrError::Stale("pipeline result"))?;
	for (at, &old) in ops.iter().enumerate().rev() {
		let replacement = if at + 1 == ops.len() {
			pipeline_result
		} else {
			first_result(rw.ir(), clones[at]).ok_or(IrError::Stale("pipeline chain result"))?
		};
		rw.replace_op(old, &[replacement])?;
	}
	Ok(pipeline)
}

/// The first result of `op`.
fn first_result(ir: &Ir, op: OpId) -> Option<ValueId> {
	ir.op(op).and_then(|data| data.results.first().copied())
}

/// The description of a copy of `old`, whose first operand is `input` when one is given, and the
/// regions `old` owns.
fn clone_spec(
	ir: &Ir,
	old: OpId,
	input: Option<ValueId>,
) -> Result<(OpSpec, SmallVec<[RegionId; 2]>), IrError> {
	let data = ir.op(old).ok_or(IrError::Stale("pipeline chain op"))?;
	let mut operands = data.operands.clone();
	if let (Some(input), Some(slot)) = (input, operands.first_mut()) {
		*slot = input;
	}
	let results = data
		.results
		.iter()
		.map(|value| {
			ir.value(*value).map(|value| value.ty.clone()).ok_or(IrError::Stale("result value"))
		})
		.collect::<Result<_, _>>()?;
	Ok((
		OpSpec {
			kind: data.kind.clone(),
			operands,
			results,
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
		},
		data.regions.clone(),
	))
}

/// How a read of a loop's body names the stored keys it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrefetchShape {
	/// One key: a record read by id.
	Point,
	/// A key range: the records an index lists under one key.
	Range,
}

/// One read of a loop's body that a batched read can cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrefetchRead {
	/// The read op.
	pub(crate) op: OpId,
	/// The value its key is built from: the read's first operand.
	pub(crate) key: ValueId,
	pub(crate) shape: PrefetchShape,
}

impl PrefetchRead {
	/// The operands the prefetch op takes for this read: the key for a point, the key twice for a
	/// range, since one key names both ends of the range the read covers.
	pub(crate) fn operands(&self) -> SmallVec<[ValueId; 2]> {
		match self.shape {
			PrefetchShape::Point => smallvec![self.key],
			PrefetchShape::Range => smallvec![self.key, self.key],
		}
	}
}

/// The operand indices of a prefetch op that takes the operands of `reads` in order.
///
/// Each read contributes the operands of [`PrefetchRead::operands`]: a point names its one
/// operand, a range names the pair that holds its begin and end keys. `None` when no read is given
/// or the operands are more than an index can name.
pub(crate) fn prefetch_keys(reads: &[PrefetchRead]) -> Option<PrefetchKeys> {
	let mut keys = PrefetchKeys {
		points: SmallVec::new(),
		ranges: SmallVec::new(),
	};
	let mut next: u16 = 0;
	for read in reads {
		match read.shape {
			PrefetchShape::Point => {
				keys.points.push(next);
				next = next.checked_add(1)?;
			}
			PrefetchShape::Range => {
				keys.ranges.push((next, next.checked_add(1)?));
				next = next.checked_add(2)?;
			}
		}
	}
	(next > 0).then_some(keys)
}

/// A read of a loop body with the resources it reads.
struct Candidate {
	read: PrefetchRead,
	reads: Vec<IrResource>,
}

/// The reads of the loop `loop_op` that one batched read can cover, in pre-order.
///
/// A read qualifies when
///
/// - it is a record read by id (never a locked read) or an index lookup, with its key as its first
///   operand;
/// - it runs on every iteration that completes: it stands in the loop's body through statements,
///   blocks and pipelines only, never under a condition, a clause, a nested loop or a choice, and
///   the loop holds no `break`, `continue` or `return` that can end an iteration before it;
/// - its footprint is disjoint from every write of the enclosing statement, triggered writes
///   included, so a key the statement writes is never batched.
///
/// A loop is judged the same whether its iterations run in order or fan out: the keys do not
/// depend on it. Nothing qualifies for an op that is not a `phys.loop` or a loop outside a
/// statement.
pub(crate) fn loop_prefetch_reads(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	loop_op: OpId,
) -> Vec<PrefetchRead> {
	let Some(OpKind::Phys(PhysOp::Loop {
		..
	})) = ir.op(loop_op).map(|data| &data.kind)
	else {
		return Vec::new();
	};
	let Some(statement) = enclosing_statement(ir, loop_op) else {
		return Vec::new();
	};
	if loop_exits_early(ir, loop_op) {
		return Vec::new();
	}
	let mut found = Vec::new();
	collect_reads(ir, loop_op, &mut found);
	let under = structural_cx(registry, ir);
	let writes = footprint::of_with(ir, facts, &under, statement).writes;
	let candidates = found
		.into_iter()
		.map(|read| Candidate {
			reads: footprint::of_with(ir, facts, &under, read.op).reads.into_iter().collect(),
			read,
		})
		.collect();
	disjoint_reads(candidates, &writes)
}

/// The point keys and key ranges of the reads of the loop `loop_op` that one batched read covers:
/// the operand indices of the loop's `phys.prefetch`, which takes the operands of
/// [`loop_prefetch_reads`] in order. `None` when no read qualifies.
///
/// It decides and inserts nothing: a `phys.prefetch` is inserted only once
/// [`schedule::PREFETCH_EXECUTABLE`] holds.
pub(crate) fn loop_prefetch(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	loop_op: OpId,
) -> Option<PrefetchKeys> {
	prefetch_keys(&loop_prefetch_reads(ir, facts, registry, loop_op))
}

/// The reads of the candidates none of whose resources a write overlaps.
fn disjoint_reads(candidates: Vec<Candidate>, writes: &[IrResource]) -> Vec<PrefetchRead> {
	candidates
		.into_iter()
		.filter(|candidate| {
			!candidate
				.reads
				.iter()
				.any(|read| writes.iter().any(|write| schedule::overlaps(write, read)))
		})
		.map(|candidate| candidate.read)
		.collect()
}

/// Whether an op runs whenever control reaches it, so that the ops of its regions run on every
/// iteration that reaches the op.
fn runs_whenever_reached(op: &PhysOp) -> bool {
	matches!(
		op,
		PhysOp::Stmt { .. }
			| PhysOp::Statement { .. }
			| PhysOp::Block
			| PhysOp::Subquery
			| PhysOp::Pipeline(..)
	)
}

/// Whether any op under `loop_op` can end an iteration early.
fn loop_exits_early(ir: &Ir, loop_op: OpId) -> bool {
	let Some(data) = ir.op(loop_op) else {
		return false;
	};
	let mut exits = false;
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			let ends = matches!(
				ir.op(op).map(|data| &data.kind),
				Some(OpKind::Phys(PhysOp::Break | PhysOp::Next | PhysOp::Return))
			);
			if ends {
				exits = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	exits
}

/// Appends the point and range reads under `owner` that run on every iteration, descending only
/// through ops that run whenever they are reached.
fn collect_reads(ir: &Ir, owner: OpId, found: &mut Vec<PrefetchRead>) {
	let Some(data) = ir.op(owner) else {
		return;
	};
	for region in &data.regions {
		for op in ops_in(ir, *region) {
			let Some(inner) = ir.op(op) else {
				continue;
			};
			let OpKind::Phys(kind) = &inner.kind else {
				continue;
			};
			let shape = if matches!(kind, PhysOp::RecordScan(attrs) if !attrs.for_update) {
				Some(PrefetchShape::Point)
			} else if matches!(kind, PhysOp::IndexLookup(..)) {
				Some(PrefetchShape::Range)
			} else {
				None
			};
			if let (Some(shape), Some(key)) = (shape, inner.operands.first().copied()) {
				found.push(PrefetchRead {
					op,
					key,
					shape,
				});
			}
			if runs_whenever_reached(kind) {
				collect_reads(ir, op, found);
			}
		}
	}
}

/// The nearest region around `op` whose kind satisfies `wanted`, and the op that owns it.
pub(super) fn enclosing_region_of(
	ir: &Ir,
	op: OpId,
	wanted: impl Fn(RegionKind) -> bool,
) -> Option<(OpId, RegionId)> {
	let mut region = ir.enclosing_region(op);
	loop {
		let owner = ir.parent_op(region)?;
		if ir.region(region).is_some_and(|data| wanted(data.kind)) {
			return Some((owner, region));
		}
		region = ir.enclosing_region(owner);
	}
}

/// The op whose `Statement` region is the nearest around `op`.
pub(super) fn enclosing_statement(ir: &Ir, op: OpId) -> Option<OpId> {
	enclosing_region_of(ir, op, |kind| matches!(kind, RegionKind::Statement(_)))
		.map(|(owner, _)| owner)
}

#[cfg(test)]
mod tests {
	use slotmap::SlotMap;
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::LocOrigin;
	use crate::exec::ir::iface::effects::{FieldSet, KeyOf};
	use crate::exec::ir::phys::ops::Batch;
	use crate::exec::vm::payload::Spill;

	/// `n` distinct op ids.
	fn op_ids(n: usize) -> Vec<OpId> {
		let mut arena: SlotMap<OpId, ()> = SlotMap::with_key();
		(0..n).map(|_| arena.insert(())).collect()
	}

	/// `n` distinct value ids.
	fn value_ids(n: usize) -> Vec<ValueId> {
		let mut arena: SlotMap<ValueId, ()> = SlotMap::with_key();
		(0..n).map(|_| arena.insert(())).collect()
	}

	fn dag(ids: &[u32], edges: &[(u32, PipelineEdge, u32)]) -> (PipelineDag, Vec<OpId>) {
		let ops = op_ids(ids.len());
		let dag = PipelineDag {
			nodes: ids.iter().zip(&ops).map(|(id, op)| (PipelineId(*id), *op)).collect(),
			edges: edges
				.iter()
				.map(|(owner, kind, other)| (PipelineId(*owner), *kind, PipelineId(*other)))
				.collect(),
		};
		(dag, ops)
	}

	fn order_of(dag: &PipelineDag) -> Vec<u32> {
		dag.topological_order()
			.expect("the graph is a DAG")
			.into_iter()
			.map(|(id, _)| id.0)
			.collect()
	}

	fn data(table: &str) -> IrResource {
		IrResource::Data {
			table: Some(TableName::new(Strand::new(table))),
			fields: FieldSet::All,
			key: KeyOf::Any,
		}
	}

	#[test]
	fn a_recursion_is_the_only_loop_kept_out_of_a_pipeline() {
		for kind in [
			LoopKind::Select,
			LoopKind::Write,
			LoopKind::For,
			LoopKind::Closure,
			LoopKind::ElementFilter,
			LoopKind::GraphStep,
			LoopKind::Broadcast,
		] {
			assert!(fans_out(kind));
		}
		assert!(!fans_out(LoopKind::Recurse));
	}

	#[test]
	fn ids_are_dense_from_zero() {
		let mut ids = PipelineIds::new();
		let issued: Vec<u32> = (0..4).map(|_| ids.allocate().expect("an id is left").0).collect();
		assert_eq!(issued, vec![0, 1, 2, 3]);
	}

	#[test]
	fn ids_run_out_without_wrapping() {
		let mut ids = PipelineIds {
			next: u32::MAX,
		};
		assert!(matches!(ids.allocate(), Err(IrError::Capacity(_))));
	}

	#[test]
	fn ids_follow_the_ids_a_graph_holds() {
		let (graph, _) = dag(&[0, 7, 3], &[]);
		let mut ids = PipelineIds::following(&graph).expect("an id is left");
		assert_eq!(ids.allocate().expect("an id is left"), PipelineId(8));
		assert_eq!(
			PipelineIds::following(&PipelineDag::default()).expect("an empty graph"),
			PipelineIds::new()
		);
	}

	#[test]
	fn a_pipeline_that_feeds_another_precedes_it() {
		// Pipeline 0 is fed by 1, which is fed by 2.
		let (graph, _) =
			dag(&[0, 1, 2], &[(0, PipelineEdge::Feeds, 1), (1, PipelineEdge::Feeds, 2)]);
		assert_eq!(order_of(&graph), vec![2, 1, 0]);
	}

	#[test]
	fn a_build_side_precedes_the_side_that_probes_it() {
		// Pipeline 1 builds what 0 probes, and the edge may be stated from either side.
		let (from_probe, _) = dag(&[0, 1], &[(0, PipelineEdge::Builds, 1)]);
		let (from_build, _) = dag(&[0, 1], &[(1, PipelineEdge::Probes, 0)]);
		let (from_both, _) =
			dag(&[0, 1], &[(0, PipelineEdge::Builds, 1), (1, PipelineEdge::Probes, 0)]);
		for graph in [from_probe, from_build, from_both] {
			assert_eq!(order_of(&graph), vec![1, 0]);
		}
	}

	#[test]
	fn pipelines_with_no_order_keep_their_position() {
		let (graph, _) = dag(&[5, 2, 9], &[]);
		assert_eq!(order_of(&graph), vec![5, 2, 9]);
	}

	#[test]
	fn a_repeated_edge_orders_once() {
		let (graph, _) = dag(
			&[0, 1],
			&[
				(0, PipelineEdge::Feeds, 1),
				(0, PipelineEdge::Builds, 1),
				(1, PipelineEdge::Probes, 0),
			],
		);
		assert_eq!(order_of(&graph), vec![1, 0]);
	}

	#[test]
	fn a_cycle_is_refused() {
		let (graph, ops) =
			dag(&[0, 1], &[(0, PipelineEdge::Feeds, 1), (1, PipelineEdge::Feeds, 0)]);
		let err = graph.check_dag().expect_err("a cycle is not a DAG");
		assert_eq!(err.rule, VerifyRule::PerOp);
		assert!(err.op.is_some_and(|op| ops.contains(&op)));
		assert!(graph.topological_order().is_err());
	}

	#[test]
	fn a_pipeline_cannot_feed_itself() {
		let (graph, _) = dag(&[0], &[(0, PipelineEdge::Feeds, 0)]);
		assert!(graph.check_dag().is_err());
	}

	#[test]
	fn a_dangling_edge_is_refused() {
		let (graph, ops) = dag(&[0, 1], &[(0, PipelineEdge::Feeds, 7)]);
		let err = graph.check_dag().expect_err("pipeline 7 does not exist");
		assert_eq!(err.op, Some(ops[0]));
	}

	#[test]
	fn a_repeated_id_is_refused() {
		let (graph, ops) = dag(&[3, 3], &[]);
		let err = graph.check_dag().expect_err("an id names two pipelines");
		assert_eq!(err.op, Some(ops[1]));
	}

	#[test]
	fn only_row_streaming_ops_continue_a_pipeline() {
		assert!(streams(&PhysOp::Filter));
		assert!(streams(&PhysOp::Fetch));
		assert!(streams(&PhysOp::Limit {
			limit: None,
			start: None,
		}));
		assert!(streams(&PhysOp::Distinct {
			spill: Spill::Bounded,
		}));
		assert!(!streams(&PhysOp::Union));
		assert!(!streams(&PhysOp::Split {
			targets: Box::new([]),
		}));
		let step = |shape| PhysOp::MatchStep(Box::new(MatchStepAttrs::Bind(shape)));
		assert!(streams(&step(BindStepShape::Bind {
			name: "a".to_owned(),
		})));
		assert!(!streams(&step(BindStepShape::Drain)));
		assert!(!streams(&PhysOp::Eval));
		assert!(!streams(&PhysOp::Yield));
	}

	#[test]
	fn a_point_names_one_operand_and_a_range_two() {
		let ops = op_ids(2);
		let values = value_ids(2);
		let point = |at: usize| PrefetchRead {
			op: ops[at],
			key: values[at],
			shape: PrefetchShape::Point,
		};
		let range = PrefetchRead {
			op: ops[1],
			key: values[1],
			shape: PrefetchShape::Range,
		};
		let keys = prefetch_keys(&[point(0), range, point(0)]).expect("reads are given");
		assert_eq!(keys.points.as_slice(), &[0, 3]);
		assert_eq!(keys.ranges.as_slice(), &[(1, 2)]);
		assert_eq!(range.operands().as_slice(), &[values[1], values[1]]);
		assert!(prefetch_keys(&[]).is_none());
	}

	/// A container whose top block holds one childless `phys.loop` of each of `kinds`.
	fn loops(kinds: &[LoopKind]) -> Ir {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		for kind in kinds {
			rw.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Loop {
						kind: *kind,
						first: false,
						batch: Batch {
							max_iterations: 1,
							max_bytes: 1,
						},
						unroll: 1,
						value: false,
						step: None,
					}),
					operands: SmallVec::new(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: Location {
						stmt: 0,
						span: None,
						origin: LocOrigin::Synthesized,
					},
				},
			)
			.expect("the loop is created");
		}
		ir
	}

	fn count_of(ir: &Ir, want: fn(&PhysOp) -> bool) -> usize {
		let mut found = 0;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(OpKind::Phys(kind)) = ir.op(op).map(|data| &data.kind)
				&& want(kind)
			{
				found += 1;
			}
			Walk::Continue
		});
		found
	}

	fn is_pipeline(op: &PhysOp) -> bool {
		matches!(op, PhysOp::Pipeline(_))
	}

	fn is_loop(op: &PhysOp) -> bool {
		matches!(op, PhysOp::Loop { .. })
	}

	#[test]
	fn build_wraps_each_fan_out_loop_once() {
		let mut ir = loops(&[LoopKind::For, LoopKind::Select, LoopKind::Recurse]);
		let mut facts = FactCache::new();
		let top = ir.top();
		assert_eq!(build(&mut ir, &mut facts, top).expect("the loops are wrapped"), 2);
		assert_eq!(count_of(&ir, is_pipeline), 2);
		assert_eq!(count_of(&ir, is_loop), 3);
		assert_eq!(build(&mut ir, &mut facts, top).expect("nothing is left to wrap"), 0);
		assert_eq!(count_of(&ir, is_pipeline), 2);
		let mut ids: Vec<u32> = PipelineDag::of(&ir).nodes.iter().map(|(id, _)| id.0).collect();
		ids.sort_unstable();
		assert_eq!(ids, [0, 1]);
	}

	#[test]
	fn build_wraps_nothing_in_a_container_without_a_fan_out_loop() {
		let mut ir = loops(&[LoopKind::Recurse]);
		let top = ir.top();
		assert_eq!(build(&mut ir, &mut FactCache::new(), top).expect("nothing to wrap"), 0);
		assert_eq!(count_of(&ir, is_pipeline), 0);
	}

	/// A top block holding a select loop followed by one op of each of `trail`, each reading the
	/// rows of the one before; answers the container and the ops, the loop first.
	fn select_then(trail: Vec<PhysOp>) -> (Ir, Vec<OpId>) {
		let mut ir = loops(&[LoopKind::Select]);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
		let first = ir.block(top).expect("the block exists").ops[0];
		let mut ops = vec![first];
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		for kind in trail {
			let rows = rw.ir().op(*ops.last().expect("the loop")).expect("the op").results[0];
			let op = rw
				.create(
					InsertPoint::End(top),
					OpSpec {
						kind: OpKind::Phys(kind),
						operands: smallvec![rows],
						results: smallvec![TypeKnowledge::Unknown],
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: Location {
							stmt: 0,
							span: None,
							origin: LocOrigin::Synthesized,
						},
					},
				)
				.expect("the op is created");
			ops.push(op);
		}
		(ir, ops)
	}

	/// The ops of the region of the only pipeline of `ir`, by tag.
	fn pipeline_tags(ir: &Ir) -> Vec<super::super::ops::PhysTag> {
		let (_, pipeline) = PipelineDag::of(ir).nodes[0];
		let region = ir.op(pipeline).expect("the pipeline").regions[0];
		ops_in(ir, region)
			.filter_map(|op| match &ir.op(op)?.kind {
				OpKind::Phys(kind) => Some(kind.tag()),
				OpKind::Expr(_) | OpKind::Logical(_) => None,
			})
			.collect()
	}

	fn limit() -> PhysOp {
		PhysOp::Limit {
			limit: None,
			start: None,
		}
	}

	#[test]
	fn a_select_loops_pipeline_holds_the_streaming_ops_that_follow_it() {
		use super::super::ops::PhysTag;
		let (mut ir, _) = select_then(vec![
			PhysOp::Distinct {
				spill: Spill::Spillable,
			},
			limit(),
		]);
		let top = ir.top();
		assert_eq!(build(&mut ir, &mut FactCache::new(), top).expect("wrapped"), 1);
		assert_eq!(
			pipeline_tags(&ir),
			[PhysTag::Loop, PhysTag::Distinct, PhysTag::Limit, PhysTag::Yield]
		);
		// Nothing is left outside the pipeline but the pipeline.
		assert_eq!(count_of(&ir, |op| matches!(op, PhysOp::Limit { .. })), 1);
		let block = ir.region(ir.top()).expect("the top region").blocks[0];
		assert_eq!(ir.block(block).expect("the block").ops.len(), 1);
		assert_eq!(build(&mut ir, &mut FactCache::new(), top).expect("nothing left"), 0);
	}

	#[test]
	fn a_streaming_op_two_ops_read_ends_the_pipeline_after_it() {
		use super::super::ops::PhysTag;
		let (mut ir, ops) = select_then(vec![limit()]);
		// The limit's rows are read twice, so neither reader may move into the pipeline.
		let top_block = ir.region(ir.top()).expect("the top region").blocks[0];
		let rows = ir.op(ops[1]).expect("the limit").results[0];
		let mut facts = FactCache::new();
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		for _ in 0..2 {
			rw.create(
				InsertPoint::End(top_block),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Fetch),
					operands: smallvec![rows],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: Location {
						stmt: 0,
						span: None,
						origin: LocOrigin::Synthesized,
					},
				},
			)
			.expect("a reader is created");
		}
		let top = ir.top();
		assert_eq!(build(&mut ir, &mut FactCache::new(), top).expect("wrapped"), 1);
		assert_eq!(pipeline_tags(&ir), [PhysTag::Loop, PhysTag::Limit, PhysTag::Yield]);
		assert_eq!(count_of(&ir, |op| matches!(op, PhysOp::Fetch)), 2);
	}

	#[test]
	fn a_breaker_after_a_select_loop_ends_the_pipeline() {
		use super::super::ops::PhysTag;
		let (mut ir, _) = select_then(vec![limit(), PhysOp::Only, PhysOp::Fetch]);
		let top = ir.top();
		assert_eq!(build(&mut ir, &mut FactCache::new(), top).expect("wrapped"), 1);
		assert_eq!(pipeline_tags(&ir), [PhysTag::Loop, PhysTag::Limit, PhysTag::Yield]);
	}

	#[test]
	fn a_read_a_write_reaches_is_not_batched() {
		let ops = op_ids(2);
		let values = value_ids(2);
		let candidate = |at: usize, table: &str| Candidate {
			read: PrefetchRead {
				op: ops[at],
				key: values[at],
				shape: PrefetchShape::Point,
			},
			reads: vec![data(table)],
		};
		let kept =
			disjoint_reads(vec![candidate(0, "post"), candidate(1, "user")], &[data("post")]);
		assert_eq!(kept.len(), 1);
		assert_eq!(kept[0].op, ops[1]);
		assert!(disjoint_reads(vec![candidate(0, "post")], &[IrResource::Unknown]).is_empty());
	}
}
