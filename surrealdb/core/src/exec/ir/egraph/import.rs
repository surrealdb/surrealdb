//! Import: which part of a statement enters the e-graph, and how it is encoded.
//!
//! A statement is an effectful skeleton with islands of pure expression and relational ops
//! hanging off it. The skeleton stays in the container; an island enters the e-graph as op nodes,
//! and everything an island reads that it cannot rewrite enters as an opaque leaf. The leaf of a
//! value is one class however many ops read it, so two reads of one binding share a kid.
//!
//! [`islands`] classifies the statement without building anything. [`import`] encodes the
//! classified ops into a fresh graph and records, per island root, the uses that leave the island,
//! so a write-back knows which operands to re-point. [`import_seed`] adds the physical ops a
//! conversion of a statement copy produced, as seeds of one candidate.
//!
//! Every class an import creates carries the facts the logical analyses answer for the op it
//! stands for. A node's identity is the value identity of its op ([`identity`]): an op whose key
//! is opaque is a leaf that equals nothing else, and the write epoch of a read is renumbered per
//! graph.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use egg::{EGraph, Id};
use smallvec::{SmallVec, smallvec};

use super::SeedHost;
use super::analysis::{
	IrAnalysis, OpaqueLeaf, SeedInfo, add_with_facts, reads_written, recanonicalise_seeds,
	weaker_determinism,
};
use super::decl::{
	BinderId, ClassFacts, ClassRef, EGRAPH_LITERAL_MAX_BYTES, EpochKey, LeafId, NodeIdentity,
	NodeShape, OrderSig, StmtMap,
};
use super::guards::{UnionWhy, guarded_union};
use super::lang::{ENode, region_kind_key};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::{effects, fallibility, footprint, identity, types, write_epoch};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::core::rewriter::Provenance;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::bounds::value_exceeds;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{Determinism, FieldSet, IrFootprint, IrResource, KeyOf};
use crate::exec::ir::iface::egraph::EgraphLimits;
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId, ValueId, WriteEpoch};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind};
use crate::exec::ir::op_kind::{OpKind, OpTag, Terminator};
use crate::expr::logical::{EffectSet, EvalMode};
use crate::val::Value;

/// The effects an expression op may have and still be an op node: reads of stored data, the
/// catalog, the session, the row and the statement clock. An external read is not among them: it
/// is not deterministic, so its value key is opaque.
const ISLAND_EFFECTS: EffectSet = EffectSet::READS_DATA
	.union(EffectSet::READS_CATALOG)
	.union(EffectSet::READS_ROW)
	.union(EffectSet::READS_SESSION)
	.union(EffectSet::CLOCK);

/// How an op of the statement takes part in the import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpRole {
	/// Encoded as an op node.
	Node,
	/// Encoded as one opaque leaf per result value. The op stays in place.
	Leaf,
	/// The yield that closes an imported region: its operands are the region node's kids.
	Yield,
	/// Not encoded. Operands that are island values are island roots.
	Skeleton,
}

/// How a region owned by an op node is encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegionRole {
	/// A region node, whose kids are the classes the region yields.
	Node,
	/// An opaque leaf. The region stays attached to its owner and no op moves into or out of it.
	Leaf,
}

/// The classification of one statement.
#[derive(Debug, Default)]
pub(crate) struct Islands {
	/// The role of every op the classification reached: the direct ops of the statement's own
	/// regions and the ops of every imported region beneath an op node. An op in no entry is
	/// inside a region that stays opaque, or inside a region of a skeleton op.
	pub(crate) ops: HashMap<OpId, OpRole>,
	/// The role of every region owned by an op node.
	pub(crate) regions: HashMap<RegionId, RegionRole>,
	/// The op nodes and op leaves, each after the ops of the regions it owns, so an op follows
	/// every op that defines one of its operands.
	pub(crate) order: Vec<OpId>,
}

/// Why an import produced no graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImportStop {
	/// The graph would hold more nodes than the limit allows.
	Limit(Limit),
	/// The statement names an op, value, region or block the container does not hold.
	Dangling,
	/// A seed copy cannot be encoded as seeds of its candidate.
	Seed(&'static str),
}

/// A value of an island that an op outside the island reads.
#[derive(Clone, Debug)]
pub(crate) struct IslandRoot {
	pub(crate) value: ValueId,
	pub(crate) class: Id,
	/// The `(user, operand index)` pairs outside the island that read `value`, in use-list order.
	pub(crate) uses: SmallVec<[(OpId, u16); 2]>,
}

/// An e-graph of one statement, with the maps a write-back and the seeding need.
#[derive(Clone)]
pub(crate) struct Imported {
	pub(crate) eg: EGraph<ENode, IrAnalysis>,
	/// The class of every value the import gave one: island values, the leaves of opaque ops, the
	/// leaves of values defined outside the island and the block arguments of imported regions.
	pub(crate) values: HashMap<ValueId, Id>,
	/// The island values read outside the island, in program order.
	pub(crate) roots: Vec<IslandRoot>,
	pub(crate) limits: EgraphLimits,
	renumber: Renumber,
	next_ordinal: u32,
}

impl Imported {
	/// The canonical class of `value`, or `None` when the import gave it none.
	pub(crate) fn class(&self, value: ValueId) -> Option<Id> {
		self.values.get(&value).map(|class| self.eg.find(*class))
	}
}

/// The per-graph numbering of write epochs.
///
/// Epochs are numbered in the order an import meets them, so a graph never carries an op id.
#[derive(Clone, Debug, Default)]
pub(crate) struct Renumber {
	stable: HashMap<u32, u32>,
	unique: HashMap<OpId, u32>,
	next_stable: u32,
	next_unique: u32,
}

impl Renumber {
	/// The graph's epoch for `epoch`: equal stable epochs stay equal, and each distinct unique
	/// epoch is a number of its own.
	pub(crate) fn epoch(&mut self, epoch: Option<WriteEpoch>) -> EpochKey {
		match epoch {
			None => EpochKey::None,
			Some(WriteEpoch::Stable(n)) => {
				let next = &mut self.next_stable;
				let k = *self.stable.entry(n).or_insert_with(|| {
					let k = *next;
					*next += 1;
					k
				});
				EpochKey::Stable(k)
			}
			Some(WriteEpoch::Unique(op)) => {
				let next = &mut self.next_unique;
				let k = *self.unique.entry(op).or_insert_with(|| {
					let k = *next;
					*next += 1;
					k
				});
				EpochKey::Unique(k)
			}
		}
	}

	/// A unique epoch no earlier call produced.
	fn fresh_unique(&mut self) -> EpochKey {
		let k = self.next_unique;
		self.next_unique += 1;
		EpochKey::Unique(k)
	}
}

/// Whether a region of `kind` may be encoded as a region node when its owner is an op node.
///
/// The other kinds are always opaque. Every kind is named, so a new kind fails the build here
/// until it is classified.
pub(crate) fn importable(kind: RegionKind) -> bool {
	match kind {
		RegionKind::Clause(_)
		| RegionKind::LazyOperand
		| RegionKind::Loop
		| RegionKind::Subquery
		| RegionKind::ValueIf
		| RegionKind::PermissionFrame => true,
		RegionKind::Request
		| RegionKind::Transaction(_)
		| RegionKind::Statement(_)
		| RegionKind::Block
		| RegionKind::ValueBlock
		| RegionKind::StatementIf
		| RegionKind::StatementFor
		| RegionKind::ValueFor
		| RegionKind::FunctionBody
		| RegionKind::ClosureBody
		| RegionKind::FieldFrame(_)
		| RegionKind::EventFrame
		| RegionKind::SubscriberFrame
		| RegionKind::ErrorScope(_)
		| RegionKind::ChooseArm
		| RegionKind::Pipeline
		| RegionKind::JoinLookupArm
		| RegionKind::WriteStage
		| RegionKind::CatalogUnit
		| RegionKind::Deadline
		| RegionKind::StoredClause(_) => false,
	}
}

/// Whether `op`, a logical op, is an op node when its value key is not opaque.
///
/// Every other logical op is skeleton. Every tag is named, so a new tag fails the build here
/// until it is classified.
fn logical_is_node(op: &LogOp) -> bool {
	match op.tag() {
		LogTag::Loop => {
			if let LogOp::Loop {
				kind,
				..
			} = op
			{
				matches!(
					kind,
					LoopKind::Select
						| LoopKind::ElementFilter
						| LoopKind::GraphStep
						| LoopKind::Broadcast
				)
			} else {
				false
			}
		}
		LogTag::Project => {
			if let LogOp::Project(attrs) = op {
				!attrs.layer
			} else {
				false
			}
		}
		LogTag::Source
		| LogTag::Values
		| LogTag::Subquery
		| LogTag::UnionAll
		| LogTag::Join
		| LogTag::Filter
		| LogTag::PermissionFilter
		| LogTag::Split
		| LogTag::Aggregate
		| LogTag::Distinct
		| LogTag::Sort
		| LogTag::Window
		| LogTag::Limit
		| LogTag::Fetch
		| LogTag::Only => true,
		LogTag::Request
		| LogTag::Txn
		| LogTag::Stmt
		| LogTag::Statement
		| LogTag::Match
		| LogTag::MatchRead
		| LogTag::MatchUpdate
		| LogTag::MatchDelete
		| LogTag::MatchInsert
		| LogTag::MatchOutput
		| LogTag::BindStep
		| LogTag::Write
		| LogTag::Assign
		| LogTag::Unset
		| LogTag::Content
		| LogTag::DdlGuard
		| LogTag::Define
		| LogTag::Remove
		| LogTag::Alter
		| LogTag::Rebuild
		| LogTag::Timeout
		| LogTag::VersionScope
		| LogTag::Explain
		| LogTag::Eval
		| LogTag::Block
		| LogTag::Let
		| LogTag::Return
		| LogTag::If
		| LogTag::Info
		| LogTag::Sleep
		| LogTag::Use
		| LogTag::Option
		| LogTag::Access
		| LogTag::Kill
		| LogTag::Live
		| LogTag::Subscriber
		| LogTag::Show
		| LogTag::Choose
		| LogTag::WriteStage
		| LogTag::Events
		| LogTag::Yield
		| LogTag::Next
		| LogTag::Break => false,
	}
}

/// Whether a set is the answer for an op nothing is known about: every flag.
fn is_unknown(set: EffectSet) -> bool {
	set.contains(EffectSet::TOP)
}

/// Whether the literal `value` is larger than [`EGRAPH_LITERAL_MAX_BYTES`]. The measure is bounded
/// by the limit: it never walks more of `value` than the limit admits.
pub(crate) fn literal_exceeds(value: &Value) -> bool {
	value_exceeds(value, EGRAPH_LITERAL_MAX_BYTES)
}

/// The role `op` plays on its own, before the regions it owns are considered.
fn own_role(ir: &Ir, facts: &mut FactCache, registry: &FunctionRegistry, op: OpId) -> OpRole {
	let Some(data) = ir.op(op) else {
		return OpRole::Skeleton;
	};
	if data.kind.is_terminator() || data.results.is_empty() {
		return OpRole::Skeleton;
	}
	let is_expr = match &data.kind {
		OpKind::Expr(_) => true,
		OpKind::Logical(logical) => {
			if !logical_is_node(logical) {
				return OpRole::Skeleton;
			}
			false
		}
		OpKind::Phys(_) => return OpRole::Skeleton,
	};
	let effects = effects::of_with(ir, facts, &structural_cx(registry, ir), op);
	if data.results.len() > 1 || is_unknown(effects.set) {
		return OpRole::Leaf;
	}
	if is_expr && !ISLAND_EFFECTS.contains(effects.set) {
		return OpRole::Leaf;
	}
	if let OpKind::Expr(ExprOp::Literal(value)) = &data.kind
		&& literal_exceeds(value)
	{
		return OpRole::Leaf;
	}
	if identity::key_with(ir, facts, &structural_cx(registry, ir), op).opaque.is_some() {
		return OpRole::Leaf;
	}
	OpRole::Node
}

/// The values `op` and the ops of its regions read.
fn collect_reads(ir: &Ir, op: OpId, out: &mut Vec<ValueId>) {
	let Some(data) = ir.op(op) else {
		return;
	};
	out.extend(data.operands.iter().copied());
	for region in &data.regions {
		for inner in ops_in(ir, *region) {
			collect_reads(ir, inner, out);
		}
	}
}

/// Whether every op of `body` feeds, directly or through the ops it owns, an operand of
/// `yield_op`.
fn reaches_yield(ir: &Ir, body: &[OpId], yield_op: OpId) -> bool {
	let mut reads: Vec<ValueId> = Vec::new();
	collect_reads(ir, yield_op, &mut reads);
	let mut needed: HashSet<ValueId> = reads.into_iter().collect();
	for op in body.iter().rev() {
		let Some(data) = ir.op(*op) else {
			return false;
		};
		if !data.results.iter().any(|result| needed.contains(result)) {
			return false;
		}
		let mut reads: Vec<ValueId> = Vec::new();
		collect_reads(ir, *op, &mut reads);
		needed.extend(reads);
	}
	true
}

/// The roles of the ops of `region`, when it is encoded as a region node: its kind is importable,
/// it holds one block whose ops are all encodable, each of them feeds the yield, and the block
/// ends in a yield. The yield is the last entry. `None` makes the region an opaque leaf.
fn region_roles(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	region: RegionId,
) -> Option<Vec<(OpId, OpRole)>> {
	let data = ir.region(region)?;
	if !importable(data.kind) || data.blocks.len() != 1 {
		return None;
	}
	let block = ir.block(data.blocks[0])?;
	if block.args.len() > usize::from(u8::MAX) {
		return None;
	}
	let (last, body) = block.ops.split_last()?;
	let closing = ir.op(*last)?;
	if closing.kind.terminator() != Some(Terminator::Yield)
		|| closing.operands.len() > usize::from(u8::MAX)
	{
		return None;
	}
	let mut roles = Vec::with_capacity(body.len() + 1);
	for op in body {
		match own_role(ir, facts, registry, *op) {
			OpRole::Node => roles.push((*op, OpRole::Node)),
			OpRole::Leaf => roles.push((*op, OpRole::Leaf)),
			OpRole::Yield | OpRole::Skeleton => return None,
		}
	}
	if !reaches_yield(ir, body, *last) {
		return None;
	}
	roles.push((*last, OpRole::Yield));
	Some(roles)
}

/// Records `role` for `op`, and the roles of the regions an op node owns.
fn visit(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	op: OpId,
	role: OpRole,
	out: &mut Islands,
) {
	if role == OpRole::Node
		&& let Some(data) = ir.op(op)
	{
		for region in &data.regions {
			match region_roles(ir, facts, registry, *region) {
				Some(roles) => {
					out.regions.insert(*region, RegionRole::Node);
					for (inner, inner_role) in roles {
						visit(ir, facts, registry, inner, inner_role, out);
					}
				}
				None => {
					out.regions.insert(*region, RegionRole::Leaf);
				}
			}
		}
	}
	out.ops.insert(op, role);
	if matches!(role, OpRole::Node | OpRole::Leaf) {
		out.order.push(op);
	}
}

/// Classifies the ops of the statement `stmt`.
///
/// The classification starts at the direct ops of the statement's own regions. A skeleton op's
/// regions are not entered: only the values it reads can be island roots. An op node's regions
/// are entered when they are encodable as region nodes, and stay opaque otherwise.
pub(crate) fn islands(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	stmt: OpId,
) -> Islands {
	let mut out = Islands::default();
	let Some(data) = ir.op(stmt) else {
		return out;
	};
	for region in &data.regions {
		for op in ops_in(ir, *region) {
			let role = own_role(ir, facts, registry, op);
			visit(ir, facts, registry, op, role, &mut out);
		}
	}
	out
}

/// The identity of the leaf `leaf`, which equals no other identity.
fn opaque_identity(leaf: LeafId, frame: FrameId, snapshot: SnapshotId) -> NodeIdentity {
	NodeIdentity {
		shape: NodeShape::Opaque {
			leaf,
		},
		frame,
		snapshot,
		epoch: EpochKey::None,
	}
}

/// The identity of the op `op` of `ir`.
///
/// Built from the op's value key: the tag and the interned attributes, the frame and the
/// snapshot, and the write epoch renumbered by `rn`. An op whose key is opaque is a leaf registered
/// in `an` for its first result. Where the key carries no epoch and the op reads stored data, the
/// epoch is the one the write-epoch analysis assigns.
pub(crate) fn identity(
	ir: &Ir,
	facts: &mut FactCache,
	op: OpId,
	an: &mut IrAnalysis,
	rn: &mut Renumber,
	cx: &EffectCx<'_>,
) -> Result<NodeIdentity, ImportStop> {
	let data = ir.op(op).ok_or(ImportStop::Dangling)?;
	let key = identity::key_with(ir, facts, &structural_cx(cx.registry, ir), op);
	if key.opaque.is_some() {
		let leaf = an.add_leaf(OpaqueLeaf {
			op: Some(op),
			region: None,
			value: data.results.first().copied().unwrap_or_default(),
		});
		return Ok(opaque_identity(leaf, key.frame, key.snapshot));
	}
	let epoch = match key.epoch {
		Some(epoch) => rn.epoch(Some(epoch)),
		None if effects::of_with(ir, facts, &structural_cx(cx.registry, ir), op)
			.set
			.intersects(EffectSet::READS_DATA) =>
		{
			rn.epoch(Some(write_epoch::assign_with(
				ir,
				&mut FactCache::new(),
				&structural_cx(cx.registry, ir),
				op,
			)))
		}
		None => EpochKey::None,
	};
	let payload = an.intern_payload(Arc::new(data.kind.clone()), cx);
	Ok(NodeIdentity {
		shape: NodeShape::Op {
			tag: key.tag,
			payload,
		},
		frame: key.frame,
		snapshot: key.snapshot,
		epoch,
	})
}

/// A footprint that reads every record of every table: what a read of the current row meets when
/// the row's source is not named.
fn row_read() -> IrFootprint {
	IrFootprint {
		reads: smallvec![IrResource::Data {
			table: None,
			fields: FieldSet::All,
			key: KeyOf::Any,
		}],
		writes: SmallVec::new(),
	}
}

/// The frame and snapshot `op` is evaluated in; the container's defaults for an unknown op.
fn frame_of(ir: &Ir, op: OpId) -> (FrameId, SnapshotId) {
	ir.op(op).map_or((FrameId::new(0), SnapshotId::new(0)), |data| (data.frame, data.snapshot))
}

/// The facts of a class that reads a value and does nothing: no effect, no failure, no
/// dependence on stored data.
fn inert_facts(
	an: &IrAnalysis,
	frame: FrameId,
	snapshot: SnapshotId,
	ordinal: u32,
	ty: TypeKnowledge,
) -> ClassFacts {
	ClassFacts {
		effects: EffectSet::PURE,
		fallibility: Fallibility::INFALLIBLE,
		determinism: Determinism::Deterministic,
		frame: Some(frame),
		snapshot: Some(snapshot),
		reads_written: false,
		ty,
		free_binders: SmallVec::new(),
		in_permission_frame: an.is_permission_frame(frame),
		provenance: SmallVec::new(),
		ordinal,
		seed_order: None,
		total_order_ok: false,
		estimate: None,
	}
}

/// The facts the logical analyses answer for `op`, transitively over the ops it owns.
fn op_facts(
	ir: &Ir,
	facts: &mut FactCache,
	an: &IrAnalysis,
	registry: &FunctionRegistry,
	op: OpId,
	ordinal: u32,
	ty: TypeKnowledge,
) -> ClassFacts {
	let (frame, snapshot) = frame_of(ir, op);
	let under = structural_cx(registry, ir);
	let effects = effects::of_with(ir, facts, &under, op);
	let mut out = inert_facts(an, frame, snapshot, ordinal, ty);
	out.reads_written = reads_written(&effects.footprint, None, &an.stmt_writes)
		|| (effects.set.intersects(EffectSet::READS_ROW)
			&& reads_written(&row_read(), None, &an.stmt_writes));
	out.effects = effects.set;
	out.determinism = effects.determinism;
	out.fallibility = fallibility::of_with(ir, facts, &under, op);
	out.provenance = smallvec![op];
	out
}

/// The facts of a region held as a leaf: the inert facts of `frame` and `snapshot`, joined with the
/// effects, fallibility, determinism and written-data reads of every op the region holds, and the
/// values those ops read.
fn region_facts(
	ir: &Ir,
	facts: &mut FactCache,
	an: &IrAnalysis,
	registry: &FunctionRegistry,
	region: RegionId,
	(frame, snapshot): (FrameId, SnapshotId),
	ordinal: u32,
) -> (ClassFacts, Vec<ValueId>) {
	let mut out = inert_facts(an, frame, snapshot, ordinal, TypeKnowledge::Unknown);
	let mut reads: Vec<ValueId> = Vec::new();
	let under = structural_cx(registry, ir);
	for inner in ops_in(ir, region) {
		let held = effects::of_with(ir, facts, &under, inner);
		out.reads_written |= reads_written(&held.footprint, None, &an.stmt_writes)
			|| (held.set.intersects(EffectSet::READS_ROW)
				&& reads_written(&row_read(), None, &an.stmt_writes));
		out.effects = out.effects.union(held.set);
		out.determinism = weaker_determinism(out.determinism, held.determinism);
		out.fallibility = out.fallibility.union(fallibility::of_with(ir, facts, &under, inner));
		collect_reads(ir, inner, &mut reads);
	}
	(out, reads)
}

/// The type knowledge of `value`: the meet of what the container holds and what the analysis
/// derives, both of which hold of the value.
fn type_of(
	ir: &Ir,
	facts: &mut FactCache,
	registry: &FunctionRegistry,
	value: ValueId,
) -> TypeKnowledge {
	match ir.value(value) {
		Some(data) => {
			data.ty.meet(&types::classify_with(ir, facts, &structural_cx(registry, ir), value))
		}
		None => TypeKnowledge::Unknown,
	}
}

/// The binders free in the classes `kids`, sorted and without duplicates.
fn free_binders_of(eg: &EGraph<ENode, IrAnalysis>, kids: &[Id]) -> SmallVec<[BinderId; 2]> {
	let mut out: SmallVec<[BinderId; 2]> = SmallVec::new();
	for kid in kids {
		out.extend(eg[*kid].data.free_binders.iter().copied());
	}
	out.sort_unstable();
	out.dedup();
	out
}

/// The state of one import.
struct Builder<'a> {
	ir: &'a Ir,
	facts: &'a mut FactCache,
	cx: EffectCx<'a>,
	islands: &'a Islands,
	eg: EGraph<ENode, IrAnalysis>,
	values: HashMap<ValueId, Id>,
	binders: HashMap<RegionId, BinderId>,
	renumber: Renumber,
	next_ordinal: u32,
	limits: EgraphLimits,
}

impl Builder<'_> {
	fn tick(&mut self) -> u32 {
		let ordinal = self.next_ordinal;
		self.next_ordinal += 1;
		ordinal
	}

	/// Adds `node` with `facts`, and stops the import once the graph outgrows its limit.
	fn add(&mut self, node: ENode, facts: ClassFacts) -> Result<Id, ImportStop> {
		let class = add_with_facts(&mut self.eg, node, facts);
		if self.eg.total_number_of_nodes() > self.limits.nodes {
			return Err(ImportStop::Limit(Limit::Nodes));
		}
		Ok(class)
	}

	/// Records that the class of `class` stands for `op` as well.
	fn note(&mut self, class: Id, op: OpId) {
		let provenance = &mut self.eg[class].data.provenance;
		if let Err(at) = provenance.binary_search(&op) {
			provenance.insert(at, op);
		}
	}

	/// The binder of the block arguments of `region`.
	fn binder_of(&mut self, region: RegionId) -> Result<BinderId, ImportStop> {
		if let Some(binder) = self.binders.get(&region) {
			return Ok(*binder);
		}
		let data = self.ir.region(region).ok_or(ImportStop::Dangling)?;
		let block = self
			.ir
			.block(*data.blocks.first().ok_or(ImportStop::Dangling)?)
			.ok_or(ImportStop::Dangling)?;
		let args = u8::try_from(block.args.len()).map_err(|_| ImportStop::Dangling)?;
		let binder = self.eg.analysis.add_binder(region, args);
		self.binders.insert(region, binder);
		Ok(binder)
	}

	/// The binders of the imported regions that `values` are block arguments of, sorted and
	/// without duplicates.
	fn binders_read(&mut self, values: &[ValueId]) -> Result<SmallVec<[BinderId; 2]>, ImportStop> {
		let mut out: SmallVec<[BinderId; 2]> = SmallVec::new();
		for value in values {
			let Some(ValueDef::BlockArg {
				block,
				..
			}) = self.ir.value(*value).map(|data| data.def)
			else {
				continue;
			};
			let region = self.ir.block(block).ok_or(ImportStop::Dangling)?.parent;
			if self.islands.regions.get(&region) == Some(&RegionRole::Node) {
				out.push(self.binder_of(region)?);
			}
		}
		out.sort_unstable();
		out.dedup();
		Ok(out)
	}

	/// The class of `value`: the class its defining op was given, the argument node of an imported
	/// region's block argument, or else a leaf for a value defined outside the island.
	fn class_of(&mut self, value: ValueId) -> Result<Id, ImportStop> {
		if let Some(class) = self.values.get(&value) {
			return Ok(*class);
		}
		let ir = self.ir;
		let data = ir.value(value).ok_or(ImportStop::Dangling)?;
		if let ValueDef::BlockArg {
			block,
			index,
		} = data.def
		{
			let region = ir.block(block).ok_or(ImportStop::Dangling)?.parent;
			if self.islands.regions.get(&region) == Some(&RegionRole::Node) {
				return self.arg_class(region, index, value);
			}
		}
		self.outer_leaf(value)
	}

	/// The argument node of block argument `index` of the imported region `region`.
	fn arg_class(
		&mut self,
		region: RegionId,
		index: u16,
		value: ValueId,
	) -> Result<Id, ImportStop> {
		let binder = self.binder_of(region)?;
		let (frame, snapshot) = self
			.ir
			.parent_op(region)
			.map_or((FrameId::new(0), SnapshotId::new(0)), |owner| frame_of(self.ir, owner));
		let index = u8::try_from(index).map_err(|_| ImportStop::Dangling)?;
		let ordinal = self.tick();
		let ty = type_of(self.ir, self.facts, self.cx.registry, value);
		let mut facts = inert_facts(&self.eg.analysis, frame, snapshot, ordinal, ty);
		facts.free_binders.push(binder);
		let node = ENode {
			id: NodeIdentity {
				shape: NodeShape::Arg {
					binder,
					index,
				},
				frame,
				snapshot,
				epoch: EpochKey::None,
			},
			kids: SmallVec::new(),
		};
		let class = self.add(node, facts)?;
		self.values.insert(value, class);
		Ok(class)
	}

	/// The leaf of `value`, which no op node defines.
	fn outer_leaf(&mut self, value: ValueId) -> Result<Id, ImportStop> {
		let ir = self.ir;
		let data = ir.value(value).ok_or(ImportStop::Dangling)?;
		let (def, owner) = match data.def {
			ValueDef::OpResult {
				op,
				..
			} => (Some(op), Some(op)),
			ValueDef::BlockArg {
				block,
				..
			} => (None, ir.block(block).and_then(|block| ir.parent_op(block.parent))),
		};
		let (frame, snapshot) =
			owner.map_or((FrameId::new(0), SnapshotId::new(0)), |op| frame_of(ir, op));
		let leaf = self.eg.analysis.add_leaf(OpaqueLeaf {
			op: def,
			region: None,
			value,
		});
		let ordinal = self.tick();
		let ty = type_of(ir, self.facts, self.cx.registry, value);
		let facts = inert_facts(&self.eg.analysis, frame, snapshot, ordinal, ty);
		let node = ENode {
			id: opaque_identity(leaf, frame, snapshot),
			kids: SmallVec::new(),
		};
		let class = self.add(node, facts)?;
		self.values.insert(value, class);
		Ok(class)
	}

	/// The class of the region `region` owned by the op node `owner`: a region node, or an opaque
	/// leaf when the region stays attached to its owner.
	fn region_class(&mut self, owner: OpId, region: RegionId) -> Result<Id, ImportStop> {
		let ir = self.ir;
		let (frame, snapshot) = frame_of(ir, owner);
		if self.islands.regions.get(&region) != Some(&RegionRole::Node) {
			return self.region_leaf(owner, region);
		}
		let data = ir.region(region).ok_or(ImportStop::Dangling)?;
		let block = ir
			.block(*data.blocks.first().ok_or(ImportStop::Dangling)?)
			.ok_or(ImportStop::Dangling)?;
		let closing = block.ops.last().and_then(|op| ir.op(*op)).ok_or(ImportStop::Dangling)?;
		let binder = if block.args.is_empty() {
			BinderId::NONE
		} else {
			self.binder_of(region)?
		};
		let mut kids: SmallVec<[Id; 4]> = SmallVec::new();
		for value in &closing.operands {
			kids.push(self.class_of(*value)?);
		}
		let results = u8::try_from(kids.len()).map_err(|_| ImportStop::Dangling)?;
		let ordinal = self.tick();
		let mut facts =
			inert_facts(&self.eg.analysis, frame, snapshot, ordinal, TypeKnowledge::Unknown);
		for kid in &kids {
			let kid = &self.eg[*kid].data;
			facts.effects = facts.effects.union(kid.effects);
			facts.fallibility = facts.fallibility.union(kid.fallibility);
			facts.determinism = weaker_determinism(facts.determinism, kid.determinism);
			facts.reads_written |= kid.reads_written;
		}
		facts.free_binders = free_binders_of(&self.eg, &kids);
		facts.free_binders.retain(|held| *held != binder);
		let node = ENode {
			id: NodeIdentity {
				shape: NodeShape::Region {
					kind_key: region_kind_key(data.kind),
					binder,
					results,
				},
				frame,
				snapshot,
				epoch: EpochKey::None,
			},
			kids,
		};
		self.add(node, facts)
	}

	/// The leaf of a region that stays attached to its owner.
	///
	/// A region leaf has no value of its own: its [`OpaqueLeaf::value`] is the null id. Its facts
	/// join those of the ops it holds, and it is free in the binders those ops read.
	fn region_leaf(&mut self, owner: OpId, region: RegionId) -> Result<Id, ImportStop> {
		let ir = self.ir;
		let (frame, snapshot) = frame_of(ir, owner);
		let leaf = self.eg.analysis.add_leaf(OpaqueLeaf {
			op: Some(owner),
			region: Some(region),
			value: ValueId::default(),
		});
		let ordinal = self.tick();
		let (mut facts, reads) = region_facts(
			ir,
			self.facts,
			&self.eg.analysis,
			self.cx.registry,
			region,
			(frame, snapshot),
			ordinal,
		);
		facts.free_binders = self.binders_read(&reads)?;
		let node = ENode {
			id: opaque_identity(leaf, frame, snapshot),
			kids: SmallVec::new(),
		};
		self.add(node, facts)
	}

	/// Encodes the op node `op`: its operands, then its regions, are its kids.
	fn node_op(&mut self, op: OpId) -> Result<(), ImportStop> {
		let ir = self.ir;
		let data = ir.op(op).ok_or(ImportStop::Dangling)?;
		let mut kids: SmallVec<[Id; 4]> =
			SmallVec::with_capacity(data.operands.len() + data.regions.len());
		for value in &data.operands {
			kids.push(self.class_of(*value)?);
		}
		for region in &data.regions {
			kids.push(self.region_class(op, *region)?);
		}
		let ident =
			identity(ir, self.facts, op, &mut self.eg.analysis, &mut self.renumber, &self.cx)?;
		let ordinal = self.tick();
		let ty = match data.results.first() {
			Some(result) => type_of(ir, self.facts, self.cx.registry, *result),
			None => TypeKnowledge::Unknown,
		};
		let mut facts =
			op_facts(ir, self.facts, &self.eg.analysis, self.cx.registry, op, ordinal, ty);
		if matches!(ident.epoch, EpochKey::Unique(_)) {
			facts.reads_written = true;
		}
		facts.free_binders = free_binders_of(&self.eg, &kids);
		let class = self.add(
			ENode {
				id: ident,
				kids,
			},
			facts,
		)?;
		self.note(class, op);
		if let Some(result) = data.results.first() {
			self.values.insert(*result, class);
		}
		self.eg.analysis.incumbents.insert(op, class);
		self.eg.analysis.ordinals.insert(op, ordinal);
		Ok(())
	}

	/// Encodes the op leaf `op`: one leaf per result. The op stays in place, so each leaf is free
	/// in the binders the op and the regions it owns read.
	fn leaf_op(&mut self, op: OpId) -> Result<(), ImportStop> {
		let ir = self.ir;
		let data = ir.op(op).ok_or(ImportStop::Dangling)?;
		let mut reads: Vec<ValueId> = Vec::new();
		collect_reads(ir, op, &mut reads);
		let free = self.binders_read(&reads)?;
		let (frame, snapshot) = frame_of(ir, op);
		for result in &data.results {
			let leaf = self.eg.analysis.add_leaf(OpaqueLeaf {
				op: Some(op),
				region: None,
				value: *result,
			});
			let ordinal = self.tick();
			let ty = type_of(ir, self.facts, self.cx.registry, *result);
			let mut facts =
				op_facts(ir, self.facts, &self.eg.analysis, self.cx.registry, op, ordinal, ty);
			facts.free_binders.clone_from(&free);
			let class = self.add(
				ENode {
					id: opaque_identity(leaf, frame, snapshot),
					kids: SmallVec::new(),
				},
				facts,
			)?;
			self.values.insert(*result, class);
		}
		Ok(())
	}

	/// The island values an op outside the island reads, in program order.
	fn roots(&self) -> Vec<IslandRoot> {
		let mut out = Vec::new();
		for op in &self.islands.order {
			if self.islands.ops.get(op) != Some(&OpRole::Node) {
				continue;
			}
			let Some(value) = self.ir.op(*op).and_then(|data| data.results.first().copied()) else {
				continue;
			};
			let Some(class) = self.values.get(&value) else {
				continue;
			};
			let uses: SmallVec<[(OpId, u16); 2]> = self
				.ir
				.uses(value)
				.iter()
				.filter(|used| {
					!matches!(
						self.islands.ops.get(&used.user),
						Some(OpRole::Node) | Some(OpRole::Yield)
					)
				})
				.map(|used| (used.user, used.operand))
				.collect();
			if !uses.is_empty() {
				out.push(IslandRoot {
					value,
					class: *class,
					uses,
				});
			}
		}
		out
	}
}

/// Imports the statement `stmt` into a fresh e-graph.
///
/// The graph holds one class per value of the statement's islands, a leaf for every value an
/// island reads and cannot rewrite, and the tables [`IrAnalysis`] documents: the incumbent class
/// and the import order of each op node, and what the statement writes. The import stops with
/// [`Limit::Nodes`] when the graph would hold more than `limits.nodes` nodes.
pub(crate) fn import(
	ir: &Ir,
	facts: &mut FactCache,
	stmt: OpId,
	cx: &EffectCx<'_>,
	limits: EgraphLimits,
) -> Result<Imported, ImportStop> {
	let classified = islands(ir, facts, cx.registry, stmt);
	let mut an = IrAnalysis::new();
	an.frames = ir
		.frames()
		.frames
		.iter()
		.map(|frame| matches!(frame.eval.mode, EvalMode::Permission | EvalMode::MutablePermission))
		.collect();
	an.stmt_writes = IrFootprint {
		reads: SmallVec::new(),
		writes: footprint::of_with(ir, facts, &structural_cx(cx.registry, ir), stmt).writes,
	};
	let mut builder = Builder {
		ir,
		facts,
		cx: *cx,
		islands: &classified,
		eg: EGraph::new(an),
		values: HashMap::new(),
		binders: HashMap::new(),
		renumber: Renumber::default(),
		next_ordinal: 0,
		limits,
	};
	for op in &classified.order {
		match classified.ops.get(op) {
			Some(OpRole::Node) => builder.node_op(*op)?,
			Some(OpRole::Leaf) => builder.leaf_op(*op)?,
			Some(OpRole::Yield) | Some(OpRole::Skeleton) | None => {}
		}
	}
	let roots = builder.roots();
	Ok(Imported {
		eg: builder.eg,
		values: builder.values,
		roots,
		limits,
		renumber: builder.renumber,
		next_ordinal: builder.next_ordinal,
	})
}

/// The longest chain of created ops a seed's provenance is followed through to an original op.
const MAX_PROVENANCE_DEPTH: u8 = 8;

/// A statement copy the conversion ran on, and what the conversion did to it.
pub(crate) struct SeedCopy<'a> {
	/// The converted copy.
	pub(crate) ir: &'a Ir,
	/// The ids of the copy and the statement it copies. A copy keeps the frame and snapshot tables
	/// of its original, so a copied op's frame and snapshot ids name the same entries in both.
	pub(crate) map: &'a StmtMap,
	/// The class each op the candidate's elaboration created is a node of, by the op's result
	/// value. Such a value has no counterpart in `map`; it stands for the class its node was built
	/// for.
	pub(crate) created: &'a HashMap<ValueId, ClassRef>,
	/// The conversion's replacement log: each `(from, to)` is a value of the copy the conversion
	/// replaced and the value that stands for it in the copy.
	pub(crate) log: &'a [(ValueId, ValueId)],
	/// The candidate the copy is.
	pub(crate) candidate: u8,
}

/// The state of one seed import.
struct SeedImport<'a> {
	imp: &'a mut Imported,
	copy: &'a SeedCopy<'a>,
	facts: &'a mut FactCache,
	host: &'a dyn SeedHost,
	cx: EffectCx<'a>,
	/// The class of every copy value met so far.
	classes: HashMap<ValueId, Id>,
	/// The delivery order of every physical value of the copy met so far.
	orders: HashMap<ValueId, OrderSig>,
}

impl SeedImport<'_> {
	/// Joins the class of the island value `from` to the class of the physical value `to`.
	fn replace(&mut self, from: ValueId, to: ValueId) -> Result<(), ImportStop> {
		let copy = self.copy;
		let from_class = match copy.map.values_to_orig.get(&from) {
			Some(original) => self.imp.values.get(original).copied(),
			None => copy.created.get(&from).map(|class| Id::from(*class)),
		};
		let Some(from_class) = from_class else {
			return Err(ImportStop::Seed("a replaced value has no class in the graph"));
		};
		let defined_by_phys = match copy.ir.value(to).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) => matches!(copy.ir.op(op).map(|data| &data.kind), Some(OpKind::Phys(_))),
			Some(ValueDef::BlockArg {
				..
			})
			| None => false,
		};
		if !defined_by_phys {
			return Err(ImportStop::Seed("a replacement is not a physical value"));
		}
		let to_class = self.class_of(to)?;
		let refused = self.imp.eg.analysis.union_refused;
		guarded_union(
			&mut self.imp.eg,
			from_class,
			to_class,
			UnionWhy::Seed {
				from_known: true,
				to_phys_of_same_copy: true,
			},
		);
		if self.imp.eg.analysis.union_refused != refused {
			return Err(ImportStop::Seed("a seed union was refused"));
		}
		Ok(())
	}

	fn tick(&mut self) -> u32 {
		let ordinal = self.imp.next_ordinal;
		self.imp.next_ordinal += 1;
		ordinal
	}

	fn check(&self) -> Result<(), ImportStop> {
		if self.imp.eg.total_number_of_nodes() > self.imp.limits.nodes {
			return Err(ImportStop::Limit(Limit::Nodes));
		}
		Ok(())
	}

	/// The class of the copy value `value`.
	///
	/// A value the conversion neither created nor replaced is the incumbent's class for the value
	/// it copies, and a value the candidate's elaboration created is the class its node was built
	/// for. A physical op the conversion created is a node whose kids are classes of its operands.
	/// Anything else the conversion created is an opaque leaf.
	fn class_of(&mut self, value: ValueId) -> Result<Id, ImportStop> {
		if let Some(class) = self.classes.get(&value) {
			return Ok(*class);
		}
		let copy = self.copy;
		let data = copy.ir.value(value).ok_or(ImportStop::Dangling)?;
		let class = if let Some(original) = copy.map.values_to_orig.get(&value) {
			match self.imp.values.get(original).copied() {
				Some(class) => class,
				None => {
					let class = self.leaf(value, Some(*original))?;
					self.imp.values.insert(*original, class);
					class
				}
			}
		} else if let Some(built) = copy.created.get(&value) {
			Id::from(*built)
		} else {
			match data.def {
				ValueDef::OpResult {
					op,
					..
				} => {
					let phys = matches!(copy.ir.op(op).map(|d| &d.kind), Some(OpKind::Phys(_)));
					let single = copy.ir.op(op).is_some_and(|d| d.results.len() == 1);
					if phys && single {
						self.seed_node(op, value)?
					} else {
						self.leaf(value, None)?
					}
				}
				ValueDef::BlockArg {
					block,
					index,
				} => self.arg(block, index, value)?,
			}
		};
		self.classes.insert(value, class);
		Ok(class)
	}

	/// The argument node of a copy block argument whose region copies an imported region, or else
	/// a leaf.
	fn arg(
		&mut self,
		block: crate::exec::ir::iface::ids::BlockId,
		index: u16,
		value: ValueId,
	) -> Result<Id, ImportStop> {
		let copy = self.copy;
		let region = copy.ir.block(block).ok_or(ImportStop::Dangling)?.parent;
		let binder = copy.map.regions_to_orig.get(&region).and_then(|original| {
			self.imp
				.eg
				.analysis
				.binders
				.iter()
				.position(|entry| entry.origin == *original)
				.map(|at| BinderId(at as u32))
		});
		let (Some(binder), Ok(index)) = (binder, u8::try_from(index)) else {
			return self.leaf(value, None);
		};
		let (frame, snapshot) = copy
			.ir
			.parent_op(region)
			.map_or((FrameId::new(0), SnapshotId::new(0)), |owner| frame_of(copy.ir, owner));
		let ordinal = self.tick();
		let ty = type_of(copy.ir, self.facts, self.cx.registry, value);
		let mut facts = inert_facts(&self.imp.eg.analysis, frame, snapshot, ordinal, ty);
		facts.free_binders.push(binder);
		let class = add_with_facts(
			&mut self.imp.eg,
			ENode {
				id: NodeIdentity {
					shape: NodeShape::Arg {
						binder,
						index,
					},
					frame,
					snapshot,
					epoch: EpochKey::None,
				},
				kids: SmallVec::new(),
			},
			facts,
		);
		self.check()?;
		Ok(class)
	}

	/// A leaf for the copy value `value`, naming `original` when it copies an original value.
	fn leaf(&mut self, value: ValueId, original: Option<ValueId>) -> Result<Id, ImportStop> {
		let copy = self.copy;
		let data = copy.ir.value(value).ok_or(ImportStop::Dangling)?;
		let def = match data.def {
			ValueDef::OpResult {
				op,
				..
			} => Some(op),
			ValueDef::BlockArg {
				..
			} => None,
		};
		let (frame, snapshot) = match def {
			Some(op) => frame_of(copy.ir, op),
			None => (FrameId::new(0), SnapshotId::new(0)),
		};
		let leaf = self.imp.eg.analysis.add_leaf(OpaqueLeaf {
			op: None,
			region: None,
			value: original.unwrap_or(value),
		});
		let ordinal = self.tick();
		let ty = type_of(copy.ir, self.facts, self.cx.registry, value);
		let facts = match def {
			Some(op) => op_facts(
				copy.ir,
				self.facts,
				&self.imp.eg.analysis,
				self.cx.registry,
				op,
				ordinal,
				ty,
			),
			None => inert_facts(&self.imp.eg.analysis, frame, snapshot, ordinal, ty),
		};
		let class = add_with_facts(
			&mut self.imp.eg,
			ENode {
				id: opaque_identity(leaf, frame, snapshot),
				kids: SmallVec::new(),
			},
			facts,
		);
		self.check()?;
		Ok(class)
	}

	/// The original ops the copy op `op` stands for, sorted and without duplicates.
	///
	/// An op the elaboration created has no original of its own; it stands for the originals of the
	/// ops its provenance names.
	fn original_provenance(&self, op: OpId) -> SmallVec<[OpId; 2]> {
		let mut out: SmallVec<[OpId; 2]> = SmallVec::new();
		self.collect_originals(op, 0, &mut out);
		out.sort_unstable();
		out.dedup();
		out
	}

	fn collect_originals(&self, op: OpId, depth: u8, out: &mut SmallVec<[OpId; 2]>) {
		let Some(data) = self.copy.ir.op(op) else {
			return;
		};
		let copies: SmallVec<[OpId; 2]> = match &data.provenance {
			Provenance::Bound => smallvec![op],
			Provenance::From(ids) => ids.clone(),
		};
		for id in copies {
			if let Some(original) = self.copy.map.ops_to_orig.get(&id) {
				out.push(*original);
			} else if id != op && depth < MAX_PROVENANCE_DEPTH {
				self.collect_originals(id, depth + 1, out);
			}
		}
	}

	/// The epoch of the physical op `op`: the epoch of the incumbent read it stands for, else a
	/// fresh unique epoch when it reads stored data, else none.
	fn epoch_of(&mut self, op: OpId, reads_data: bool) -> EpochKey {
		for original in self.original_provenance(op) {
			let Some(class) = self.imp.eg.analysis.incumbents.get(&original) else {
				continue;
			};
			let epoch = self.imp.eg[*class]
				.nodes
				.iter()
				.filter_map(|node| match node.id.shape {
					NodeShape::Op {
						..
					} => Some(node.id.epoch),
					NodeShape::Region {
						..
					}
					| NodeShape::Arg {
						..
					}
					| NodeShape::Opaque {
						..
					} => None,
				})
				.filter(|epoch| *epoch != EpochKey::None)
				.min();
			if let Some(epoch) = epoch {
				return epoch;
			}
		}
		if reads_data {
			self.imp.renumber.fresh_unique()
		} else {
			EpochKey::None
		}
	}

	/// Encodes the physical op `op`, whose one result is `value`, as a seed of the candidate.
	///
	/// Each region of the op is a leaf that carries the effects, fallibility, determinism and
	/// written-data reads of the ops it holds, so a guard sees what the region does. Its ops are
	/// not nodes, so an implementation rule cannot match inside it.
	fn seed_node(&mut self, op: OpId, value: ValueId) -> Result<Id, ImportStop> {
		let copy = self.copy;
		let data = copy.ir.op(op).ok_or(ImportStop::Dangling)?;
		let OpKind::Phys(phys) = &data.kind else {
			return Err(ImportStop::Dangling);
		};
		let mut kids: SmallVec<[Id; 4]> = SmallVec::new();
		let mut input = OrderSig::Unordered;
		let mut input_known = false;
		for operand in &data.operands {
			kids.push(self.class_of(*operand)?);
			if !input_known && let Some(order) = self.orders.get(operand) {
				input = order.clone();
				input_known = true;
			}
		}
		for region in &data.regions {
			let leaf = self.imp.eg.analysis.add_leaf(OpaqueLeaf {
				op: Some(op),
				region: Some(*region),
				value: ValueId::default(),
			});
			let (frame, snapshot) = frame_of(copy.ir, op);
			let ordinal = self.tick();
			let (facts, _) = region_facts(
				copy.ir,
				self.facts,
				&self.imp.eg.analysis,
				self.cx.registry,
				*region,
				(frame, snapshot),
				ordinal,
			);
			let class = add_with_facts(
				&mut self.imp.eg,
				ENode {
					id: opaque_identity(leaf, frame, snapshot),
					kids: SmallVec::new(),
				},
				facts,
			);
			self.check()?;
			kids.push(class);
		}
		let payload = self.imp.eg.analysis.intern_payload(Arc::new(data.kind.clone()), &self.cx);
		let reads_data = self
			.imp
			.eg
			.analysis
			.payload(payload)
			.is_some_and(|entry| entry.local.set.intersects(EffectSet::READS_DATA));
		let epoch = self.epoch_of(op, reads_data);
		let node = ENode {
			id: NodeIdentity {
				shape: NodeShape::Op {
					tag: OpTag::Phys(phys.tag()),
					payload,
				},
				frame: data.frame,
				snapshot: data.snapshot,
				epoch,
			},
			kids,
		};
		let ordinal = self.tick();
		self.imp.eg.analysis.creating_for = Some(ordinal);
		let class = self.imp.eg.add(node.clone());
		self.imp.eg.analysis.creating_for = None;
		self.check()?;

		let provenance = self.original_provenance(op);
		let order = self.host.order_sig(phys, input);
		let candidate = copy.candidate;
		{
			let facts = &mut self.imp.eg[class].data;
			for original in provenance {
				if let Err(at) = facts.provenance.binary_search(&original) {
					facts.provenance.insert(at, original);
				}
			}
			if candidate == 0 && facts.seed_order.is_none() {
				facts.seed_order = Some(order.clone());
			}
		}
		let mut canonical = node;
		for kid in canonical.kids.iter_mut() {
			*kid = self.imp.eg.find(*kid);
		}
		let infos = self.imp.eg.analysis.seeds.entry(canonical).or_default();
		if !infos.iter().any(|held| held.candidate == candidate && held.op == op) {
			infos.push(SeedInfo {
				candidate,
				op,
				order: order.clone(),
			});
			infos.sort_by_key(|info| (info.candidate, info.op));
		}
		self.orders.insert(value, order);
		Ok(class)
	}
}

/// Adds the physical ops of one converted statement copy to the graph as seeds of its candidate,
/// and joins each replaced island value to the physical value that replaced it.
///
/// A copy value the conversion neither created nor replaced is the incumbent's class, so a seed's
/// kids are the incumbent's classes wherever they can be. A union of a replaced value with a value
/// that is not a physical value of the copy, or one a guard refuses, stops the import with
/// [`ImportStop::Seed`]: the candidate is dropped. Each seed records the delivery order
/// `host.order_sig` answers, and a candidate 0 seed sets the class's `seed_order`.
pub(crate) fn import_seed(
	imp: &mut Imported,
	copy: &SeedCopy<'_>,
	facts: &mut FactCache,
	host: &dyn SeedHost,
	cx: &EffectCx<'_>,
) -> Result<(), ImportStop> {
	{
		let mut seeding = SeedImport {
			imp: &mut *imp,
			copy,
			facts,
			host,
			cx: *cx,
			classes: HashMap::new(),
			orders: HashMap::new(),
		};
		for (from, to) in copy.log {
			seeding.replace(*from, *to)?;
		}
	}
	imp.eg.rebuild();
	recanonicalise_seeds(&mut imp.eg);
	Ok(())
}
