//! Write-back: building the extracted term into a statement and adopting it into the container.
//!
//! Elaboration is two steps. [`plan`] reads the e-graph, the chosen term and the target statement
//! and answers an [`ElabPlan`]; it mutates nothing and fails with an [`Abort`] naming the rule it
//! could not honour. [`apply`] runs a plan through one [`Rewriter`] over the target. A write-back
//! therefore either holds a complete plan or leaves the container as it was.
//!
//! The target holds one op per imported (or seeded) node, its anchor. An anchor whose node, and
//! the nodes of its regions, are the ones the term chose is kept as it stands. An anchor whose
//! class the term chose another node for is replaced: a new subtree is built immediately before it,
//! and every use of its value moves to the subtree's root. A kept op that reads a replaced value
//! therefore reads the new one without being touched, whatever its depth below an island root.
//! Ops with no remaining uses are erased, users first, each with the reason the plan fixed for it.
//!
//! The plan holds no sharing the program did not have and drops none it had: a replaced anchor
//! gets one new subtree for all of its uses, and two anchors of one class get one each. A leaf, an
//! op the e-graph cannot rewrite, is never copied: the term may read its value, and the plan aborts
//! when one tree reads the same leaf from two new ops or moves one region twice.
//!
//! [`write_logical`] and [`write_seeded`] drive plan and apply over statement copies the container
//! offers through [`StatementSurgery`], and adopt the patched copy only when it is complete. A
//! failure before the first adopt leaves the container untouched.

use std::collections::{HashMap, HashSet};
use std::fmt;

use egg::{EGraph, Id};
use smallvec::SmallVec;

use super::FallbackReason;
use super::analysis::IrAnalysis;
use super::decl::{BinderId, ClassRef, ENodeSnapshot, LeafId, NodeShape, PayloadId, StmtMap};
use super::extract::{Candidate, PhysChoice, Term};
use super::guards::{deterministic, infallible};
use super::lang::{ENode, region_kind_key, snapshot_of};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::tokens;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::verify::{VerifyAt, verify};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::{EliminationReason, LOGICAL_PHASE, Phase};
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::{OpKind, OpTag, Terminator};

/// The deepest term a plan builds. A deeper term aborts: the planner recurses once per level.
const MAX_DEPTH: usize = 256;

// ---------------------------------------------------------------------------------------------
// The container's statement copies
// ---------------------------------------------------------------------------------------------

/// What the write-back needs of a container: a deep copy of one statement, adoption of a patched
/// copy in place of the statement, and the legality snapshot of the statement retaken afterwards.
///
/// An implementation answers `Err` without having changed `ir`, `facts` or its legality snapshot,
/// so a failed call is a no-op. Adopting a copy back into the container it was copied from does
/// not fail. A container that offers none of the three answers an error, and the caller then
/// plans the statement unrewritten.
pub(crate) trait StatementSurgery: Send + Sync {
	/// A copy of the statement `stmt` as a container of its own, and the ids of the copy against
	/// the ids of the statement.
	fn copy_statement(&self, ir: &Ir, stmt: OpId) -> Result<(Ir, StmtMap), IrError>;

	/// Replaces the contents of `stmt` with the statement of `from`, a patched copy `map`
	/// describes. The op `stmt` keeps its id, and `facts` drops what it cached of the replaced
	/// code.
	fn adopt_statement(
		&self,
		ir: &mut Ir,
		facts: &mut FactCache,
		stmt: OpId,
		from: Ir,
		map: &StmtMap,
	) -> Result<(), IrError>;

	/// Recomputes the legality snapshot of `stmt` over its current ops, judging calls against
	/// `registry`.
	fn retake_legality(
		&self,
		ir: &Ir,
		facts: &mut FactCache,
		registry: &FunctionRegistry,
		stmt: OpId,
	) -> Result<(), IrError>;
}

/// The surgery of a container that offers none: every call answers [`IrError::Invariant`].
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoSurgery;

impl StatementSurgery for NoSurgery {
	fn copy_statement(&self, _ir: &Ir, _stmt: OpId) -> Result<(Ir, StmtMap), IrError> {
		Err(IrError::Invariant {
			what: "a container with no statement surgery cannot copy",
		})
	}

	fn adopt_statement(
		&self,
		_ir: &mut Ir,
		_facts: &mut FactCache,
		_stmt: OpId,
		_from: Ir,
		_map: &StmtMap,
	) -> Result<(), IrError> {
		Err(IrError::Invariant {
			what: "a container with no statement surgery cannot adopt",
		})
	}

	fn retake_legality(
		&self,
		_ir: &Ir,
		_facts: &mut FactCache,
		_registry: &FunctionRegistry,
		_stmt: OpId,
	) -> Result<(), IrError> {
		Err(IrError::Invariant {
			what: "a container with no statement surgery cannot retake legality",
		})
	}
}

// ---------------------------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------------------------

/// The elaboration rule a plan could not honour.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ElabRule {
	/// Reuse: an incumbent or seeded node maps to an op of the target.
	E1,
	/// Trees: one new subtree per replaced op, and no copy of a region.
	E2,
	/// Placement: a new op stands before its use and never crosses a region boundary.
	E3,
	/// Regions: a region node is built with its binder in scope.
	E4,
	/// Opaque leaves: never copied, never read twice by one tree.
	E5,
	/// Provenance: every new op names the ops it stands for in the target's ids.
	E6,
	/// Eliminated ops: each erased op has a justified reason.
	E8,
}

/// A plan that cannot be built, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Abort {
	pub(crate) rule: ElabRule,
	pub(crate) why: &'static str,
}

impl Abort {
	fn new(rule: ElabRule, why: &'static str) -> Self {
		Self {
			rule,
			why,
		}
	}
}

impl fmt::Display for Abort {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:?}: {}", self.rule, self.why)
	}
}

/// What a plan is built over besides the term.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Basis<'a> {
	/// The candidate whose converted copy is the target, or `None` when the target is a copy of
	/// the logical statement.
	pub(crate) seed: Option<u8>,
	/// The classes a rule listed in `answers`: an access path answers what they computed. A class
	/// outside the list is never eliminated as answered.
	pub(crate) answered: &'a [ClassRef],
}

impl Basis<'static> {
	/// The target is a copy of the logical statement, and no rule answered a class.
	pub(crate) const LOGICAL: Basis<'static> = Basis {
		seed: None,
		answered: &[],
	};
}

/// A step of the index of ops a plan creates. Operands name a step by its position.
pub(crate) type Slot = u32;

/// A value a new op reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operand {
	/// A value the target holds.
	Value(ValueId),
	/// The result of the op step at this slot.
	Result(Slot),
	/// Block argument `index` of the region step at this slot.
	Arg(Slot, u16),
}

/// Where a new op goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
	/// Immediately before an op of the target.
	Before(OpId),
	/// At the end of the block of the region step at this slot.
	End(Slot),
}

/// An op to create, with the logical ops it stands for.
#[derive(Clone, Debug)]
pub(crate) struct NewOp {
	pub(crate) kind: OpKind,
	pub(crate) operands: SmallVec<[Operand; 4]>,
	/// How many results the op has: one for a node, none for a closing yield.
	pub(crate) results: u8,
	pub(crate) ty: TypeKnowledge,
	pub(crate) frame: FrameId,
	pub(crate) snapshot: SnapshotId,
	pub(crate) loc: Location,
	/// The ops of the target the new op stands for, sorted and without duplicates.
	pub(crate) provenance: SmallVec<[OpId; 2]>,
	pub(crate) place: Place,
}

/// One mutation that creates something. Its position in the plan is its [`Slot`].
#[derive(Clone, Debug)]
pub(crate) enum Step {
	Op(NewOp),
	/// A region of `kind` owned by the op step at `owner`, with one block whose arguments are typed
	/// by `args`.
	Region {
		owner: Slot,
		kind: RegionKind,
		args: SmallVec<[TypeKnowledge; 2]>,
	},
	/// An existing region becomes the next region of the op step at `owner`.
	Move {
		region: RegionId,
		owner: Slot,
	},
}

/// Every use of `from` reads `to` instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Swap {
	pub(crate) from: ValueId,
	pub(crate) to: Operand,
}

/// Why an op leaves the container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Elimination {
	Reason(EliminationReason),
	/// The op's value is the result of the op step at this slot.
	MergedInto(Slot),
}

/// An op to erase once none of its results is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Erase {
	pub(crate) op: OpId,
	pub(crate) reason: Elimination,
}

/// The mutations that build a chosen term into a target statement.
#[derive(Clone, Debug, Default)]
pub(crate) struct ElabPlan {
	/// What to create, in order; an operand names an earlier step.
	pub(crate) steps: Vec<Step>,
	/// The uses to move onto new values, after every step ran.
	pub(crate) swaps: Vec<Swap>,
	/// The ops to erase after the swaps, users before the ops they read.
	pub(crate) erase: Vec<Erase>,
	/// The breakers (`sort`, `limit`, `distinct`, `aggregate`) an access path answered. The list
	/// is not read by the write-back; the legality of the statement is retaken over its final ops.
	pub(crate) fuse: Vec<OpId>,
	/// The class each op step built a node of, by the step's slot. A closing yield stands for no
	/// class and is absent.
	pub(crate) built: Vec<(Slot, ClassRef)>,
}

impl ElabPlan {
	/// Whether the plan changes nothing.
	pub(crate) fn is_empty(&self) -> bool {
		self.steps.is_empty() && self.swaps.is_empty() && self.erase.is_empty()
	}
}

// ---------------------------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------------------------

/// The canonical snapshot of `node`.
fn canonical(eg: &EGraph<ENode, IrAnalysis>, node: &ENode) -> ENodeSnapshot {
	let mut node = node.clone();
	for kid in node.kids.iter_mut() {
		*kid = eg.find(*kid);
	}
	snapshot_of(&node)
}

/// The first result of `op`.
fn result_of(ir: &Ir, op: OpId) -> Option<ValueId> {
	ir.op(op).and_then(|data| data.results.first().copied())
}

/// The position of `op` in its block.
fn position(ir: &Ir, block: BlockId, op: OpId) -> Option<usize> {
	ir.block(block).and_then(|data| data.ops.iter().position(|held| *held == op))
}

/// Whether the result of `def` is defined before `anchor` runs: `def` stands earlier in the block
/// of `anchor`, or earlier in the block of an op that encloses `anchor`.
fn dominates(ir: &Ir, def: OpId, anchor: OpId) -> bool {
	let Some(block) = ir.op(def).map(|data| data.parent) else {
		return false;
	};
	let mut at = anchor;
	loop {
		let Some(data) = ir.op(at) else {
			return false;
		};
		if data.parent == block {
			return match (position(ir, block, def), position(ir, block, at)) {
				(Some(def), Some(at)) => def < at,
				_ => false,
			};
		}
		let Some(owner) = ir.block(data.parent).and_then(|b| ir.parent_op(b.parent)) else {
			return false;
		};
		at = owner;
	}
}

/// Whether `anchor` stands in `block` or in a block of a region beneath it.
fn within_block(ir: &Ir, anchor: OpId, block: BlockId) -> bool {
	let mut at = anchor;
	loop {
		let Some(data) = ir.op(at) else {
			return false;
		};
		if data.parent == block {
			return true;
		}
		let Some(owner) = ir.block(data.parent).and_then(|b| ir.parent_op(b.parent)) else {
			return false;
		};
		at = owner;
	}
}

/// Whether the value `value` can be read by a new op placed before `anchor`.
fn visible(ir: &Ir, value: ValueId, anchor: OpId) -> bool {
	match ir.value(value).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => dominates(ir, op, anchor),
		Some(ValueDef::BlockArg {
			block,
			..
		}) => within_block(ir, anchor, block),
		None => false,
	}
}

/// An op of the target the e-graph holds a node for.
#[derive(Clone, Debug)]
struct Anchor {
	/// The canonical node the op was imported or seeded as.
	node: ENodeSnapshot,
	class: ClassRef,
	/// The class and canonical node of each region of the op, in position order.
	regions: SmallVec<[(ClassRef, ENodeSnapshot); 1]>,
}

/// A region of the target that a region class stands for.
#[derive(Clone, Debug)]
struct RegionHome {
	owner: OpId,
	region: RegionId,
	node: ENodeSnapshot,
}

/// Where the next new op goes, and the op whose position bounds what it may read.
#[derive(Clone, Copy, Debug)]
struct Site {
	anchor: OpId,
	place: Place,
}

struct Planner<'a> {
	eg: &'a EGraph<ENode, IrAnalysis>,
	term: &'a Term,
	map: &'a StmtMap,
	ir: &'a Ir,
	basis: &'a Basis<'a>,
	anchors: HashMap<OpId, Anchor>,
	/// The anchors in program order.
	order: Vec<OpId>,
	by_class: HashMap<ClassRef, SmallVec<[OpId; 2]>>,
	homes: HashMap<ClassRef, SmallVec<[RegionHome; 1]>>,
	kept: HashSet<OpId>,
	/// The classes of the regions some chosen node owns.
	region_classes: HashSet<ClassRef>,
	/// The op a region of each kind closes with.
	yields: HashMap<u16, OpKind>,
	steps: Vec<Step>,
	/// The class each op step built a node of.
	built: Vec<(Slot, ClassRef)>,
	swaps: Vec<Swap>,
	swapped: HashMap<OpId, usize>,
	/// Binders of the regions the plan is building, innermost last.
	scope: Vec<(BinderId, Slot)>,
	/// The leaves the tree under construction reads from a new op.
	tree_leaves: HashSet<LeafId>,
	moved: HashSet<RegionId>,
	max_steps: usize,
}

impl<'a> Planner<'a> {
	fn new(
		eg: &'a EGraph<ENode, IrAnalysis>,
		term: &'a Term,
		map: &'a StmtMap,
		ir: &'a Ir,
		basis: &'a Basis<'a>,
	) -> Self {
		Self {
			eg,
			term,
			map,
			ir,
			basis,
			anchors: HashMap::new(),
			order: Vec::new(),
			by_class: HashMap::new(),
			homes: HashMap::new(),
			kept: HashSet::new(),
			region_classes: HashSet::new(),
			yields: HashMap::new(),
			steps: Vec::new(),
			built: Vec::new(),
			swaps: Vec::new(),
			swapped: HashMap::new(),
			scope: Vec::new(),
			tree_leaves: HashSet::new(),
			moved: HashSet::new(),
			max_steps: eg_size(eg).saturating_mul(2),
		}
	}

	/// Records the op `target` as the anchor of the node `raw`.
	fn anchor(
		&mut self,
		target: OpId,
		raw: &ENode,
		canonical_node: ENodeSnapshot,
		class: Id,
	) -> Result<(), Abort> {
		let data =
			self.ir.op(target).ok_or(Abort::new(ElabRule::E1, "an anchor is not in the target"))?;
		let operands = data.operands.len();
		if raw.kids.len() != operands + data.regions.len() {
			return Err(Abort::new(
				ElabRule::E1,
				"an anchor's kids do not match its operands and regions",
			));
		}
		let class = ClassRef::from(class);
		let mut regions: SmallVec<[(ClassRef, ENodeSnapshot); 1]> = SmallVec::new();
		for (at, kid) in raw.kids[operands..].iter().enumerate() {
			let region_class = ClassRef::from(self.eg.find(*kid));
			let node = canonical(self.eg, self.eg.id_to_node(*kid));
			let region = data.regions[at];
			self.homes.entry(region_class).or_default().push(RegionHome {
				owner: target,
				region,
				node: node.clone(),
			});
			regions.push((region_class, node));
		}
		self.anchors.insert(
			target,
			Anchor {
				node: canonical_node,
				class,
				regions,
			},
		);
		Ok(())
	}

	/// Finds the anchors, their program order, the regions chosen nodes own and the closing op of
	/// each region kind.
	fn index(&mut self) -> Result<(), Abort> {
		let eg = self.eg;
		let an = &eg.analysis;
		let mut imported: Vec<(u32, OpId, Id)> = an
			.incumbents
			.iter()
			.map(|(op, id)| (an.ordinals.get(op).copied().unwrap_or(u32::MAX), *op, *id))
			.collect();
		imported.sort_unstable();
		for (_, original, id) in imported {
			let target = *self
				.map
				.ops_to_copy
				.get(&original)
				.ok_or(Abort::new(ElabRule::E1, "an imported op has no copy"))?;
			if self.basis.seed.is_some() && self.ir.op(target).is_none() {
				continue;
			}
			let raw = eg.id_to_node(id).clone();
			let node = canonical(eg, &raw);
			self.anchor(target, &raw, node, eg.find(id))?;
		}
		if let Some(candidate) = self.basis.seed {
			let mut seeded: Vec<(OpId, &ENode)> = Vec::new();
			for (node, infos) in &an.seeds {
				for info in infos.iter().filter(|info| info.candidate == candidate) {
					seeded.push((info.op, node));
				}
			}
			seeded.sort_unstable_by_key(|a| a.0);
			for (target, node) in seeded {
				if self.ir.op(target).is_none() {
					continue;
				}
				let class = eg
					.lookup(node.clone())
					.ok_or(Abort::new(ElabRule::E1, "a seed has no class"))?;
				let snapshot = canonical(eg, node);
				self.anchor(target, node, snapshot, class)?;
			}
		}

		let mut order = Vec::new();
		let mut yields: HashMap<u16, OpKind> = HashMap::new();
		let ir = self.ir;
		let anchors = &self.anchors;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if anchors.contains_key(&op) {
				order.push(op);
			}
			if let Some(data) = ir.op(op) {
				for region in &data.regions {
					let Some(held) = ir.region(*region) else {
						continue;
					};
					let closing = held
						.blocks
						.first()
						.and_then(|block| ir.block(*block))
						.and_then(|block| block.ops.last())
						.and_then(|last| ir.op(*last))
						.filter(|last| last.kind.terminator() == Some(Terminator::Yield));
					if let Some(closing) = closing {
						yields
							.entry(region_kind_key(held.kind))
							.or_insert_with(|| closing.kind.clone());
					}
				}
			}
			Walk::Continue
		});
		self.order = order;
		self.yields = yields;
		for node in self.term.chosen.values() {
			if let NodeShape::Op {
				payload,
				..
			} = node.identity.shape
				&& let Some(entry) = self.eg.analysis.payload(payload)
			{
				let regions = entry.kind.region_kinds().len();
				let operands = node.kids.len().saturating_sub(regions);
				self.region_classes.extend(node.kids[operands..].iter().copied());
			}
		}
		for op in self.order.clone() {
			let class = self.anchors[&op].class;
			self.by_class.entry(class).or_default().push(op);
			if self.keeps(&self.anchors[&op]) && self.user_alive(op) {
				self.kept.insert(op);
			}
		}
		Ok(())
	}

	/// Whether the term keeps `anchor` as it stands: it chose the anchor's node for its class and
	/// the anchor's node for each of its regions.
	fn keeps(&self, anchor: &Anchor) -> bool {
		self.term.chosen.get(&anchor.class) == Some(&anchor.node)
			&& anchor.regions.iter().all(|(class, node)| self.term.chosen.get(class) == Some(node))
	}

	/// Whether the region `region` outlives its owner: its owner is kept, or a chosen node owns
	/// the region's class.
	fn region_survives(&self, region: RegionId, owner: OpId) -> bool {
		if self.kept.contains(&owner) {
			return true;
		}
		self.homes.iter().any(|(class, homes)| {
			self.region_classes.contains(class)
				&& homes.iter().any(|home| {
					home.region == region && self.term.chosen.get(class) == Some(&home.node)
				})
		})
	}

	/// Whether `user` still runs once the plan has been applied.
	fn user_alive(&self, user: OpId) -> bool {
		let mut at = user;
		loop {
			let region = self.ir.enclosing_region(at);
			let Some(owner) = self.ir.parent_op(region) else {
				return true;
			};
			if self.anchors.contains_key(&owner) && !self.region_survives(region, owner) {
				return false;
			}
			at = owner;
		}
	}

	/// Whether some use of the value of the replaced anchor `op` survives the plan: it belongs to
	/// an op outside the island, or to a kept anchor, that still runs.
	fn needs_replacement(&self, op: OpId) -> bool {
		let Some(value) = result_of(self.ir, op) else {
			return false;
		};
		self.ir.uses(value).iter().any(|used| {
			self.user_alive(used.user)
				&& (!self.anchors.contains_key(&used.user) || self.kept.contains(&used.user))
		})
	}

	/// The provenance of `class` in the ids of the target.
	fn provenance(&self, class: ClassRef) -> Result<SmallVec<[OpId; 2]>, Abort> {
		let mut out: SmallVec<[OpId; 2]> = SmallVec::new();
		for original in &self.eg[Id::from(class)].data.provenance {
			let copy = self
				.map
				.ops_to_copy
				.get(original)
				.ok_or(Abort::new(ElabRule::E6, "a provenance id has no copy"))?;
			out.push(*copy);
		}
		out.sort_unstable();
		out.dedup();
		Ok(out)
	}

	/// The language-level type of `class`: what its first anchor's value holds.
	fn type_of(&self, class: ClassRef) -> TypeKnowledge {
		self.by_class
			.get(&class)
			.and_then(|ops| ops.first())
			.and_then(|op| result_of(self.ir, *op))
			.and_then(|value| self.ir.value(value))
			.map_or(TypeKnowledge::Unknown, |data| data.ty.clone())
	}

	/// The location of a new op placed because of `anchor`. The text the anchor was written as is
	/// kept, so the clause an op computes still prints as written.
	fn location(&self, anchor: OpId) -> Location {
		self.ir.op(anchor).map_or(
			Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
			|data| {
				let synthesized = Location {
					stmt: data.loc.stmt,
					span: data.loc.span,
					origin: LocOrigin::Synthesized,
				};
				match &data.loc.origin {
					LocOrigin::Written(text) => Location {
						origin: LocOrigin::Written(std::sync::Arc::clone(text)),
						..synthesized
					},
					LocOrigin::Query | LocOrigin::Catalog(_) | LocOrigin::Synthesized => {
						synthesized
					}
				}
			},
		)
	}

	/// The value of the target a leaf's value names.
	fn resolve(&self, value: ValueId) -> Result<ValueId, Abort> {
		let mapped =
			self.map.values_to_copy.get(&value).copied().filter(|v| self.ir.value(*v).is_some());
		let direct = (self.map.values_to_orig.contains_key(&value)
			&& self.ir.value(value).is_some())
		.then_some(value);
		match (mapped, direct) {
			(Some(a), Some(b)) if a != b => {
				Err(Abort::new(ElabRule::E5, "a leaf's value names two values of the target"))
			}
			(Some(value), _) | (None, Some(value)) => Ok(value),
			(None, None) => Err(Abort::new(ElabRule::E5, "a leaf's value is not in the target")),
		}
	}

	fn leaf_value(&self, leaf: LeafId, site: &Site) -> Result<Operand, Abort> {
		let entry = self
			.eg
			.analysis
			.leaves
			.get(leaf.0 as usize)
			.ok_or(Abort::new(ElabRule::E5, "a leaf is not in the graph"))?;
		if entry.region.is_some() {
			return Err(Abort::new(ElabRule::E5, "a region leaf stands where a value is read"));
		}
		let value = self.resolve(entry.value)?;
		if !visible(self.ir, value, site.anchor) {
			return Err(Abort::new(ElabRule::E3, "a leaf's value is not defined where it is read"));
		}
		Ok(Operand::Value(value))
	}

	fn arg_value(&self, binder: BinderId, index: u8, site: &Site) -> Result<Operand, Abort> {
		if let Some((_, slot)) = self.scope.iter().rev().find(|(held, _)| *held == binder) {
			return Ok(Operand::Arg(*slot, u16::from(index)));
		}
		let entry = self
			.eg
			.analysis
			.binders
			.get(binder.0 as usize)
			.ok_or(Abort::new(ElabRule::E4, "a block argument has no binder"))?;
		let region = self
			.map
			.regions_to_copy
			.get(&entry.origin)
			.copied()
			.ok_or(Abort::new(ElabRule::E4, "a binder's region is not in the target"))?;
		let arg = self
			.ir
			.region(region)
			.and_then(|held| held.blocks.first())
			.and_then(|block| self.ir.block(*block))
			.and_then(|block| block.args.get(usize::from(index)).copied())
			.ok_or(Abort::new(ElabRule::E4, "a binder's block has no such argument"))?;
		if !visible(self.ir, arg, site.anchor) {
			return Err(Abort::new(ElabRule::E4, "a binder is not in scope where it is read"));
		}
		Ok(Operand::Value(arg))
	}

	/// The operand that stands for `class` at `site`.
	fn value_of(&mut self, class: ClassRef, site: &Site, depth: usize) -> Result<Operand, Abort> {
		if depth > MAX_DEPTH || self.steps.len() > self.max_steps {
			return Err(Abort::new(ElabRule::E2, "the term is larger than the plan allows"));
		}
		let node = self
			.term
			.chosen
			.get(&class)
			.cloned()
			.ok_or(Abort::new(ElabRule::E1, "a class of the term has no chosen node"))?;
		if let Some(ops) = self.by_class.get(&class) {
			for op in ops {
				if self.kept.contains(op)
					&& dominates(self.ir, *op, site.anchor)
					&& let Some(value) = result_of(self.ir, *op)
				{
					return Ok(Operand::Value(value));
				}
			}
		}
		match node.identity.shape {
			NodeShape::Opaque {
				leaf,
			} => self.leaf_value(leaf, site),
			NodeShape::Arg {
				binder,
				index,
			} => self.arg_value(binder, index, site),
			NodeShape::Region {
				..
			} => Err(Abort::new(ElabRule::E4, "a region node stands where a value is read")),
			NodeShape::Op {
				payload,
				..
			} => self.new_op(class, &node, payload, site, depth),
		}
	}

	/// Creates the op `node` stands for, with its operands and regions, at `site`.
	fn new_op(
		&mut self,
		class: ClassRef,
		node: &ENodeSnapshot,
		payload: PayloadId,
		site: &Site,
		depth: usize,
	) -> Result<Operand, Abort> {
		let kind = self
			.eg
			.analysis
			.payload(payload)
			.map(|entry| (*entry.kind).clone())
			.ok_or(Abort::new(ElabRule::E1, "a node's payload is not in the graph"))?;
		let region_kinds = kind.region_kinds();
		let operand_count = node
			.kids
			.len()
			.checked_sub(region_kinds.len())
			.ok_or(Abort::new(ElabRule::E4, "a node has fewer kids than its op has regions"))?;
		let mut operands: SmallVec<[Operand; 4]> = SmallVec::new();
		for kid in &node.kids[..operand_count] {
			operands.push(self.value_of(*kid, site, depth + 1)?);
			if let Some(NodeShape::Opaque {
				leaf,
			}) = self.term.chosen.get(kid).map(|held| held.identity.shape)
				&& !self.tree_leaves.insert(leaf)
			{
				return Err(Abort::new(ElabRule::E5, "one tree reads a leaf from two new ops"));
			}
		}
		let provenance = self.provenance(class)?;
		let ty = self.type_of(class);
		let loc = self.location(site.anchor);
		let slot = self.steps.len() as Slot;
		self.steps.push(Step::Op(NewOp {
			kind,
			operands,
			results: 1,
			ty,
			frame: node.identity.frame,
			snapshot: node.identity.snapshot,
			loc,
			provenance,
			place: site.place,
		}));
		self.built.push((slot, class));
		for (at, kid) in node.kids[operand_count..].iter().enumerate() {
			self.region_kid(*kid, region_kinds[at], slot, site, depth)?;
		}
		Ok(Operand::Result(slot))
	}

	/// Gives the op step `owner` the region the class `class` stands for: the incumbent region
	/// moves, and any other region node is built.
	fn region_kid(
		&mut self,
		class: ClassRef,
		kind: RegionKind,
		owner: Slot,
		site: &Site,
		depth: usize,
	) -> Result<(), Abort> {
		let node = self
			.term
			.chosen
			.get(&class)
			.cloned()
			.ok_or(Abort::new(ElabRule::E1, "a region class of the term has no chosen node"))?;
		if let Some(homes) = self.homes.get(&class)
			&& homes.iter().any(|home| home.node == node)
		{
			let home = homes
				.iter()
				.find(|home| {
					home.node == node
						&& !self.kept.contains(&home.owner)
						&& !self.moved.contains(&home.region)
				})
				.cloned()
				.ok_or(Abort::new(ElabRule::E5, "a region would belong to two ops"))?;
			self.moved.insert(home.region);
			self.steps.push(Step::Move {
				region: home.region,
				owner,
			});
			return Ok(());
		}
		let NodeShape::Region {
			kind_key,
			binder,
			..
		} = node.identity.shape
		else {
			return Err(Abort::new(ElabRule::E5, "a region leaf has no region to move"));
		};
		if region_kind_key(kind) != kind_key {
			return Err(Abort::new(
				ElabRule::E4,
				"a region node has the wrong kind for its position",
			));
		}
		let closing = self
			.yields
			.get(&kind_key)
			.cloned()
			.ok_or(Abort::new(ElabRule::E4, "no region of this kind shows how it closes"))?;
		let args = self.binder_types(binder);
		let region = self.steps.len() as Slot;
		self.steps.push(Step::Region {
			owner,
			kind,
			args,
		});
		self.scope.push((binder, region));
		let inner = Site {
			anchor: site.anchor,
			place: Place::End(region),
		};
		let mut yielded: SmallVec<[Operand; 4]> = SmallVec::new();
		for kid in &node.kids {
			yielded.push(self.value_of(*kid, &inner, depth + 1)?);
		}
		self.scope.pop();
		let loc = self.location(site.anchor);
		self.steps.push(Step::Op(NewOp {
			kind: closing,
			operands: yielded,
			results: 0,
			ty: TypeKnowledge::Unknown,
			frame: node.identity.frame,
			snapshot: node.identity.snapshot,
			loc,
			provenance: SmallVec::new(),
			place: Place::End(region),
		}));
		Ok(())
	}

	/// The types of the block arguments a region of `binder` takes.
	fn binder_types(&self, binder: BinderId) -> SmallVec<[TypeKnowledge; 2]> {
		let Some(entry) = self.eg.analysis.binders.get(binder.0 as usize) else {
			return SmallVec::new();
		};
		let args = self
			.map
			.regions_to_copy
			.get(&entry.origin)
			.and_then(|region| self.ir.region(*region))
			.and_then(|region| region.blocks.first())
			.and_then(|block| self.ir.block(*block))
			.map(|block| block.args.clone());
		(0..usize::from(entry.args))
			.map(|at| {
				args.as_ref()
					.and_then(|held| held.get(at))
					.and_then(|value| self.ir.value(*value))
					.map_or(TypeKnowledge::Unknown, |data| data.ty.clone())
			})
			.collect()
	}

	/// Builds a subtree for every replaced anchor whose value is still read.
	fn replace(&mut self) -> Result<(), Abort> {
		let order = self.order.clone();
		for op in order {
			let class = self.anchors[&op].class;
			if self.kept.contains(&op) || !self.term.chosen.contains_key(&class) {
				continue;
			}
			if !self.needs_replacement(op) {
				continue;
			}
			let Some(from) = result_of(self.ir, op) else {
				continue;
			};
			self.tree_leaves.clear();
			let site = Site {
				anchor: op,
				place: Place::Before(op),
			};
			let to = self.value_of(class, &site, 0)?;
			self.swapped.insert(op, self.swaps.len());
			self.swaps.push(Swap {
				from,
				to,
			});
		}
		Ok(())
	}

	/// Whether the class may be dropped: nothing in it can raise, draw or write.
	fn droppable(&self, class: ClassRef) -> bool {
		let facts = &self.eg[Id::from(class)].data;
		infallible(facts.fallibility)
			&& deterministic(facts.determinism, facts.reads_written)
			&& !facts.effects.mutates_state()
	}

	/// Fixes the reason each op that stops being used leaves with, and the breakers an access path
	/// answered.
	fn eliminate(&self) -> Result<(Vec<Erase>, Vec<OpId>), Abort> {
		let mut erase = Vec::new();
		let mut fuse = Vec::new();
		for op in self.order.iter().rev() {
			if self.kept.contains(op) {
				continue;
			}
			let used = result_of(self.ir, *op).is_some_and(|value| !self.ir.uses(value).is_empty());
			if !used {
				continue;
			}
			let class = self.anchors[op].class;
			let reason = match self.swapped.get(op).map(|at| self.swaps[*at].to) {
				Some(Operand::Result(slot)) => Elimination::MergedInto(slot),
				Some(Operand::Value(value)) => match self.ir.value(value).map(|data| data.def) {
					Some(ValueDef::OpResult {
						op: into,
						..
					}) => Elimination::Reason(EliminationReason::MergedInto(into)),
					_ => self.dead(class)?,
				},
				Some(Operand::Arg(..)) | None => {
					if self.basis.answered.contains(&class) {
						if !infallible(self.eg[Id::from(class)].data.fallibility) {
							return Err(Abort::new(
								ElabRule::E8,
								"an access path answers a class that may raise",
							));
						}
						if self.is_breaker(*op) {
							fuse.push(*op);
						}
						Elimination::Reason(EliminationReason::AnsweredByAccessPath)
					} else {
						self.dead(class)?
					}
				}
			};
			erase.push(Erase {
				op: *op,
				reason,
			});
		}
		Ok((erase, fuse))
	}

	fn dead(&self, class: ClassRef) -> Result<Elimination, Abort> {
		if self.droppable(class) {
			Ok(Elimination::Reason(EliminationReason::DeadPure))
		} else {
			Err(Abort::new(ElabRule::E8, "an op that may raise, draw or write would be dropped"))
		}
	}

	fn is_breaker(&self, op: OpId) -> bool {
		matches!(
			self.ir.op(op).map(|data| data.kind.tag()),
			Some(OpTag::Logical(
				LogTag::Sort
					| LogTag::Window
					| LogTag::Limit | LogTag::Distinct
					| LogTag::Aggregate
			))
		)
	}
}

/// The most nodes the graph holds, which bounds the work a plan may do.
fn eg_size(eg: &EGraph<ENode, IrAnalysis>) -> usize {
	eg.total_number_of_nodes().max(16)
}

/// Aborts when a node the rules created reads one leaf twice: a leaf is never read from two places
/// of a new tree, so a rule that duplicates one cannot be written back.
fn check_leaf_reads(eg: &EGraph<ENode, IrAnalysis>, term: &Term) -> Result<(), Abort> {
	let an = &eg.analysis;
	let mut imported: HashSet<ENodeSnapshot> = HashSet::new();
	for id in an.incumbents.values() {
		let raw = eg.id_to_node(*id);
		imported.insert(canonical(eg, raw));
		for kid in &raw.kids {
			imported.insert(canonical(eg, eg.id_to_node(*kid)));
		}
	}
	for node in an.seeds.keys() {
		imported.insert(canonical(eg, node));
	}
	let mut read: HashSet<LeafId> = HashSet::new();
	for class in &term.order {
		let Some(node) = term.chosen.get(class) else {
			continue;
		};
		if imported.contains(node) {
			continue;
		}
		for kid in &node.kids {
			if let Some(NodeShape::Opaque {
				leaf,
			}) = term.chosen.get(kid).map(|held| held.identity.shape)
				&& !read.insert(leaf)
			{
				return Err(Abort::new(ElabRule::E5, "a new node reads a leaf twice"));
			}
		}
	}
	Ok(())
}

/// Plans the build of `choice` into `ir_target`, the statement copy `target_map` describes.
///
/// Mutates nothing. The plan keeps every op the term keeps, builds a subtree before each op whose
/// class the term chose another node for, and moves the uses of that op's value to the subtree.
/// It aborts when the term needs something the target cannot give: a leaf read twice, a binder
/// out of scope or a region that cannot be built, a provenance id with no copy, or an op that
/// may raise, draw or write and would be dropped.
pub(crate) fn plan(
	eg: &EGraph<ENode, IrAnalysis>,
	choice: &Term,
	target_map: &StmtMap,
	ir_target: &Ir,
	basis: &Basis<'_>,
) -> Result<ElabPlan, Abort> {
	check_leaf_reads(eg, choice)?;
	let mut planner = Planner::new(eg, choice, target_map, ir_target, basis);
	planner.index()?;
	planner.replace()?;
	let (erase, fuse) = planner.eliminate()?;
	Ok(ElabPlan {
		steps: planner.steps,
		swaps: planner.swaps,
		erase,
		fuse,
		built: planner.built,
	})
}

// ---------------------------------------------------------------------------------------------
// Applying
// ---------------------------------------------------------------------------------------------

/// What a step created.
enum Made {
	Op {
		op: OpId,
		value: Option<ValueId>,
	},
	Region {
		block: BlockId,
		args: SmallVec<[ValueId; 2]>,
	},
	Moved,
}

fn operand_value(operand: &Operand, made: &[Made]) -> Result<ValueId, IrError> {
	let stale = IrError::Stale("an elaboration operand");
	match operand {
		Operand::Value(value) => Ok(*value),
		Operand::Result(slot) => match made.get(*slot as usize) {
			Some(Made::Op {
				value: Some(value),
				..
			}) => Ok(*value),
			_ => Err(stale),
		},
		Operand::Arg(slot, index) => match made.get(*slot as usize) {
			Some(Made::Region {
				args,
				..
			}) => args.get(usize::from(*index)).copied().ok_or(stale),
			_ => Err(stale),
		},
	}
}

/// Runs `plan` through `rw`, which must be over the container the plan was made for.
///
/// Every new op is created inside `rw.derived` over the provenance the plan fixed. The swaps then
/// move the uses, and the ops left without uses are erased in the plan's order. An op that is
/// still used stays; erasing an op takes the ops of its regions with it. An `Err` leaves the
/// container half-built, so the caller discards it.
pub(crate) fn apply(plan: &ElabPlan, rw: &mut Rewriter<'_>) -> Result<(), IrError> {
	apply_mapped(plan, rw).map(|_| ())
}

/// Runs `plan` as [`apply`] does, and answers the result value of each created op with the class
/// its node was built for.
pub(crate) fn apply_mapped(
	plan: &ElabPlan,
	rw: &mut Rewriter<'_>,
) -> Result<Vec<(ValueId, ClassRef)>, IrError> {
	let mut made: Vec<Made> = Vec::with_capacity(plan.steps.len());
	for step in &plan.steps {
		let step_made = match step {
			Step::Op(new) => {
				let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
				for operand in &new.operands {
					operands.push(operand_value(operand, &made)?);
				}
				let at = match new.place {
					Place::Before(op) => InsertPoint::Before(op),
					Place::End(slot) => match made.get(slot as usize) {
						Some(Made::Region {
							block,
							..
						}) => InsertPoint::End(*block),
						_ => return Err(IrError::Stale("an elaboration region slot")),
					},
				};
				let spec = OpSpec {
					kind: new.kind.clone(),
					operands,
					results: (0..new.results).map(|_| new.ty.clone()).collect(),
					frame: new.frame,
					snapshot: new.snapshot,
					loc: new.loc.clone(),
				};
				let op = rw.derived(&new.provenance, |rw| rw.create(at, spec))?;
				let value = rw.ir().op(op).and_then(|data| data.results.first().copied());
				Made::Op {
					op,
					value,
				}
			}
			Step::Region {
				owner,
				kind,
				args,
			} => {
				let Some(Made::Op {
					op,
					..
				}) = made.get(*owner as usize)
				else {
					return Err(IrError::Stale("an elaboration region owner"));
				};
				let (_, block) = rw.create_region(*op, *kind, args)?;
				let args = rw.ir().block(block).map(|held| held.args.clone()).unwrap_or_default();
				Made::Region {
					block,
					args,
				}
			}
			Step::Move {
				region,
				owner,
			} => {
				let Some(Made::Op {
					op,
					..
				}) = made.get(*owner as usize)
				else {
					return Err(IrError::Stale("an elaboration region owner"));
				};
				rw.move_region(*region, *op)?;
				Made::Moved
			}
		};
		made.push(step_made);
	}
	for swap in &plan.swaps {
		let to = operand_value(&swap.to, &made)?;
		rw.replace_all_uses(swap.from, to)?;
	}
	for step_made in &made {
		if let Made::Op {
			value: Some(value),
			..
		} = step_made
			&& rw.ir().uses(*value).is_empty()
		{
			return Err(IrError::Stale("an elaborated op is not used"));
		}
	}
	for erase in &plan.erase {
		let Some(data) = rw.ir().op(erase.op) else {
			continue;
		};
		if data.results.iter().any(|result| !rw.ir().uses(*result).is_empty()) {
			continue;
		}
		let reason = match erase.reason {
			Elimination::Reason(reason) => reason,
			Elimination::MergedInto(slot) => match made.get(slot as usize) {
				Some(Made::Op {
					op,
					..
				}) => EliminationReason::MergedInto(*op),
				_ => return Err(IrError::Stale("an elaboration merge target")),
			},
		};
		rw.erase(erase.op, reason)?;
	}
	let mut created = Vec::with_capacity(plan.built.len());
	for (slot, class) in &plan.built {
		if let Some(Made::Op {
			value: Some(value),
			..
		}) = made.get(*slot as usize)
		{
			created.push((*value, *class));
		}
	}
	Ok(created)
}

// ---------------------------------------------------------------------------------------------
// Write-back
// ---------------------------------------------------------------------------------------------

/// Writes candidate `cand1`, a rewritten logical form of the statement `stmt`, into `ir`.
///
/// Two copies of the statement are taken, a backup and a target. The plan is built over the target
/// and applied to it through a fresh rewriter and fact cache, and the patched target is adopted.
/// The container is then verified at the logical phase boundary and the legality of the statement
/// retaken; when either fails the backup is adopted and the statement is as it was.
///
/// Any failure before the first adopt is [`FallbackReason::WritebackAborted`] and leaves `ir`
/// untouched. A rewrite that does not verify is [`FallbackReason::LogicalVerifyFailed`]. A plan
/// that changes nothing writes nothing.
pub(crate) fn write_logical(
	ir: &mut Ir,
	facts: &mut FactCache,
	stmt: OpId,
	eg: &EGraph<ENode, IrAnalysis>,
	cand1: &Candidate,
	surgery: &dyn StatementSurgery,
	cx: &EffectCx<'_>,
) -> Result<(), FallbackReason> {
	let aborted = |_: IrError| FallbackReason::WritebackAborted;
	let (backup, backup_map) = surgery.copy_statement(ir, stmt).map_err(aborted)?;
	let (mut target, target_map) = surgery.copy_statement(ir, stmt).map_err(aborted)?;
	let built = plan(eg, &cand1.term, &target_map, &target, &Basis::LOGICAL)
		.map_err(|_| FallbackReason::WritebackAborted)?;
	if built.is_empty() {
		return Ok(());
	}
	{
		let mut scratch = FactCache::new();
		let mut rw = Rewriter::new(&mut target, &mut scratch);
		apply(&built, &mut rw).map_err(aborted)?;
	}
	surgery.adopt_statement(ir, facts, stmt, target, &target_map).map_err(aborted)?;
	let sound = tokens::rethread_statement(ir, facts, cx, stmt).is_ok()
		&& verify(ir, facts, VerifyAt::PhaseBoundary(Phase::Logical), &LOGICAL_PHASE, cx).is_ok()
		&& surgery.retake_legality(ir, facts, cx.registry, stmt).is_ok();
	if sound {
		super::note_adopted();
		return Ok(());
	}
	let restored = surgery.adopt_statement(ir, facts, stmt, backup, &backup_map);
	debug_assert!(restored.is_ok(), "a statement copy adopts back into its own container");
	let rethreaded = tokens::rethread_statement(ir, facts, cx, stmt);
	debug_assert!(rethreaded.is_ok(), "a restored statement is threaded again");
	Err(FallbackReason::LogicalVerifyFailed)
}

/// The converted copy of the winning candidate, and what describes it.
pub(crate) struct SeedTarget {
	pub(crate) ir: Ir,
	pub(crate) map: StmtMap,
	pub(crate) candidate: u8,
}

/// Writes the physical plan `choice` into `ir` by patching the converted copy of the winning
/// candidate and adopting it in place of `stmt`.
///
/// A plan that changes nothing over the copy of candidate 0 writes nothing: converting the real
/// statement produces the same ops. Any failure before the adopt is
/// [`FallbackReason::WritebackAborted`] and leaves `ir` untouched. The conversion's own checks and
/// the physical boundary's verification cover the adopted statement.
pub(crate) fn write_seeded(
	ir: &mut Ir,
	facts: &mut FactCache,
	stmt: OpId,
	eg: &EGraph<ENode, IrAnalysis>,
	choice: &PhysChoice,
	winner: SeedTarget,
	surgery: &dyn StatementSurgery,
) -> Result<(), FallbackReason> {
	let SeedTarget {
		ir: mut target,
		map,
		candidate,
	} = winner;
	let basis = Basis {
		seed: Some(candidate),
		answered: &[],
	};
	let built = plan(eg, &choice.term, &map, &target, &basis)
		.map_err(|_| FallbackReason::WritebackAborted)?;
	if built.is_empty() && candidate == 0 {
		return Ok(());
	}
	{
		let mut scratch = FactCache::new();
		let mut rw = Rewriter::new(&mut target, &mut scratch);
		apply(&built, &mut rw).map_err(|_| FallbackReason::WritebackAborted)?;
	}
	surgery
		.adopt_statement(ir, facts, stmt, target, &map)
		.map_err(|_| FallbackReason::WritebackAborted)?;
	super::note_adopted();
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use egg::EGraph;
	use smallvec::smallvec;

	use super::{Basis, Planner, RegionHome};
	use crate::exec::ir::core::ir::{Ir, IrScope};
	use crate::exec::ir::egraph::analysis::IrAnalysis;
	use crate::exec::ir::egraph::decl::{
		BinderId, ClassRef, ENodeSnapshot, EpochKey, LeafId, NodeIdentity, NodeShape, StmtMap,
	};
	use crate::exec::ir::egraph::extract::Term;
	use crate::exec::ir::egraph::lang::ENode;
	use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId};
	use crate::exec::ir::iface::region_kind::RegionKind;

	fn dummy_snapshot(shape: NodeShape) -> ENodeSnapshot {
		ENodeSnapshot {
			identity: NodeIdentity {
				shape,
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				epoch: EpochKey::None,
			},
			kids: smallvec![],
		}
	}

	#[test]
	fn region_survives_requires_chosen_node_to_match_home_node() {
		let eg: EGraph<ENode, IrAnalysis> = EGraph::new(IrAnalysis::new());
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let map = StmtMap::default();
		let region_class = ClassRef(0);
		let home_node = dummy_snapshot(NodeShape::Opaque {
			leaf: LeafId(0),
		});
		let built_region_node = dummy_snapshot(NodeShape::Region {
			kind_key: 0,
			binder: BinderId(0),
			results: 1,
		});
		let owner = OpId::default();
		let region = RegionId::default();

		// When the term chose a newly built Region node for `region_class` instead of `home.node`,
		// the incumbent `home.region` is not moved and does not survive its replaced owner.
		let mut chosen = HashMap::new();
		chosen.insert(region_class, built_region_node);
		let term = Term {
			roots: smallvec![region_class],
			order: vec![region_class],
			chosen,
		};
		let mut planner = Planner::new(&eg, &term, &map, &ir, &Basis::LOGICAL);
		planner.region_classes.insert(region_class);
		planner.homes.insert(
			region_class,
			smallvec![RegionHome {
				owner,
				region,
				node: home_node.clone(),
			}],
		);
		assert!(!planner.region_survives(region, owner));

		// When the term chose `home.node` for `region_class`, `home.region` is moved and survives.
		let mut chosen_home = HashMap::new();
		chosen_home.insert(region_class, home_node);
		let term_home = Term {
			roots: smallvec![region_class],
			order: vec![region_class],
			chosen: chosen_home,
		};
		let mut planner_home = Planner::new(&eg, &term_home, &map, &ir, &Basis::LOGICAL);
		planner_home.region_classes.insert(region_class);
		planner_home.homes.insert(
			region_class,
			smallvec![RegionHome {
				owner,
				region,
				node: term_home.chosen[&region_class].clone(),
			}],
		);
		assert!(planner_home.region_survives(region, owner));
	}
}
