//! The container: arenas of ops, values, blocks and regions, the interned frames and snapshots,
//! the eliminated-op list, per-op generation stamps and the placement table.
//!
//! One container is single-owner and holds no interior mutability and no non-`Send` handle, so a
//! planner future may hold it across an `.await`. Entities are mutated through the rewriter only;
//! every public accessor here is a pure read of the arenas.
//!
//! Four private side tables record what the verifier checks and the rewriter alone maintains:
//! how many live ops hold each logical id, every logical op ever created, the error scope an op
//! was bound in, and the generation at which each stored-clause op was sealed.

use slotmap::SlotMap;
use smallvec::SmallVec;

use super::block::BlockData;
use super::op::OpData;
use super::region::RegionData;
use super::rewriter::Provenance;
use super::side_map::SideMap;
use super::token::OpTokens;
use super::value::{Use, ValueData, ValueDef};
use super::walk::ops_in;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::conversion::{EliminationReason, Phase};
use crate::exec::ir::iface::frame::{AuthorityFrame, FrameTable, Snapshot};
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::premise::CatalogVersionVector;
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::phys::hop_key::{HopKey, HopKeyTable};
use crate::exec::ir::phys::matches::{IndexScope, MatchesAttr, MatchesTable};
use crate::exec::ir::phys::placement_attr::{PlacementAttr, PlacementTable};
use crate::exec::vm::program::UnitFrame;
use crate::expr::logical::EvalFrame;

/// What a container holds the program of.
///
/// One per container, so the size of the larger variant is not boxed away.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum IrScope {
	/// A request, or one fragment of a streaming session. The top region is a `Request`.
	Request {
		fragment: Option<u32>,
	},
	/// Definitions and memoised triggered subgraphs read at one catalog version by one planning
	/// session. A request's ops may be cloned from it by id remapping while the session lasts;
	/// it is dropped with the session, and nothing keeps it or refers to it afterwards.
	CatalogVersion {
		version: CatalogVersionVector,
	},
}

/// The order a walk visits an op relative to the ops in its regions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WalkOrder {
	/// An op, then the ops of its regions.
	PreOrder,
	/// The ops of an op's regions, then the op.
	PostOrder,
}

/// What a walk does after visiting an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Walk {
	Continue,
	/// Do not visit the ops of this op's regions. Under [`WalkOrder::PostOrder`] they have been
	/// visited already, so this behaves as [`Walk::Continue`].
	SkipRegions,
	/// End the walk.
	Stop,
}

/// The arena capacities a new container starts with: enough for a small statement.
const INITIAL_OPS: usize = 32;
const INITIAL_VALUES: usize = 48;
const INITIAL_REGIONS: usize = 16;

/// A list of op ids collected from a walk, held inline for a small program.
pub(crate) type OpList = SmallVec<[OpId; 32]>;

/// One program: a request's or a fragment's, or one catalog version's definitions.
///
/// The frame table and the snapshot table each hold the container's default entry at index 0:
/// the caller's own authority lowered for no principal, and the current state. Only an op built
/// with id 0 resolves to it: an id outside a table resolves to nothing, because answering the
/// default would run an op under the caller's authority, or read the current state, on a
/// mis-indexed id.
#[derive(Clone, Debug)]
pub(crate) struct Ir {
	pub(super) scope: IrScope,
	pub(super) phase: Phase,
	pub(super) top: RegionId,
	pub(super) ops: SlotMap<OpId, OpData>,
	pub(super) values: SlotMap<ValueId, ValueData>,
	pub(super) blocks: SlotMap<BlockId, BlockData>,
	pub(super) regions: SlotMap<RegionId, RegionData>,
	pub(super) frames: FrameTable,
	/// The interned snapshots. A `Version` entry names a value outside every use list, so the
	/// rewriter keeps it exact: a replacement of the value rewrites the entry, and an op whose
	/// result the entry names is not erased while an op outside the erased ops reads it.
	pub(super) snapshots: Vec<Snapshot>,
	pub(super) eliminated: Vec<(OpId, EliminationReason)>,
	pub(super) generations: SideMap<OpId, u32>,
	pub(super) placement: PlacementTable,
	/// What the physical plan decided for each `MATCHES` operator; never identity.
	pub(super) matches: MatchesTable,
	/// The key condition the physical plan decided for each graph hop; never identity.
	pub(super) hop_keys: HopKeyTable,
	/// For each op id, the number of live ops whose provenance holds it. `Bound` holds the op's
	/// own id. An id no live op holds has no entry.
	pub(super) holders: FxHashMap<OpId, u32>,
	/// Every op created `Bound` with an [`OpKind::Logical`](super::op::OpKind) kind, in creation
	/// order: the logical ids the container must keep covered.
	pub(super) logical_ids: Vec<OpId>,
	/// For a live op created `Bound`: the owner of its innermost error-scope region at creation,
	/// or `None` outside every error scope.
	pub(super) bind_scope: SideMap<OpId, Option<OpId>>,
	/// The entries of `bind_scope` of erased ops.
	pub(super) retired_scope: FxHashMap<OpId, Option<OpId>>,
	/// The generation of each op inside a stored-clause region at the moment the container left
	/// [`Phase::Bound`].
	pub(super) sealed: SideMap<OpId, u32>,
	/// The number of ops standing directly in each stored-clause region, and in each region
	/// beneath one, at the moment the container left [`Phase::Bound`]. An entry is dropped with
	/// its region when the region's owner is erased. A sealed region that is gone while its owner
	/// lives, or that holds another number of ops, was edited.
	pub(super) sealed_regions: SideMap<RegionId, usize>,
	/// Whether the container holds code the request wrote, which the region constraints judge
	/// ([`Ir::judges_constraints`]).
	pub(super) constrained: bool,
	/// The effect tokens each op takes and produces, apart from its operands and results.
	pub(super) op_tokens: SideMap<OpId, OpTokens>,
	/// Whether effect tokens are threaded through the container ([`Ir::is_threaded`]).
	pub(super) threaded: bool,
	/// The ops created while the container is threaded and not threaded themselves since
	/// ([`Ir::holds_no_tokens`]).
	pub(super) untokened: SideMap<OpId, ()>,
}

impl Ir {
	/// An empty container whose top region has kind `top` and holds one empty block.
	pub(crate) fn new(scope: IrScope, top: RegionKind) -> Self {
		Self::with_top_args(scope, top, &[])
	}

	/// An empty container whose top region has kind `top` and holds one empty block whose
	/// arguments are typed by `args`.
	///
	/// A container that is a unit, planned as a body of its own, has no statement around it to
	/// supply the statement timestamp, so it takes the timestamp as the first argument of its
	/// top block.
	pub(crate) fn with_top_args(scope: IrScope, top: RegionKind, args: &[TypeKnowledge]) -> Self {
		// Each arena allocates on creation whatever its capacity, so it is sized for a small
		// program at no extra allocation and grows only past that.
		let mut regions = SlotMap::with_capacity_and_key(INITIAL_REGIONS);
		let mut blocks = SlotMap::with_capacity_and_key(INITIAL_REGIONS);
		let mut values = SlotMap::with_capacity_and_key(INITIAL_VALUES);
		let top_id = regions.insert(RegionData {
			kind: top,
			blocks: Default::default(),
			parent: None,
		});
		let block = blocks.insert(BlockData {
			args: Default::default(),
			token_args: Default::default(),
			ops: Vec::new(),
			parent: top_id,
		});
		for (at, ty) in args.iter().enumerate() {
			let value = values.insert(ValueData {
				def: ValueDef::BlockArg {
					block,
					index: u16::try_from(at).unwrap_or(u16::MAX),
				},
				ty: ty.clone(),
				uses: Default::default(),
				hint_name: None,
			});
			if let Some(data) = blocks.get_mut(block) {
				data.args.push(value);
			}
		}
		if let Some(region) = regions.get_mut(top_id) {
			region.blocks.push(block);
		}
		Self {
			scope,
			phase: Phase::Bound,
			top: top_id,
			ops: SlotMap::with_capacity_and_key(INITIAL_OPS),
			values,
			blocks,
			regions,
			frames: FrameTable::of(vec![AuthorityFrame {
				eval: EvalFrame::caller(),
				unit: UnitFrame::unprincipled(),
			}]),
			snapshots: vec![Snapshot::Current],
			eliminated: Vec::new(),
			generations: SideMap::new(),
			placement: PlacementTable::new(),
			matches: MatchesTable::new(),
			hop_keys: HopKeyTable::new(),
			holders: FxHashMap::default(),
			logical_ids: Vec::new(),
			bind_scope: SideMap::new(),
			retired_scope: FxHashMap::default(),
			sealed: SideMap::new(),
			sealed_regions: SideMap::new(),
			constrained: false,
			op_tokens: SideMap::new(),
			threaded: false,
			untokened: SideMap::new(),
		}
	}

	/// Whether the region constraints judge the container: it was bound from code the request
	/// wrote and admitted when bound, so every region of it admits what it holds for as long as it
	/// lives, whatever a pass does to it.
	pub(crate) fn judges_constraints(&self) -> bool {
		self.constrained
	}

	/// States that the region constraints judge the container ([`Ir::judges_constraints`]).
	pub(crate) fn judge_constraints(&mut self) {
		self.constrained = true;
	}

	/// What the container holds the program of.
	pub(crate) fn scope(&self) -> &IrScope {
		&self.scope
	}

	/// The top region.
	pub(crate) fn top(&self) -> RegionId {
		self.top
	}

	/// How far the container has progressed.
	pub(crate) fn phase(&self) -> Phase {
		self.phase
	}

	/// The op `id` names, or `None` for a dangling id.
	pub(crate) fn op(&self, id: OpId) -> Option<&OpData> {
		self.ops.get(id)
	}

	/// The data of every live op, in no particular order.
	pub(crate) fn live_op_data(&self) -> impl Iterator<Item = &OpData> + '_ {
		self.ops.values()
	}

	/// The value `id` names, or `None` for a dangling id.
	pub(crate) fn value(&self, id: ValueId) -> Option<&ValueData> {
		self.values.get(id)
	}

	/// The block `id` names, or `None` for a dangling id.
	pub(crate) fn block(&self, id: BlockId) -> Option<&BlockData> {
		self.blocks.get(id)
	}

	/// The region `id` names, or `None` for a dangling id.
	pub(crate) fn region(&self, id: RegionId) -> Option<&RegionData> {
		self.regions.get(id)
	}

	/// The uses of `v`, in the order they were recorded; empty for a dangling id.
	pub(crate) fn uses(&self, v: ValueId) -> &[Use] {
		self.values.get(v).map_or(&[], |value| value.uses.as_slice())
	}

	/// The op that owns `r`; `None` for the top region and for a dangling id.
	pub(crate) fn parent_op(&self, r: RegionId) -> Option<OpId> {
		self.regions.get(r).and_then(|region| region.parent)
	}

	/// The region whose block holds `op`. A dangling id answers the top region.
	pub(crate) fn enclosing_region(&self, op: OpId) -> RegionId {
		self.ops
			.get(op)
			.and_then(|op| self.blocks.get(op.parent))
			.map_or(self.top, |block| block.parent)
	}

	/// Whether the region `id` holds a statement list, as its kind and its place as the top
	/// region of the container decide ([`RegionKind::holds_statement_list`]). A dangling id
	/// holds none.
	pub(crate) fn holds_statement_list(&self, id: RegionId) -> bool {
		self.regions.get(id).is_some_and(|region| region.kind.holds_statement_list(id == self.top))
	}

	/// The frame `id` names, or `None` for an id outside the table.
	pub(crate) fn frame(&self, id: FrameId) -> Option<&AuthorityFrame> {
		self.frames.get(id)
	}

	/// The frames of the container, indexed by [`FrameId`].
	pub(crate) fn frames(&self) -> &FrameTable {
		&self.frames
	}

	/// The snapshot `id` names, or `None` for an id outside the table.
	pub(crate) fn snapshot(&self, id: SnapshotId) -> Option<&Snapshot> {
		self.snapshots.get(usize::from(id.index()))
	}

	/// An op whose frame or snapshot id names no entry of its table, if any op has one. O(ops).
	pub(crate) fn op_outside_tables(&self) -> Option<OpId> {
		self.ops
			.iter()
			.find(|(_, op)| self.frame(op.frame).is_none() || self.snapshot(op.snapshot).is_none())
			.map(|(id, _)| id)
	}

	/// Every op the rewriter eliminated without a replacement, with the reason.
	pub(crate) fn eliminated(&self) -> &[(OpId, EliminationReason)] {
		&self.eliminated
	}

	/// The generation of `op`: bumped by every rewriter mutation of the op or of a descendant.
	/// An op never mutated, and a dangling id, answer `0`.
	pub(crate) fn generation(&self, op: OpId) -> u32 {
		self.generations.get(op).copied().unwrap_or(0)
	}

	/// The placement of the pipeline `pipeline`, or `None` when it has none.
	pub(crate) fn placement(&self, pipeline: OpId) -> Option<&PlacementAttr> {
		self.placement.get(pipeline)
	}

	/// The placement table. Placement is not identity: writing it never passes through the
	/// rewriter and never bumps a generation. Its writers are private to the physical dialect.
	pub(crate) fn placement_table_mut(&mut self) -> &mut PlacementTable {
		&mut self.placement
	}

	/// What the physical plan decided for the `MATCHES` operator `op`, or `None` when no select
	/// loop holds it.
	pub(crate) fn matches_attr(&self, op: OpId) -> Option<&MatchesAttr> {
		self.matches.get(op)
	}

	/// The scope the index function call `op` reports against: the one stated for it, else the
	/// one of the nearest region around it that holds a select loop declaring one. `None` when
	/// neither exists.
	pub(crate) fn index_scope(&self, op: OpId) -> Option<&IndexScope> {
		if let Some(scope) = self.matches.index_scope(op) {
			return Some(scope);
		}
		let mut at = op;
		loop {
			let region = self.enclosing_region(at);
			if let Some(scope) = self.matches.region_scope(region) {
				return Some(scope);
			}
			at = self.regions.get(region)?.parent?;
		}
	}

	/// Whether a search is in scope for the `KNN` operator `op`: a select loop holding it, or one
	/// around that, holds a `KNN` operator in its stages.
	pub(crate) fn knn_scoped(&self, op: OpId) -> bool {
		self.matches.knn_scoped(op)
	}

	/// Whether the `KNN` operator `op`, which a search is in scope for, judges the subjects of a
	/// write that no search of the statement reads.
	pub(crate) fn knn_unsearched(&self, op: OpId) -> bool {
		self.matches.knn_unsearched(op)
	}

	/// Whether the literal `op` stands for a `KNN` operator no search answers, and so reads the row
	/// as that operator does.
	pub(crate) fn knn_unanswered(&self, op: OpId) -> bool {
		self.matches.knn_unanswered(op)
	}

	/// The `MATCHES` decisions. Like placement, they are not identity and never bump a generation.
	pub(crate) fn matches_table_mut(&mut self) -> &mut MatchesTable {
		&mut self.matches
	}

	/// The key condition the physical plan decided for the graph hop `op`, or `None` when it has
	/// none.
	pub(crate) fn hop_key(&self, op: OpId) -> Option<&HopKey> {
		self.hop_keys.get(op)
	}

	/// The graph hops' key conditions. Like placement, they are not identity and never bump a
	/// generation. Their writer is private to the physical dialect.
	pub(crate) fn hop_key_table_mut(&mut self) -> &mut HopKeyTable {
		&mut self.hop_keys
	}

	/// Whether the definition of `def` is visible at `user`: `def` is an argument of a block
	/// enclosing `user`, or a result of an op that stands before `user`, or before the op
	/// enclosing `user`, in a block enclosing `user`. A value never reaches into a region that is
	/// isolated from above, so a definition outside such a region, with `user` inside it, does
	/// not dominate. A dangling id answers `false`, and so does an op's use of its own result.
	///
	/// O(depth of `user` + length of the block holding `def`).
	pub(crate) fn dominates(&self, def: ValueId, user: OpId) -> bool {
		let Some(def) = self.values.get(def).map(|value| value.def) else {
			return false;
		};
		let (def_block, def_op) = match def {
			ValueDef::OpResult {
				op,
				..
			} => match self.ops.get(op) {
				Some(data) => (data.parent, Some(op)),
				None => return false,
			},
			ValueDef::BlockArg {
				block,
				..
			} => (block, None),
		};
		let mut at = user;
		loop {
			let Some(block) = self.ops.get(at).map(|data| data.parent) else {
				return false;
			};
			if block == def_block {
				let Some(def_op) = def_op else {
					return true;
				};
				let Some(held) = self.blocks.get(block) else {
					return false;
				};
				return held.ops.iter().find(|op| **op == def_op || **op == at) == Some(&def_op)
					&& def_op != at;
			}
			let Some(region) =
				self.blocks.get(block).and_then(|held| self.regions.get(held.parent))
			else {
				return false;
			};
			if region.kind.isolated_from_above() {
				return false;
			}
			match region.parent {
				Some(owner) => at = owner,
				None => return false,
			}
		}
	}

	/// Whether `inner` is `outer` or lies beneath it, through the ops its regions own.
	pub(super) fn region_encloses(&self, outer: RegionId, inner: RegionId) -> bool {
		let mut at = inner;
		loop {
			if at == outer {
				return true;
			}
			match self.parent_op(at).and_then(|owner| self.ops.get(owner)) {
				Some(owner) => match self.blocks.get(owner.parent) {
					Some(block) => at = block.parent,
					None => return false,
				},
				None => return false,
			}
		}
	}

	/// The region whose block holds the definition of `v`; `None` for a dangling id.
	pub(super) fn value_region(&self, v: ValueId) -> Option<RegionId> {
		let block = match self.values.get(v)?.def {
			ValueDef::OpResult {
				op,
				..
			} => self.ops.get(op)?.parent,
			ValueDef::BlockArg {
				block,
				..
			} => block,
		};
		Some(self.blocks.get(block)?.parent)
	}

	/// The op a value belongs to: the op that defines it, or the owner of the region whose
	/// block it is an argument of. `None` for a dangling id and for an argument of the top region.
	pub(super) fn value_owner(&self, v: ValueId) -> Option<OpId> {
		match self.values.get(v)?.def {
			ValueDef::OpResult {
				op,
				..
			} => Some(op),
			ValueDef::BlockArg {
				block,
				..
			} => self.parent_op(self.blocks.get(block)?.parent),
		}
	}

	/// Whether `v` is defined by `op` or by an op, or as a block argument, inside `op`'s regions.
	pub(super) fn value_inside_op(&self, v: ValueId, op: OpId) -> bool {
		let Some(data) = self.ops.get(op) else {
			return false;
		};
		if let Some(ValueDef::OpResult {
			op: definer,
			..
		}) = self.values.get(v).map(|value| value.def)
			&& definer == op
		{
			return true;
		}
		self.value_region(v).is_some_and(|region| {
			data.regions.iter().any(|held| self.region_encloses(*held, region))
		})
	}

	/// Visits the ops of `r` and of every region beneath them, in `order`, as `f` directs.
	///
	/// Ops of one block are visited in block order. A dangling `r` visits nothing.
	pub(crate) fn walk(&self, r: RegionId, order: WalkOrder, f: &mut dyn FnMut(OpId) -> Walk) {
		self.walk_region(r, order, false, f);
	}

	/// The ops of `r` and of every region beneath them, in `order`, that `keep` admits.
	///
	/// The list is sized for every op of the container, so it grows at most once, and a program
	/// of up to [`OpList`]'s inline length collects without allocating.
	pub(crate) fn collect_ops(
		&self,
		r: RegionId,
		order: WalkOrder,
		mut keep: impl FnMut(OpId) -> bool,
	) -> OpList {
		let mut out = OpList::with_capacity(self.ops.len());
		self.walk(r, order, &mut |op| {
			if keep(op) {
				out.push(op);
			}
			Walk::Continue
		});
		out
	}

	/// [`Ir::walk`] without the ops inside a [`RegionKind::StoredClause`] region: stored code
	/// is bound and analysed, never offered to a rewrite. When `r` is itself a stored clause the
	/// walk visits nothing.
	pub(super) fn walk_outside_stored(
		&self,
		r: RegionId,
		order: WalkOrder,
		f: &mut dyn FnMut(OpId) -> Walk,
	) {
		self.walk_region(r, order, true, f);
	}

	/// Walks one region; answers `false` once `f` has answered [`Walk::Stop`]. With
	/// `skip_stored`, a stored-clause region is not entered.
	fn walk_region(
		&self,
		r: RegionId,
		order: WalkOrder,
		skip_stored: bool,
		f: &mut dyn FnMut(OpId) -> Walk,
	) -> bool {
		if skip_stored && self.is_stored_clause(r) {
			return true;
		}
		for op in ops_in(self, r) {
			match order {
				WalkOrder::PreOrder => match f(op) {
					Walk::Stop => return false,
					Walk::SkipRegions => {}
					Walk::Continue => {
						if !self.walk_regions_of(op, order, skip_stored, f) {
							return false;
						}
					}
				},
				WalkOrder::PostOrder => {
					if !self.walk_regions_of(op, order, skip_stored, f) {
						return false;
					}
					if matches!(f(op), Walk::Stop) {
						return false;
					}
				}
			}
		}
		true
	}

	/// Walks the regions `op` owns; answers `false` once `f` has answered [`Walk::Stop`].
	fn walk_regions_of(
		&self,
		op: OpId,
		order: WalkOrder,
		skip_stored: bool,
		f: &mut dyn FnMut(OpId) -> Walk,
	) -> bool {
		let Some(data) = self.ops.get(op) else {
			return true;
		};
		data.regions.iter().all(|region| self.walk_region(*region, order, skip_stored, f))
	}

	/// The number of ops that stand directly in the blocks of `r`; zero for a dangling `r`.
	pub(super) fn region_op_count(&self, r: RegionId) -> usize {
		ops_in(self, r).count()
	}

	/// Appends `r` and every region beneath it that is, or lies inside, a stored-clause region;
	/// `inside` says `r` itself lies inside one.
	pub(super) fn collect_stored_regions(
		&self,
		r: RegionId,
		inside: bool,
		out: &mut Vec<RegionId>,
	) {
		let Some(data) = self.regions.get(r) else {
			return;
		};
		let inside = inside || matches!(data.kind, RegionKind::StoredClause(_));
		if inside {
			out.push(r);
		}
		for op in ops_in(self, r) {
			for nested in self.ops.get(op).map_or(&[][..], |data| &data.regions) {
				self.collect_stored_regions(*nested, inside, out);
			}
		}
	}

	/// Whether `r` exists and is a stored-clause region.
	fn is_stored_clause(&self, r: RegionId) -> bool {
		self.regions.get(r).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	}

	/// Bumps the generation of the live op `op`. A dangling id changes nothing.
	pub(super) fn bump(&mut self, op: OpId) {
		if self.ops.contains_key(op) {
			let next = self.generation(op).wrapping_add(1);
			self.generations.insert(op, next);
		}
	}

	/// Bumps the generation of every op that owns a region enclosing `block`, innermost first.
	pub(super) fn bump_enclosing(&mut self, block: BlockId) {
		let mut block = block;
		while let Some(owner) = self
			.blocks
			.get(block)
			.and_then(|block| self.regions.get(block.parent))
			.and_then(|region| region.parent)
		{
			self.bump(owner);
			match self.ops.get(owner) {
				Some(op) => block = op.parent,
				None => break,
			}
		}
	}

	/// Bumps the generation of `op` and of every op that owns a region enclosing it.
	pub(super) fn bump_with_ancestors(&mut self, op: OpId) {
		self.bump(op);
		if let Some(parent) = self.ops.get(op).map(|op| op.parent) {
			self.bump_enclosing(parent);
		}
	}

	/// The owner of the innermost error-scope region enclosing `block`, or `None` when no
	/// error-scope region encloses it.
	pub(crate) fn error_scope_owner(&self, block: BlockId) -> Option<OpId> {
		let mut block = block;
		loop {
			let region = self.regions.get(self.blocks.get(block)?.parent)?;
			if matches!(region.kind, RegionKind::ErrorScope(_)) {
				return region.parent;
			}
			block = self.ops.get(region.parent?)?.parent;
		}
	}

	/// Counts one more live op holding `id`.
	pub(super) fn hold(&mut self, id: OpId) {
		*self.holders.entry(id).or_insert(0) += 1;
	}

	/// Counts one fewer live op holding `id`; answers whether no live op holds it any more.
	pub(super) fn release(&mut self, id: OpId) -> bool {
		match self.holders.get_mut(&id) {
			Some(count) if *count > 1 => {
				*count -= 1;
				false
			}
			Some(_) => {
				self.holders.remove(&id);
				true
			}
			None => true,
		}
	}

	/// Drops every id of `ids` from the eliminated list, in one pass over it: an op that stands
	/// for each exists again.
	pub(super) fn remove_from_eliminated(&mut self, ids: &FxHashSet<OpId>) {
		self.eliminated.retain(|(eliminated, _)| !ids.contains(eliminated));
	}

	/// Frees `op`, its values, its regions and its blocks, and records the ids it stood for that
	/// no other op holds. `detach` also removes it from its block, which the caller sets for the
	/// root of an erased subtree only: the block of any other op is freed with its region. Each op
	/// and value freed is reported to `forget`, so the owner of a cache keyed by them drops it.
	pub(super) fn free_op(
		&mut self,
		op: OpId,
		reason: EliminationReason,
		detach: bool,
		forget: &mut impl FnMut(Freed),
	) {
		let Some(data) = self.ops.remove(op) else {
			return;
		};
		for id in held_ids(&data.provenance, op) {
			if self.release(id) {
				self.eliminated.push((id, reason));
			}
		}
		if let Some(scope) = self.bind_scope.remove(op) {
			self.retired_scope.insert(op, scope);
		}
		self.sealed.remove(op);
		self.generations.remove(op);
		forget(Freed::Op(op));
		for value in &data.results {
			self.values.remove(*value);
			forget(Freed::Value(*value));
		}
		self.untokened.remove(op);
		if let Some(tokens) = self.op_tokens.remove(op) {
			for value in &tokens.results {
				self.values.remove(*value);
				forget(Freed::Value(*value));
			}
		}
		for region in &data.regions {
			self.sealed_regions.remove(*region);
			let Some(region) = self.regions.remove(*region) else {
				continue;
			};
			for block in &region.blocks {
				let Some(block) = self.blocks.remove(*block) else {
					continue;
				};
				for arg in block.args.iter().chain(&block.token_args) {
					self.values.remove(*arg);
					forget(Freed::Value(*arg));
				}
			}
		}
		if detach
			&& let Some(block) = self.blocks.get_mut(data.parent)
			&& let Some(at) = block.ops.iter().position(|held| *held == op)
		{
			block.ops.remove(at);
		}
	}

	/// Interns `frame`: the id of the equal frame already in the table, else a new one.
	///
	/// Fails when the table already holds as many frames as an id can name. A frame is never
	/// answered the id of a different one.
	pub(super) fn intern_frame(&mut self, frame: AuthorityFrame) -> Result<FrameId, IrError> {
		let at = match self.frames.frames.iter().position(|held| *held == frame) {
			Some(at) => at,
			None => {
				// No entry is pushed that no id can name, so a failed intern changes nothing.
				let at = self.frames.frames.len();
				u16::try_from(at).map_err(|_| IrError::Capacity("frame table"))?;
				self.frames.frames.push(frame);
				at
			}
		};
		u16::try_from(at).map(FrameId::new).map_err(|_| IrError::Capacity("frame table"))
	}

	/// Interns `snapshot`: the id of the equal snapshot already in the table, else a new one.
	///
	/// Fails when the table already holds as many snapshots as an id can name.
	pub(super) fn intern_snapshot(&mut self, snapshot: Snapshot) -> Result<SnapshotId, IrError> {
		let at = match self.snapshots.iter().position(|held| *held == snapshot) {
			Some(at) => at,
			None => {
				// No entry is pushed that no id can name, so a failed intern changes nothing.
				let at = self.snapshots.len();
				u16::try_from(at).map_err(|_| IrError::Capacity("snapshot table"))?;
				self.snapshots.push(snapshot);
				at
			}
		};
		u16::try_from(at).map(SnapshotId::new).map_err(|_| IrError::Capacity("snapshot table"))
	}
}

/// An entity [`Ir::free_op`] freed.
pub(super) enum Freed {
	Op(OpId),
	Value(ValueId),
}

/// The ids an op with `provenance` stands for: its own id when `Bound`, else the listed ids.
pub(super) fn held_ids(provenance: &Provenance, own: OpId) -> impl Iterator<Item = OpId> + '_ {
	let (itself, listed) = match provenance {
		Provenance::Bound => (Some(own), &[][..]),
		Provenance::From(ids) => (None, ids.as_slice()),
	};
	itself.into_iter().chain(listed.iter().copied())
}

#[cfg(test)]
impl Ir {
	/// Drops the last recorded use of `v` and leaves the operand that made it in place, so the
	/// use-lists do not match the operands. Lets a test hold a container no rewriter call can
	/// produce, to check that the verifier reports the break.
	pub(crate) fn forget_last_use(&mut self, v: ValueId) {
		if let Some(value) = self.values.get_mut(v) {
			value.uses.pop();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn request() -> Ir {
		Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		)
	}

	#[test]
	fn a_new_container_holds_a_top_region_with_one_empty_block() {
		let ir = request();
		let top = ir.region(ir.top()).expect("the top region exists");
		assert_eq!(top.kind, RegionKind::Request);
		assert_eq!(top.parent, None);
		assert_eq!(top.blocks.len(), 1);
		let block = ir.block(top.blocks[0]).expect("the top block exists");
		assert!(block.ops.is_empty());
		assert_eq!(block.parent, ir.top());
		assert_eq!(ir.parent_op(ir.top()), None);
		assert_eq!(ir.phase(), Phase::Bound);
		assert!(ir.eliminated().is_empty());
	}

	#[test]
	fn an_unknown_frame_or_snapshot_resolves_to_nothing() {
		let ir = request();
		assert!(ir.frame(FrameId::new(0)).is_some());
		assert!(ir.frame(FrameId::new(9)).is_none());
		assert_eq!(ir.snapshot(SnapshotId::new(0)), Some(&Snapshot::Current));
		assert!(ir.snapshot(SnapshotId::new(9)).is_none());
	}

	#[test]
	fn interning_returns_the_id_of_an_equal_entry() {
		let mut ir = request();
		let default = ir.frame(FrameId::new(0)).expect("the default frame exists").clone();
		assert_eq!(ir.intern_frame(default).ok(), Some(FrameId::new(0)));
		assert_eq!(ir.intern_snapshot(Snapshot::Current).ok(), Some(SnapshotId::new(0)));
		let other = AuthorityFrame {
			eval: EvalFrame::permission(None),
			unit: UnitFrame::unprincipled(),
		};
		let interned = ir.intern_frame(other.clone()).ok();
		assert_eq!(interned, Some(FrameId::new(1)));
		assert_eq!(ir.intern_frame(other).ok(), interned);
	}

	#[test]
	fn a_walk_of_an_empty_region_visits_nothing() {
		let ir = request();
		let mut seen = 0;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |_| {
			seen += 1;
			Walk::Continue
		});
		assert_eq!(seen, 0);
	}
}
