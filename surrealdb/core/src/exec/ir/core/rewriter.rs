//! The rewriter: the only API that mutates a container.
//!
//! Every method keeps the use-lists exact. `erase` fails while the op's results have uses and
//! `update_kind` fails when the closure changes the op's tag. Every mutation bumps the generation
//! of the touched op and of its ancestors, which makes their cached facts stale. An erased op
//! whose provenance holds a logical id is recorded as eliminated with its reason unless another
//! op's provenance holds that id. Placement entries are written through the physical dialect's
//! placement module, never through the rewriter.
//!
//! The rewriter enforces only what keeps the container addressable: exact use-lists, live ids,
//! in-range operand indices, table capacity, `erase` refusing a used op and `update_kind`
//! refusing a tag change. It does not check frames, snapshots, dominance, terminators, arity or
//! region kinds; the verifier does, so a container that breaks those rules can be built and
//! reported. A call that fails changes nothing.

use smallvec::SmallVec;

use super::block::BlockData;
use super::copy::{self, IdMap, StatementCopy};
use super::facts::{FactCache, OpFacts};
use super::ir::{Freed, Ir, held_ids};
use super::location::Location;
use super::op::OpData;
use super::region::RegionData;
use super::value::{TOKEN_SLOT_BASE, Use, ValueData, ValueDef};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::cx::structural_cx;
use crate::exec::ir::analysis::effects;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::conversion::{EliminationReason, Phase};
use crate::exec::ir::iface::fold::{FoldOutcome, FoldRequest};
use crate::exec::ir::iface::frame::{AuthorityFrame, Snapshot};
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, RegionProperties};
use crate::exec::ir::iface::types::{TokenClass, TypeKnowledge};
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::{OpKind, Terminator};
use crate::exec::ir::phys::ops::PhysOp;

/// The position an id or count names in a `u16`-indexed list.
///
/// Callers have checked the length against `u16::MAX`, so the saturation is never reached.
fn index_u16(at: usize) -> u16 {
	u16::try_from(at).unwrap_or(u16::MAX)
}

/// Whether `kind` ends a region by yielding its operands as the region's results.
fn is_yield_terminator(kind: &OpKind) -> bool {
	matches!(kind.terminator(), Some(Terminator::Yield))
		|| matches!(
			kind,
			OpKind::Expr(ExprOp::Yield)
				| OpKind::Logical(LogOp::Yield)
				| OpKind::Phys(PhysOp::Yield)
		)
}

/// Where a new op goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InsertPoint {
	/// Immediately before the op, in its block.
	Before(OpId),
	/// Immediately after the op, in its block.
	After(OpId),
	/// At the end of the block.
	End(BlockId),
}

/// The description of an op to create. The rewriter allocates one result value per entry of
/// `results`, typed by it.
#[derive(Clone, Debug)]
pub(crate) struct OpSpec {
	pub(crate) kind: OpKind,
	pub(crate) operands: SmallVec<[ValueId; 4]>,
	pub(crate) results: SmallVec<[TypeKnowledge; 2]>,
	pub(crate) frame: FrameId,
	pub(crate) snapshot: SnapshotId,
	pub(crate) loc: Location,
}

/// The logical ops an op stands for. A set: replacing an op unions the sets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Provenance {
	/// Built from the bound plan; stands for itself.
	Bound,
	/// Stands for these logical ops, sorted and deduplicated.
	From(SmallVec<[OpId; 2]>),
}

/// The provenance scope and re-made set a [`Rewriter::derived`] replaces while its body runs.
struct DerivedScope {
	provenance: Option<SmallVec<[OpId; 2]>>,
	remade: SmallVec<[OpId; 2]>,
}

/// A mutable view of one container and its fact cache.
pub(crate) struct Rewriter<'a> {
	ir: &'a mut Ir,
	facts: &'a mut FactCache,
	/// The function registry the facts a pattern reads are judged under, when the driver of the
	/// rewriter supplies one ([`Rewriter::with_registry`]).
	registry: Option<&'a FunctionRegistry>,
	/// The provenance every op created now records, while [`Rewriter::derived`] runs.
	provenance_scope: Option<SmallVec<[OpId; 2]>>,
	/// The ops the ops created now are derived from, while [`Rewriter::derived`] runs.
	remade_from: SmallVec<[OpId; 2]>,
	/// The ops created since the last [`Rewriter::take_created`].
	created: Vec<OpId>,
	/// The value replacements made while the log records; `None` while it is off.
	replacements: Option<Vec<(ValueId, ValueId)>>,
}

impl<'a> Rewriter<'a> {
	/// A rewriter over `ir`, reading and invalidating facts in `facts`.
	pub(crate) fn new(ir: &'a mut Ir, facts: &'a mut FactCache) -> Self {
		Self {
			ir,
			facts,
			registry: None,
			provenance_scope: None,
			remade_from: SmallVec::new(),
			created: Vec::new(),
			replacements: None,
		}
	}

	/// This rewriter judging the facts a pattern reads under `registry`. Without one, those facts
	/// are the top of every lattice.
	pub(crate) fn with_registry(mut self, registry: &'a FunctionRegistry) -> Self {
		self.registry = Some(registry);
		self
	}

	/// The function registry the facts a pattern reads are judged under, if one was supplied.
	pub(crate) fn registry(&self) -> Option<&'a FunctionRegistry> {
		self.registry
	}

	/// The container, read-only.
	pub(crate) fn ir(&self) -> &Ir {
		self.ir
	}

	/// The facts of `op` under the rewriter's registry and no definitions, its regions and the
	/// closure bodies it invokes included. Patterns read facts only through this method.
	///
	/// The answer is assembled from the per-op memo, which files each op's regional answer under
	/// the context it was computed in, so an answer computed under a request's own context never
	/// stands in for this one. A dangling op, and any op of a rewriter that holds no registry,
	/// answers the top of every lattice.
	pub(crate) fn facts(&mut self, op: OpId) -> OpFacts {
		let Some(registry) = self.registry else {
			return self.facts.top().clone();
		};
		if self.ir.op(op).is_none() {
			return self.facts.top().clone();
		}
		OpFacts {
			effects: effects::of_with(self.ir, self.facts, &structural_cx(registry, self.ir), op),
		}
	}

	/// The folded outcome of `v`, or `None` while it is pending or was never requested.
	pub(crate) fn fold_outcome(&self, v: ValueId) -> Option<&FoldOutcome> {
		self.facts.fold_outcome(v)
	}

	/// Asks the driver to fold `req.value`. The caller reports that it did not rewrite the op,
	/// and reads the outcome on a later pass.
	pub(crate) fn request_fold(&mut self, req: FoldRequest) {
		self.facts.request_fold(req);
	}

	/// Creates the op `spec` describes at `at` and answers its id.
	///
	/// The op records the provenance set of the enclosing [`Rewriter::derived`] scope, and
	/// `Provenance::Bound` outside one. It is `Bound` to the innermost error scope enclosing `at`.
	/// A logical op joins the container's logical ids only when it is `Bound`: an op made under
	/// `derived` stands for the ops it lists, and no live op holds an id of its own for it.
	pub(crate) fn create(&mut self, at: InsertPoint, spec: OpSpec) -> Result<OpId, IrError> {
		let (block, index) = self.resolve(at)?;
		if spec.operands.iter().any(|operand| self.ir.values.get(*operand).is_none()) {
			return Err(IrError::Stale("operand"));
		}
		if spec.operands.len() > usize::from(u16::MAX) {
			return Err(IrError::Capacity("operands"));
		}
		if spec.results.len() > usize::from(u16::MAX) {
			return Err(IrError::Capacity("results"));
		}
		let OpSpec {
			kind,
			operands,
			results,
			frame,
			snapshot,
			loc,
		} = spec;
		let provenance = match &self.provenance_scope {
			Some(set) => Provenance::From(set.clone()),
			None => Provenance::Bound,
		};
		let bound_scope = match provenance {
			Provenance::Bound => Some(self.ir.error_scope_owner(block)),
			Provenance::From(_) => None,
		};
		let logical = matches!(kind, OpKind::Logical(_)) && matches!(provenance, Provenance::Bound);
		let remade =
			self.remade_from.iter().copied().find(|from| {
				self.ir.matches.holds(*from)
					&& self.ir.ops.get(*from).is_some_and(|held| {
						held.kind.tag() == kind.tag() && held.kind.attrs_eq(&kind)
					})
			});
		let op = self.ir.ops.insert(OpData {
			kind,
			operands,
			results: SmallVec::new(),
			regions: SmallVec::new(),
			frame,
			snapshot,
			loc,
			provenance,
			parent: block,
		});
		self.ir.note_created(op);
		for (at, ty) in results.into_iter().enumerate() {
			let value = self.ir.values.insert(ValueData {
				def: ValueDef::OpResult {
					op,
					index: index_u16(at),
				},
				ty,
				uses: SmallVec::new(),
				hint_name: None,
			});
			if let Some(data) = self.ir.ops.get_mut(op) {
				data.results.push(value);
			}
		}
		if let Some(data) = self.ir.ops.get(op) {
			for (at, operand) in data.operands.iter().enumerate() {
				if let Some(value) = self.ir.values.get_mut(*operand) {
					value.uses.push(Use {
						user: op,
						operand: index_u16(at),
					});
				}
			}
		}
		if let Some(held) = self.ir.blocks.get_mut(block) {
			held.ops.insert(index, op);
		}
		let held: SmallVec<[OpId; 2]> = match self.ir.ops.get(op) {
			Some(data) => held_ids(&data.provenance, op).collect(),
			None => SmallVec::new(),
		};
		for id in held {
			self.ir.hold(id);
		}
		if logical {
			self.ir.logical_ids.push(op);
		}
		if let Some(scope) = bound_scope {
			self.ir.bind_scope.insert(op, scope);
		}
		if let Some(from) = remade {
			self.ir.matches.copy_entry(from, op);
		}
		self.created.push(op);
		self.ir.bump(op);
		self.ir.bump_enclosing(block);
		Ok(op)
	}

	/// Creates a region of `kind` owned by `owner`, with one block whose arguments are typed by
	/// `args`. The block holds no op; the caller closes it.
	pub(crate) fn create_region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
	) -> Result<(RegionId, BlockId), IrError> {
		if self.ir.ops.get(owner).is_none() {
			return Err(IrError::Stale("region owner"));
		}
		if args.len() > usize::from(u16::MAX) {
			return Err(IrError::Capacity("block arguments"));
		}
		let region = self.ir.regions.insert(RegionData {
			kind,
			blocks: SmallVec::new(),
			parent: Some(owner),
		});
		let block = self.ir.blocks.insert(BlockData {
			args: SmallVec::new(),
			token_args: SmallVec::new(),
			ops: Vec::new(),
			parent: region,
		});
		for (at, ty) in args.iter().enumerate() {
			let value = self.ir.values.insert(ValueData {
				def: ValueDef::BlockArg {
					block,
					index: index_u16(at),
				},
				ty: ty.clone(),
				uses: SmallVec::new(),
				hint_name: None,
			});
			if let Some(data) = self.ir.blocks.get_mut(block) {
				data.args.push(value);
			}
		}
		if let Some(data) = self.ir.regions.get_mut(region) {
			data.blocks.push(block);
		}
		if let Some(data) = self.ir.ops.get_mut(owner) {
			data.regions.push(region);
		}
		self.ir.bump_with_ancestors(owner);
		Ok((region, block))
	}

	/// Gives `op` the token `value` as its next token operand and answers its slot.
	///
	/// The tokens an op takes are held apart from its data operands and are not identity, so the
	/// generation of the op does not move.
	pub(crate) fn add_token_operand(&mut self, op: OpId, value: ValueId) -> Result<(), IrError> {
		if self.ir.ops.get(op).is_none() {
			return Err(IrError::Stale("op"));
		}
		if self.ir.token_class(value).is_none() {
			return Err(IrError::Refused("token operand"));
		}
		let tokens = self.ir.op_tokens.entry(op).ok_or(IrError::Stale("op"))?.or_default();
		let slot = tokens.operands.len();
		if slot >= usize::from(u16::MAX - TOKEN_SLOT_BASE) {
			return Err(IrError::Capacity("token operands"));
		}
		tokens.operands.push(value);
		if let Some(held) = self.ir.values.get_mut(value) {
			held.uses.push(Use {
				user: op,
				operand: TOKEN_SLOT_BASE + index_u16(slot),
			});
		}
		Ok(())
	}

	/// Gives `op` a new token of `class` as its next token result and answers it.
	pub(crate) fn add_token_result(
		&mut self,
		op: OpId,
		class: TokenClass,
	) -> Result<ValueId, IrError> {
		if self.ir.ops.get(op).is_none() {
			return Err(IrError::Stale("op"));
		}
		let tokens = self.ir.op_tokens.entry(op).ok_or(IrError::Stale("op"))?.or_default();
		let slot = tokens.results.len();
		if slot >= usize::from(u16::MAX - TOKEN_SLOT_BASE) {
			return Err(IrError::Capacity("token results"));
		}
		let value = self.ir.values.insert(ValueData {
			def: ValueDef::OpResult {
				op,
				index: TOKEN_SLOT_BASE + index_u16(slot),
			},
			ty: TypeKnowledge::Token(class),
			uses: SmallVec::new(),
			hint_name: None,
		});
		if let Some(tokens) = self.ir.op_tokens.get_mut(op) {
			tokens.results.push(value);
		}
		Ok(value)
	}

	/// Gives `block` a new token of `class` as its next token argument and answers it.
	pub(crate) fn add_token_arg(
		&mut self,
		block: BlockId,
		class: TokenClass,
	) -> Result<ValueId, IrError> {
		let slot = self.ir.blocks.get(block).ok_or(IrError::Stale("block"))?.token_args.len();
		if slot >= usize::from(u16::MAX - TOKEN_SLOT_BASE) {
			return Err(IrError::Capacity("token arguments"));
		}
		let value = self.ir.values.insert(ValueData {
			def: ValueDef::BlockArg {
				block,
				index: TOKEN_SLOT_BASE + index_u16(slot),
			},
			ty: TypeKnowledge::Token(class),
			uses: SmallVec::new(),
			hint_name: None,
		});
		if let Some(held) = self.ir.blocks.get_mut(block) {
			held.token_args.push(value);
		}
		Ok(value)
	}

	/// Records that `op` has been threaded: it holds the tokens its effects call for.
	pub(crate) fn mark_threaded(&mut self, op: OpId) {
		self.ir.untokened.remove(op);
	}

	/// States whether effect tokens are threaded through the container
	/// ([`Ir::is_threaded`](super::ir::Ir)).
	pub(crate) fn set_threaded(&mut self, threaded: bool) {
		self.ir.threaded = threaded;
		if !threaded {
			self.ir.untokened.clear();
		}
	}

	/// Removes every token of the ops and blocks at and beneath `region`: the token operands, the
	/// token results and the token block arguments. A token is defined and read within one
	/// threaded region, so no use outside it is left behind. In a threaded container the ops hold
	/// no tokens afterwards ([`Ir::holds_no_tokens`](super::ir::Ir)) until they are threaded
	/// again.
	pub(crate) fn strip_tokens(&mut self, region: RegionId) {
		let mut ops: Vec<OpId> = Vec::new();
		let mut blocks: Vec<BlockId> = Vec::new();
		self.collect_scope(region, &mut ops, &mut blocks);
		for op in &ops {
			self.drop_token_operand_uses(*op);
		}
		for op in &ops {
			self.ir.note_created(*op);
			let Some(tokens) = self.ir.op_tokens.remove(*op) else {
				continue;
			};
			for value in &tokens.results {
				self.ir.values.remove(*value);
				self.facts.invalidate_value(*value);
			}
		}
		for block in &blocks {
			let args = match self.ir.blocks.get_mut(*block) {
				Some(held) => ::core::mem::take(&mut held.token_args),
				None => continue,
			};
			for value in &args {
				self.ir.values.remove(*value);
				self.facts.invalidate_value(*value);
			}
		}
	}

	/// Every op and block at and beneath `region`.
	fn collect_scope(&self, region: RegionId, ops: &mut Vec<OpId>, blocks: &mut Vec<BlockId>) {
		let Some(data) = self.ir.regions.get(region) else {
			return;
		};
		for block in &data.blocks {
			blocks.push(*block);
			for op in self.ir.blocks.get(*block).map_or(&[][..], |held| &held.ops) {
				ops.push(*op);
				for nested in self.ir.ops.get(*op).map_or(&[][..], |held| &held.regions) {
					self.collect_scope(*nested, ops, blocks);
				}
			}
		}
	}

	/// Removes the uses `op`'s token operands hold on their values, and the operands.
	fn drop_token_operand_uses(&mut self, op: OpId) {
		let Some(tokens) = self.ir.op_tokens.get_mut(op) else {
			return;
		};
		let operands = ::core::mem::take(&mut tokens.operands);
		for (slot, operand) in operands.iter().enumerate() {
			let Some(value) = self.ir.values.get_mut(*operand) else {
				continue;
			};
			let at = TOKEN_SLOT_BASE + index_u16(slot);
			if let Some(held) = value.uses.iter().position(|u| u.user == op && u.operand == at) {
				value.uses.remove(held);
			}
		}
	}

	/// The token `op` takes of the class of its token result `result`.
	fn incoming_of(&self, op: OpId, result: ValueId) -> Option<ValueId> {
		let class = self.ir.token_class(result)?;
		self.ir.token_of_class(self.ir.token_operands(op), class)
	}

	/// Fails when a token `op` produces is read and `op` takes no token of its class to stand in
	/// for it. An op takes the class of every token it produces, so this holds for a well-formed
	/// container.
	fn check_token_results_forwardable(&self, op: OpId) -> Result<(), IrError> {
		for result in self.ir.token_results(op) {
			if !self.ir.uses(*result).is_empty() && self.incoming_of(op, *result).is_none() {
				return Err(IrError::HasUses(op));
			}
		}
		Ok(())
	}

	/// Makes every read of a token `op` produces read the token `op` takes of that class, so
	/// erasing or replacing `op` leaves the chain of every class unbroken.
	fn forward_token_results(&mut self, op: OpId) -> Result<(), IrError> {
		let results: SmallVec<[ValueId; 2]> = self.ir.token_results(op).iter().copied().collect();
		for result in results {
			if self.ir.uses(result).is_empty() {
				continue;
			}
			let incoming = self.incoming_of(op, result).ok_or(IrError::HasUses(op))?;
			self.replace_all_uses(result, incoming)?;
		}
		Ok(())
	}

	/// Makes the `index`th operand of `op` read `to`. Reading the value it already reads changes
	/// nothing.
	pub(crate) fn set_operand(&mut self, op: OpId, index: u16, to: ValueId) -> Result<(), IrError> {
		let data = self.ir.ops.get(op).ok_or(IrError::Stale("op"))?;
		let from = *data.operands.get(usize::from(index)).ok_or(IrError::Stale("operand index"))?;
		if self.ir.values.get(to).is_none() {
			return Err(IrError::Stale("operand"));
		}
		if from == to {
			return Ok(());
		}
		if let Some(value) = self.ir.values.get_mut(from)
			&& let Some(at) =
				value.uses.iter().position(|held| held.user == op && held.operand == index)
		{
			value.uses.remove(at);
		}
		if let Some(value) = self.ir.values.get_mut(to) {
			value.uses.push(Use {
				user: op,
				operand: index,
			});
		}
		if let Some(slot) =
			self.ir.ops.get_mut(op).and_then(|data| data.operands.get_mut(usize::from(index)))
		{
			*slot = to;
		}
		self.ir.bump_with_ancestors(op);
		Ok(())
	}

	/// Makes every use of `from` read `to`. The uses join the end of `to`'s list in the order
	/// they stood in `from`'s. Replacing a value by itself changes nothing. While the log records
	/// (see [`Rewriter::record_replacements`]), a replacement of one value by another is appended
	/// to it, even when `from` has no use.
	pub(crate) fn replace_all_uses(&mut self, from: ValueId, to: ValueId) -> Result<(), IrError> {
		if self.ir.values.get(from).is_none() || self.ir.values.get(to).is_none() {
			return Err(IrError::Stale("value"));
		}
		if from == to {
			return Ok(());
		}
		let moved = match self.ir.values.get_mut(from) {
			Some(value) => ::core::mem::take(&mut value.uses),
			None => SmallVec::new(),
		};
		for held in &moved {
			if let Some(slot) = held.token_slot() {
				if let Some(operand) = self
					.ir
					.op_tokens
					.get_mut(held.user)
					.and_then(|tokens| tokens.operands.get_mut(slot))
				{
					*operand = to;
				}
				continue;
			}
			if let Some(data) = self.ir.ops.get_mut(held.user)
				&& let Some(operand) = data.operands.get_mut(usize::from(held.operand))
			{
				*operand = to;
			}
			self.ir.bump_with_ancestors(held.user);
		}
		if let Some(value) = self.ir.values.get_mut(to) {
			value.uses.extend(moved);
		}
		self.retarget_snapshots(from, to);
		if let Some(log) = &mut self.replacements {
			log.push((from, to));
		}
		Ok(())
	}

	/// Makes every snapshot that names `from` name `to`, and bumps the ops reading it.
	///
	/// A snapshot's version value is held by the snapshot table, outside every use list, so a
	/// replacement must rewrite the table or the entry would name a value that is gone. The table
	/// may then hold equal entries; ids stay stable and equal ids still hold equal snapshots.
	fn retarget_snapshots(&mut self, from: ValueId, to: ValueId) {
		let mut moved: SmallVec<[SnapshotId; 2]> = SmallVec::new();
		for (at, held) in self.ir.snapshots.iter_mut().enumerate() {
			if *held == Snapshot::Version(from) {
				*held = Snapshot::Version(to);
				if let Ok(at) = u16::try_from(at) {
					moved.push(SnapshotId::new(at));
				}
			}
		}
		if moved.is_empty() {
			return;
		}
		let readers: Vec<OpId> = self
			.ir
			.ops
			.iter()
			.filter(|(_, data)| moved.contains(&data.snapshot))
			.map(|(id, _)| id)
			.collect();
		for reader in readers {
			self.ir.bump_with_ancestors(reader);
		}
	}

	/// Replaces the results of `op` by `with` and erases `op`.
	///
	/// `with` holds one value per result of `op`, none of them defined by `op` or inside its
	/// regions. The op defining each `with` value unions the logical ids `op` stands for into its
	/// own provenance, a `Bound` defining op contributing its own id, so no logical id is lost.
	/// An id that no other op holds afterwards, because every `with` value is a block argument or
	/// there is none, is recorded as eliminated: `MergedInto` the op owning the first `with`
	/// value's block, or `DeadPure` when that value has no owner. The ops of `op`'s regions are
	/// erased with it under the same reason. The call fails before changing anything when `with`
	/// does not match `op`'s results, names a dangling value or a value `op` defines, or when a
	/// nested op's result is read outside `op`.
	pub(crate) fn replace_op(&mut self, op: OpId, with: &[ValueId]) -> Result<(), IrError> {
		let data = self.ir.ops.get(op).ok_or(IrError::Stale("op"))?;
		if with.len() != data.results.len() {
			return Err(IrError::Refused("replace_op arity"));
		}
		let results = data.results.clone();
		let parent = data.parent;
		for value in with {
			if self.ir.values.get(*value).is_none() {
				return Err(IrError::Stale("value"));
			}
			if self.ir.value_inside_op(*value, op) {
				return Err(IrError::Refused("replace_op inside"));
			}
		}
		let subtree = self.erasable_subtree(op, true)?;
		self.check_token_results_forwardable(op)?;
		let ids: SmallVec<[OpId; 2]> = held_ids(&data.provenance, op).collect();
		let reason = with
			.iter()
			.find_map(|value| self.ir.value_owner(*value))
			.map_or(EliminationReason::DeadPure, EliminationReason::MergedInto);
		for (result, value) in results.iter().zip(with) {
			self.replace_all_uses(*result, *value)?;
		}
		self.forward_token_results(op)?;
		for value in with {
			self.merge_provenance_into_definer(*value, &ids);
		}
		for member in &subtree {
			self.drop_operand_uses(*member);
		}
		for member in &subtree {
			self.free_op(*member, reason, *member == op);
		}
		self.ir.bump_enclosing(parent);
		Ok(())
	}

	/// Erases `op`, which must have no used results, and records `reason`.
	///
	/// The ops of its regions are erased with it, and the uses they hold on outer values are
	/// dropped. Each logical id the erased ops stood for that no remaining op holds is recorded
	/// as eliminated with `reason`. An op of a region whose result is read outside the erased
	/// subtree answers [`IrError::HasUses`] for that op, and nothing changes.
	pub(crate) fn erase(&mut self, op: OpId, reason: EliminationReason) -> Result<(), IrError> {
		let parent = self.ir.ops.get(op).ok_or(IrError::Stale("op"))?.parent;
		let subtree = self.erasable_subtree(op, false)?;
		self.check_token_results_forwardable(op)?;
		self.forward_token_results(op)?;
		for member in &subtree {
			self.drop_operand_uses(*member);
		}
		for member in &subtree {
			self.free_op(*member, reason, *member == op);
		}
		self.ir.bump_enclosing(parent);
		Ok(())
	}

	/// Erases `region` with the ops it holds, and records `reason` for each logical id they stood
	/// for that no remaining op holds. The owner keeps its other regions, and holds fewer than its
	/// kind names where the region is not one the kind makes optional; the caller says the owner
	/// may. The call fails before changing anything when `region` is the top region, or when the
	/// result of an op in it is read outside it. A region isolated from above reads no value
	/// defined outside it that its erasure leaves dangling: the uses its ops hold are dropped.
	pub(crate) fn erase_region(
		&mut self,
		region: RegionId,
		reason: EliminationReason,
	) -> Result<(), IrError> {
		let data = self.ir.regions.get(region).ok_or(IrError::Stale("region"))?;
		let owner = data.parent.ok_or(IrError::Refused("erase_region top"))?;
		let blocks = data.blocks.clone();
		let mut members: SmallVec<[OpId; 8]> = SmallVec::new();
		for block in &blocks {
			for child in self.ir.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
				self.collect_post_order(*child, &mut members);
			}
		}
		if let Some(pinned) = self.version_read_outside(&members, None) {
			return Err(IrError::HasUses(pinned));
		}
		let held: FxHashSet<OpId> = members.iter().copied().collect();
		for member in &members {
			let used_outside = self.ir.ops.get(*member).is_some_and(|data| {
				data.results.iter().any(|result| {
					self.ir.uses(*result).iter().any(|use_| !held.contains(&use_.user))
				})
			});
			if used_outside {
				return Err(IrError::HasUses(*member));
			}
		}
		for member in &members {
			self.drop_operand_uses(*member);
		}
		for member in &members {
			self.free_op(*member, reason, false);
		}
		for block in &blocks {
			let Some(block) = self.ir.blocks.remove(*block) else {
				continue;
			};
			for arg in block.args.iter().chain(&block.token_args) {
				self.ir.values.remove(*arg);
				self.facts.invalidate_value(*arg);
			}
		}
		self.ir.sealed_regions.remove(region);
		self.ir.regions.remove(region);
		if let Some(data) = self.ir.ops.get_mut(owner) {
			data.regions.retain(|held| *held != region);
		}
		self.ir.bump_with_ancestors(owner);
		Ok(())
	}

	/// Moves the ops of `region` to `at`, binding its block arguments to `args`, and answers the
	/// values its terminator yielded.
	///
	/// Every op but the terminator keeps its order, provenance and location. The terminator is
	/// erased, and its id is recorded as eliminated with `DeadPure` only when no other op holds
	/// it. The region leaves its owner, which then holds fewer regions than its kind names; the
	/// caller replaces the owner. The call fails before changing anything when `region` is the
	/// top region or does not hold one block ending in a yield, `args` does not match the block's
	/// arguments or names a dangling value or one defined inside `region`, `at` lies inside
	/// `region`, or the yield's results are used.
	///
	/// A token the region's ops read is replaced by the token the owner takes of its class. When
	/// the region runs after the owner's effect ([`effects::follows_effect`]) it is replaced by
	/// the token the owner produces instead, so the reads still see the effect; the call is
	/// refused when that token is not visible at `at`, which a point before the owner never
	/// sees. It is also refused when the owner holds no token of the class.
	pub(crate) fn inline_region(
		&mut self,
		region: RegionId,
		at: InsertPoint,
		args: &[ValueId],
	) -> Result<SmallVec<[ValueId; 2]>, IrError> {
		let data = self.ir.regions.get(region).ok_or(IrError::Stale("region"))?;
		let owner = data.parent.ok_or(IrError::Refused("inline_region top"))?;
		let [source] = data.blocks.as_slice() else {
			return Err(IrError::Refused("inline_region terminator"));
		};
		let source = *source;
		let block = self.ir.blocks.get(source).ok_or(IrError::Stale("block"))?;
		if args.len() != block.args.len() {
			return Err(IrError::Refused("inline_region arity"));
		}
		let terminator = block
			.ops
			.last()
			.copied()
			.filter(|last| {
				self.ir.ops.get(*last).is_some_and(|data| is_yield_terminator(&data.kind))
			})
			.ok_or(IrError::Refused("inline_region terminator"))?;
		for value in args {
			let Some(region_of) = self.ir.value_region(*value) else {
				return Err(IrError::Stale("value"));
			};
			if self.ir.region_encloses(region, region_of) {
				return Err(IrError::Refused("inline_region argument"));
			}
		}
		let (dest, index) = self.resolve(at)?;
		let dest_region = self.ir.blocks.get(dest).map(|block| block.parent);
		if dest_region.is_some_and(|inner| self.ir.region_encloses(region, inner)) {
			return Err(IrError::Refused("inline_region insert point"));
		}
		let used = self.ir.ops.get(terminator).is_some_and(|data| {
			data.results.iter().any(|result| !self.ir.uses(*result).is_empty())
		});
		if used {
			return Err(IrError::HasUses(terminator));
		}
		let formals = block.args.clone();
		let token_formals = block.token_args.clone();
		let follows =
			self.ir.ops.get(owner).is_some_and(|data| effects::follows_effect(&data.kind));
		let mut standing_in: SmallVec<[(ValueId, ValueId); 2]> = SmallVec::new();
		for formal in &token_formals {
			if self.ir.uses(*formal).is_empty() {
				continue;
			}
			let class =
				self.ir.token_class(*formal).ok_or(IrError::Refused("inline_region token"))?;
			let stands_in = match self.ir.token_of_class(self.ir.token_results(owner), class) {
				Some(produced) if follows => {
					if !self.visible_at(produced, dest, index) {
						return Err(IrError::Refused("inline_region token"));
					}
					produced
				}
				_ => self
					.ir
					.token_of_class(self.ir.token_operands(owner), class)
					.ok_or(IrError::Refused("inline_region token"))?,
			};
			standing_in.push((*formal, stands_in));
		}
		for (formal, actual) in formals.iter().zip(args) {
			self.replace_all_uses(*formal, *actual)?;
		}
		for (formal, incoming) in standing_in {
			self.replace_all_uses(formal, incoming)?;
		}
		let yielded: SmallVec<[ValueId; 2]> = match self.ir.ops.get(terminator) {
			Some(data) => data.operands.iter().copied().collect(),
			None => return Err(IrError::Refused("inline_region terminator")),
		};
		let mut moved = match self.ir.blocks.get_mut(source) {
			Some(block) => ::core::mem::take(&mut block.ops),
			None => return Err(IrError::Stale("block")),
		};
		moved.pop();
		if let Some(block) = self.ir.blocks.get_mut(source) {
			block.ops.push(terminator);
		}
		for op in &moved {
			if let Some(data) = self.ir.ops.get_mut(*op) {
				data.parent = dest;
			}
		}
		if let Some(block) = self.ir.blocks.get_mut(dest) {
			block.ops.splice(index..index, moved.iter().copied());
		}
		self.drop_operand_uses(terminator);
		self.free_op(terminator, EliminationReason::DeadPure, true);
		if let Some(data) = self.ir.ops.get_mut(owner) {
			data.regions.retain(|held| *held != region);
		}
		self.ir.regions.remove(region);
		self.ir.blocks.remove(source);
		for formal in formals.iter().chain(&token_formals) {
			self.ir.values.remove(*formal);
			self.facts.invalidate_value(*formal);
		}
		for op in &moved {
			self.ir.bump(*op);
		}
		self.ir.bump_enclosing(dest);
		self.ir.bump_with_ancestors(owner);
		Ok(yielded)
	}

	/// Whether `value` is visible to an op inserted at position `index` of `block`: it is an
	/// argument of an enclosing block, or a result of an op that stands before the position, or
	/// before an op enclosing it, in an enclosing block. A value outside a region isolated from
	/// above is not visible inside it.
	fn visible_at(&self, value: ValueId, block: BlockId, index: usize) -> bool {
		let Some(held) = self.ir.blocks.get(block) else {
			return false;
		};
		if let Some(next) = held.ops.get(index) {
			return self.ir.dominates(value, *next);
		}
		match self.ir.values.get(value).map(|data| data.def) {
			Some(ValueDef::OpResult {
				op,
				..
			}) if self.ir.ops.get(op).is_some_and(|data| data.parent == block) => true,
			Some(ValueDef::BlockArg {
				block: defined,
				..
			}) if defined == block => true,
			Some(_) => self.ir.regions.get(held.parent).is_some_and(|region| {
				!region.kind.isolated_from_above()
					&& region.parent.is_some_and(|enclosing| self.ir.dominates(value, enclosing))
			}),
			None => false,
		}
	}

	/// Makes `new_owner` the owner of `region`, appended after the regions it already owns.
	///
	/// Fails with `Refused("move_region cycle")` when `new_owner` stands inside `region`, which
	/// would leave the region under itself, and with `Stale("region")` for the top region.
	pub(crate) fn move_region(&mut self, region: RegionId, new_owner: OpId) -> Result<(), IrError> {
		let data = self.ir.regions.get(region).ok_or(IrError::Stale("region"))?;
		let old_owner = data.parent.ok_or(IrError::Stale("region"))?;
		if self.ir.ops.get(new_owner).is_none() {
			return Err(IrError::Stale("region owner"));
		}
		if self.ir.region_encloses(region, self.ir.enclosing_region(new_owner)) {
			return Err(IrError::Refused("move_region cycle"));
		}
		if let Some(owner) = self.ir.ops.get_mut(old_owner) {
			owner.regions.retain(|held| *held != region);
		}
		if let Some(owner) = self.ir.ops.get_mut(new_owner) {
			owner.regions.push(region);
		}
		if let Some(data) = self.ir.regions.get_mut(region) {
			data.parent = Some(new_owner);
		}
		self.ir.bump_with_ancestors(old_owner);
		self.ir.bump_with_ancestors(new_owner);
		Ok(())
	}

	/// Clones `region` of the container `src` as a new region of this one, owned by `owner`, and
	/// answers it with the correspondence of ids, `src` being the original.
	///
	/// Every cloned op is `Bound` here and keeps its location. Its frame and snapshot are
	/// interned into this container's tables, a snapshot over the value that stands for its
	/// version. Once this container has left [`Phase::Bound`], the cloned ops inside a
	/// stored-clause region are sealed at their generation. Nothing of `src` is referred to
	/// afterwards. The call fails when `owner` or `region` is dangling, when a cloned op reads a
	/// value defined outside `region`, and when a table is full; the tables may then hold the
	/// entries interned so far, and nothing else changes.
	pub(crate) fn clone_region_from(
		&mut self,
		src: &Ir,
		region: RegionId,
		owner: OpId,
	) -> Result<(RegionId, IdMap), IrError> {
		let (cloned, map, created) = copy::clone_region(self.ir, src, region, owner)?;
		self.created.extend(created);
		Ok((cloned, map))
	}

	/// Replaces the code of the statement that `copy` was made of with the code of `from`.
	/// `copy` is the value [`Ir::copy_statement`] answered. The replacement is the one
	/// [`Ir::adopt_statement`] makes, and the cached facts of every op and value it replaces
	/// are dropped.
	pub(crate) fn adopt_statement(
		&mut self,
		from: Ir,
		copy: &StatementCopy,
	) -> Result<(), IrError> {
		let facts = &mut *self.facts;
		self.ir.adopt_statement_with(from, copy, &mut |freed| match freed {
			Freed::Op(op) => facts.invalidate(op),
			Freed::Value(value) => facts.invalidate_value(value),
		})
	}

	/// Gives the op `to`, a copy of `from`, the decisions the physical plan made for `from`'s
	/// `MATCHES` operator, `KNN` operator or index function call. Like placement, they are not
	/// identity.
	pub(crate) fn carry_decisions(&mut self, from: OpId, to: OpId) {
		self.ir.matches.copy_entry(from, to);
	}

	/// Changes the attributes of `op` through `f`. Fails with [`IrError::TagChanged`] when `f`
	/// changes the op's tag, and the op keeps the kind it had.
	pub(crate) fn update_kind(
		&mut self,
		op: OpId,
		f: impl FnOnce(&mut OpKind),
	) -> Result<(), IrError> {
		let data = self.ir.ops.get_mut(op).ok_or(IrError::Stale("op"))?;
		let before = data.kind.clone();
		f(&mut data.kind);
		if data.kind.tag() != before.tag() {
			data.kind = before;
			return Err(IrError::TagChanged(op));
		}
		self.ir.bump_with_ancestors(op);
		Ok(())
	}

	/// Gives `op` the location `loc`. A location never enters identity, so no generation moves and
	/// no fact is invalidated.
	pub(crate) fn set_location(&mut self, op: OpId, loc: Location) -> Result<(), IrError> {
		self.ir.ops.get_mut(op).ok_or(IrError::Stale("op"))?.loc = loc;
		Ok(())
	}

	/// Names `value` for a reader of the container. A name is a label: it never enters identity.
	pub(crate) fn set_hint_name(
		&mut self,
		value: ValueId,
		name: surrealdb_strand::Strand,
	) -> Result<(), IrError> {
		self.ir.values.get_mut(value).ok_or(IrError::Stale("value"))?.hint_name = Some(name);
		Ok(())
	}

	/// The id of `frame` in the container's frame table, adding it when absent.
	pub(crate) fn intern_frame(&mut self, frame: AuthorityFrame) -> Result<FrameId, IrError> {
		self.ir.intern_frame(frame)
	}

	/// The id of `snapshot` in the container's snapshot table, adding it when absent.
	pub(crate) fn intern_snapshot(&mut self, snapshot: Snapshot) -> Result<SnapshotId, IrError> {
		self.ir.intern_snapshot(snapshot)
	}

	/// Runs `f`; every op it creates records the provenance of the ops in `from`.
	///
	/// A `Bound` op in `from` contributes its own id, a listed op the ids it stands for, and a
	/// dangling id itself. A call inside another `derived` adds to the outer set.
	///
	/// An op `f` creates with the kind and attributes of an op of `from` is that op re-made: it
	/// takes the decisions the physical plan made for the first such op that holds any (a `MATCHES`
	/// operator's registration, a `KNN` operator's search scope, an index function call's scope),
	/// which are keyed by op and not part of identity.
	///
	/// Only the call of `f` is generic: the scope is entered and left by two non-generic
	/// functions, so every closure shares one copy of the bookkeeping.
	pub(crate) fn derived<R>(&mut self, from: &[OpId], f: impl FnOnce(&mut Self) -> R) -> R {
		let outer = self.enter_derived(from);
		let result = f(self);
		self.leave_derived(outer);
		result
	}

	/// Opens the scope of a [`Rewriter::derived`] over `from` and answers the scope it replaces.
	#[inline(never)]
	fn enter_derived(&mut self, from: &[OpId]) -> DerivedScope {
		let remade = std::mem::replace(&mut self.remade_from, from.iter().copied().collect());
		let provenance = self.provenance_scope.take();
		let mut set: SmallVec<[OpId; 2]> = provenance.clone().unwrap_or_default();
		for id in from {
			match self.ir.ops.get(*id) {
				Some(data) => set.extend(held_ids(&data.provenance, *id)),
				None => set.push(*id),
			}
		}
		set.sort_unstable();
		set.dedup();
		self.provenance_scope = Some(set);
		DerivedScope {
			provenance,
			remade,
		}
	}

	/// Closes the scope [`Rewriter::enter_derived`] opened, restoring `outer`.
	#[inline(never)]
	fn leave_derived(&mut self, outer: DerivedScope) {
		self.provenance_scope = outer.provenance;
		self.remade_from = outer.remade;
	}

	/// Moves the container forward to `to`. The only operation that changes a container's
	/// phase; a statement copy starts at the phase of the container it was copied from.
	///
	/// The phase never moves back: a smaller `to` answers `Refused("phase regress")`. Leaving
	/// [`Phase::Bound`] records the generation of every op inside a stored-clause region and the
	/// op count of every such region, which the verifier compares against later. The caller
	/// verifies the boundary; this never does.
	pub(crate) fn advance_phase(&mut self, to: Phase) -> Result<(), IrError> {
		if to < self.ir.phase {
			return Err(IrError::Refused("phase regress"));
		}
		if self.ir.phase == Phase::Bound && to > Phase::Bound {
			let mut stored = Vec::new();
			self.collect_stored(self.ir.top, false, &mut stored);
			for op in stored {
				let generation = self.ir.generation(op);
				self.ir.sealed.insert(op, generation);
			}
			let mut regions = Vec::new();
			self.ir.collect_stored_regions(self.ir.top, false, &mut regions);
			for region in regions {
				let count = self.ir.region_op_count(region);
				self.ir.sealed_regions.insert(region, count);
			}
		}
		self.ir.phase = to;
		Ok(())
	}

	/// The ops created since the last call.
	pub(crate) fn take_created(&mut self) -> Vec<OpId> {
		::core::mem::take(&mut self.created)
	}

	/// Starts logging every [`Rewriter::replace_all_uses`] as a `(from, to)` pair, which
	/// [`Rewriter::replace_op`] and [`Rewriter::inline_region`] make too. A log already
	/// recording keeps its entries.
	pub(crate) fn record_replacements(&mut self) {
		self.replacements.get_or_insert_with(Vec::new);
	}

	/// The replacements logged since [`Rewriter::record_replacements`], in the order they were
	/// made, and stops logging. Answers no pairs when the log was not recording.
	pub(crate) fn take_replacements(&mut self) -> Vec<(ValueId, ValueId)> {
		self.replacements.take().unwrap_or_default()
	}

	/// The block and position `at` names.
	fn resolve(&self, at: InsertPoint) -> Result<(BlockId, usize), IrError> {
		let beside = |op: OpId, after: usize| {
			let data = self.ir.ops.get(op).ok_or(IrError::Stale("insert point"))?;
			let block = self.ir.blocks.get(data.parent).ok_or(IrError::Stale("insert point"))?;
			let at = block
				.ops
				.iter()
				.position(|held| *held == op)
				.ok_or(IrError::Stale("insert point"))?;
			Ok((data.parent, at + after))
		};
		match at {
			InsertPoint::Before(op) => beside(op, 0),
			InsertPoint::After(op) => beside(op, 1),
			InsertPoint::End(block) => {
				let held = self.ir.blocks.get(block).ok_or(IrError::Stale("block"))?;
				Ok((block, held.ops.len()))
			}
		}
	}

	/// The ops an erase of `op` removes, each after the ops of its regions and `op` last.
	///
	/// Answers [`IrError::HasUses`] for the first op whose result is read by an op outside the
	/// subtree. `replaced` says the uses of `op`'s own results are moved elsewhere first, so
	/// they do not count.
	fn erasable_subtree(&self, op: OpId, replaced: bool) -> Result<SmallVec<[OpId; 8]>, IrError> {
		let data = self.ir.ops.get(op).ok_or(IrError::Stale("op"))?;
		let mut subtree: SmallVec<[OpId; 8]> = SmallVec::new();
		if data.regions.is_empty() {
			subtree.push(op);
			let used = data.results.iter().any(|result| !self.ir.uses(*result).is_empty());
			if used && !replaced {
				return Err(IrError::HasUses(op));
			}
			if let Some(pinned) = self.version_read_outside(&subtree, replaced.then_some(op)) {
				return Err(IrError::HasUses(pinned));
			}
			return Ok(subtree);
		}
		self.collect_post_order(op, &mut subtree);
		if let Some(pinned) = self.version_read_outside(&subtree, replaced.then_some(op)) {
			return Err(IrError::HasUses(pinned));
		}
		let members: FxHashSet<OpId> = subtree.iter().copied().collect();
		for member in &subtree {
			if replaced && *member == op {
				continue;
			}
			let used_outside = self.ir.ops.get(*member).is_some_and(|data| {
				data.results.iter().any(|result| {
					self.ir.uses(*result).iter().any(|held| !members.contains(&held.user))
				})
			});
			if used_outside {
				return Err(IrError::HasUses(*member));
			}
		}
		Ok(subtree)
	}

	/// The op of `members`, other than `skip`, whose result a snapshot names as its version while
	/// an op outside `members` reads that snapshot. Erasing it would leave the snapshot naming a
	/// value that is gone.
	fn version_read_outside(&self, members: &[OpId], skip: Option<OpId>) -> Option<OpId> {
		if self.ir.snapshots.len() <= 1 {
			return None;
		}
		let mut named: SmallVec<[(SnapshotId, OpId); 2]> = SmallVec::new();
		for (at, held) in self.ir.snapshots.iter().enumerate() {
			let Snapshot::Version(value) = held else {
				continue;
			};
			let Some(ValueDef::OpResult {
				op: definer,
				..
			}) = self.ir.values.get(*value).map(|data| data.def)
			else {
				continue;
			};
			if Some(definer) != skip
				&& members.contains(&definer)
				&& let Ok(at) = u16::try_from(at)
			{
				named.push((SnapshotId::new(at), definer));
			}
		}
		if named.is_empty() {
			return None;
		}
		self.ir.ops.iter().find_map(|(reader, data)| {
			let (_, definer) = named.iter().find(|(id, _)| *id == data.snapshot)?;
			(!members.contains(&reader)).then_some(*definer)
		})
	}

	/// Adds the ids in `ids` to the provenance of the op defining `value`, when an op defines
	/// it. A `Bound` definer keeps standing for itself; a definer that already holds every id
	/// is left as it is.
	fn merge_provenance_into_definer(&mut self, value: ValueId, ids: &[OpId]) {
		let Some(ValueDef::OpResult {
			op: definer,
			..
		}) = self.ir.values.get(value).map(|data| data.def)
		else {
			return;
		};
		let Some(data) = self.ir.ops.get(definer) else {
			return;
		};
		let held: SmallVec<[OpId; 2]> = held_ids(&data.provenance, definer).collect();
		let mut merged = held.clone();
		merged.extend(ids.iter().copied().filter(|id| !held.contains(id)));
		if merged.len() == held.len() {
			return;
		}
		for id in &merged[held.len()..] {
			self.ir.hold(*id);
		}
		merged.sort_unstable();
		merged.dedup();
		if let Some(data) = self.ir.ops.get_mut(definer) {
			data.provenance = Provenance::From(merged);
		}
		self.ir.bump_with_ancestors(definer);
	}

	/// Appends `op` and every op of its regions to `out`, each op after the ops of its regions.
	fn collect_post_order(&self, op: OpId, out: &mut SmallVec<[OpId; 8]>) {
		let Some(data) = self.ir.ops.get(op) else {
			return;
		};
		for region in &data.regions {
			let blocks = self.ir.regions.get(*region).map(|region| region.blocks.as_slice());
			for block in blocks.unwrap_or_default() {
				for child in self.ir.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
					self.collect_post_order(*child, out);
				}
			}
		}
		out.push(op);
	}

	/// Appends every op of `region` and of the regions beneath it that stands inside a
	/// stored-clause region; `inside` says `region` itself does.
	fn collect_stored(&self, region: RegionId, inside: bool, out: &mut Vec<OpId>) {
		let Some(data) = self.ir.regions.get(region) else {
			return;
		};
		let inside = inside || matches!(data.kind, RegionKind::StoredClause(_));
		for block in &data.blocks {
			for op in self.ir.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
				if inside {
					out.push(*op);
				}
				for nested in self.ir.ops.get(*op).map_or(&[][..], |op| &op.regions) {
					self.collect_stored(*nested, inside, out);
				}
			}
		}
	}

	/// Removes the uses `op`'s operands hold on their values.
	fn drop_operand_uses(&mut self, op: OpId) {
		let Some(data) = self.ir.ops.get(op) else {
			return;
		};
		for (at, operand) in data.operands.iter().enumerate() {
			let Some(value) = self.ir.values.get_mut(*operand) else {
				continue;
			};
			let at = index_u16(at);
			if let Some(held) = value.uses.iter().position(|u| u.user == op && u.operand == at) {
				value.uses.remove(held);
			}
		}
		let Some(tokens) = self.ir.op_tokens.get(op) else {
			return;
		};
		for (slot, operand) in tokens.operands.iter().enumerate() {
			let Some(value) = self.ir.values.get_mut(*operand) else {
				continue;
			};
			let at = TOKEN_SLOT_BASE + index_u16(slot);
			if let Some(held) = value.uses.iter().position(|u| u.user == op && u.operand == at) {
				value.uses.remove(held);
			}
		}
	}

	/// Frees `op`, its values, its regions and its blocks, and records the ids it stood for that
	/// no other op holds. `detach` also removes it from its block, which the caller sets for the
	/// root of an erased subtree only: the block of any other op is freed with its region.
	fn free_op(&mut self, op: OpId, reason: EliminationReason, detach: bool) {
		let facts = &mut *self.facts;
		self.ir.free_op(op, reason, detach, &mut |freed| match freed {
			Freed::Op(op) => facts.invalidate(op),
			Freed::Value(value) => facts.invalidate_value(value),
		});
	}
}
