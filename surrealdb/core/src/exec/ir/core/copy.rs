//! Moving ops between containers: a deep copy of one statement into a container of its own, the
//! adoption of such a copy back into the statement it came from, the clone of a region of one
//! container under an op of another, and the copy of a region as a unit that runs as a container
//! of its own.
//!
//! Ids never cross containers, so each move allocates fresh ids and records the correspondence in
//! an [`IdMap`]. A move builds the new entities off to the side and links them into the target
//! only once every operand, frame and snapshot has resolved: a call that fails changes nothing.
//!
//! # Ids across a copy
//!
//! A copy has ids of its own, so everything in it, provenance included, names ops of the copy. An
//! op that stands for itself in the original stands for itself in the copy. A provenance id of an
//! op that was not copied (an op outside the statement, or one erased earlier) becomes a key that
//! names no op of the copy; the [`IdMap`] links it to the original, so adoption maps it back.
//! Adoption maps every id of the copy: the id of a copied op to the original, the id of an op the
//! copy created to the fresh id of its adopted op, and drops the id of an op the copy created and
//! erased, which never existed in the container adopted into.

use smallvec::SmallVec;

use super::block::BlockData;
use super::ir::{Freed, Ir, held_ids};
use super::location::{LocOrigin, Location};
use super::op::OpData;
use super::region::RegionData;
use super::rewriter::Provenance;
use super::value::{TOKEN_SLOT_BASE, Use, ValueData, ValueDef};
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet, map_with_capacity};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::conversion::{EliminationReason, Phase};
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::LogOp;
use crate::exec::ir::op_kind::OpKind;

/// The position an id names in a `u16`-indexed list; callers have checked the length.
fn index_u16(at: usize) -> u16 {
	u16::try_from(at).unwrap_or(u16::MAX)
}

/// A region of a planned container, copied as a container of its own ([`Ir::copy_region`]).
#[allow(dead_code, reason = "the compile driver's planning callback extracts the units it plans")]
pub(crate) struct RegionUnit {
	/// The container. Its top region has the kind of the region copied.
	pub(crate) ir: Ir,
	/// The ids of the copy and of the container it was copied from.
	pub(crate) map: IdMap,
	/// The values of the container copied from that the arguments of the copy's top block past the
	/// region's own stand for, in argument order.
	pub(crate) inputs: Vec<ValueId>,
}

/// The ids of the ops, values and regions of one container and the ids that stand for them in
/// another, in both directions.
///
/// For a statement copy the original is the container the statement was copied from. For a
/// region clone the original is the container cloned from. In a statement copy, an original op
/// that was not copied but is named by the provenance of a copied op maps to a key that names no
/// op of the copy.
#[derive(Clone, Debug, Default)]
pub(crate) struct IdMap {
	ops: FxHashMap<OpId, OpId>,
	ops_back: FxHashMap<OpId, OpId>,
	values: FxHashMap<ValueId, ValueId>,
	values_back: FxHashMap<ValueId, ValueId>,
	regions: FxHashMap<RegionId, RegionId>,
	regions_back: FxHashMap<RegionId, RegionId>,
	/// The ops of the copy that copy a definition lying outside the statement.
	outside: FxHashSet<OpId>,
}

/// The token of one statement copy: the statement [`Ir::copy_statement`] copied and its map of
/// the copy. Only that call makes one, so the statement and the map an adoption reads are always
/// the pair the copy was made with. It reads as the [`IdMap`] it holds.
#[derive(Clone, Debug)]
pub(crate) struct StatementCopy {
	stmt: OpId,
	map: IdMap,
}

impl StatementCopy {
	/// The statement the copy was made of, which adoption replaces.
	pub(crate) fn stmt(&self) -> OpId {
		self.stmt
	}
}

impl std::ops::Deref for StatementCopy {
	type Target = IdMap;

	fn deref(&self) -> &IdMap {
		&self.map
	}
}

/// The answer of [`Ir::reroot`] for a region whose code reads a value, defined outside it, that
/// is not a literal. The container is left unchanged.
#[derive(Debug)]
pub(crate) struct OutsideRead;

/// The ops and values [`Ir::reroot`] freed, so the owner of a cache keyed by them drops its
/// entries.
#[derive(Debug, Default)]
pub(crate) struct Rerooted {
	pub(crate) ops: Vec<OpId>,
	pub(crate) values: Vec<ValueId>,
	/// How many literals defined outside the region moved into it.
	pub(crate) moved: usize,
}

/// What lies outside the region [`Ir::reroot`] keeps.
#[derive(Default)]
struct Enclosure {
	regions: Vec<RegionId>,
	blocks: FxHashSet<BlockId>,
	ops: FxHashSet<OpId>,
	/// Whether the region kept stands beneath the top region.
	found: bool,
}

/// A container copied from a region, the map from the original to the copy, and the pairs of an
/// original op and its copy, parents before their children.
type CopiedTop = (Ir, IdMap, Vec<(OpId, OpId)>);

impl IdMap {
	/// An empty map sized for `ops` ops, `values` values and `regions` regions.
	fn with_capacity(ops: usize, values: usize, regions: usize) -> Self {
		Self {
			ops: map_with_capacity(ops),
			ops_back: map_with_capacity(ops),
			values: map_with_capacity(values),
			values_back: map_with_capacity(values),
			regions: map_with_capacity(regions),
			regions_back: map_with_capacity(regions),
			outside: FxHashSet::default(),
		}
	}

	/// The op that stands for the original op `original`.
	pub(crate) fn map_op(&self, original: OpId) -> Option<OpId> {
		self.ops.get(&original).copied()
	}

	/// The original op that `copy` stands for.
	pub(crate) fn original_op(&self, copy: OpId) -> Option<OpId> {
		self.ops_back.get(&copy).copied()
	}

	/// The value that stands for the original value `original`.
	pub(crate) fn map_value(&self, original: ValueId) -> Option<ValueId> {
		self.values.get(&original).copied()
	}

	/// The original value that `copy` stands for.
	pub(crate) fn original_value(&self, copy: ValueId) -> Option<ValueId> {
		self.values_back.get(&copy).copied()
	}

	/// The region that stands for the original region `original`.
	pub(crate) fn map_region(&self, original: RegionId) -> Option<RegionId> {
		self.regions.get(&original).copied()
	}

	/// The original region that `copy` stands for.
	pub(crate) fn original_region(&self, copy: RegionId) -> Option<RegionId> {
		self.regions_back.get(&copy).copied()
	}

	/// Whether `copy` is a copy of a definition that lies outside the statement, which adoption
	/// leaves to the original.
	pub(crate) fn is_outside_copy(&self, copy: OpId) -> bool {
		self.outside.contains(&copy)
	}

	fn insert_op(&mut self, original: OpId, copy: OpId) {
		self.ops.insert(original, copy);
		self.ops_back.insert(copy, original);
	}

	fn insert_value(&mut self, original: ValueId, copy: ValueId) {
		self.values.insert(original, copy);
		self.values_back.insert(copy, original);
	}

	fn insert_region(&mut self, original: RegionId, copy: RegionId) {
		self.regions.insert(original, copy);
		self.regions_back.insert(copy, original);
	}
}

impl Ir {
	/// The region of `stmt` that holds the statement's code, and its block. `stmt` must be a
	/// `logical.stmt` op that owns a `Statement(_)` region of one block.
	fn statement_block(&self, stmt: OpId) -> Result<(RegionId, BlockId), IrError> {
		let data = self.ops.get(stmt).ok_or(IrError::Stale("op"))?;
		if !matches!(data.kind, OpKind::Logical(LogOp::Stmt { .. })) {
			return Err(IrError::Refused("statement op"));
		}
		let region = data
			.regions
			.iter()
			.copied()
			.find(|region| {
				self.regions
					.get(*region)
					.is_some_and(|held| matches!(held.kind, RegionKind::Statement(_)))
			})
			.ok_or(IrError::Stale("statement region"))?;
		let held = self.regions.get(region).ok_or(IrError::Stale("statement region"))?;
		match held.blocks.as_slice() {
			[block] => Ok((region, *block)),
			_ => Err(IrError::Refused("statement block")),
		}
	}

	/// A container holding a deep copy of the code of the statement `stmt`.
	///
	/// The copy has the scope, phase, frame table and snapshot table of this container. Its top
	/// region has the kind of the statement's region, and its top block the region's arguments.
	/// A value the statement reads from outside its region is copied with its defining op when
	/// that op is a literal or has neither operands nor regions, and otherwise becomes a further
	/// argument of the top block that carries the original's type. The copy has an id space of
	/// its own, so the provenance of a copied op names ops of the copy, and `IdMap::original_op`
	/// maps such an id back (see the module documentation). The seals of stored-clause ops, and
	/// the op counts of stored-clause regions, are carried over: a copied op keeps its seal only
	/// when the seal holds for the copy.
	///
	/// Fails with `Stale` when `stmt` is dangling and with `Refused` when it is not a
	/// `logical.stmt` op that owns a `Statement(_)` region of one block. Placement is not copied.
	pub(crate) fn copy_statement(&self, stmt: OpId) -> Result<(Ir, StatementCopy), IrError> {
		let (region, _) = self.statement_block(stmt)?;
		let (copy, map, _) = self.copy_top(region, Outside::Definers)?;
		Ok((
			copy,
			StatementCopy {
				stmt,
				map,
			},
		))
	}

	/// A region of this container as a container of its own that runs as a unit: the region of a
	/// subquery, a recursion's body or a closure's body of a planned container, which the
	/// compile driver lowers separately from the container that holds it.
	///
	/// The copy has the scope, phase, frame table and snapshot table of this container, and a
	/// top region of the kind of `region`. Its top block takes the region's own arguments, in
	/// order, then one argument for each value the region's code reads from outside it, in the
	/// order the code first reads them; [`RegionUnit::inputs`] names the value of this container
	/// that each of those stands for. A value defined outside by a literal is copied with its
	/// defining op instead, since a literal is the same value wherever it is evaluated: every
	/// other definition (a call that takes no operand, a free or session parameter, the clock of
	/// the statement, a row) is the requester's value and becomes an input, so the unit never
	/// evaluates it again. A region kind that takes the statement timestamp as its first
	/// argument takes its own; for any other kind the timestamp is read from outside and is one
	/// of the inputs.
	///
	/// The placement of each pipeline is carried over to the copy of the pipeline. Nothing of
	/// this container is referred to afterwards; the ids of the copy are its own, and
	/// [`RegionUnit::map`] relates them. The copy holds no facts.
	///
	/// Fails with `Stale` when `region` is dangling, and with `Refused` when it is the top region
	/// of this container or does not hold exactly one block.
	#[allow(
		dead_code,
		reason = "the compile driver's planning callback extracts the units it plans"
	)]
	pub(crate) fn copy_region(&self, region: RegionId) -> Result<RegionUnit, IrError> {
		let held = self.regions.get(region).ok_or(IrError::Stale("region"))?;
		if held.parent.is_none() {
			return Err(IrError::Refused("unit region"));
		}
		let (mut copy, map, copied) = self.copy_top(region, Outside::Literals)?;
		let own = held
			.blocks
			.first()
			.and_then(|block| self.blocks.get(*block))
			.map_or(0, |block| block.args.len());
		let mut inputs = Vec::new();
		if let Some(top) = copy.regions.get(copy.top).and_then(|held| held.blocks.first()) {
			let args = copy.blocks.get(*top).map_or(&[][..], |block| block.args.as_slice());
			for arg in args.iter().skip(own) {
				inputs.push(map.original_value(*arg).ok_or(IrError::Stale("unit input"))?);
			}
		}
		copy.matches.carry(
			&self.matches,
			copied.iter().copied(),
			&|value| map.map_value(value),
			&|region| map.map_region(region),
		);
		copy.hop_keys.carry(&self.hop_keys, copied.iter().copied());
		copy.placement.carry(&self.placement, copied);
		Ok(RegionUnit {
			ir: copy,
			map,
			inputs,
		})
	}

	/// The values of this container that the unit [`Ir::copy_region`] makes of `region` takes as
	/// inputs, in the order of the unit's trailing top-block arguments: the value each stands for,
	/// first read first.
	///
	/// This is the one definition of the order. A requester that lowers a unit it did not copy
	/// itself passes exactly these values, in this order, as the unit's trailing arguments; an
	/// order read any other way can bind a value to the wrong argument without an error. It costs
	/// one copy of the region.
	///
	/// Fails where [`Ir::copy_region`] does.
	#[allow(dead_code, reason = "the compile driver binds the inputs of the units it lowers")]
	pub(crate) fn region_inputs(&self, region: RegionId) -> Result<Vec<ValueId>, IrError> {
		self.copy_region(region).map(|unit| unit.inputs)
	}

	/// Makes `region`, which a single block of ops fills, the top region of this container, in
	/// place, and frees every op, value, block and region outside it.
	///
	/// The result is what [`Ir::copy_region`] leaves for a region that takes no input, without
	/// the copy: ops, values, blocks and regions inside `region` keep their ids and their
	/// generations, so the facts held for them stay current, and nothing inside is renumbered or
	/// bumped. The phase, the frame table and the snapshot table are kept whole; the `matches`,
	/// `hop_keys` and `placement` entries of the retained ops are kept and the others dropped; the
	/// eliminated list, the retired error scopes, the effect tokens and the constraint judgement
	/// are cleared, as a copy holds none of them. A literal that lies outside `region` and is read
	/// inside, by an operand or by the version snapshot of an op, moves to the front of the
	/// region's block, first read first, and every read of it keeps its value.
	///
	/// Fails, before changing anything, with `Stale` when `region` is dangling or stands beneath
	/// no op of this container's top region, and with `Refused` when it is the top region or does
	/// not hold exactly one block. Answers [`OutsideRead`], also before changing anything, when an
	/// op inside reads a value defined outside that is not a literal.
	pub(crate) fn reroot(
		&mut self,
		region: RegionId,
	) -> Result<Result<Rerooted, OutsideRead>, IrError> {
		let held = self.regions.get(region).ok_or(IrError::Stale("region"))?;
		let owner = held.parent.ok_or(IrError::Refused("unit region"))?;
		let block = match held.blocks.as_slice() {
			[block] => *block,
			_ => return Err(IrError::Refused("statement block")),
		};
		let mut enclosure = Enclosure::default();
		self.collect_enclosure(self.top, region, &mut enclosure);
		if !enclosure.found {
			return Err(IrError::Stale("region"));
		}
		let mut inside = Vec::new();
		let mut inside_blocks = Vec::new();
		self.collect_inside(region, &mut inside, &mut inside_blocks);
		let Ok(moved) = self.outside_literals(&inside, &enclosure)? else {
			return Ok(Err(OutsideRead));
		};

		// Nothing below fails.
		let mut freed = Rerooted {
			moved: moved.len(),
			..Rerooted::default()
		};
		for op in &moved {
			enclosure.ops.remove(op);
		}
		for op in inside.iter().chain(&moved) {
			if let Some(tokens) = self.op_tokens.remove(*op) {
				for value in tokens.results {
					self.values.remove(value);
					freed.values.push(value);
				}
			}
		}
		for held in &inside_blocks {
			let tokens =
				self.blocks.get_mut(*held).map(|data| std::mem::take(&mut data.token_args));
			for value in tokens.into_iter().flatten() {
				self.values.remove(value);
				freed.values.push(value);
			}
		}
		self.untokened.clear();
		self.threaded = false;
		for op in &enclosure.ops {
			self.drop_operand_uses(*op);
		}
		for op in &moved {
			if let Some(data) = self.ops.get_mut(*op) {
				data.parent = block;
			}
		}
		if let Some(data) = self.blocks.get_mut(block) {
			data.ops.splice(0..0, moved.iter().copied());
		}
		if let Some(data) = self.ops.get_mut(owner) {
			data.regions.retain(|held| *held != region);
		}
		for op in &enclosure.ops {
			self.free_op(*op, EliminationReason::DeadPure, false, &mut |entity| match entity {
				Freed::Op(op) => freed.ops.push(op),
				Freed::Value(value) => freed.values.push(value),
			});
		}
		for old in &enclosure.regions {
			self.sealed_regions.remove(*old);
			let Some(data) = self.regions.remove(*old) else {
				continue;
			};
			for held in &data.blocks {
				let Some(data) = self.blocks.remove(*held) else {
					continue;
				};
				for arg in data.args.iter().chain(&data.token_args) {
					self.values.remove(*arg);
					freed.values.push(*arg);
				}
			}
		}
		if let Some(data) = self.regions.get_mut(region) {
			data.parent = None;
		}
		self.top = region;
		self.eliminated.clear();
		self.retired_scope.clear();
		self.constrained = false;
		let ops = &self.ops;
		self.logical_ids.retain(|id| {
			ops.get(*id).is_some_and(|data| {
				matches!(data.provenance, Provenance::Bound)
					&& matches!(data.kind, OpKind::Logical(_))
			})
		});
		let pairs = || inside.iter().chain(&moved).map(|op| (*op, *op));
		let (values, regions) = (&self.values, &self.regions);
		let live_value = |value: ValueId| values.contains_key(value).then_some(value);
		let live_region = |held: RegionId| regions.contains_key(held).then_some(held);
		let matches = std::mem::take(&mut self.matches);
		self.matches.carry(&matches, pairs(), &live_value, &live_region);
		let hop_keys = std::mem::take(&mut self.hop_keys);
		self.hop_keys.carry(&hop_keys, pairs());
		let placement = std::mem::take(&mut self.placement);
		self.placement.carry(&placement, pairs());
		Ok(Ok(freed))
	}

	/// Collects what lies outside `target`, a region beneath `region`, into `out`: the regions,
	/// blocks and ops of `region` and of the regions beneath it, `target`'s subtree excluded. Sets
	/// `found` when `target` stands beneath `region`.
	fn collect_enclosure(&self, region: RegionId, target: RegionId, out: &mut Enclosure) {
		let Some(held) = self.regions.get(region) else {
			return;
		};
		out.regions.push(region);
		for block in &held.blocks {
			out.blocks.insert(*block);
			for op in self.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
				out.ops.insert(*op);
				for nested in self.ops.get(*op).map_or(&[][..], |data| &data.regions) {
					if *nested == target {
						out.found = true;
					} else {
						self.collect_enclosure(*nested, target, out);
					}
				}
			}
		}
	}

	/// Appends the ops of `region` and of every region beneath them to `ops`, each op before the
	/// ops of its own regions, and the blocks of those regions to `blocks`.
	fn collect_inside(&self, region: RegionId, ops: &mut Vec<OpId>, blocks: &mut Vec<BlockId>) {
		let Some(held) = self.regions.get(region) else {
			return;
		};
		for block in &held.blocks {
			blocks.push(*block);
			for op in self.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
				ops.push(*op);
				for nested in self.ops.get(*op).map_or(&[][..], |data| &data.regions) {
					self.collect_inside(*nested, ops, blocks);
				}
			}
		}
	}

	/// The literals defined in `enclosure` that the ops `inside` read, in the order
	/// [`Ir::copy_region`] first meets them: an op's operands in order, then the value its
	/// snapshot names, each op after the ops before it and the literals met after them.
	/// Answers [`OutsideRead`] for a read of any other value `enclosure` defines.
	///
	/// A bound request defines every value a statement reads inside the statement's region, so
	/// none moves for one; a container that defines a read literal outside has it moved.
	fn outside_literals(
		&self,
		inside: &[OpId],
		enclosure: &Enclosure,
	) -> Result<Result<Vec<OpId>, OutsideRead>, IrError> {
		let mut moved: Vec<OpId> = Vec::new();
		let mut order: Vec<OpId> = inside.to_vec();
		let mut snapshots: SmallVec<[SnapshotId; 4]> = SmallVec::new();
		let mut at = 0;
		while at < order.len() {
			let data = self.ops.get(order[at]).ok_or(IrError::Stale("op"))?;
			at += 1;
			let version = if snapshots.contains(&data.snapshot) {
				None
			} else {
				snapshots.push(data.snapshot);
				match self.snapshot(data.snapshot) {
					Some(Snapshot::Version(version)) => Some(*version),
					_ => None,
				}
			};
			for read in data.operands.iter().copied().chain(version) {
				let definer = match self.values.get(read).ok_or(IrError::Stale("value"))?.def {
					ValueDef::OpResult {
						op,
						..
					} if enclosure.ops.contains(&op) => op,
					ValueDef::BlockArg {
						block,
						..
					} if enclosure.blocks.contains(&block) => {
						return Ok(Err(OutsideRead));
					}
					ValueDef::OpResult {
						..
					}
					| ValueDef::BlockArg {
						..
					} => continue,
				};
				if moved.contains(&definer) {
					continue;
				}
				let held = self.ops.get(definer).ok_or(IrError::Stale("op"))?;
				let literal = matches!(held.kind, OpKind::Expr(ExprOp::Literal(_)))
					&& held.operands.is_empty()
					&& held.regions.is_empty();
				if !literal {
					return Ok(Err(OutsideRead));
				}
				moved.push(definer);
				order.push(definer);
			}
		}
		Ok(Ok(moved))
	}

	/// A container whose top region is a copy of `region`, with the pairs of an original op and
	/// its copy, parents before their children. What the copy does with a value read from
	/// outside the region is `outside`.
	fn copy_top(&self, region: RegionId, outside: Outside) -> Result<CopiedTop, IrError> {
		let held = self.regions.get(region).ok_or(IrError::Stale("region"))?;
		let kind = held.kind;
		let block = match held.blocks.as_slice() {
			[block] => *block,
			_ => return Err(IrError::Refused("statement block")),
		};
		let block = self.blocks.get(block).ok_or(IrError::Stale("block"))?;
		let mut copy = Ir::new(self.scope.clone(), kind);
		copy.phase = self.phase;
		copy.frames = self.frames.clone();
		copy.snapshots.clone_from(&self.snapshots);
		let top = copy.top;
		let top_block = copy
			.regions
			.get(top)
			.and_then(|held| held.blocks.first().copied())
			.ok_or(IrError::Stale("block"))?;
		let mut transplant = Transplant::new(self, &mut copy, Mode::Copy(top_block, outside));
		transplant.map.insert_region(region, top);
		transplant.alloc_args(&block.args, top_block)?;
		for op in &block.ops {
			let new = transplant.alloc_op(*op, top_block, false)?;
			transplant.top_ops.push(new);
		}
		transplant.wire()?;
		transplant.attach(top_block);
		transplant.hold_all();
		transplant.link();
		transplant.record();
		let map = transplant.map;
		let copied = transplant.ops;
		Ok((copy, map, copied))
	}

	/// Replaces the code of the statement `copy` was made of by the code of `from`, the copy
	/// [`Ir::copy_statement`] answered with `copy`, patched or not.
	///
	/// The statement keeps its `OpId`, its region and the arguments of the region's block. Ops of
	/// `from` get fresh ids here. Reads of the copy's outside arguments, and of the outside
	/// definitions the copy made, read the original values again; those definitions are not
	/// adopted. The provenance of an adopted op names ops of the copy, and adoption maps every such
	/// id through the map `copy` holds: the id of a copied op to the original, the id of an op the
	/// copy created to the fresh id of its adopted op (see the module documentation). The entries
	/// of `from.eliminated()` that name ops of this container give their reason to the entry the
	/// replaced code leaves for the same id, and an id that an adopted op holds is dropped from
	/// this container's list. Side tables stay consistent: holders, logical ids and the error
	/// scope of adopted ops that stand for themselves, and seals, which an adopted op keeps only
	/// when its copy's seal holds; an adopted stored-clause region keeps the op count it was
	/// sealed with.
	///
	/// Fails, before changing anything, with `Stale` when the statement is gone, with `Refused`
	/// when `from` does not have the shape of a copy of it or a value of `from` resolves to nothing
	/// here, and with `HasUses` when a value of the statement's current code is read outside it.
	pub(crate) fn adopt_statement(
		&mut self,
		from: Ir,
		copy: &StatementCopy,
	) -> Result<(), IrError> {
		self.adopt_statement_with(from, copy, &mut |_| {})
	}

	/// [`Ir::adopt_statement`], reporting each op and value of the replaced code to `forget`, so
	/// the owner of a cache keyed by them drops its entries.
	#[expect(
		clippy::needless_pass_by_value,
		reason = "adoption consumes the copy; the caller has no further use for it"
	)]
	pub(super) fn adopt_statement_with(
		&mut self,
		from: Ir,
		copy: &StatementCopy,
		forget: &mut impl FnMut(Freed),
	) -> Result<(), IrError> {
		let (stmt, map) = (copy.stmt, &copy.map);
		let (region, block) = self.statement_block(stmt)?;
		let kind =
			self.regions.get(region).map(|held| held.kind).ok_or(IrError::Stale("region"))?;
		let from_top = from.regions.get(from.top).ok_or(IrError::Stale("region"))?;
		if from_top.kind != kind {
			return Err(IrError::Refused("adopt region kind"));
		}
		let [from_block] = from_top.blocks.as_slice() else {
			return Err(IrError::Refused("adopt block"));
		};
		let from_block = from.blocks.get(*from_block).ok_or(IrError::Stale("block"))?;
		let arguments = self.blocks.get(block).ok_or(IrError::Stale("block"))?.args.clone();
		if from_block.args.len() < arguments.len() {
			return Err(IrError::Refused("adopt arguments"));
		}
		let mut old = Vec::new();
		self.collect_region_post_order(region, &mut old);
		let members: FxHashSet<OpId> = old.iter().copied().collect();
		for op in &old {
			let used_outside = self.ops.get(*op).is_some_and(|data| {
				data.results.iter().any(|result| {
					self.uses(*result).iter().any(|held| !members.contains(&held.user))
				})
			});
			if used_outside {
				return Err(IrError::HasUses(*op));
			}
		}
		let mut transplant = Transplant::new(&from, self, Mode::Adopt(map));
		for (copied, kept) in from_block.args.iter().zip(&arguments) {
			transplant.map.insert_value(*copied, *kept);
		}
		for op in &from_block.ops {
			if map.is_outside_copy(*op) {
				continue;
			}
			match transplant.alloc_op(*op, block, false) {
				Ok(new) => transplant.top_ops.push(new),
				Err(error) => {
					transplant.abandon();
					return Err(error);
				}
			}
		}
		if let Err(error) = transplant.wire() {
			transplant.abandon();
			return Err(error);
		}
		transplant.hold_all();
		transplant.drop_stale_eliminated();
		let mark = transplant.dst.eliminated.len();
		transplant.erase_old(&old, block, forget);
		transplant.carry_reasons(mark);
		transplant.attach(block);
		transplant.link();
		transplant.record();
		self.bump_with_ancestors(stmt);
		Ok(())
	}

	/// Appends the ops of `region` and of every region beneath them to `out`, each op after the
	/// ops of its own regions.
	fn collect_region_post_order(&self, region: RegionId, out: &mut Vec<OpId>) {
		let Some(held) = self.regions.get(region) else {
			return;
		};
		for block in &held.blocks {
			for op in self.blocks.get(*block).map_or(&[][..], |block| &block.ops) {
				for nested in self.ops.get(*op).map_or(&[][..], |data| &data.regions) {
					self.collect_region_post_order(*nested, out);
				}
				out.push(*op);
			}
		}
	}

	/// Removes the uses the operands of `op` hold on their values.
	fn drop_operand_uses(&mut self, op: OpId) {
		let Some(data) = self.ops.get(op) else {
			return;
		};
		for (at, operand) in data.operands.iter().enumerate() {
			let Some(value) = self.values.get_mut(*operand) else {
				continue;
			};
			let at = index_u16(at);
			if let Some(held) = value.uses.iter().position(|u| u.user == op && u.operand == at) {
				value.uses.remove(held);
			}
		}
		let Some(tokens) = self.op_tokens.get(op) else {
			return;
		};
		for (slot, operand) in tokens.operands.iter().enumerate() {
			let Some(value) = self.values.get_mut(*operand) else {
				continue;
			};
			let at = TOKEN_SLOT_BASE + index_u16(slot);
			if let Some(held) = value.uses.iter().position(|u| u.user == op && u.operand == at) {
				value.uses.remove(held);
			}
		}
	}
}

/// Copies `region` of `src` as a new region of `dst` owned by `owner`.
///
/// The new region has `region`'s kind and a block with its arguments and ops. Every copied op is
/// `Bound`, with its location unchanged, and has the frame and snapshot of the same value in
/// `dst`'s tables, interned there. When `dst` has left [`Phase::Bound`], the copied ops that lie
/// inside a stored-clause region are sealed at their generation. Answers the new region, the id
/// map and the ids of the ops created in pre-order.
///
/// Fails, before changing anything, with `Stale` when `owner` or `region` is dangling or an id
/// of `src` names nothing, with `Refused` when an operand or snapshot reads a value defined
/// outside `region`, and with `Capacity` when a table is full.
pub(super) fn clone_region(
	dst: &mut Ir,
	src: &Ir,
	region: RegionId,
	owner: OpId,
) -> Result<(RegionId, IdMap, Vec<OpId>), IrError> {
	if dst.ops.get(owner).is_none() {
		return Err(IrError::Stale("region owner"));
	}
	if src.regions.get(region).is_none() {
		return Err(IrError::Stale("region"));
	}
	let inside_stored = super::walk::ancestors(dst, owner).any(|(held, _)| {
		dst.region(held).is_some_and(|r| matches!(r.kind, RegionKind::StoredClause(_)))
	});
	let mut transplant = Transplant::new(src, dst, Mode::Clone);
	let new = match transplant.alloc_region(region, owner, inside_stored) {
		Ok(new) => new,
		Err(error) => {
			transplant.abandon();
			return Err(error);
		}
	};
	if let Err(error) = transplant.wire() {
		transplant.abandon();
		return Err(error);
	}
	if let Some(data) = transplant.dst.ops.get_mut(owner) {
		data.regions.push(new);
	}
	transplant.hold_all();
	transplant.link();
	transplant.record();
	transplant.dst.bump_with_ancestors(owner);
	let created = transplant.ops.iter().map(|(_, new)| *new).collect();
	Ok((new, transplant.map, created))
}

/// How a move treats what the moved code reads outside itself, and the ids it records.
#[derive(Clone, Copy)]
enum Mode<'s> {
	/// A copy into a fresh container; the block is the copy's top block, and the second field says
	/// what the copy does with a value its code reads from outside.
	Copy(BlockId, Outside),
	/// An adoption of a copy that `IdMap` describes.
	Adopt(&'s IdMap),
	/// A region clone, which reads nothing outside the region.
	Clone,
}

/// What a copy does with a value its code reads from outside the code it copies.
#[derive(Clone, Copy)]
enum Outside {
	/// The result of a copy of the defining op when it has neither operands nor regions, else a
	/// new argument of the top block. A statement copy is adopted back, so the copies of
	/// definitions read the original values again.
	Definers,
	/// The result of a copy of the defining op when it is a literal, else a new argument of the
	/// top block. The copy runs as a container of its own, so a definition that computes
	/// anything (a call with no operands, a free parameter, the statement clock) is the
	/// requester's value and never evaluated again.
	Literals,
}

/// One move of ops from `src` into `dst`.
struct Transplant<'s, 'd> {
	src: &'s Ir,
	dst: &'d mut Ir,
	mode: Mode<'s>,
	/// The correspondence built so far; the result of a copy or a clone.
	map: IdMap,
	/// Each source op moved with the new op that stands for it, parents before their children.
	ops: Vec<(OpId, OpId)>,
	/// The entities allocated in `dst`, which an abandoned move removes.
	values: Vec<ValueId>,
	blocks: Vec<BlockId>,
	regions: Vec<RegionId>,
	/// The new ops that stand directly in the target block, in order, after `lead_ops`.
	top_ops: Vec<OpId>,
	/// The copies of outside definitions, which stand first in the target block.
	lead_ops: Vec<OpId>,
	/// The new ops that lie inside a stored-clause region.
	stored: Vec<OpId>,
	/// The new regions that are, or lie inside, a stored-clause region.
	stored_regions: Vec<RegionId>,
	frames: FxHashMap<FrameId, FrameId>,
	snapshots: FxHashMap<SnapshotId, SnapshotId>,
	/// The lengths of `dst`'s frame and snapshot tables before the move, which an abandoned move
	/// truncates them back to.
	frames_len: usize,
	snapshots_len: usize,
}

impl<'s, 'd> Transplant<'s, 'd> {
	fn new(src: &'s Ir, dst: &'d mut Ir, mode: Mode<'s>) -> Self {
		let frames_len = dst.frames.frames.len();
		let snapshots_len = dst.snapshots.len();
		// A copy holds at most every entity of the source, so the arenas and the map are sized
		// for that once rather than grown one doubling at a time.
		dst.ops.reserve(src.ops.len());
		dst.values.reserve(src.values.len());
		dst.blocks.reserve(src.blocks.len());
		dst.regions.reserve(src.regions.len());
		Self {
			src,
			dst,
			mode,
			map: IdMap::with_capacity(src.ops.len(), src.values.len(), src.regions.len()),
			ops: Vec::with_capacity(src.ops.len()),
			values: Vec::with_capacity(src.values.len()),
			blocks: Vec::with_capacity(src.blocks.len()),
			regions: Vec::with_capacity(src.regions.len()),
			top_ops: Vec::new(),
			lead_ops: Vec::new(),
			stored: Vec::new(),
			stored_regions: Vec::new(),
			frames: FxHashMap::default(),
			snapshots: FxHashMap::default(),
			frames_len,
			snapshots_len,
		}
	}

	/// Allocates block arguments in `block` for the source values `args`.
	fn alloc_args(&mut self, args: &[ValueId], block: BlockId) -> Result<(), IrError> {
		let src = self.src;
		for (at, arg) in args.iter().enumerate() {
			let data = src.values.get(*arg).ok_or(IrError::Stale("value"))?;
			let value = self.dst.values.insert(ValueData {
				def: ValueDef::BlockArg {
					block,
					index: index_u16(at),
				},
				ty: data.ty.clone(),
				uses: SmallVec::new(),
				hint_name: data.hint_name.clone(),
			});
			self.values.push(value);
			self.map.insert_value(*arg, value);
			if let Some(held) = self.dst.blocks.get_mut(block) {
				held.args.push(value);
			}
		}
		Ok(())
	}

	/// Allocates the op `op` of `src` with its results and regions, standing in `parent`, and
	/// answers it. Its operands, frame, snapshot and provenance are set by [`Transplant::wire`].
	/// `stored` says the op lies inside a stored-clause region. The op is not listed in `parent`.
	fn alloc_op(&mut self, op: OpId, parent: BlockId, stored: bool) -> Result<OpId, IrError> {
		let src = self.src;
		let data = src.ops.get(op).ok_or(IrError::Stale("op"))?;
		let new = self.dst.ops.insert(OpData {
			kind: data.kind.clone(),
			operands: SmallVec::new(),
			results: SmallVec::new(),
			regions: SmallVec::new(),
			frame: data.frame,
			snapshot: data.snapshot,
			loc: data.loc.clone(),
			provenance: Provenance::Bound,
			parent,
		});
		self.dst.note_created(new);
		self.map.insert_op(op, new);
		self.ops.push((op, new));
		if stored {
			self.stored.push(new);
		}
		for (at, result) in data.results.iter().enumerate() {
			let value = src.values.get(*result).ok_or(IrError::Stale("value"))?;
			let id = self.dst.values.insert(ValueData {
				def: ValueDef::OpResult {
					op: new,
					index: index_u16(at),
				},
				ty: value.ty.clone(),
				uses: SmallVec::new(),
				hint_name: value.hint_name.clone(),
			});
			self.values.push(id);
			self.map.insert_value(*result, id);
			if let Some(held) = self.dst.ops.get_mut(new) {
				held.results.push(id);
			}
		}
		for region in &data.regions {
			let id = self.alloc_region(*region, new, stored)?;
			if let Some(held) = self.dst.ops.get_mut(new) {
				held.regions.push(id);
			}
		}
		Ok(new)
	}

	/// Allocates the region `region` of `src` with its blocks and ops, owned by `owner`, and
	/// answers it. The owner does not list it yet. `stored` says the region lies inside a
	/// stored-clause region.
	fn alloc_region(
		&mut self,
		region: RegionId,
		owner: OpId,
		stored: bool,
	) -> Result<RegionId, IrError> {
		let src = self.src;
		let data = src.regions.get(region).ok_or(IrError::Stale("region"))?;
		let id = self.dst.regions.insert(RegionData {
			kind: data.kind,
			blocks: SmallVec::new(),
			parent: Some(owner),
		});
		self.regions.push(id);
		self.map.insert_region(region, id);
		let stored = stored || matches!(data.kind, RegionKind::StoredClause(_));
		if stored {
			self.stored_regions.push(id);
		}
		for block in &data.blocks {
			let held = src.blocks.get(*block).ok_or(IrError::Stale("block"))?;
			let new = self.dst.blocks.insert(BlockData {
				token_args: SmallVec::new(),
				args: SmallVec::new(),
				ops: Vec::with_capacity(held.ops.len()),
				parent: id,
			});
			self.blocks.push(new);
			if let Some(listed) = self.dst.regions.get_mut(id) {
				listed.blocks.push(new);
			}
			self.alloc_args(&held.args, new)?;
			for op in &held.ops {
				let moved = self.alloc_op(*op, new, stored)?;
				if let Some(listed) = self.dst.blocks.get_mut(new) {
					listed.ops.push(moved);
				}
			}
		}
		Ok(id)
	}

	/// Sets the operands, frame, snapshot and provenance of every allocated op, which may
	/// allocate the outside definitions and arguments of a statement copy; provenance is set
	/// once all of them exist. Nothing is linked yet.
	fn wire(&mut self) -> Result<(), IrError> {
		let src = self.src;
		let mut at = 0;
		while at < self.ops.len() {
			let (from, to) = self.ops[at];
			at += 1;
			let data = src.ops.get(from).ok_or(IrError::Stale("op"))?;
			let mut operands: SmallVec<[ValueId; 4]> = SmallVec::with_capacity(data.operands.len());
			for operand in &data.operands {
				operands.push(self.value(*operand)?);
			}
			let frame = self.frame(data.frame)?;
			let snapshot = self.snapshot(data.snapshot)?;
			if let Some(held) = self.dst.ops.get_mut(to) {
				held.operands = operands;
				held.frame = frame;
				held.snapshot = snapshot;
			}
		}
		for at in 0..self.ops.len() {
			let (from, to) = self.ops[at];
			let data = src.ops.get(from).ok_or(IrError::Stale("op"))?;
			let provenance = self.provenance(from, &data.provenance);
			if let Some(held) = self.dst.ops.get_mut(to) {
				held.provenance = provenance;
			}
		}
		Ok(())
	}

	/// The value of `dst` that stands for the source value `v`.
	fn value(&mut self, v: ValueId) -> Result<ValueId, IrError> {
		if let Some(found) = self.map.values.get(&v) {
			return Ok(*found);
		}
		match self.mode {
			Mode::Copy(top, outside) => self.copy_outside(v, top, outside),
			Mode::Adopt(original) => original
				.original_value(v)
				.filter(|value| self.dst.values.contains_key(*value))
				.ok_or(IrError::Refused("adopt value")),
			Mode::Clone => Err(IrError::Refused("clone value")),
		}
	}

	/// The value of the copy that stands for `v`, defined outside the copied code: the result of a
	/// copy of its defining op when `outside` admits the op and it has neither operands nor
	/// regions, otherwise a new argument of the copy's top block.
	fn copy_outside(
		&mut self,
		v: ValueId,
		top: BlockId,
		outside: Outside,
	) -> Result<ValueId, IrError> {
		let src = self.src;
		let data = src.values.get(v).ok_or(IrError::Stale("value"))?;
		if let ValueDef::OpResult {
			op,
			..
		} = data.def
		{
			let definer = src.ops.get(op).ok_or(IrError::Stale("op"))?;
			let copyable = match outside {
				Outside::Definers => true,
				Outside::Literals => matches!(definer.kind, OpKind::Expr(ExprOp::Literal(_))),
			};
			if copyable && definer.operands.is_empty() && definer.regions.is_empty() {
				let new = self.alloc_op(op, top, false)?;
				self.map.outside.insert(new);
				self.lead_ops.push(new);
				return self.map.values.get(&v).copied().ok_or(IrError::Stale("value"));
			}
		}
		let at = self.dst.blocks.get(top).map_or(0, |held| held.args.len());
		let value = self.dst.values.insert(ValueData {
			def: ValueDef::BlockArg {
				block: top,
				index: index_u16(at),
			},
			ty: data.ty.clone(),
			uses: SmallVec::new(),
			hint_name: data.hint_name.clone(),
		});
		self.values.push(value);
		self.map.insert_value(v, value);
		if let Some(held) = self.dst.blocks.get_mut(top) {
			held.args.push(value);
		}
		Ok(value)
	}

	/// The frame of `dst` that stands for the source frame `id`. A copy shares its source's frame
	/// table, and the other moves intern the frame.
	fn frame(&mut self, id: FrameId) -> Result<FrameId, IrError> {
		if let Some(found) = self.frames.get(&id) {
			return Ok(*found);
		}
		let resolved = match self.mode {
			Mode::Copy(..) => id,
			Mode::Adopt(_) | Mode::Clone => {
				let frame = self.src.frame(id).ok_or(IrError::Stale("frame"))?;
				self.dst.intern_frame(frame.clone())?
			}
		};
		self.frames.insert(id, resolved);
		Ok(resolved)
	}

	/// The snapshot of `dst` that stands for the source snapshot `id`. A copy shares its source's
	/// snapshot table and rewrites the version value of an entry in place; the other moves intern
	/// the snapshot over the value that stands for its version.
	fn snapshot(&mut self, id: SnapshotId) -> Result<SnapshotId, IrError> {
		if let Some(found) = self.snapshots.get(&id) {
			return Ok(*found);
		}
		let resolved = match self.mode {
			Mode::Copy(..) => {
				if let Some(Snapshot::Version(version)) = self.src.snapshot(id).copied() {
					let moved = self.value(version)?;
					if let Some(entry) = self.dst.snapshots.get_mut(usize::from(id.index())) {
						*entry = Snapshot::Version(moved);
					}
				}
				id
			}
			Mode::Adopt(_) | Mode::Clone => {
				let snapshot = match self.src.snapshot(id).copied() {
					Some(Snapshot::Current) => Snapshot::Current,
					Some(Snapshot::Version(version)) => Snapshot::Version(self.value(version)?),
					None => return Err(IrError::Stale("snapshot")),
				};
				self.dst.intern_snapshot(snapshot)?
			}
		};
		self.snapshots.insert(id, resolved);
		Ok(resolved)
	}

	/// The provenance of the new op that stands for the source op `from`, whose own is `prov`.
	///
	/// A copy keeps `Bound` and maps each listed id to the copy's op for it, which for an op that
	/// was not copied is a key naming no op. Adoption makes a copied op that stood for itself
	/// stand for its original, and maps each listed id back, dropping the id of an op the copy
	/// created and erased. A clone is `Bound`.
	fn provenance(&mut self, from: OpId, prov: &Provenance) -> Provenance {
		match (self.mode, prov) {
			(Mode::Clone, _) | (Mode::Copy(..), Provenance::Bound) => Provenance::Bound,
			(Mode::Copy(..), Provenance::From(ids)) => {
				let mut mapped: SmallVec<[OpId; 2]> = SmallVec::new();
				for id in ids {
					mapped.push(self.copy_id(*id));
				}
				mapped.sort_unstable();
				mapped.dedup();
				Provenance::From(mapped)
			}
			(Mode::Adopt(original), Provenance::Bound) => match original.original_op(from) {
				Some(op) => Provenance::From(SmallVec::from_slice(&[op])),
				None => Provenance::Bound,
			},
			(Mode::Adopt(original), Provenance::From(ids)) => {
				let mut mapped: SmallVec<[OpId; 2]> = SmallVec::new();
				for id in ids {
					if let Some(op) = original.original_op(*id) {
						mapped.push(op);
					} else if let Some(new) = self.map.ops.get(id) {
						mapped.push(*new);
					}
				}
				mapped.sort_unstable();
				mapped.dedup();
				Provenance::From(mapped)
			}
		}
	}

	/// The id of the copy that stands for the original op `id`: its copy, or else a key that
	/// names no op of the copy, which the map links to `id`. The arena never hands such a key to
	/// another op.
	fn copy_id(&mut self, id: OpId) -> OpId {
		if let Some(copy) = self.map.ops.get(&id) {
			return *copy;
		}
		let key = self.dst.ops.insert(OpData {
			kind: OpKind::Expr(ExprOp::Yield),
			operands: SmallVec::new(),
			results: SmallVec::new(),
			regions: SmallVec::new(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Query,
			},
			provenance: Provenance::Bound,
			parent: BlockId::default(),
		});
		self.dst.ops.remove(key);
		self.map.insert_op(id, key);
		key
	}

	/// Lists the new top-level ops in `block`, the copies of outside definitions first.
	fn attach(&mut self, block: BlockId) {
		if let Some(held) = self.dst.blocks.get_mut(block) {
			held.ops.extend(self.lead_ops.iter().chain(&self.top_ops).copied());
		}
	}

	/// Counts the new ops as holders of the ids they stand for.
	fn hold_all(&mut self) {
		for (_, new) in &self.ops {
			let ids: SmallVec<[OpId; 2]> = match self.dst.ops.get(*new) {
				Some(data) => held_ids(&data.provenance, *new).collect(),
				None => SmallVec::new(),
			};
			for id in ids {
				self.dst.hold(id);
			}
		}
	}

	/// Records the uses the new ops hold and gives each its first generation.
	fn link(&mut self) {
		for (_, new) in &self.ops {
			let operands: SmallVec<[ValueId; 4]> = match self.dst.ops.get(*new) {
				Some(data) => data.operands.clone(),
				None => continue,
			};
			for (at, operand) in operands.iter().enumerate() {
				if let Some(value) = self.dst.values.get_mut(*operand) {
					value.uses.push(Use {
						user: *new,
						operand: index_u16(at),
					});
				}
			}
			self.dst.bump(*new);
		}
	}

	/// Records the error scope, logical id and seal of each new op, and the physical plan's
	/// decisions for its original ([`crate::exec::ir::phys::matches`]), once the new ops are
	/// linked into the container.
	fn record(&mut self) {
		let src = self.src;
		// A value the code reads from outside an adopted copy is the original's value.
		let (map, mode) = (&self.map, &self.mode);
		let values = |value: ValueId| {
			map.map_value(value).or_else(|| match mode {
				Mode::Adopt(original) => original.original_value(value),
				Mode::Copy(..) | Mode::Clone => None,
			})
		};
		self.dst.matches.carry(&src.matches, self.ops.iter().copied(), &values, &|region| {
			map.map_region(region)
		});
		self.dst.hop_keys.carry(&src.hop_keys, self.ops.iter().copied());
		for at in 0..self.ops.len() {
			let (from, new) = self.ops[at];
			let Some(data) = self.dst.ops.get(new) else {
				continue;
			};
			let block = data.parent;
			let bound = matches!(data.provenance, Provenance::Bound);
			let logical = matches!(data.kind, OpKind::Logical(_));
			match self.mode {
				Mode::Copy(..) => {
					if let Some(scope) = src.bind_scope.get(from).copied() {
						let owner = scope.map(|owner| self.copy_id(owner));
						self.dst.bind_scope.insert(new, owner);
					}
					if bound && logical {
						self.dst.logical_ids.push(new);
					}
				}
				Mode::Adopt(_) | Mode::Clone => {
					if bound {
						let owner = self.dst.error_scope_owner(block);
						self.dst.bind_scope.insert(new, owner);
						if logical {
							self.dst.logical_ids.push(new);
						}
					}
				}
			}
			if !matches!(self.mode, Mode::Clone)
				&& let Some(seal) = src.sealed.get(from)
			{
				let generation = self.dst.generation(new);
				let kept = if src.generation(from) == *seal {
					generation
				} else {
					generation.wrapping_sub(1)
				};
				self.dst.sealed.insert(new, kept);
			}
		}
		if matches!(self.mode, Mode::Clone) {
			if self.dst.phase > Phase::Bound {
				for op in &self.stored {
					let generation = self.dst.generation(*op);
					self.dst.sealed.insert(*op, generation);
				}
				for region in &self.stored_regions {
					let count = self.dst.region_op_count(*region);
					self.dst.sealed_regions.insert(*region, count);
				}
			}
		} else {
			// A copied region keeps the count its original was sealed with, so an edit made in
			// the copy still shows after adoption.
			for region in &self.stored_regions {
				let sealed = self
					.map
					.regions_back
					.get(region)
					.and_then(|from| self.src.sealed_regions.get(*from))
					.copied();
				if let Some(count) = sealed {
					self.dst.sealed_regions.insert(*region, count);
				}
			}
		}
	}

	/// Removes the new entities of a move that failed before it linked anything, and the frames
	/// and snapshots it interned. Interning only appends, so the entries past the lengths the
	/// move started with are the move's own.
	fn abandon(self) {
		self.dst.frames.frames.truncate(self.frames_len);
		self.dst.snapshots.truncate(self.snapshots_len);
		for (_, op) in &self.ops {
			self.dst.ops.remove(*op);
		}
		for value in &self.values {
			self.dst.values.remove(*value);
		}
		for block in &self.blocks {
			self.dst.blocks.remove(*block);
		}
		for region in &self.regions {
			self.dst.regions.remove(*region);
		}
	}

	/// Drops from the eliminated list every id an adopted op now holds.
	fn drop_stale_eliminated(&mut self) {
		if self.dst.eliminated.is_empty() {
			return;
		}
		let mut present: FxHashSet<OpId> = self.dst.eliminated.iter().map(|(id, _)| *id).collect();
		let mut stale: FxHashSet<OpId> = FxHashSet::default();
		for (_, new) in &self.ops {
			let ids: SmallVec<[OpId; 2]> = match self.dst.ops.get(*new) {
				Some(data) => held_ids(&data.provenance, *new).collect(),
				None => SmallVec::new(),
			};
			for id in ids {
				if present.remove(&id) {
					stale.insert(id);
				}
			}
		}
		if !stale.is_empty() {
			self.dst.remove_from_eliminated(&stale);
		}
	}

	/// Gives the entries the erase of the old code recorded after `mark` the reasons the copy
	/// recorded for the same ids. An entry the erase did not record stands for an id some op still
	/// holds, so a copy's entry for it is not moved.
	fn carry_reasons(&mut self, mark: usize) {
		let Mode::Adopt(original) = self.mode else {
			return;
		};
		if self.dst.eliminated.len() <= mark || self.src.eliminated.is_empty() {
			return;
		}
		let mut reasons: FxHashMap<OpId, EliminationReason> = FxHashMap::default();
		for (id, reason) in &self.src.eliminated {
			let Some(id) = original.original_op(*id) else {
				continue;
			};
			let reason = match reason {
				EliminationReason::MergedInto(op) => {
					match self.map.ops.get(op).copied().or_else(|| original.original_op(*op)) {
						Some(merged) => EliminationReason::MergedInto(merged),
						None => EliminationReason::DeadPure,
					}
				}
				other @ (EliminationReason::DeadPure
				| EliminationReason::AnsweredByAccessPath
				| EliminationReason::FoldedConstant
				| EliminationReason::DeadBeforeCertainError
				| EliminationReason::InvisibleToPrincipal
				| EliminationReason::Unreferenced
				| EliminationReason::DeadLayerColumn
				| EliminationReason::Unreachable) => *other,
			};
			reasons.entry(id).or_insert(reason);
		}
		for entry in &mut self.dst.eliminated[mark..] {
			if let Some(reason) = reasons.get(&entry.0) {
				entry.1 = *reason;
			}
		}
	}

	/// Erases the ops `old`, which are the target statement's current code, leaving `block` empty.
	fn erase_old(&mut self, old: &[OpId], block: BlockId, forget: &mut impl FnMut(Freed)) {
		for op in old {
			self.dst.drop_operand_uses(*op);
		}
		for op in old {
			self.dst.free_op(*op, EliminationReason::DeadPure, false, forget);
		}
		if let Some(held) = self.dst.blocks.get_mut(block) {
			held.ops.clear();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
	use crate::exec::ir::iface::region_kind::StatementCommit;
	use crate::exec::ir::iface::types::{TokenClass, TypeKnowledge};
	use crate::val::Value;

	fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	#[test]
	fn adopt_statement_drops_token_operand_uses_of_erased_ops() {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let (stmt, token_arg, old_op) = {
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let top_block = rw.ir().region(rw.ir().top()).expect("top region").blocks[0];
			let stmt = rw
				.create(
					InsertPoint::End(top_block),
					spec(
						OpKind::Logical(LogOp::Stmt {
							commit: StatementCommit::Auto,
							control: None,
						}),
						&[],
						0,
					),
				)
				.expect("stmt created");
			let (_, stmt_block) = rw
				.create_region(stmt, RegionKind::Statement(StatementCommit::Auto), &[])
				.expect("stmt region created");
			let token_arg =
				rw.add_token_arg(stmt_block, TokenClass::Store).expect("token arg created");
			let lit = rw
				.create(
					InsertPoint::End(stmt_block),
					spec(OpKind::Expr(ExprOp::Literal(Box::new(Value::from(1i64)))), &[], 1),
				)
				.expect("lit created");
			rw.add_token_operand(lit, token_arg).expect("token operand added");
			let lit_val = rw.ir().op(lit).expect("lit").results[0];
			let _ = rw
				.create(
					InsertPoint::End(stmt_block),
					spec(OpKind::Logical(LogOp::Yield), &[lit_val], 0),
				)
				.expect("yield created");
			(stmt, token_arg, lit)
		};
		assert_eq!(ir.uses(token_arg).len(), 1);
		assert_eq!(ir.uses(token_arg)[0].user, old_op);

		let (copied, copy_token) = ir.copy_statement(stmt).expect("statement copied");
		ir.adopt_statement(copied, &copy_token).expect("statement adopted");

		assert!(
			ir.uses(token_arg).is_empty(),
			"erasing old ops during adopt_statement must drop their token operand uses"
		);
	}
}
