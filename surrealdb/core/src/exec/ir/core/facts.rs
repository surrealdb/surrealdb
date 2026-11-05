//! The fact cache: per-op facts, per-value type knowledge and invariance classes, and per-value
//! fold outcomes, owned apart from the container.
//!
//! The driver passes the cache `&mut` beside the container, so no interior mutability is needed
//! and the pair is `Send`. An entry records the generation of its op when it was computed; an
//! entry whose generation differs from the container's is stale and is recomputed on its next
//! read. An entry of an erased op or value is dropped, so a recycled slot never answers with the
//! entry of its previous occupant; only [`FactCache::last_under`] still answers for an erased op,
//! and only to the context the entry was computed under.

use std::collections::BTreeMap;

use smallvec::SmallVec;

use super::side_map::SideMap;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::iface::catalog::CxKey;
use crate::exec::ir::iface::effects::{EffectSummary, OpEffects};
use crate::exec::ir::iface::fold::{FoldOutcome, FoldRequest};
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::logical::pipelines::{Legality, StatementLegality};

/// What analyses know about one op.
#[derive(Clone, Debug)]
pub(crate) struct OpFacts {
	pub(crate) effects: OpEffects,
}

impl OpFacts {
	/// The facts of an op nothing is known about: the top of every lattice.
	pub(crate) fn top() -> Self {
		Self {
			effects: OpEffects::top(),
		}
	}
}

/// The state of one fold request.
#[derive(Clone, Debug)]
enum FoldSlot {
	/// Requested; awaited by the driver between passes.
	Pending(FoldRequest),
	Done(Box<FoldOutcome>),
}

/// The loop facts taken at the logical phase boundary, keyed by the logical loop op.
pub(crate) type LegalitySnapshotSlot = BTreeMap<OpId, LoopFacts>;

/// What the logical plan states of the pipelines and statements of a container, keyed by the
/// logical op each stands for: a pipeline by its loop, a statement by its `logical.stmt`.
///
/// The planner of a request fills it from the plan it bound the container from, so a pipeline
/// converted from a logical loop reads the legality the plan stated for it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LogicalLegalities {
	/// The legality of the segment of the logical plan each pipeline came from.
	pub(crate) pipelines: BTreeMap<OpId, Legality>,
	/// What each statement refuses its pipelines.
	pub(crate) statements: BTreeMap<OpId, StatementLegality>,
}

/// The invariance classes of one value: per op it was computed against, the generation of that
/// op and the class.
type Invariance = SmallVec<[(OpId, u32, u8); 2]>;

/// The facts of one op, one entry per context: the generation they were computed at, the
/// context's key and the facts.
/// Held out of line: an answer is several hundred bytes, and a slot of `entries` is sized for one
/// whether or not its op has any, so an inline answer makes every slot, and every growth of the
/// map, that large.
type ContextFacts = Vec<(u32, CxKey, OpFacts)>;

type ContextSummaries = SmallVec<[(u32, CxKey, EffectSummary); 2]>;

/// The memo of one container's analyses.
#[derive(Clone, Debug)]
pub(crate) struct FactCache {
	/// The answer for every op while no analysis has computed one.
	top: OpFacts,
	/// Per op: the facts computed under each context, with the generation they were computed at
	/// and the context's key. The last entry is the one recorded last.
	entries: SideMap<OpId, ContextFacts>,
	/// The [`EffectSummary`] of the region-local answer of an op at its generation, under each
	/// context, held apart from `entries` so that it is computed without the footprint.
	summaries: SideMap<OpId, ContextSummaries>,
	/// The facts of erased ops, as they stood when the op was erased, one entry per context.
	retired: FxHashMap<OpId, ContextFacts>,
	/// Per value: the generation of its defining op when the knowledge was computed, and it.
	types: SideMap<ValueId, (u32, TypeKnowledge)>,
	/// Per value: the invariance class computed against an op, with the generation of that op.
	invariance: SideMap<ValueId, Invariance>,
	/// One fold slot per value.
	folds: SideMap<ValueId, FoldSlot>,
	/// The snapshot taken at the logical boundary, once an analysis has filled it.
	legality_snapshot: Option<LegalitySnapshotSlot>,
	/// What the logical plan states of the container's pipelines and statements, once the planner
	/// has stated it.
	logical_legalities: Option<LogicalLegalities>,
}

impl FactCache {
	/// An empty cache.
	pub(crate) fn new() -> Self {
		Self {
			top: OpFacts::top(),
			entries: SideMap::new(),
			summaries: SideMap::new(),
			retired: FxHashMap::default(),
			types: SideMap::new(),
			invariance: SideMap::new(),
			folds: SideMap::new(),
			legality_snapshot: None,
			logical_legalities: None,
		}
	}

	/// The facts of `op` computed at `generation` under the context `key` names, or `None` when
	/// absent or stale. Facts computed under another context never answer.
	pub(crate) fn get(&self, op: OpId, generation: u32, key: &CxKey) -> Option<&OpFacts> {
		self.entries
			.get(op)?
			.iter()
			.find(|(computed_at, held, _)| *computed_at == generation && held == key)
			.map(|(_, _, facts)| facts)
	}

	/// The answer that holds for every op no analysis has computed a fact for.
	/// The summary of the region-local answer of `op` computed at `generation` under `key`.
	pub(crate) fn summary(&self, op: OpId, generation: u32, key: &CxKey) -> Option<EffectSummary> {
		self.summaries
			.get(op)?
			.iter()
			.find(|(computed_at, held, _)| *computed_at == generation && held == key)
			.map(|(_, _, summary)| *summary)
	}

	/// Records the summary of the region-local answer of `op` at `generation` under `key`,
	/// replacing what was held under `key` and dropping what was held at another generation.
	pub(crate) fn insert_summary(
		&mut self,
		op: OpId,
		generation: u32,
		key: CxKey,
		summary: EffectSummary,
	) {
		if let Some(held) = self.entries.get_mut(op) {
			held.retain(|(computed_at, _, _)| *computed_at == generation);
		}
		self.retired.remove(&op);
		match self.summaries.get_mut(op) {
			Some(held) => {
				held.retain(|(computed_at, other, _)| *computed_at == generation && *other != key);
				held.push((generation, key, summary));
			}
			None => {
				let mut held = SmallVec::new();
				held.push((generation, key, summary));
				self.summaries.insert(op, held);
			}
		}
	}

	pub(crate) fn top(&self) -> &OpFacts {
		&self.top
	}

	/// Records `facts` of `op` computed at `generation` under the context `key` names. Entries of
	/// an older generation are dropped.
	pub(crate) fn insert(&mut self, op: OpId, generation: u32, key: CxKey, facts: OpFacts) {
		if let Some(held) = self.summaries.get_mut(op) {
			held.retain(|(computed_at, _, _)| *computed_at == generation);
		}
		self.retired.remove(&op);
		if self.entries.get(op).is_none() {
			self.entries.insert(op, Vec::new());
		}
		if let Some(held) = self.entries.get_mut(op) {
			held.retain(|(computed_at, other, _)| *computed_at == generation && *other != key);
			held.push((generation, key, facts));
		}
	}

	/// The facts last recorded for `op` under the context `key` names, whatever their generation.
	/// An erased op answers the facts it had when it was erased; an op never computed under `key`
	/// answers `None`. Facts computed under another context never answer.
	pub(crate) fn last_under(&self, op: OpId, key: &CxKey) -> Option<&OpFacts> {
		fn under<'a>(held: &'a ContextFacts, key: &CxKey) -> Option<&'a OpFacts> {
			held.iter().rev().find(|(_, other, _)| other == key).map(|(_, _, facts)| facts)
		}
		let is_live = self.entries.get(op).is_some_and(|held| !held.is_empty())
			|| self.summaries.get(op).is_some_and(|held| !held.is_empty());
		if is_live {
			return self.entries.get(op).and_then(|held| under(held, key));
		}
		self.entries
			.get(op)
			.and_then(|held| under(held, key))
			.or_else(|| self.retired.get(&op).and_then(|held| under(held, key)))
	}

	/// Drops the entry of `op`, which keeps answering [`FactCache::last_under`].
	pub(crate) fn invalidate(&mut self, op: OpId) {
		self.summaries.remove(op);
		match self.entries.remove(op) {
			Some(held) if !held.is_empty() => {
				self.retired.insert(op, held);
			}
			Some(_) | None => {
				self.retired.remove(&op);
			}
		}
	}

	/// Drops every entry held for `op`, a freed op that no later read names, so not even
	/// [`FactCache::last_under`] answers for it.
	pub(crate) fn retire(&mut self, op: OpId) {
		self.entries.remove(op);
		self.summaries.remove(op);
		self.retired.remove(&op);
	}

	/// Drops what only planning reads: the logical-boundary snapshot, the logical plan's
	/// legalities and the facts of erased ops. What stays is the per-op entries, summaries and
	/// per-value knowledge, each keyed by an op or value that is still live and the generation it
	/// was computed at, so a container that keeps its ops' ids answers lowering's reads from it,
	/// while no reader of the physical boundary finds a snapshot or an erased op's facts.
	pub(crate) fn clear_planning_only(&mut self) {
		self.legality_snapshot = None;
		self.logical_legalities = None;
		self.retired.clear();
	}

	/// Drops every entry held for the erased value `v`.
	pub(crate) fn invalidate_value(&mut self, v: ValueId) {
		self.types.remove(v);
		self.invariance.remove(v);
		self.folds.remove(v);
	}

	/// The type knowledge of `v` computed when its defining op stood at `generation`, or `None`
	/// when absent or stale.
	pub(crate) fn type_of(&self, v: ValueId, generation: u32) -> Option<&TypeKnowledge> {
		self.types
			.get(v)
			.filter(|(computed_at, _)| *computed_at == generation)
			.map(|(_, knowledge)| knowledge)
	}

	/// Records the type knowledge of `v` computed at `generation` of its defining op.
	pub(crate) fn set_type(&mut self, v: ValueId, generation: u32, knowledge: TypeKnowledge) {
		self.types.insert(v, (generation, knowledge));
	}

	/// The invariance class of `v` against `op` computed when `op` stood at `generation`, or
	/// `None` when absent or stale. The class is the analysis's own encoding.
	pub(crate) fn invariance(&self, v: ValueId, op: OpId, generation: u32) -> Option<u8> {
		self.invariance
			.get(v)?
			.iter()
			.find(|(against, computed_at, _)| *against == op && *computed_at == generation)
			.map(|(_, _, class)| *class)
	}

	/// Records the invariance class of `v` against `op` computed at `generation` of `op`.
	pub(crate) fn set_invariance(&mut self, v: ValueId, op: OpId, generation: u32, class: u8) {
		if self.invariance.get(v).is_none() {
			self.invariance.insert(v, SmallVec::new());
		}
		if let Some(entries) = self.invariance.get_mut(v) {
			match entries.iter_mut().find(|(against, _, _)| *against == op) {
				Some(entry) => *entry = (op, generation, class),
				None => entries.push((op, generation, class)),
			}
		}
	}

	/// The loop facts taken at the logical boundary, or `None` until an analysis has taken them.
	pub(crate) fn legality_snapshot(&self) -> Option<&LegalitySnapshotSlot> {
		self.legality_snapshot.as_ref()
	}

	/// Stores the loop facts taken at the logical boundary.
	pub(crate) fn set_legality_snapshot(&mut self, snapshot: LegalitySnapshotSlot) {
		self.legality_snapshot = Some(snapshot);
	}

	/// What the logical plan states of the container's pipelines and statements, or `None` until
	/// the planner has stated it.
	pub(crate) fn logical_legalities(&self) -> Option<&LogicalLegalities> {
		self.logical_legalities.as_ref()
	}

	/// Stores what the logical plan states of the container's pipelines and statements.
	pub(crate) fn set_logical_legalities(&mut self, legalities: LogicalLegalities) {
		self.logical_legalities = Some(legalities);
	}

	/// Records that `req.value` is to be folded. An outcome already stored is kept.
	pub(crate) fn request_fold(&mut self, req: FoldRequest) {
		if !self.folds.contains_key(req.value) {
			self.folds.insert(req.value, FoldSlot::Pending(req));
		}
	}

	/// The outcome stored for `v`, or `None` while it is pending or was never requested.
	pub(crate) fn fold_outcome(&self, v: ValueId) -> Option<&FoldOutcome> {
		match self.folds.get(v) {
			Some(FoldSlot::Done(outcome)) => Some(&**outcome),
			Some(FoldSlot::Pending(_)) | None => None,
		}
	}

	/// The requests awaiting an outcome.
	pub(crate) fn pending_folds(&self) -> impl Iterator<Item = FoldRequest> {
		self.folds.values().filter_map(|slot| match slot {
			FoldSlot::Pending(req) => Some(*req),
			FoldSlot::Done(_) => None,
		})
	}

	/// Stores the outcome of the fold requested for `v`.
	pub(crate) fn store_fold(&mut self, v: ValueId, outcome: FoldOutcome) {
		self.folds.insert(v, FoldSlot::Done(Box::new(outcome)));
	}
}

impl Default for FactCache {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use slotmap::SlotMap;

	use super::*;
	use crate::exec::ir::iface::effects::{Determinism, IrFootprint, Rollback};
	use crate::exec::ir::iface::fallibility::Fallibility;
	use crate::expr::logical::EffectSet;

	fn sample_key(seed: usize) -> CxKey {
		CxKey {
			registry: seed,
			catalog: (seed as u64, "test"),
			frames: (seed as u64, seed),
		}
	}

	fn sample_facts(set: EffectSet) -> OpFacts {
		OpFacts {
			effects: OpEffects {
				set,
				fallibility: Fallibility::INFALLIBLE,
				context_deps: Default::default(),
				footprint: IrFootprint {
					reads: Default::default(),
					writes: Default::default(),
				},
				rollback: Rollback::NoEffects,
				determinism: Determinism::Deterministic,
			},
		}
	}

	#[test]
	fn insert_summary_at_newer_generation_prunes_stale_entries_and_retired() {
		let mut ops: SlotMap<OpId, ()> = SlotMap::with_key();
		let op = ops.insert(());
		let mut cache = FactCache::new();
		let key = sample_key(1);
		let old_facts = sample_facts(EffectSet::READS_DATA);
		cache.insert(op, 0, key, old_facts);
		assert!(cache.last_under(op, &key).is_some());

		let new_summary = EffectSummary {
			set: EffectSet::PURE,
			fallibility: Fallibility::INFALLIBLE,
			determinism: Determinism::Deterministic,
		};
		cache.insert_summary(op, 1, key, new_summary);
		assert!(cache.get(op, 0, &key).is_none());
		assert!(cache.last_under(op, &key).is_none());

		cache.invalidate(op);
		assert!(cache.last_under(op, &key).is_none());
	}

	#[test]
	fn insert_at_newer_generation_prunes_stale_summaries_and_retired() {
		let mut ops: SlotMap<OpId, ()> = SlotMap::with_key();
		let op = ops.insert(());
		let mut cache = FactCache::new();
		let key = sample_key(2);
		let old_facts = sample_facts(EffectSet::READS_DATA);
		cache.insert(op, 0, key, old_facts);
		cache.invalidate(op);
		assert!(cache.last_under(op, &key).is_some());

		let summary = EffectSummary {
			set: EffectSet::CLOCK,
			fallibility: Fallibility::INFALLIBLE,
			determinism: Determinism::PerStatement,
		};
		cache.insert_summary(op, 1, key, summary);
		assert!(cache.last_under(op, &key).is_none());

		let new_facts = sample_facts(EffectSet::PURE);
		cache.insert(op, 2, key, new_facts.clone());
		assert_eq!(cache.summary(op, 1, &key), None);
		assert_eq!(cache.last_under(op, &key).map(|held| &held.effects), Some(&new_facts.effects));
	}
}
