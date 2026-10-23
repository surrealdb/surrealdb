//! The placement of a pipeline: the legality the logical plan allows and the decision made for it.
//!
//! A [`PlacementAttr`] is written only by modules inside the physical dialect: its constructor and
//! setters are visible to `exec::ir::phys` alone, so no logical or expression code can place a
//! pipeline. Everything outside reads it through the getters. Placement is never part of an op's
//! identity.

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use super::write_cut::WriteCut;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::side_map::SideMap;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::logical::pipelines::{Legality, StatementLegality};
use crate::exec::planner::placement::{Decision, MergeOrder, PhysicalFacts};

/// How one pipeline reads storage beyond what its ops state. Every field is conservative at its
/// default: no window, no read-ahead.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PipelineIo {
	/// The pipeline's `FETCH` reads each collection of links in windows.
	pub(crate) fetch_windows: bool,
	/// The pipeline's `.*` and field-over-array dereferences read their links in windows, and so
	/// does a value source read the records of the record ids an array of it holds.
	pub(crate) deref_windows: bool,
	/// The graph and reference hops of the pipeline whose sources are read in windows: one
	/// multi-get of their inline caches and one multi-range read of their adjacency keys. Empty
	/// where the pipeline reads none that way.
	pub(crate) frontier_hops: FrontierHops,
	/// The graph and reference lookups of the pipeline's projection that start from the source
	/// row read what their first hop holds for a batch of source rows before the rows run.
	pub(crate) frontier_rows: Option<FrontierRows>,
	/// The top-level link fields of the pipeline's `FETCH` or of its projection's dereferences,
	/// read for a batch of source rows before the rows run.
	pub(crate) rows_ahead: Option<RowsAhead>,
	/// The pipeline's scan reads its next chunk on a task of its own while the body runs the
	/// current one, where the body runs in the storage cursor.
	pub(crate) scan_read_ahead: bool,
}

/// The hops of a pipeline whose sources a window serves, by the graph-step ops that state them.
///
/// A hop that is not listed reads each source for itself. The ids are those of the container the
/// placement belongs to; [`PlacementTable::carry`] maps them onto a copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FrontierHops {
	served: Vec<OpId>,
}

impl FrontierHops {
	/// The hops `served`.
	pub(super) fn new(served: Vec<OpId>) -> Self {
		Self {
			served,
		}
	}

	/// Whether the hop the graph-step op `hop` states is served.
	pub(crate) fn serves(&self, hop: OpId) -> bool {
		self.served.contains(&hop)
	}

	/// The hops that `map` carries to another container; a hop it does not carry is unserved.
	fn mapped(&self, map: &impl Fn(OpId) -> Option<OpId>) -> Self {
		Self {
			served: self.served.iter().filter_map(|hop| map(*hop)).collect(),
		}
	}
}

/// What the plan states of the rows window of a pipeline's lookups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrontierRows {
	/// The chains of hops the projection runs over the row, each by the graph-step ops that state
	/// its hops in the order they are read: the first hop starts from the row and each later hop
	/// starts from the answer of the one before. Every hop of a chain is served
	/// ([`PipelineIo::frontier_hops`]). Never empty, and no chain is empty.
	pub(crate) chains: Vec<Vec<OpId>>,
	/// The paging clause that stands before the lookups, so the rows it skips and the rows past
	/// the page it admits never reach them, as its place among the paging clauses of the
	/// pipeline's body in the order the rows meet them; `None` where no clause pages the rows
	/// first.
	pub(crate) paging: Option<usize>,
}

impl FrontierRows {
	/// The chains of hops that `map` carries to another container, each cut before the first hop
	/// it does not carry; `None` where none is left.
	fn mapped(&self, map: &impl Fn(OpId) -> Option<OpId>) -> Option<Self> {
		let chains: Vec<Vec<OpId>> = self
			.chains
			.iter()
			.map(|chain| chain.iter().map_while(|hop| map(*hop)).collect::<Vec<_>>())
			.filter(|chain| !chain.is_empty())
			.collect();
		(!chains.is_empty()).then_some(Self {
			chains,
			paging: self.paging,
		})
	}
}

impl PipelineIo {
	/// The decision for the copy of a pipeline, `map` giving the op of the copy that stands for an
	/// op of the original. What names an op of the original and has no counterpart is dropped, so
	/// the copy reads those hops for itself.
	fn mapped(&self, map: &impl Fn(OpId) -> Option<OpId>) -> Self {
		Self {
			frontier_hops: self.frontier_hops.mapped(map),
			frontier_rows: self.frontier_rows.as_ref().and_then(|rows| rows.mapped(map)),
			..self.clone()
		}
	}
}

/// The fields of the source rows a pipeline's `FETCH`, or the field dereferences of its
/// projection, read the links of ahead of the rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowsAhead {
	pub(crate) fields: SmallVec<[Strand; 2]>,
	/// Whether the links are read for the dereferences of the projection (`link.field`) and not
	/// for a `FETCH`.
	pub(crate) derefs: bool,
	/// The paging clause that stands before the `FETCH`, so the rows it skips and the rows past
	/// the page it admits never reach the `FETCH`, as its place among the paging clauses of the
	/// pipeline's body in the order the rows meet them; `None` where no clause pages the rows
	/// first.
	pub(crate) paging: Option<usize>,
}

/// What is known and decided about one pipeline.
#[derive(Clone, Debug)]
pub(crate) struct PlacementAttr {
	/// What the logical plan allows, as the legality snapshot at the logical phase boundary
	/// judged it. The physical layer never recomputes it.
	legality: Legality,
	/// What the enclosing statement allows its pipelines.
	statement: StatementLegality,
	/// The placement decision. `None` until the pipeline is placed; lowering a pipeline that is
	/// not placed is an internal error.
	decision: Option<Decision>,
	/// The physical facts the decision was made from. `None` until the pipeline is placed.
	facts: Option<PhysicalFacts>,
	/// The pipelines of the chain the decision judged, this one first: the pipelines that continue
	/// it through the streaming ops and select loops that read its rows, up to the one whose
	/// result a sink reads. `None` until the pipeline is placed.
	chain: Option<usize>,
	/// Whether the pipeline raises for no row, so an entry that a top-k threshold shows worse than
	/// every row the sort keeps may be rejected before the body runs.
	prunes: bool,
	/// Whether the full-text scan of the pipeline may reject a hit by the threshold of the sort
	/// ordered first by the clause's score, though the pipeline does not prune.
	///
	/// Narrower than [`Self::prunes`] and read by nothing else: it holds where every op a rejected
	/// hit would skip raises for no row except that clause's `search::score` call, whose value the
	/// scan computed for the hit before rejecting it. A hit the scan holds no score for is never
	/// rejected, so its call still runs.
	prunes_by_score: bool,
	/// How the pipeline reads storage. Placement is not part of op identity, so it changes no
	/// digest and is never printed.
	io: PipelineIo,
	/// Where the iterations of a write loop that fans out are cut in two: the compute half runs
	/// ahead of the cut on any task and the rest in iteration order. `None` for every pipeline
	/// whose decision is not an ordered apply.
	write_cut: Option<WriteCut>,
	/// In what order the merge of the pipeline's morsels takes their results; serial for a
	/// pipeline that runs serially.
	merge: MergeOrder,
	/// The places of the accumulators whose terms the morsels of the pipeline keep for the merge
	/// to fold in the order of the serial run
	/// ([`crate::exec::planner::placement::MorselPlan::ordered_fold`]); empty when none do.
	ordered_fold: Vec<usize>,
}

impl PlacementAttr {
	/// An attribute for a pipeline not yet placed.
	pub(super) fn new(legality: Legality, statement: StatementLegality) -> Self {
		Self {
			legality,
			statement,
			decision: None,
			facts: None,
			chain: None,
			prunes: false,
			prunes_by_score: false,
			io: PipelineIo::default(),
			write_cut: None,
			merge: MergeOrder::Serial,
			ordered_fold: Vec::new(),
		}
	}

	pub(super) fn set_legality(&mut self, legality: Legality) {
		self.legality = legality;
	}

	pub(super) fn set_statement(&mut self, statement: StatementLegality) {
		self.statement = statement;
	}

	pub(super) fn set_decision(&mut self, decision: Option<Decision>) {
		self.decision = decision;
	}

	pub(super) fn set_facts(&mut self, facts: Option<PhysicalFacts>) {
		self.facts = facts;
	}

	pub(super) fn set_chain(&mut self, chain: Option<usize>) {
		self.chain = chain;
	}

	pub(super) fn set_prunes(&mut self, prunes: bool) {
		self.prunes = prunes;
	}

	pub(super) fn set_prunes_by_score(&mut self, prunes_by_score: bool) {
		self.prunes_by_score = prunes_by_score;
	}

	pub(super) fn set_io(&mut self, io: PipelineIo) {
		self.io = io;
	}

	pub(super) fn set_write_cut(&mut self, write_cut: Option<WriteCut>) {
		self.write_cut = write_cut;
	}

	pub(super) fn set_merge(&mut self, merge: MergeOrder) {
		self.merge = merge;
	}

	pub(super) fn set_ordered_fold(&mut self, ordered_fold: Vec<usize>) {
		self.ordered_fold = ordered_fold;
	}

	pub(crate) fn legality(&self) -> &Legality {
		&self.legality
	}

	pub(crate) fn statement(&self) -> &StatementLegality {
		&self.statement
	}

	pub(crate) fn decision(&self) -> Option<Decision> {
		self.decision
	}

	pub(crate) fn facts(&self) -> Option<&PhysicalFacts> {
		self.facts.as_ref()
	}

	pub(crate) fn chain(&self) -> Option<usize> {
		self.chain
	}

	pub(crate) fn prunes(&self) -> bool {
		self.prunes
	}

	pub(crate) fn prunes_by_score(&self) -> bool {
		self.prunes_by_score
	}

	pub(crate) fn io(&self) -> &PipelineIo {
		&self.io
	}

	pub(crate) fn write_cut(&self) -> Option<WriteCut> {
		self.write_cut
	}

	pub(crate) fn merge(&self) -> MergeOrder {
		self.merge
	}

	pub(crate) fn ordered_fold(&self) -> &[usize] {
		&self.ordered_fold
	}
}

/// The placement of every pipeline of one container, keyed by the id of its `phys.pipeline` op.
///
/// Never part of identity: two programs that differ only in placement hold equal ops.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlacementTable(SideMap<OpId, PlacementAttr>);

impl PlacementTable {
	/// A table holding no placement.
	pub(crate) fn new() -> Self {
		Self(SideMap::new())
	}

	/// The pipelines that hold a placement.
	pub(super) fn pipelines(&self) -> impl Iterator<Item = OpId> + '_ {
		self.0.keys()
	}

	/// The placement of the pipeline `op`, or `None` when it has none.
	pub(crate) fn get(&self, op: OpId) -> Option<&PlacementAttr> {
		self.0.get(op)
	}

	/// Gives each `to` of `pairs` the placement `source` holds for the `from` it is paired with.
	///
	/// A pair whose `from` has no placement is skipped. Use this when the container is a copy of
	/// code in the container `source` belongs to. The copy's pipelines then keep the placement of
	/// the originals and are not placed again. The ops a placement names inside its pipeline are
	/// carried through the same pairs.
	pub(in crate::exec::ir) fn carry(
		&mut self,
		source: &PlacementTable,
		pairs: impl IntoIterator<Item = (OpId, OpId)>,
	) {
		let pairs: Vec<(OpId, OpId)> = pairs.into_iter().collect();
		let copies: FxHashMap<OpId, OpId> = pairs.iter().copied().collect();
		let map = |original: OpId| copies.get(&original).copied();
		for &(from, to) in &pairs {
			if let Some(attr) = source.0.get(from) {
				let mut carried = attr.clone();
				carried.io = attr.io.mapped(&map);
				self.0.insert(to, carried);
			}
		}
	}

	/// States how the pipeline `op` reads storage. A pipeline with no placement is left as it is.
	pub(super) fn set_io(&mut self, op: OpId, io: PipelineIo) {
		if let Some(attr) = self.0.get_mut(op) {
			attr.set_io(io);
		}
	}

	/// Records the placement of the pipeline `op`. Returns the placement it replaced, if any.
	pub(super) fn insert(&mut self, op: OpId, attr: PlacementAttr) -> Option<PlacementAttr> {
		self.0.insert(op, attr)
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	fn ops(count: usize) -> Vec<OpId> {
		let mut ops: slotmap::SlotMap<OpId, ()> = slotmap::SlotMap::with_key();
		(0..count).map(|_| ops.insert(())).collect()
	}

	#[test]
	fn the_ops_a_placement_names_follow_the_copy_of_their_pipeline() {
		let all = ops(8);
		let (before, after) = all.split_at(4);
		let copies: FxHashMap<OpId, OpId> =
			before.iter().copied().zip(after.iter().copied()).collect();
		let map = |op: OpId| copies.get(&op).copied();
		let io = PipelineIo {
			frontier_hops: FrontierHops::new(vec![before[0], before[2]]),
			frontier_rows: Some(FrontierRows {
				chains: vec![vec![before[0], before[1]], vec![before[2]]],
				paging: Some(0),
			}),
			..PipelineIo::default()
		};
		let carried = io.mapped(&map);
		assert!(carried.frontier_hops.serves(after[0]) && carried.frontier_hops.serves(after[2]));
		assert!(
			!carried.frontier_hops.serves(before[0]) && !carried.frontier_hops.serves(after[1])
		);
		assert_eq!(
			carried.frontier_rows,
			Some(FrontierRows {
				chains: vec![vec![after[0], after[1]], vec![after[2]]],
				paging: Some(0),
			})
		);
	}

	#[test]
	fn what_the_copy_does_not_hold_is_read_for_itself() {
		let all = ops(6);
		let (before, after) = all.split_at(3);
		// Only the first two hops are copied.
		let copies: FxHashMap<OpId, OpId> =
			before.iter().copied().zip(after.iter().copied()).take(2).collect();
		let map = |op: OpId| copies.get(&op).copied();
		let io = PipelineIo {
			frontier_hops: FrontierHops::new(vec![before[1], before[2]]),
			frontier_rows: Some(FrontierRows {
				chains: vec![vec![before[0], before[2], before[1]], vec![before[2]]],
				paging: None,
			}),
			..PipelineIo::default()
		};
		let carried = io.mapped(&map);
		assert!(carried.frontier_hops.serves(after[1]));
		assert!(!carried.frontier_hops.serves(before[2]));
		// A chain ends before the first hop that has no counterpart, and a chain left with none
		// is dropped.
		assert_eq!(
			carried.frontier_rows,
			Some(FrontierRows {
				chains: vec![vec![after[0]]],
				paging: None,
			})
		);
		let none = PipelineIo {
			frontier_rows: Some(FrontierRows {
				chains: vec![vec![before[2]]],
				paging: None,
			}),
			..PipelineIo::default()
		};
		assert_eq!(none.mapped(&map).frontier_rows, None);
	}
}
