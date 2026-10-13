//! The placement of a pipeline: the legality the logical plan allows and the decision made for it.
//!
//! A [`PlacementAttr`] is written only by modules inside the physical dialect: its constructor and
//! setters are visible to `exec::ir::phys` alone, so no logical or expression code can place a
//! pipeline. Everything outside reads it through the getters. Placement is never part of an op's
//! identity.

use slotmap::SecondaryMap;

use crate::exec::ir::iface::ids::OpId;
use crate::exec::logical::pipelines::{Legality, StatementLegality};
use crate::exec::planner::placement::{Decision, PhysicalFacts};

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
	/// Whether the pipeline's body cannot fail, so a morsel that overruns an ordered exit may be
	/// pruned. Answered from the transitive fallibility of the body.
	prunes: bool,
}

impl PlacementAttr {
	/// An attribute for a pipeline not yet placed.
	pub(super) fn new(legality: Legality, statement: StatementLegality) -> Self {
		Self {
			legality,
			statement,
			decision: None,
			facts: None,
			prunes: false,
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

	pub(super) fn set_prunes(&mut self, prunes: bool) {
		self.prunes = prunes;
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

	pub(crate) fn prunes(&self) -> bool {
		self.prunes
	}
}

/// The placement of every pipeline of one container, keyed by the id of its `phys.pipeline` op.
///
/// Never part of identity: two programs that differ only in placement hold equal ops.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlacementTable(SecondaryMap<OpId, PlacementAttr>);

impl PlacementTable {
	/// A table holding no placement.
	pub(crate) fn new() -> Self {
		Self(SecondaryMap::new())
	}

	/// The placement of the pipeline `op`, or `None` when it has none.
	pub(crate) fn get(&self, op: OpId) -> Option<&PlacementAttr> {
		self.0.get(op)
	}

	/// Gives each `to` of `pairs` the placement `source` holds for the `from` it is paired with.
	///
	/// A pair whose `from` has no placement is skipped. Use this when the container is a copy of
	/// code in the container `source` belongs to. The copy's pipelines then keep the placement of
	/// the originals and are not placed again.
	pub(in crate::exec::ir) fn carry(
		&mut self,
		source: &PlacementTable,
		pairs: impl IntoIterator<Item = (OpId, OpId)>,
	) {
		for (from, to) in pairs {
			if let Some(attr) = source.0.get(from) {
				self.0.insert(to, attr.clone());
			}
		}
	}

	/// Records the placement of the pipeline `op`. Returns the placement it replaced, if any.
	pub(super) fn insert(&mut self, op: OpId, attr: PlacementAttr) -> Option<PlacementAttr> {
		self.0.insert(op, attr)
	}
}
