//! The fixed physical extraction cost.
//!
//! [`PhysFixedCost`] prices a node from its own tag and the sum of its kids' costs. It reads no
//! `Policy`, no `Signals`, no data and no estimate: `ClassFacts::estimate` and
//! `PhysTableFacts::estimate` are the slot a statistics-backed cost fills. The weights are an
//! access-path and algorithm decision, so they live with the physical layer.
//!
//! `illegal` is the disjunction, and `work`, `novelty` and `rank` are saturating sums, over the
//! kids and the node's own term. `prov` is the greatest class ordinal among the node and its kids.
//! A node therefore never costs less than a kid on any field, which is what makes extraction's
//! fixpoint settle, and the function is total over every tag. `novelty` counts the nodes the
//! conversion did not seed, so a plan of seeds beats an equal-work plan of rule-created nodes.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::{PhysOp, PhysTag};
use crate::exec::ir::egraph::decl::{
	CostHook, CostNode, EgraphView, NodeShape, PayloadId, PlanCost,
};
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::op_kind::{OpKind, OpTag};

/// The cost hook of the physical extraction round.
pub(crate) struct PhysFixedCost;

/// The one [`PhysFixedCost`] the registry offers.
pub(crate) static PHYS_FIXED_COST: PhysFixedCost = PhysFixedCost;

/// Work of an operation that reads no stored data and computes nothing.
const FREE: u64 = 0;
/// Work of a scalar operation, a record lookup and the physical operators that only forward rows.
const UNIT: u64 = 1;
/// Work of a function call.
const CALL: u64 = 4;
/// Work of a read through a unique key, which lists at most one record.
const UNIQUE_KEY_READ: u64 = 2;
/// Work of an index read, a key-range read, a full-text read and a nearest-neighbour read.
const INDEX_READ: u64 = 8;
/// Work of a sort that keeps a bounded top.
const TOP_K_SORT: u64 = 8;
/// Work of a hash aggregation and of a distinct.
const GROUPING: u64 = 8;
/// Work of a record dereference.
const DEREF: u64 = 16;
/// Work of a sort that keeps every row.
const SORT: u64 = 16;
/// Work of a nested statement.
const SUBQUERY: u64 = 32;
/// Work of a read that merges several index reads.
const MERGED_READ: u64 = 12;
/// Work of a scan of every record of a table.
const TABLE_SCAN: u64 = 64;

impl CostHook for PhysFixedCost {
	fn node(&self, n: &CostNode<'_>, kids: &[PlanCost]) -> PlanCost {
		let mut illegal = matches!(
			n.node.identity.shape,
			NodeShape::Op {
				tag: OpTag::Logical(_),
				..
			}
		);
		let mut work = own_work(n);
		let mut novelty = u32::from(!n.seed);
		let mut rank = n.rule_rank;
		let mut prov = n.view.facts(n.class).ordinal;
		for kid in kids {
			illegal |= kid.illegal;
			work = work.saturating_add(kid.work);
			novelty = novelty.saturating_add(kid.novelty);
			rank = rank.saturating_add(kid.rank);
			prov = prov.max(kid.prov);
		}
		PlanCost {
			illegal,
			work,
			novelty,
			rank,
			prov,
		}
	}
}

/// The work of the node itself, without its kids.
fn own_work(n: &CostNode<'_>) -> u64 {
	match n.node.identity.shape {
		NodeShape::Op {
			tag,
			payload,
		} => match tag {
			OpTag::Expr(tag) => expr_work(tag),
			// A logical node is illegal whatever its work, so its own term is the least one.
			OpTag::Logical(_) => FREE,
			OpTag::Phys(tag) => phys_work(tag, payload, n.view),
		},
		NodeShape::Region {
			..
		}
		| NodeShape::Arg {
			..
		}
		| NodeShape::Opaque {
			..
		} => FREE,
	}
}

/// The work of an expression op. Reads of a literal, a field, the document, a parameter or a
/// block argument cost nothing; every other scalar op costs one unit, except a function call, a
/// record dereference and a subquery.
fn expr_work(tag: ExprTag) -> u64 {
	match tag {
		ExprTag::Literal
		| ExprTag::FreeParam
		| ExprTag::SessionParam
		| ExprTag::CatalogParam
		| ExprTag::Field
		| ExprTag::ComputedRead
		| ExprTag::Doc => FREE,
		ExprTag::Call | ExprTag::Method | ExprTag::Invoke | ExprTag::Agg => CALL,
		ExprTag::Deref => DEREF,
		ExprTag::Subquery => SUBQUERY,
		ExprTag::Array
		| ExprTag::Set
		| ExprTag::Object
		| ExprTag::RecordId
		| ExprTag::Table
		| ExprTag::Mock
		| ExprTag::Constant
		| ExprTag::Access
		| ExprTag::Index
		| ExprTag::First
		| ExprTag::Last
		| ExprTag::Each
		| ExprTag::Flatten
		| ExprTag::Dispatch
		| ExprTag::Destructure
		| ExprTag::Optional
		| ExprTag::RepeatRecurse
		| ExprTag::SetField
		| ExprTag::TupleRow
		| ExprTag::Prefix
		| ExprTag::Convert
		| ExprTag::Postfix
		| ExprTag::Binary
		| ExprTag::ShortCircuit
		| ExprTag::ClosureCreate
		| ExprTag::If
		| ExprTag::Throw
		| ExprTag::Yield
		| ExprTag::Truthy
		| ExprTag::Now
		| ExprTag::Raise
		| ExprTag::DepthCharge => UNIT,
	}
}

/// The work of a physical op. A payload the view does not hold takes the tag's weight with no
/// refinement.
fn phys_work(tag: PhysTag, payload: PayloadId, view: &dyn EgraphView) -> u64 {
	match tag {
		PhysTag::TableScan => TABLE_SCAN,
		PhysTag::IndexScan => {
			if is_unique_exact_scan(payload, view) {
				UNIQUE_KEY_READ
			} else {
				INDEX_READ
			}
		}
		PhysTag::FullTextScan | PhysTag::KeyRangeScan | PhysTag::KnnScan | PhysTag::KnnTopK => {
			INDEX_READ
		}
		PhysTag::IndexUnion | PhysTag::Bitmap => MERGED_READ,
		PhysTag::Sort => {
			let top_k = matches!(
				view.payload(payload),
				Some(OpKind::Phys(PhysOp::Sort(attrs))) if attrs.keep.is_some()
			);
			if top_k {
				TOP_K_SORT
			} else {
				SORT
			}
		}
		PhysTag::Aggregate | PhysTag::Distinct | PhysTag::Window => GROUPING,
		PhysTag::RecordScan | PhysTag::Count => UNIT,
		PhysTag::IndexLookup
		| PhysTag::Vacant
		| PhysTag::Enforced
		| PhysTag::Values
		| PhysTag::Subquery
		| PhysTag::Probe
		| PhysTag::Union
		| PhysTag::Join
		| PhysTag::PermissionFilter
		| PhysTag::Layer
		| PhysTag::Filter
		| PhysTag::Fetch
		| PhysTag::Split
		| PhysTag::Limit
		| PhysTag::Project
		| PhysTag::Eval
		| PhysTag::Request
		| PhysTag::Txn
		| PhysTag::Stmt
		| PhysTag::Statement
		| PhysTag::Match
		| PhysTag::MatchStep
		| PhysTag::DdlGuard
		| PhysTag::Define
		| PhysTag::Remove
		| PhysTag::Alter
		| PhysTag::Rebuild
		| PhysTag::Info
		| PhysTag::Use
		| PhysTag::Option
		| PhysTag::Access
		| PhysTag::Kill
		| PhysTag::Live
		| PhysTag::Subscriber
		| PhysTag::Show
		| PhysTag::Write
		| PhysTag::WriteStage
		| PhysTag::Events
		| PhysTag::Timeout
		| PhysTag::VersionScope
		| PhysTag::Only
		| PhysTag::Explain
		| PhysTag::Assign
		| PhysTag::Unset
		| PhysTag::Content
		| PhysTag::Block
		| PhysTag::Let
		| PhysTag::Sleep
		| PhysTag::If
		| PhysTag::Loop
		| PhysTag::Pipeline
		| PhysTag::Choose
		| PhysTag::Prefetch
		| PhysTag::Materialize
		| PhysTag::LoadUnit
		| PhysTag::Yield
		| PhysTag::Return
		| PhysTag::Break
		| PhysTag::Next => UNIT,
	}
}

/// Whether the index scan `payload` interns lists exactly the records its access names through a
/// unique index of the table, as the view's candidates for that table state it.
fn is_unique_exact_scan(payload: PayloadId, view: &dyn EgraphView) -> bool {
	let Some(OpKind::Phys(PhysOp::IndexScan(attrs))) = view.payload(payload) else {
		return false;
	};
	attrs.exact()
		&& view.table(&attrs.table).is_some_and(|facts| {
			facts.indexes.iter().any(|candidate| candidate.index == attrs.index && candidate.unique)
		})
}

#[cfg(test)]
mod tests {
	use smallvec::SmallVec;
	use surrealdb_strand::{Strand, TableName};

	use super::*;
	use crate::exec::ir::egraph::decl::{
		ClassFacts, ClassRef, ENodeSnapshot, EpochKey, IndexCandidate, IndexKindClass,
		NodeIdentity, NodeSpec, PhysTableFacts,
	};
	use crate::exec::ir::iface::choose::IndexRef;
	use crate::exec::ir::iface::effects::{Determinism, FieldSet};
	use crate::exec::ir::iface::fallibility::Fallibility;
	use crate::exec::ir::iface::ids::{FrameId, SnapshotId};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::logical::ops::LogTag;
	use crate::exec::ir::phys::ops::{IndexScanAttrs, ReadClauses, SortAttrs, TableScanAttrs};
	use crate::exec::vm::payload::Spill;
	use crate::expr::logical::EffectSet;
	use crate::kvs::Direction;
	use crate::val::Value;

	fn table() -> TableName {
		TableName::new(Strand::new("t"))
	}

	fn index_ref(name: &str) -> IndexRef {
		IndexRef {
			table: table(),
			index: Strand::new(name),
			position: 0,
			scalar: Box::default(),
		}
	}

	#[test]
	fn two_sources_of_one_table_meet_to_what_both_admit() {
		let candidate = |name: &str, unique: bool| IndexCandidate {
			index: index_ref(name),
			columns: SmallVec::new(),
			unique,
			kind: IndexKindClass::BTree,
		};
		let facts = |indexes: Vec<IndexCandidate>, withheld: &str, current: bool| PhysTableFacts {
			table: table(),
			indexes: SmallVec::from_vec(indexes),
			withheld_order: FieldSet::Some(SmallVec::from_vec(vec![Strand::new(withheld)])),
			hint: None,
			current_snapshot: current,
			estimate: None,
		};
		let outer = facts(vec![candidate("by_key", true), candidate("by_other", false)], "x", true);
		let inner = facts(vec![candidate("by_key", false)], "y", false);
		let met = outer.meet(&inner);
		let kept: Vec<(IndexRef, bool)> =
			met.indexes.iter().map(|held| (held.index.clone(), held.unique)).collect();
		assert_eq!(
			kept,
			vec![(index_ref("by_key"), false)],
			"only what both admit, unique by both"
		);
		assert_eq!(
			met.withheld_order,
			FieldSet::Some(SmallVec::from_vec(vec![Strand::new("x"), Strand::new("y")]))
		);
		assert!(!met.current_snapshot);
		assert_eq!(inner.meet(&outer).withheld_order, met.withheld_order);
	}

	fn facts(ordinal: u32) -> ClassFacts {
		ClassFacts {
			effects: EffectSet::PURE,
			fallibility: Fallibility::INFALLIBLE,
			determinism: Determinism::Deterministic,
			frame: Some(FrameId::new(0)),
			snapshot: Some(SnapshotId::new(0)),
			reads_written: false,
			ty: TypeKnowledge::Unknown,
			free_binders: SmallVec::new(),
			in_permission_frame: false,
			provenance: SmallVec::new(),
			ordinal,
			seed_order: None,
			total_order_ok: false,
			estimate: None,
		}
	}

	/// A view over a hand-built class table, an interned payload list and one table's facts.
	struct TestView {
		classes: Vec<ClassFacts>,
		payloads: Vec<OpKind>,
		table_facts: Option<PhysTableFacts>,
	}

	impl TestView {
		fn new(payloads: Vec<OpKind>, unique: bool) -> Self {
			Self {
				classes: vec![facts(0), facts(1)],
				payloads,
				table_facts: Some(PhysTableFacts {
					table: table(),
					indexes: SmallVec::from_vec(vec![
						IndexCandidate {
							index: index_ref("by_key"),
							columns: SmallVec::new(),
							unique,
							kind: IndexKindClass::BTree,
						},
						IndexCandidate {
							index: index_ref("by_other"),
							columns: SmallVec::new(),
							unique: false,
							kind: IndexKindClass::BTree,
						},
					]),
					withheld_order: FieldSet::Some(SmallVec::new()),
					hint: None,
					current_snapshot: true,
					estimate: None,
				}),
			}
		}
	}

	impl EgraphView for TestView {
		fn find(&self, c: ClassRef) -> ClassRef {
			c
		}

		fn facts(&self, c: ClassRef) -> &ClassFacts {
			&self.classes[c.0 as usize]
		}

		fn nodes(&self, _c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
			SmallVec::new()
		}

		fn payload(&self, p: PayloadId) -> Option<&OpKind> {
			self.payloads.get(p.0 as usize)
		}

		fn table(&self, t: &TableName) -> Option<&PhysTableFacts> {
			self.table_facts.as_ref().filter(|facts| facts.table == *t)
		}

		fn total_order_ok(&self, c: ClassRef) -> bool {
			self.facts(c).total_order_ok
		}

		fn is_seed(&self, _n: &ENodeSnapshot) -> bool {
			false
		}

		fn add(&mut self, spec: NodeSpec) -> ClassRef {
			spec.frame_of
		}
	}

	fn snapshot(tag: OpTag, payload: u32) -> ENodeSnapshot {
		ENodeSnapshot {
			identity: NodeIdentity {
				shape: NodeShape::Op {
					tag,
					payload: PayloadId(payload),
				},
				frame: FrameId::new(0),
				snapshot: SnapshotId::new(0),
				epoch: EpochKey::None,
			},
			kids: SmallVec::new(),
		}
	}

	fn price(
		view: &TestView,
		tag: OpTag,
		payload: u32,
		seed: bool,
		rule_rank: u32,
		kids: &[PlanCost],
	) -> PlanCost {
		let node = snapshot(tag, payload);
		PHYS_FIXED_COST.node(
			&CostNode {
				class: ClassRef(1),
				node: &node,
				view,
				rule_rank,
				seed,
			},
			kids,
		)
	}

	fn leaf(work: u64) -> PlanCost {
		PlanCost {
			illegal: false,
			work,
			novelty: 0,
			rank: 0,
			prov: 0,
		}
	}

	fn table_scan() -> OpKind {
		OpKind::Phys(PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: table(),
			direction: Direction::Forward,
			topk: None,
			lightweight: false,
			id_order: true,
			read: ReadClauses::default(),
		})))
	}

	fn index_scan(index: &str, exact: bool) -> OpKind {
		OpKind::Phys(PhysOp::IndexScan(Box::new(IndexScanAttrs {
			table: table(),
			index: index_ref(index),
			access: crate::exec::index::access_path::BTreeAccess::literal_equality(Value::None),
			direction: Direction::Forward,
			ordered_by: None,
			pinned: exact.then(|| vec![crate::exec::field_path::FieldPath::field("a")]),
			stated: None,
			rechecked: false,
			decode: None,
			read: ReadClauses::default(),
		})))
	}

	fn sort(keep: Option<usize>) -> OpKind {
		OpKind::Phys(PhysOp::Sort(Box::new(SortAttrs {
			keys: Vec::new(),
			keep,
			pushdown: false,
			random: false,
			spill: Spill::of_sort(keep),
			tempfiles: false,
		})))
	}

	fn phys(tag: PhysTag) -> OpTag {
		OpTag::Phys(tag)
	}

	#[test]
	fn a_logical_node_is_illegal() {
		let view = TestView::new(Vec::new(), true);
		let logical = price(&view, OpTag::Logical(LogTag::Filter), 0, true, 0, &[]);
		assert!(logical.illegal);
		// A node over an illegal kid is illegal, and a node over legal kids is not.
		let over = price(&view, phys(PhysTag::Filter), 0, true, 0, &[logical]);
		assert!(over.illegal);
		let legal = price(&view, phys(PhysTag::Filter), 0, true, 0, &[leaf(1)]);
		assert!(!legal.illegal);
	}

	#[test]
	fn cost_is_monotone() {
		let view = TestView::new(
			vec![table_scan(), index_scan("by_key", true), sort(Some(3)), sort(None)],
			true,
		);
		let kid = PlanCost {
			illegal: false,
			work: 7,
			novelty: 2,
			rank: 3,
			prov: 9,
		};
		let nodes = [
			(phys(PhysTag::TableScan), 0),
			(phys(PhysTag::IndexScan), 1),
			(phys(PhysTag::Sort), 2),
			(phys(PhysTag::Sort), 3),
			(phys(PhysTag::Filter), 0),
			(phys(PhysTag::Pipeline), 0),
			(OpTag::Expr(ExprTag::Literal), 0),
			(OpTag::Expr(ExprTag::Subquery), 0),
		];
		for (tag, payload) in nodes {
			for seed in [true, false] {
				let cost = price(&view, tag, payload, seed, 1, &[kid, kid]);
				assert!(cost.work >= kid.work, "{tag:?}");
				assert!(cost.novelty >= kid.novelty, "{tag:?}");
				assert!(cost.rank >= kid.rank, "{tag:?}");
				assert!(cost.prov >= kid.prov, "{tag:?}");
				assert!(cost >= kid, "{tag:?}");
			}
		}
		let saturated = leaf(u64::MAX);
		assert_eq!(price(&view, phys(PhysTag::TableScan), 0, true, 0, &[saturated]).work, u64::MAX);
	}

	#[test]
	fn the_seed_wins_a_tie() {
		let view = TestView::new(vec![table_scan()], true);
		let seed = price(&view, phys(PhysTag::TableScan), 0, true, 0, &[]);
		let created = price(&view, phys(PhysTag::TableScan), 0, false, 1, &[]);
		assert_eq!(seed.work, created.work);
		assert!(seed < created);
		assert_eq!(seed.novelty, 0);
		assert_eq!(created.novelty, 1);
		assert_eq!(created.rank, 1);
		assert_eq!(created.prov, 1);
	}

	#[test]
	fn a_table_scan_costs_more_than_an_index_scan() {
		let view = TestView::new(
			vec![table_scan(), index_scan("by_other", false), index_scan("by_key", false)],
			false,
		);
		let table_scan = price(&view, phys(PhysTag::TableScan), 0, true, 0, &[]);
		for payload in [1, 2] {
			let index = price(&view, phys(PhysTag::IndexScan), payload, true, 0, &[]);
			assert!(table_scan.work > index.work);
		}
	}

	#[test]
	fn a_unique_exact_index_scan_is_cheapest_of_the_index_reads() {
		let view = TestView::new(
			vec![
				index_scan("by_key", true),
				index_scan("by_key", false),
				index_scan("by_other", true),
			],
			true,
		);
		let unique_exact = price(&view, phys(PhysTag::IndexScan), 0, true, 0, &[]).work;
		let inexact = price(&view, phys(PhysTag::IndexScan), 1, true, 0, &[]).work;
		let not_unique = price(&view, phys(PhysTag::IndexScan), 2, true, 0, &[]).work;
		let unheld = price(&view, phys(PhysTag::IndexScan), 9, true, 0, &[]).work;
		assert!(unique_exact < inexact);
		assert!(unique_exact < not_unique);
		assert!(unique_exact < unheld);
		for tag in [
			PhysTag::FullTextScan,
			PhysTag::KnnScan,
			PhysTag::KnnTopK,
			PhysTag::IndexUnion,
			PhysTag::Bitmap,
			PhysTag::TableScan,
		] {
			assert!(unique_exact < price(&view, phys(tag), 9, true, 0, &[]).work, "{tag:?}");
		}
	}
}
