//! The e-graph's boundary types: handles, node identity, class facts, rule declarations, the view
//! a named applier and a cost hook see, and extraction cost.
//!
//! Everything here is plain data or a trait over plain data, and names no `egg` type. A
//! [`RuleDecl`] and everything it holds borrow only `'static` data, so a declaration is a `static`.
//! Handles and interned ids are numbered per e-graph, in import or creation order, and are never
//! compared across graphs.

use std::collections::HashMap;

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::{IdMap, StatementCopy};
use crate::exec::ir::expr::ops::ExprTag;
use crate::exec::ir::iface::choose::IndexRef;
use crate::exec::ir::iface::effects::{Determinism, FieldSet};
use crate::exec::ir::iface::egraph::{RuleDialect, SideCondition};
use crate::exec::ir::iface::estimate::Estimate;
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::With;
use crate::expr::logical::EffectSet;

/// An e-class at the boundary. Every [`EgraphView`] method canonicalises a `ClassRef` through
/// `find` on entry, so a handle taken before a union stays usable after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ClassRef(pub(crate) u32);

/// An e-node captured by value at search time.
///
/// No boundary handle is a position in a class's node list, because a union or a rebuild reorders
/// that list.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ENodeSnapshot {
	pub(crate) identity: NodeIdentity,
	pub(crate) kids: SmallVec<[ClassRef; 4]>,
}

/// The interned `(tag, attribute digest)` of an op: equal ids mean equal identity attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PayloadId(pub(crate) u32);

/// The binder of one region's block arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct BinderId(pub(crate) u32);

impl BinderId {
	/// The binder of a region that has no block arguments.
	pub(crate) const NONE: BinderId = BinderId(u32::MAX);
}

/// An opaque leaf: equal to nothing but itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct LeafId(pub(crate) u32);

/// The value identity of an op as an e-node head. The kids are not part of it: the e-graph
/// compares them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct NodeIdentity {
	pub(crate) shape: NodeShape,
	pub(crate) frame: FrameId,
	pub(crate) snapshot: SnapshotId,
	pub(crate) epoch: EpochKey,
}

/// What an e-node stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum NodeShape {
	/// An op. `payload` interns the tag and the attribute digest.
	Op {
		tag: OpTag,
		payload: PayloadId,
	},
	/// A structural region of one block. `kind_key` is the region kind's key, `binder` the binder
	/// of the region's block arguments ([`BinderId::NONE`] when it has none) and `results` the
	/// yield arity.
	Region {
		kind_key: u16,
		binder: BinderId,
		results: u8,
	},
	/// Block argument `index` of the region whose binder is `binder`.
	Arg {
		binder: BinderId,
		index: u8,
	},
	/// Equal to nothing else: an op whose value key is opaque, or an isolated or effectful region.
	Opaque {
		leaf: LeafId,
	},
}

/// The write-epoch term of an identity, renumbered per graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum EpochKey {
	/// The op reads no stored data.
	None,
	/// Reads no write of the statement can reach: equal numbers are one value.
	Stable(u32),
	/// A read a write can reach: equal to no other.
	Unique(u32),
}

/// The facts of one e-class. Owned data: it borrows neither the container nor the fact cache.
#[derive(Clone, Debug)]
pub(crate) struct ClassFacts {
	pub(crate) effects: EffectSet,
	pub(crate) fallibility: Fallibility,
	pub(crate) determinism: Determinism,
	/// `None` only after two merged nodes disagreed, which is a guard failure.
	pub(crate) frame: Option<FrameId>,
	pub(crate) snapshot: Option<SnapshotId>,
	/// Some read in the class meets a write of the statement.
	pub(crate) reads_written: bool,
	pub(crate) ty: TypeKnowledge,
	pub(crate) free_binders: SmallVec<[BinderId; 2]>,
	pub(crate) in_permission_frame: bool,
	/// The logical ops the class stands for, sorted and without duplicates.
	pub(crate) provenance: SmallVec<[OpId; 2]>,
	/// The tie-rule ordinal: an imported class's import order, and for a rule-created class the
	/// ordinal of the class its rule matched as root. Never an e-graph id.
	pub(crate) ordinal: u32,
	/// The delivery order of the class's seed node.
	pub(crate) seed_order: Option<OrderSig>,
	/// Row-producer classes of a seeded run only: whether the producer's order may change
	/// (`false` everywhere else).
	pub(crate) total_order_ok: bool,
	pub(crate) estimate: Option<Estimate>,
}

/// The delivery order of a row-producing node. Scalar classes are [`OrderSig::Unordered`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum OrderSig {
	Unordered,
	IdAsc,
	IdDesc,
	Index {
		index: IndexRef,
		reverse: bool,
	},
	Positional,
	/// The order of a key-range scan over the components of the record id key: by the component
	/// at each position in turn, in the scan's direction. A position the access pins to one value
	/// orders either way; the access is the op's, which the consumer reads.
	KeyComponents {
		positions: Vec<u32>,
		reverse: bool,
	},
	Opaque(u32),
}

/// The facts of one row source at the bound catalog version.
///
/// A source has an entry only where an index may be chosen for it.
#[derive(Clone, Debug)]
pub(crate) struct PhysTableFacts {
	pub(crate) table: TableName,
	/// The admissible access candidates of the source, as the physical layer answers them.
	pub(crate) indexes: SmallVec<[IndexCandidate; 4]>,
	/// Fields whose value order the principal may not observe: no alternative may deliver order
	/// over them.
	pub(crate) withheld_order: FieldSet,
	/// The statement's `WITH` clause for this source.
	pub(crate) hint: Option<With>,
	/// The source reads the statement's current snapshot (no `VERSION`).
	pub(crate) current_snapshot: bool,
	pub(crate) estimate: Option<Estimate>,
}

impl PhysTableFacts {
	/// The facts that hold for every one of two row sources of the same table, which a graph
	/// keyed by table holds as one entry: the index candidates both admit, unique only where both
	/// say so; the fields either withholds the order of; the hint and estimate only where both
	/// agree; and the current snapshot only where both read it.
	pub(crate) fn meet(&self, other: &Self) -> Self {
		let indexes = self
			.indexes
			.iter()
			.filter_map(|candidate| {
				let held = other.indexes.iter().find(|held| held.index == candidate.index)?;
				let mut kept = candidate.clone();
				kept.unique = candidate.unique && held.unique;
				Some(kept)
			})
			.collect();
		let withheld_order = match (&self.withheld_order, &other.withheld_order) {
			(FieldSet::Some(left), FieldSet::Some(right)) => {
				let mut fields = left.clone();
				fields.extend(right.iter().filter(|field| !left.contains(field)).cloned());
				fields.sort_unstable();
				FieldSet::Some(fields)
			}
			(FieldSet::All, _) | (_, FieldSet::All) => FieldSet::All,
		};
		Self {
			table: self.table.clone(),
			indexes,
			withheld_order,
			hint: if self.hint == other.hint {
				self.hint.clone()
			} else {
				None
			},
			current_snapshot: self.current_snapshot && other.current_snapshot,
			estimate: if self.estimate == other.estimate {
				self.estimate
			} else {
				None
			},
		}
	}
}

/// An index a row source may read through.
#[derive(Clone, Debug)]
pub(crate) struct IndexCandidate {
	pub(crate) index: IndexRef,
	pub(crate) columns: SmallVec<[Strand; 4]>,
	pub(crate) unique: bool,
	pub(crate) kind: IndexKindClass,
}

/// The family of an index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IndexKindClass {
	BTree,
	FullText,
	Hnsw,
	DiskAnn,
	Count,
	Spatial,
}

/// When a rule runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuleTier {
	Normal,
	Explore,
}

/// A rewrite rule as data.
#[derive(Clone, Debug)]
pub(crate) struct RuleDecl {
	pub(crate) name: &'static str,
	pub(crate) dialect: RuleDialect,
	pub(crate) tier: RuleTier,
	pub(crate) lhs: Pat,
	pub(crate) rhs: Rhs,
	pub(crate) when: &'static [SideCondition],
	/// Why the rule is sound; required when `when` is empty, and printed with the rule.
	pub(crate) sound: Option<&'static str>,
	/// Left-hand variables the right side drops because the new form answers them. Each must
	/// still be infallible.
	pub(crate) answers: &'static [&'static str],
	/// For a [`Rhs::Named`] right side, the left-hand variables the applier may drop. Empty
	/// otherwise.
	pub(crate) drops: &'static [&'static str],
	/// For a [`Rhs::Named`] right side, the left-hand variables the applier may duplicate. Empty
	/// otherwise.
	pub(crate) duplicates: &'static [&'static str],
	/// The rule's only benefit is an access-path change, so it can win extraction only under a
	/// total `ORDER BY` key.
	pub(crate) needs_total_order: bool,
}

/// A left-hand-side pattern.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Pat {
	/// Any class, bound to the variable.
	Var(&'static str),
	/// An op node, optionally bound by name for its payload and frame.
	Node {
		bind: Option<&'static str>,
		head: HeadPat,
		kids: &'static [Pat],
	},
	/// A region of `kind`; `binder` binds the region's block-argument binder.
	Region {
		kind: RegionKind,
		binder: Option<&'static str>,
		kids: &'static [Pat],
	},
	/// Block argument `index` of the region bound as `binder`.
	Arg {
		binder: &'static str,
		index: u8,
	},
}

/// The head of an op pattern: a tag and an optional attribute test.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HeadPat {
	pub(crate) tag: OpTag,
	pub(crate) attr: Option<AttrPred>,
}

/// A named attribute test. The name is what the rule prints.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AttrPred {
	pub(crate) name: &'static str,
	pub(crate) test: fn(&OpKind) -> bool,
}

/// A right-hand side.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Rhs {
	Tmpl(Tmpl),
	/// A Rust applier for a rewrite that re-binds.
	Named {
		name: &'static str,
		apply: ApplyFn,
	},
}

/// A right-hand-side template.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Tmpl {
	Var(&'static str),
	/// A new op node. `frame_of` names the bound node or variable whose frame and snapshot the
	/// node takes.
	Node {
		tag: OpTag,
		attrs: AttrSrc,
		frame_of: &'static str,
		kids: &'static [Tmpl],
	},
	/// A new region. `binder_of` names a matched region pattern whose binder the region reuses.
	Region {
		kind: RegionKind,
		binder_of: Option<&'static str>,
		kids: &'static [Tmpl],
	},
}

/// Where a new node's attributes come from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum AttrSrc {
	/// The payload of the node bound by name on the left side.
	CopyOf(&'static str),
	/// Attributes built from the match; allowed only for the tags of [`TEMPLATE_TAGS`].
	Build {
		name: &'static str,
		build: fn(&BuildCx<'_>) -> OpKind,
	},
}

/// What an attribute builder reads.
pub(crate) struct BuildCx<'a> {
	pub(crate) view: &'a dyn EgraphView,
	pub(crate) m: &'a Match,
}

/// One match of a rule's left side: each variable's class, and each bound node as the snapshot
/// taken at search time. A snapshot stays correct after an earlier match's union reorders its
/// class.
#[derive(Clone, Debug)]
pub(crate) struct Match {
	pub(crate) root: ClassRef,
	pub(crate) vars: SmallVec<[(&'static str, ClassRef); 6]>,
	pub(crate) nodes: SmallVec<[(&'static str, ENodeSnapshot); 4]>,
}

/// A named applier. It answers the classes to union with `m.root`, and may add nodes through
/// [`EgraphView::add`].
pub(crate) type ApplyFn = fn(view: &mut dyn EgraphView, m: &Match) -> SmallVec<[ClassRef; 1]>;

/// What a named applier and a cost hook may see of an e-graph.
pub(crate) trait EgraphView {
	/// The canonical class of `c`.
	fn find(&self, c: ClassRef) -> ClassRef;
	/// The facts of the class of `c`.
	fn facts(&self, c: ClassRef) -> &ClassFacts;
	/// The nodes of the class of `c`, by value, with canonical kids.
	fn nodes(&self, c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]>;
	/// The op attributes `p` interns, or `None` for an id the graph does not hold.
	fn payload(&self, p: PayloadId) -> Option<&OpKind>;
	/// The physical facts of `t`, or `None` when the run holds none.
	fn table(&self, t: &TableName) -> Option<&PhysTableFacts>;
	/// Whether the row producer the class of `c` stands for may change its order.
	fn total_order_ok(&self, c: ClassRef) -> bool;
	/// Whether `n` is a node the conversion seeded.
	fn is_seed(&self, n: &ENodeSnapshot) -> bool;
	/// Adds a node and answers its class. The node's facts come from the analysis.
	fn add(&mut self, spec: NodeSpec) -> ClassRef;
}

/// A node an applier asks the view to add.
#[derive(Clone, Debug)]
pub(crate) struct NodeSpec {
	pub(crate) shape: NodeSpecShape,
	/// The class whose frame and snapshot the node takes.
	pub(crate) frame_of: ClassRef,
	pub(crate) kids: SmallVec<[ClassRef; 4]>,
}

/// The shape of a node an applier asks the view to add.
#[derive(Clone, Debug)]
pub(crate) enum NodeSpecShape {
	Op(OpKind),
	Region {
		kind: RegionKind,
		binder: BinderId,
	},
	Arg {
		binder: BinderId,
		index: u8,
	},
}

/// The cost of a plan, compared lexicographically in field order. The order is total.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PlanCost {
	/// The plan holds a node that cannot be part of a final plan.
	pub(crate) illegal: bool,
	pub(crate) work: u64,
	/// Lower for a node a rule created.
	pub(crate) novelty: u32,
	/// The sum of the registration ranks of the rules that created the plan's nodes.
	pub(crate) rank: u32,
	/// The ordinal of the class the plan stands for.
	pub(crate) prov: u32,
}

/// One node a [`CostHook`] prices.
pub(crate) struct CostNode<'a> {
	pub(crate) class: ClassRef,
	pub(crate) node: &'a ENodeSnapshot,
	pub(crate) view: &'a dyn EgraphView,
	/// The registration rank of the rule that created the node (0 for an imported or seeded node).
	pub(crate) rule_rank: u32,
	/// The node is one the conversion seeded.
	pub(crate) seed: bool,
}

/// The cost function extraction consults.
pub(crate) trait CostHook: Send + Sync {
	/// The cost of `n` given its kids' costs. Monotone (never below a kid's cost) and total.
	fn node(&self, n: &CostNode<'_>, kids: &[PlanCost]) -> PlanCost;
}

/// The most logical candidates one statement carries: the incumbent and one alternative.
pub(crate) const MAX_LOGICAL_SEEDS: usize = 2;
/// The most saturation passes one statement runs. Each pass runs a logical round, then a physical
/// round.
pub(crate) const MAX_ROUNDS: usize = 2;
/// The most matches of one rule a round admits.
pub(crate) const MATCH_CAP_PER_RULE: usize = 256;
/// An `expr.literal` whose attribute payload is larger than this many bytes is imported as an
/// opaque leaf.
pub(crate) const EGRAPH_LITERAL_MAX_BYTES: usize = 64 * 1024;

/// The tags a rule's right side may build attributes for. It holds no `phys.*` tag.
pub(crate) const TEMPLATE_TAGS: &[OpTag] = &[
	OpTag::Logical(LogTag::Filter),
	OpTag::Logical(LogTag::PermissionFilter),
	OpTag::Logical(LogTag::Limit),
	OpTag::Expr(ExprTag::ShortCircuit),
	OpTag::Expr(ExprTag::Truthy),
	OpTag::Expr(ExprTag::Literal),
];

/// The position of `tag` in [`OpTag::ALL`]. The tag enums are fieldless and declared in `ALL`
/// order, so a tag's discriminant is its position within its dialect.
pub(crate) fn tag_index(tag: OpTag) -> usize {
	match tag {
		OpTag::Expr(t) => t as usize,
		OpTag::Logical(t) => ExprTag::ALL.len() + t as usize,
		OpTag::Phys(t) => ExprTag::ALL.len() + LogTag::ALL.len() + t as usize,
	}
}

/// A set of op tags, one bit per [`OpTag::ALL`] position.
#[derive(Clone, Debug, Default)]
pub(crate) struct TagBitmap(SmallVec<[u64; 4]>);

impl TagBitmap {
	/// Adds `tag` to the set.
	pub(crate) fn insert(&mut self, tag: OpTag) {
		let index = tag_index(tag);
		let word = index / 64;
		if self.0.len() <= word {
			self.0.resize(word + 1, 0);
		}
		self.0[word] |= 1 << (index % 64);
	}

	/// Whether `tag` is in the set.
	pub(crate) fn contains(&self, tag: OpTag) -> bool {
		let index = tag_index(tag);
		self.0.get(index / 64).is_some_and(|word| word & (1 << (index % 64)) != 0)
	}

	/// Whether the two sets share a tag.
	pub(crate) fn intersects(&self, other: &TagBitmap) -> bool {
		self.0.iter().zip(other.0.iter()).any(|(a, b)| a & b != 0)
	}
}

/// The ids of a statement copy, in both directions: each original op, value and region and the
/// copy's id for it.
///
/// An id absent from a direction has no counterpart there: a copy-only id was created in the
/// copy, and an original-only id was not copied.
///
/// A map is built once, when its copy is made ([`StmtMap::from_id_map`]), and is never updated.
/// Ops the write-back creates in the copy are tracked by the plan's own record of what it built,
/// so the six directions and `container` cannot drift apart.
#[derive(Clone, Debug, Default)]
pub(crate) struct StmtMap {
	pub(crate) ops_to_copy: HashMap<OpId, OpId>,
	pub(crate) ops_to_orig: HashMap<OpId, OpId>,
	pub(crate) values_to_copy: HashMap<ValueId, ValueId>,
	pub(crate) values_to_orig: HashMap<ValueId, ValueId>,
	pub(crate) regions_to_copy: HashMap<RegionId, RegionId>,
	pub(crate) regions_to_orig: HashMap<RegionId, RegionId>,
	/// The container's token of the copy, which adoption reads. `None` for a map not built from
	/// a container copy.
	pub(crate) container: Option<StatementCopy>,
}

impl StmtMap {
	/// The map of `copy`, the statement copy `map` is the token of.
	///
	/// One walk of `copy` enters each id that has an original in the six directions: the copy's
	/// top region against the statement's region, the arguments of the top block (the values the
	/// statement reads from outside), every op in pre-order including the copies of outside
	/// definitions, and each op's results, regions and region block arguments. An id with no
	/// original is entered in neither direction. `map` is kept as [`StmtMap::container`].
	pub(crate) fn from_id_map(copy: &Ir, map: StatementCopy) -> StmtMap {
		let mut out = StmtMap::default();
		let top = copy.top();
		if let Some(original) = map.original_region(top) {
			out.note_region(original, top);
		}
		out.note_region_args(copy, &map, top);
		copy.walk(top, WalkOrder::PreOrder, &mut |op| {
			if let Some(original) = map.original_op(op) {
				out.ops_to_copy.insert(original, op);
				out.ops_to_orig.insert(op, original);
			}
			let Some(data) = copy.op(op) else {
				return Walk::Continue;
			};
			for result in &data.results {
				out.note_value(&map, *result);
			}
			for region in &data.regions {
				if let Some(original) = map.original_region(*region) {
					out.note_region(original, *region);
				}
				out.note_region_args(copy, &map, *region);
			}
			Walk::Continue
		});
		out.container = Some(map);
		out
	}

	fn note_region(&mut self, original: RegionId, copy: RegionId) {
		self.regions_to_copy.insert(original, copy);
		self.regions_to_orig.insert(copy, original);
	}

	fn note_value(&mut self, map: &IdMap, copy: ValueId) {
		if let Some(original) = map.original_value(copy) {
			self.values_to_copy.insert(original, copy);
			self.values_to_orig.insert(copy, original);
		}
	}

	/// Enters the block arguments of `region` of `copy`.
	fn note_region_args(&mut self, copy: &Ir, map: &IdMap, region: RegionId) {
		let blocks = copy.region(region).map_or(&[][..], |held| held.blocks.as_slice());
		for block in blocks {
			for arg in copy.block(*block).map_or(&[][..], |held| held.args.as_slice()) {
				self.note_value(map, *arg);
			}
		}
	}
}
