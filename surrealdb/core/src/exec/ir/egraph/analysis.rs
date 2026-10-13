//! The class facts of the e-graph: the per-graph tables an e-graph run owns, and the analysis that
//! derives a class's facts from its nodes and merges them when two classes join.
//!
//! [`IrAnalysis`] holds no borrow, so it is `'static` and `Send`. Everything that needs the
//! evaluation context is computed when it enters a table: the local effects of a payload when it
//! is interned, the facts of an imported node before it is added. The analysis itself reads only
//! its own tables and the facts of a node's kids.

use std::collections::HashMap;
use std::sync::Arc;

use egg::{Analysis, DidMerge, EGraph, Id};
use smallvec::{Array, SmallVec};
use surrealdb_strand::TableName;

use super::decl::{
	BinderId, ClassFacts, EpochKey, LeafId, NodeShape, OrderSig, PayloadId, PhysTableFacts,
};
use super::lang::ENode;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::effects::{
	Determinism, FieldSet, IrFootprint, IrResource, KeyOf, OpEffects,
};
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::identity::AttrDigest;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::logical::footprint::CatalogKind;
use crate::expr::logical::EffectSet;

/// An interned op payload: the attributes of an op, shared by every node and seed that carries
/// them, and the op's own effects computed once when the payload is interned.
#[derive(Clone, Debug)]
pub(crate) struct PayloadEntry {
	pub(crate) kind: Arc<OpKind>,
	pub(crate) local: OpEffects,
}

/// The region a binder stands for and how many block arguments it binds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BinderEntry {
	pub(crate) origin: RegionId,
	pub(crate) args: u8,
}

/// What an opaque leaf stands for in the statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OpaqueLeaf {
	pub(crate) op: Option<OpId>,
	pub(crate) region: Option<RegionId>,
	pub(crate) value: ValueId,
}

/// A node the conversion seeded: which candidate copy it came from, the op it is in that copy and
/// the order it delivers.
#[derive(Clone, Debug)]
pub(crate) struct SeedInfo {
	pub(crate) candidate: u8,
	pub(crate) op: OpId,
	pub(crate) order: OrderSig,
}

/// The tables of one e-graph run, and the analysis over them.
///
/// No output of a run depends on the iteration order of a table: every consumer sorts before it
/// uses one.
#[derive(Clone, Debug)]
pub(crate) struct IrAnalysis {
	/// Interned payloads, by [`PayloadId`].
	pub(crate) payloads: Vec<PayloadEntry>,
	/// The payloads of each tag and digest. A digest is a candidate match only, so a key holds
	/// every payload whose digest collides with another's attributes.
	payload_index: HashMap<(OpTag, AttrDigest), SmallVec<[PayloadId; 1]>>,
	/// Block-argument binders, by [`BinderId`].
	pub(crate) binders: Vec<BinderEntry>,
	/// Opaque leaves, by [`LeafId`].
	pub(crate) leaves: Vec<OpaqueLeaf>,
	/// Whether each frame, by [`FrameId`] position, is a permission frame.
	pub(crate) frames: Vec<bool>,
	/// The physical facts of each table the statement reads, met across its row sources
	/// ([`PhysTableFacts::meet`]). Empty in a logical-only run.
	pub(crate) tables: HashMap<TableName, PhysTableFacts>,
	/// The seeded nodes, keyed by node with canonical kids. [`recanonicalise_seeds`] restores the
	/// canonical form after a rebuild. A node is a seed of candidate `c` iff its list holds an
	/// entry with `candidate == c`.
	pub(crate) seeds: HashMap<ENode, SmallVec<[SeedInfo; 2]>>,
	/// The class of each imported logical op.
	pub(crate) incumbents: HashMap<OpId, Id>,
	/// The import order of each imported logical op.
	pub(crate) ordinals: HashMap<OpId, u32>,
	/// What the statement writes, triggered writes included.
	pub(crate) stmt_writes: IrFootprint,
	/// The ordinal of the class whose rule is creating nodes: the ordinal every node added
	/// without imported facts takes.
	pub(crate) creating_for: Option<u32>,
	/// The facts of the imported node about to be added. [`Analysis::make`] takes them.
	pub(crate) pending: Option<ClassFacts>,
	/// The unions the guards refused.
	pub(crate) union_refused: u32,
	/// The unique epochs a rule-built read has taken. They count down from `u32::MAX`, and the
	/// import and seeding count up from zero within the node limit, so no two ever meet.
	rule_epochs: u32,
}

impl Default for IrAnalysis {
	fn default() -> Self {
		Self::new()
	}
}

impl IrAnalysis {
	/// Empty tables, and a statement that writes nothing.
	pub(crate) fn new() -> Self {
		Self {
			payloads: Vec::new(),
			payload_index: HashMap::new(),
			binders: Vec::new(),
			leaves: Vec::new(),
			frames: Vec::new(),
			tables: HashMap::new(),
			seeds: HashMap::new(),
			incumbents: HashMap::new(),
			ordinals: HashMap::new(),
			stmt_writes: IrFootprint {
				reads: SmallVec::new(),
				writes: SmallVec::new(),
			},
			creating_for: None,
			pending: None,
			union_refused: 0,
			rule_epochs: 0,
		}
	}

	/// A unique epoch no import or seeding and no earlier call produced: the epoch of a read a
	/// rule builds, which equals no other read.
	pub(crate) fn fresh_rule_epoch(&mut self) -> EpochKey {
		let epoch = EpochKey::Unique(u32::MAX - self.rule_epochs);
		self.rule_epochs = self.rule_epochs.saturating_add(1);
		epoch
	}

	/// The id of the payload with `kind`'s tag and attributes, interning it with its local
	/// effects under `cx` when no equal payload exists.
	///
	/// A payload of the same tag and digest is the same payload only when its attributes are equal
	/// ([`OpKind::attrs_eq`]). An op whose digest is [`AttrDigest::OPAQUE`] equals no other, so
	/// each such payload is a new entry.
	pub(crate) fn intern_payload(&mut self, kind: Arc<OpKind>, cx: &EffectCx<'_>) -> PayloadId {
		let digest = kind.attr_digest();
		self.intern_payload_under(digest, kind, cx)
	}

	/// [`Self::intern_payload`] with `kind`'s digest given as `digest`.
	pub(super) fn intern_payload_under(
		&mut self,
		digest: AttrDigest,
		kind: Arc<OpKind>,
		cx: &EffectCx<'_>,
	) -> PayloadId {
		let key = (kind.tag(), digest);
		let interned = digest != AttrDigest::OPAQUE;
		if interned
			&& let Some(held) = self.payload_index.get(&key)
			&& let Some(existing) = held.iter().copied().find(|id| {
				self.payloads.get(id.0 as usize).is_some_and(|entry| entry.kind.attrs_eq(&kind))
			}) {
			return existing;
		}
		let id = PayloadId(self.payloads.len() as u32);
		let local = kind.local_effects(cx);
		self.payloads.push(PayloadEntry {
			kind,
			local,
		});
		if interned {
			self.payload_index.entry(key).or_default().push(id);
		}
		id
	}

	/// The payload `id` interns, or `None` for an id this run did not issue.
	pub(crate) fn payload(&self, id: PayloadId) -> Option<&PayloadEntry> {
		self.payloads.get(id.0 as usize)
	}

	/// Registers the binder of `origin`'s `args` block arguments.
	pub(crate) fn add_binder(&mut self, origin: RegionId, args: u8) -> BinderId {
		let id = BinderId(self.binders.len() as u32);
		self.binders.push(BinderEntry {
			origin,
			args,
		});
		id
	}

	/// Registers an opaque leaf.
	pub(crate) fn add_leaf(&mut self, leaf: OpaqueLeaf) -> LeafId {
		let id = LeafId(self.leaves.len() as u32);
		self.leaves.push(leaf);
		id
	}

	/// Whether `frame` is a permission frame. A frame the run does not know counts as one, so a
	/// guard that asks whether a node is outside every permission frame refuses.
	pub(crate) fn is_permission_frame(&self, frame: FrameId) -> bool {
		self.frames.get(usize::from(frame.index())).copied().unwrap_or(true)
	}
}

/// Adds `node` with the facts an import derived for it, and answers its class.
///
/// A node already in the graph keeps its class's facts: the facts offered are dropped, never
/// merged.
pub(crate) fn add_with_facts(
	eg: &mut EGraph<ENode, IrAnalysis>,
	node: ENode,
	facts: ClassFacts,
) -> Id {
	debug_assert!(eg.analysis.pending.is_none(), "facts pending for a node that was never added");
	eg.analysis.pending = Some(facts);
	let id = eg.add(node);
	eg.analysis.pending = None;
	id
}

/// Rewrites every key of `seeds` to its canonical form after a rebuild.
///
/// Two keys that become one node join their entries, which stay sorted by `(candidate, op)` and
/// free of duplicates.
pub(crate) fn recanonicalise_seeds(eg: &mut EGraph<ENode, IrAnalysis>) {
	let old = std::mem::take(&mut eg.analysis.seeds);
	let mut next: HashMap<ENode, SmallVec<[SeedInfo; 2]>> = HashMap::with_capacity(old.len());
	for (mut node, infos) in old {
		for kid in node.kids.iter_mut() {
			*kid = eg.find(*kid);
		}
		let slot = next.entry(node).or_default();
		for info in infos {
			if !slot.iter().any(|held| held.candidate == info.candidate && held.op == info.op) {
				slot.push(info);
			}
		}
	}
	for infos in next.values_mut() {
		infos.sort_by_key(|info| (info.candidate, info.op));
	}
	eg.analysis.seeds = next;
}

/// Whether a read of `footprint`, and of `row_fields` of its row source where it reads the row,
/// meets a write of `writes`.
///
/// Only the table and fields of a data resource are compared, never its key, so two accesses of
/// one table and field meet even where they touch different records.
pub(crate) fn reads_written(
	footprint: &IrFootprint,
	row_fields: Option<(&TableName, &FieldSet)>,
	writes: &IrFootprint,
) -> bool {
	let meets_write = |read: &IrResource| writes.writes.iter().any(|w| resources_meet(read, w));
	if footprint.reads.iter().any(meets_write) {
		return true;
	}
	match row_fields {
		Some((table, fields)) => meets_write(&IrResource::Data {
			table: Some(table.clone()),
			fields: fields.clone(),
			key: KeyOf::Any,
		}),
		None => false,
	}
}

/// Whether two resources name some part in common.
fn resources_meet(a: &IrResource, b: &IrResource) -> bool {
	fn same<T: PartialEq>(a: &Option<T>, b: &Option<T>) -> bool {
		match (a, b) {
			(Some(a), Some(b)) => a == b,
			_ => true,
		}
	}
	fn fields_meet(a: &FieldSet, b: &FieldSet) -> bool {
		match (a, b) {
			(FieldSet::All, _) | (_, FieldSet::All) => true,
			(FieldSet::Some(a), FieldSet::Some(b)) => a.iter().any(|field| b.contains(field)),
		}
	}
	match (a, b) {
		(IrResource::Unknown, _) | (_, IrResource::Unknown) => true,
		(
			IrResource::Data {
				table: ta,
				fields: fa,
				..
			},
			IrResource::Data {
				table: tb,
				fields: fb,
				..
			},
		) => same(ta, tb) && fields_meet(fa, fb),
		// A cascade writes the records of tables the resource does not name.
		(
			IrResource::Cascade {
				..
			},
			IrResource::Data {
				..
			},
		)
		| (
			IrResource::Data {
				..
			},
			IrResource::Cascade {
				..
			},
		) => true,
		(
			IrResource::Catalog {
				kind: ka,
				table: ta,
				name: na,
			},
			IrResource::Catalog {
				kind: kb,
				table: tb,
				name: nb,
			},
		) => {
			if !same(ta, tb) {
				return false;
			}
			let tabled = |kind: CatalogKind| {
				matches!(
					kind,
					CatalogKind::Field
						| CatalogKind::Index
						| CatalogKind::Event
						| CatalogKind::View
				)
			};
			match (*ka, *kb) {
				// A table's definition covers every definition on it.
				(CatalogKind::Table, other) | (other, CatalogKind::Table)
					if other == CatalogKind::Table || tabled(other) =>
				{
					ka != kb || same(na, nb)
				}
				(ka, kb) => ka == kb && same(na, nb),
			}
		}
		(IrResource::Snapshot(a), IrResource::Snapshot(b)) => a == b,
		(
			IrResource::Cascade {
				table: a,
			},
			IrResource::Cascade {
				table: b,
			},
		) => a == b,
		(
			IrResource::Reject {
				table: a,
			},
			IrResource::Reject {
				table: b,
			},
		) => a == b,
		(
			IrResource::Queue {
				event: a,
			},
			IrResource::Queue {
				event: b,
			},
		) => a == b,
		(
			IrResource::Session {
				name: a,
			},
			IrResource::Session {
				name: b,
			},
		) => a == b,
		// Resources of different kinds name different parts of the world.
		_ => false,
	}
}

/// Whether `writes` may write stored records: a data write, a cascade or an unknown write.
fn writes_records(writes: &IrFootprint) -> bool {
	writes.writes.iter().any(|resource| {
		matches!(
			resource,
			IrResource::Data { .. } | IrResource::Cascade { .. } | IrResource::Unknown
		)
	})
}

/// The position of `d` from the strongest guarantee to the weakest.
///
/// A deterministic op is the strongest. A clock read holds for one statement. A read of stored
/// data holds only where no write of the statement reaches it, so it is weaker than a clock read.
fn determinism_rank(d: Determinism) -> u8 {
	match d {
		Determinism::Deterministic => 0,
		Determinism::PerStatement => 1,
		Determinism::WithinStatementUnlessWritten => 2,
		Determinism::NonDeterministic => 3,
	}
}

/// The weaker of two determinism classes.
pub(crate) fn weaker_determinism(a: Determinism, b: Determinism) -> Determinism {
	if determinism_rank(a) >= determinism_rank(b) {
		a
	} else {
		b
	}
}

/// The elements of two sorted, duplicate-free lists, sorted and duplicate-free.
fn sorted_union<A: Array>(a: &SmallVec<A>, b: &SmallVec<A>) -> SmallVec<A>
where
	A::Item: Ord + Copy,
{
	let mut out: SmallVec<A> = a.iter().chain(b.iter()).copied().collect();
	out.sort_unstable();
	out.dedup();
	out
}

/// The elements of both of two sorted, duplicate-free lists.
fn sorted_intersection<A: Array>(a: &SmallVec<A>, b: &SmallVec<A>) -> SmallVec<A>
where
	A::Item: Ord + Copy,
{
	a.iter().filter(|item| b.binary_search(item).is_ok()).copied().collect()
}

/// Joins the facts of `b`'s class into `a`'s, and says which side changed.
///
/// `DidMerge.0` is whether `a` differs after the call, `DidMerge.1` whether `b` differs from the
/// result. Each field is compared before and after, so a join that changes nothing reports
/// nothing.
pub(crate) fn merge_facts(a: &mut ClassFacts, b: &ClassFacts) -> DidMerge {
	fn settle<T: PartialEq>(a: &mut T, b: &T, merged: T) -> DidMerge {
		let did = DidMerge(*a != merged, *b != merged);
		*a = merged;
		did
	}

	let mut did = DidMerge(false, false);

	let merged = a.effects.union(b.effects);
	did = did | settle(&mut a.effects, &b.effects, merged);
	let merged = a.fallibility.union(b.fallibility);
	did = did | settle(&mut a.fallibility, &b.fallibility, merged);
	let merged = weaker_determinism(a.determinism, b.determinism);
	did = did | settle(&mut a.determinism, &b.determinism, merged);

	debug_assert!(
		a.frame == b.frame && a.snapshot == b.snapshot,
		"classes of different frames or snapshots were joined"
	);
	let merged = if a.frame == b.frame {
		a.frame
	} else {
		None
	};
	did = did | settle(&mut a.frame, &b.frame, merged);
	let merged = if a.snapshot == b.snapshot {
		a.snapshot
	} else {
		None
	};
	did = did | settle(&mut a.snapshot, &b.snapshot, merged);

	let merged = a.reads_written || b.reads_written;
	did = did | settle(&mut a.reads_written, &b.reads_written, merged);
	let merged: TypeKnowledge = a.ty.meet(&b.ty);
	did = did | settle(&mut a.ty, &b.ty, merged);
	let merged = sorted_intersection(&a.free_binders, &b.free_binders);
	did = did | settle(&mut a.free_binders, &b.free_binders, merged);
	let merged = a.in_permission_frame || b.in_permission_frame;
	did = did | settle(&mut a.in_permission_frame, &b.in_permission_frame, merged);
	let merged = sorted_union(&a.provenance, &b.provenance);
	did = did | settle(&mut a.provenance, &b.provenance, merged);
	let merged = a.ordinal.min(b.ordinal);
	did = did | settle(&mut a.ordinal, &b.ordinal, merged);
	let merged = a.seed_order.clone().or_else(|| b.seed_order.clone());
	did = did | settle(&mut a.seed_order, &b.seed_order, merged);
	let merged = a.total_order_ok && b.total_order_ok;
	did = did | settle(&mut a.total_order_ok, &b.total_order_ok, merged);
	let merged = a.estimate.or(b.estimate);
	did = did | settle(&mut a.estimate, &b.estimate, merged);

	did
}

/// The facts a node's own payload and its kids give it.
///
/// The fields no node determines (type knowledge, provenance, seed order, estimate) take their
/// least informative value, and the ordinal is the one of the rule creating the node.
fn derive_facts(eg: &EGraph<ENode, IrAnalysis>, node: &ENode) -> ClassFacts {
	let an = &eg.analysis;
	let (mut effects, mut fallibility, mut determinism, mut reads) = match node.id.shape {
		NodeShape::Op {
			payload,
			..
		} => match an.payload(payload) {
			Some(entry) => {
				let local = &entry.local;
				// A row source the payload does not name is every table: the node meets any write
				// of records.
				let reads =
					!super::guards::footprint_disjoint(&local.footprint, None, &an.stmt_writes)
						|| (local.set.intersects(EffectSet::READS_ROW)
							&& writes_records(&an.stmt_writes));
				(local.set, local.fallibility, local.determinism, reads)
			}
			None => (EffectSet::TOP, Fallibility::TOP, Determinism::NonDeterministic, true),
		},
		NodeShape::Region {
			..
		}
		| NodeShape::Arg {
			..
		} => (EffectSet::PURE, Fallibility::INFALLIBLE, Determinism::Deterministic, false),
		NodeShape::Opaque {
			..
		} => (EffectSet::TOP, Fallibility::TOP, Determinism::NonDeterministic, true),
	};
	let mut free_binders: SmallVec<[BinderId; 2]> = SmallVec::new();
	for kid in &node.kids {
		let facts = &eg[*kid].data;
		effects = effects.union(facts.effects);
		fallibility = fallibility.union(facts.fallibility);
		determinism = weaker_determinism(determinism, facts.determinism);
		reads |= facts.reads_written;
		free_binders.extend(facts.free_binders.iter().copied());
	}
	match node.id.shape {
		NodeShape::Region {
			binder,
			..
		} => free_binders.retain(|held| *held != binder),
		NodeShape::Arg {
			binder,
			..
		} => free_binders.push(binder),
		NodeShape::Op {
			..
		}
		| NodeShape::Opaque {
			..
		} => {}
	}
	free_binders.sort_unstable();
	free_binders.dedup();

	ClassFacts {
		effects,
		fallibility,
		determinism,
		frame: Some(node.id.frame),
		snapshot: Some(node.id.snapshot),
		reads_written: reads,
		ty: TypeKnowledge::Unknown,
		free_binders,
		in_permission_frame: an.is_permission_frame(node.id.frame),
		provenance: SmallVec::new(),
		ordinal: an.creating_for.unwrap_or(u32::MAX),
		seed_order: None,
		total_order_ok: false,
		estimate: None,
	}
}

impl Analysis<ENode> for IrAnalysis {
	type Data = ClassFacts;

	/// The facts of a node being added: those an import filed in [`IrAnalysis::pending`], else
	/// those its payload and kids give it (a node a rule creates).
	fn make(egraph: &mut EGraph<ENode, Self>, enode: &ENode, _id: Id) -> ClassFacts {
		match egraph.analysis.pending.take() {
			Some(facts) => facts,
			None => derive_facts(egraph, enode),
		}
	}

	/// The facts a node gives its class when a kid's facts change.
	///
	/// Only the facts that flow from the kids (effects, fallibility, determinism, reads) are
	/// derived again. Every other field is the class's own, so the join that follows leaves it
	/// as it is: an imported class keeps the type knowledge and provenance its import gave it.
	fn remake(egraph: &mut EGraph<ENode, Self>, enode: &ENode, id: Id) -> ClassFacts {
		let derived = derive_facts(egraph, enode);
		let mut facts = egraph[id].data.clone();
		facts.effects = derived.effects;
		facts.fallibility = derived.fallibility;
		facts.determinism = derived.determinism;
		facts.reads_written = derived.reads_written;
		facts
	}

	fn merge(&mut self, a: &mut ClassFacts, b: ClassFacts) -> DidMerge {
		merge_facts(a, &b)
	}
}
