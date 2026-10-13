//! Extraction: the cheapest term of each e-class under a cost hook.
//!
//! [`best_costs`] is a bottom-up fixpoint over the classes of a rebuilt graph. It prices a node
//! through a [`CostHook`] from the costs of its kids' best nodes, and keeps the minimum per class
//! under [`PlanCost`] and then under the structural tie rule. Nothing depends on e-graph ids, on
//! the iteration order of a hash table or on the clock: nodes are visited in content order, so two
//! graphs holding the same terms extract the same plan whatever order they were built in.
//!
//! Logical-only runs use [`logical_candidates`]: the incumbent statement, and the cheapest
//! rewritten form when it differs. Seeded runs use [`extract_phys`], which answers a plan made of
//! physical nodes only, or says there is none. Extraction chooses among the nodes the graph holds
//! and creates none, so it never produces a `phys.choose`. The elaborator builds the chosen term
//! into the statement.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use egg::{EGraph, Id};
use smallvec::SmallVec;
use surrealdb_strand::TableName;

use super::FallbackReason;
use super::analysis::IrAnalysis;
use super::decl::{
	ClassFacts, ClassRef, CostHook, CostNode, ENodeSnapshot, EgraphView, MAX_LOGICAL_SEEDS,
	NodeShape, NodeSpec, OrderSig, PayloadId, PhysTableFacts, PlanCost,
};
use super::import::IslandRoot;
use super::lang::{ENode, identity_of, snapshot_of};
use super::rule::{NodeKey, node_key, sorted_nodes};
use crate::exec::ir::iface::effects::FieldSet;
use crate::exec::ir::op_kind::{OpKind, OpTag};

/// The cheapest node of each class that has one, with its cost, keyed by canonical class.
///
/// A class with no entry has no admissible node whose kids all have entries.
pub(crate) type Best = HashMap<ClassRef, (PlanCost, ENodeSnapshot)>;

/// A chosen term: the node picked for each class reachable from the roots.
///
/// `order` lists those classes in pre-order from the roots, each once, so a consumer that walks the
/// term does so in an order that names no e-graph id. A class with no entry in `chosen` is not
/// part of the term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Term {
	/// The canonical classes of the island roots, in root order, without repeats.
	pub(crate) roots: SmallVec<[ClassRef; 4]>,
	/// The classes of the term in pre-order.
	pub(crate) order: Vec<ClassRef>,
	/// The node chosen for each class of `order`, with canonical kids.
	pub(crate) chosen: HashMap<ClassRef, ENodeSnapshot>,
}

impl Term {
	fn empty() -> Self {
		Self {
			roots: SmallVec::new(),
			order: Vec::new(),
			chosen: HashMap::new(),
		}
	}
}

/// One logical plan for the statement: candidate 0 is the incumbent and candidate 1 the cheapest
/// rewritten form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
	/// 0 for the incumbent, 1 for the rewritten form.
	pub(crate) index: u8,
	pub(crate) term: Term,
}

/// The plan seeded extraction chose: a node per class, none of them logical.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysChoice {
	pub(crate) term: Term,
}

// ---------------------------------------------------------------------------------------------
// The view a cost hook reads
// ---------------------------------------------------------------------------------------------

/// A read-only view of a graph for a cost hook. [`EgraphView::add`] adds nothing through it.
struct ExtractView<'a> {
	eg: &'a EGraph<ENode, IrAnalysis>,
}

impl EgraphView for ExtractView<'_> {
	fn find(&self, c: ClassRef) -> ClassRef {
		ClassRef::from(self.eg.find(Id::from(c)))
	}

	fn facts(&self, c: ClassRef) -> &ClassFacts {
		&self.eg[Id::from(c)].data
	}

	fn nodes(&self, c: ClassRef) -> SmallVec<[ENodeSnapshot; 4]> {
		sorted_nodes(self.eg, Id::from(c)).iter().map(snapshot_of).collect()
	}

	fn payload(&self, p: PayloadId) -> Option<&OpKind> {
		self.eg.analysis.payload(p).map(|entry| &*entry.kind)
	}

	fn table(&self, t: &TableName) -> Option<&PhysTableFacts> {
		self.eg.analysis.tables.get(t)
	}

	fn total_order_ok(&self, c: ClassRef) -> bool {
		self.facts(c).total_order_ok
	}

	fn is_seed(&self, n: &ENodeSnapshot) -> bool {
		let mut node = identity_of(n);
		for kid in node.kids.iter_mut() {
			*kid = self.eg.find(*kid);
		}
		self.eg.analysis.seeds.contains_key(&node)
	}

	fn add(&mut self, spec: NodeSpec) -> ClassRef {
		debug_assert!(false, "a cost hook cannot add a node");
		spec.frame_of
	}
}

/// The snapshot of `node` with its kids canonical.
fn canonical_snapshot(eg: &EGraph<ENode, IrAnalysis>, node: &ENode) -> ENodeSnapshot {
	let mut node = node.clone();
	for kid in node.kids.iter_mut() {
		*kid = eg.find(*kid);
	}
	snapshot_of(&node)
}

// ---------------------------------------------------------------------------------------------
// The tie rule
// ---------------------------------------------------------------------------------------------

/// What one step of a [`term_key`] walk holds.
enum Pending<'a> {
	Node(&'a ENodeSnapshot),
	Class(ClassRef),
}

/// The structural key of the term that has `node` at its root and the chosen nodes of `best`
/// below it.
///
/// The key is the content key of each node in pre-order, each led by its length, so two terms
/// compare by their first difference in shape, identity or kid ordinals. A class met a second
/// time, or one with no entry, contributes a marker. No e-graph id enters the key.
fn term_key<'a>(
	eg: &EGraph<ENode, IrAnalysis>,
	node: &'a ENodeSnapshot,
	best: &'a Best,
) -> Vec<u64> {
	let mut key = Vec::new();
	let mut seen: HashSet<ClassRef> = HashSet::new();
	let mut stack = vec![Pending::Node(node)];
	while let Some(item) = stack.pop() {
		let node = match item {
			Pending::Node(node) => node,
			Pending::Class(class) => match best.get(&class) {
				Some((_, node)) if !seen.contains(&class) => {
					seen.insert(class);
					node
				}
				_ => {
					key.push(0);
					continue;
				}
			},
		};
		let content: NodeKey = node_key(eg, &identity_of(node));
		key.push(1);
		key.push(content.len() as u64);
		key.extend(content.iter().copied());
		for kid in node.kids.iter().rev() {
			stack.push(Pending::Class(*kid));
		}
	}
	key
}

/// The structural key of the term `chosen` holds for `class`: the tie rule of extraction.
///
/// Two nodes of one class whose costs are equal are ordered by the keys of their chosen terms. The
/// key reads the identity of each node with its payload, binder and leaf as creation numbers, and
/// each kid as the ordinal of its class, so it never compares an e-graph id and does not depend on
/// the order the graph was built in. A class with no entry in `chosen` has the empty key.
pub(crate) fn structural_key(
	eg: &EGraph<ENode, IrAnalysis>,
	class: ClassRef,
	chosen: &Best,
) -> Vec<u64> {
	match chosen.get(&class) {
		Some((_, node)) => term_key(eg, node, chosen),
		None => Vec::new(),
	}
}

// ---------------------------------------------------------------------------------------------
// The fixpoint
// ---------------------------------------------------------------------------------------------

/// The canonical classes of `eg` in content order: by ordinal, then by their least node key.
///
/// Imported classes are numbered kids first, so this order is close to bottom-up and the fixpoint
/// settles in few passes. Only content is compared.
fn ordered_classes(eg: &EGraph<ENode, IrAnalysis>) -> Vec<Id> {
	let mut keyed: Vec<(u32, Option<NodeKey>, Id)> = eg
		.classes()
		.map(|class| {
			let least = class.nodes.iter().map(|node| node_key(eg, node)).min();
			(class.data.ordinal, least, class.id)
		})
		.collect();
	keyed.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
	keyed.into_iter().map(|(_, _, id)| id).collect()
}

/// The cheapest of `candidates` for `class` under the costs `best` holds, or `None` when no
/// candidate has an entry for every kid.
fn pick(
	eg: &EGraph<ENode, IrAnalysis>,
	view: &ExtractView<'_>,
	hook: &dyn CostHook,
	class: ClassRef,
	candidates: &[ENodeSnapshot],
	best: &Best,
) -> Option<(PlanCost, ENodeSnapshot)> {
	let mut winner: Option<(PlanCost, &ENodeSnapshot, Option<Vec<u64>>)> = None;
	let mut kid_costs: SmallVec<[PlanCost; 4]> = SmallVec::new();
	'nodes: for node in candidates {
		kid_costs.clear();
		for kid in &node.kids {
			match best.get(kid) {
				Some((cost, _)) => kid_costs.push(*cost),
				None => continue 'nodes,
			}
		}
		let cost = hook.node(
			&CostNode {
				class,
				node,
				view,
				rule_rank: 0,
				seed: view.is_seed(node),
			},
			&kid_costs,
		);
		let replace = match winner.as_mut() {
			None => true,
			Some(held) => match cost.cmp(&held.0) {
				Ordering::Less => true,
				Ordering::Greater => false,
				Ordering::Equal => {
					let theirs = held.2.get_or_insert_with(|| term_key(eg, held.1, best));
					term_key(eg, node, best) < *theirs
				}
			},
		};
		if replace {
			winner = Some((cost, node, None));
		}
	}
	winner.map(|(cost, node, _)| (cost, node.clone()))
}

/// The cheapest admissible node of every class that has one, with its cost.
///
/// `hook` prices a node from the costs of its kids' best nodes. A node competes only when
/// `admissible(class, node)` holds and every kid class has an entry, so a node that depends on a
/// cycle with no way out never gets one. Within a class the minimum is taken under [`PlanCost`],
/// and equal costs are ordered by [`structural_key`]. Nodes are visited in content order, never in
/// the order the graph keeps them.
///
/// The graph must be rebuilt. The loop re-prices every class until no entry changes, and stops
/// after one pass more than there are classes: with a monotone hook the costs settle within that
/// bound, and the result is the same on every run either way.
pub(crate) fn best_costs(
	eg: &EGraph<ENode, IrAnalysis>,
	hook: &dyn CostHook,
	admissible: impl Fn(ClassRef, &ENodeSnapshot) -> bool,
) -> Best {
	debug_assert!(eg.clean, "extraction needs a rebuilt graph");
	let view = ExtractView {
		eg,
	};
	let classes: Vec<(ClassRef, Vec<ENodeSnapshot>)> = ordered_classes(eg)
		.into_iter()
		.map(|id| {
			let class = ClassRef::from(id);
			let nodes = sorted_nodes(eg, id)
				.iter()
				.map(snapshot_of)
				.filter(|node| admissible(class, node))
				.collect();
			(class, nodes)
		})
		.collect();
	let mut best = Best::with_capacity(classes.len());
	for _ in 0..=classes.len() {
		let mut changed = false;
		for (class, nodes) in &classes {
			let Some(winner) = pick(eg, &view, hook, *class, nodes, &best) else {
				continue;
			};
			if best.get(class) != Some(&winner) {
				best.insert(*class, winner);
				changed = true;
			}
		}
		if !changed {
			break;
		}
	}
	best
}

// ---------------------------------------------------------------------------------------------
// Terms
// ---------------------------------------------------------------------------------------------

/// The term `best` chooses below `roots`, or `None` when a class on the way has no entry or the
/// choices form a cycle.
fn extract_term(roots: &[ClassRef], best: &Best) -> Option<Term> {
	enum Mark {
		Open,
		Done,
	}
	let mut term = Term::empty();
	let mut marks: HashMap<ClassRef, Mark> = HashMap::new();
	let mut stack: Vec<(ClassRef, usize)> = Vec::new();
	for root in roots {
		if !term.roots.contains(root) {
			term.roots.push(*root);
		}
		if marks.contains_key(root) {
			continue;
		}
		marks.insert(*root, Mark::Open);
		term.order.push(*root);
		stack.push((*root, 0));
		while let Some((class, at)) = stack.last().copied() {
			let (_, node) = best.get(&class)?;
			term.chosen.entry(class).or_insert_with(|| node.clone());
			match node.kids.get(at) {
				Some(kid) => {
					if let Some(top) = stack.last_mut() {
						top.1 += 1;
					}
					match marks.get(kid) {
						Some(Mark::Open) => return None,
						Some(Mark::Done) => {}
						None => {
							marks.insert(*kid, Mark::Open);
							term.order.push(*kid);
							stack.push((*kid, 0));
						}
					}
				}
				None => {
					marks.insert(class, Mark::Done);
					stack.pop();
				}
			}
		}
	}
	Some(term)
}

/// The incumbent term, and every node of it, from the nodes the import added.
///
/// The graph keeps the node it first stored for each id, with the kids it had then. The walk
/// follows those kids from each root's import-time id, so it reads the statement as it was
/// imported however many classes have joined since. Where two imported nodes now share a class,
/// the term holds the one met first. Each node is taken once, in pre-order.
fn incumbent(
	eg: &EGraph<ENode, IrAnalysis>,
	roots: &[IslandRoot],
) -> (Term, HashSet<ENodeSnapshot>) {
	let mut term = Term::empty();
	let mut nodes = HashSet::new();
	let mut seen: HashSet<Id> = HashSet::new();
	for root in roots {
		let class = ClassRef::from(eg.find(root.class));
		if !term.roots.contains(&class) {
			term.roots.push(class);
		}
	}
	let mut stack: Vec<Id> = roots.iter().rev().map(|root| root.class).collect();
	while let Some(id) = stack.pop() {
		if !seen.insert(id) {
			continue;
		}
		let node = eg.id_to_node(id);
		let snapshot = canonical_snapshot(eg, node);
		let class = ClassRef::from(eg.find(id));
		if !term.chosen.contains_key(&class) {
			term.order.push(class);
			term.chosen.insert(class, snapshot.clone());
		}
		nodes.insert(snapshot);
		stack.extend(node.kids.iter().rev().copied());
	}
	(term, nodes)
}

/// Whether `best` chooses, at some position of the incumbent term, a node other than the one
/// imported there.
///
/// A class a rule has joined with a cheaper form is a position whose choice differs. A position
/// whose choice equals the incumbent's is checked below, at its kids' positions.
fn rewritten(eg: &EGraph<ENode, IrAnalysis>, roots: &[IslandRoot], best: &Best) -> bool {
	let mut seen: HashSet<Id> = HashSet::new();
	let mut stack: Vec<Id> = roots.iter().rev().map(|root| root.class).collect();
	while let Some(id) = stack.pop() {
		if !seen.insert(id) {
			continue;
		}
		let node = eg.id_to_node(id);
		let Some((_, chosen)) = best.get(&ClassRef::from(eg.find(id))) else {
			continue;
		};
		if *chosen != canonical_snapshot(eg, node) {
			return true;
		}
		stack.extend(node.kids.iter().copied());
	}
	false
}

// ---------------------------------------------------------------------------------------------
// Logical extraction
// ---------------------------------------------------------------------------------------------

/// The cost of a logical plan: its size, with a preference for rewritten nodes.
///
/// `work` is one per node, so a plan with fewer nodes is cheaper. Among plans of equal size,
/// `novelty` counts the nodes of the incumbent statement, so a plan with more rewritten nodes
/// scores lower. `rank` sums the registration ranks the hook is told of, and `prov` is the ordinal
/// of the node's class. No node is illegal, and no data or statistic is read.
#[derive(Clone, Debug)]
pub(crate) struct LogicalCost {
	incumbent: HashSet<ENodeSnapshot>,
}

impl LogicalCost {
	/// The cost over the statement imported at `roots`: its nodes are the incumbent ones.
	pub(crate) fn of(eg: &EGraph<ENode, IrAnalysis>, roots: &[IslandRoot]) -> Self {
		Self {
			incumbent: incumbent(eg, roots).1,
		}
	}
}

impl CostHook for LogicalCost {
	fn node(&self, n: &CostNode<'_>, kids: &[PlanCost]) -> PlanCost {
		let mut work = 1u64;
		let mut novelty = u32::from(self.incumbent.contains(n.node));
		let mut rank = n.rule_rank;
		for kid in kids {
			work = work.saturating_add(kid.work);
			novelty = novelty.saturating_add(kid.novelty);
			rank = rank.saturating_add(kid.rank);
		}
		PlanCost {
			illegal: false,
			work,
			novelty,
			rank,
			prov: n.view.facts(n.class).ordinal,
		}
	}
}

/// The logical plans of the statement imported at `roots`, from a graph the logical phase has
/// rebuilt.
///
/// Candidate 0 is the incumbent term and is always present. Candidate 1 is the cheapest term under
/// [`LogicalCost`] and is present only when that term chooses, at some position of the incumbent,
/// a node other than the imported one. A graph no rule has changed therefore answers one candidate.
pub(crate) fn logical_candidates(
	eg: &EGraph<ENode, IrAnalysis>,
	roots: &[IslandRoot],
) -> SmallVec<[Candidate; MAX_LOGICAL_SEEDS]> {
	let (term, nodes) = incumbent(eg, roots);
	let root_classes = term.roots.clone();
	let mut out: SmallVec<[Candidate; MAX_LOGICAL_SEEDS]> = SmallVec::new();
	out.push(Candidate {
		index: 0,
		term,
	});
	let cost = LogicalCost {
		incumbent: nodes,
	};
	let best = best_costs(eg, &cost, |_, _| true);
	if rewritten(eg, roots, &best)
		&& let Some(term) = extract_term(&root_classes, &best)
	{
		out.push(Candidate {
			index: 1,
			term,
		});
	}
	out
}

// ---------------------------------------------------------------------------------------------
// Physical extraction
// ---------------------------------------------------------------------------------------------

/// A hook that prices every logical node as illegal, over the hook it wraps.
///
/// A plan holding a logical node is illegal however the wrapped hook prices it, and a node is
/// illegal when any kid is, so a root at `illegal = false` has no logical node below it.
struct PhysOnly<'a>(&'a dyn CostHook);

impl CostHook for PhysOnly<'_> {
	fn node(&self, n: &CostNode<'_>, kids: &[PlanCost]) -> PlanCost {
		let mut cost = self.0.node(n, kids);
		cost.illegal |= matches!(
			n.node.identity.shape,
			NodeShape::Op {
				tag: OpTag::Logical(_),
				..
			}
		) || kids.iter().any(|kid| kid.illegal);
		cost
	}
}

/// Whether `node` may stand for `class` in a plan that must deliver the order the seed did.
///
/// A node that delivers an order over a field its table withholds ([`order_withheld`]) is refused
/// in every class. Otherwise a class with a total order key, or no seeded order, or an unordered
/// one (a scalar), admits every node, and any other class admits only a seed that delivers the
/// seed order: extraction cannot compute the order of a node the conversion did not produce, so
/// such a node is refused.
fn phys_admissible(eg: &EGraph<ENode, IrAnalysis>, class: ClassRef, node: &ENodeSnapshot) -> bool {
	let seeds = eg.analysis.seeds.get(&identity_of(node));
	if seeds.is_some_and(|seeds| {
		seeds.iter().any(|seed| order_withheld(&eg.analysis.tables, &seed.order))
	}) {
		return false;
	}
	let facts = &eg[Id::from(class)].data;
	if facts.total_order_ok {
		return true;
	}
	match &facts.seed_order {
		None | Some(OrderSig::Unordered) => true,
		Some(want) => eg
			.analysis
			.seeds
			.get(&identity_of(node))
			.is_some_and(|seeds| seeds.iter().any(|seed| seed.order == *want)),
	}
}

/// Whether `order` is an order over a field whose order the principal may not observe.
///
/// Only an index order is over fields: it is withheld when its table has no facts (no index may be
/// chosen for it), when the table withholds every field, or when a column of the index starts at a
/// withheld field. A column whose root is escaped is compared as withheld, since its root cannot
/// be read from the column text.
fn order_withheld(tables: &HashMap<TableName, PhysTableFacts>, order: &OrderSig) -> bool {
	let OrderSig::Index {
		index,
		..
	} = order
	else {
		return false;
	};
	let Some(facts) = tables.get(&index.table) else {
		return true;
	};
	let fields = match &facts.withheld_order {
		FieldSet::All => return true,
		FieldSet::Some(fields) if fields.is_empty() => return false,
		FieldSet::Some(fields) => fields,
	};
	let Some(candidate) = facts.indexes.iter().find(|candidate| candidate.index == *index) else {
		return true;
	};
	candidate.columns.iter().any(|column| {
		let column = column.as_str();
		if column.starts_with(['`', '⟨']) {
			return true;
		}
		fields.iter().any(|field| {
			column
				.strip_prefix(field.as_str())
				.is_some_and(|rest| rest.is_empty() || rest.starts_with(['.', '[']))
		})
	})
}

/// The cheapest all-physical plan below `roots` under `hook`, from a graph the physical phase has
/// rebuilt.
///
/// A logical node is priced illegal, so a legal plan holds none. A root with no entry, or one left
/// at `illegal = true`, is [`FallbackReason::NoPhysRoot`]; so is a plan whose chosen nodes form a
/// cycle. A node that would change the order of a row producer is admissible only where the class
/// allows it ([`ClassFacts::total_order_ok`]) or the node is a seed that delivers the seed order.
/// The plan holds nodes of the graph only: extraction never builds a `phys.choose`.
pub(crate) fn extract_phys(
	eg: &EGraph<ENode, IrAnalysis>,
	roots: &[IslandRoot],
	hook: &dyn CostHook,
) -> Result<PhysChoice, FallbackReason> {
	let best = best_costs(eg, &PhysOnly(hook), |class, node| phys_admissible(eg, class, node));
	let mut classes: SmallVec<[ClassRef; 4]> = SmallVec::new();
	for root in roots {
		let class = ClassRef::from(eg.find(root.class));
		match best.get(&class) {
			Some((cost, _)) if !cost.illegal => classes.push(class),
			_ => return Err(FallbackReason::NoPhysRoot),
		}
	}
	extract_term(&classes, &best)
		.map(|term| PhysChoice {
			term,
		})
		.ok_or(FallbackReason::NoPhysRoot)
}
