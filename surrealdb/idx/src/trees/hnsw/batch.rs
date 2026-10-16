//! Staged batch insertion: one compaction batch's graph changes, built on many
//! tasks against the committed graph and published after the batch commits.
//!
//! A batch that only adds vectors never touches the resident graph, the
//! vector cache or the document caches before its transaction commits. It
//! reads the graph as committed before the batch — *G₀* — under a read lock,
//! writes every change it derives into its transaction, and hands the resident
//! graph its new nodes through a commit action. A transaction that does not
//! commit discards that action, so the resident graph never holds a topology
//! that did not commit, and nothing needs discarding afterwards.
//!
//! The work splits into two parallel steps, both reading *G₀* only:
//!
//! 1. **Select** ([`select`]): each new element searches *G₀* exactly as a serial insertion would,
//!    is offered the exact nearest batch elements at every layer as well, and chooses its out-edges
//!    with the index's heuristic.
//! 2. **Link** ([`link`]): every node an out-edge reaches — or that is itself new — is resolved
//!    once: its base edges joined with every batch element that selected it, re-pruned by the
//!    heuristic past its layer's cap.
//!
//! Both steps are functions of *G₀* and the batch alone, and their results are
//! combined in element-id order, so the graph a batch produces does not depend
//! on how many tasks built it. The design and its consistency argument are in
//! `doc/design/hnsw-batch-compaction.md`.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_datastore::close::{CommitAction, RollbackAction};
use tokio::sync::RwLock;
use tracing::debug;
use web_time::Instant;

use crate::error::Error;
use crate::parallel::ReadView;
use crate::places::Ticket;
use crate::trees::dynamicset::DynamicSet;
use crate::trees::graph::encode_edges;
use crate::trees::hnsw::docs::{ElementSink, VecDocs};
use crate::trees::hnsw::elements::{ElementOverlay, HnswElements};
use crate::trees::hnsw::flavor::{HnswFlavor, with_hnsw};
use crate::trees::hnsw::heuristic::Heuristic;
use crate::trees::hnsw::index::{HnswContext, ResidentGraph};
use crate::trees::hnsw::layer::{HnswLayer, LayerState};
use crate::trees::hnsw::{ElementId, Hnsw, HnswState};
use crate::trees::knn::{DoublePriorityQueue, Ids64};
use crate::trees::vector::{SerializedVector, SharedVector, Vector};

/// One vector a staged batch adds to the graph as a new element.
pub(super) struct NewElement {
	/// The element's id, allocated from the committed `next_element_id`.
	pub(super) id: ElementId,
	/// The top layer the element is inserted into.
	pub(super) level: usize,
	/// The element's vector.
	pub(super) vector: SharedVector,
}

/// What step 0 of a staged batch produced: the elements it added and the
/// document sets it changed, neither yet visible outside the transaction.
pub(super) struct Staging {
	/// The first element id this batch may allocate.
	first: ElementId,
	/// The batch's new elements, in id order from `first`.
	elements: Vec<NewElement>,
	/// The document set each changed element carries once the batch commits,
	/// keyed by element so a later change in the batch replaces an earlier one.
	doc_sets: BTreeMap<ElementId, Ids64>,
}

impl Staging {
	pub(super) fn new(first: ElementId) -> Self {
		Self {
			first,
			elements: Vec::new(),
			doc_sets: BTreeMap::new(),
		}
	}

	/// Allocates the next element id for `vector` at `level`.
	pub(super) fn allocate(&mut self, vector: Vector, level: usize) -> ElementId {
		let id = self.first + self.elements.len() as ElementId;
		self.elements.push(NewElement {
			id,
			level,
			vector: vector.into(),
		});
		id
	}

	/// Records the document set `element` carries once the batch commits.
	pub(super) fn defer_doc_set(&mut self, element: ElementId, docs: Ids64) {
		self.doc_sets.insert(element, docs);
	}

	/// Whether the batch adds no element, and so changes no graph topology.
	pub(super) fn adds_nothing(&self) -> bool {
		self.elements.is_empty()
	}

	/// The new elements, and the document sets to cache once the batch commits.
	pub(super) fn into_parts(self) -> (Vec<NewElement>, Vec<(ElementId, Ids64)>) {
		(self.elements, self.doc_sets.into_iter().collect())
	}
}

/// Allocates a staged batch's new elements: the next element id and a level
/// for each, with its vector written into the transaction, while the document
/// sets are held back for publication.
pub(super) struct Staged<'a> {
	pub(super) graph: &'a mut HnswFlavor,
	pub(super) staging: &'a mut Staging,
}

impl ElementSink for Staged<'_> {
	async fn add_element(
		&mut self,
		ctx: &HnswContext<'_>,
		vector: Vector,
		ser_vec: &SerializedVector,
	) -> Result<ElementId> {
		let level = self.graph.draw_level();
		let id = self.staging.allocate(vector, level);
		ctx.tx.set_key(&ctx.ikb.new_he_key(id), ser_vec).await?;
		Ok(id)
	}

	async fn record_doc_set(&mut self, _vec_docs: &VecDocs, element: ElementId, docs: Ids64) {
		self.staging.defer_doc_set(element, docs);
	}
}

/// The committed state a batch was computed from.
///
/// Every layer version the resident graph can hold identifies one committed
/// topology, so a resident graph that matches this exactly is *G₀*, and the
/// batch's nodes can be installed onto it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GraphBase {
	enter_point: Option<ElementId>,
	next_element_id: ElementId,
	layer0: u64,
	layers: Vec<u64>,
}

impl GraphBase {
	pub(super) fn of<L0, L>(h: &Hnsw<L0, L>) -> Self
	where
		L0: DynamicSet,
		L: DynamicSet,
	{
		Self {
			enter_point: h.state.enter_point,
			next_element_id: h.state.next_element_id,
			layer0: h.state.layer0.version,
			layers: h.state.layers.iter().map(|l| l.version).collect(),
		}
	}

	pub(super) fn next_element_id(&self) -> ElementId {
		self.next_element_id
	}

	fn matches<L0, L>(&self, h: &Hnsw<L0, L>) -> bool
	where
		L0: DynamicSet,
		L: DynamicSet,
	{
		h.layers.len() == self.layers.len() && Self::of(h) == *self
	}
}

/// The batch's new elements, shared by every task that builds it.
pub(super) struct BatchInput {
	/// The first new element id.
	first: ElementId,
	/// The new elements, in id order from `first`.
	elements: Vec<NewElement>,
	/// For each layer, the positions of the elements that reach it.
	members: Vec<Vec<usize>>,
}

impl BatchInput {
	fn new(first: ElementId, elements: Vec<NewElement>) -> Self {
		let top = elements.iter().map(|e| e.level).max().unwrap_or(0);
		let mut members = vec![Vec::new(); top + 1];
		for (i, e) in elements.iter().enumerate() {
			for level in members.iter_mut().take(e.level + 1) {
				level.push(i);
			}
		}
		Self {
			first,
			elements,
			members,
		}
	}

	pub(super) fn len(&self) -> usize {
		self.elements.len()
	}

	/// The top layer any new element reaches.
	fn top_level(&self) -> usize {
		self.members.len() - 1
	}

	/// The position of `id` among the new elements, if it is one.
	fn position(&self, id: ElementId) -> Option<usize> {
		let offset = usize::try_from(id.checked_sub(self.first)?).ok()?;
		(offset < self.elements.len()).then_some(offset)
	}

	fn overlay(&self) -> ElementOverlay {
		ElementOverlay::new(self.first, self.elements.iter().map(|e| e.vector.clone()).collect())
	}

	/// The `efc` batch elements nearest element `i` among those that reach
	/// `level`, excluding `i` itself.
	fn nearest_members(
		&self,
		elements: &HnswElements,
		i: usize,
		level: usize,
		efc: usize,
	) -> DoublePriorityQueue {
		let q = &self.elements[i].vector;
		let mut nearest = DoublePriorityQueue::default();
		for &j in &self.members[level] {
			if j == i {
				continue;
			}
			let d = elements.distance(q, &self.elements[j].vector);
			if nearest.len() < efc || nearest.peek_last_dist().is_some_and(|last| d < last) {
				nearest.push(d, self.elements[j].id);
				if nearest.len() > efc {
					nearest.pop_last();
				}
			}
		}
		nearest
	}
}

/// Splits candidates into those from the committed graph and those from the
/// batch, keeping each side's distance order.
fn split(
	input: &BatchInput,
	candidates: &DoublePriorityQueue,
) -> (DoublePriorityQueue, DoublePriorityQueue) {
	let mut graph = DoublePriorityQueue::default();
	let mut batch = DoublePriorityQueue::default();
	for (d, id) in candidates.to_vec() {
		if input.position(id).is_some() {
			batch.push(d, id);
		} else {
			graph.push(d, id);
		}
	}
	(graph, batch)
}

/// Runs the heuristic over split candidates and returns the chosen neighbours
/// in id order.
#[expect(clippy::too_many_arguments)]
async fn choose<S>(
	heuristic: &Heuristic,
	tx: &Transaction,
	elements: &HnswElements,
	layer: Option<&HnswLayer<S>>,
	m_max: usize,
	q_id: ElementId,
	q_pt: &SharedVector,
	graph: DoublePriorityQueue,
	batch: DoublePriorityQueue,
) -> Result<Vec<ElementId>>
where
	S: DynamicSet,
{
	let mut res = S::with_capacity(m_max + 1);
	heuristic.select_batch(tx, elements, layer, m_max, q_id, q_pt, graph, batch, &mut res).await?;
	let mut chosen: Vec<ElementId> = res.iter().copied().collect();
	chosen.sort_unstable();
	Ok(chosen)
}

/// Step 1 for new element `i`: its out-edges on each layer from 0 up to its
/// level, chosen from *G₀* and from the batch.
///
/// The search against *G₀* is the serial insertion's: greedy descent from the
/// entry point through the layers above the element's level, then an `efc`
/// search at each layer it joins, seeded from the layer above.
pub(super) async fn select<L0, L>(
	h: &Hnsw<L0, L>,
	tx: &Transaction,
	elements: &HnswElements,
	input: &BatchInput,
	i: usize,
) -> Result<Vec<Vec<ElementId>>>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	let e = &input.elements[i];
	let top0 = h.layers.len();
	let mut found: Vec<DoublePriorityQueue> = vec![DoublePriorityQueue::default(); e.level + 1];
	if let Some(ep) = h.state.enter_point {
		let Some(mut ep_dist) = elements.get_distance(tx, &e.vector, &ep).await? else {
			return Err(anyhow::anyhow!(Error::AnnEntryPointUnreadable {
				table: h.ikb.table().to_string(),
				index_id: h.ikb.index().0,
				element: ep,
			}));
		};
		let mut ep_id = ep;
		for layer in h.layers[e.level.min(top0)..].iter().rev() {
			if let Some(nearest) =
				layer.search_nearest(tx, elements, &e.vector, ep_dist, ep_id, 1).await?.peek_first()
			{
				(ep_dist, ep_id) = nearest;
			}
		}
		let mut eps = DoublePriorityQueue::from(ep_dist, ep_id);
		for level in (1..=e.level.min(top0)).rev() {
			let w = h.layers[level - 1].search_multi(tx, elements, &e.vector, eps, h.efc).await?;
			eps = w.clone();
			found[level] = w;
		}
		found[0] = h.layer0.search_multi(tx, elements, &e.vector, eps, h.efc).await?;
	}
	let mut chosen = Vec::with_capacity(e.level + 1);
	for (level, mut candidates) in found.into_iter().enumerate() {
		for (d, id) in input.nearest_members(elements, i, level, h.efc).to_vec() {
			candidates.push(d, id);
		}
		while candidates.len() > h.efc {
			candidates.pop_last();
		}
		let (graph, batch) = split(input, &candidates);
		let neighbours = if level == 0 {
			let m_max = h.layer0.m_max();
			choose(
				&h.heuristic,
				tx,
				elements,
				Some(&h.layer0),
				m_max,
				e.id,
				&e.vector,
				graph,
				batch,
			)
			.await?
		} else {
			let layer = h.layers.get(level - 1);
			choose(&h.heuristic, tx, elements, layer, h.m, e.id, &e.vector, graph, batch).await?
		};
		chosen.push(neighbours);
	}
	Ok(chosen)
}

/// A node a batch resolves at one layer: a new element, or a committed node
/// that batch elements selected.
pub(super) struct LinkJob {
	level: usize,
	node: ElementId,
	/// The batch elements that selected this node, in id order.
	sources: Vec<ElementId>,
}

/// Groups step 1's out-edges into one job per node to resolve, in
/// `(level, node)` order: every new element at each of its layers, and every
/// committed node an out-edge reaches.
pub(super) fn link_jobs(input: &BatchInput, selections: &[Vec<Vec<ElementId>>]) -> Vec<LinkJob> {
	let mut sources: BTreeMap<(usize, ElementId), Vec<ElementId>> = BTreeMap::new();
	for (i, chosen) in selections.iter().enumerate() {
		let source = input.elements[i].id;
		for (level, neighbours) in chosen.iter().enumerate() {
			sources.entry((level, source)).or_default();
			for &target in neighbours {
				sources.entry((level, target)).or_default().push(source);
			}
		}
	}
	sources
		.into_iter()
		.map(|((level, node), sources)| LinkJob {
			level,
			node,
			sources,
		})
		.collect()
}

/// Step 2 for one job: the node's final edges at the job's layer.
///
/// The node's base edges — its *G₀* edges, or for a new element its own
/// out-edges — are joined with every batch element that selected it. Past the
/// layer's cap they are re-pruned by the heuristic over the distances from the
/// node, as a serial insertion prunes a neighbour it overfills.
pub(super) async fn link<L0, L>(
	h: &Hnsw<L0, L>,
	tx: &Transaction,
	elements: &HnswElements,
	input: &BatchInput,
	selections: &[Vec<Vec<ElementId>>],
	job: &LinkJob,
) -> Result<Vec<ElementId>>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	let m_max = if job.level == 0 {
		h.layer0.m_max()
	} else {
		h.m
	};
	let mut candidates: Vec<ElementId> = match input.position(job.node) {
		Some(i) => selections[i][job.level].clone(),
		None => {
			let edges = if job.level == 0 {
				h.layer0.get_edges(job.node).map(|s| s.iter().copied().collect::<Vec<_>>())
			} else {
				h.layers
					.get(job.level - 1)
					.and_then(|layer| layer.get_edges(job.node))
					.map(|s| s.iter().copied().collect::<Vec<_>>())
			};
			let mut edges = edges.unwrap_or_default();
			edges.sort_unstable();
			edges
		}
	};
	for &source in &job.sources {
		if source != job.node && !candidates.contains(&source) {
			candidates.push(source);
		}
	}
	if candidates.len() <= m_max {
		candidates.sort_unstable();
		return Ok(candidates);
	}
	// The node's vector and every candidate's, in one read of whatever the
	// cache and the batch do not hold.
	let mut wanted = Vec::with_capacity(candidates.len() + 1);
	wanted.push(job.node);
	wanted.extend_from_slice(&candidates);
	let mut vectors = elements.get_vectors(tx, &wanted).await?.into_iter();
	let Some(node_pt) = vectors.next().flatten() else {
		// A committed node whose vector cannot be read has nothing to prune
		// by. Keep what fits, in id order, rather than overfill the layer.
		candidates.sort_unstable();
		candidates.truncate(m_max);
		return Ok(candidates);
	};
	let mut graph = DoublePriorityQueue::default();
	let mut batch = DoublePriorityQueue::default();
	for (id, pt) in candidates.into_iter().zip(vectors) {
		if let Some(pt) = pt {
			let d = elements.distance(&node_pt, &pt);
			if input.position(id).is_some() {
				batch.push(d, id);
			} else {
				graph.push(d, id);
			}
		}
	}
	if job.level == 0 {
		choose(&h.heuristic, tx, elements, Some(&h.layer0), m_max, job.node, &node_pt, graph, batch)
			.await
	} else {
		let layer = h.layers.get(job.level - 1);
		choose(&h.heuristic, tx, elements, layer, m_max, job.node, &node_pt, graph, batch).await
	}
}

/// A batch's graph changes, ready to write into its transaction and to
/// install into the resident graph once that transaction commits.
pub(super) struct StagedGraph {
	/// The committed state the changes were derived from.
	base: GraphBase,
	/// The state the batch commits: every changed layer's version advanced,
	/// the layers it adds, its entry point and next element id.
	state: HnswState,
	/// Every resolved node, as `(level, node, edges)`.
	nodes: Vec<(usize, ElementId, Vec<ElementId>)>,
	/// The task count the batch's steps were scheduled with.
	tasks: usize,
}

impl StagedGraph {
	pub(super) fn new<L0, L>(
		h: &Hnsw<L0, L>,
		base: GraphBase,
		input: &BatchInput,
		jobs: &[LinkJob],
		edges: Vec<Vec<ElementId>>,
		tasks: usize,
	) -> Self
	where
		L0: DynamicSet,
		L: DynamicSet,
	{
		let top0 = h.layers.len();
		let top = top0.max(input.top_level());
		let mut changed = vec![false; top + 1];
		for job in jobs {
			changed[job.level] = true;
		}
		let layers = (1..=top)
			.map(|level| match h.state.layers.get(level - 1) {
				Some(st) => LayerState {
					version: st.version + u64::from(changed[level]),
					chunks: st.chunks,
				},
				None => LayerState {
					version: 1,
					chunks: 0,
				},
			})
			.collect();
		// The serial rule: the first element to reach a new top layer becomes
		// the entry point, and an empty graph takes its first element.
		let highest = input.elements.iter().fold(None::<&NewElement>, |best, e| match best {
			Some(b) if b.level >= e.level => Some(b),
			_ => Some(e),
		});
		let enter_point = match (h.state.enter_point, highest) {
			(None, Some(e)) => Some(e.id),
			(Some(_), Some(e)) if e.level > top0 => Some(e.id),
			(current, _) => current,
		};
		let state = HnswState {
			enter_point,
			next_element_id: input.first + input.len() as ElementId,
			layer0: LayerState {
				version: h.state.layer0.version + u64::from(changed[0]),
				chunks: h.state.layer0.chunks,
			},
			layers,
		};
		let nodes =
			jobs.iter().zip(edges).map(|(job, edges)| (job.level, job.node, edges)).collect();
		Self {
			base,
			state,
			nodes,
			tasks,
		}
	}

	/// The task count the batch's steps were scheduled with: the count the
	/// batch was given where the datastore gives a read view, and one where it
	/// gives none. It is a ceiling: `parallel::run_jobs` runs on the calling
	/// task and on as many workers beyond it as its ticket is granted places,
	/// spawns at most one task per job, and none without a runtime to spawn
	/// onto.
	pub(super) fn tasks(&self) -> usize {
		self.tasks
	}

	/// Writes the resolved nodes and the new state into the batch's transaction.
	pub(super) async fn write<L0, L>(&self, h: &Hnsw<L0, L>, tx: &Transaction) -> Result<()>
	where
		L0: DynamicSet,
		L: DynamicSet,
	{
		for (level, node, edges) in &self.nodes {
			let key = h.ikb.new_hn_key(*level as u16, *node);
			tx.set_key(&key, &encode_edges(edges.len(), edges.iter())).await?;
		}
		tx.set_key(&h.ikb.new_hs_key(), &self.state).await?;
		Ok(())
	}

	/// Installs the batch into a resident graph that still stands at its base.
	///
	/// Returns `false`, changing nothing, when the resident graph stands
	/// anywhere else: it has been reloaded since — from this commit or from
	/// another committed state — and the next state check reconciles it with
	/// the store.
	pub(super) fn install<L0, L>(self, h: &mut Hnsw<L0, L>) -> bool
	where
		L0: DynamicSet,
		L: DynamicSet,
	{
		if !self.base.matches(h) {
			return false;
		}
		while h.layers.len() < self.state.layers.len() {
			let level = h.layers.len() + 1;
			h.layers.push(HnswLayer::new(h.ikb.clone(), level, h.m));
		}
		for (level, node, edges) in self.nodes {
			if level == 0 {
				let mut set = h.layer0.new_edges();
				for e in edges {
					set.insert(e);
				}
				h.layer0.set_edges(node, set);
			} else {
				let layer = &mut h.layers[level - 1];
				let mut set = layer.new_edges();
				for e in edges {
					set.insert(e);
				}
				layer.set_edges(node, set);
			}
		}
		h.elements.set_next_element_id(self.state.next_element_id);
		h.state = self.state;
		true
	}
}

/// Publishes a committed staged batch into the resident graph and the
/// process-wide caches.
///
/// Runs only once the batch's transaction has committed, and never for one
/// that did not, which is what keeps uncommitted topology out of the resident
/// graph.
pub(super) struct PublishBatch {
	pub(super) resident: Arc<RwLock<ResidentGraph>>,
	pub(super) staged: Option<StagedGraph>,
	pub(super) elements: HnswElements,
	pub(super) vectors: Vec<(ElementId, SharedVector)>,
	pub(super) vec_docs: VecDocs,
	pub(super) doc_sets: Vec<(ElementId, Ids64)>,
}

impl CommitAction for PublishBatch {
	fn run(self: Box<Self>) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(async move {
			let Self {
				resident,
				staged,
				elements,
				vectors,
				vec_docs,
				doc_sets,
			} = *self;
			if let Some(staged) = staged {
				let mut resident = resident.write().await;
				with_hnsw!(&mut resident.graph, h => {
					staged.install(h);
				});
			}
			// Committed now, so the vectors and document sets are correct to
			// cache whether or not the resident graph took the nodes.
			elements.cache_committed(vectors.into_iter()).await;
			for (element, docs) in doc_sets {
				vec_docs.insert_cached_doc_set(element, docs).await;
			}
		})
	}
}

/// Cleans up after a staged batch whose transaction did not commit.
///
/// The batch changed nothing outside its transaction, so this usually has
/// nothing to do. It covers the case where something read the resident graph
/// back through the same transaction after the batch staged — a second apply,
/// or a search — and so could have taken the batch's uncommitted writes into
/// it. A resident graph that no longer stands at the batch's base is
/// invalidated, and the batch's elements and document sets are dropped from
/// the caches.
pub(super) struct RollBackBatch {
	pub(super) resident: Arc<RwLock<ResidentGraph>>,
	pub(super) invalidations: Arc<AtomicU64>,
	pub(super) base: GraphBase,
	pub(super) elements: HnswElements,
	pub(super) new_elements: Range<ElementId>,
	pub(super) vec_docs: VecDocs,
	pub(super) doc_sets: Vec<ElementId>,
}

impl RollbackAction for RollBackBatch {
	fn run(
		self: Box<Self>,
		_drain_started_at: Instant,
	) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		Box::pin(async move {
			let at_base = {
				let resident = self.resident.read().await;
				with_hnsw!(&resident.graph, h => self.base.matches(h))
			};
			if !at_base {
				self.invalidations.fetch_add(1, Ordering::AcqRel);
			}
			self.elements.evict(self.new_elements.clone()).await;
			for element in self.new_elements.clone().chain(self.doc_sets.iter().copied()) {
				self.vec_docs.remove_cached_doc_set(element).await;
			}
			Ok(())
		})
	}
}

/// Invalidates the resident graph after an in-place batch whose transaction
/// did not commit.
///
/// An in-place batch changes the resident graph and the vector cache as it
/// applies, so a batch that does not commit leaves both holding what the
/// store never took. The invalidation makes every holder of the index — the
/// registry's and any handle cloned out of it — reload every layer at its
/// next state check, and the cache eviction drops the vectors the batch
/// entered.
pub(super) struct InvalidateResident {
	pub(super) invalidations: Arc<AtomicU64>,
	pub(super) elements: HnswElements,
}

impl RollbackAction for InvalidateResident {
	fn run(
		self: Box<Self>,
		_drain_started_at: Instant,
	) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		Box::pin(async move {
			self.invalidations.fetch_add(1, Ordering::AcqRel);
			self.elements.evict_index().await;
			Ok(())
		})
	}
}

/// Builds a batch from its staged elements: steps 1 and 2, over the resident
/// graph as it stands under `resident`. Every task beyond the calling one runs
/// in a place taken through `places`.
///
/// Every vector the steps read belongs to *G₀*, which was committed before this
/// batch began, or to the batch, which they read from its own elements. Where
/// the datastore gives a read view, the steps run on up to `tasks` tasks and
/// read committed vectors through one view taken for them ([`ReadView`]),
/// whose transaction is cancelled before this returns. They read nothing
/// through `tx` then.
///
/// Where it gives none, the steps run on one task and read through `tx`, the
/// batch's own transaction. That reads the same vectors: what `tx` has buffered
/// for the batch is its new elements' vectors, which the batch's own elements
/// answer first, and a staged batch rewrites no committed vector. The graph is
/// the same either way, because it is a function of *G₀* and the batch alone.
pub(super) async fn build(
	resident: Arc<tokio::sync::OwnedRwLockReadGuard<ResidentGraph>>,
	tx: Arc<Transaction>,
	base: GraphBase,
	elements: Vec<NewElement>,
	places: &Ticket,
	tasks: usize,
) -> Result<StagedGraph> {
	let Some(view) = ReadView::open(&tx).await? else {
		return build_on(resident, tx, None, base, elements, places, 1).await;
	};
	let view = Arc::new(view);
	let res = build_on(resident, tx, Some(Arc::clone(&view)), base, elements, places, tasks).await;
	view.close().await;
	res
}

/// Steps 1 and 2 on up to `tasks` tasks, reading stored vectors through
/// `reads` when given, and through `tx` otherwise.
async fn build_on(
	resident: Arc<tokio::sync::OwnedRwLockReadGuard<ResidentGraph>>,
	tx: Arc<Transaction>,
	reads: Option<Arc<ReadView>>,
	base: GraphBase,
	elements: Vec<NewElement>,
	places: &Ticket,
	tasks: usize,
) -> Result<StagedGraph> {
	let input = Arc::new(BatchInput::new(base.next_element_id(), elements));
	let view = with_hnsw!(&resident.graph, h => h.elements.view(Some(Arc::new(input.overlay()))));
	let view = Arc::new(match reads {
		Some(reads) => view.reading_through(reads),
		None => view,
	});
	let started = Instant::now();
	let selections = {
		let (resident, tx, view, input) =
			(Arc::clone(&resident), Arc::clone(&tx), Arc::clone(&view), Arc::clone(&input));
		crate::parallel::run_jobs(places, tasks, input.len(), move |i| {
			let (resident, tx, view, input) =
				(Arc::clone(&resident), Arc::clone(&tx), Arc::clone(&view), Arc::clone(&input));
			async move { with_hnsw!(&resident.graph, h => select(h, &tx, &view, &input, i).await) }
		})
		.await?
	};
	let selected = started.elapsed();
	let jobs = Arc::new(link_jobs(&input, &selections));
	let selections = Arc::new(selections);
	let started = Instant::now();
	let edges = {
		let (resident, tx, view, input, selections, jobs) = (
			Arc::clone(&resident),
			Arc::clone(&tx),
			Arc::clone(&view),
			Arc::clone(&input),
			Arc::clone(&selections),
			Arc::clone(&jobs),
		);
		crate::parallel::run_jobs(places, tasks, jobs.len(), move |j| {
			let (resident, tx, view, input, selections, jobs) = (
				Arc::clone(&resident),
				Arc::clone(&tx),
				Arc::clone(&view),
				Arc::clone(&input),
				Arc::clone(&selections),
				Arc::clone(&jobs),
			);
			async move {
				with_hnsw!(&resident.graph, h => {
					link(h, &tx, &view, &input, &selections, &jobs[j]).await
				})
			}
		})
		.await?
	};
	debug!(
		target: "surrealdb::idx::hnsw",
		elements = input.len(),
		nodes = jobs.len(),
		tasks,
		select_ms = selected.as_millis() as u64,
		link_ms = started.elapsed().as_millis() as u64,
		"built staged HNSW batch"
	);
	Ok(with_hnsw!(&resident.graph, h => StagedGraph::new(h, base, &input, &jobs, edges, tasks)))
}

#[cfg(test)]
mod tests {
	use std::ops::Deref;
	use std::sync::Arc;

	use anyhow::Result;
	use ndarray::Array1;
	use rand::rngs::SmallRng;
	use rand::{Rng, SeedableRng};
	use surrealdb_datastore::Transaction;
	use surrealdb_kvs::TransactionType;

	use super::{NewElement, StagedGraph, build, build_on};
	use crate::IndexKeyBase;
	use crate::catalog::{
		DOC_IDS_FORMAT_VERSION, DatabaseId, Distance, HnswParams, IndexId, NamespaceId, TableId,
		VectorType,
	};
	use crate::config::BuildParallelism;
	use crate::env::IndexEnv;
	use crate::key::{KVKey, RawRange};
	use crate::test_env::{CountingPlaces, TestIndexEnv, TestIndexStore};
	use crate::trees::hnsw::batch::GraphBase;
	use crate::trees::hnsw::flavor::with_hnsw;
	use crate::trees::hnsw::index::{HnswCompactionOutcome, HnswIndex, ResidentGraph};
	use crate::trees::vector::{SharedVector, Vector};
	use crate::val::{RecordIdKey, Value};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);
	const TB: TableId = TableId(3);
	const IX: IndexId = IndexId(4);
	const DIMENSION: usize = 8;

	/// Parameters whose layer-0 sets are hashed and whose upper-layer sets are
	/// arrays, so the fixture covers both kinds.
	fn params(extend_candidates: bool, keep_pruned_connections: bool) -> HnswParams {
		let m: u8 = 16;
		HnswParams {
			dimension: DIMENSION as u16,
			distance: Distance::Euclidean,
			vector_type: VectorType::F32,
			m,
			m0: m * 2,
			ml: (1.0 / (m as f64).ln()).into(),
			ef_construction: 40,
			extend_candidates,
			keep_pruned_connections,
			use_hashed_vector: true,
		}
	}

	fn random_vector(rng: &mut SmallRng) -> SharedVector {
		let v: Vec<f32> = (0..DIMENSION).map(|_| rng.random_range(-1.0..1.0)).collect();
		Vector::F32(Array1::from_vec(v)).into()
	}

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NS, DB, "tb".into(), IX)
	}

	/// Commits a graph of `n` random vectors, built by the index's own
	/// compaction.
	async fn seed(ds: &TestIndexStore, p: &HnswParams, n: usize) -> Result<HnswIndex> {
		let ikb = ikb();
		let env = ds.env(TransactionType::Read).await;
		let h = HnswIndex::new(
			ds.index_stores().vector_cache().clone(),
			&env.tx(),
			ikb,
			TB,
			p,
			DOC_IDS_FORMAT_VERSION,
		)
		.await?;
		env.tx().cancel().await?;
		let mut rng = SmallRng::seed_from_u64(7);
		{
			let env = ds.env(TransactionType::Write).await;
			for i in 0..n {
				let content = vec![Value::from(random_vector(&mut rng).deref())];
				h.index(&env, &RecordIdKey::Number(i as i64), None, Some(content)).await?;
			}
			env.tx().commit().await?;
		}
		loop {
			let env = ds.env(TransactionType::Write).await;
			let compacted = h.index_pendings(&env).await?;
			env.tx().commit().await?;
			if compacted == 0 {
				return Ok(h);
			}
		}
	}

	/// A batch of `n` new elements, with levels that reach every layer the
	/// seeded graph has and one above it, so some layers are created by the
	/// batch.
	fn new_elements(first: u64, n: usize, top: usize) -> Vec<NewElement> {
		let mut rng = SmallRng::seed_from_u64(11);
		(0..n)
			.map(|i| NewElement {
				id: first + i as u64,
				level: match i % 97 {
					0 => top + 1,
					j if j % 13 == 0 => 1.min(top + 1),
					_ => 0,
				},
				vector: random_vector(&mut rng),
			})
			.collect()
	}

	/// What two builds of one batch are compared by: every resolved node, the
	/// layer versions, the entry point and the next element id.
	type Fingerprint = (Vec<(usize, u64, Vec<u64>)>, Vec<u64>, Option<u64>, u64);

	/// The resolved nodes and committed state a staged graph carries, in a
	/// form two builds can be compared by.
	fn fingerprint(staged: &StagedGraph) -> Fingerprint {
		let mut layers = vec![staged.state.layer0.version];
		layers.extend(staged.state.layers.iter().map(|l| l.version));
		(staged.nodes.clone(), layers, staged.state.enter_point, staged.state.next_element_id)
	}

	/// The graph a staged batch builds is a function of the committed graph
	/// and the batch alone: however many tasks build it, every resolved node,
	/// layer version and the entry point come out the same. The in-memory store
	/// gives no read view, so the steps are run directly, on each task count,
	/// through the one transaction.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn staged_batch_graph_does_not_depend_on_the_task_count() -> Result<()> {
		for (extend, keep) in [(false, false), (true, false), (false, true), (true, true)] {
			let p = params(extend, keep);
			let ds = TestIndexStore::new().await;
			let h = seed(&ds, &p, 150).await?;
			let resident = h.resident();
			let env = ds.env(TransactionType::Write).await;
			h.check_state(&env).await?;
			let mut builds = Vec::new();
			for tasks in [1, 4, 16] {
				let guard = Arc::new(Arc::clone(&resident).read_owned().await);
				let (base, top) = with_hnsw!(&guard.graph, h => (GraphBase::of(h), h.layers.len()));
				let elements = new_elements(base.next_element_id(), 120, top);
				let staged =
					build_on(guard, env.tx(), None, base, elements, &IndexEnv::places(&env), tasks)
						.await?;
				builds.push((tasks, fingerprint(&staged)));
			}
			env.tx().cancel().await?;
			let (_, first) = &builds[0];
			assert!(first.0.len() > 120, "the batch must resolve committed nodes too");
			for (tasks, build) in &builds[1..] {
				assert_eq!(
					build, first,
					"extend = {extend}, keep = {keep}: {tasks} tasks built a different graph"
				);
			}
		}
		Ok(())
	}

	/// A store opened on a fresh RocksDB directory, which gives read views.
	#[cfg(feature = "kv-rocksdb")]
	async fn rocksdb_store() -> (temp_dir::TempDir, TestIndexStore) {
		let dir = temp_dir::TempDir::new().unwrap();
		let ds =
			TestIndexStore::with_path(&format!("rocksdb:{}", dir.path().to_string_lossy())).await;
		(dir, ds)
	}

	/// A read guard on the resident graph, its base and its layer count, with
	/// the index's vectors dropped from the cache so the batch reads every
	/// committed vector it needs from the store.
	async fn cold_base(
		h: &HnswIndex,
		env: &TestIndexEnv,
	) -> Result<(Arc<tokio::sync::OwnedRwLockReadGuard<ResidentGraph>>, GraphBase, usize)> {
		h.check_state(env).await?;
		let guard = Arc::new(h.resident().read_owned().await);
		let (base, top) = with_hnsw!(&guard.graph, h => (GraphBase::of(h), h.layers.len()));
		with_hnsw!(&guard.graph, h => h.elements.evict_index().await);
		Ok((guard, base, top))
	}

	/// How many point reads a transaction has made so far.
	fn gets(tx: &Transaction) -> u32 {
		tx.metrics_snapshot_for_test().ops_get
	}

	/// Where a datastore gives read views, a staged batch's steps read through
	/// one, taken from one read-only transaction opened for them whatever the
	/// batch's task count, run on that task count, and read nothing through
	/// the batch's own transaction.
	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_staged_batch_reads_through_one_view_whatever_its_task_count() -> Result<()> {
		let (_dir, ds) = rocksdb_store().await;
		let h = seed(&ds, &params(false, false), 150).await?;
		for tasks in [1, 4, 16] {
			let env = ds.env(TransactionType::Write).await;
			let (guard, base, top) = cold_base(&h, &env).await?;
			let elements = new_elements(base.next_element_id(), 120, top);
			let (reads, gets_before) = (ds.reads_opened(), gets(&env.tx()));
			let staged = build(guard, env.tx(), base, elements, &env.places(), tasks).await?;
			assert_eq!(ds.reads_opened() - reads, 1, "tasks = {tasks}: read transactions opened");
			assert_eq!(staged.tasks(), tasks, "tasks the steps ran on");
			assert_eq!(
				gets(&env.tx()),
				gets_before,
				"tasks = {tasks}: reads through the batch's own transaction"
			);
			env.tx().cancel().await?;
		}
		Ok(())
	}

	/// Where a datastore gives no read view, a staged batch's steps run on one
	/// task, whatever task count the batch was given, read through the batch's
	/// own transaction, and open no other transaction.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn without_a_view_a_staged_batch_runs_on_one_task_through_its_transaction() -> Result<()>
	{
		let ds = TestIndexStore::new().await;
		let h = seed(&ds, &params(false, false), 150).await?;
		for tasks in [1, 4, 16] {
			let env = ds.env(TransactionType::Write).await;
			let (guard, base, top) = cold_base(&h, &env).await?;
			let elements = new_elements(base.next_element_id(), 120, top);
			let (reads, gets_before) = (ds.reads_opened(), gets(&env.tx()));
			let staged = build(guard, env.tx(), base, elements, &env.places(), tasks).await?;
			assert_eq!(ds.reads_opened() - reads, 0, "tasks = {tasks}: read transactions opened");
			assert_eq!(staged.tasks(), 1, "tasks = {tasks}: tasks the steps ran on");
			assert!(
				gets(&env.tx()) > gets_before,
				"tasks = {tasks}: no read through the batch's own transaction"
			);
			env.tx().cancel().await?;
		}
		Ok(())
	}

	/// A plan that only adds vectors is staged, not applied in place, on a
	/// datastore that gives no read view, and applying it opens no other
	/// transaction.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn without_a_view_an_insert_only_plan_is_still_staged() -> Result<()> {
		let ds = TestIndexStore::with_idx_config(|c| {
			c.hnsw_build_parallelism = BuildParallelism::Fixed(4.try_into().unwrap());
		})
		.await;
		let h = seed(&ds, &params(false, false), 150).await?;
		{
			let env = ds.env(TransactionType::Write).await;
			let mut rng = SmallRng::seed_from_u64(13);
			for i in 150..250 {
				let content = vec![Value::from(random_vector(&mut rng).deref())];
				h.index(&env, &RecordIdKey::Number(i), None, Some(content)).await?;
			}
			env.tx().commit().await?;
		}
		let env = ds.env(TransactionType::Write).await;
		let plan = HnswIndex::prepare_compaction(&env, &ikb()).await?;
		assert_eq!(plan.len(), 100, "the plan carries every new vector");
		let reads = ds.reads_opened();
		let outcome = h.apply_compaction(&env, plan).await?;
		assert_eq!(outcome, HnswCompactionOutcome::Staged);
		assert_eq!(ds.reads_opened() - reads, 0, "read transactions opened by the apply");
		env.tx().cancel().await?;
		Ok(())
	}

	/// The `!hn` and `!hs` entries `tx` holds for the index, as stored bytes.
	async fn stored_graph(tx: &Transaction, layers: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let ikb = ikb();
		let mut entries = Vec::new();
		for layer in 0..=layers {
			entries.extend(tx.getr(ikb.new_hn_layer_range(layer as u16)?, None).await?);
		}
		let state = ikb.new_hs_key().encode_key()?;
		let value = tx.get(state.clone(), None).await?.expect("a staged batch writes the state");
		entries.push((state.as_slice().to_vec(), value));
		Ok(entries)
	}

	/// One plan over one base graph writes byte-identical `!hn` and `!hs`
	/// entries whether its steps read through a read view, on any task count,
	/// or through the batch's own transaction.
	async fn one_plan_writes_one_graph(ds: &TestIndexStore, views: bool) -> Result<()> {
		let h = seed(ds, &params(true, true), 150).await?;
		let mut writes = Vec::new();
		for (tasks, as_built) in [(1, true), (4, true), (16, true), (1, false), (4, false)] {
			let env = ds.env(TransactionType::Write).await;
			let (guard, base, top) = cold_base(&h, &env).await?;
			let elements = new_elements(base.next_element_id(), 120, top);
			// As a batch builds itself, through a view where there is one, or
			// forced through its own transaction.
			let staged = if as_built {
				build(Arc::clone(&guard), env.tx(), base, elements, &env.places(), tasks).await?
			} else {
				build_on(Arc::clone(&guard), env.tx(), None, base, elements, &env.places(), tasks)
					.await?
			};
			if as_built {
				let expected = if views {
					tasks
				} else {
					1
				};
				assert_eq!(staged.tasks(), expected, "tasks the steps ran on");
			}
			with_hnsw!(&guard.graph, h => staged.write(h, &env.tx()).await)?;
			writes.push((tasks, as_built, stored_graph(&env.tx(), top + 1).await?));
			env.tx().cancel().await?;
		}
		let (_, _, first) = &writes[0];
		assert!(first.len() > 120, "the batch must write committed nodes too");
		for (tasks, as_built, written) in &writes[1..] {
			assert!(
				written == first,
				"{tasks} tasks, as built = {as_built}: a different graph was written"
			);
		}
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn one_plan_writes_one_graph_without_a_view() -> Result<()> {
		one_plan_writes_one_graph(&TestIndexStore::new().await, false).await
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn one_plan_writes_one_graph_with_a_view_or_without() -> Result<()> {
		let (_dir, ds) = rocksdb_store().await;
		one_plan_writes_one_graph(&ds, true).await
	}

	/// The entries a staged batch writes, every `!hn` node and the `!hs`
	/// state, are the same bytes whether its workers were granted places,
	/// gave them back mid-run because another registration waited, or were
	/// never granted one. The in-memory store gives no read view, so each batch
	/// runs its steps on eight tasks through its own transaction, where `build`
	/// would run them on one.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn staged_batch_entries_do_not_depend_on_the_places_granted() -> Result<()> {
		let p = params(true, false);
		let ds = TestIndexStore::new().await;
		let h = seed(&ds, &p, 150).await?;
		let resident = h.resident();
		let env = ds.env(TransactionType::Write).await;
		h.check_state(&env).await?;
		env.tx().cancel().await?;
		let granted = CountingPlaces::new(8, 1);
		let yielding = CountingPlaces::new(8, 1);
		yielding.others_wait_from(6);
		let never = CountingPlaces::new(0, 1);
		let mut written = Vec::new();
		for pool in [&granted, &yielding, &never] {
			let env = ds.env(TransactionType::Write).await;
			let guard = Arc::new(Arc::clone(&resident).read_owned().await);
			let (base, top) = with_hnsw!(&guard.graph, h => (GraphBase::of(h), h.layers.len()));
			let elements = new_elements(base.next_element_id(), 120, top);
			let staged =
				build_on(Arc::clone(&guard), env.tx(), None, base, elements, &pool.ticket(), 8)
					.await?;
			with_hnsw!(&guard.graph, h => staged.write(h, &env.tx()).await)?;
			written.push(env.tx().scan_raw(RawRange::every_key(), u32::MAX, 0, None).await?);
			env.tx().cancel().await?;
			assert_eq!(pool.held(), 0, "every place went back");
		}
		assert!(granted.peak() > 1, "the first build ran on several places");
		assert!(yielding.taken() > 0 && yielding.yields() > 0, "the second yielded mid-run");
		assert_eq!(never.taken(), 0, "the third ran on the calling task alone");
		assert!(written[0].len() > 150, "the scan reads the written graph");
		assert!(written[1] == written[0], "a build that yielded mid-run wrote other bytes");
		assert!(written[2] == written[0], "a build without a place wrote other bytes");
		Ok(())
	}
}
