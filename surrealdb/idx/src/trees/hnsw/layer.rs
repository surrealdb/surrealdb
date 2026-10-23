use ahash::HashSet;
use anyhow::{Result, bail};
use common::EngineError;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
pub(super) use surrealdb_datastore::values::hnsw::LayerState;
use surrealdb_kvs::Direction;

use crate::IndexKeyBase;
use crate::env::IndexEnv;
use crate::key::KVKeyDecode;
use crate::key::schema::HnswNodeKey;
use crate::trees::dynamicset::DynamicSet;
use crate::trees::graph::UndirectedGraph;
use crate::trees::hnsw::docs::VecDocs;
use crate::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::trees::hnsw::heuristic::Heuristic;
use crate::trees::hnsw::index::HnswContext;
use crate::trees::hnsw::{ElementId, HnswElements, HnswSearch, VectorId};
use crate::trees::knn::{DoublePriorityQueue, Ids64};
use crate::trees::vector::SharedVector;

/// What [`HnswLayer::load`] re-read from the store.
///
/// A reload drops the layer's resident graph and streams its whole `Hn` range
/// back, so this is the accounting a test needs to say how much of the graph an
/// operation rebuilt.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ReloadCounts {
	/// `Hn` range scans opened, one per layer reloaded.
	pub(super) scans: usize,
	/// Nodes those scans streamed back into a layer graph.
	pub(super) nodes: usize,
}

// Reload work performed on this thread and not yet taken.
//
// Per thread rather than per index: the layers sit behind the graph lock and
// are reachable only through the index that owns them, so there is nowhere to
// hang a per-index counter that a test can read. A test reading it must
// therefore run on a current-thread runtime, where its own future is the only
// producer.
#[cfg(test)]
thread_local! {
	static RELOAD_COUNTS: std::cell::Cell<ReloadCounts> =
		const { std::cell::Cell::new(ReloadCounts { scans: 0, nodes: 0 }) };
}

/// Returns the reload work recorded on this thread and resets the count.
#[cfg(test)]
pub(super) fn take_reload_counts() -> ReloadCounts {
	RELOAD_COUNTS.with(|c| c.replace(ReloadCounts::default()))
}

#[cfg(test)]
fn record_reload_scan() {
	RELOAD_COUNTS.with(|c| {
		let mut counts = c.get();
		counts.scans += 1;
		c.set(counts);
	});
}

#[cfg(test)]
fn record_reload_node() {
	RELOAD_COUNTS.with(|c| {
		let mut counts = c.get();
		counts.nodes += 1;
		c.set(counts);
	});
}

/// The documents a kNN search's pending scan has already scored, which the
/// graph search must not surface again, and the mapping that resolves an
/// element to its documents.
#[derive(Clone, Copy)]
pub(super) struct PendingDocs<'a> {
	pub(super) vec_docs: &'a VecDocs,
	pub(super) docs: &'a RoaringTreemap,
}

#[derive(Debug)]
pub(super) struct HnswLayer<S>
where
	S: DynamicSet,
{
	ikb: IndexKeyBase,
	level: u16,
	graph: UndirectedGraph<S>,
	m_max: usize,
}

impl<S> HnswLayer<S>
where
	S: DynamicSet,
{
	pub(super) fn new(ikb: IndexKeyBase, level: usize, m_max: usize) -> Self {
		Self {
			ikb,
			level: level as u16,
			graph: UndirectedGraph::new(m_max + 1),
			m_max,
		}
	}

	pub(super) fn m_max(&self) -> usize {
		self.m_max
	}

	pub(super) fn get_edges(&self, e_id: ElementId) -> Option<&S> {
		self.graph.get_edges(e_id)
	}

	/// Whether `e_id` is an element of this layer.
	///
	/// Edges are not kept symmetric, so removing an element leaves the edges
	/// other nodes hold towards it in place, here and in the store. The layer,
	/// reconciled with the reader's snapshot, is what says an element still
	/// exists; the process-wide vector cache cannot, because a reader on an
	/// older snapshot can enter a removed element's vector into it after the
	/// removal evicted it. Anything that reaches an element through an edge
	/// checks this before reading its vector, so a removed element is never
	/// searched through, selected as a neighbour, or written back as a node.
	pub(super) fn is_node(&self, e_id: ElementId) -> bool {
		self.graph.get_edges(e_id).is_some()
	}

	/// An empty edge set sized for this layer.
	pub(super) fn new_edges(&self) -> S {
		self.graph.new_edges()
	}

	/// Replaces a node's edges, adding the node if the layer lacks it. Used to
	/// install a committed batch's resolved nodes; persisting them is the
	/// batch's own business.
	pub(super) fn set_edges(&mut self, node: ElementId, edges: S) {
		self.graph.set_node(node, edges);
	}

	pub(super) async fn add_empty_node(
		&mut self,
		tx: &Transaction,
		node: ElementId,
		st: &mut LayerState,
	) -> Result<bool> {
		if !self.graph.add_empty_node(node) {
			return Ok(false);
		}
		self.save_nodes(tx, st, &[node]).await?;
		Ok(true)
	}
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search_single(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		ef: usize,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<DoublePriorityQueue> {
		let Some(pending_docs) = pending_docs else {
			return self.search_nearest(&ctx.tx, elements, pt, ep_dist, ep_id, ef).await;
		};
		let pending = PendingDocs {
			vec_docs: ctx.vec_docs,
			docs: pending_docs,
		};
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let mut w = DoublePriorityQueue::default();
		if let Some(ep_pt) = elements.get_vector(&ctx.tx, &ep_id).await?
			&& !Self::are_all_docs_in_pending(&ctx.tx, Some(pending), ep_id, &ep_pt).await?
		{
			w.push(ep_dist, ep_id);
		}
		self.search(&ctx.tx, elements, pt, candidates, visited, w, ef, Some(pending)).await
	}

	/// The `ef` elements nearest `pt` reachable from one entry point, with no
	/// pending documents to suppress.
	pub(super) async fn search_nearest(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		ef: usize,
	) -> Result<DoublePriorityQueue> {
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let w = candidates.clone();
		self.search(tx, elements, pt, candidates, visited, w, ef, None).await
	}

	pub(super) async fn search_single_with_ignore(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		pt: &SharedVector,
		ignore_id: ElementId,
		ef: usize,
	) -> Result<Option<ElementId>> {
		let visited = HashSet::from_iter([ignore_id]);
		let mut candidates = DoublePriorityQueue::default();
		if let Some(dist) = elements.get_distance(&ctx.tx, pt, &ignore_id).await? {
			candidates.push(dist, ignore_id);
		}
		let w = DoublePriorityQueue::default();
		let q = self.search(&ctx.tx, elements, pt, candidates, visited, w, ef, None).await?;
		Ok(q.peek_first().map(|(_, e_id)| e_id))
	}

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search_single_with_filter(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		elements: &HnswElements,
		search: &HnswSearch,
		ep_dist: f64,
		ep_id: ElementId,
		ep_pt: &SharedVector,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<DoublePriorityQueue> {
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let mut w = DoublePriorityQueue::default();
		// The entry point's admission must use ITS vector, not the query's:
		// on a cold cache the doc-set lookup keys off the vector, so passing
		// the query vector resolves another vector's (usually no) doc set,
		// wrongly rejecting the entry point and caching the wrong verdict
		// under its element id.
		Self::add_if_truthy(
			ctx,
			stk,
			search.ef,
			&mut w,
			ep_pt,
			ep_dist,
			ep_id,
			filter,
			pending_docs,
			allow_list,
		)
		.await?;
		self.search_with_filter(
			ctx,
			stk,
			elements,
			search,
			candidates,
			visited,
			w,
			filter,
			pending_docs,
			allow_list,
		)
		.await
	}

	pub(super) async fn search_multi(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		candidates: DoublePriorityQueue,
		ef: usize,
	) -> Result<DoublePriorityQueue> {
		let w = candidates.clone();
		let visited = w.to_set();
		self.search(tx, elements, pt, candidates, visited, w, ef, None).await
	}

	/// The candidates a removal re-links `q_id` to once `removed`, one of its
	/// neighbours, has left the layer: a search for `pt`, `q_id`'s vector,
	/// seeded with `q_id` and with every other node of `neighbourhood`, the
	/// removed node's former neighbours, which are the nodes the removal cut
	/// `q_id` off from. Seeded with `q_id` alone, a node whose only edge led to
	/// `removed` would find nothing and be left without edges, reachable by no
	/// walk.
	#[expect(clippy::too_many_arguments)]
	async fn search_repair(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		pt: &SharedVector,
		q_id: ElementId,
		removed: ElementId,
		neighbourhood: &[ElementId],
		efc: usize,
	) -> Result<DoublePriorityQueue> {
		let mut candidates = DoublePriorityQueue::default();
		let mut w = DoublePriorityQueue::default();
		let mut visited = HashSet::from_iter([q_id, removed]);
		// Expanded first under every metric: `q_id` is never in `w`, and an
		// inner-product distance can rank a neighbour ahead of `q_id` itself, so
		// `q_id` popped at its own distance could end the search unexpanded.
		candidates.push(f64::NEG_INFINITY, q_id);
		for &n_id in neighbourhood {
			if !self.is_node(n_id) || !visited.insert(n_id) {
				continue;
			}
			if let Some(dist) = elements.get_distance(&ctx.tx, pt, &n_id).await? {
				candidates.push(dist, n_id);
				w.push(dist, n_id);
				if w.len() > efc {
					w.pop_last();
				}
			}
		}
		self.search(&ctx.tx, elements, pt, candidates, visited, w, efc, None).await
	}

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		q: &SharedVector,
		mut candidates: DoublePriorityQueue, // set of candidates
		mut visited: HashSet<ElementId>,     // set of visited elements
		mut w: DoublePriorityQueue,
		ef: usize,
		pending: Option<PendingDocs<'_>>,
	) -> Result<DoublePriorityQueue> {
		let mut fq_dist = w.peek_last_dist().unwrap_or(f64::MAX);
		while let Some((cq_dist, doc)) = candidates.pop_first() {
			// Distance ends the walk only once `ef` results are held. Pending
			// elements are expanded without being admitted, so a few near
			// results can sit closer than the pending elements that are the
			// only way on to the rest; stopping at them would return fewer
			// results than the graph holds. Without pending documents every
			// visited element is admitted, and no candidate is farther than
			// the last result until `w` is full.
			if w.len() >= ef && cq_dist > fq_dist {
				break;
			}
			if let Some(neighbourhood) = self.graph.get_edges(doc) {
				for &e_id in neighbourhood.iter() {
					// Did we already visit it? Is it still an element?
					if !visited.insert(e_id) || !self.is_node(e_id) {
						continue;
					}
					if let Some(e_pt) = elements.get_vector(tx, &e_id).await? {
						let e_dist = elements.distance(&e_pt, q);
						if e_dist < fq_dist || w.len() < ef {
							// An element whose documents are all pending is still
							// expanded, only never admitted: the search scores
							// pending documents on their own, and dropping their
							// elements from the frontier as well would cut the walk
							// off from everything reachable only through them.
							candidates.push(e_dist, e_id);
							if Self::are_all_docs_in_pending(tx, pending, e_id, &e_pt).await? {
								continue;
							}
							w.push(e_dist, e_id);
							if w.len() > ef {
								w.pop_last();
							}
							fq_dist = w.peek_last_dist().unwrap_or(f64::MAX);
						}
					}
				}
			}
		}
		Ok(w)
	}

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search_with_filter(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		elements: &HnswElements,
		search: &HnswSearch,
		mut candidates: DoublePriorityQueue,
		mut visited: HashSet<ElementId>,
		mut w: DoublePriorityQueue,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<DoublePriorityQueue> {
		let mut f_dist = w.peek_last_dist().unwrap_or(f64::MAX);

		while let Some((dist, doc)) = candidates.pop_first() {
			if dist > f_dist {
				break;
			}
			if let Some(neighbourhood) = self.graph.get_edges(doc) {
				// Warm the transaction record cache for this neighbourhood's
				// filter-eligible candidates in one batch, so the per-candidate
				// `get_record` calls inside `add_if_truthy` below become cache
				// hits instead of each issuing an individual round-trip. The loop
				// itself is left unchanged, so results are identical.
				self.prefetch_neighbourhood_records(
					ctx,
					elements,
					search,
					neighbourhood,
					&visited,
					w.len(),
					f_dist,
					filter,
					pending_docs,
					allow_list,
				)
				.await?;
				for &e_id in neighbourhood.iter() {
					// Did we already visit it? Is it still an element?
					if !visited.insert(e_id) || !self.is_node(e_id) {
						continue;
					}
					if let Some(e_pt) = elements.get_vector(&ctx.tx, &e_id).await? {
						let e_dist = elements.distance(&e_pt, &search.pt);
						if e_dist < f_dist || w.len() < search.ef {
							candidates.push(e_dist, e_id);
							if Self::add_if_truthy(
								ctx,
								stk,
								search.ef,
								&mut w,
								&e_pt,
								e_dist,
								e_id,
								filter,
								pending_docs,
								allow_list,
							)
							.await?
							{
								f_dist = w.peek_last_dist().expect("w is non-empty"); // w can't be empty
							}
						}
					}
				}
			}
		}
		Ok(w)
	}

	/// Prefetches the records of a popped node's filter-eligible neighbours in a
	/// single batch, warming the transaction cache before the (unchanged)
	/// evaluation loop in [`Self::search_with_filter`] reads them one by one.
	///
	/// Only neighbours that are unvisited and pass the distance gate — using the
	/// `f_dist` / `w_len` captured at the *start* of this neighbourhood — are
	/// considered. That start-of-neighbourhood gate is the loosest the loop will
	/// use: while `w` is below `ef` the `w_len < ef` term holds unconditionally
	/// (so `f_dist`, which can transiently rise in that phase, is irrelevant), and
	/// once `w` is full `f_dist` only decreases — so the loop's evolving gate is
	/// always a subset. The collected set is therefore a superset of what the loop
	/// fetches, so every record it needs is already cached. Over-collecting never
	/// affects correctness, but it is not free: a candidate the evolving gate
	/// later skips still has its record pulled in this batch (the old one-by-one
	/// path skipped such candidates *before* fetching), so this trades fewer
	/// round-trips for potentially more total record bytes read — a win only on
	/// latency-bound backends. Doc-sets are resolved cache-first and pending-only
	/// elements are skipped, mirroring [`Self::add_if_truthy`].
	#[expect(clippy::too_many_arguments)]
	async fn prefetch_neighbourhood_records(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		search: &HnswSearch,
		neighbourhood: &S,
		visited: &HashSet<ElementId>,
		w_len: usize,
		f_dist: f64,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<()> {
		// Without a truthy filter there is no record evaluation to warm up:
		// admission is decided purely on the allow-list bitmap, with zero
		// record fetches.
		let Some(filter) = filter.as_mut() else {
			return Ok(());
		};
		let mut ids: Vec<VectorId> = Vec::new();
		for &e_id in neighbourhood.iter() {
			if visited.contains(&e_id) || !self.is_node(e_id) {
				continue;
			}
			let Some(e_pt) = elements.get_vector(&ctx.tx, &e_id).await? else {
				continue;
			};
			let e_dist = elements.distance(&e_pt, &search.pt);
			// Same gate as the evaluation loop, but against the f_dist / w.len()
			// captured at the start of this neighbourhood — the loosest form (see
			// the method doc), so the collected set is a superset of the loop's.
			if !(e_dist < f_dist || w_len < search.ef) {
				continue;
			}
			let Some(docs) = ctx.vec_docs.get_docs_by_element(&ctx.tx, e_id, &e_pt).await? else {
				continue;
			};
			if let Some(pending_docs) = pending_docs
				&& Self::check_all_docs_in_pending(&docs, pending_docs)
			{
				continue;
			}
			for doc_id in docs.iter() {
				// Bitmap-rejected and individually-pending candidates are never
				// evaluated by the filter, so their records must not be
				// prefetched either.
				if pending_docs.is_some_and(|pending| pending.contains(doc_id)) {
					continue;
				}
				if let Some(allow) = allow_list
					&& !allow.contains(doc_id)
				{
					continue;
				}
				ids.push(VectorId::DocId(doc_id));
			}
		}
		filter.prefetch_records(ctx, &ids).await
	}

	/// Decides whether an element is admitted into the result queue `w`.
	///
	/// The caller has already pushed the element into the traversal
	/// candidates, so a rejection here never stops the graph walk (standard
	/// filtered-HNSW: expansion traverses disallowed nodes, admission is
	/// gated). Gates run cheapest-first:
	/// 1. pending suppression (`pending_docs` — the exact pending scan is the source of truth for
	///    those docs, so the graph hit is ignored);
	/// 2. allow-list admission (#548 — `allow_list` holds the doc-IDs whose records satisfy the
	///    index-covered WHERE conjuncts; an element with no doc in the bitmap is rejected without
	///    any record fetch);
	/// 3. the truthy filter (record fetch + permission gate + residual cond), when one is present.
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn add_if_truthy(
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		efc: usize,
		w: &mut DoublePriorityQueue,
		e_pt: &SharedVector,
		e_dist: f64,
		e_id: ElementId,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<bool> {
		if let Some(docs) = ctx.vec_docs.get_docs_by_element(&ctx.tx, e_id, e_pt).await? {
			if let Some(pending_docs) = pending_docs
				// Check all these docs are currently updated the pending
				&& Self::check_all_docs_in_pending(&docs, pending_docs)
			{
				// In this case we ignore the one in the HNSW index
				return Ok(false);
			}
			// Per-doc gates ignore pending docs entirely: the pending scan is
			// their source of truth (it scores their CURRENT vectors) and
			// result materialization suppresses them, so admitting an element
			// on the strength of a pending doc alone would burn an `ef` slot
			// that can never yield a row — crowding out committed neighbours.
			if let Some(allow) = allow_list
				&& !Self::check_any_doc_in_allow(&docs, pending_docs, allow)
			{
				return Ok(false);
			}
			let truthy = match filter {
				Some(filter) => {
					filter.check_any_doc_truthy(ctx, stk, docs, pending_docs, allow_list).await?
				}
				None => true,
			};
			if truthy {
				w.push(e_dist, e_id);
				if w.len() > efc {
					w.pop_last();
				}
				return Ok(true);
			}
		}
		Ok(false)
	}

	fn check_all_docs_in_pending(docs: &Ids64, pending_docs: &RoaringTreemap) -> bool {
		if pending_docs.is_empty() {
			return false;
		}
		for doc_id in docs.iter() {
			if !pending_docs.contains(doc_id) {
				return false;
			}
		}
		true
	}

	/// Returns `true` iff any of the element's non-pending doc-IDs is in the
	/// allow-list.
	///
	/// Inverse polarity of [`Self::check_all_docs_in_pending`]: `pending_docs`
	/// suppresses an element when *all* its docs are pending, while the
	/// allow-list admits an element when *any* of its docs is allowed (an
	/// empty allow-list therefore admits nothing). Pending docs never count
	/// towards admission — the pending scan is their source of truth and
	/// result materialization skips them.
	fn check_any_doc_in_allow(
		docs: &Ids64,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: &RoaringTreemap,
	) -> bool {
		for doc_id in docs.iter() {
			if pending_docs.is_some_and(|pending| pending.contains(doc_id)) {
				continue;
			}
			if allow_list.contains(doc_id) {
				return true;
			}
		}
		false
	}

	async fn are_all_docs_in_pending(
		tx: &Transaction,
		pending: Option<PendingDocs<'_>>,
		e_id: ElementId,
		e_pt: &SharedVector,
	) -> Result<bool> {
		let Some(pending) = pending else {
			return Ok(false);
		};
		if pending.docs.is_empty() {
			return Ok(false);
		}
		if let Some(docs) = pending.vec_docs.get_docs_by_element(tx, e_id, e_pt).await? {
			for doc_id in docs.iter() {
				if !pending.docs.contains(doc_id) {
					return Ok(false);
				}
			}
		}
		Ok(true)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn insert(
		&mut self,
		ctx: &HnswContext<'_>,
		st: &mut LayerState,
		elements: &HnswElements,
		heuristic: &Heuristic,
		efc: usize,
		(q_id, q_pt): (ElementId, &SharedVector),
		mut eps: DoublePriorityQueue,
	) -> Result<DoublePriorityQueue> {
		let w;
		let mut neighbors = self.graph.new_edges();
		{
			w = self.search_multi(&ctx.tx, elements, q_pt, eps, efc).await?;
			eps = w.clone();
			heuristic.select(&ctx.tx, elements, self, q_id, q_pt, w, None, &mut neighbors).await?;
		};

		let neighbors = self.graph.add_node_and_bidirectional_edges(q_id, neighbors);
		self.prune_overfull(&ctx.tx, elements, heuristic, &neighbors).await?;
		// Save the new node and all its neighbors (which had bidirectional edges added/pruned)
		let mut changed_nodes = Vec::with_capacity(neighbors.len() + 1);
		changed_nodes.push(q_id);
		changed_nodes.extend_from_slice(&neighbors);
		self.save_nodes(&ctx.tx, st, &changed_nodes).await?;
		Ok(eps)
	}

	/// Re-selects, with `heuristic`, the list of each of `neighbors` that an
	/// edge just added took past `m_max`.
	async fn prune_overfull(
		&mut self,
		tx: &Transaction,
		elements: &HnswElements,
		heuristic: &Heuristic,
		neighbors: &[ElementId],
	) -> Result<()> {
		for e_id in neighbors {
			if let Some(e_conn) = self.graph.get_edges(*e_id) {
				if e_conn.len() > self.m_max
					&& let Some(e_pt) = elements.get_vector(tx, e_id).await?
				{
					let e_c = self.build_priority_list(tx, elements, *e_id, e_conn).await?;
					let mut e_new_conn = self.graph.new_edges();
					heuristic
						.select(tx, elements, self, *e_id, &e_pt, e_c, None, &mut e_new_conn)
						.await?;
					#[cfg(debug_assertions)]
					assert!(!e_new_conn.contains(e_id));
					self.graph.set_node(*e_id, e_new_conn);
				}
			} else {
				#[cfg(debug_assertions)]
				unreachable!("Element: {}", e_id);
			}
		}
		Ok(())
	}

	async fn build_priority_list(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		e_id: ElementId,
		neighbors: &S,
	) -> Result<DoublePriorityQueue> {
		let mut w = DoublePriorityQueue::default();
		if let Some(e_pt) = elements.get_vector(tx, &e_id).await? {
			for n_id in neighbors.iter() {
				if !self.is_node(*n_id) {
					continue;
				}
				if let Some(n_pt) = elements.get_vector(tx, n_id).await? {
					let dist = elements.distance(&e_pt, &n_pt);
					w.push(dist, *n_id);
				}
			}
		}
		Ok(w)
	}

	pub(super) async fn remove(
		&mut self,
		ctx: &HnswContext<'_>,
		st: &mut LayerState,
		elements: &HnswElements,
		heuristic: &Heuristic,
		e_id: ElementId,
		efc: usize,
	) -> Result<bool> {
		if let Some(f_ids) = self.graph.remove_node_and_bidirectional_edges(e_id) {
			let mut changed_nodes = Vec::with_capacity(f_ids.len());
			let neighbourhood: Vec<ElementId> = f_ids.iter().copied().collect();
			for &q_id in f_ids.iter() {
				// An edge towards an element already removed is dropped with
				// the node, not repaired: repairing it would write the removed
				// element back as a node.
				if !self.is_node(q_id) {
					continue;
				}
				// Its list lost the edge to the removed element, whether or not
				// it is re-linked below.
				changed_nodes.push(q_id);
				if let Some(q_pt) = elements.get_vector(&ctx.tx, &q_id).await? {
					let c = self
						.search_repair(ctx, elements, &q_pt, q_id, e_id, &neighbourhood, efc)
						.await?;
					let mut q_new_conn = self.graph.new_edges();
					heuristic
						.select(
							&ctx.tx,
							elements,
							self,
							q_id,
							&q_pt,
							c,
							Some(e_id),
							&mut q_new_conn,
						)
						.await?;
					#[cfg(debug_assertions)]
					{
						assert!(
							!q_new_conn.contains(&q_id),
							"!q_new_conn.contains(&q_id) - q_id: {q_id} - f_ids: {q_new_conn:?}"
						);
						assert!(
							!q_new_conn.contains(&e_id),
							"!q_new_conn.contains(&e_id) - e_id: {e_id} - f_ids: {q_new_conn:?}"
						);
						assert!(q_new_conn.len() <= self.m_max);
					}
					// A walk here can follow an edge to an element already
					// removed, so the selection may name one; linking it would
					// write it back as a node.
					let mut live = self.graph.new_edges();
					for n_id in q_new_conn.iter().copied().filter(|n_id| self.is_node(*n_id)) {
						live.insert(n_id);
					}
					// Linked both ways, as an insert links: a list rewritten
					// without edges back to it leaves `q` reachable only through
					// the edges still pointing at it, which later rewrites drop.
					let linked = self.graph.add_node_and_bidirectional_edges(q_id, live);
					self.prune_overfull(&ctx.tx, elements, heuristic, &linked).await?;
					changed_nodes.extend(linked);
				}
			}
			changed_nodes.sort_unstable();
			changed_nodes.dedup();
			// Delete the removed node's key and save all modified neighbor nodes
			self.delete_node(&ctx.tx, e_id).await?;
			self.save_nodes(&ctx.tx, st, &changed_nodes).await?;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	/// Persists only the specified nodes to the KV store using per-node `Hn` keys.
	/// Each node's edge list is serialized independently, avoiding full-graph serialization.
	async fn save_nodes(
		&self,
		tx: &Transaction,
		st: &mut LayerState,
		nodes: &[ElementId],
	) -> Result<()> {
		for &node_id in nodes {
			if let Some(val) = self.graph.node_to_val(node_id) {
				let key = self.ikb.new_hn_key(self.level, node_id);
				tx.set_key(&key, &val).await?;
			}
		}
		// Increase the version
		st.version += 1;
		Ok(())
	}

	/// Deletes a single node's `Hn` key from the KV store.
	async fn delete_node(&self, tx: &Transaction, node_id: ElementId) -> Result<()> {
		let key = self.ikb.new_hn_key(self.level, node_id);
		tx.del_key(&key).await?;
		Ok(())
	}

	/// Loads the graph for this layer from the KV store.
	///
	/// Handles three storage states:
	/// 1. **Fully migrated** (`st.chunks == 0`): loads only from per-node `Hn` keys.
	/// 2. **Legacy only** (`st.chunks > 0`, no `Hn` keys): loads from chunk-based `Hl` keys.
	/// 3. **Mixed** (`st.chunks > 0` *and* `Hn` keys exist): loads `Hl` chunks first for the
	///    complete baseline graph, then overlays `Hn` keys which carry the most recent state for
	///    their respective nodes.
	///
	/// In cases 2 and 3, if the transaction is writable the method completes the
	/// migration: all nodes are persisted as `Hn` keys, the old `Hl` chunk keys
	/// are deleted, and `st.chunks` is reset to 0. On a read-only transaction the
	/// legacy data is loaded into memory without migration.
	///
	/// Returns `true` if a migration was performed, so the caller can persist
	/// the updated layer state.
	pub(super) async fn load(
		&mut self,
		env: &dyn IndexEnv,
		tx: &Transaction,
		st: &mut LayerState,
	) -> Result<bool> {
		self.graph.clear();

		// Load legacy Hl chunks (if any) as the baseline graph.
		if st.chunks > 0 {
			let mut val = Vec::new();
			for i in 0..st.chunks {
				let key = self.ikb.new_hl_key(self.level, i);
				let chunk = tx
					.get_key(&key, None)
					.await?
					.ok_or_else(|| EngineError::unreachable("Missing chunk"))?;
				val.extend(chunk);
			}
			self.graph.lecacy_reload(&val)?;
		}

		// These represent the most recent state
		// for each node and take precedence over the Hl data loaded above.
		let range = self.ikb.new_hn_layer_range(self.level)?;
		let mut count = 0;
		#[cfg(test)]
		record_reload_scan();
		let mut cursor = tx.open_vals_cursor(range, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (k, v) in &batch {
				// Check if the query has been stopped
				if env.is_done(Some(count)).await? {
					bail!(EngineError::QueryCancelled);
				}
				let key = HnswNodeKey::decode_key(k)?;
				self.graph.load_node(key.node, v);
				#[cfg(test)]
				record_reload_node();
				count += 1;
			}
		}
		drop(cursor);

		// If we can write, complete the migration:
		// persist every node as an Hn key and remove the old Hl chunk keys.
		if st.chunks > 0 && tx.writeable() {
			// Write every node as an Hn key. Nodes that already had Hn entries
			// are rewritten with the same data (their state was overlaid onto
			// the graph in the streaming step above).
			for &node_id in &self.graph.node_ids() {
				if let Some(node_val) = self.graph.node_to_val(node_id) {
					let key = self.ikb.new_hn_key(self.level, node_id);
					tx.set_key(&key, &node_val).await?;
				}
			}
			// Delete old Hl chunk keys in a single range deletion
			let hl_range = self.ikb.new_hl_layer_range(self.level)?;
			tx.delr(hl_range).await?;
			// Reset the chunk count so subsequent reloads don't
			// attempt to fetch the now-deleted Hl keys.
			st.chunks = 0;
			return Ok(true);
		}
		Ok(false)
	}
}

#[cfg(test)]
impl<S> HnswLayer<S>
where
	S: DynamicSet,
{
	/// The layer's node count and its total out-degree.
	#[cfg(test)]
	pub(in crate::trees::hnsw) fn degree_totals(&self) -> (usize, usize) {
		let nodes = self.graph.nodes();
		(nodes.len(), nodes.values().map(|edges| edges.len()).sum())
	}

	/// Every node of the layer with its edges, both in id order.
	#[cfg(test)]
	pub(in crate::trees::hnsw) fn adjacency(&self) -> Vec<(ElementId, Vec<ElementId>)> {
		let mut nodes: Vec<(ElementId, Vec<ElementId>)> = self
			.graph
			.nodes()
			.iter()
			.map(|(node, edges)| {
				let mut edges: Vec<ElementId> = edges.iter().copied().collect();
				edges.sort_unstable();
				(*node, edges)
			})
			.collect();
		nodes.sort_unstable_by_key(|(node, _)| *node);
		nodes
	}

	/// The nodes of the layer a walk from `entry` cannot reach along edges to
	/// nodes of the layer, in id order.
	pub(in crate::trees::hnsw) fn unreachable_from(&self, entry: ElementId) -> Vec<ElementId> {
		let mut seen = std::collections::HashSet::from([entry]);
		let mut stack = vec![entry];
		while let Some(node) = stack.pop() {
			if let Some(edges) = self.graph.get_edges(node) {
				for edge in edges.iter().copied().filter(|edge| self.is_node(*edge)) {
					if seen.insert(edge) {
						stack.push(edge);
					}
				}
			}
		}
		let mut unreachable: Vec<ElementId> =
			self.graph.nodes().keys().copied().filter(|node| !seen.contains(node)).collect();
		unreachable.sort_unstable();
		unreachable
	}

	pub(in crate::trees::hnsw) async fn check_props(&self, elements: &HnswElements) {
		let elements_len = elements.len().await;
		assert!(self.graph.len() <= elements_len, "{} - {}", self.graph.len(), elements_len);
		for (e_id, f_ids) in self.graph.nodes() {
			assert!(
				f_ids.len() <= self.m_max,
				"Foreign list e_id: {e_id} - len = len({}) <= m_layer({})",
				self.m_max,
				f_ids.len(),
			);
			assert!(!f_ids.contains(e_id), "!f_ids.contains(e_id) - el: {e_id} - f_ids: {f_ids:?}");
			assert!(
				elements.contains(*e_id).await,
				"h.elements.contains_key(e_id) - el: {e_id} - f_ids: {f_ids:?}"
			);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Deref;

	use anyhow::Result;
	use ndarray::Array1;
	use rand::rngs::SmallRng;
	use rand::{Rng, SeedableRng};
	use surrealdb_datastore::Transaction;
	use surrealdb_kvs::TransactionType;

	use super::{ReloadCounts, take_reload_counts};
	use crate::IndexKeyBase;
	use crate::catalog::{
		DOC_IDS_FORMAT_VERSION, DatabaseId, Distance, HnswParams, IndexId, NamespaceId, TableId,
		VectorType,
	};
	use crate::config::BuildParallelism;
	use crate::test_env::TestIndexStore;
	use crate::trees::hnsw::index::{HnswCompactionOutcome, HnswIndex};
	use crate::trees::hnsw::{ElementId, HnswState};
	use crate::trees::vector::{SerializedVector, SharedVector, Vector};
	use crate::val::{Number, RecordIdKey, Value};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);
	const TB: TableId = TableId(3);
	const IX: IndexId = IndexId(4);

	fn params() -> HnswParams {
		let m: u8 = 12;
		HnswParams {
			dimension: 2,
			distance: Distance::Euclidean,
			vector_type: VectorType::I16,
			m,
			m0: m * 2,
			ml: (1.0 / (m as f64).ln()).into(),
			ef_construction: 40,
			extend_candidates: true,
			keep_pruned_connections: true,
			use_hashed_vector: true,
		}
	}

	/// The indexed content for record `i`: the vector `(i, i)`, so record order
	/// along the diagonal is also distance order from the origin.
	fn diagonal(i: i16) -> Vec<Value> {
		let vector: SharedVector = Vector::I16(Array1::from_vec(vec![i, i])).into();
		vec![Value::from(vector.deref())]
	}

	/// A process-local wrapper over the index at `ikb`, holding no graph until
	/// its first state check.
	async fn new_instance(ds: &TestIndexStore, ikb: &IndexKeyBase) -> Result<HnswIndex> {
		let env = ds.env(TransactionType::Read).await;
		let h = HnswIndex::new(
			ds.index_stores().vector_cache().clone(),
			&env.tx(),
			ikb.clone(),
			TB,
			&params(),
			DOC_IDS_FORMAT_VERSION,
		)
		.await?;
		env.tx().cancel().await?;
		Ok(h)
	}

	/// Persists a graph of `n` elements, records `1..=n` on the diagonal, with
	/// the pending queue fully drained.
	async fn seed_graph(ds: &TestIndexStore, ikb: &IndexKeyBase, n: i16) -> Result<HnswIndex> {
		let h = new_instance(ds, ikb).await?;
		{
			let env = ds.env(TransactionType::Write).await;
			for i in 1..=n {
				h.index(&env, &RecordIdKey::Number(i.into()), None, Some(diagonal(i))).await?;
			}
			env.tx().commit().await?;
		}
		loop {
			let env = ds.env(TransactionType::Write).await;
			let compacted = h.index_pendings(&env).await?;
			env.tx().commit().await?;
			if compacted == 0 {
				break;
			}
		}
		Ok(h)
	}

	/// The layer versions the given transaction sees, layer 0 first.
	async fn layer_versions(tx: &Transaction, ikb: &IndexKeyBase) -> Result<Vec<u64>> {
		let st: HnswState = tx.get_key(&ikb.new_hs_key(), None).await?.unwrap_or_default();
		let mut versions = Vec::with_capacity(1 + st.layers.len());
		versions.push(st.layer0.version);
		versions.extend(st.layers.iter().map(|l| l.version));
		Ok(versions)
	}

	async fn persisted_layer_versions(ds: &TestIndexStore, ikb: &IndexKeyBase) -> Result<Vec<u64>> {
		let tx = ds.transaction(TransactionType::Read).await?;
		let versions = layer_versions(&tx, ikb).await?;
		tx.cancel().await?;
		Ok(versions)
	}

	/// The levels a write moved, among the levels the persisted state still
	/// names. A layer the write stacked above the persisted top is dropped by
	/// the next state check rather than reloaded, so it is not one of them.
	fn moved_levels(persisted: &[u64], staged: &[u64]) -> Vec<u16> {
		persisted
			.iter()
			.zip(staged)
			.enumerate()
			.filter(|(_, (p, s))| p != s)
			.map(|(level, _)| level as u16)
			.collect()
	}

	/// The number of persisted `Hn` nodes across the given levels, which is
	/// what reloading those levels streams back.
	async fn persisted_nodes(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
		levels: &[u16],
	) -> Result<usize> {
		let tx = ds.transaction(TransactionType::Read).await?;
		let mut nodes = 0;
		for &level in levels {
			nodes += tx.count(ikb.new_hn_layer_range(level)?, None).await?;
		}
		tx.cancel().await?;
		Ok(nodes)
	}

	/// The `k` records nearest the origin, by record number.
	async fn nearest_to_origin(ds: &TestIndexStore, h: &HnswIndex, k: usize) -> Result<Vec<i64>> {
		let env = ds.env(TransactionType::Read).await;
		let pt = vec![Number::Int(0), Number::Int(0)];
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async { h.knn_search(&env, stk, &pt, k, 40, None, None).await })
			.finish()
			.await?;
		env.tx().cancel().await?;
		let mut keys: Vec<i64> = res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		keys.sort();
		Ok(keys)
	}

	/// The `k` records nearest the diagonal point `(i, i)`, by record number.
	async fn nearest_to(ds: &TestIndexStore, h: &HnswIndex, i: i16, k: usize) -> Result<Vec<i64>> {
		let env = ds.env(TransactionType::Read).await;
		let pt = vec![Number::Int(i.into()), Number::Int(i.into())];
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async { h.knn_search(&env, stk, &pt, k, 40, None, None).await })
			.finish()
			.await?;
		env.tx().cancel().await?;
		let mut keys: Vec<i64> = res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		keys.sort();
		Ok(keys)
	}

	/// Whether the process-wide cache holds the vector of `element`.
	async fn cached(ds: &TestIndexStore, element: ElementId) -> bool {
		ds.index_stores()
			.vector_cache()
			.contains(NS, DB, TB, IX, DOC_IDS_FORMAT_VERSION, element)
			.await
	}

	async fn check_state(ds: &TestIndexStore, h: &HnswIndex) -> Result<()> {
		let env = ds.env(TransactionType::Read).await;
		let res = h.check_state(&env).await;
		env.tx().cancel().await?;
		res
	}

	/// A staged batch that does not commit leaves the resident graph as it
	/// stood.
	///
	/// The batch writes its nodes and state into its transaction and hands them
	/// to the resident graph only from a commit action. Rolled back, it leaves
	/// the graph at the committed state — level with the store, so the next
	/// state check has nothing to reload — and the element it allocated out of
	/// the vector cache. The retry takes the same element id, and its
	/// publication leaves the graph level with the store again.
	#[tokio::test]
	async fn hnsw_rolled_back_staged_batch_leaves_the_resident_graph_as_committed() -> Result<()> {
		// Enough records for the graph to stack layers above layer 0.
		const RECORDS: i16 = 300;
		let added = RecordIdKey::Number((RECORDS + 1).into());

		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let _seed = seed_graph(&ds, &ikb, RECORDS).await?;

		// An instance holding the whole graph, level with the store.
		let h = new_instance(&ds, &ikb).await?;
		check_state(&ds, &h).await?;
		let persisted = persisted_layer_versions(&ds, &ikb).await?;
		assert!(
			persisted.len() > 1,
			"fixture must build a layered graph: {RECORDS} records produced a flat one"
		);
		// Every seeded vector is distinct, so the next element takes the next id.
		let element = RECORDS as ElementId;

		// One more record for the compaction to apply.
		{
			let env = ds.env(TransactionType::Write).await;
			h.index(&env, &added, None, Some(diagonal(RECORDS + 1))).await?;
			env.tx().commit().await?;
		}

		// Stage it, then roll the transaction back.
		{
			let env = ds.env(TransactionType::Write).await;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert!(plan.has_work(), "the pending must give the apply something to do");
			assert_eq!(h.apply_compaction(&env, plan).await?, HnswCompactionOutcome::Staged);
			assert_ne!(
				layer_versions(&env.tx(), &ikb).await?,
				persisted,
				"the batch must write its new state into its transaction"
			);
			env.tx().cancel().await?;
		}
		assert!(!cached(&ds, element).await, "a rolled-back batch must leave no vector cached");

		// The resident graph never took the batch, so nothing needs reloading.
		take_reload_counts();
		check_state(&ds, &h).await?;
		assert_eq!(
			take_reload_counts(),
			ReloadCounts::default(),
			"a rolled-back staged batch must leave the resident graph level with the store"
		);
		assert_eq!(nearest_to_origin(&ds, &h, 5).await?, (1..=5).collect::<Vec<i64>>());

		// The retry commits and publishes: level with the store without a reload,
		// and the new element is cached and answers from the graph.
		{
			let env = ds.env(TransactionType::Write).await;
			assert_eq!(h.index_pendings(&env).await?, 1);
			env.tx().commit().await?;
		}
		take_reload_counts();
		check_state(&ds, &h).await?;
		assert_eq!(
			take_reload_counts(),
			ReloadCounts::default(),
			"publication must leave the resident graph level with the store"
		);
		assert!(cached(&ds, element).await, "publication must cache the committed vector");
		assert_eq!(nearest_to(&ds, &h, RECORDS + 1, 1).await?, vec![i64::from(RECORDS + 1)]);
		Ok(())
	}

	/// A staged batch whose transaction applies again and then does not commit
	/// leaves the resident graph invalidated.
	///
	/// The second apply reads the graph back through the transaction, which
	/// sees the state the first batch staged, so it brings that uncommitted
	/// state into the resident graph. When the transaction rolls back, the
	/// first batch's rollback finds the resident graph away from the base it
	/// staged from and invalidates it: the next state check reloads every layer
	/// from the store, and the graph answers as committed.
	#[tokio::test]
	async fn hnsw_second_apply_in_a_rolled_back_transaction_reloads_every_layer() -> Result<()> {
		// Enough records for the graph to stack layers above layer 0.
		const RECORDS: i16 = 300;

		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let _seed = seed_graph(&ds, &ikb, RECORDS).await?;
		let h = new_instance(&ds, &ikb).await?;
		check_state(&ds, &h).await?;
		let persisted = persisted_layer_versions(&ds, &ikb).await?;
		assert!(
			persisted.len() > 1,
			"fixture must build a layered graph: {RECORDS} records produced a flat one"
		);
		{
			let env = ds.env(TransactionType::Write).await;
			let id = RecordIdKey::Number((RECORDS + 1).into());
			h.index(&env, &id, None, Some(diagonal(RECORDS + 1))).await?;
			env.tx().commit().await?;
		}

		// Two staged applies in one transaction, the second over a pending the
		// transaction wrote itself, then a rollback.
		{
			let env = ds.env(TransactionType::Write).await;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert_eq!(h.apply_compaction(&env, plan).await?, HnswCompactionOutcome::Staged);
			let id = RecordIdKey::Number((RECORDS + 2).into());
			h.index(&env, &id, None, Some(diagonal(RECORDS + 2))).await?;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert_eq!(h.apply_compaction(&env, plan).await?, HnswCompactionOutcome::Staged);
			env.tx().cancel().await?;
		}
		for element in [RECORDS as ElementId, RECORDS as ElementId + 1] {
			assert!(!cached(&ds, element).await, "element {element} must not stay cached");
		}

		let every_level: Vec<u16> = (0..persisted.len() as u16).collect();
		take_reload_counts();
		check_state(&ds, &h).await?;
		assert_eq!(
			take_reload_counts(),
			ReloadCounts {
				scans: persisted.len(),
				nodes: persisted_nodes(&ds, &ikb, &every_level).await?,
			},
			"the state check must reload every layer the store holds"
		);
		assert_eq!(persisted_layer_versions(&ds, &ikb).await?, persisted);
		assert_eq!(nearest_to_origin(&ds, &h, 5).await?, (1..=5).collect::<Vec<i64>>());
		Ok(())
	}

	/// An in-place batch that does not commit invalidates the resident graph.
	///
	/// An in-place apply changes the resident graph as it writes, so a
	/// rollback leaves the graph holding a topology the store never took, under
	/// layer versions a later commit can reach as well (see the next test). The
	/// rollback therefore invalidates the index: the next state check reloads
	/// every layer, as a wrapper created in its place would, and the one after
	/// owes nothing.
	#[tokio::test]
	async fn hnsw_rolled_back_in_place_write_reloads_every_layer() -> Result<()> {
		// Enough records for the graph to stack layers above layer 0, so
		// "every layer" is a claim with more than layer 0 in it.
		const RECORDS: i16 = 300;

		let ds = TestIndexStore::with_idx_config(|c| {
			c.hnsw_build_parallelism = BuildParallelism::Serial;
		})
		.await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let _seed = seed_graph(&ds, &ikb, RECORDS).await?;

		// An instance holding the whole graph, level with the store.
		let h = new_instance(&ds, &ikb).await?;
		check_state(&ds, &h).await?;
		let persisted = persisted_layer_versions(&ds, &ikb).await?;
		assert!(
			persisted.len() > 1,
			"fixture must build a layered graph: {RECORDS} records produced a flat one"
		);

		// One more record for the compaction to apply.
		{
			let env = ds.env(TransactionType::Write).await;
			let id = RecordIdKey::Number((RECORDS + 1).into());
			h.index(&env, &id, None, Some(diagonal(RECORDS + 1))).await?;
			env.tx().commit().await?;
		}

		// Apply it in place, then roll the transaction back.
		{
			let env = ds.env(TransactionType::Write).await;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert!(plan.has_work(), "the pending must give the apply something to do");
			assert_eq!(h.apply_compaction(&env, plan).await?, HnswCompactionOutcome::InPlace);
			env.tx().cancel().await?;
		}

		// The rollback owes a pass over every layer, and then nothing.
		let every_level: Vec<u16> = (0..persisted.len() as u16).collect();
		take_reload_counts();
		check_state(&ds, &h).await?;
		assert_eq!(
			take_reload_counts(),
			ReloadCounts {
				scans: persisted.len(),
				nodes: persisted_nodes(&ds, &ikb, &every_level).await?,
			},
			"the state check must reload every layer after a rolled-back in-place write"
		);
		check_state(&ds, &h).await?;
		assert_eq!(take_reload_counts(), ReloadCounts::default());

		// The reloaded graph answers. The rolled-back record sits at the far end
		// of the diagonal, so the nearest records are the seeded ones.
		assert_eq!(nearest_to_origin(&ds, &h, 5).await?, (1..=5).collect::<Vec<i64>>());
		Ok(())
	}

	/// A layer version identifies how far a writer's own copy has moved, not
	/// which writer moved it, so a rolled-back in-place copy is reloaded
	/// through its invalidation rather than through its versions.
	///
	/// `save_nodes` advances a per-layer counter in the applying copy, so two
	/// writers that apply the same batch in place from the same version reach
	/// the same version on the same layers. Once one of them commits, the
	/// other's versions match the store and cannot show that the topology under
	/// them is the winner's and not its own. The loser's rollback invalidated
	/// it, though, so its next state check reloads every layer — exactly what a
	/// wrapper created in its place reads.
	///
	/// A removal makes the two applies exactly comparable: it takes its levels
	/// and its per-level write count from the graph both copies loaded, with no
	/// random level draw in between.
	#[tokio::test]
	async fn hnsw_rolled_back_apply_reloads_although_a_peer_commits_its_version() -> Result<()> {
		// Enough records for the graph to stack layers above layer 0, so a
		// reload that does happen has something to count beyond layer 0.
		const RECORDS: i16 = 300;
		// The record both applies remove.
		const REMOVED: i16 = 7;

		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let seed = seed_graph(&ds, &ikb, RECORDS).await?;

		// Two independent wrappers over the one store, each holding the whole
		// graph and level with it: the writer whose apply will roll back, and
		// the peer whose apply will commit.
		let loser = new_instance(&ds, &ikb).await?;
		let winner = new_instance(&ds, &ikb).await?;
		check_state(&ds, &loser).await?;
		check_state(&ds, &winner).await?;
		let persisted = persisted_layer_versions(&ds, &ikb).await?;
		assert!(
			persisted.len() > 1,
			"fixture must build a layered graph: {RECORDS} records produced a flat one"
		);

		// One staged removal for both of them to apply.
		{
			let env = ds.env(TransactionType::Write).await;
			let id = RecordIdKey::Number(REMOVED.into());
			seed.index(&env, &id, Some(diagonal(REMOVED)), None).await?;
			env.tx().commit().await?;
		}

		// The apply that rolls back. Its transaction sees the state it staged,
		// which is the state its copy now carries.
		let staged = {
			let env = ds.env(TransactionType::Write).await;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert!(plan.has_work(), "the staged pending must give the apply something to do");
			assert!(
				loser.apply_compaction(&env, plan).await?.applied(),
				"the apply must reach the graph"
			);
			let staged = layer_versions(&env.tx(), &ikb).await?;
			env.tx().cancel().await?;
			staged
		};
		let moved = moved_levels(&persisted, &staged);
		assert_eq!(moved.first(), Some(&0), "removing a record always mutates layer 0");

		// The apply that commits, from the same base and over the same pending
		// the rollback left in place.
		{
			let env = ds.env(TransactionType::Write).await;
			let plan = HnswIndex::prepare_compaction(&env, &ikb).await?;
			assert!(plan.has_work(), "the rollback must leave the pending for the peer");
			assert!(
				winner.apply_compaction(&env, plan).await?.applied(),
				"the apply must reach the graph"
			);
			env.tx().commit().await?;
		}

		// The store now stands at the version the rolled-back copy stands at,
		// layer for layer.
		let committed = persisted_layer_versions(&ds, &ikb).await?;
		assert_eq!(
			staged, committed,
			"the peer's commit must land the versions the rollback left in memory"
		);

		// The versions alone would leave the rolled-back copy with nothing to
		// do. Its invalidation makes its state check read every layer back from
		// the store instead, as a copy that carries no versions does.
		let every_layer = ReloadCounts {
			scans: committed.len(),
			nodes: persisted_nodes(&ds, &ikb, &(0..committed.len() as u16).collect::<Vec<_>>())
				.await?,
		};
		take_reload_counts();
		check_state(&ds, &loser).await?;
		assert_eq!(
			take_reload_counts(),
			every_layer,
			"the rolled-back copy must take every layer from the store"
		);
		let replacement = new_instance(&ds, &ikb).await?;
		take_reload_counts();
		check_state(&ds, &replacement).await?;
		assert_eq!(
			take_reload_counts(),
			every_layer,
			"a replacement wrapper must take every layer from the store"
		);
		Ok(())
	}

	/// A search walks through the elements whose documents are all pending
	/// rather than stopping at them.
	///
	/// The search scores pending documents on their own and keeps their
	/// elements out of the graph results, but those elements still carry the
	/// graph's edges. Here every record but the two nearest the query has a
	/// pending delete, and the records lie on a line, so from an entry point
	/// among them the two nearest are reachable only through pending elements:
	/// a walk that stopped at pending elements would return fewer than two.
	/// A third live record at the far end of the line is reachable only across
	/// the pending ones as well, after the two nearest are found: a walk that
	/// let those two set its distance bound before holding `ef` results would
	/// stop short of it.
	#[tokio::test]
	async fn hnsw_search_walks_through_pending_elements() -> Result<()> {
		const FAR: std::ops::RangeInclusive<i16> = 100..=499;

		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let h = new_instance(&ds, &ikb).await?;
		{
			let env = ds.env(TransactionType::Write).await;
			for i in [1, 2].into_iter().chain(FAR) {
				h.index(&env, &RecordIdKey::Number(i.into()), None, Some(diagonal(i))).await?;
			}
			env.tx().commit().await?;
		}
		loop {
			let env = ds.env(TransactionType::Write).await;
			let compacted = h.index_pendings(&env).await?;
			env.tx().commit().await?;
			if compacted == 0 {
				break;
			}
		}
		assert_eq!(nearest_to_origin(&ds, &h, 2).await?, vec![1, 2]);
		// Delete every far record but the last, and leave the deletes pending.
		{
			let env = ds.env(TransactionType::Write).await;
			for i in FAR.filter(|i| *i != *FAR.end()) {
				h.index(&env, &RecordIdKey::Number(i.into()), Some(diagonal(i)), None).await?;
			}
			env.tx().commit().await?;
		}
		assert_eq!(
			nearest_to_origin(&ds, &h, 3).await?,
			vec![1, 2, i64::from(*FAR.end())],
			"the three records left must be found through the pending ones"
		);
		Ok(())
	}

	/// A removed element stays out of the graph, whatever an older reader
	/// enters into the vector cache.
	///
	/// Edges are not symmetric, so a removal leaves other nodes' edges towards
	/// the removed element in the store. A reader whose snapshot predates the
	/// removal walks the graph as it stood and enters the removed element's
	/// vector into the process-wide cache after the removal evicted it. A
	/// compaction that took that entry as proof the element exists would link
	/// it through a leftover edge, writing it back as a node — and in time as
	/// the entry point — with no vector behind it, and every later search and
	/// compaction would fail on it.
	#[tokio::test(flavor = "multi_thread")]
	async fn hnsw_removed_element_stays_out_of_the_graph() -> Result<()> {
		let p = HnswParams {
			dimension: 3,
			vector_type: VectorType::F32,
			ef_construction: 150,
			extend_candidates: false,
			keep_pruned_connections: false,
			use_hashed_vector: false,
			..params()
		};
		let content = |v: [f32; 3]| {
			let vector: SharedVector = Vector::F32(Array1::from_vec(v.to_vec())).into();
			vec![Value::from(vector.deref())]
		};
		let compact = async |ds: &TestIndexStore, h: &HnswIndex| -> Result<()> {
			loop {
				let env = ds.env(TransactionType::Write).await;
				let compacted = h.index_pendings(&env).await?;
				env.tx().commit().await?;
				if compacted == 0 {
					return Ok(());
				}
			}
		};

		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NS, DB, "tb".into(), IX);
		let h = {
			let env = ds.env(TransactionType::Read).await;
			let h = HnswIndex::new(
				ds.index_stores().vector_cache().clone(),
				&env.tx(),
				ikb.clone(),
				TB,
				&p,
				DOC_IDS_FORMAT_VERSION,
			)
			.await?;
			env.tx().cancel().await?;
			h
		};
		let mut rng = SmallRng::seed_from_u64(7);
		let mut next = 0;
		for round in 0..20 {
			// Index fifty records, then remove forty of them.
			let mut added = Vec::new();
			{
				let env = ds.env(TransactionType::Write).await;
				for _ in 0..50 {
					let v = [(); 3].map(|_| rng.random_range(-60.0..60.0));
					h.index(&env, &RecordIdKey::Number(next), None, Some(content(v))).await?;
					added.push((next, v));
					next += 1;
				}
				env.tx().commit().await?;
			}
			compact(&ds, &h).await?;
			let older = ds.env(TransactionType::Read).await;
			{
				let env = ds.env(TransactionType::Write).await;
				for (id, v) in added.iter().take(40) {
					h.index(&env, &RecordIdKey::Number(*id), Some(content(*v)), None).await?;
				}
				env.tx().commit().await?;
			}
			compact(&ds, &h).await?;
			// A reader that began before the removals searches the graph as
			// it stood, and caches the removed elements' vectors on the way.
			let pt: Vec<Number> = [5.6, -1.7, 8.5].map(Number::Float).to_vec();
			let mut stack = reblessive::tree::TreeStack::new();
			stack
				.enter(|stk| async { h.knn_search(&older, stk, &pt, 2, 40, None, None).await })
				.finish()
				.await?;
			older.tx().cancel().await?;
			// Every element the persisted graph holds has a vector.
			let env = ds.env(TransactionType::Read).await;
			let persisted = HnswIndex::new(
				ds.index_stores().vector_cache().clone(),
				&env.tx(),
				ikb.clone(),
				TB,
				&p,
				DOC_IDS_FORMAT_VERSION,
			)
			.await?;
			persisted.check_state(&env).await?;
			let (enter_point, layers) = persisted.adjacency().await;
			let nodes = layers.iter().flat_map(|layer| layer.iter().map(|(node, _)| *node));
			for element in enter_point.into_iter().chain(nodes) {
				let vector: Option<SerializedVector> =
					env.tx().get_key(&ikb.new_he_key(element), None).await?;
				assert!(
					vector.is_some(),
					"round {round}: element {element} is in the persisted graph without a vector"
				);
			}
			env.tx().cancel().await?;
		}
		Ok(())
	}
}
