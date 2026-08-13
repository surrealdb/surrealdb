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
use crate::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::trees::hnsw::heuristic::Heuristic;
use crate::trees::hnsw::index::HnswContext;
use crate::trees::hnsw::{ElementId, HnswElements, HnswSearch, VectorId};
use crate::trees::knn::{DoublePriorityQueue, Ids64};
use crate::trees::vector::SharedVector;

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
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let w = if pending_docs.is_some() {
			let mut w = DoublePriorityQueue::default();
			if let Some(ep_pt) = elements.get_vector(&ctx.tx, &ep_id).await?
				&& !Self::are_all_docs_in_pending(ctx, ep_id, &ep_pt, pending_docs).await?
			{
				w.push(ep_dist, ep_id);
			}
			w
		} else {
			candidates.clone()
		};
		self.search(ctx, elements, pt, candidates, visited, w, ef, pending_docs).await
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
		let q = self.search(ctx, elements, pt, candidates, visited, w, ef, None).await?;
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
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		pt: &SharedVector,
		candidates: DoublePriorityQueue,
		ef: usize,
	) -> Result<DoublePriorityQueue> {
		let w = candidates.clone();
		let visited = w.to_set();
		self.search(ctx, elements, pt, candidates, visited, w, ef, None).await
	}

	pub(super) async fn search_multi_with_ignore(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		pt: &SharedVector,
		ignore_ids: Vec<ElementId>,
		efc: usize,
	) -> Result<DoublePriorityQueue> {
		let mut candidates = DoublePriorityQueue::default();
		for id in &ignore_ids {
			if let Some(dist) = elements.get_distance(&ctx.tx, pt, id).await? {
				candidates.push(dist, *id);
			}
		}
		let visited = HashSet::from_iter(ignore_ids);
		let w = DoublePriorityQueue::default();
		self.search(ctx, elements, pt, candidates, visited, w, efc, None).await
	}

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search(
		&self,
		ctx: &HnswContext<'_>,
		elements: &HnswElements,
		q: &SharedVector,
		mut candidates: DoublePriorityQueue, // set of candidates
		mut visited: HashSet<ElementId>,     // set of visited elements
		mut w: DoublePriorityQueue,
		ef: usize,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<DoublePriorityQueue> {
		let mut fq_dist = w.peek_last_dist().unwrap_or(f64::MAX);
		while let Some((cq_dist, doc)) = candidates.pop_first() {
			if cq_dist > fq_dist {
				break;
			}
			if let Some(neighbourhood) = self.graph.get_edges(doc) {
				for &e_id in neighbourhood.iter() {
					// Did we already visit it?
					if !visited.insert(e_id) {
						continue;
					}
					if let Some(e_pt) = elements.get_vector(&ctx.tx, &e_id).await? {
						let e_dist = elements.distance(&e_pt, q);
						if e_dist < fq_dist || w.len() < ef {
							if Self::are_all_docs_in_pending(ctx, e_id, &e_pt, pending_docs).await?
							{
								continue;
							}
							candidates.push(e_dist, e_id);
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
					// Did we already visit it?
					if !visited.insert(e_id) {
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
			if visited.contains(&e_id) {
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
		search_ctx: &HnswContext<'_>,
		e_id: ElementId,
		e_pt: &SharedVector,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<bool> {
		let Some(pending_docs) = pending_docs else {
			return Ok(false);
		};
		if pending_docs.is_empty() {
			return Ok(false);
		}
		if let Some(docs) =
			search_ctx.vec_docs.get_docs_by_element(&search_ctx.tx, e_id, e_pt).await?
		{
			for doc_id in docs.iter() {
				if !pending_docs.contains(doc_id) {
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
			w = self.search_multi(ctx, elements, q_pt, eps, efc).await?;
			eps = w.clone();
			heuristic.select(&ctx.tx, elements, self, q_id, q_pt, w, None, &mut neighbors).await?;
		};

		let neighbors = self.graph.add_node_and_bidirectional_edges(q_id, neighbors);

		for e_id in &neighbors {
			if let Some(e_conn) = self.graph.get_edges(*e_id) {
				if e_conn.len() > self.m_max
					&& let Some(e_pt) = elements.get_vector(&ctx.tx, e_id).await?
				{
					let e_c = self.build_priority_list(&ctx.tx, elements, *e_id, e_conn).await?;
					let mut e_new_conn = self.graph.new_edges();
					heuristic
						.select(&ctx.tx, elements, self, *e_id, &e_pt, e_c, None, &mut e_new_conn)
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
		// Save the new node and all its neighbors (which had bidirectional edges added/pruned)
		let mut changed_nodes = Vec::with_capacity(neighbors.len() + 1);
		changed_nodes.push(q_id);
		changed_nodes.extend_from_slice(&neighbors);
		self.save_nodes(&ctx.tx, st, &changed_nodes).await?;
		Ok(eps)
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
			for &q_id in f_ids.iter() {
				if let Some(q_pt) = elements.get_vector(&ctx.tx, &q_id).await? {
					let c = self
						.search_multi_with_ignore(ctx, elements, &q_pt, vec![q_id, e_id], efc)
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
					self.graph.set_node(q_id, q_new_conn);
					changed_nodes.push(q_id);
				}
			}
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
