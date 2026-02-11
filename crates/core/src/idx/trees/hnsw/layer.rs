use crate::ctx::Context;
use crate::err::Error;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::planner::ScanDirection;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::graph::UndirectedGraph;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::hnsw::{ElementId, HnswElements};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use crate::idx::IndexKeyBase;
use crate::key::index::hn::HnswNode;
use crate::kvs::Transaction;
use ahash::HashSet;
use futures::StreamExt;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::mem;

#[revisioned(revision = 1)]
#[derive(Default, Debug, Serialize, Deserialize)]
pub(super) struct LayerState {
	pub(super) version: u64,
	pub(super) chunks: u32,
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

	pub(super) fn get_edges(&self, e_id: &ElementId) -> Option<&S> {
		self.graph.get_edges(e_id)
	}

	pub(super) async fn add_empty_node(
		&mut self,
		tx: &Transaction,
		node: ElementId,
		st: &mut LayerState,
	) -> Result<bool, Error> {
		if !self.graph.add_empty_node(node) {
			return Ok(false);
		}
		self.save_nodes(tx, st, &[node]).await?;
		Ok(true)
	}
	pub(super) async fn search_single(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		ef: usize,
	) -> Result<DoublePriorityQueue, Error> {
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let w = candidates.clone();
		self.search(tx, elements, pt, candidates, visited, w, ef).await
	}

	pub(super) async fn search_single_with_ignore(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		ignore_id: ElementId,
		ef: usize,
	) -> Result<Option<ElementId>, Error> {
		let visited = HashSet::from_iter([ignore_id]);
		let mut candidates = DoublePriorityQueue::default();
		if let Some(dist) = elements.get_distance(tx, pt, &ignore_id).await? {
			candidates.push(dist, ignore_id);
		}
		let w = DoublePriorityQueue::default();
		let q = self.search(tx, elements, pt, candidates, visited, w, ef).await?;
		Ok(q.peek_first().map(|(_, e_id)| e_id))
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search_single_checked(
		&self,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		ep_pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<DoublePriorityQueue, Error> {
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let mut w = DoublePriorityQueue::default();
		Self::add_if_truthy(tx, stk, search, &mut w, ep_pt, ep_dist, ep_id, chk).await?;
		self.search_checked(tx, stk, search, candidates, visited, w, chk).await
	}

	pub(super) async fn search_multi(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		candidates: DoublePriorityQueue,
		ef: usize,
	) -> Result<DoublePriorityQueue, Error> {
		let w = candidates.clone();
		let visited = w.to_set();
		self.search(tx, elements, pt, candidates, visited, w, ef).await
	}

	pub(super) async fn search_multi_with_ignore(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		ignore_ids: Vec<ElementId>,
		efc: usize,
	) -> Result<DoublePriorityQueue, Error> {
		let mut candidates = DoublePriorityQueue::default();
		for id in &ignore_ids {
			if let Some(dist) = elements.get_distance(tx, pt, id).await? {
				candidates.push(dist, *id);
			}
		}
		let visited = HashSet::from_iter(ignore_ids);
		let w = DoublePriorityQueue::default();
		self.search(tx, elements, pt, candidates, visited, w, efc).await
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		q: &SharedVector,
		mut candidates: DoublePriorityQueue, // set of candidates
		mut visited: HashSet<ElementId>,     // set of visited elements
		mut w: DoublePriorityQueue,          // dynamic list of found nearest neighbors
		ef: usize,
	) -> Result<DoublePriorityQueue, Error> {
		let mut fq_dist = w.peek_last_dist().unwrap_or(f64::MAX);
		while let Some((cq_dist, doc)) = candidates.pop_first() {
			if cq_dist > fq_dist {
				break;
			}
			if let Some(neighbourhood) = self.graph.get_edges(&doc) {
				for &e_id in neighbourhood.iter() {
					// Did we already visit it?
					if !visited.insert(e_id) {
						continue;
					}
					if let Some(e_pt) = elements.get_vector(tx, &e_id).await? {
						let e_dist = elements.distance(&e_pt, q);
						if e_dist < fq_dist || w.len() < ef {
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

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search_checked(
		&self,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		mut candidates: DoublePriorityQueue,
		mut visited: HashSet<ElementId>,
		mut w: DoublePriorityQueue,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<DoublePriorityQueue, Error> {
		let mut f_dist = w.peek_last_dist().unwrap_or(f64::MAX);

		let ef = search.ef();
		let pt = search.pt();
		let elements = search.elements();

		while let Some((dist, doc)) = candidates.pop_first() {
			if dist > f_dist {
				break;
			}
			if let Some(neighbourhood) = self.graph.get_edges(&doc) {
				for &e_id in neighbourhood.iter() {
					// Did we already visit it?
					if !visited.insert(e_id) {
						continue;
					}
					if let Some(e_pt) = elements.get_vector(tx, &e_id).await? {
						let e_dist = elements.distance(&e_pt, pt);
						if e_dist < f_dist || w.len() < ef {
							candidates.push(e_dist, e_id);
							if Self::add_if_truthy(
								tx, stk, search, &mut w, &e_pt, e_dist, e_id, chk,
							)
							.await?
							{
								f_dist = w.peek_last_dist().unwrap(); // w can't be empty
							}
						}
					}
				}
			}
		}
		Ok(w)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn add_if_truthy(
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		w: &mut DoublePriorityQueue,
		e_pt: &SharedVector,
		e_dist: f64,
		e_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<bool, Error> {
		if let Some(docs) = search.vec_docs().get_docs(tx, e_pt).await? {
			if chk.check_truthy(tx, stk, search.docs(), docs).await? {
				w.push(e_dist, e_id);
				if w.len() > search.ef() {
					if let Some((_, id)) = w.pop_last() {
						chk.expire(id);
					}
				}
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(super) async fn insert(
		&mut self,
		(tx, st): (&Transaction, &mut LayerState),
		elements: &HnswElements,
		heuristic: &Heuristic,
		efc: usize,
		(q_id, q_pt): (ElementId, &SharedVector),
		mut eps: DoublePriorityQueue,
	) -> Result<DoublePriorityQueue, Error> {
		let w;
		let mut neighbors = self.graph.new_edges();
		{
			w = self.search_multi(tx, elements, q_pt, eps, efc).await?;
			eps = w.clone();
			heuristic.select(tx, elements, self, q_id, q_pt, w, None, &mut neighbors).await?;
		};

		let neighbors = self.graph.add_node_and_bidirectional_edges(q_id, neighbors);

		for e_id in &neighbors {
			if let Some(e_conn) = self.graph.get_edges(e_id) {
				if e_conn.len() > self.m_max {
					if let Some(e_pt) = elements.get_vector(tx, e_id).await? {
						let e_c = self.build_priority_list(tx, elements, *e_id, e_conn).await?;
						let mut e_new_conn = self.graph.new_edges();
						heuristic
							.select(tx, elements, self, *e_id, &e_pt, e_c, None, &mut e_new_conn)
							.await?;
						#[cfg(debug_assertions)]
						assert!(!e_new_conn.contains(e_id));
						self.graph.set_node(*e_id, e_new_conn);
					}
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
		self.save_nodes(tx, st, &changed_nodes).await?;
		Ok(eps)
	}

	async fn build_priority_list(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		e_id: ElementId,
		neighbors: &S,
	) -> Result<DoublePriorityQueue, Error> {
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
		tx: &Transaction,
		st: &mut LayerState,
		elements: &HnswElements,
		heuristic: &Heuristic,
		e_id: ElementId,
		efc: usize,
	) -> Result<bool, Error> {
		if let Some(f_ids) = self.graph.remove_node_and_bidirectional_edges(&e_id) {
			let mut changed_nodes = Vec::with_capacity(f_ids.len());
			for &q_id in f_ids.iter() {
				if let Some(q_pt) = elements.get_vector(tx, &q_id).await? {
					let c = self
						.search_multi_with_ignore(tx, elements, &q_pt, vec![q_id, e_id], efc)
						.await?;
					let mut q_new_conn = self.graph.new_edges();
					heuristic
						.select(tx, elements, self, q_id, &q_pt, c, Some(e_id), &mut q_new_conn)
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
			self.delete_node(tx, e_id).await?;
			self.save_nodes(tx, st, &changed_nodes).await?;
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
	) -> Result<(), Error> {
		for &node_id in nodes {
			if let Some(val) = self.graph.node_to_val(&node_id) {
				let key = self.ikb.new_hn_key(self.level, node_id)?;
				tx.set(key, val, None).await?;
			}
		}
		// Increase the version
		st.version += 1;
		Ok(())
	}

	/// Deletes a single node's `Hn` key from the KV store.
	async fn delete_node(&self, tx: &Transaction, node_id: ElementId) -> Result<(), Error> {
		let key = self.ikb.new_hn_key(self.level, node_id)?;
		tx.del(key).await?;
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
		ctx: &Context,
		tx: &Transaction,
		st: &mut LayerState,
	) -> Result<bool, Error> {
		self.graph.clear();

		// Load legacy Hl chunks (if any) as the baseline graph.
		if st.chunks > 0 {
			let mut val = Vec::new();
			for i in 0..st.chunks {
				let key = self.ikb.new_hl_key(self.level, i)?;
				let chunk = tx.get(key, None).await?.ok_or_else(|| fail!("Missing chunk"))?;
				val.extend(chunk);
			}
			self.graph.lecacy_reload(&val)?;
		}

		// These represent the most recent state
		// for each node and take precedence over the Hl data loaded above.
		let range = self.ikb.new_hn_layer_range(self.level)?;
		let mut count = 0;
		let mut stream = tx.stream(range, None, None, ScanDirection::Forward);
		while let Some(res) = stream.next().await {
			let (k, v) = res?;
			// Check if the context is finished
			if ctx.is_done(count % 100 == 0)? {
				return Ok(false);
			}
			let key = HnswNode::decode_key(&k)?;
			self.graph.load_node(key.node, &v);
			count += 1;
		}

		// If we can write, complete the migration:
		// persist every node as an Hn key and remove the old Hl chunk keys.
		if st.chunks > 0 && tx.writeable() {
			// Write every node as an Hn key. Nodes that already had Hn entries
			// are rewritten with the same data (their state was overlaid onto
			// the graph in the streaming step above).
			for &node_id in &self.graph.node_ids() {
				if let Some(node_val) = self.graph.node_to_val(&node_id) {
					let key = self.ikb.new_hn_key(self.level, node_id)?;
					tx.set(key, node_val, None).await?;
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
	pub(in crate::idx::trees::hnsw) fn check_props(&self, elements: &HnswElements) {
		assert!(self.graph.len() <= elements.len(), "{} - {}", self.graph.len(), elements.len());
		for (e_id, f_ids) in self.graph.nodes() {
			assert!(
				f_ids.len() <= self.m_max,
				"Foreign list e_id: {e_id} - len = len({}) <= m_layer({})",
				self.m_max,
				f_ids.len(),
			);
			assert!(!f_ids.contains(e_id), "!f_ids.contains(e_id) - el: {e_id} - f_ids: {f_ids:?}");
			assert!(
				elements.contains(e_id),
				"h.elements.contains_key(e_id) - el: {e_id} - f_ids: {f_ids:?}"
			);
		}
	}
}
