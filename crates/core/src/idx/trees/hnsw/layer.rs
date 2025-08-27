use std::mem;

use ahash::HashSet;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::catalog::DatabaseDefinition;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::graph::UndirectedGraph;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::hnsw::{ElementId, HnswElements};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use crate::kvs::Transaction;

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
	) -> Result<bool> {
		if !self.graph.add_empty_node(node) {
			return Ok(false);
		}
		self.save(tx, st).await?;
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
	) -> Result<DoublePriorityQueue> {
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
	) -> Result<Option<ElementId>> {
		let visited = HashSet::from_iter([ignore_id]);
		let mut candidates = DoublePriorityQueue::default();
		if let Some(dist) = elements.get_distance(tx, pt, &ignore_id).await? {
			candidates.push(dist, ignore_id);
		}
		let w = DoublePriorityQueue::default();
		let q = self.search(tx, elements, pt, candidates, visited, w, ef).await?;
		Ok(q.peek_first().map(|(_, e_id)| e_id))
	}

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search_single_checked(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		ep_pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<DoublePriorityQueue> {
		let visited = HashSet::from_iter([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let mut w = DoublePriorityQueue::default();
		Self::add_if_truthy(db, tx, stk, search, &mut w, ep_pt, ep_dist, ep_id, chk).await?;
		self.search_checked(db, tx, stk, search, candidates, visited, w, chk).await
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
		self.search(tx, elements, pt, candidates, visited, w, ef).await
	}

	pub(super) async fn search_multi_with_ignore(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		pt: &SharedVector,
		ignore_ids: Vec<ElementId>,
		efc: usize,
	) -> Result<DoublePriorityQueue> {
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

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		q: &SharedVector,
		mut candidates: DoublePriorityQueue, // set of candidates
		mut visited: HashSet<ElementId>,     // set of visited elements
		mut w: DoublePriorityQueue,          // dynamic list of found nearest neighbors
		ef: usize,
	) -> Result<DoublePriorityQueue> {
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

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search_checked(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		mut candidates: DoublePriorityQueue,
		mut visited: HashSet<ElementId>,
		mut w: DoublePriorityQueue,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<DoublePriorityQueue> {
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
								db, tx, stk, search, &mut w, &e_pt, e_dist, e_id, chk,
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

	#[expect(clippy::too_many_arguments)]
	pub(super) async fn add_if_truthy(
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		w: &mut DoublePriorityQueue,
		e_pt: &SharedVector,
		e_dist: f64,
		e_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<bool> {
		if let Some(docs) = search.vec_docs().get_docs(tx, e_pt).await? {
			if chk.check_truthy(db, tx, stk, search.docs(), docs).await? {
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
	) -> Result<DoublePriorityQueue> {
		let w;
		let mut neighbors = self.graph.new_edges();
		{
			w = self.search_multi(tx, elements, q_pt, eps, efc).await?;
			eps = w.clone();
			heuristic.select(tx, elements, self, q_id, q_pt, w, None, &mut neighbors).await?;
		};

		let neighbors = self.graph.add_node_and_bidirectional_edges(q_id, neighbors);

		for e_id in neighbors {
			if let Some(e_conn) = self.graph.get_edges(&e_id) {
				if e_conn.len() > self.m_max {
					if let Some(e_pt) = elements.get_vector(tx, &e_id).await? {
						let e_c = self.build_priority_list(tx, elements, e_id, e_conn).await?;
						let mut e_new_conn = self.graph.new_edges();
						heuristic
							.select(tx, elements, self, e_id, &e_pt, e_c, None, &mut e_new_conn)
							.await?;
						#[cfg(debug_assertions)]
						assert!(!e_new_conn.contains(&e_id));
						self.graph.set_node(e_id, e_new_conn);
					}
				}
			} else {
				#[cfg(debug_assertions)]
				unreachable!("Element: {}", e_id);
			}
		}
		self.save(tx, st).await?;
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
		tx: &Transaction,
		st: &mut LayerState,
		elements: &HnswElements,
		heuristic: &Heuristic,
		e_id: ElementId,
		efc: usize,
	) -> Result<bool> {
		if let Some(f_ids) = self.graph.remove_node_and_bidirectional_edges(&e_id) {
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
				}
			}
			self.save(tx, st).await?;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	// Base on FoundationDB max value size (100K)
	// https://apple.github.io/foundationdb/known-limitations.html#large-keys-and-values
	const CHUNK_SIZE: usize = 100_000;
	async fn save(&mut self, tx: &Transaction, st: &mut LayerState) -> Result<()> {
		// Serialise the graph
		let val = self.graph.to_val()?;
		// Split it into chunks
		let chunks = val.chunks(Self::CHUNK_SIZE);
		let old_chunks_len = mem::replace(&mut st.chunks, chunks.len() as u32);
		for (i, chunk) in chunks.enumerate() {
			let key = self.ikb.new_hl_key(self.level, i as u32);
			let chunk = chunk.to_vec();
			tx.set(&key, &chunk, None).await?;
		}
		// Delete larger chunks if they exists
		for i in st.chunks..old_chunks_len {
			let key = self.ikb.new_hl_key(self.level, i);
			tx.del(&key).await?;
		}
		// Increase the version
		st.version += 1;
		Ok(())
	}

	pub(super) async fn load(&mut self, tx: &Transaction, st: &LayerState) -> Result<()> {
		let mut val = Vec::new();
		// Load the chunks
		for i in 0..st.chunks {
			let key = self.ikb.new_hl_key(self.level, i);
			let chunk =
				tx.get(&key, None).await?.ok_or_else(|| Error::unreachable("Missing chunk"))?;
			val.extend(chunk);
		}
		// Rebuild the graph
		self.graph.reload(&val)
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
