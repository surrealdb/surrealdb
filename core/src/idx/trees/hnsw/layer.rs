use crate::ctx::Context;
use crate::err::Error;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::graph::UndirectedGraph;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::hnsw::{ElementId, HnswElements};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use hashbrown::HashSet;
use reblessive::tree::Stk;

#[derive(Debug)]
pub(super) struct HnswLayer<S>
where
	S: DynamicSet<ElementId>,
{
	graph: UndirectedGraph<ElementId, S>,
	m_max: usize,
}

impl<S> HnswLayer<S>
where
	S: DynamicSet<ElementId>,
{
	pub(super) fn new(m_max: usize) -> Self {
		Self {
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

	pub(super) fn add_empty_node(&mut self, node: ElementId) -> bool {
		self.graph.add_empty_node(node)
	}
	pub(super) fn search_single(
		&self,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		ef: usize,
	) -> DoublePriorityQueue {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let w = candidates.clone();
		self.search(elements, pt, candidates, visited, w, ef)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search_single_checked(
		&self,
		ctx: &Context<'_>,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		ep_pt: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<DoublePriorityQueue, Error> {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let mut w = DoublePriorityQueue::default();
		Self::add_if_truthy(ctx, stk, search, &mut w, ep_pt, ep_dist, ep_id, chk).await?;
		self.search_checked(ctx, stk, search, candidates, visited, w, chk).await
	}

	pub(super) fn search_multi(
		&self,
		elements: &HnswElements,
		pt: &SharedVector,
		candidates: DoublePriorityQueue,
		ef: usize,
	) -> DoublePriorityQueue {
		let w = candidates.clone();
		let visited = w.to_set();
		self.search(elements, pt, candidates, visited, w, ef)
	}

	pub(super) fn search_single_ignore_ep(
		&self,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_id: ElementId,
	) -> Option<(f64, ElementId)> {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(0.0, ep_id);
		let w = candidates.clone();
		let q = self.search(elements, pt, candidates, visited, w, 1);
		q.peek_first()
	}

	pub(super) fn search_multi_ignore_ep(
		&self,
		elements: &HnswElements,
		pt: &SharedVector,
		ep_id: ElementId,
		efc: usize,
	) -> DoublePriorityQueue {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(0.0, ep_id);
		let w = DoublePriorityQueue::default();
		self.search(elements, pt, candidates, visited, w, efc)
	}

	pub(super) fn search(
		&self,
		elements: &HnswElements,
		pt: &SharedVector,
		mut candidates: DoublePriorityQueue,
		mut visited: HashSet<ElementId>,
		mut w: DoublePriorityQueue,
		ef: usize,
	) -> DoublePriorityQueue {
		let mut f_dist = if let Some(d) = w.peek_last_dist() {
			d
		} else {
			return w;
		};
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
					if let Some(e_pt) = elements.get_vector(&e_id) {
						let e_dist = elements.distance(e_pt, pt);
						if e_dist < f_dist || w.len() < ef {
							candidates.push(e_dist, e_id);
							w.push(e_dist, e_id);
							if w.len() > ef {
								w.pop_last();
							}
							f_dist = w.peek_last_dist().unwrap(); // w can't be empty
						}
					}
				}
			}
		}
		w
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn search_checked(
		&self,
		ctx: &Context<'_>,
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
					if let Some(e_pt) = elements.get_vector(&e_id) {
						let e_dist = elements.distance(e_pt, pt);
						if e_dist < f_dist || w.len() < ef {
							candidates.push(e_dist, e_id);
							if Self::add_if_truthy(
								ctx, stk, search, &mut w, e_pt, e_dist, e_id, chk,
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
		ctx: &Context<'_>,
		stk: &mut Stk,
		search: &HnswCheckedSearchContext<'_>,
		w: &mut DoublePriorityQueue,
		e_pt: &SharedVector,
		e_dist: f64,
		e_id: ElementId,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<bool, Error> {
		if let Some(docs) = search.vec_docs().get_docs(e_pt) {
			if chk.check_truthy(ctx, stk, search.docs(), docs).await? {
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

	pub(super) fn insert(
		&mut self,
		elements: &HnswElements,
		heuristic: &Heuristic,
		efc: usize,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut eps: DoublePriorityQueue,
	) -> DoublePriorityQueue {
		let w;
		let mut neighbors = self.graph.new_edges();
		{
			w = self.search_multi(elements, q_pt, eps, efc);
			eps = w.clone();
			heuristic.select(elements, self, q_id, q_pt, w, &mut neighbors);
		};

		let neighbors = self.graph.add_node_and_bidirectional_edges(q_id, neighbors);

		for e_id in neighbors {
			if let Some(e_conn) = self.graph.get_edges(&e_id) {
				if e_conn.len() > self.m_max {
					if let Some(e_pt) = elements.get_vector(&e_id) {
						let e_c = self.build_priority_list(elements, e_id, e_conn);
						let mut e_new_conn = self.graph.new_edges();
						heuristic.select(elements, self, e_id, e_pt, e_c, &mut e_new_conn);
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
		eps
	}

	fn build_priority_list(
		&self,
		elements: &HnswElements,
		e_id: ElementId,
		neighbors: &S,
	) -> DoublePriorityQueue {
		let mut w = DoublePriorityQueue::default();
		if let Some(e_pt) = elements.get_vector(&e_id) {
			for n_id in neighbors.iter() {
				if let Some(n_pt) = elements.get_vector(n_id) {
					let dist = elements.distance(e_pt, n_pt);
					w.push(dist, *n_id);
				}
			}
		}
		w
	}

	pub(super) fn remove(
		&mut self,
		elements: &HnswElements,
		heuristic: &Heuristic,
		e_id: ElementId,
		efc: usize,
	) -> bool {
		if let Some(f_ids) = self.graph.remove_node_and_bidirectional_edges(&e_id) {
			for &q_id in f_ids.iter() {
				if let Some(q_pt) = elements.get_vector(&q_id) {
					let c = self.search_multi_ignore_ep(elements, q_pt, q_id, efc);
					let mut neighbors = self.graph.new_edges();
					heuristic.select(elements, self, q_id, q_pt, c, &mut neighbors);
					#[cfg(debug_assertions)]
					{
						assert!(
							!neighbors.contains(&q_id),
							"!neighbors.contains(&q_id) - q_id: {q_id} - f_ids: {neighbors:?}"
						);
						assert!(
							!neighbors.contains(&e_id),
							"!neighbors.contains(&e_id) - e_id: {e_id} - f_ids: {neighbors:?}"
						);
						assert!(neighbors.len() < self.m_max);
					}
					self.graph.set_node(q_id, neighbors);
				}
			}
			true
		} else {
			false
		}
	}
}

#[cfg(test)]
impl<S> HnswLayer<S>
where
	S: DynamicSet<ElementId>,
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
