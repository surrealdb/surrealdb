use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::layer::HnswLayer;
use crate::idx::trees::hnsw::{ElementId, HnswElements};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::HnswParams;

#[derive(Debug)]
pub(super) enum Heuristic {
	Standard,
	Ext,
	Keep,
	ExtAndKeep,
}

impl From<&HnswParams> for Heuristic {
	fn from(p: &HnswParams) -> Self {
		if p.keep_pruned_connections {
			if p.extend_candidates {
				Self::ExtAndKeep
			} else {
				Self::Keep
			}
		} else if p.extend_candidates {
			Self::Ext
		} else {
			Self::Standard
		}
	}
}

impl Heuristic {
	pub(super) fn select<S>(
		&self,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: DoublePriorityQueue,
		res: &mut S,
	) where
		S: DynamicSet<ElementId>,
	{
		match self {
			Self::Standard => Self::heuristic(elements, layer, c, res),
			Self::Ext => Self::heuristic_ext(elements, layer, q_id, q_pt, c, res),
			Self::Keep => Self::heuristic_keep(elements, layer, c, res),
			Self::ExtAndKeep => Self::heuristic_ext_keep(elements, layer, q_id, q_pt, c, res),
		}
	}

	fn heuristic<S>(
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) where
		S: DynamicSet<ElementId>,
	{
		let m_max = layer.m_max();
		if c.len() <= m_max {
			c.to_dynamic_set(res);
		} else {
			while let Some((e_dist, e_id)) = c.pop_first() {
				if Self::is_closer(elements, e_dist, e_id, res) && res.len() == m_max {
					break;
				}
			}
		}
	}

	fn heuristic_keep<S>(
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) where
		S: DynamicSet<ElementId>,
	{
		let m_max = layer.m_max();
		if c.len() <= m_max {
			c.to_dynamic_set(res);
			return;
		}
		let mut pruned = Vec::new();
		while let Some((e_dist, e_id)) = c.pop_first() {
			if Self::is_closer(elements, e_dist, e_id, res) {
				if res.len() == m_max {
					break;
				}
			} else {
				pruned.push(e_id);
			}
		}
		let n = m_max - res.len();
		if n > 0 {
			for e_id in pruned.drain(0..n) {
				res.insert(e_id);
			}
		}
	}

	fn extend_candidates<S>(
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: &mut DoublePriorityQueue,
	) where
		S: DynamicSet<ElementId>,
	{
		let m_max = layer.m_max();
		let mut ex = c.to_set();
		let mut ext = Vec::with_capacity(m_max.min(c.len()));
		for (_, e_id) in c.to_vec().into_iter() {
			if let Some(e_conn) = layer.get_edges(&e_id) {
				for &e_adj in e_conn.iter() {
					if e_adj != q_id && ex.insert(e_adj) {
						if let Some(d) = elements.get_distance(q_pt, &e_adj) {
							ext.push((d, e_adj));
						}
					}
				}
			} else {
				#[cfg(debug_assertions)]
				unreachable!()
			}
		}
		for (e_dist, e_id) in ext {
			c.push(e_dist, e_id);
		}
	}

	fn heuristic_ext<S>(
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) where
		S: DynamicSet<ElementId>,
	{
		Self::extend_candidates(elements, layer, q_id, q_pt, &mut c);
		Self::heuristic(elements, layer, c, res)
	}

	fn heuristic_ext_keep<S>(
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) where
		S: DynamicSet<ElementId>,
	{
		Self::extend_candidates(elements, layer, q_id, q_pt, &mut c);
		Self::heuristic_keep(elements, layer, c, res)
	}

	fn is_closer<S>(elements: &HnswElements, e_dist: f64, e_id: ElementId, r: &mut S) -> bool
	where
		S: DynamicSet<ElementId>,
	{
		if let Some(current_vec) = elements.get_vector(&e_id) {
			for r_id in r.iter() {
				if let Some(r_dist) = elements.get_distance(current_vec, r_id) {
					if e_dist > r_dist {
						return false;
					}
				}
			}
			r.insert(e_id);
			true
		} else {
			false
		}
	}
}
