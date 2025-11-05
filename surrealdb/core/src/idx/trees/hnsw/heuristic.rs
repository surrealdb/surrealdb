use anyhow::Result;

use crate::catalog::HnswParams;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::layer::HnswLayer;
use crate::idx::trees::hnsw::{ElementId, HnswElements};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use crate::kvs::Transaction;

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
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn select<S>(
		&self,
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: DoublePriorityQueue,
		ignore: Option<ElementId>,
		res: &mut S,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		match self {
			Self::Standard => Self::heuristic(tx, elements, layer, c, res).await,
			Self::Ext => Self::heuristic_ext(tx, elements, layer, q_id, q_pt, c, ignore, res).await,
			Self::Keep => Self::heuristic_keep(tx, elements, layer, c, res).await,
			Self::ExtAndKeep => {
				Self::heuristic_ext_keep(tx, elements, layer, q_id, q_pt, c, ignore, res).await
			}
		}
	}

	async fn heuristic<S>(
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		let m_max = layer.m_max();
		if c.len() <= m_max {
			c.to_dynamic_set(res);
		} else {
			while let Some((e_dist, e_id)) = c.pop_first() {
				if Self::is_closer(tx, elements, e_dist, e_id, res).await? && res.len() == m_max {
					break;
				}
			}
		}
		Ok(())
	}

	async fn heuristic_keep<S>(
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		mut c: DoublePriorityQueue,
		res: &mut S,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		let m_max = layer.m_max();
		if c.len() <= m_max {
			c.to_dynamic_set(res);
			return Ok(());
		}
		let mut pruned = Vec::new();
		while let Some((e_dist, e_id)) = c.pop_first() {
			if Self::is_closer(tx, elements, e_dist, e_id, res).await? {
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
		Ok(())
	}

	async fn extend_candidates<S>(
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: &mut DoublePriorityQueue,
		ignore: Option<ElementId>,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		let m_max = layer.m_max();
		let mut ex = c.to_set();
		if let Some(i) = ignore {
			ex.insert(i);
		}
		let mut ext = Vec::with_capacity(m_max.min(c.len()));
		for (_, e_id) in c.to_vec().into_iter() {
			if let Some(e_conn) = layer.get_edges(&e_id) {
				for &e_adj in e_conn.iter() {
					if e_adj != q_id && ex.insert(e_adj) {
						if let Some(d) = elements.get_distance(tx, q_pt, &e_adj).await? {
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
		Ok(())
	}

	#[expect(clippy::too_many_arguments)]
	async fn heuristic_ext<S>(
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		ignore: Option<ElementId>,
		res: &mut S,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		Self::extend_candidates(tx, elements, layer, q_id, q_pt, &mut c, ignore).await?;
		Self::heuristic(tx, elements, layer, c, res).await
	}

	#[expect(clippy::too_many_arguments)]
	async fn heuristic_ext_keep<S>(
		tx: &Transaction,
		elements: &HnswElements,
		layer: &HnswLayer<S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		ignore: Option<ElementId>,
		res: &mut S,
	) -> Result<()>
	where
		S: DynamicSet,
	{
		Self::extend_candidates(tx, elements, layer, q_id, q_pt, &mut c, ignore).await?;
		Self::heuristic_keep(tx, elements, layer, c, res).await
	}

	async fn is_closer<S>(
		tx: &Transaction,
		elements: &HnswElements,
		e_dist: f64,
		e_id: ElementId,
		r: &mut S,
	) -> Result<bool>
	where
		S: DynamicSet,
	{
		if let Some(current_vec) = elements.get_vector(tx, &e_id).await? {
			for r_id in r.iter() {
				if let Some(r_dist) = elements.get_distance(tx, &current_vec, r_id).await? {
					if e_dist > r_dist {
						return Ok(false);
					}
				}
			}
			r.insert(e_id);
			Ok(true)
		} else {
			Ok(false)
		}
	}
}
