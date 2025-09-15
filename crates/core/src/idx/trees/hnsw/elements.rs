use anyhow::Result;
use dashmap::DashMap;

use crate::catalog::Distance;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::Transaction;

pub(super) struct HnswElements {
	ikb: IndexKeyBase,
	elements: DashMap<ElementId, SharedVector>,
	next_element_id: ElementId,
	dist: Distance,
}

impl HnswElements {
	pub(super) fn new(ikb: IndexKeyBase, dist: Distance) -> Self {
		Self {
			ikb,
			elements: Default::default(),
			next_element_id: 0,
			dist,
		}
	}

	pub(super) fn set_next_element_id(&mut self, next: ElementId) {
		self.next_element_id = next;
	}

	pub(super) fn next_element_id(&self) -> ElementId {
		self.next_element_id
	}

	pub(super) fn inc_next_element_id(&mut self) -> ElementId {
		self.next_element_id += 1;
		self.next_element_id
	}

	#[cfg(test)]
	pub(super) fn len(&self) -> usize {
		self.elements.len()
	}

	#[cfg(test)]
	pub(super) fn contains(&self, e_id: &ElementId) -> bool {
		self.elements.contains_key(e_id)
	}

	pub(super) async fn insert(
		&mut self,
		tx: &Transaction,
		id: ElementId,
		vec: Vector,
		ser_vec: &SerializedVector,
	) -> Result<SharedVector> {
		let key = self.ikb.new_he_key(id);
		tx.set(&key, ser_vec, None).await?;
		let pt: SharedVector = vec.into();
		self.elements.insert(id, pt.clone());
		Ok(pt)
	}

	pub(super) async fn get_vector(
		&self,
		tx: &Transaction,
		e_id: &ElementId,
	) -> Result<Option<SharedVector>> {
		if let Some(r) = self.elements.get(e_id) {
			return Ok(Some(r.value().clone()));
		}
		let key = self.ikb.new_he_key(*e_id);
		match tx.get(&key, None).await? {
			None => Ok(None),
			Some(vec) => {
				let vec = Vector::from(vec);
				let vec: SharedVector = vec.into();
				self.elements.insert(*e_id, vec.clone());
				Ok(Some(vec))
			}
		}
	}

	pub(super) fn distance(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		self.dist.calculate(a, b)
	}

	pub(super) async fn get_distance(
		&self,
		tx: &Transaction,
		q: &SharedVector,
		e_id: &ElementId,
	) -> Result<Option<f64>> {
		Ok(self.get_vector(tx, e_id).await?.map(|r| self.dist.calculate(&r, q)))
	}

	pub(super) async fn remove(&mut self, tx: &Transaction, e_id: ElementId) -> Result<()> {
		self.elements.remove(&e_id);
		let key = self.ikb.new_he_key(e_id);
		tx.del(&key).await?;
		Ok(())
	}
}
