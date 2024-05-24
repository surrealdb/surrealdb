use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use hashbrown::HashMap;

pub(super) struct HnswElements {
	elements: HashMap<ElementId, SharedVector>,
	next_element_id: ElementId,
	dist: Distance,
}

impl HnswElements {
	pub(super) fn new(dist: Distance) -> Self {
		Self {
			elements: Default::default(),
			next_element_id: 0,
			dist,
		}
	}

	pub(super) fn next_element_id(&self) -> ElementId {
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

	pub(super) fn inc_next_element_id(&mut self) {
		self.next_element_id += 1;
	}

	pub(super) fn insert(&mut self, id: ElementId, pt: SharedVector) {
		self.elements.insert(id, pt);
	}

	pub(super) fn get_vector(&self, e_id: &ElementId) -> Option<&SharedVector> {
		self.elements.get(e_id)
	}

	pub(super) fn distance(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		self.dist.calculate(a, b)
	}

	pub(super) fn get_distance(&self, q: &SharedVector, e_id: &ElementId) -> Option<f64> {
		self.elements.get(e_id).map(|e_pt| self.dist.calculate(e_pt, q))
	}

	pub(super) fn remove(&mut self, e_id: &ElementId) {
		self.elements.remove(e_id);
	}
}
