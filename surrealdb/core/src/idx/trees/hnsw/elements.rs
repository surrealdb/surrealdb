use anyhow::Result;

use crate::catalog::{Distance, IndexId, TableId};
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::Transaction;

/// Manages storage and retrieval of element vectors in the HNSW graph.
///
/// Vectors are stored in the key-value store and cached in-memory via
/// [`VectorCache`] for fast distance computations during graph traversal.
pub(super) struct HnswElements {
	/// The table this index belongs to.
	table_id: TableId,
	/// The index identifier.
	index_id: IndexId,
	/// Key base for generating element storage keys.
	ikb: IndexKeyBase,
	/// In-memory LRU cache for element vectors.
	vector_cache: VectorCache,
	/// The next element ID to assign.
	next_element_id: ElementId,
	/// Distance metric for similarity computations.
	dist: Distance,
}

impl HnswElements {
	/// Creates a new `HnswElements` instance.
	pub(super) fn new(
		table_id: TableId,
		ikb: IndexKeyBase,
		dist: Distance,
		vector_cache: VectorCache,
	) -> Self {
		Self {
			table_id,
			index_id: ikb.index(),
			ikb,
			vector_cache,
			next_element_id: 0,
			dist,
		}
	}

	/// Sets the next element ID (used when loading state from the key-value store).
	pub(super) fn set_next_element_id(&mut self, next: ElementId) {
		self.next_element_id = next;
	}

	/// Returns the current next element ID without incrementing.
	pub(super) fn next_element_id(&self) -> ElementId {
		self.next_element_id
	}

	/// Increments and returns the next element ID.
	pub(super) fn inc_next_element_id(&mut self) -> ElementId {
		self.next_element_id += 1;
		self.next_element_id
	}

	#[cfg(test)]
	pub(super) async fn len(&self) -> usize {
		self.vector_cache.len(self.table_id, self.index_id).await as usize
	}

	#[cfg(test)]
	pub(super) async fn contains(&self, e_id: ElementId) -> bool {
		self.vector_cache.contains(self.table_id, self.index_id, e_id).await
	}

	/// Stores a vector in the key-value store and caches it. Returns the shared vector.
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
		self.vector_cache.insert(self.table_id, self.index_id, id, pt.clone()).await;
		Ok(pt)
	}

	/// Retrieves a vector by element ID, checking the cache first then the key-value store.
	pub(super) async fn get_vector(
		&self,
		tx: &Transaction,
		e_id: &ElementId,
	) -> Result<Option<SharedVector>> {
		if let Some(v) = self.vector_cache.get(self.table_id, self.index_id, *e_id).await {
			return Ok(Some(v));
		}
		let key = self.ikb.new_he_key(*e_id);
		match tx.get(&key, None).await? {
			None => Ok(None),
			Some(vec) => {
				let vec = Vector::from(vec);
				let vec: SharedVector = vec.into();
				self.vector_cache.insert(self.table_id, self.index_id, *e_id, vec.clone()).await;
				Ok(Some(vec))
			}
		}
	}

	/// Computes the distance between two vectors using the configured distance metric.
	pub(super) fn distance(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		self.dist.calculate(a, b)
	}

	/// Computes the distance between a query vector and an element's stored vector.
	pub(super) async fn get_distance(
		&self,
		tx: &Transaction,
		q: &SharedVector,
		e_id: &ElementId,
	) -> Result<Option<f64>> {
		Ok(self.get_vector(tx, e_id).await?.map(|r| self.dist.calculate(&r, q)))
	}

	/// Removes an element's vector from both the cache and the key-value store.
	pub(super) async fn remove(&mut self, tx: &Transaction, e_id: ElementId) -> Result<()> {
		self.vector_cache.remove(self.table_id, self.index_id, e_id).await;
		let key = self.ikb.new_he_key(e_id);
		tx.del(&key).await?;
		Ok(())
	}
}
