use crate::catalog::IndexId;
use crate::cnf;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;
use quick_cache::Weighter;
use quick_cache::sync::Cache;
use std::sync::Arc;

/// Custom weighter for SharedVector that calculates memory usage
/// based on a vector type and dimensions
#[derive(Clone)]
struct VectorWeighter;

type VectorCacheKey = (IndexId, ElementId);
impl Weighter<VectorCacheKey, SharedVector> for VectorWeighter {
	fn weight(&self, key: &(IndexId, ElementId), val: &SharedVector) -> u64 {
		(val.mem_size() + size_of_val(&key.0) + size_of_val(&key.1)) as u64
	}
}

#[derive(Clone)]
pub(crate) struct VectorCache(Arc<Cache<VectorCacheKey, SharedVector, VectorWeighter>>);

impl Default for VectorCache {
	fn default() -> Self {
		Self::new(*cnf::HNSW_CACHE_SIZE)
	}
}
impl VectorCache {
	fn new(cache_size: u64) -> Self {
		let cache = Cache::with_weighter(
			cache_size as usize, // estimated_items_capacity (rough estimate)
			cache_size,          // weight_capacity in bytes
			VectorWeighter,
		);
		Self(Arc::new(cache))
	}

	pub(super) fn insert(&self, index_id: IndexId, element_id: ElementId, vector: SharedVector) {
		self.0.insert((index_id, element_id), vector);
	}

	pub(super) fn get(&self, index_id: IndexId, element_id: ElementId) -> Option<SharedVector> {
		self.0.get(&(index_id, element_id))
	}

	pub(super) fn remove(&self, index_id: IndexId, element_id: ElementId) {
		self.0.remove(&(index_id, element_id));
	}

	#[cfg(test)]
	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	#[cfg(test)]
	pub(super) fn contains(&self, index_id: IndexId, element_id: ElementId) -> bool {
		self.0.contains_key(&(index_id, element_id))
	}
}
