use std::sync::Arc;

use dashmap::{DashMap, Entry};
use quick_cache::Weighter;
use quick_cache::sync::Cache;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::catalog::IndexId;
use crate::cnf;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;

/// Custom weighter for SharedVector that calculates memory usage
/// based on a vector type and dimensions
#[derive(Clone)]
struct VectorWeighter;

type VectorCacheKey = (IndexId, ElementId);
impl Weighter<VectorCacheKey, SharedVector> for VectorWeighter {
	fn weight(&self, key: &(IndexId, ElementId), val: &SharedVector) -> u64 {
		// Calculate total memory: vector (including Arc + hash) + IndexId + ElementId
		(val.mem_size() + size_of_val(&key.0) + size_of_val(&key.1)) as u64
	}
}

#[derive(Clone)]
pub(crate) struct VectorCache(Arc<Inner>);

struct Inner {
	/// For each index/element pair, the vector
	vectors: Cache<VectorCacheKey, SharedVector, VectorWeighter>,
	/// For each index, the set of element ids that have been cached.
	/// This allows efficient bulk removal of all vectors for an index without
	/// iterating through the entire cache.
	indexes: DashMap<IndexId, RwLock<RoaringTreemap>>,
}

impl Default for VectorCache {
	fn default() -> Self {
		Self::new(*cnf::HNSW_CACHE_SIZE)
	}
}
impl VectorCache {
	fn new(cache_size: u64) -> Self {
		Self(Arc::new(Inner {
			vectors: Cache::with_weighter(
				// estimated_items_capacity (rough estimate)
				(cache_size / 256) as usize,
				// weight_capacity in bytes
				cache_size,
				VectorWeighter,
			),
			indexes: DashMap::new(),
		}))
	}

	pub(super) async fn insert(
		&self,
		index_id: IndexId,
		element_id: ElementId,
		vector: SharedVector,
	) {
		self.0.vectors.insert((index_id, element_id), vector);
		self.0.indexes.entry(index_id).or_default().write().await.insert(element_id);
	}

	pub(super) async fn get(
		&self,
		index_id: IndexId,
		element_id: ElementId,
	) -> Option<SharedVector> {
		let key = (index_id, element_id);
		self.0.vectors.get(&key)
	}

	pub(super) async fn remove(&self, index_id: IndexId, element_id: ElementId) {
		// Remove from the indexes tracking structure first
		if let Entry::Occupied(mut entry) = self.0.indexes.entry(index_id) {
			let is_empty = {
				let mut elements_ids = entry.get_mut().write().await;
				elements_ids.remove(element_id);
				elements_ids.is_empty()
			};
			// Clean up the index entry if no elements remain
			if is_empty {
				entry.remove_entry();
			}
		}
		// Remove from the vectors cache
		self.0.vectors.remove(&(index_id, element_id));
	}

	pub(crate) async fn remove_index(&self, index_id: IndexId) {
		let mut count = 0;
		if let Some((_key, elements_ids)) = self.0.indexes.remove(&index_id) {
			for element_id in elements_ids.read().await.iter() {
				self.0.vectors.remove(&(index_id, element_id));
				// Yield control every 1000 removals to prevent blocking other async tasks
				// during bulk operations
				if count % 1000 == 0 {
					yield_now!()
				}
				count += 1;
			}
		}
	}
	#[cfg(test)]
	pub(super) async fn len(&self, index_id: &IndexId) -> u64 {
		if let Some(elements_ids) = self.0.indexes.get(index_id) {
			elements_ids.read().await.len()
		} else {
			0
		}
	}

	#[cfg(test)]
	pub(super) async fn contains(&self, index_id: IndexId, element_id: ElementId) -> bool {
		self.0.vectors.contains_key(&(index_id, element_id))
	}
}
