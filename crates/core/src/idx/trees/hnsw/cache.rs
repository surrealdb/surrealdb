use std::sync::Arc;

use crate::catalog::IndexId;
use crate::cnf;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;
use dashmap::{DashMap, Entry};
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, Weighter};
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

/// Custom weighter for SharedVector that calculates memory usage
/// based on a vector type and dimensions.
/// Called during cache eviction, so must be fast and lightweight.
#[derive(Clone)]
struct VectorWeighter;

type VectorCacheKey = (IndexId, ElementId);
impl Weighter<VectorCacheKey, SharedVector> for VectorWeighter {
	fn weight(&self, key: &(IndexId, ElementId), val: &SharedVector) -> u64 {
		// Calculate total memory: vector (including Arc + hash) + IndexId + ElementId
		(val.mem_size() + std::mem::size_of_val(&key.0) + std::mem::size_of_val(&key.1)) as u64
	}
}

/// Tracks which element IDs are cached for each index.
/// Wrapped in Arc to share ownership between VectorCache and VectorCacheLifecycle.
#[derive(Clone, Default)]
struct ElementsPerIndex(Arc<DashMap<IndexId, RwLock<RoaringTreemap>>>);

impl ElementsPerIndex {
	async fn insert(&self, index_id: IndexId, element_id: ElementId) {
		self.0.entry(index_id).or_default().write().await.insert(element_id);
	}

	#[cfg(test)]
	async fn len(&self, index_id: &IndexId) -> u64 {
		if let Some(elements_ids) = self.0.get(index_id) {
			elements_ids.read().await.len()
		} else {
			0
		}
	}
	async fn remove_index(&self, index_id: &IndexId) -> Option<RwLock<RoaringTreemap>> {
		self.0.remove(index_id).map(|entry| entry.1)
	}
	async fn remove_element(&self, index_id: IndexId, element_id: ElementId) {
		if let Entry::Occupied(mut entry) = self.0.entry(index_id) {
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
	}

	fn remove_element_sync(&self, index_id: IndexId, element_id: ElementId) {
		if let Entry::Occupied(mut entry) = self.0.entry(index_id) {
			let is_empty = {
				// Use blocking_write() because on_evict is called from cache's synchronous context.
				// This is safe as evictions happen during cache operations that don't hold async locks.
				let mut elements_ids = entry.get_mut().blocking_write();
				elements_ids.remove(element_id);
				elements_ids.is_empty()
			};
			// Clean up the index entry if no elements remain
			if is_empty {
				entry.remove_entry();
			}
		}
	}
}

/// Lifecycle hook that removes element IDs from the indexes tracking structure
/// when entries are evicted from the LRU cache
#[derive(Clone)]
struct VectorCacheLifecycle(ElementsPerIndex);

impl Lifecycle<VectorCacheKey, SharedVector> for VectorCacheLifecycle {
	type RequestState = ();

	fn begin_request(&self) -> Self::RequestState {}

	fn on_evict(&self, _state: &mut Self::RequestState, key: VectorCacheKey, _val: SharedVector) {
		// Called synchronously by quick_cache during eviction.
		// We use the sync variant to maintain consistency without async overhead.
		self.0.remove_element_sync(key.0, key.1)
	}
}

#[derive(Clone)]
pub(crate) struct VectorCache(Arc<Inner>);

struct Inner {
	/// For each index/element pair, the vector
	vectors: Cache<
		VectorCacheKey,
		SharedVector,
		VectorWeighter,
		DefaultHashBuilder,
		VectorCacheLifecycle,
	>,
	/// For each index, the set of element ids that have been cached.
	/// This allows efficient bulk removal of all vectors for an index without
	/// iterating through the entire cache.
	indexes: ElementsPerIndex,
}

impl Default for VectorCache {
	fn default() -> Self {
		Self::new(*cnf::HNSW_CACHE_SIZE)
	}
}
impl VectorCache {
	fn new(cache_size: u64) -> Self {
		// Create the shared indexes structure
		let indexes = ElementsPerIndex::default();

		// Create the lifecycle hook with access to the indexes
		let lifecycle = VectorCacheLifecycle(indexes.clone());

		Self(Arc::new(Inner {
			vectors: Cache::with(
				// estimated_items_capacity (rough estimate)
				(cache_size / 256) as usize,
				// weight_capacity in bytes
				cache_size,
				VectorWeighter,
				DefaultHashBuilder::default(),
				lifecycle,
			),
			indexes,
		}))
	}

	pub(super) async fn insert(
		&self,
		index_id: IndexId,
		element_id: ElementId,
		vector: SharedVector,
	) {
		self.0.vectors.insert((index_id, element_id), vector);
		self.0.indexes.insert(index_id, element_id).await;
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
		self.0.indexes.remove_element(index_id, element_id).await;
		// Remove from the vector cache
		self.0.vectors.remove(&(index_id, element_id));
	}

	pub(crate) async fn remove_index(&self, index_id: IndexId) {
		let mut count = 0;
		if let Some(elements_ids) = self.0.indexes.remove_index(&index_id).await {
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
		self.0.indexes.len(index_id).await
	}

	#[cfg(test)]
	pub(super) async fn contains(&self, index_id: IndexId, element_id: ElementId) -> bool {
		self.0.vectors.contains_key(&(index_id, element_id))
	}
}
