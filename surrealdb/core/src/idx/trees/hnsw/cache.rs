use std::sync::Arc;

use dashmap::{DashMap, Entry};
use parking_lot::RwLock;
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, Weighter};
use roaring::RoaringTreemap;

use crate::catalog::{IndexId, TableId};
use crate::cnf;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;

/// Custom weighter for SharedVector that calculates memory usage
/// based on a vector type and dimensions.
/// Called during cache eviction, so must be fast and lightweight.
#[derive(Clone)]
struct VectorWeighter;

/// Cache key uniquely identifying a vector: (table, index, element).
type VectorCacheKey = (TableId, IndexId, ElementId);
impl Weighter<VectorCacheKey, SharedVector> for VectorWeighter {
	fn weight(&self, key: &VectorCacheKey, val: &SharedVector) -> u64 {
		// Calculate total memory: vector (including Arc + hash) + TableId + IndexId
		(val.mem_size() + std::mem::size_of_val(&key.0) + std::mem::size_of_val(&key.1)) as u64
	}
}

type ElementsPerIndexKey = (TableId, IndexId);

/// Tracks which element IDs are cached for each index.
/// Wrapped in Arc to share ownership between VectorCache and VectorCacheLifecycle.
#[derive(Clone, Default)]
struct ElementsPerIndex(Arc<DashMap<ElementsPerIndexKey, RwLock<RoaringTreemap>>>);

impl ElementsPerIndex {
	fn insert(&self, table_id: TableId, index_id: IndexId, element_id: ElementId) {
		self.0.entry((table_id, index_id)).or_default().write().insert(element_id);
	}

	#[cfg(test)]
	fn len(&self, table_id: TableId, index_id: IndexId) -> u64 {
		if let Some(elements_ids) = self.0.get(&(table_id, index_id)) {
			elements_ids.read().len()
		} else {
			0
		}
	}
	fn remove_index(&self, table_id: TableId, index_id: IndexId) -> Option<RwLock<RoaringTreemap>> {
		self.0.remove(&(table_id, index_id)).map(|entry| entry.1)
	}
	fn remove_element(&self, table_id: TableId, index_id: IndexId, element_id: ElementId) {
		if let Entry::Occupied(mut entry) = self.0.entry((table_id, index_id)) {
			let is_empty = {
				let mut elements_ids = entry.get_mut().write();
				elements_ids.remove(element_id);
				elements_ids.is_empty()
			};
			// Clean up the index entry if no elements remain to prevent memory leaks
			if is_empty {
				entry.remove_entry();
			}
		}
	}

	fn evict_element(&self, key: VectorCacheKey) {
		if let Entry::Occupied(mut entry) = self.0.entry((key.0, key.1)) {
			entry.get_mut().write().remove(key.2);
			// Note: We intentionally don't clean up empty index entries here to avoid potential
			// race conditions. Empty entries are cleaned up during remove_element() calls.
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
		self.0.evict_element(key)
	}
}

/// Thread-safe, weighted LRU cache for HNSW element vectors.
///
/// Shared across all HNSW indexes via `Arc`. Tracks which elements are
/// cached per index for efficient bulk eviction when an index is dropped.
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
	/// Creates a new vector cache with the given weight capacity (in bytes).
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

	/// Inserts a vector into the cache, tracking it in the per-index element set.
	pub(super) async fn insert(
		&self,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
		vector: SharedVector,
	) {
		// Update indexes tracking first, before inserting into cache.
		// This prevents a race condition where eviction could occur immediately after
		// cache insertion but before index tracking is updated, leaving an inconsistent state.
		self.0.indexes.insert(table_id, index_id, element_id);
		self.0.vectors.insert((table_id, index_id, element_id), vector);
	}

	/// Retrieves a cached vector, if present.
	pub(super) async fn get(
		&self,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) -> Option<SharedVector> {
		let key = (table_id, index_id, element_id);
		self.0.vectors.get(&key)
	}

	/// Removes a single vector from the cache.
	pub(super) async fn remove(&self, table_id: TableId, index_id: IndexId, element_id: ElementId) {
		// Remove from the indexes tracking structure first
		self.0.indexes.remove_element(table_id, index_id, element_id);
		// Remove from the vector cache
		self.0.vectors.remove(&(table_id, index_id, element_id));
	}

	/// Removes all cached vectors for a given index, yielding periodically during bulk removal.
	pub(crate) async fn remove_index(&self, table_id: TableId, index_id: IndexId) {
		let mut count = 0;
		if let Some(elements_ids) = self.0.indexes.remove_index(table_id, index_id) {
			let ids: Vec<ElementId> = elements_ids.read().iter().collect();
			for element_id in ids {
				self.0.vectors.remove(&(table_id, index_id, element_id));
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
	pub(super) async fn len(&self, table_id: TableId, index_id: IndexId) -> u64 {
		self.0.indexes.len(table_id, index_id)
	}

	#[cfg(test)]
	pub(super) async fn contains(
		&self,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) -> bool {
		self.0.vectors.contains_key(&(table_id, index_id, element_id))
	}
}

#[cfg(test)]
mod tests {
	use ndarray::Array1;

	use super::*;
	use crate::idx::trees::vector::Vector;

	/// Test that cache eviction works correctly within an async runtime.
	///
	/// This test verifies the fix for a panic that occurred when using
	/// `tokio::sync::RwLock::blocking_write()` inside the eviction callback.
	/// The `blocking_write()` method panics when called from within an async
	/// runtime, which happened during cache eviction triggered by `insert()`.
	///
	/// The fix was to replace `tokio::sync::RwLock` with `parking_lot::RwLock`,
	/// which works safely in both sync and async contexts.
	#[tokio::test]
	async fn test_eviction_in_async_context() {
		// Create a very small cache (1KB) to force evictions quickly
		let cache = VectorCache::new(1024);

		let table_id = TableId(1);
		let index_id = IndexId(1);

		let dimensions = 128;

		for i in 0..100u64 {
			let data: Vec<f32> = (0..dimensions).map(|j| (i * dimensions + j) as f32).collect();
			let vector = Vector::F32(Array1::from_vec(data));
			let shared = SharedVector::from(vector);

			cache.insert(table_id, index_id, i, shared).await;
		}

		assert!(cache.len(table_id, index_id).await < 100);
	}

	#[tokio::test]
	async fn test_cache_insert_get_remove() {
		let cache = VectorCache::new(1024 * 1024); // 1MB cache

		let table_id = TableId(1);
		let index_id = IndexId(1);
		let element_id = 42u64;

		let data: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
		let vector = Vector::F32(Array1::from_vec(data));
		let shared = SharedVector::from(vector);

		// Insert
		cache.insert(table_id, index_id, element_id, shared.clone()).await;
		assert!(cache.contains(table_id, index_id, element_id).await);
		assert_eq!(cache.len(table_id, index_id).await, 1);

		// Get
		let retrieved = cache.get(table_id, index_id, element_id).await;
		assert!(retrieved.is_some());
		assert_eq!(retrieved.unwrap(), shared);

		// Remove
		cache.remove(table_id, index_id, element_id).await;
		assert!(!cache.contains(table_id, index_id, element_id).await);
		assert_eq!(cache.len(table_id, index_id).await, 0);
	}

	#[tokio::test]
	async fn test_remove_index() {
		let cache = VectorCache::new(1024 * 1024);

		let table_id = TableId(1);
		let index_id = IndexId(1);

		// Insert multiple vectors
		for i in 0..10u64 {
			let data: Vec<f32> = vec![i as f32; 4];
			let vector = Vector::F32(Array1::from_vec(data));
			let shared = SharedVector::from(vector);
			cache.insert(table_id, index_id, i, shared).await;
		}

		assert_eq!(cache.len(table_id, index_id).await, 10);

		// Remove entire index
		cache.remove_index(table_id, index_id).await;

		// All vectors should be gone
		for i in 0..10u64 {
			assert!(!cache.contains(table_id, index_id, i).await);
		}
	}
}
