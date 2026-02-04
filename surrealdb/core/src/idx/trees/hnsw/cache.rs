use std::sync::Arc;

use dashmap::{DashMap, Entry};
use parking_lot::RwLock;
use priority_lfu::{Cache, CacheKey};
use roaring::RoaringTreemap;

use crate::catalog::{IndexId, TableId};
use crate::cnf;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::vector::SharedVector;

// Newtype wrapper for the cache key to avoid orphan rule issues
#[derive(Clone, Hash, Eq, PartialEq)]
struct VectorCacheKey(TableId, IndexId, ElementId);

// Implement CacheKey for the newtype wrapper
impl CacheKey for VectorCacheKey {
	type Value = SharedVector;
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

	// Note: With weighted_cache, we no longer have lifecycle hooks for eviction tracking.
	// The ElementsPerIndex may contain IDs for vectors that have been evicted, but this
	// is acceptable since remove_index already checks for existence before removing.
	// This trade-off simplifies the cache implementation and avoids the complexity
	// of tracking evictions separately.
}

#[derive(Clone)]
pub(crate) struct VectorCache(Arc<Inner>);

struct Inner {
	/// For each index/element pair, the vector
	vectors: Cache,
	/// For each index, the set of element ids that have been cached.
	/// This allows efficient bulk removal of all vectors for an index without
	/// iterating through the entire cache.
	/// Note: May contain IDs for evicted vectors, but this is acceptable.
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

		Self(Arc::new(Inner {
			vectors: Cache::new(cache_size as usize),
			indexes,
		}))
	}

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
		let key = VectorCacheKey(table_id, index_id, element_id);
		self.0.vectors.insert(key, vector);
	}

	pub(super) async fn get(
		&self,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) -> Option<SharedVector> {
		let key = VectorCacheKey(table_id, index_id, element_id);
		self.0.vectors.get_clone(&key)
	}

	pub(super) async fn remove(&self, table_id: TableId, index_id: IndexId, element_id: ElementId) {
		// Remove from the indexes tracking structure first
		self.0.indexes.remove_element(table_id, index_id, element_id);
		// Remove from the vector cache
		let key = VectorCacheKey(table_id, index_id, element_id);
		self.0.vectors.remove(&key);
	}

	pub(crate) async fn remove_index(&self, table_id: TableId, index_id: IndexId) {
		let mut count = 0;
		if let Some(elements_ids) = self.0.indexes.remove_index(table_id, index_id) {
			let ids: Vec<ElementId> = elements_ids.read().iter().collect();
			for element_id in ids {
				let key = VectorCacheKey(table_id, index_id, element_id);
				self.0.vectors.remove(&key);
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
		let key = VectorCacheKey(table_id, index_id, element_id);
		self.0.vectors.contains(&key)
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
