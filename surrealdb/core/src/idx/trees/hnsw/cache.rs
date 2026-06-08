use std::sync::Arc;

use dashmap::{DashMap, Entry};
use parking_lot::RwLock;
use quick_cache::sync::Cache;
use quick_cache::{DefaultHashBuilder, Lifecycle, Weighter};
use roaring::RoaringTreemap;

use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::SharedVector;
use crate::val::RecordIdKey;

pub(super) type HnswCacheIndex = (NamespaceId, DatabaseId, TableId, IndexId);
/// Cache key uniquely identifying a vector: (table, index, element).
type VectorCacheKey = (NamespaceId, DatabaseId, TableId, IndexId, ElementId);
type DocSetCacheKey = VectorCacheKey;
type DocIdCacheKey = (NamespaceId, DatabaseId, TableId, IndexId, DocId);

#[derive(Clone)]
struct CachedDocId {
	/// Pending-state generation observed when this mapping was read from KV.
	generation: Option<u64>,
	/// Record key stored under the compact document-id mapping.
	id: Arc<RecordIdKey>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum HnswCacheKey {
	Vector(VectorCacheKey),
	DocSet(DocSetCacheKey),
	DocId(DocIdCacheKey),
}

#[derive(Clone)]
enum HnswCacheValue {
	Vector(SharedVector),
	DocSet(Ids64),
	DocId(CachedDocId),
}

#[derive(Clone)]
struct HnswCacheWeighter;

impl Weighter<HnswCacheKey, HnswCacheValue> for HnswCacheWeighter {
	fn weight(&self, key: &HnswCacheKey, val: &HnswCacheValue) -> u64 {
		match (key, val) {
			(HnswCacheKey::Vector(key), HnswCacheValue::Vector(val)) => {
				// Calculate total memory: vector (including Arc + hash) + TableId + IndexId.
				(val.mem_size() + std::mem::size_of_val(&key.0) + std::mem::size_of_val(&key.1))
					as u64
			}
			(HnswCacheKey::DocSet(key), HnswCacheValue::DocSet(val)) => {
				(val.iter().count() * std::mem::size_of::<u64>() + std::mem::size_of_val(key))
					as u64
			}
			(HnswCacheKey::DocId(key), HnswCacheValue::DocId(val)) => {
				(std::mem::size_of_val(key)
					+ std::mem::size_of_val(&val.generation)
					+ std::mem::size_of::<Arc<RecordIdKey>>()
					+ std::mem::size_of_val(val.id.as_ref())) as u64
			}
			_ => unreachable!("mismatched HNSW cache key/value"),
		}
	}
}

/// Tracks which element IDs are cached for each index.
/// Wrapped in Arc to share ownership between VectorCache and VectorCacheLifecycle.
#[derive(Clone, Default)]
struct IdsPerIndex(Arc<DashMap<HnswCacheIndex, RwLock<RoaringTreemap>>>);

impl IdsPerIndex {
	fn insert(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) {
		self.0
			.entry((namespace_id, database_id, table_id, index_id))
			.or_default()
			.write()
			.insert(element_id);
	}

	#[cfg(test)]
	fn len(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) -> u64 {
		if let Some(elements_ids) = self.0.get(&(namespace_id, database_id, table_id, index_id)) {
			elements_ids.read().len()
		} else {
			0
		}
	}
	fn remove_index(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) -> Option<RwLock<RoaringTreemap>> {
		self.0.remove(&(namespace_id, database_id, table_id, index_id)).map(|entry| entry.1)
	}
	fn remove_element(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) {
		if let Entry::Occupied(mut entry) =
			self.0.entry((namespace_id, database_id, table_id, index_id))
		{
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
		if let Entry::Occupied(mut entry) = self.0.entry((key.0, key.1, key.2, key.3)) {
			entry.get_mut().write().remove(key.4);
			// Note: We intentionally don't clean up empty index entries here to avoid potential
			// race conditions. Empty entries are cleaned up during remove_element() calls.
		}
	}
}

#[derive(Clone)]
struct HnswCacheLifecycle {
	indexes: IdsPerIndex,
	doc_set_indexes: IdsPerIndex,
	doc_id_indexes: IdsPerIndex,
}

impl Lifecycle<HnswCacheKey, HnswCacheValue> for HnswCacheLifecycle {
	type RequestState = ();

	fn begin_request(&self) -> Self::RequestState {}

	fn on_evict(&self, _: &mut Self::RequestState, key: HnswCacheKey, _: HnswCacheValue) {
		// Called synchronously by quick_cache during eviction.
		// We use the sync variant to maintain consistency without async overhead.
		match key {
			HnswCacheKey::Vector(key) => self.indexes.evict_element(key),
			HnswCacheKey::DocSet(key) => self.doc_set_indexes.evict_element(key),
			HnswCacheKey::DocId(key) => self.doc_id_indexes.evict_element(key),
		}
	}
}

/// Thread-safe, weighted cache for HNSW ANN vectors and document mappings.
///
/// Shared across all HNSW indexes via `Arc`. Tracks cached entries per index for efficient bulk
/// eviction when an index is dropped.
#[derive(Clone)]
pub(crate) struct VectorCache(Arc<Inner>);

struct Inner {
	/// Shared weighted budget for all HNSW ANN cache families.
	cache: Cache<
		HnswCacheKey,
		HnswCacheValue,
		HnswCacheWeighter,
		DefaultHashBuilder,
		HnswCacheLifecycle,
	>,
	/// For each index, the set of element ids that have been cached.
	/// This allows efficient bulk removal of all vectors for an index without
	/// iterating through the entire cache.
	indexes: IdsPerIndex,
	/// Element IDs currently present in the doc-set cache.
	doc_set_indexes: IdsPerIndex,
	/// Document IDs currently present in the doc-id cache.
	doc_id_indexes: IdsPerIndex,
}

impl VectorCache {
	/// Creates a new HNSW ANN cache with one shared weight capacity (in bytes).
	pub(crate) fn new(cache_size: u64) -> Self {
		let indexes = IdsPerIndex::default();
		let doc_set_indexes = IdsPerIndex::default();
		let doc_id_indexes = IdsPerIndex::default();
		let lifecycle = HnswCacheLifecycle {
			indexes: indexes.clone(),
			doc_set_indexes: doc_set_indexes.clone(),
			doc_id_indexes: doc_id_indexes.clone(),
		};
		let estimated_items = (cache_size / 256).max(1) as usize;

		Self(Arc::new(Inner {
			cache: Cache::with(
				estimated_items,
				cache_size,
				HnswCacheWeighter,
				DefaultHashBuilder::default(),
				lifecycle,
			),
			indexes,
			doc_set_indexes,
			doc_id_indexes,
		}))
	}

	/// Inserts a vector into the cache, tracking it in the per-index element set.
	pub(super) async fn insert(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
		vector: SharedVector,
	) {
		// Update indexes tracking first, before inserting into cache.
		// This prevents a race condition where eviction could occur immediately after
		// cache insertion but before index tracking is updated, leaving an inconsistent state.
		let key = (namespace_id, database_id, table_id, index_id, element_id);
		self.0.indexes.insert(namespace_id, database_id, table_id, index_id, element_id);
		self.0.cache.insert(HnswCacheKey::Vector(key), HnswCacheValue::Vector(vector));
	}

	/// Retrieves a cached vector, if present.
	pub(super) async fn get(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) -> Option<SharedVector> {
		let key = HnswCacheKey::Vector((namespace_id, database_id, table_id, index_id, element_id));
		match self.0.cache.get(&key) {
			Some(HnswCacheValue::Vector(vector)) => Some(vector),
			_ => None,
		}
	}

	/// Removes a single vector from the cache.
	pub(super) async fn remove(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) {
		// Remove from the indexes tracking structure first
		self.0.indexes.remove_element(namespace_id, database_id, table_id, index_id, element_id);
		self.0.cache.remove(&HnswCacheKey::Vector((
			namespace_id,
			database_id,
			table_id,
			index_id,
			element_id,
		)));
	}

	/// Retrieves cached compact document IDs for an element, if present.
	pub(super) async fn get_doc_set(
		&self,
		index: HnswCacheIndex,
		element_id: ElementId,
	) -> Option<Ids64> {
		let key = HnswCacheKey::DocSet((index.0, index.1, index.2, index.3, element_id));
		match self.0.cache.get(&key) {
			Some(HnswCacheValue::DocSet(docs)) => Some(docs),
			_ => None,
		}
	}

	/// Inserts or refreshes compact document IDs represented by one element.
	pub(super) async fn insert_doc_set(
		&self,
		index: HnswCacheIndex,
		element_id: ElementId,
		docs: Ids64,
	) {
		let key = (index.0, index.1, index.2, index.3, element_id);
		self.0.doc_set_indexes.insert(index.0, index.1, index.2, index.3, element_id);
		self.0.cache.insert(HnswCacheKey::DocSet(key), HnswCacheValue::DocSet(docs));
	}

	/// Evicts cached compact document IDs for one element.
	pub(super) async fn remove_doc_set(&self, index: HnswCacheIndex, element_id: ElementId) {
		self.0.doc_set_indexes.remove_element(index.0, index.1, index.2, index.3, element_id);
		self.0
			.cache
			.remove(&HnswCacheKey::DocSet((index.0, index.1, index.2, index.3, element_id)));
	}

	/// Retrieves a cached compact document-id mapping if its generation still matches.
	pub(super) async fn get_doc_id(
		&self,
		index: HnswCacheIndex,
		doc_id: DocId,
		generation: Option<u64>,
	) -> Option<Arc<RecordIdKey>> {
		let key = HnswCacheKey::DocId((index.0, index.1, index.2, index.3, doc_id));
		let cached = match self.0.cache.get(&key)? {
			HnswCacheValue::DocId(cached) => cached,
			_ => return None,
		};
		(cached.generation == generation).then_some(cached.id)
	}

	/// Inserts a compact document-id mapping read from KV.
	pub(super) async fn insert_doc_id(
		&self,
		index: HnswCacheIndex,
		doc_id: DocId,
		generation: Option<u64>,
		id: RecordIdKey,
	) -> Arc<RecordIdKey> {
		let id = Arc::new(id);
		self.0.doc_id_indexes.insert(index.0, index.1, index.2, index.3, doc_id);
		self.0.cache.insert(
			HnswCacheKey::DocId((index.0, index.1, index.2, index.3, doc_id)),
			HnswCacheValue::DocId(CachedDocId {
				generation,
				id: Arc::clone(&id),
			}),
		);
		id
	}

	/// Evicts a cached compact document-id mapping.
	pub(super) async fn remove_doc_id(&self, index: HnswCacheIndex, doc_id: DocId) {
		self.0.doc_id_indexes.remove_element(index.0, index.1, index.2, index.3, doc_id);
		self.0.cache.remove(&HnswCacheKey::DocId((index.0, index.1, index.2, index.3, doc_id)));
	}

	/// Removes all cached entries for a given index, yielding periodically during bulk removal.
	pub(crate) async fn remove_index(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) {
		let mut count = 0;
		if let Some(elements_ids) =
			self.0.indexes.remove_index(namespace_id, database_id, table_id, index_id)
		{
			let ids: Vec<ElementId> = elements_ids.read().iter().collect();
			for element_id in ids {
				self.0.cache.remove(&HnswCacheKey::Vector((
					namespace_id,
					database_id,
					table_id,
					index_id,
					element_id,
				)));
				// Yield control every 1000 removals to prevent blocking other async tasks
				// during bulk operations
				if count % 1000 == 0 {
					yield_now!()
				}
				count += 1;
			}
		}
		if let Some(elements_ids) =
			self.0.doc_set_indexes.remove_index(namespace_id, database_id, table_id, index_id)
		{
			let ids: Vec<ElementId> = elements_ids.read().iter().collect();
			for (count, element_id) in ids.into_iter().enumerate() {
				self.0.cache.remove(&HnswCacheKey::DocSet((
					namespace_id,
					database_id,
					table_id,
					index_id,
					element_id,
				)));
				if count % 1000 == 0 {
					yield_now!()
				}
			}
		}
		if let Some(doc_ids) =
			self.0.doc_id_indexes.remove_index(namespace_id, database_id, table_id, index_id)
		{
			let ids: Vec<DocId> = doc_ids.read().iter().collect();
			for (count, doc_id) in ids.into_iter().enumerate() {
				self.0.cache.remove(&HnswCacheKey::DocId((
					namespace_id,
					database_id,
					table_id,
					index_id,
					doc_id,
				)));
				if count % 1000 == 0 {
					yield_now!()
				}
			}
		}
	}
	#[cfg(test)]
	pub(super) async fn len(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) -> u64 {
		self.0.indexes.len(namespace_id, database_id, table_id, index_id)
	}

	#[cfg(test)]
	pub(super) async fn contains(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		element_id: ElementId,
	) -> bool {
		self.0.cache.contains_key(&HnswCacheKey::Vector((
			namespace_id,
			database_id,
			table_id,
			index_id,
			element_id,
		)))
	}

	#[cfg(test)]
	fn weight(&self) -> u64 {
		self.0.cache.weight()
	}

	#[cfg(test)]
	fn capacity(&self) -> u64 {
		self.0.cache.capacity()
	}

	#[cfg(test)]
	pub(super) async fn doc_set_len(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) -> u64 {
		self.0.doc_set_indexes.len(namespace_id, database_id, table_id, index_id)
	}

	#[cfg(test)]
	pub(super) async fn doc_id_len(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
	) -> u64 {
		self.0.doc_id_indexes.len(namespace_id, database_id, table_id, index_id)
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
	async fn test_cache_families_share_one_capacity() {
		let cache = VectorCache::new(1024);

		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let index_id = IndexId(4);
		let index = (namespace_id, database_id, table_id, index_id);
		let vector = Vector::F32(Array1::from_vec(vec![1.0, 2.0, 3.0, 4.0]));

		cache
			.insert(namespace_id, database_id, table_id, index_id, 7, SharedVector::from(vector))
			.await;
		cache.insert_doc_set(index, 7, Ids64::Vec2([11, 12])).await;
		cache.insert_doc_id(index, 11, Some(1), RecordIdKey::Number(99)).await;

		assert_eq!(cache.capacity(), 1024);
		assert!(cache.weight() > 0);
		assert!(cache.weight() <= cache.capacity());
	}

	#[tokio::test]
	async fn test_eviction_in_async_context() {
		// Create a very small cache (1KB) to force evictions quickly
		let cache = VectorCache::new(1024);

		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let index_id = IndexId(4);

		let dimensions = 128;

		for i in 0..100u64 {
			let data: Vec<f32> = (0..dimensions).map(|j| (i * dimensions + j) as f32).collect();
			let vector = Vector::F32(Array1::from_vec(data));
			let shared = SharedVector::from(vector);

			cache.insert(namespace_id, database_id, table_id, index_id, i, shared).await;
		}

		assert!(cache.len(namespace_id, database_id, table_id, index_id).await < 100);
	}

	#[tokio::test]
	async fn test_cache_insert_get_remove() {
		let cache = VectorCache::new(1024 * 1024); // 1MB cache

		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let index_id = IndexId(4);
		let element_id = 42u64;
		let index = (namespace_id, database_id, table_id, index_id);

		let data: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
		let vector = Vector::F32(Array1::from_vec(data));
		let shared = SharedVector::from(vector);

		// Insert
		cache
			.insert(namespace_id, database_id, table_id, index_id, element_id, shared.clone())
			.await;
		assert!(cache.contains(namespace_id, database_id, table_id, index_id, element_id).await);
		assert_eq!(cache.len(namespace_id, database_id, table_id, index_id).await, 1);

		// Get
		let retrieved = cache.get(namespace_id, database_id, table_id, index_id, element_id).await;
		assert!(retrieved.is_some());
		assert_eq!(retrieved.unwrap(), shared);
		cache.insert_doc_set(index, element_id, Ids64::Vec2([7, 8])).await;
		assert_eq!(cache.get_doc_set(index, element_id).await, Some(Ids64::Vec2([7, 8])));
		let id = cache.insert_doc_id(index, 7, Some(3), RecordIdKey::Number(99)).await;
		assert_eq!(id.as_ref(), &RecordIdKey::Number(99));
		assert_eq!(
			cache.get_doc_id(index, 7, Some(3)).await.unwrap().as_ref(),
			&RecordIdKey::Number(99)
		);
		assert!(cache.get_doc_id(index, 7, Some(4)).await.is_none());

		// Remove
		cache.remove(namespace_id, database_id, table_id, index_id, element_id).await;
		cache.remove_doc_set(index, element_id).await;
		cache.remove_doc_id(index, 7).await;
		assert!(!cache.contains(namespace_id, database_id, table_id, index_id, element_id).await);
		assert_eq!(cache.len(namespace_id, database_id, table_id, index_id).await, 0);
		assert_eq!(cache.doc_set_len(namespace_id, database_id, table_id, index_id).await, 0);
		assert_eq!(cache.doc_id_len(namespace_id, database_id, table_id, index_id).await, 0);
	}

	#[tokio::test]
	async fn test_remove_index() {
		let cache = VectorCache::new(1024 * 1024);

		let namespace_id = NamespaceId(1);
		let database_id = DatabaseId(2);
		let table_id = TableId(3);
		let index_id = IndexId(4);
		let index = (namespace_id, database_id, table_id, index_id);

		// Insert multiple vectors
		for i in 0..10u64 {
			let data: Vec<f32> = vec![i as f32; 4];
			let vector = Vector::F32(Array1::from_vec(data));
			let shared = SharedVector::from(vector);
			cache.insert(namespace_id, database_id, table_id, index_id, i, shared).await;
			cache.insert_doc_set(index, i, Ids64::One(i)).await;
			cache.insert_doc_id(index, i, Some(1), RecordIdKey::Number(i as i64)).await;
		}

		assert_eq!(cache.len(namespace_id, database_id, table_id, index_id).await, 10);
		assert_eq!(cache.doc_set_len(namespace_id, database_id, table_id, index_id).await, 10);
		assert_eq!(cache.doc_id_len(namespace_id, database_id, table_id, index_id).await, 10);

		// Remove entire index
		cache.remove_index(namespace_id, database_id, table_id, index_id).await;

		// All vectors should be gone
		for i in 0..10u64 {
			assert!(!cache.contains(namespace_id, database_id, table_id, index_id, i).await);
			assert!(cache.get_doc_set(index, i).await.is_none());
			assert!(cache.get_doc_id(index, i, Some(1)).await.is_none());
		}
	}
}
