//! In-memory cache for KV-backed DiskANN graph data.
//!
//! DiskANN search repeatedly touches a small working set of graph state, element vectors, and
//! adjacency lists. The persisted KV layout remains the source of truth, while this cache reduces
//! point-read latency and lets the provider batch only the entries that missed in memory.
//!
//! Every graph entry — state, element, adjacency list and doc set — carries a [`Stamp`]: the
//! [`DiskAnnIndex`](super::index::DiskAnnIndex) wrapper that published it and the `!dg` compaction
//! generation its value holds at. A transaction reads through a [`CacheView`], which is served an
//! entry only when the entry holds at the generation the transaction's snapshot holds. Every commit
//! that writes the graph or a vector's document mapping also advances `!dg`, so one generation
//! identifies one committed state of everything cached here. The provider module documents who
//! publishes which stamp, and why an entry can be trusted beyond its own generation.
//!
//! The doc-ID family maps a compact document ID to its record key, which never changes while the
//! ID is live, and is served on generation equality alone.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quick_cache::Weighter;
use quick_cache::sync::Cache;

use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::docids::DocId;
use crate::next_compaction_generation;
use crate::trees::diskann::{DiskAnnElement, DiskAnnNode, DiskAnnState, ElementId};
use crate::trees::knn::Ids64;
use crate::trees::vector::SerializedVector;
use crate::val::RecordIdKey;

type IndexKey = (NamespaceId, DatabaseId, TableId, IndexId, u16);
type ElementCacheKey = (NamespaceId, DatabaseId, TableId, IndexId, u16, ElementId);
type NodeCacheKey = ElementCacheKey;
type DocSetCacheKey = ElementCacheKey;
type DocIdCacheKey = (NamespaceId, DatabaseId, TableId, IndexId, u16, DocId);

/// The span of `!dg` generations over which one wrapper's cache has followed every commit to its
/// index.
///
/// Every generation in `(base, head]` was committed by one of the wrapper's own compaction frames,
/// each of which wrote through, or evicted, every entry whose value it changed. So an entry the
/// wrapper stamped at a generation inside the span, and that no commit since has replaced, still
/// holds at every generation from its stamp up to `head`. `None` stands for a store no compaction
/// has advanced yet, and orders before every generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Continuity {
	/// The generation the span opened at: the snapshot of the frame that found the span broken.
	pub(super) base: Option<u64>,
	/// The latest generation the wrapper's frames committed, or `base` before the first commit.
	pub(super) head: Option<u64>,
}

/// Who published a graph entry, and the `!dg` generation its value holds at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Stamp {
	/// The publishing wrapper's [`DiskAnnCache::new_owner`] id.
	owner: u64,
	/// The generation of the snapshot the value was read from, or the generation the frame that
	/// wrote it commits at.
	generation: Option<u64>,
}

/// How one transaction reads, and publishes into, one wrapper's graph entries.
///
/// A view is taken under the wrapper's graph lock and used only while that lock is held, so the
/// [`Continuity`] it carries cannot move under it: only a compaction frame, holding the lock
/// exclusively, changes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CacheView {
	/// The wrapper whose entries the view is served, and whose stamp it publishes under.
	owner: u64,
	/// The `!dg` generation the transaction's snapshot holds.
	generation: Option<u64>,
	/// The wrapper's continuity span when the view was taken.
	continuity: Option<Continuity>,
	/// For a compaction frame, the generation it commits at. `None` outside a frame.
	commit_generation: Option<u64>,
}

impl CacheView {
	/// The view of a transaction that only reads the graph.
	pub(super) fn reader(
		owner: u64,
		generation: Option<u64>,
		continuity: Option<Continuity>,
	) -> Self {
		Self {
			owner,
			generation,
			continuity,
			commit_generation: None,
		}
	}

	/// The view of a compaction frame whose snapshot holds `generation` and which commits the
	/// next one.
	pub(super) fn frame(
		owner: u64,
		generation: Option<u64>,
		continuity: Option<Continuity>,
	) -> Self {
		Self {
			commit_generation: Some(next_compaction_generation(generation)),
			..Self::reader(owner, generation, continuity)
		}
	}

	/// For a compaction frame, the generation it commits at.
	pub(super) fn commit_generation(&self) -> Option<u64> {
		self.commit_generation
	}

	/// Whether an entry with `stamp` holds the value this view's transaction reads from KV.
	///
	/// Only the view's own wrapper's entries qualify. Of those, an entry holds when it was stamped
	/// at the view's own generation; when the view is a frame's and it carries the frame's commit
	/// generation, since only the frame itself writes at that generation before it commits; and
	/// when the view's generation lies inside the continuity span, at or after the entry's stamp,
	/// which the span guarantees nothing has changed since.
	fn serves(&self, stamp: Stamp) -> bool {
		if stamp.owner != self.owner {
			return false;
		}
		let at = stamp.generation;
		at == self.generation
			|| (self.commit_generation.is_some() && at == self.commit_generation)
			|| self.continuity.is_some_and(|span| {
				span.base <= at && at <= self.generation && self.generation <= span.head
			})
	}

	/// The stamp a read-only transaction publishes a value it read from KV under, or `None` when it
	/// must not publish.
	///
	/// A reader whose generation is inside the span but behind its head must not publish: what it
	/// read holds at its own generation, but a frame since may have changed the key, and once the
	/// frame's write-through is evicted such an entry would be served across the span as if nothing
	/// had. Any other reader publishes at its own generation, where the entry holds exactly.
	pub(super) fn publication(&self) -> Option<Stamp> {
		let behind_head = self
			.continuity
			.is_some_and(|span| span.base <= self.generation && self.generation < span.head);
		(!behind_head).then_some(Stamp {
			owner: self.owner,
			generation: self.generation,
		})
	}

	/// The stamp a compaction frame writes its writes through under: its commit generation. `None`
	/// outside a frame, where a write evicts instead.
	pub(super) fn write_through(&self) -> Option<Stamp> {
		self.commit_generation.map(|generation| Stamp {
			owner: self.owner,
			generation: Some(generation),
		})
	}
}

/// A cached graph value and its stamp.
#[derive(Clone)]
struct Stamped<T> {
	stamp: Stamp,
	value: T,
}

impl<T> Stamped<T> {
	/// The value, when `view` is served it.
	fn served(self, view: &CacheView) -> Option<T> {
		view.serves(self.stamp).then_some(self.value)
	}
}

/// A cached doc-ID mapping and the `!dg` generation it was read at.
#[derive(Clone)]
struct AtGeneration<T> {
	/// The generation of the snapshot the value was read from.
	generation: Option<u64>,
	value: T,
}

impl<T> AtGeneration<T> {
	/// The value, when it was read at `generation`.
	fn at(self, generation: Option<u64>) -> Option<T> {
		(self.generation == generation).then_some(self.value)
	}
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum DiskAnnCacheKey {
	Element(ElementCacheKey),
	Node(NodeCacheKey),
	DocSet(DocSetCacheKey),
	DocId(DocIdCacheKey),
	State(IndexKey),
}

#[derive(Clone)]
enum DiskAnnCacheValue {
	Element(Stamped<Arc<DiskAnnElement>>),
	Node(Stamped<Arc<DiskAnnNode>>),
	DocSet(Stamped<Ids64>),
	DocId(AtGeneration<Arc<RecordIdKey>>),
	State(Stamped<DiskAnnState>),
}

#[derive(Clone)]
struct DiskAnnCacheWeighter;

impl Weighter<DiskAnnCacheKey, DiskAnnCacheValue> for DiskAnnCacheWeighter {
	fn weight(&self, key: &DiskAnnCacheKey, val: &DiskAnnCacheValue) -> u64 {
		match (key, val) {
			(DiskAnnCacheKey::Element(key), DiskAnnCacheValue::Element(val)) => {
				(serialized_vector_size(&val.value.vector)
					+ std::mem::size_of_val(&val.value.deleted)
					+ std::mem::size_of::<Stamped<Arc<DiskAnnElement>>>()
					+ std::mem::size_of_val(key)) as u64
			}
			(DiskAnnCacheKey::Node(key), DiskAnnCacheValue::Node(val)) => {
				(val.value.neighbors.len() * std::mem::size_of::<ElementId>()
					+ std::mem::size_of::<Stamped<Arc<DiskAnnNode>>>()
					+ std::mem::size_of_val(key)) as u64
			}
			(DiskAnnCacheKey::DocSet(key), DiskAnnCacheValue::DocSet(val)) => {
				(val.value.iter().count() * std::mem::size_of::<u64>()
					+ std::mem::size_of_val(&val.stamp)
					+ std::mem::size_of_val(key)) as u64
			}
			(DiskAnnCacheKey::DocId(key), DiskAnnCacheValue::DocId(val)) => {
				(std::mem::size_of_val(key)
					+ std::mem::size_of::<AtGeneration<Arc<RecordIdKey>>>()
					+ std::mem::size_of_val(val.value.as_ref())) as u64
			}
			(DiskAnnCacheKey::State(key), DiskAnnCacheValue::State(val)) => {
				(std::mem::size_of_val(key) + std::mem::size_of_val(val)) as u64
			}
			_ => unreachable!("mismatched DiskANN cache key/value"),
		}
	}
}

/// Shared weighted cache for one process' DiskANN graph data.
///
/// The cache is scoped by namespace, database, table, index id, and stored format version.
/// Format-version scoping keeps an index's legacy and rebuilt layouts in disjoint keyspaces, so
/// readers on either side of a `REBUILD INDEX` never observe — or evict — the other side's
/// entries. Element vectors (`De` keys), adjacency lists (`Dn` keys), graph state (`Ds` key), and
/// document mappings share one weighted budget keyed by the composite
/// (`namespace`, `database`, `table`, `index`, `format`, `id`) tuples; index removal walks every
/// shard with [`Cache::retain`] because per-key membership trackers cannot be kept reliably in
/// sync with quick_cache's hot/cold/ghost eviction.
#[derive(Clone)]
pub(crate) struct DiskAnnCache(Arc<Inner>);

struct Inner {
	/// Shared weighted budget for all DiskANN cache families.
	cache: Cache<DiskAnnCacheKey, DiskAnnCacheValue, DiskAnnCacheWeighter>,
	/// The last id [`DiskAnnCache::new_owner`] handed out.
	owners: AtomicU64,
}

impl DiskAnnCache {
	/// Creates a DiskANN ANN cache with one shared weight capacity (in bytes).
	pub(crate) fn new(cache_size: u64) -> Self {
		let estimated_items = (cache_size / 256).max(1) as usize;
		Self(Arc::new(Inner {
			cache: Cache::with_weighter(estimated_items, cache_size, DiskAnnCacheWeighter),
			owners: AtomicU64::new(0),
		}))
	}

	/// A fresh owner id for a wrapper's graph entries, distinct from every other this cache has
	/// handed out.
	///
	/// The registry keeps one wrapper per index, but a wrapper it drops stays alive in the searches
	/// still holding it, beside its replacement on the same keys. Each wrapper's lock orders only
	/// its own publishers against its own frames, so each is served only the entries it published.
	pub(super) fn new_owner(&self) -> u64 {
		self.0.owners.fetch_add(1, Ordering::Relaxed) + 1
	}

	fn index_key(
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		format_version: u16,
	) -> IndexKey {
		(namespace_id, database_id, table_id, index_id, format_version)
	}

	fn element_key(index: IndexKey, element_id: ElementId) -> ElementCacheKey {
		(index.0, index.1, index.2, index.3, index.4, element_id)
	}

	fn doc_id_key(index: IndexKey, doc_id: DocId) -> DocIdCacheKey {
		(index.0, index.1, index.2, index.3, index.4, doc_id)
	}

	/// Returns cached graph state for one index, when `view` is served it.
	pub(super) fn get_state(&self, index: IndexKey, view: &CacheView) -> Option<DiskAnnState> {
		match self.0.cache.get(&DiskAnnCacheKey::State(index))? {
			DiskAnnCacheValue::State(state) => state.served(view),
			_ => None,
		}
	}

	/// Inserts or replaces cached graph state for one index.
	pub(super) fn insert_state(&self, index: IndexKey, stamp: Stamp, state: DiskAnnState) {
		self.0.cache.insert(
			DiskAnnCacheKey::State(index),
			DiskAnnCacheValue::State(Stamped {
				stamp,
				value: state,
			}),
		);
	}

	/// Evicts the cached graph state for one index.
	pub(super) fn remove_state(&self, index: IndexKey) {
		self.0.cache.remove(&DiskAnnCacheKey::State(index));
	}

	/// Returns a shared cached graph element payload, when `view` is served it.
	pub(super) fn get_element(
		&self,
		index: IndexKey,
		element_id: ElementId,
		view: &CacheView,
	) -> Option<Arc<DiskAnnElement>> {
		match self.0.cache.get(&DiskAnnCacheKey::Element(Self::element_key(index, element_id)))? {
			DiskAnnCacheValue::Element(element) => element.served(view),
			_ => None,
		}
	}

	/// Inserts or replaces a graph element and returns the shared cached payload.
	pub(super) fn insert_element(
		&self,
		index: IndexKey,
		element_id: ElementId,
		stamp: Stamp,
		element: DiskAnnElement,
	) -> Arc<DiskAnnElement> {
		let element = Arc::new(element);
		self.0.cache.insert(
			DiskAnnCacheKey::Element(Self::element_key(index, element_id)),
			DiskAnnCacheValue::Element(Stamped {
				stamp,
				value: Arc::clone(&element),
			}),
		);
		element
	}

	/// Evicts one graph element from the cache.
	pub(super) fn remove_element(&self, index: IndexKey, element_id: ElementId) {
		self.0.cache.remove(&DiskAnnCacheKey::Element(Self::element_key(index, element_id)));
	}

	/// Returns a shared cached adjacency-list payload, when `view` is served it.
	pub(super) fn get_node(
		&self,
		index: IndexKey,
		element_id: ElementId,
		view: &CacheView,
	) -> Option<Arc<DiskAnnNode>> {
		match self.0.cache.get(&DiskAnnCacheKey::Node(Self::element_key(index, element_id)))? {
			DiskAnnCacheValue::Node(node) => node.served(view),
			_ => None,
		}
	}

	/// Inserts or replaces an adjacency list and returns the shared cached payload.
	pub(super) fn insert_node(
		&self,
		index: IndexKey,
		element_id: ElementId,
		stamp: Stamp,
		node: DiskAnnNode,
	) -> Arc<DiskAnnNode> {
		let node = Arc::new(node);
		self.0.cache.insert(
			DiskAnnCacheKey::Node(Self::element_key(index, element_id)),
			DiskAnnCacheValue::Node(Stamped {
				stamp,
				value: Arc::clone(&node),
			}),
		);
		node
	}

	/// Evicts one adjacency list from the cache.
	pub(super) fn remove_node(&self, index: IndexKey, element_id: ElementId) {
		self.0.cache.remove(&DiskAnnCacheKey::Node(Self::element_key(index, element_id)));
	}

	/// Returns a cached set of compact document IDs for one graph element, when `view` is served
	/// it.
	pub(super) fn get_doc_set(
		&self,
		index: IndexKey,
		element_id: ElementId,
		view: &CacheView,
	) -> Option<Ids64> {
		match self.0.cache.get(&DiskAnnCacheKey::DocSet(Self::element_key(index, element_id)))? {
			DiskAnnCacheValue::DocSet(docs) => docs.served(view),
			_ => None,
		}
	}

	/// Inserts or replaces the compact document IDs represented by one graph element.
	pub(super) fn insert_doc_set(
		&self,
		index: IndexKey,
		element_id: ElementId,
		stamp: Stamp,
		docs: Ids64,
	) {
		self.0.cache.insert(
			DiskAnnCacheKey::DocSet(Self::element_key(index, element_id)),
			DiskAnnCacheValue::DocSet(Stamped {
				stamp,
				value: docs,
			}),
		);
	}

	/// Evicts the cached document set for one graph element.
	pub(super) fn remove_doc_set(&self, index: IndexKey, element_id: ElementId) {
		self.0.cache.remove(&DiskAnnCacheKey::DocSet(Self::element_key(index, element_id)));
	}

	/// Returns a cached record key for a compact document ID, when it was read at `generation`.
	pub(super) fn get_doc_id(
		&self,
		index: IndexKey,
		doc_id: DocId,
		generation: Option<u64>,
	) -> Option<Arc<RecordIdKey>> {
		match self.0.cache.get(&DiskAnnCacheKey::DocId(Self::doc_id_key(index, doc_id)))? {
			DiskAnnCacheValue::DocId(id) => id.at(generation),
			_ => None,
		}
	}

	/// Inserts a compact document ID to record-key mapping read from KV at `generation`.
	pub(super) fn insert_doc_id(
		&self,
		index: IndexKey,
		doc_id: DocId,
		generation: Option<u64>,
		id: RecordIdKey,
	) -> Arc<RecordIdKey> {
		let id = Arc::new(id);
		self.0.cache.insert(
			DiskAnnCacheKey::DocId(Self::doc_id_key(index, doc_id)),
			DiskAnnCacheValue::DocId(AtGeneration {
				generation,
				value: Arc::clone(&id),
			}),
		);
		id
	}

	/// Evicts a cached compact document ID mapping.
	pub(super) fn remove_doc_id(&self, index: IndexKey, doc_id: DocId) {
		self.0.cache.remove(&DiskAnnCacheKey::DocId(Self::doc_id_key(index, doc_id)));
	}

	/// Evicts every cache family entry scoped to one DiskANN index at one stored format version.
	///
	/// Entries cached for the same index under a different format version are left in place: they
	/// belong to a different persisted layout, stay correct for readers still on that layout, and
	/// age out through normal weighted eviction once unused.
	///
	/// `Cache::retain` walks every shard of the shared cache. The DiskANN cache is shared across
	/// all indices in the process, so this is the only sound way to guarantee no stale adjacency
	/// or vector entry survives a failed-compaction cleanup or an index drop. The cost is
	/// proportional to the total resident cache, which is fine because this path runs only on
	/// rare events: a compaction frame that fails to commit, `REMOVE INDEX`, and index retirement.
	///
	/// This is the failure-side half of the
	/// [cache coherency invariant](crate::trees::diskann::provider): a compaction frame's
	/// write-throughs carry the generation it commits at, and a frame that fails to commit leaves
	/// that generation free for another process to commit with a different graph, so the frame
	/// clears its entries with this before it releases the graph lock.
	pub(crate) async fn remove_index(
		&self,
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		index_id: IndexId,
		format_version: u16,
	) {
		let index = Self::index_key(namespace_id, database_id, table_id, index_id, format_version);
		self.0.cache.retain(|key, _| match key {
			DiskAnnCacheKey::State(k) => *k != index,
			DiskAnnCacheKey::Element(k) => (k.0, k.1, k.2, k.3, k.4) != index,
			DiskAnnCacheKey::Node(k) => (k.0, k.1, k.2, k.3, k.4) != index,
			DiskAnnCacheKey::DocSet(k) => (k.0, k.1, k.2, k.3, k.4) != index,
			DiskAnnCacheKey::DocId(k) => (k.0, k.1, k.2, k.3, k.4) != index,
		});
		yield_now!();
	}

	#[cfg(test)]
	pub(super) fn weight(&self) -> u64 {
		self.0.cache.weight()
	}

	#[cfg(test)]
	fn capacity(&self) -> u64 {
		self.0.cache.capacity()
	}
}

fn serialized_vector_size(vector: &SerializedVector) -> usize {
	match vector {
		SerializedVector::F64(values) => values.len() * std::mem::size_of::<f64>(),
		SerializedVector::F32(values) => values.len() * std::mem::size_of::<f32>(),
		SerializedVector::I64(values) => values.len() * std::mem::size_of::<i64>(),
		SerializedVector::I32(values) => values.len() * std::mem::size_of::<i32>(),
		SerializedVector::I16(values) => values.len() * std::mem::size_of::<i16>(),
		SerializedVector::F16(values) => values.len() * std::mem::size_of::<u16>(),
		SerializedVector::I8(values) => values.len() * std::mem::size_of::<i8>(),
		SerializedVector::U8(values) => values.len() * std::mem::size_of::<u8>(),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
	use crate::trees::vector::SerializedVector;

	fn index() -> IndexKey {
		(NamespaceId(1), DatabaseId(2), TableId(3), IndexId(4), 0)
	}

	const OWNER: u64 = 1;

	fn stamp(generation: Option<u64>) -> Stamp {
		Stamp {
			owner: OWNER,
			generation,
		}
	}

	/// A reader of [`OWNER`]'s entries with no continuity span.
	fn at(generation: Option<u64>) -> CacheView {
		CacheView::reader(OWNER, generation, None)
	}

	fn span(base: u64, head: u64) -> Option<Continuity> {
		Some(Continuity {
			base: Some(base),
			head: Some(head),
		})
	}

	fn state() -> DiskAnnState {
		DiskAnnState {
			enter_point: Some(7),
			next_element_id: 10,
		}
	}

	fn element() -> DiskAnnElement {
		DiskAnnElement {
			vector: SerializedVector::F32(vec![1.0, 2.0]),
			deleted: false,
		}
	}

	fn node() -> DiskAnnNode {
		DiskAnnNode {
			neighbors: vec![8, 9],
		}
	}

	/// Inserts one entry of every graph family for element 7, and a doc-ID mapping for 11, all
	/// stamped at `generation`.
	fn fill(cache: &DiskAnnCache, index: IndexKey, generation: Option<u64>) {
		cache.insert_state(index, stamp(generation), state());
		cache.insert_element(index, 7, stamp(generation), element());
		cache.insert_node(index, 7, stamp(generation), node());
		cache.insert_doc_set(index, 7, stamp(generation), Ids64::One(11));
		cache.insert_doc_id(index, 11, generation, RecordIdKey::Number(17));
	}

	/// Which graph families `view` is served for element 7.
	fn served(cache: &DiskAnnCache, index: IndexKey, view: &CacheView) -> [bool; 4] {
		[
			cache.get_state(index, view).is_some(),
			cache.get_element(index, 7, view).is_some(),
			cache.get_node(index, 7, view).is_some(),
			cache.get_doc_set(index, 7, view).is_some(),
		]
	}

	#[tokio::test]
	async fn diskann_cache_families_share_one_capacity() {
		let cache = DiskAnnCache::new(1024);
		let index = index();
		cache.insert_state(index, stamp(Some(5)), state());
		cache.insert_element(
			index,
			7,
			stamp(Some(5)),
			DiskAnnElement {
				vector: SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0]),
				deleted: false,
			},
		);
		cache.insert_node(index, 7, stamp(Some(5)), node());
		cache.insert_doc_set(index, 7, stamp(Some(5)), Ids64::Vec2([11, 12]));
		cache.insert_doc_id(index, 11, Some(1), RecordIdKey::Number(99));

		assert_eq!(cache.capacity(), 1024);
		assert!(cache.weight() > 0);
		assert!(cache.weight() <= cache.capacity());
	}

	#[tokio::test]
	async fn diskann_cache_tracks_and_evicts_per_index() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let index = index();
		fill(&cache, index, Some(5));
		let view = at(Some(5));

		assert_eq!(cache.get_state(index, &view).unwrap().enter_point, state().enter_point);
		let first_element = cache.get_element(index, 7, &view).unwrap();
		let second_element = cache.get_element(index, 7, &view).unwrap();
		let first_node = cache.get_node(index, 7, &view).unwrap();
		let second_node = cache.get_node(index, 7, &view).unwrap();
		assert_eq!(first_element.vector, element().vector);
		assert_eq!(first_node.neighbors, node().neighbors);
		assert!(Arc::ptr_eq(&first_element, &second_element));
		assert!(Arc::ptr_eq(&first_node, &second_node));
		assert_eq!(cache.get_doc_set(index, 7, &view), Some(Ids64::One(11)));
		assert_eq!(
			cache.get_doc_id(index, 11, Some(5)).unwrap().as_ref(),
			&RecordIdKey::Number(17)
		);
		assert!(cache.get_doc_id(index, 11, Some(6)).is_none());

		cache.remove_index(index.0, index.1, index.2, index.3, index.4).await;

		assert_eq!(served(&cache, index, &view), [false; 4]);
		assert!(cache.get_doc_id(index, 11, Some(5)).is_none());
	}

	#[tokio::test]
	async fn diskann_cache_remove_index_is_scoped_to_format_version() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let legacy = index();
		let rebuilt = (legacy.0, legacy.1, legacy.2, legacy.3, 1u16);
		for index in [legacy, rebuilt] {
			fill(&cache, index, Some(5));
		}

		// Evicting one format version must leave the other version's entries untouched, so
		// readers on the other side of a rebuild keep their warm cache.
		cache.remove_index(legacy.0, legacy.1, legacy.2, legacy.3, legacy.4).await;

		let view = at(Some(5));
		assert_eq!(served(&cache, legacy, &view), [false; 4]);
		assert!(cache.get_doc_id(legacy, 11, Some(5)).is_none());
		assert_eq!(served(&cache, rebuilt, &view), [true; 4]);
		assert_eq!(cache.get_doc_set(rebuilt, 7, &view), Some(Ids64::One(11)));
		assert!(cache.get_doc_id(rebuilt, 11, Some(5)).is_some());
	}

	/// Without a continuity span, every family serves an entry only to a reader at the generation
	/// the entry holds at, older or newer, and the absent generation is a generation like any
	/// other. No reader is served another wrapper's entries.
	#[test]
	fn diskann_cache_serves_an_entry_only_at_its_own_generation() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let index = index();
		fill(&cache, index, Some(5));
		for other in [None, Some(4), Some(6)] {
			assert_eq!(served(&cache, index, &at(other)), [false; 4], "at {other:?}");
			assert!(cache.get_doc_id(index, 11, other).is_none(), "doc id at {other:?}");
		}
		assert_eq!(served(&cache, index, &at(Some(5))), [true; 4]);
		assert!(cache.get_doc_id(index, 11, Some(5)).is_some());
		let stranger = CacheView::reader(OWNER + 1, Some(5), span(0, 9));
		assert_eq!(served(&cache, index, &stranger), [false; 4]);

		// A publish from another generation replaces the entry rather than joining it.
		cache.insert_node(
			index,
			7,
			stamp(None),
			DiskAnnNode {
				neighbors: vec![8],
			},
		);
		assert!(cache.get_node(index, 7, &at(Some(5))).is_none());
		assert_eq!(cache.get_node(index, 7, &at(None)).unwrap().neighbors, vec![8]);
	}

	/// Inside a continuity span an entry holds from its stamp up to the span's head, so a reader
	/// is served it at any generation in between, and at no generation outside.
	#[test]
	fn diskann_cache_serves_an_entry_across_its_continuity_span() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let index = index();
		fill(&cache, index, Some(6));
		for (generation, expected) in
			[(4, false), (5, false), (6, true), (7, true), (8, true), (9, false)]
		{
			let view = CacheView::reader(OWNER, Some(generation), span(5, 8));
			assert_eq!(served(&cache, index, &view), [expected; 4], "reader at {generation}");
		}
		// The doc-ID family is served on generation equality alone.
		assert!(cache.get_doc_id(index, 11, Some(7)).is_none());

		// An entry stamped below the span's base predates a commit the wrapper did not make, so
		// the span says nothing about it: it is served at its own generation only.
		fill(&cache, index, Some(4));
		for generation in [5, 6, 8] {
			let view = CacheView::reader(OWNER, Some(generation), span(5, 8));
			assert_eq!(served(&cache, index, &view), [false; 4], "reader at {generation}");
		}
		assert_eq!(
			served(&cache, index, &CacheView::reader(OWNER, Some(4), span(5, 8))),
			[true; 4]
		);
	}

	/// A reader behind the span's head publishes nothing; every other reader publishes at its own
	/// generation.
	#[test]
	fn diskann_cache_fences_publication_behind_the_span_head() {
		let behind = [5, 6, 7];
		for generation in behind {
			let view = CacheView::reader(OWNER, Some(generation), span(5, 8));
			assert_eq!(view.publication(), None, "reader at {generation}");
		}
		for generation in [3, 4, 8, 9] {
			let view = CacheView::reader(OWNER, Some(generation), span(5, 8));
			assert_eq!(view.publication(), Some(stamp(Some(generation))), "reader at {generation}");
		}
		assert_eq!(at(Some(6)).publication(), Some(stamp(Some(6))));
		assert_eq!(at(Some(6)).write_through(), None);
	}

	/// A frame writes through at the generation it commits at, and is served what it wrote there
	/// as well as what its own snapshot holds.
	#[test]
	fn diskann_cache_frame_is_served_its_own_write_throughs() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let index = index();
		let frame = CacheView::frame(OWNER, Some(8), span(5, 8));
		assert_eq!(frame.commit_generation(), Some(9));
		let written = frame.write_through().expect("a frame writes through");
		assert_eq!(written, stamp(Some(9)));
		cache.insert_node(index, 7, written, node());
		assert!(cache.get_node(index, 7, &frame).is_some());
		// Nobody else is served the frame's generation before it commits.
		assert!(cache.get_node(index, 7, &CacheView::reader(OWNER, Some(8), span(5, 8))).is_none());
		// What the span holds at the frame's snapshot is served to it as to any reader there.
		cache.insert_element(index, 7, stamp(Some(6)), element());
		assert!(cache.get_element(index, 7, &frame).is_some());
		// A frame on a store no compaction has advanced commits the first generation.
		assert_eq!(CacheView::frame(OWNER, None, None).commit_generation(), Some(1));
	}

	#[test]
	fn diskann_cache_invalidates_single_element_node_and_doc_set() {
		let cache = DiskAnnCache::new(1024 * 1024);
		let index = index();
		fill(&cache, index, Some(5));
		let view = at(Some(5));
		assert_eq!(served(&cache, index, &view), [true; 4]);
		assert!(cache.get_doc_id(index, 11, Some(4)).is_none());
		assert_eq!(
			cache.get_doc_id(index, 11, Some(5)).unwrap().as_ref(),
			&RecordIdKey::Number(17)
		);

		cache.remove_state(index);
		cache.remove_element(index, 7);
		cache.remove_node(index, 7);
		cache.remove_doc_set(index, 7);
		cache.remove_doc_id(index, 11);

		assert_eq!(served(&cache, index, &view), [false; 4]);
		assert!(cache.get_doc_id(index, 11, Some(5)).is_none());
	}
}
