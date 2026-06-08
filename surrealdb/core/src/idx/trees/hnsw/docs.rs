use std::sync::Arc;

use anyhow::Result;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::idx::IndexKeyBase;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::{SerializedVector, Vector};
use crate::kvs::{KVValue, Transaction};
use crate::val::{RecordId, RecordIdKey};

/// Manages the bidirectional mapping between record IDs and internal document IDs.
///
/// Maintains a pool of available (recycled) doc IDs and a monotonic counter
/// for allocating new ones, persisting the state to the key-value store.
pub(in crate::idx) struct HnswDocs {
	/// Key base for generating storage keys.
	ikb: IndexKeyBase,
	/// Whether the state has been modified and needs to be persisted.
	state_updated: bool,
	/// The persisted document allocation state.
	state: HnswDocsState,
}

/// Persisted state for document ID allocation.
#[revisioned(revision = 1)]
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct HnswDocsState {
	/// Pool of recycled doc IDs available for reuse.
	available: RoaringTreemap,
	/// The next doc ID to allocate when the pool is empty.
	next_doc_id: DocId,
}

impl HnswDocs {
	/// Creates a new `HnswDocs`, loading existing state from the key-value store.
	pub(in crate::idx) async fn new(tx: &Transaction, ikb: IndexKeyBase) -> Result<Self> {
		let state_key = ikb.new_hd_root_key();
		let state = tx.get(&state_key, None).await?.unwrap_or_default();
		Ok(Self {
			ikb,
			state_updated: false,
			state,
		})
	}

	/// Looks up the internal doc ID for a given record key, if it exists.
	///
	/// This is a static method that reads directly from the key-value store,
	/// avoiding the need to hold a lock on `HnswDocs`.
	pub(super) async fn get_doc_id(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<DocId>> {
		tx.get(&ikb.new_hi_key(id), None).await
	}

	/// Resolves a record key to its internal doc ID, creating a new mapping if needed.
	pub(super) async fn resolve(&mut self, tx: &Transaction, id: &RecordIdKey) -> Result<DocId> {
		if let Some(doc_id) = tx.get(&self.ikb.new_hi_key(id), None).await? {
			Ok(doc_id)
		} else {
			let doc_id = self.next_doc_id();
			let id_key = self.ikb.new_hi_key(id);
			tx.set(&id_key, &doc_id).await?;
			let doc_key = self.ikb.new_hd_key(doc_id);
			tx.set(&doc_key, id).await?;
			Ok(doc_id)
		}
	}

	/// Allocates the next available doc ID, reusing a recycled one if possible.
	fn next_doc_id(&mut self) -> DocId {
		self.state_updated = true;
		if let Some(doc_id) = self.state.available.iter().next() {
			self.state.available.remove(doc_id);
			doc_id
		} else {
			let doc_id = self.state.next_doc_id;
			self.state.next_doc_id += 1;
			doc_id
		}
	}

	fn cache_index(
		ikb: &IndexKeyBase,
		table_id: TableId,
	) -> (NamespaceId, DatabaseId, TableId, IndexId) {
		(ikb.ns(), ikb.db(), table_id, ikb.index())
	}

	/// Resolves one compact document ID through the shared batch/cache path.
	pub(super) async fn get_thing_cached(
		ikb: &IndexKeyBase,
		table_id: TableId,
		cache: &VectorCache,
		tx: &Transaction,
		doc_id: DocId,
		generation: Option<u64>,
	) -> Result<Option<Arc<RecordId>>> {
		Ok(Self::get_things_batch(ikb, table_id, cache, tx, &[doc_id], generation)
			.await?
			.into_iter()
			.next()
			.flatten())
	}

	/// Resolves compact document IDs to ordered record IDs, using cache hits before batched KV
	/// reads.
	///
	/// Positive mappings are inserted into the shared process-local cache only for read-only
	/// transactions. Write transactions can observe uncommitted `!hd` values, so those mappings
	/// stay local to the caller.
	pub(super) async fn get_things_batch(
		ikb: &IndexKeyBase,
		table_id: TableId,
		cache: &VectorCache,
		tx: &Transaction,
		doc_ids: &[DocId],
		generation: Option<u64>,
	) -> Result<Vec<Option<Arc<RecordId>>>> {
		let index = Self::cache_index(ikb, table_id);
		let table = ikb.table().clone();
		let mut rids = vec![None; doc_ids.len()];
		let mut misses = Vec::new();
		for (pos, doc_id) in doc_ids.iter().copied().enumerate() {
			if let Some(id) = cache.get_doc_id(index, doc_id, generation).await {
				rids[pos] = Some(Arc::new(RecordId {
					table: table.clone(),
					key: id.as_ref().clone(),
				}));
			} else {
				misses.push((pos, doc_id));
			}
		}
		if misses.is_empty() {
			return Ok(rids);
		}
		let keys: Vec<_> = misses.iter().map(|(_, doc_id)| ikb.new_hd_key(*doc_id)).collect();
		let ids: Vec<Option<RecordIdKey>> = tx.getm(keys, None).await?;
		let cache_misses = !tx.writeable();
		for ((pos, doc_id), id) in misses.into_iter().zip(ids) {
			if let Some(id) = id {
				let id = if cache_misses {
					cache.insert_doc_id(index, doc_id, generation, id).await
				} else {
					Arc::new(id)
				};
				rids[pos] = Some(Arc::new(RecordId {
					table: table.clone(),
					key: id.as_ref().clone(),
				}));
			}
		}
		Ok(rids)
	}

	/// Removes the mapping for a doc ID, recycling it for future reuse.
	/// Returns the removed doc ID if it existed.
	pub(super) async fn remove(
		&mut self,
		tx: &Transaction,
		doc_id: DocId,
		table_id: TableId,
		cache: &VectorCache,
	) -> Result<Option<DocId>> {
		let index = Self::cache_index(&self.ikb, table_id);
		cache.remove_doc_id(index, doc_id).await;
		let doc_key = self.ikb.new_hd_key(doc_id);
		let Some(id) = tx.get(&doc_key, None).await? else {
			return Ok(None);
		};
		self.state_updated = true;
		tx.del(&doc_key).await?;
		let id_key = self.ikb.new_hi_key(&id);
		if let Some(doc_id) = tx.get(&id_key, None).await? {
			tx.del(&id_key).await?;
			self.state.available.insert(doc_id);
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	/// Persists the document allocation state if it has been modified,
	/// then resets the dirty flag so subsequent calls are no-ops until
	/// the state is modified again.
	pub(in crate::idx) async fn finish(&mut self, tx: &Transaction) -> Result<()> {
		if self.state_updated {
			let state_key = self.ikb.new_hd_root_key();
			tx.set(&state_key, &self.state).await?;
			self.state_updated = false;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use anyhow::Result;

	use super::*;
	use crate::kvs::{Datastore, LockType, TransactionType};

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	#[tokio::test]
	async fn hnsw_docs_batch_preserves_order_and_uses_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set(&ikb.new_hd_key(1), &RecordIdKey::Number(11)).await?;
			tx.set(&ikb.new_hd_key(2), &RecordIdKey::Number(22)).await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got =
			HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[2, 1, 3], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(22));
		assert_eq!(&got[1].as_ref().unwrap().key, &RecordIdKey::Number(11));
		assert!(got[2].is_none());
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.del(&ikb.new_hd_key(1)).await?;
		tx.commit().await?;

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let cached =
			HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(5)).await?;
		assert_eq!(&cached[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_batch_does_not_cache_missing_or_write_transaction_mappings() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		assert_eq!(
			HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[9], Some(5)).await?,
			vec![None]
		);
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.set(&ikb.new_hd_key(9), &RecordIdKey::Number(99)).await?;
		let got = HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[9], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(99));
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 9, Some(5))
				.await
				.is_none()
		);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_batch_ignores_doc_id_cache_from_old_generation() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set(&ikb.new_hd_key(1), &RecordIdKey::Number(11)).await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got = HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		tx.cancel().await?;
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 1, Some(6))
				.await
				.is_none()
		);

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.set(&ikb.new_hd_key(1), &RecordIdKey::Number(22)).await?;
		tx.commit().await?;

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got = HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(6)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(22));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_remove_evicts_doc_id_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		let id = RecordIdKey::Number(77);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set(&ikb.new_hd_key(7), &id).await?;
			tx.set(&ikb.new_hi_key(&id), &7_u64).await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got = HnswDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[7], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &id);
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 7, Some(5))
				.await
				.is_some()
		);
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let mut docs = HnswDocs::new(&tx, ikb.clone()).await?;
		assert_eq!(docs.remove(&tx, 7, TableId(4), &cache).await?, Some(7));
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 7, Some(5))
				.await
				.is_none()
		);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_populates_and_uses_doc_set_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb.clone(), TableId(4), cache.clone(), false);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let vector = Vector::from(ser_vec.clone());
		tx.set(
			&ikb.new_hv_key(&ser_vec),
			&ElementDocs {
				e_id: 7,
				docs: Ids64::One(42),
			},
		)
		.await?;

		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		assert_eq!(
			cache.get_doc_set((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 7).await,
			Some(Ids64::One(42))
		);

		tx.del(&ikb.new_hv_key(&ser_vec)).await?;
		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		assert_eq!(vec_docs.get_docs_uncached(&tx, &vector).await?, None);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_hashed_disambiguates_and_caches_by_element() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb.clone(), TableId(4), cache.clone(), true);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let other_vec = SerializedVector::F32(vec![3.0, 4.0]);
		let vector = Vector::from(ser_vec.clone());
		let key = ikb.new_hh_key(ser_vec.compute_hash());
		tx.set(
			&key,
			&ElementHashedDocs {
				vectors: vec![
					(
						other_vec,
						ElementDocs {
							e_id: 8,
							docs: Ids64::One(88),
						},
					),
					(
						ser_vec,
						ElementDocs {
							e_id: 7,
							docs: Ids64::One(42),
						},
					),
				],
			},
		)
		.await?;

		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		tx.del(&key).await?;
		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_missing_mapping_returns_none_without_caching() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb.clone(), TableId(4), cache.clone(), false);
		let vector = Vector::from(SerializedVector::F32(vec![1.0, 2.0]));

		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, None);
		assert!(
			cache.get_doc_set((ikb.ns(), ikb.db(), TableId(4), ikb.index()), 7).await.is_none()
		);
		tx.cancel().await?;
		Ok(())
	}
}

impl KVValue for HnswDocsState {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Contains the mapping between an element ID and the document IDs that share the same vector.
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(crate) struct ElementDocs {
	e_id: ElementId,
	docs: Ids64,
}

impl ElementDocs {
	fn new(element_id: ElementId, d: DocId) -> Self {
		Self {
			e_id: element_id,
			docs: Ids64::One(d),
		}
	}
}

/// Contains a list of vectors and their associated document IDs that share the same hash.
#[revisioned(revision = 1)]
pub(crate) struct ElementHashedDocs {
	vectors: Vec<(SerializedVector, ElementDocs)>,
}

/// Result of removing a document from an [`ElementHashedDocs`] entry.
enum RemoveResult {
	/// The vector has no remaining documents; the element should be removed from the graph.
	Empty(ElementId),
	/// A document set changed without removing the graph element.
	Updated(ElementId, Ids64),
	/// A colliding vector was removed while other vectors remain in the hash bucket.
	RemovedElement(ElementId),
	/// The document was not found; no changes were made.
	Unchanged,
}

impl ElementHashedDocs {
	fn new(element_id: ElementId, vec: SerializedVector, doc_id: DocId) -> Self {
		let vectors = vec![(vec, ElementDocs::new(element_id, doc_id))];
		Self {
			vectors,
		}
	}

	fn get_element_docs(&mut self, vec: &SerializedVector) -> Option<&mut ElementDocs> {
		for (vector, ed) in self.vectors.iter_mut() {
			if *vec == *vector {
				return Some(ed);
			}
		}
		None
	}

	/// Returns the documents for the given vector if it exists in the list.
	fn get_docs(self, vec: &SerializedVector) -> Option<Ids64> {
		for (vector, ed) in self.vectors {
			if vector == *vec {
				return Some(ed.docs);
			}
		}
		None
	}

	fn add(&mut self, element_id: ElementId, vec: SerializedVector, doc_id: DocId) {
		self.vectors.push((vec, ElementDocs::new(element_id, doc_id)));
	}

	fn remove(&mut self, vec: &SerializedVector, doc_id: DocId) -> RemoveResult {
		let mut action = None;
		for (i, (vector, ed)) in self.vectors.iter_mut().enumerate() {
			if *vector == *vec
				&& let Some(new_docs) = ed.docs.remove(doc_id)
			{
				if new_docs.is_empty() {
					action = Some((i, ed.e_id));
					break;
				}
				ed.docs = new_docs;
				// The partition has been updated, but this vector has still connected document(s)
				return RemoveResult::Updated(ed.e_id, ed.docs.clone());
			}
		}
		if let Some((i, e_id)) = action {
			// There are no more documents for this vector, remove it
			self.vectors.remove(i);
			if self.vectors.is_empty() {
				// The vector partition is empty, remove the element and the hash entry
				return RemoveResult::Empty(e_id);
			}
			return RemoveResult::RemovedElement(e_id);
		}
		RemoveResult::Unchanged
	}
}
impl KVValue for ElementHashedDocs {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self>
	where
		Self: Sized,
	{
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

impl KVValue for ElementDocs {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

/// Manages the mapping between vectors and document IDs in the HNSW index.
pub(in crate::idx) struct VecDocs {
	ikb: IndexKeyBase,
	table_id: TableId,
	cache: VectorCache,
	use_hashed_vector: bool,
}

impl VecDocs {
	/// Creates a new `VecDocs` with the given index key base and hashing mode.
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		cache: VectorCache,
		use_hashed_vector: bool,
	) -> Self {
		Self {
			ikb,
			table_id,
			cache,
			use_hashed_vector,
		}
	}

	fn cache_index(&self) -> (NamespaceId, DatabaseId, TableId, IndexId) {
		(self.ikb.ns(), self.ikb.db(), self.table_id, self.ikb.index())
	}

	pub(super) async fn get_cached_doc_set(&self, element_id: ElementId) -> Option<Ids64> {
		let index = self.cache_index();
		self.cache.get_doc_set(index, element_id).await
	}

	async fn insert_cached_doc_set(&self, element_id: ElementId, docs: Ids64) {
		let index = self.cache_index();
		self.cache.insert_doc_set(index, element_id, docs).await;
	}

	async fn remove_cached_doc_set(&self, element_id: ElementId) {
		let index = self.cache_index();
		self.cache.remove_doc_set(index, element_id).await;
	}

	/// Retrieves document IDs for a given vector using its hash.
	async fn get_docs_hashed(
		&self,
		tx: &Transaction,
		ser_vec: SerializedVector,
	) -> Result<Option<Ids64>> {
		let hash = ser_vec.compute_hash();
		let key = self.ikb.new_hh_key(hash);
		// We search first in the new hash structure
		if let Some(ehd) = tx.get(&key, None).await?
			&& let Some(docs) = ehd.get_docs(&ser_vec)
		{
			return Ok(Some(docs));
		}
		Ok(None)
	}

	/// Retrieves document IDs for a given vector without consulting the process-local cache.
	async fn get_docs_uncached(&self, tx: &Transaction, pt: &Vector) -> Result<Option<Ids64>> {
		let ser_vec: SerializedVector = pt.into();
		if self.use_hashed_vector {
			return self.get_docs_hashed(tx, ser_vec).await;
		}
		// Otherwise we search in the structure
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(ed) = tx.get(&key, None).await? {
			return Ok(Some(ed.docs));
		}
		Ok(None)
	}

	/// Retrieves document IDs for a graph element, caching the vector-to-doc mapping by element ID.
	pub(super) async fn get_docs_by_element(
		&self,
		tx: &Transaction,
		element_id: ElementId,
		pt: &Vector,
	) -> Result<Option<Ids64>> {
		if let Some(docs) = self.get_cached_doc_set(element_id).await {
			return Ok(Some(docs));
		}
		let docs = self.get_docs_uncached(tx, pt).await?;
		if let Some(docs) = docs.clone() {
			self.insert_cached_doc_set(element_id, docs).await;
		}
		Ok(docs)
	}

	/// Inserts a vector and its associated document ID using its hash.
	async fn insert_hashed(
		&self,
		ctx: &HnswContext<'_>,
		o: Vector,
		ser_vec: SerializedVector,
		doc_id: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let key = self.ikb.new_hh_key(ser_vec.compute_hash());
		match ctx.tx.get(&key, None).await? {
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(ctx, o).await?;
				let docs = Ids64::One(doc_id);
				let ehd = ElementHashedDocs::new(element_id, ser_vec, doc_id);
				ctx.tx.set(&key, &ehd).await?;
				self.insert_cached_doc_set(element_id, docs).await;
			}
			Some(mut ehd) => {
				if let Some(ed) = ehd.get_element_docs(&ser_vec) {
					// We already have the vector
					if let Some(docs) = ed.docs.insert(doc_id) {
						ed.docs = docs;
						let element_id = ed.e_id;
						let docs = ed.docs.clone();
						ctx.tx.set(&key, &ehd).await?;
						self.insert_cached_doc_set(element_id, docs).await;
					};
				} else {
					//  We don't have the vector, we insert it in the graph
					let element_id = h.insert(ctx, o).await?;
					let docs = Ids64::One(doc_id);
					ehd.add(element_id, ser_vec, doc_id);
					ctx.tx.set(&key, &ehd).await?;
					self.insert_cached_doc_set(element_id, docs).await;
				}
			}
		};
		Ok(())
	}

	/// Inserts a vector and its associated document ID.
	pub(super) async fn insert(
		&self,
		ctx: &mut HnswContext<'_>,
		vec: Vector,
		doc_id: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let ser_vec = SerializedVector::from(&vec);
		if self.use_hashed_vector {
			return self.insert_hashed(ctx, vec, ser_vec, doc_id, h).await;
		}
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(ed) = match ctx.tx.get(&key, None).await? {
			Some(mut ed) => {
				// We already have the vector
				ed.docs.insert(doc_id).map(|new_docs| {
					ed.docs = new_docs;
					ed
				})
			}
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = h.insert(ctx, vec).await?;
				let ed = ElementDocs::new(element_id, doc_id);
				Some(ed)
			}
		} {
			ctx.tx.set(&key, &ed).await?;
			self.insert_cached_doc_set(ed.e_id, ed.docs.clone()).await;
		}
		Ok(())
	}

	/// Removes a vector and its associated document ID using its hash.
	async fn remove_hashed(
		&self,
		ctx: &HnswContext<'_>,
		ser_vec: SerializedVector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let key = self.ikb.new_hh_key(ser_vec.compute_hash());
		if let Some(mut ehd) = ctx.tx.get(&key, None).await? {
			match ehd.remove(&ser_vec, d) {
				RemoveResult::Empty(deleted_element_id) => {
					ctx.tx.del(&key).await?;
					self.remove_cached_doc_set(deleted_element_id).await;
					h.remove(ctx, deleted_element_id).await?;
				}
				RemoveResult::Updated(element_id, docs) => {
					ctx.tx.set(&key, &ehd).await?;
					self.insert_cached_doc_set(element_id, docs).await;
				}
				RemoveResult::RemovedElement(deleted_element_id) => {
					ctx.tx.set(&key, &ehd).await?;
					self.remove_cached_doc_set(deleted_element_id).await;
					h.remove(ctx, deleted_element_id).await?;
				}
				RemoveResult::Unchanged => {
					// The element was not existing or already deleted
				}
			}
		}
		Ok(())
	}

	/// Removes a vector and its associated document ID.
	pub(super) async fn remove(
		&self,
		ctx: &HnswContext<'_>,
		o: &Vector,
		d: DocId,
		h: &mut HnswFlavor,
	) -> Result<()> {
		let ser_vec = o.into();
		if self.use_hashed_vector {
			return self.remove_hashed(ctx, ser_vec, d, h).await;
		}
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(mut ed) = ctx.tx.get(&key, None).await?
			&& let Some(new_docs) = ed.docs.remove(d)
		{
			if new_docs.is_empty() {
				ctx.tx.del(&key).await?;
				self.remove_cached_doc_set(ed.e_id).await;
				h.remove(ctx, ed.e_id).await?;
			} else {
				ed.docs = new_docs;
				ctx.tx.set(&key, &ed).await?;
				self.insert_cached_doc_set(ed.e_id, ed.docs.clone()).await;
			}
		};
		Ok(())
	}
}
