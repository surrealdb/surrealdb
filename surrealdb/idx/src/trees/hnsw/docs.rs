use std::sync::Arc;

use anyhow::Result;
use surrealdb_datastore::Transaction;
// The vector-to-documents mapping is a stored value, and the bucket-mutation rule
// that keeps one entry per distinct vector travels with it.
pub(crate) use surrealdb_datastore::values::hnsw::{ElementDocs, ElementHashedDocs, RemoveResult};

use crate::IndexKeyBase;
use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::docids::{DocId, LegacyVectorDocIds, TableDocIds};
use crate::env::IndexEnv;
use crate::trees::hnsw::ElementId;
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::flavor::HnswFlavor;
use crate::trees::hnsw::index::HnswContext;
use crate::trees::knn::Ids64;
use crate::trees::vector::{SerializedVector, Vector};
use crate::val::{RecordId, RecordIdKey};

/// Per-index facade over the table's shared doc-ID space for HNSW.
///
/// The record ↔ doc-ID mapping lives in [`TableDocIds`] (keys `!di`/`!dd` under
/// the table prefix), shared by every index on the table so cross-index candidate
/// composition can rely on one doc-ID per record. HNSW keeps its own element/graph
/// keys; this type only maps between record IDs and those shared doc-IDs (plus the
/// process-local resolution cache).
pub(crate) struct HnswDocs {
	/// Key base for generating storage keys.
	ikb: IndexKeyBase,
	/// Whether the index uses the table-level shared doc-ID space.
	uses_shared_doc_ids: bool,
	/// Stored index format used to scope cache entries.
	format_version: u16,
}

impl HnswDocs {
	/// Creates an `HnswDocs` facade over the table's shared doc-ID space.
	pub(crate) fn with_format_version(ikb: IndexKeyBase, format_version: u16) -> Self {
		Self {
			uses_shared_doc_ids: surrealdb_catalog::uses_shared_doc_ids(format_version),
			ikb,
			format_version,
		}
	}

	/// Looks up the internal doc ID for a given record key, if it exists.
	///
	/// This is a static method that reads directly from the key-value store,
	/// avoiding the need to hold a lock on `HnswDocs`.
	pub(super) async fn get_doc_id(
		ikb: &IndexKeyBase,
		format_version: u16,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<DocId>> {
		if surrealdb_catalog::uses_shared_doc_ids(format_version) {
			TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone()).get_doc_id(tx, id).await
		} else {
			LegacyVectorDocIds::Hnsw(ikb.clone()).get_doc_id(tx, id).await
		}
	}

	/// Resolves a record key to its doc ID in the table's shared space, allocating
	/// a new one if needed.
	pub(super) async fn resolve(&self, env: &dyn IndexEnv, id: &RecordIdKey) -> Result<DocId> {
		if self.uses_shared_doc_ids {
			TableDocIds::new(self.ikb.ns(), self.ikb.db(), self.ikb.table().clone())
				.resolve_or_assign(env, id)
				.await
		} else {
			LegacyVectorDocIds::Hnsw(self.ikb.clone()).resolve_or_assign(env, id).await
		}
	}

	fn cache_index(
		ikb: &IndexKeyBase,
		table_id: TableId,
		format_version: u16,
	) -> (NamespaceId, DatabaseId, TableId, IndexId, u16) {
		(ikb.ns(), ikb.db(), table_id, ikb.index(), format_version)
	}

	/// Resolves one compact document ID through the shared batch/cache path.
	pub(super) async fn get_thing_cached(
		ikb: &IndexKeyBase,
		format_version: u16,
		table_id: TableId,
		cache: &VectorCache,
		tx: &Transaction,
		doc_id: DocId,
		generation: Option<u64>,
	) -> Result<Option<Arc<RecordId>>> {
		Ok(Self::get_things_batch(ikb, format_version, table_id, cache, tx, &[doc_id], generation)
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
		format_version: u16,
		table_id: TableId,
		cache: &VectorCache,
		tx: &Transaction,
		doc_ids: &[DocId],
		generation: Option<u64>,
	) -> Result<Vec<Option<Arc<RecordId>>>> {
		let index = Self::cache_index(ikb, table_id, format_version);
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
		let miss_ids: Vec<DocId> = misses.iter().map(|(_, doc_id)| *doc_id).collect();
		let ids = if surrealdb_catalog::uses_shared_doc_ids(format_version) {
			TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
				.get_record_ids_batch(tx, &miss_ids)
				.await?
		} else {
			LegacyVectorDocIds::Hnsw(ikb.clone()).get_record_ids_batch(tx, &miss_ids).await?
		};
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

	/// Evicts the cached doc-ID → record resolution for a removed document.
	///
	/// The record ↔ doc-ID mapping is shared across the table's indexes and is
	/// removed centrally at record purge (see `doc::index`'s `remove_doc_id`), so
	/// this only drops the process-local cache entry. Doc-IDs are never recycled.
	pub(super) async fn remove(&self, doc_id: DocId, table_id: TableId, cache: &VectorCache) {
		cache
			.remove_doc_id(Self::cache_index(&self.ikb, table_id, self.format_version), doc_id)
			.await;
	}
}

/// Where [`VecDocs::insert`] puts a vector the index does not hold yet, and
/// where it records that an element's document set changed.
///
/// The mapping rules are the same either way; only this differs. Applying in
/// place adds the element to the resident graph and evicts its cached set at
/// once. A staged batch allocates the element and holds the eviction back until
/// its transaction commits.
pub(super) trait ElementSink {
	/// Adds `vector` to the index as a new element and returns its id.
	async fn add_element(
		&mut self,
		ctx: &HnswContext<'_>,
		vector: Vector,
		ser_vec: &SerializedVector,
	) -> Result<ElementId>;

	/// Records that `element` carries the document set `docs` after this write.
	async fn record_doc_set(&mut self, vec_docs: &VecDocs, element: ElementId, docs: Ids64);
}

/// Adds elements straight to the resident graph, evicting their cached document
/// sets as it goes: the in-place apply.
pub(super) struct InPlace<'a>(pub(super) &'a mut HnswFlavor);

impl ElementSink for InPlace<'_> {
	async fn add_element(
		&mut self,
		ctx: &HnswContext<'_>,
		vector: Vector,
		_ser_vec: &SerializedVector,
	) -> Result<ElementId> {
		self.0.insert(ctx, vector).await
	}

	async fn record_doc_set(&mut self, vec_docs: &VecDocs, element: ElementId, _docs: Ids64) {
		vec_docs.remove_cached_doc_set(element).await;
	}
}

/// How [`VecDocs::get_docs_by_element`] uses the process-wide doc-set cache.
#[derive(Clone, Copy)]
enum DocSetRead {
	/// Every read goes to the transaction. This is what a writer needs, since a write
	/// transaction can observe uncommitted mappings.
	Uncached,
	/// Reads share cache entries read at `generation`, the compaction generation of the
	/// reader's snapshot. A compaction commits its doc-set changes together with a new
	/// generation, so a set read at one generation is exactly the set every snapshot at that
	/// generation sees, and no other.
	AtGeneration(Option<u64>),
}

/// Manages the mapping between vectors and document IDs in the HNSW index.
#[derive(Clone)]
pub(crate) struct VecDocs {
	ikb: IndexKeyBase,
	table_id: TableId,
	cache: VectorCache,
	use_hashed_vector: bool,
	format_version: u16,
	read: DocSetRead,
}

impl VecDocs {
	/// Creates a new `VecDocs` with the given index key base and hashing mode.
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		cache: VectorCache,
		use_hashed_vector: bool,
		format_version: u16,
	) -> Self {
		Self {
			ikb,
			table_id,
			cache,
			use_hashed_vector,
			format_version,
			read: DocSetRead::Uncached,
		}
	}

	/// A copy whose doc-set reads are scoped to `tx`'s snapshot, taken at compaction
	/// generation `generation`.
	///
	/// Only a read-only transaction shares the cache; a write transaction keeps reading its
	/// own view.
	pub(super) fn at_snapshot(&self, tx: &Transaction, generation: Option<u64>) -> Self {
		Self {
			read: if tx.writeable() {
				DocSetRead::Uncached
			} else {
				DocSetRead::AtGeneration(generation)
			},
			..self.clone()
		}
	}

	fn cache_index(&self) -> (NamespaceId, DatabaseId, TableId, IndexId, u16) {
		(self.ikb.ns(), self.ikb.db(), self.table_id, self.ikb.index(), self.format_version)
	}

	/// Evicts the cached doc set of an element whose set a compaction changes.
	pub(super) async fn remove_cached_doc_set(&self, element_id: ElementId) {
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
		if let Some(ehd) = tx.get_key(&key, None).await?
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
		if let Some(ed) = tx.get_key(&key, None).await? {
			return Ok(Some(ed.docs));
		}
		Ok(None)
	}

	/// Retrieves the document IDs of a graph element as `tx` sees them.
	///
	/// A compaction can add or remove a document of an element already in the graph without
	/// changing the index state, so a reader must never be served a set committed after its
	/// snapshot. Entries are shared only between reads at the same compaction generation (see
	/// [`DocSetRead`]); any other read comes from `tx`.
	pub(super) async fn get_docs_by_element(
		&self,
		tx: &Transaction,
		element_id: ElementId,
		pt: &Vector,
	) -> Result<Option<Ids64>> {
		let DocSetRead::AtGeneration(generation) = self.read else {
			return self.get_docs_uncached(tx, pt).await;
		};
		let index = self.cache_index();
		if let Some(docs) = self.cache.get_doc_set(index, element_id, generation).await {
			return Ok(Some(docs));
		}
		let docs = self.get_docs_uncached(tx, pt).await?;
		if let Some(docs) = docs.clone() {
			self.cache.insert_doc_set(index, element_id, generation, docs).await;
		}
		Ok(docs)
	}

	/// Inserts a vector and its associated document ID using its hash.
	async fn insert_hashed<K: ElementSink>(
		&self,
		ctx: &HnswContext<'_>,
		o: Vector,
		ser_vec: SerializedVector,
		doc_id: DocId,
		sink: &mut K,
	) -> Result<()> {
		let key = self.ikb.new_hh_key(ser_vec.compute_hash());
		match ctx.tx.get_key(&key, None).await? {
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = sink.add_element(ctx, o, &ser_vec).await?;
				let docs = Ids64::One(doc_id);
				let ehd = ElementHashedDocs::new(element_id, ser_vec, doc_id);
				ctx.tx.set_key(&key, &ehd).await?;
				sink.record_doc_set(self, element_id, docs).await;
			}
			Some(mut ehd) => {
				if let Some(ed) = ehd.get_element_docs(&ser_vec) {
					// We already have the vector
					if let Some(docs) = ed.docs.insert(doc_id) {
						ed.docs = docs;
						let element_id = ed.e_id;
						let docs = ed.docs.clone();
						ctx.tx.set_key(&key, &ehd).await?;
						sink.record_doc_set(self, element_id, docs).await;
					};
				} else {
					//  We don't have the vector, we insert it in the graph
					let element_id = sink.add_element(ctx, o, &ser_vec).await?;
					let docs = Ids64::One(doc_id);
					ehd.add(element_id, ser_vec, doc_id);
					ctx.tx.set_key(&key, &ehd).await?;
					sink.record_doc_set(self, element_id, docs).await;
				}
			}
		};
		Ok(())
	}

	/// Inserts a vector and its associated document ID.
	///
	/// A vector the index already holds — committed, or written earlier in the
	/// same transaction — gains the document; any other vector goes to `sink`
	/// as a new element.
	pub(super) async fn insert<K: ElementSink>(
		&self,
		ctx: &HnswContext<'_>,
		vec: Vector,
		doc_id: DocId,
		sink: &mut K,
	) -> Result<()> {
		let ser_vec = SerializedVector::from(&vec);
		if self.use_hashed_vector {
			return self.insert_hashed(ctx, vec, ser_vec, doc_id, sink).await;
		}
		let key = self.ikb.new_hv_key(&ser_vec);
		if let Some(ed) = match ctx.tx.get_key(&key, None).await? {
			Some(mut ed) => {
				// We already have the vector
				ed.docs.insert(doc_id).map(|new_docs| {
					ed.docs = new_docs;
					ed
				})
			}
			None => {
				//  We don't have the vector, we insert it in the graph
				let element_id = sink.add_element(ctx, vec, &ser_vec).await?;
				let ed = ElementDocs::new(element_id, doc_id);
				Some(ed)
			}
		} {
			ctx.tx.set_key(&key, &ed).await?;
			sink.record_doc_set(self, ed.e_id, ed.docs.clone()).await;
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
		if let Some(mut ehd) = ctx.tx.get_key(&key, None).await? {
			match ehd.remove(&ser_vec, d) {
				RemoveResult::Empty(deleted_element_id) => {
					ctx.tx.del_key(&key).await?;
					self.remove_cached_doc_set(deleted_element_id).await;
					h.remove(ctx, deleted_element_id).await?;
				}
				RemoveResult::Updated(element_id, _) => {
					ctx.tx.set_key(&key, &ehd).await?;
					self.remove_cached_doc_set(element_id).await;
				}
				RemoveResult::RemovedElement(deleted_element_id) => {
					ctx.tx.set_key(&key, &ehd).await?;
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
		if let Some(mut ed) = ctx.tx.get_key(&key, None).await?
			&& let Some(new_docs) = ed.docs.remove(d)
		{
			if new_docs.is_empty() {
				ctx.tx.del_key(&key).await?;
				self.remove_cached_doc_set(ed.e_id).await;
				h.remove(ctx, ed.e_id).await?;
			} else {
				ed.docs = new_docs;
				ctx.tx.set_key(&key, &ed).await?;
				self.remove_cached_doc_set(ed.e_id).await;
			}
		};
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use anyhow::Result;
	use surrealdb_kvs::TransactionType;

	use super::*;
	use crate::catalog::DOC_IDS_FORMAT_VERSION;
	use crate::key::schema::DocKeyKey;
	use crate::test_env::TestIndexStore;

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	#[tokio::test]
	async fn hnsw_docs_batch_preserves_order_and_uses_cache() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		{
			let ctx = ds.env(TransactionType::Write).await;
			LegacyVectorDocIds::Hnsw(ikb.clone())
				.resolve_or_assign(&ctx, &RecordIdKey::Number(11))
				.await?;
			LegacyVectorDocIds::Hnsw(ikb.clone())
				.resolve_or_assign(&ctx, &RecordIdKey::Number(22))
				.await?;
			ctx.tx().commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read).await?;
		let got = HnswDocs::get_things_batch(&ikb, 0, TableId(4), &cache, &tx, &[9, 0, 1], Some(5))
			.await?;
		assert!(got[0].is_none());
		assert_eq!(got[1].as_ref().map(|r| r.key.clone()), Some(RecordIdKey::Number(11)));
		assert_eq!(got[2].as_ref().map(|r| r.key.clone()), Some(RecordIdKey::Number(22)));
		tx.cancel().await?;

		let ctx = ds.env(TransactionType::Write).await;
		LegacyVectorDocIds::Hnsw(ikb.clone()).remove(&ctx.tx(), &RecordIdKey::Number(11)).await?;
		ctx.tx().commit().await?;
		HnswDocs::with_format_version(ikb.clone(), 0).remove(0, TableId(4), &cache).await;

		let tx = ds.transaction(TransactionType::Read).await?;
		let cached =
			HnswDocs::get_things_batch(&ikb, 0, TableId(4), &cache, &tx, &[0, 1], Some(5)).await?;
		assert!(cached[0].is_none());
		assert_eq!(cached[1].as_ref().unwrap().key, RecordIdKey::Number(22));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_batch_does_not_cache_missing_or_write_transaction_mappings() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);

		let tx = ds.transaction(TransactionType::Read).await?;
		assert_eq!(
			HnswDocs::get_things_batch(
				&ikb,
				DOC_IDS_FORMAT_VERSION,
				TableId(4),
				&cache,
				&tx,
				&[9],
				Some(5),
			)
			.await?,
			vec![None]
		);
		tx.cancel().await?;

		let ctx = ds.env(TransactionType::Write).await;
		LegacyVectorDocIds::Hnsw(ikb.clone())
			.resolve_or_assign(&ctx, &RecordIdKey::Number(99))
			.await?;
		ctx.tx().commit().await?;

		let ctx2 = ds.env(TransactionType::Write).await;
		let got = HnswDocs::get_things_batch(
			&ikb,
			0,
			TableId(4),
			&cache,
			ctx2.tx().as_ref(),
			&[9],
			Some(5),
		)
		.await?;
		let doc_id = LegacyVectorDocIds::Hnsw(ikb.clone())
			.get_doc_id(ctx2.tx().as_ref(), &RecordIdKey::Number(99))
			.await?
			.expect("mapping");
		assert_eq!(got.len(), 1);
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index(), 0u16), doc_id, Some(5))
				.await
				.is_none()
		);
		ctx2.tx().commit().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_batch_ignores_doc_id_cache_from_old_generation() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		{
			let tx = ds.transaction(TransactionType::Write).await?;
			tx.set_key(
				&DocKeyKey::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()), 1),
				&RecordIdKey::Number(11),
			)
			.await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read).await?;
		let got = HnswDocs::get_things_batch(
			&ikb,
			DOC_IDS_FORMAT_VERSION,
			TableId(4),
			&cache,
			&tx,
			&[1],
			Some(5),
		)
		.await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		tx.cancel().await?;
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index(), 0u16), 1, Some(6))
				.await
				.is_none()
		);

		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&DocKeyKey::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()), 1),
			&RecordIdKey::Number(22),
		)
		.await?;
		tx.commit().await?;

		let tx = ds.transaction(TransactionType::Read).await?;
		let got = HnswDocs::get_things_batch(
			&ikb,
			DOC_IDS_FORMAT_VERSION,
			TableId(4),
			&cache,
			&tx,
			&[1],
			Some(6),
		)
		.await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(22));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_docs_remove_evicts_doc_id_cache() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		{
			let ctx = ds.env(TransactionType::Write).await;
			LegacyVectorDocIds::Hnsw(ikb.clone())
				.resolve_or_assign(&ctx, &RecordIdKey::Number(11))
				.await?;
			ctx.tx().commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read).await?;
		let got =
			HnswDocs::get_things_batch(&ikb, 0, TableId(4), &cache, &tx, &[0], Some(5)).await?;
		assert_eq!(got[0].as_ref().unwrap().key, RecordIdKey::Number(11));
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index(), 0u16), 0, Some(5))
				.await
				.is_some()
		);
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write).await?;
		LegacyVectorDocIds::Hnsw(ikb.clone()).remove(&tx, &RecordIdKey::Number(11)).await?;
		HnswDocs::with_format_version(ikb.clone(), 0).remove(0, TableId(4), &cache).await;
		assert!(
			cache
				.get_doc_id((ikb.ns(), ikb.db(), TableId(4), ikb.index(), 0u16), 0, Some(5))
				.await
				.is_none()
		);
		tx.cancel().await?;
		Ok(())
	}

	fn cache_key() -> (NamespaceId, DatabaseId, TableId, IndexId, u16) {
		(NamespaceId(1), DatabaseId(2), TableId(4), IndexId(3), 0u16)
	}

	async fn write_mapping(
		ds: &TestIndexStore,
		ser_vec: &SerializedVector,
		docs: Ids64,
	) -> Result<()> {
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set_key(
			&ikb().new_hv_key(ser_vec),
			&ElementDocs {
				e_id: 7,
				docs,
			},
		)
		.await?;
		tx.commit().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_share_a_cached_set_only_within_a_generation() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb(), TableId(4), cache.clone(), false, 0);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let vector = Vector::from(ser_vec.clone());
		write_mapping(&ds, &ser_vec, Ids64::One(42)).await?;

		let tx = ds.transaction(TransactionType::Read).await?;
		let at_3 = vec_docs.at_snapshot(&tx, Some(3));
		assert_eq!(at_3.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		tx.cancel().await?;
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(3)).await, Some(Ids64::One(42)));
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(4)).await, None);
		assert_eq!(cache.get_doc_set(cache_key(), 7, None).await, None);

		// A compaction commits a new set together with a new generation.
		write_mapping(&ds, &ser_vec, Ids64::Vec2([42, 43])).await?;
		let tx = ds.transaction(TransactionType::Read).await?;
		// A reader still at the old generation shares the entry read there.
		assert_eq!(
			vec_docs.at_snapshot(&tx, Some(3)).get_docs_by_element(&tx, 7, &vector).await?,
			Some(Ids64::One(42))
		);
		// A reader at the new generation never sees it, and replaces it.
		let at_4 = vec_docs.at_snapshot(&tx, Some(4));
		assert_eq!(at_4.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::Vec2([42, 43])));
		tx.cancel().await?;
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(3)).await, None);
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(4)).await, Some(Ids64::Vec2([42, 43])));
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_writers_and_default_reads_bypass_the_cache() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb(), TableId(4), cache.clone(), false, 0);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let vector = Vector::from(ser_vec.clone());
		write_mapping(&ds, &ser_vec, Ids64::One(42)).await?;

		// A read-only transaction through the unscoped handle.
		let tx = ds.transaction(TransactionType::Read).await?;
		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		tx.cancel().await?;
		// A write transaction, whose own uncommitted mapping it must see.
		let tx = ds.transaction(TransactionType::Write).await?;
		let scoped = vec_docs.at_snapshot(&tx, Some(3));
		tx.set_key(
			&ikb().new_hv_key(&ser_vec),
			&ElementDocs {
				e_id: 7,
				docs: Ids64::Vec2([42, 43]),
			},
		)
		.await?;
		assert_eq!(scoped.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::Vec2([42, 43])));
		tx.cancel().await?;
		assert_eq!(cache.get_doc_set(cache_key(), 7, None).await, None);
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(3)).await, None);
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_hashed_disambiguates_by_vector() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let tx = ds.transaction(TransactionType::Write).await?;
		let ikb = ikb();
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb.clone(), TableId(4), cache.clone(), true, 0);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let other_vec = SerializedVector::F32(vec![3.0, 4.0]);
		let vector = Vector::from(ser_vec.clone());
		let key = ikb.new_hh_key(ser_vec.compute_hash());
		// Built through the bucket's own constructors, which is the only way in
		// now that it owns the one-entry-per-vector invariant. The colliding
		// vector is added first so the vector under test is not the first match.
		let mut bucket = ElementHashedDocs::new(8, other_vec, 88);
		bucket.add(7, ser_vec, 42);
		tx.set_key(&key, &bucket).await?;

		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, Some(Ids64::One(42)));
		tx.del_key(&key).await?;
		assert_eq!(vec_docs.get_docs_by_element(&tx, 7, &vector).await?, None);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn hnsw_vec_docs_missing_mapping_returns_none_without_caching() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let cache = VectorCache::new(1024 * 1024);
		let vec_docs = VecDocs::new(ikb(), TableId(4), cache.clone(), false, 0);
		let vector = Vector::from(SerializedVector::F32(vec![1.0, 2.0]));

		let tx = ds.transaction(TransactionType::Read).await?;
		let scoped = vec_docs.at_snapshot(&tx, Some(3));
		assert_eq!(scoped.get_docs_by_element(&tx, 7, &vector).await?, None);
		tx.cancel().await?;
		assert_eq!(cache.get_doc_set(cache_key(), 7, Some(3)).await, None);
		Ok(())
	}
}
