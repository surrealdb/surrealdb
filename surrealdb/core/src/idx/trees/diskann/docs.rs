//! DiskANN document and vector-document mapping helpers.
//!
//! DiskANN graph elements are keyed by compact internal element IDs, while query execution returns
//! SurrealDB record IDs. This module owns the KV mappings between record IDs, compact document IDs,
//! and graph element document sets. It also coordinates the process-local doc-id and doc-set caches
//! used to keep warm lookup paths away from repeated KV reads.

#[cfg(not(target_family = "wasm"))]
use std::sync::Arc;

use anyhow::Result;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use serde::{Deserialize, Serialize};

#[cfg(not(target_family = "wasm"))]
use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, TableDocIds};
#[cfg(not(target_family = "wasm"))]
use crate::idx::trees::diskann::cache::DiskAnnCache;
#[cfg(not(target_family = "wasm"))]
use crate::idx::trees::diskann::index::{DiskAnnContext, DiskAnnGraph};
use crate::idx::trees::diskann::{DiskAnnElement, ElementId};
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::vector::{SerializedVector, Vector};
use crate::key::KVValue;
use crate::kvs::Transaction;
use crate::val::{RecordId, RecordIdKey};

/// Per-index facade over the table's shared doc-ID space for DiskANN.
///
/// The record ↔ doc-ID mapping itself lives in [`TableDocIds`] (keys `!di`/`!dd`
/// under the table prefix), shared by every index on the table so cross-index
/// candidate composition can rely on one doc-ID per record. DiskANN keeps its own
/// element/graph keys; this type only maps between record IDs and those shared
/// doc-IDs (plus the process-local resolution cache).
pub(in crate::idx) struct DiskAnnDocs {
	/// Shared key builder for all DiskANN document mapping keys.
	ikb: IndexKeyBase,
	/// The table-level doc-ID mapping shared by every index on the table.
	docids: TableDocIds,
}

impl DiskAnnDocs {
	/// Creates a `DiskAnnDocs` facade over the table's shared doc-ID space.
	pub(in crate::idx) fn new(ikb: IndexKeyBase) -> Self {
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		Self {
			ikb,
			docids,
		}
	}

	/// Looks up the compact document ID for a record key without allocating a new one.
	pub(super) async fn get_doc_id(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<DocId>> {
		TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone()).get_doc_id(tx, id).await
	}

	/// Returns the existing document ID for a record key or allocates a new one in
	/// the table's shared doc-ID space.
	pub(super) async fn resolve(&self, ctx: &FrozenContext, id: &RecordIdKey) -> Result<DocId> {
		self.docids.resolve_or_assign(ctx, id).await
	}

	#[cfg(not(target_family = "wasm"))]
	fn cache_index(
		ikb: &IndexKeyBase,
		table_id: TableId,
	) -> (NamespaceId, DatabaseId, TableId, IndexId) {
		(ikb.ns(), ikb.db(), table_id, ikb.index())
	}

	/// Resolves one compact document ID through the shared batch/cache path.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn get_thing_cached(
		ikb: &IndexKeyBase,
		table_id: TableId,
		cache: &DiskAnnCache,
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
	/// Positive mappings are inserted into the process-local cache only when the transaction is
	/// read-only. Write transactions may observe uncommitted `!dd` values, so those hits stay local
	/// to the caller and are not published into the shared cache.
	// NOTE (#7318 review followup, P3): each result here pays two `Strand`/`RecordIdKey`
	// clones (`table.clone()` + `id.as_ref().clone()`) to assemble the `RecordId`. Per result,
	// not per query, so KNN with `k` neighbours pays `2k` clones. `Strand` clones are cheap for
	// the typical inline/static variants and a heap copy only for long table names. The "real"
	// fix would make `RecordId` hold `Arc<RecordIdKey>` / `Arc<Table>` so the per-result cost
	// becomes a pair of ref-count bumps — that's a workspace-wide refactor (every consumer of
	// `RecordId` would change), so it's deliberately scoped out of this PR.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn get_things_batch(
		ikb: &IndexKeyBase,
		table_id: TableId,
		cache: &DiskAnnCache,
		tx: &Transaction,
		doc_ids: &[DocId],
		generation: Option<u64>,
	) -> Result<Vec<Option<Arc<RecordId>>>> {
		let index = Self::cache_index(ikb, table_id);
		let table = ikb.table().clone();
		let mut rids = vec![None; doc_ids.len()];
		let mut misses = Vec::new();
		for (pos, doc_id) in doc_ids.iter().copied().enumerate() {
			if let Some(id) = cache.get_doc_id(index, doc_id, generation) {
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
		let ids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
			.get_record_ids_batch(tx, &miss_ids)
			.await?;
		let cache_misses = !tx.writeable();
		for ((pos, doc_id), id) in misses.into_iter().zip(ids) {
			if let Some(id) = id {
				let id = if cache_misses {
					cache.insert_doc_id(index, doc_id, generation, id)
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
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn remove(&self, doc_id: DocId, table_id: TableId, cache: &DiskAnnCache) {
		cache.remove_doc_id(Self::cache_index(&self.ikb, table_id), doc_id);
	}

	#[cfg(target_family = "wasm")]
	pub(super) fn remove(&self, _doc_id: DocId) {}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{Datastore, LockType, TransactionType};

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	fn cache_index() -> (NamespaceId, DatabaseId, TableId, IndexId) {
		(NamespaceId(1), DatabaseId(2), TableId(4), IndexId(3))
	}

	#[tokio::test]
	async fn diskann_docs_batch_populates_and_uses_doc_id_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set_key(
				&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 1),
				&RecordIdKey::Number(11),
			)
			.await?;
			tx.set_key(
				&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 3),
				&RecordIdKey::Number(33),
			)
			.await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got = DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1, 2, 3], Some(5))
			.await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		assert!(got[1].is_none());
		assert_eq!(&got[2].as_ref().unwrap().key, &RecordIdKey::Number(33));
		assert_eq!(
			cache.get_doc_id(cache_index(), 1, Some(5)).unwrap().as_ref(),
			&RecordIdKey::Number(11)
		);
		assert!(cache.get_doc_id(cache_index(), 2, Some(5)).is_none());
		assert_eq!(
			cache.get_doc_id(cache_index(), 3, Some(5)).unwrap().as_ref(),
			&RecordIdKey::Number(33)
		);
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.del_key(&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 1)).await?;
		tx.commit().await?;
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let missing: Option<RecordIdKey> = tx
			.get_key(&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 1), None)
			.await?;
		assert!(missing.is_none());
		let cached =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(5)).await?;
		assert_eq!(&cached[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_docs_batch_does_not_cache_write_transaction_mappings() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		tx.set_key(
			&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 9),
			&RecordIdKey::Number(99),
		)
		.await?;

		let got =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[9], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(99));
		assert!(cache.get_doc_id(cache_index(), 9, Some(5)).is_none());
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		assert_eq!(
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[9], Some(5)).await?,
			vec![None]
		);
		assert!(cache.get_doc_id(cache_index(), 9, Some(5)).is_none());
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_docs_batch_ignores_doc_id_cache_from_old_generation() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set_key(
				&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 1),
				&RecordIdKey::Number(11),
			)
			.await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(11));
		tx.cancel().await?;
		assert!(cache.get_doc_id(cache_index(), 1, Some(6)).is_none());

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.set_key(
			&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 1),
			&RecordIdKey::Number(22),
		)
		.await?;
		tx.commit().await?;

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[1], Some(6)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &RecordIdKey::Number(22));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_docs_remove_evicts_doc_id_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let id = RecordIdKey::Number(77);
		{
			let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
			tx.set_key(&crate::key::table::dd::Dd::new(ikb.ns(), ikb.db(), ikb.table(), 7), &id)
				.await?;
			tx.commit().await?;
		}

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let got =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[7], Some(5)).await?;
		assert_eq!(&got[0].as_ref().unwrap().key, &id);
		assert!(cache.get_doc_id(cache_index(), 7, Some(5)).is_some());
		tx.cancel().await?;

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let docs = DiskAnnDocs::new(ikb.clone());
		// `remove` is cache-only now: it evicts the process-local doc-id cache and never
		// recycles ids (the shared record↔doc-id mapping is deleted centrally at record
		// purge), so the KV mapping survives.
		docs.remove(7, TableId(4), &cache);
		assert!(cache.get_doc_id(cache_index(), 7, Some(5)).is_none());
		// The mapping is still in KV, so a fresh lookup re-resolves it and re-warms the cache.
		let reloaded =
			DiskAnnDocs::get_things_batch(&ikb, TableId(4), &cache, &tx, &[7], Some(5)).await?;
		assert_eq!(&reloaded[0].as_ref().unwrap().key, &id);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_populates_and_uses_doc_set_cache() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb.clone(), TableId(4), cache.clone(), false);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let vector = Vector::from(ser_vec.clone());
		tx.set_key(
			&ikb.new_dq_key(&ser_vec),
			&DiskAnnElementDocs {
				e_id: 7,
				docs: Ids64::One(42),
			},
		)
		.await?;

		assert_eq!(
			vec_docs.get_docs_batch(&tx, &[(7, &vector)]).await?,
			vec![Some(Ids64::One(42))]
		);
		assert_eq!(cache.get_doc_set(cache_index(), 7), Some(Ids64::One(42)));

		tx.del_key(&ikb.new_dq_key(&ser_vec)).await?;
		assert_eq!(
			vec_docs.get_docs_batch(&tx, &[(7, &vector)]).await?,
			vec![Some(Ids64::One(42))]
		);
		assert_eq!(vec_docs.get_docs_batch_uncached(&tx, &[(7, &vector)]).await?, vec![None]);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_resolves_warmed_element_docs_without_vector_mapping() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb.clone(), TableId(4), cache.clone(), false);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		tx.set_key(
			&ikb.new_de_key(7),
			&DiskAnnElement {
				vector: ser_vec.clone(),
				deleted: false,
			},
		)
		.await?;
		tx.set_key(
			&ikb.new_dq_key(&ser_vec),
			&DiskAnnElementDocs {
				e_id: 7,
				docs: Ids64::One(42),
			},
		)
		.await?;

		assert_eq!(
			vec_docs.get_docs_by_element_batch(&tx, &[(7, 0.5)]).await?,
			vec![(7, 0.5, Some(Ids64::One(42)))]
		);
		assert_eq!(cache.get_doc_set(cache_index(), 7), Some(Ids64::One(42)));

		tx.del_key(&ikb.new_de_key(7)).await?;
		tx.del_key(&ikb.new_dq_key(&ser_vec)).await?;
		assert_eq!(
			vec_docs.get_docs_by_element_batch(&tx, &[(7, 0.5)]).await?,
			vec![(7, 0.5, Some(Ids64::One(42)))]
		);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_resolves_element_doc_cache_misses_in_order() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb.clone(), TableId(4), cache.clone(), false);
		let first_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let second_vec = SerializedVector::F32(vec![3.0, 4.0]);
		for (element_id, ser_vec, docs) in
			[(7, first_vec.clone(), Ids64::One(70)), (9, second_vec.clone(), Ids64::One(90))]
		{
			tx.set_key(
				&ikb.new_de_key(element_id),
				&DiskAnnElement {
					vector: ser_vec.clone(),
					deleted: false,
				},
			)
			.await?;
			tx.set_key(
				&ikb.new_dq_key(&ser_vec),
				&DiskAnnElementDocs {
					e_id: element_id,
					docs,
				},
			)
			.await?;
		}

		assert_eq!(
			vec_docs.get_docs_by_element_batch(&tx, &[(9, 0.9), (8, 0.8), (7, 0.7)]).await?,
			vec![(9, 0.9, Some(Ids64::One(90))), (8, 0.8, None), (7, 0.7, Some(Ids64::One(70)))]
		);
		assert_eq!(cache.get_doc_set(cache_index(), 9), Some(Ids64::One(90)));
		assert_eq!(cache.get_doc_set(cache_index(), 7), Some(Ids64::One(70)));
		assert!(cache.get_doc_set(cache_index(), 8).is_none());
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_caches_hashed_docs_after_disambiguating_vector() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb.clone(), TableId(4), cache.clone(), true);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let other_vec = SerializedVector::F32(vec![9.0, 9.0]);
		let vector = Vector::from(ser_vec.clone());
		tx.set_key(
			&ikb.new_dh_key(ser_vec.compute_hash()),
			&DiskAnnElementHashedDocs {
				vectors: vec![
					(
						other_vec,
						DiskAnnElementDocs {
							e_id: 5,
							docs: Ids64::One(5),
						},
					),
					(
						ser_vec.clone(),
						DiskAnnElementDocs {
							e_id: 9,
							docs: Ids64::One(42),
						},
					),
				],
			},
		)
		.await?;

		assert_eq!(
			vec_docs.get_docs_batch(&tx, &[(9, &vector)]).await?,
			vec![Some(Ids64::One(42))]
		);
		assert_eq!(cache.get_doc_set(cache_index(), 9), Some(Ids64::One(42)));

		tx.del_key(&ikb.new_dh_key(ser_vec.compute_hash())).await?;
		assert_eq!(
			vec_docs.get_docs_batch(&tx, &[(9, &vector)]).await?,
			vec![Some(Ids64::One(42))]
		);
		assert_eq!(vec_docs.get_docs_batch_uncached(&tx, &[(9, &vector)]).await?, vec![None]);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_resolves_hashed_element_doc_cache_misses() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb.clone(), TableId(4), cache.clone(), true);
		let ser_vec = SerializedVector::F32(vec![1.0, 2.0]);
		let other_vec = SerializedVector::F32(vec![9.0, 9.0]);
		tx.set_key(
			&ikb.new_de_key(9),
			&DiskAnnElement {
				vector: ser_vec.clone(),
				deleted: false,
			},
		)
		.await?;
		tx.set_key(
			&ikb.new_dh_key(ser_vec.compute_hash()),
			&DiskAnnElementHashedDocs {
				vectors: vec![
					(
						other_vec,
						DiskAnnElementDocs {
							e_id: 5,
							docs: Ids64::One(5),
						},
					),
					(
						ser_vec,
						DiskAnnElementDocs {
							e_id: 9,
							docs: Ids64::One(42),
						},
					),
				],
			},
		)
		.await?;

		assert_eq!(
			vec_docs.get_docs_by_element_batch(&tx, &[(9, 0.9)]).await?,
			vec![(9, 0.9, Some(Ids64::One(42)))]
		);
		assert_eq!(cache.get_doc_set(cache_index(), 9), Some(Ids64::One(42)));
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_vec_docs_missing_element_doc_candidate_returns_none() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let ikb = ikb();
		let cache = DiskAnnCache::new(1024 * 1024);
		let vec_docs = DiskAnnVecDocs::new(ikb, TableId(4), cache.clone(), false);

		assert_eq!(
			vec_docs.get_docs_by_element_batch(&tx, &[(99, 0.99)]).await?,
			vec![(99, 0.99, None)]
		);
		assert!(cache.get_doc_set(cache_index(), 99).is_none());
		tx.cancel().await?;
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(crate) struct DiskAnnElementDocs {
	/// Graph element ID that owns this exact vector.
	e_id: ElementId,
	/// Compact document IDs currently sharing the vector.
	docs: Ids64,
}

impl DiskAnnElementDocs {
	fn new(element_id: ElementId, d: DocId) -> Self {
		Self {
			e_id: element_id,
			docs: Ids64::One(d),
		}
	}
}

/// Soft cap on hashed-vector collision-bucket size. Real-world hash collisions across
/// distinct full-fidelity vectors are vanishingly rare; if a bucket grows past this size we
/// emit a `warn!` because every lookup of the bucket is O(bucket-size) full-vector compares
/// (see [`DiskAnnElementHashedDocs::get_docs`]) and an adversarial or buggy hash distribution
/// would otherwise silently scale the cost of every KNN search.
const HASHED_BUCKET_WARN_THRESHOLD: usize = 16;

#[revisioned(revision = 1)]
pub(crate) struct DiskAnnElementHashedDocs {
	/// Collision bucket keyed by vector hash; each entry retains the full vector for
	/// disambiguation.
	vectors: Vec<(SerializedVector, DiskAnnElementDocs)>,
}

/// Result of removing one document ID from a hashed vector collision bucket.
enum RemoveResult {
	/// The whole hash bucket became empty and its graph element should be removed.
	Empty(ElementId),
	/// The doc set of one bucket entry shrank, but the entry (and its graph element)
	/// still has other docs sharing it. Caller must evict the cached doc set.
	BucketShrunk {
		e_id: ElementId,
	},
	/// One entry in the bucket was removed entirely (its last doc went away). The
	/// graph element must be removed from the upstream graph; the rest of the bucket
	/// is intact and must be persisted back to KV.
	EntryRemoved {
		e_id: ElementId,
	},
	/// The requested vector/document pair was not present.
	Unchanged,
}

impl DiskAnnElementHashedDocs {
	fn new(element_id: ElementId, vec: SerializedVector, doc_id: DocId) -> Self {
		Self {
			vectors: vec![(vec, DiskAnnElementDocs::new(element_id, doc_id))],
		}
	}

	fn get_element_docs(&mut self, vec: &SerializedVector) -> Option<&mut DiskAnnElementDocs> {
		self.vectors.iter_mut().find_map(|(vector, ed)| {
			if *vec == *vector {
				Some(ed)
			} else {
				None
			}
		})
	}

	fn get_docs(self, vec: &SerializedVector) -> Option<(ElementId, Ids64)> {
		for (vector, ed) in self.vectors {
			if vector == *vec {
				return Some((ed.e_id, ed.docs));
			}
		}
		None
	}

	fn add(&mut self, element_id: ElementId, vec: SerializedVector, doc_id: DocId) {
		self.vectors.push((vec, DiskAnnElementDocs::new(element_id, doc_id)));
		// Real-world vector-hash collisions across distinct full-fidelity vectors are
		// vanishingly rare; warn loudly if we ever cross the soft cap so an unexpected hash
		// distribution doesn't silently scale KNN search by bucket size. Fire on every
		// power-of-two crossing at or above the threshold (16, 32, 64, …) so an operator
		// sees runaway growth, not just the first crossing.
		let len = self.vectors.len();
		if len >= HASHED_BUCKET_WARN_THRESHOLD && len.is_power_of_two() {
			warn!(
				bucket_size = len,
				new_element_id = element_id,
				"DiskANN hashed-vector collision bucket exceeded soft warn threshold; \
				 every lookup of this hash now does {len} full-vector compares",
			);
		}
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
				let e_id = ed.e_id;
				ed.docs = new_docs;
				return RemoveResult::BucketShrunk {
					e_id,
				};
			}
		}
		if let Some((i, e_id)) = action {
			self.vectors.remove(i);
			if self.vectors.is_empty() {
				return RemoveResult::Empty(e_id);
			}
			return RemoveResult::EntryRemoved {
				e_id,
			};
		}
		RemoveResult::Unchanged
	}
}

impl KVValue for DiskAnnElementHashedDocs {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

impl KVValue for DiskAnnElementDocs {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut bytes: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes)?)
	}
}

/// Manages vector-to-document mappings in the DiskANN index.
#[cfg(not(target_family = "wasm"))]
pub(in crate::idx) struct DiskAnnVecDocs {
	/// Shared key builder for `!dq` and `!dh` mapping keys.
	ikb: IndexKeyBase,
	/// Stable table id used to scope process-local cache entries.
	table_id: TableId,
	/// Process-local doc-set cache keyed by graph element ID.
	cache: DiskAnnCache,
	/// Whether vector mappings are stored by vector hash (`!dh`) instead of full vector key
	/// (`!dq`).
	use_hashed_vector: bool,
}

#[cfg(not(target_family = "wasm"))]
impl DiskAnnVecDocs {
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		cache: DiskAnnCache,
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

	fn evict_cached_doc_set(&self, element_id: ElementId) {
		self.cache.remove_doc_set(self.cache_index(), element_id);
	}

	/// Returns document id sets for multiple candidate vectors in input order.
	///
	/// Search already has graph element IDs and candidate vectors at this point. Warm element-doc
	/// cache hits avoid vector-key encoding and KV reads entirely; misses still batch through the
	/// persisted vector-doc keys so KV remains the source of truth.
	pub(super) async fn get_docs_batch(
		&self,
		tx: &Transaction,
		candidates: &[(ElementId, &Vector)],
	) -> Result<Vec<Option<Ids64>>> {
		let index = self.cache_index();
		let mut doc_sets = vec![None; candidates.len()];
		let mut misses = Vec::new();
		for (pos, (element_id, vector)) in candidates.iter().enumerate() {
			if let Some(docs) = self.cache.get_doc_set(index, *element_id) {
				doc_sets[pos] = Some(docs);
			} else {
				misses.push((pos, *element_id, SerializedVector::from(*vector)));
			}
		}
		if misses.is_empty() {
			return Ok(doc_sets);
		}
		if self.use_hashed_vector {
			let keys: Vec<_> = misses
				.iter()
				.map(|(_, _, ser_vec)| self.ikb.new_dh_key(ser_vec.compute_hash()))
				.collect();
			let docs: Vec<Option<DiskAnnElementHashedDocs>> = tx.get_many_key(keys, None).await?;
			for ((pos, _, ser_vec), docs) in misses.into_iter().zip(docs) {
				if let Some((element_id, docs)) = docs.and_then(|docs| docs.get_docs(&ser_vec)) {
					self.cache.insert_doc_set(index, element_id, docs.clone());
					doc_sets[pos] = Some(docs);
				}
			}
			return Ok(doc_sets);
		}
		let keys: Vec<_> =
			misses.iter().map(|(_, _, ser_vec)| self.ikb.new_dq_key(ser_vec)).collect();
		let docs: Vec<Option<DiskAnnElementDocs>> = tx.get_many_key(keys, None).await?;
		for ((pos, _, _), docs) in misses.into_iter().zip(docs) {
			if let Some(docs) = docs {
				self.cache.insert_doc_set(index, docs.e_id, docs.docs.clone());
				doc_sets[pos] = Some(docs.docs);
			}
		}
		Ok(doc_sets)
	}

	/// Resolves graph candidates to document id sets in input order.
	///
	/// Candidate vectors are only read for element IDs whose doc set is not already cached. This
	/// keeps warmed lookup materialization on the element-doc cache path and avoids re-reading
	/// graph vectors after search has already produced scored element IDs.
	pub(super) async fn get_docs_by_element_batch(
		&self,
		tx: &Transaction,
		candidates: &[(ElementId, f64)],
	) -> Result<Vec<(ElementId, f64, Option<Ids64>)>> {
		let index = self.cache_index();
		let mut resolved: Vec<_> = candidates
			.iter()
			.map(|(element_id, distance)| (*element_id, *distance, None))
			.collect();
		let mut element_misses = Vec::new();
		let mut element_keys = Vec::new();
		for (pos, (element_id, _)) in candidates.iter().copied().enumerate() {
			if let Some(docs) = self.cache.get_doc_set(index, element_id) {
				resolved[pos].2 = Some(docs);
			} else if let Some(element) = self.cache.get_element(index, element_id) {
				if !element.deleted {
					element_misses.push((
						pos,
						element_id,
						Some(Vector::from(element.vector.clone())),
					));
				}
			} else {
				element_misses.push((pos, element_id, None));
				element_keys.push(self.ikb.new_de_key(element_id));
			}
		}
		if !element_keys.is_empty() {
			let elements: Vec<Option<DiskAnnElement>> = tx.get_many_key(element_keys, None).await?;
			let mut fetched = elements.into_iter();
			for (_, element_id, vector) in &mut element_misses {
				if vector.is_some() {
					continue;
				}
				let Some(element) = fetched.next().flatten() else {
					continue;
				};
				let element = self.cache.insert_element(index, *element_id, element);
				if !element.deleted {
					*vector = Some(Vector::from(element.vector.clone()));
				}
			}
		}
		element_misses.retain(|(_, _, vector)| vector.is_some());
		if element_misses.is_empty() {
			return Ok(resolved);
		}
		let doc_candidates: Vec<_> = element_misses
			.iter()
			.filter_map(|(_, element_id, vector)| {
				vector.as_ref().map(|vector| (*element_id, vector))
			})
			.collect();
		let docs = self.get_docs_batch(tx, &doc_candidates).await?;
		for ((pos, _, _), docs) in element_misses.into_iter().zip(docs) {
			resolved[pos].2 = docs;
		}
		Ok(resolved)
	}

	#[cfg(test)]
	async fn get_docs_batch_uncached(
		&self,
		tx: &Transaction,
		candidates: &[(ElementId, &Vector)],
	) -> Result<Vec<Option<Ids64>>> {
		let ser_vecs: Vec<_> =
			candidates.iter().map(|(_, vector)| SerializedVector::from(*vector)).collect();
		if self.use_hashed_vector {
			let keys: Vec<_> = ser_vecs
				.iter()
				.map(|ser_vec| self.ikb.new_dh_key(ser_vec.compute_hash()))
				.collect();
			let docs: Vec<Option<DiskAnnElementHashedDocs>> = tx.get_many_key(keys, None).await?;
			return Ok(ser_vecs
				.into_iter()
				.zip(docs)
				.map(|(ser_vec, docs)| {
					docs.and_then(|docs| docs.get_docs(&ser_vec).map(|(_, d)| d))
				})
				.collect());
		}
		let keys: Vec<_> = ser_vecs.iter().map(|ser_vec| self.ikb.new_dq_key(ser_vec)).collect();
		let docs: Vec<Option<DiskAnnElementDocs>> = tx.get_many_key(keys, None).await?;
		Ok(docs.into_iter().map(|docs| docs.map(|docs| docs.docs)).collect())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn insert_hashed(
		&self,
		ctx: &DiskAnnContext<'_>,
		graph: &mut DiskAnnGraph,
		vec: Vector,
		ser_vec: SerializedVector,
		doc_id: DocId,
	) -> Result<()> {
		let key = self.ikb.new_dh_key(ser_vec.compute_hash());
		match ctx.tx.get_key(&key, None).await? {
			None => {
				let element_id = graph.insert(ctx, vec).await?;
				ctx.tx
					.set_key(&key, &DiskAnnElementHashedDocs::new(element_id, ser_vec, doc_id))
					.await?;
				self.evict_cached_doc_set(element_id);
			}
			Some(mut ehd) => {
				if let Some(ed) = ehd.get_element_docs(&ser_vec) {
					if let Some(docs) = ed.docs.insert(doc_id) {
						let element_id = ed.e_id;
						ed.docs = docs;
						ctx.tx.set_key(&key, &ehd).await?;
						self.evict_cached_doc_set(element_id);
					}
				} else {
					let element_id = graph.insert(ctx, vec).await?;
					ehd.add(element_id, ser_vec, doc_id);
					ctx.tx.set_key(&key, &ehd).await?;
					self.evict_cached_doc_set(element_id);
				}
			}
		}
		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn insert(
		&self,
		ctx: &DiskAnnContext<'_>,
		vec: Vector,
		doc_id: DocId,
		graph: &mut DiskAnnGraph,
	) -> Result<()> {
		let ser_vec = SerializedVector::from(&vec);
		if self.use_hashed_vector {
			return self.insert_hashed(ctx, graph, vec, ser_vec, doc_id).await;
		}
		let key = self.ikb.new_dq_key(&ser_vec);
		if let Some(ed) = match ctx.tx.get_key(&key, None).await? {
			Some(mut ed) => ed.docs.insert(doc_id).map(|new_docs| {
				ed.docs = new_docs;
				ed
			}),
			None => {
				let element_id = graph.insert(ctx, vec).await?;
				self.evict_cached_doc_set(element_id);
				Some(DiskAnnElementDocs::new(element_id, doc_id))
			}
		} {
			self.evict_cached_doc_set(ed.e_id);
			ctx.tx.set_key(&key, &ed).await?;
		}
		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn remove_hashed(
		&self,
		ctx: &DiskAnnContext<'_>,
		graph: &mut DiskAnnGraph,
		ser_vec: SerializedVector,
		doc_id: DocId,
	) -> Result<()> {
		let key = self.ikb.new_dh_key(ser_vec.compute_hash());
		if let Some(mut ehd) = ctx.tx.get_key(&key, None).await? {
			match ehd.remove(&ser_vec, doc_id) {
				RemoveResult::Empty(e_id) => {
					ctx.tx.del_key(&key).await?;
					self.evict_cached_doc_set(e_id);
					graph.remove(ctx, e_id).await?;
				}
				RemoveResult::BucketShrunk {
					e_id,
				} => {
					ctx.tx.set_key(&key, &ehd).await?;
					self.evict_cached_doc_set(e_id);
				}
				RemoveResult::EntryRemoved {
					e_id,
				} => {
					ctx.tx.set_key(&key, &ehd).await?;
					self.evict_cached_doc_set(e_id);
					graph.remove(ctx, e_id).await?;
				}
				RemoveResult::Unchanged => {}
			}
		}
		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn remove(
		&self,
		ctx: &DiskAnnContext<'_>,
		vec: &Vector,
		doc_id: DocId,
		graph: &mut DiskAnnGraph,
	) -> Result<()> {
		let ser_vec = vec.into();
		if self.use_hashed_vector {
			return self.remove_hashed(ctx, graph, ser_vec, doc_id).await;
		}
		let key = self.ikb.new_dq_key(&ser_vec);
		if let Some(mut ed) = ctx.tx.get_key(&key, None).await?
			&& let Some(new_docs) = ed.docs.remove(doc_id)
		{
			if new_docs.is_empty() {
				ctx.tx.del_key(&key).await?;
				self.evict_cached_doc_set(ed.e_id);
				graph.remove(ctx, ed.e_id).await?;
			} else {
				ed.docs = new_docs;
				ctx.tx.set_key(&key, &ed).await?;
				self.evict_cached_doc_set(ed.e_id);
			}
		}
		Ok(())
	}
}
