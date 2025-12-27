pub(crate) mod hnsw;
mod mapper;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::{Rng, thread_rng};
use tokio::time::sleep;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, HnswParams, Index, IndexDefinition, NamespaceId, TableDefinition, TableId,
};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::store::hnsw::{HnswIndexes, SharedHnswIndex};
use crate::idx::trees::store::mapper::Mappers;
use crate::kvs::Transaction;
use crate::kvs::TransactionFactory;
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::{LockType, TransactionType};
use crate::val::RecordIdKey;
use crate::val::Value;

#[derive(Clone)]
pub struct IndexStores(Arc<Inner>);

struct Inner {
	hnsw_indexes: HnswIndexes,
	mappers: Mappers,
	vector_cache: VectorCache,
	transaction_factory: Option<TransactionFactory>,
	sequences: Option<Sequences>,
}

impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			vector_cache: VectorCache::default(),
			transaction_factory: None,
			sequences: None,
		}))
	}
}

impl IndexStores {
	/// Creates a new IndexStores with transaction factory for retry logic
	pub(crate) fn new(tf: TransactionFactory, sequences: Sequences) -> Self {
		Self(Arc::new(Inner {
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			vector_cache: VectorCache::default(),
			transaction_factory: Some(tf),
			sequences: Some(sequences),
		}))
	}

	/// Index a document in HNSW with retry logic for transaction conflicts.
	///
	/// This method handles transaction conflicts by retrying with exponential backoff,
	/// similar to how sequences handle batch allocation conflicts.
	pub(crate) async fn index_hnsw_document_with_retry(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ctx: &FrozenContext,
		tb: TableId,
		ix: &IndexDefinition,
		p: &HnswParams,
		id: &RecordIdKey,
		content: &[Value],
	) -> Result<()> {
		// If we don't have transaction factory, fall back to non-retry behavior
		let (tf, sequences) = match (&self.0.transaction_factory, &self.0.sequences) {
			(Some(tf), Some(seq)) => (tf, seq),
			_ => {
				let hnsw = self.get_index_hnsw(ns, db, ctx, tb, ix, p).await?;
				let txn = ctx.tx();
				let mut hnsw = hnsw.write().await;
				return hnsw.index_document(&txn, id, content).await;
			}
		};

		// Retry with exponential backoff
		let mut tempo = 4u64;
		const MAX_BACKOFF: u64 = 32_768;
		const MAX_RETRIES: u32 = 50;
		let mut retries = 0u32;

		loop {
			let tx = tf
				.transaction(TransactionType::Write, LockType::Optimistic, sequences.clone())
				.await?;

			// Get the HNSW index and perform operation in a scope to release lock before sleep
			let result = {
				let hnsw = self.get_index_hnsw(ns, db, ctx, tb, ix, p).await?;
				let mut hnsw = hnsw.write().await;
				hnsw.index_document(&tx, id, content).await
			};

			match result {
				Ok(_) => {
					// Try to commit
					match tx.commit().await {
						Ok(_) => return Ok(()),
						Err(e) => {
							let err_str = e.to_string();
							if err_str.contains("Resource busy")
								|| err_str.contains("TryAgain")
								|| err_str.contains("transaction conflict")
							{
								// Retry with backoff (lock already released)
								retries += 1;
								if retries >= MAX_RETRIES {
									return Err(e);
								}
								let sleep_ms = thread_rng().gen_range(1..=tempo);
								sleep(Duration::from_millis(sleep_ms)).await;
								if tempo < MAX_BACKOFF {
									tempo *= 2;
								}
								continue;
							}
							return Err(e);
						}
					}
				}
				Err(e) => {
					let _ = tx.cancel().await;
					let err_str = e.to_string();
					if err_str.contains("Resource busy")
						|| err_str.contains("TryAgain")
						|| err_str.contains("transaction conflict")
					{
						// Retry with backoff (lock already released)
						retries += 1;
						if retries >= MAX_RETRIES {
							return Err(e);
						}
						let sleep_ms = thread_rng().gen_range(1..=tempo);
						sleep(Duration::from_millis(sleep_ms)).await;
						if tempo < MAX_BACKOFF {
							tempo *= 2;
						}
						continue;
					}
					return Err(e);
				}
			}
		}
	}

	/// Remove a document from HNSW with retry logic for transaction conflicts.
	pub(crate) async fn remove_hnsw_document_with_retry(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ctx: &FrozenContext,
		tb: TableId,
		ix: &IndexDefinition,
		p: &HnswParams,
		id: RecordIdKey,
		content: &[Value],
	) -> Result<()> {
		// If we don't have transaction factory, fall back to non-retry behavior
		let (tf, sequences) = match (&self.0.transaction_factory, &self.0.sequences) {
			(Some(tf), Some(seq)) => (tf, seq),
			_ => {
				// Fallback: use the provided transaction without retry
				let hnsw = self.get_index_hnsw(ns, db, ctx, tb, ix, p).await?;
				let txn = ctx.tx();
				let mut hnsw = hnsw.write().await;
				return hnsw.remove_document(&txn, id, content).await;
			}
		};

		let mut tempo = 4u64;
		const MAX_BACKOFF: u64 = 32_768;
		const MAX_RETRIES: u32 = 50;
		let mut retries = 0u32;

		loop {
			let tx = tf
				.transaction(TransactionType::Write, LockType::Optimistic, sequences.clone())
				.await?;

			// Get the HNSW index and perform operation in a scope to release lock before sleep
			let result = {
				let hnsw = self.get_index_hnsw(ns, db, ctx, tb, ix, p).await?;
				let mut hnsw = hnsw.write().await;
				hnsw.remove_document(&tx, id.clone(), content).await
			};

			match result {
				Ok(_) => {
					// Try to commit
					match tx.commit().await {
						Ok(_) => return Ok(()),
						Err(e) => {
							let err_str = e.to_string();
							if err_str.contains("Resource busy")
								|| err_str.contains("TryAgain")
								|| err_str.contains("transaction conflict")
							{
								// Retry with backoff (lock already released)
								retries += 1;
								if retries >= MAX_RETRIES {
									return Err(e);
								}
								let sleep_ms = thread_rng().gen_range(1..=tempo);
								sleep(Duration::from_millis(sleep_ms)).await;
								if tempo < MAX_BACKOFF {
									tempo *= 2;
								}
								continue;
							}
							return Err(e);
						}
					}
				}
				Err(e) => {
					let _ = tx.cancel().await;
					let err_str = e.to_string();
					if err_str.contains("Resource busy")
						|| err_str.contains("TryAgain")
						|| err_str.contains("transaction conflict")
					{
						// Retry with backoff (lock already released)
						retries += 1;
						if retries >= MAX_RETRIES {
							return Err(e);
						}
						let sleep_ms = thread_rng().gen_range(1..=tempo);
						sleep(Duration::from_millis(sleep_ms)).await;
						if tempo < MAX_BACKOFF {
							tempo *= 2;
						}
						continue;
					}
					return Err(e);
				}
			}
		}
	}

	pub(crate) async fn get_index_hnsw(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ctx: &FrozenContext,
		tb: TableId,
		ix: &IndexDefinition,
		p: &HnswParams,
	) -> Result<SharedHnswIndex> {
		let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
		self.0.hnsw_indexes.get(ctx, tb, &ikb, p).await
	}

	pub(crate) async fn index_removed(
		&self,
		ib: Option<&IndexBuilder>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableDefinition,
		ix: &IndexDefinition,
	) -> Result<()> {
		if let Some(ib) = ib {
			ib.remove_index(ns, db, &tb.name, ix.index_id).await?;
		}
		self.remove_index(ns, db, tb.table_id, ix).await
	}

	pub(crate) async fn namespace_removed(
		&self,
		ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: NamespaceId,
	) -> Result<()> {
		for db in tx.all_db(ns).await?.iter() {
			self.database_removed(ib, tx, ns, db.database_id).await?;
		}
		Ok(())
	}

	pub(crate) async fn database_removed(
		&self,
		ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		for tb in tx.all_tb(ns, db, None).await?.iter() {
			self.table_removed(ib, tx, ns, db, tb).await?;
		}
		Ok(())
	}

	pub(crate) async fn table_removed(
		&self,
		ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableDefinition,
	) -> Result<()> {
		for ix in tx.all_tb_indexes(ns, db, &tb.name).await?.iter() {
			if let Some(ib) = ib {
				ib.remove_index(ns, db, &tb.name, ix.index_id).await?;
			}
			self.remove_index(ns, db, tb.table_id, ix).await?;
		}
		Ok(())
	}

	async fn remove_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: &IndexDefinition,
	) -> Result<()> {
		if matches!(ix.index, Index::Hnsw(_)) {
			let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
			self.remove_hnsw_index(tb, ikb).await?;
		}
		Ok(())
	}

	async fn remove_hnsw_index(&self, tb: TableId, ikb: IndexKeyBase) -> Result<()> {
		self.0.hnsw_indexes.remove(tb, &ikb).await?;
		self.0.vector_cache.remove_index(tb, ikb.index()).await;
		Ok(())
	}

	pub(crate) fn mappers(&self) -> &Mappers {
		&self.0.mappers
	}

	pub(crate) fn vector_cache(&self) -> &VectorCache {
		&self.0.vector_cache
	}
}
