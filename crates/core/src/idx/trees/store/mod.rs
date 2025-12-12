pub(crate) mod hnsw;
mod mapper;

use std::sync::Arc;

use anyhow::Result;

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
use crate::kvs::index::IndexBuilder;

#[derive(Clone)]
pub struct IndexStores(Arc<Inner>);

struct Inner {
	hnsw_indexes: HnswIndexes,
	mappers: Mappers,
	vector_cache: VectorCache,
}

impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			vector_cache: VectorCache::default(),
		}))
	}
}

impl IndexStores {
	pub(crate) async fn get_index_hnsw(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ctx: &FrozenContext,
		tb: TableId,
		ix: &IndexDefinition,
		p: &HnswParams,
	) -> Result<SharedHnswIndex> {
		let ikb = IndexKeyBase::new(ns, db, &ix.table_name, ix.index_id);
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
			let ikb = IndexKeyBase::new(ns, db, &ix.table_name, ix.index_id);
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
