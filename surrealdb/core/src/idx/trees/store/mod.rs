#[cfg(diskann)]
pub(crate) mod diskann;
pub(crate) mod hnsw;
mod mapper;

use std::sync::Arc;

use anyhow::Result;

#[cfg(diskann)]
use crate::catalog::DiskAnnParams;
use crate::catalog::{
	DatabaseId, HnswParams, Index, IndexDefinition, NamespaceId, TableDefinition, TableId,
};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
#[cfg(diskann)]
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::hnsw::cache::VectorCache;
#[cfg(diskann)]
use crate::idx::trees::store::diskann::{DiskAnnIndexes, SharedDiskAnnIndex};
use crate::idx::trees::store::hnsw::{HnswIndexes, SharedHnswIndex};
use crate::idx::trees::store::mapper::Mappers;

#[derive(Clone)]
pub struct IndexStores(Arc<Inner>);

struct Inner {
	#[cfg(diskann)]
	diskann_indexes: DiskAnnIndexes,
	/// Shared hot graph-data cache for all loaded DiskANN indexes.
	#[cfg(diskann)]
	diskann_cache: DiskAnnCache,
	hnsw_indexes: HnswIndexes,
	mappers: Mappers,
	vector_cache: VectorCache,
}

impl IndexStores {
	pub(crate) fn new(hnsw_cache_size: u64, diskann_cache_size: u64) -> Self {
		#[cfg(not(diskann))]
		let _ = diskann_cache_size;
		Self(Arc::new(Inner {
			#[cfg(diskann)]
			diskann_indexes: DiskAnnIndexes::default(),
			#[cfg(diskann)]
			diskann_cache: DiskAnnCache::new(diskann_cache_size),
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			vector_cache: VectorCache::new(hnsw_cache_size),
		}))
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

	/// Returns the process-local DiskANN wrapper for an index, creating it and sharing the cache.
	#[cfg(diskann)]
	pub(crate) async fn get_index_diskann(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: &IndexDefinition,
		p: &DiskAnnParams,
	) -> Result<SharedDiskAnnIndex> {
		let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
		self.0.diskann_indexes.get(tb, &ikb, p, self.0.diskann_cache.clone()).await
	}

	/// Evict process-local index wrappers and caches for a retired index.
	///
	/// This deliberately does not abort durable index builders. Schema statements
	/// register those aborts on the surrounding transaction so rollback/cancel
	/// keeps still-valid builders alive.
	pub(crate) async fn index_removed(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableDefinition,
		ix: &IndexDefinition,
	) -> Result<()> {
		self.remove_index(ns, db, tb.table_id, ix).await
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
		#[cfg(diskann)]
		if matches!(ix.index, Index::DiskAnn(_)) {
			let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
			self.remove_diskann_index(tb, ikb).await?;
		}
		Ok(())
	}

	/// Evicts cached HNSW graph and vector entries for one table index.
	pub(crate) async fn remove_hnsw_index(&self, tb: TableId, ikb: IndexKeyBase) -> Result<()> {
		self.0.hnsw_indexes.remove(tb, &ikb).await?;
		self.0.vector_cache.remove_index(ikb.ns(), ikb.db(), tb, ikb.index()).await;
		Ok(())
	}

	/// Evicts the loaded DiskANN graph and its cached KV-backed graph data for one table index.
	#[cfg(diskann)]
	pub(crate) async fn remove_diskann_index(&self, tb: TableId, ikb: IndexKeyBase) -> Result<()> {
		self.0.diskann_indexes.remove(tb, &ikb).await?;
		self.0.diskann_cache.remove_index(ikb.ns(), ikb.db(), tb, ikb.index()).await;
		Ok(())
	}

	pub(crate) fn mappers(&self) -> &Mappers {
		&self.0.mappers
	}

	pub(crate) fn vector_cache(&self) -> &VectorCache {
		&self.0.vector_cache
	}
}
