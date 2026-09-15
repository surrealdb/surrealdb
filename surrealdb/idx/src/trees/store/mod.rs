use std::path::PathBuf;

#[cfg(diskann)]
pub mod diskann;
pub mod hnsw;
mod mapper;
mod segmenter;

use std::sync::Arc;

use anyhow::Result;

use crate::IndexKeyBase;
#[cfg(diskann)]
use crate::catalog::DiskAnnParams;
use crate::catalog::{
	DatabaseId, HnswParams, Index, IndexDefinition, NamespaceId, TableDefinition, TableId,
};
use crate::env::IndexEnv;
#[cfg(diskann)]
use crate::trees::diskann::cache::DiskAnnCache;
use crate::trees::hnsw::cache::VectorCache;
#[cfg(diskann)]
use crate::trees::store::diskann::{DiskAnnIndexes, SharedDiskAnnIndex};
use crate::trees::store::hnsw::{HnswIndexes, SharedHnswIndex};
use crate::trees::store::mapper::Mappers;
use crate::trees::store::segmenter::Segmenters;

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
	/// Behind an `Arc` so a derived store can share it. A segmentation
	/// dictionary is the one thing here too expensive to hold a second copy
	/// of, so a store built for a short-lived context keeps its own caches but
	/// borrows these.
	segmenters: Arc<Segmenters>,
	vector_cache: VectorCache,
	/// Doc-ID → record-key cache for numeric adjacency resolution.
	adjacency_resolve: crate::adjacency::AdjacencyResolveCache,
}

impl IndexStores {
	pub fn new(
		hnsw_cache_size: u64,
		diskann_cache_size: u64,
		adjacency_resolve_cache_size: u64,
		segment_dictionary_path: Option<PathBuf>,
	) -> Self {
		#[cfg(not(diskann))]
		let _ = diskann_cache_size;
		Self(Arc::new(Inner {
			#[cfg(diskann)]
			diskann_indexes: DiskAnnIndexes::default(),
			#[cfg(diskann)]
			diskann_cache: DiskAnnCache::new(diskann_cache_size),
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			segmenters: Arc::new(Segmenters::new(segment_dictionary_path)),
			vector_cache: VectorCache::new(hnsw_cache_size),
			adjacency_resolve: crate::adjacency::AdjacencyResolveCache::new(
				adjacency_resolve_cache_size,
			),
		}))
	}

	/// Builds a store for a short-lived context derived from this one: its own
	/// index and vector caches, but this store's segmentation dictionaries.
	///
	/// A context built per unit of work — a LIVE query delivery, say — would
	/// otherwise load its own dictionary and drop it again, so a burst of them
	/// multiplies peak memory by the number in flight and reloads the same
	/// dictionary every time. The per-language lock inside [`Segmenters`] only
	/// coordinates callers sharing one map, so the map itself is what has to be
	/// shared. The other caches stay separate: they are cheap to rebuild, and
	/// isolating them is why such a context exists.
	pub fn derive(
		&self,
		hnsw_cache_size: u64,
		diskann_cache_size: u64,
		adjacency_resolve_cache_size: u64,
	) -> Self {
		#[cfg(not(diskann))]
		let _ = diskann_cache_size;
		Self(Arc::new(Inner {
			#[cfg(diskann)]
			diskann_indexes: DiskAnnIndexes::default(),
			#[cfg(diskann)]
			diskann_cache: DiskAnnCache::new(diskann_cache_size),
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
			segmenters: Arc::clone(&self.0.segmenters),
			vector_cache: VectorCache::new(hnsw_cache_size),
			adjacency_resolve: crate::adjacency::AdjacencyResolveCache::new(
				adjacency_resolve_cache_size,
			),
		}))
	}

	/// The doc-ID → record-key cache for numeric adjacency resolution.
	pub fn adjacency_resolve(&self) -> &crate::adjacency::AdjacencyResolveCache {
		&self.0.adjacency_resolve
	}

	pub async fn get_index_hnsw(
		&self,
		env: &dyn IndexEnv,
		tb: TableId,
		ikb: &IndexKeyBase,
		p: &HnswParams,
		format_version: u16,
	) -> Result<SharedHnswIndex> {
		self.0.hnsw_indexes.get(env, tb, ikb, p, format_version).await
	}

	/// Returns the process-local DiskANN wrapper for an index, creating it and sharing the cache.
	#[cfg(diskann)]
	pub async fn get_index_diskann(
		&self,
		format_version: u16,
		tb: TableId,
		ikb: &IndexKeyBase,
		p: &DiskAnnParams,
	) -> Result<SharedDiskAnnIndex> {
		self.0.diskann_indexes.get(tb, ikb, p, format_version, self.0.diskann_cache.clone()).await
	}

	/// Evict process-local index wrappers and caches for a retired index.
	///
	/// This deliberately does not abort durable index builders. Schema statements
	/// register those aborts on the surrounding transaction so rollback/cancel
	/// keeps still-valid builders alive.
	pub async fn index_removed(
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
			let format_version = ix.format_version;
			self.remove_hnsw_index(tb, ikb, format_version).await?;
		}
		#[cfg(diskann)]
		if matches!(ix.index, Index::DiskAnn(_)) {
			let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
			self.remove_diskann_index(tb, ikb, ix.format_version).await?;
		}
		Ok(())
	}

	/// Evicts the loaded HNSW graph and the element caches for one table index.
	///
	/// The next access rebuilds the wrapper and reloads every layer from
	/// persisted state. This is the only recovery available to a caller whose
	/// write reached the graph and then rolled back: a layer's version is a
	/// counter each writer advances in its own copy as it applies, so two
	/// writers applying the same batch from the same version reach the same
	/// one, and a state check comparing versions cannot tell a rolled-back
	/// topology from the commit that won.
	pub async fn remove_hnsw_index(
		&self,
		tb: TableId,
		ikb: IndexKeyBase,
		format_version: u16,
	) -> Result<()> {
		self.0.hnsw_indexes.remove(tb, &ikb).await?;
		self.0.vector_cache.remove_index(ikb.ns(), ikb.db(), tb, ikb.index(), format_version).await;
		Ok(())
	}

	/// Drops the process-local element caches for one table index at one format
	/// version, leaving any loaded graph in place.
	///
	/// The vector and document caches are keyed by element and carry no
	/// version, so nothing reconciles them against the store; a caller that has
	/// made their contents unreachable must drop them itself. Reach for
	/// [`Self::remove_hnsw_index`] instead whenever the loaded graph is also in
	/// doubt: it drops the wrapper as well, and a graph cannot be validated
	/// against the store by its layer versions alone.
	pub async fn invalidate_hnsw_index_caches(
		&self,
		tb: TableId,
		ikb: &IndexKeyBase,
		format_version: u16,
	) {
		self.0.vector_cache.remove_index(ikb.ns(), ikb.db(), tb, ikb.index(), format_version).await;
	}

	/// Evicts the loaded DiskANN graph and its cached KV-backed graph data for one table index.
	#[cfg(diskann)]
	pub(crate) async fn remove_diskann_index(
		&self,
		tb: TableId,
		ikb: IndexKeyBase,
		format_version: u16,
	) -> Result<()> {
		self.0.diskann_indexes.remove(tb, &ikb).await?;
		self.0
			.diskann_cache
			.remove_index(ikb.ns(), ikb.db(), tb, ikb.index(), format_version)
			.await;
		Ok(())
	}

	pub fn segmenters(&self) -> &Segmenters {
		&self.0.segmenters
	}

	pub fn mappers(&self) -> &Mappers {
		&self.0.mappers
	}

	pub(crate) fn vector_cache(&self) -> &VectorCache {
		&self.0.vector_cache
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::IndexStores;

	/// A derived store shares its parent's dictionaries, and does not share
	/// everything else by aliasing the whole store.
	///
	/// Contexts built per unit of work derive rather than construct precisely
	/// so that a burst of them cannot each load their own segmentation
	/// dictionary. Nothing else observes that sharing, so nothing else would
	/// notice it being lost.
	#[test]
	fn derive_shares_the_dictionaries_but_not_the_store() {
		let parent = IndexStores::new(1024, 1024, 1024, None);
		let child = parent.derive(1024, 1024, 1024);
		assert!(
			!Arc::ptr_eq(&parent.0, &child.0),
			"a derived store must have its own caches, not alias the parent wholesale"
		);
		assert!(
			Arc::ptr_eq(&parent.0.segmenters, &child.0.segmenters),
			"a derived store must share the parent's segmentation dictionaries"
		);
	}
}
