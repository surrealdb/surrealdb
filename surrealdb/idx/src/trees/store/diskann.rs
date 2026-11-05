//! Process-local registry of DiskANN index instances.
//!
//! The executor reuses one [`DiskAnnIndex`] wrapper per namespace/database/table/index identity so
//! graph state, provider caches, and locks are shared across queries in this process. Persisted KV
//! data remains authoritative; removing an index drops only the local wrapper.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::IndexKeyBase;
use crate::catalog::{DatabaseId, DiskAnnParams, IndexId, NamespaceId, TableId};
use crate::trees::diskann::cache::DiskAnnCache;
use crate::trees::diskann::index::DiskAnnIndex;

/// Shared handle to a cached DiskANN index wrapper.
pub type SharedDiskAnnIndex = Arc<DiskAnnIndex>;

/// Cache key for a DiskANN index instance inside one process.
pub(crate) type SharedDiskAnnKey = (NamespaceId, DatabaseId, TableId, IndexId);

/// Registry of live DiskANN index wrappers keyed by catalog identity.
pub(crate) struct DiskAnnIndexes(Arc<RwLock<HashMap<SharedDiskAnnKey, SharedDiskAnnIndex>>>);

impl Default for DiskAnnIndexes {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

impl DiskAnnIndexes {
	/// Returns the live DiskANN wrapper for an index, creating it on the first access.
	pub(super) async fn get(
		&self,
		tb: TableId,
		ikb: &IndexKeyBase,
		p: &DiskAnnParams,
		format_version: u16,
		cache: DiskAnnCache,
	) -> Result<SharedDiskAnnIndex> {
		let key = (ikb.ns(), ikb.db(), tb, ikb.index());
		let h = self.0.read().await.get(&key).cloned();
		if h.as_ref().is_some_and(|h| h.format_version() == format_version) {
			return Ok(h.expect("checked Some"));
		}
		let mut w = self.0.write().await;
		let ix = if let Some(ix) =
			w.get(&key).filter(|ix| ix.format_version() == format_version).map(Arc::clone)
		{
			Some(ix)
		} else {
			w.remove(&key);
			cache.remove_index(ikb.ns(), ikb.db(), tb, ikb.index(), format_version).await;
			let h = Arc::new(DiskAnnIndex::new(ikb.clone(), tb, p, cache, format_version).await?);
			w.insert(key, Arc::clone(&h));
			Some(h)
		};
		Ok(ix.expect("wrapper is always present after creation"))
	}

	/// Drops the process-local wrapper for an index after catalog/index removal.
	pub(super) async fn remove(&self, tb: TableId, ikb: &IndexKeyBase) -> Result<()> {
		let key = (ikb.ns(), ikb.db(), tb, ikb.index());
		self.0.write().await.remove(&key);
		Ok(())
	}
}
