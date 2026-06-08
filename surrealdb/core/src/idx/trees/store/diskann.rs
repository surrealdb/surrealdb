//! Process-local registry of DiskANN index instances.
//!
//! The executor reuses one [`DiskAnnIndex`] wrapper per namespace/database/table/index identity so
//! graph state, provider caches, and locks are shared across queries in this process. Persisted KV
//! data remains authoritative; removing an index drops only the local wrapper.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::catalog::{DatabaseId, DiskAnnParams, IndexId, NamespaceId, TableId};
use crate::idx::IndexKeyBase;
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::diskann::index::DiskAnnIndex;

/// Shared handle to a cached DiskANN index wrapper.
pub(crate) type SharedDiskAnnIndex = Arc<DiskAnnIndex>;

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
		cache: DiskAnnCache,
	) -> Result<SharedDiskAnnIndex> {
		let key = (ikb.ns(), ikb.db(), tb, ikb.index());
		let h = self.0.read().await.get(&key).cloned();
		if let Some(h) = h {
			return Ok(h);
		}
		let mut w = self.0.write().await;
		let ix = match w.entry(key) {
			Entry::Occupied(e) => Arc::clone(e.get()),
			Entry::Vacant(e) => {
				let h = Arc::new(DiskAnnIndex::new(ikb.clone(), tb, p, cache).await?);
				e.insert(Arc::clone(&h));
				h
			}
		};
		Ok(ix)
	}

	/// Drops the process-local wrapper for an index after catalog/index removal.
	pub(super) async fn remove(&self, tb: TableId, ikb: &IndexKeyBase) -> Result<()> {
		let key = (ikb.ns(), ikb.db(), tb, ikb.index());
		self.0.write().await.remove(&key);
		Ok(())
	}
}
