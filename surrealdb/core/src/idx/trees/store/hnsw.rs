use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::catalog::{HnswParams, IndexId, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::index::HnswIndex;

/// A thread-safe, shared reference to an [`HnswIndex`].
///
/// The `HnswIndex` itself manages internal concurrency using `RwLock` fields,
/// so the outer `Arc` provides shared ownership without additional locking.
pub(crate) type SharedHnswIndex = Arc<HnswIndex>;

/// Registry of all active HNSW indexes, keyed by `(TableId, IndexId)`.
///
/// Provides shared, concurrent access to HNSW indexes across the system.
/// Indexes are lazily initialized on first access and cached for subsequent use.
pub(crate) struct HnswIndexes(Arc<RwLock<HashMap<(TableId, IndexId), SharedHnswIndex>>>);

impl Default for HnswIndexes {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

impl HnswIndexes {
	/// Retrieves or lazily creates an HNSW index for the given table and index key.
	///
	/// Uses a double-checked locking pattern: first attempts a read lock lookup,
	/// then falls back to a write lock for creation if the index is not yet cached.
	pub(super) async fn get(
		&self,
		ctx: &FrozenContext,
		tb: TableId,
		ikb: &IndexKeyBase,
		p: &HnswParams,
	) -> Result<SharedHnswIndex> {
		let key = (tb, ikb.index());
		let h = self.0.read().await.get(&key).cloned();
		if let Some(h) = h {
			return Ok(h);
		}
		let mut w = self.0.write().await;
		let ix = match w.entry(key) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let h = Arc::new(
					HnswIndex::new(
						ctx.get_index_stores().vector_cache().clone(),
						&ctx.tx(),
						ikb.clone(),
						tb,
						p,
					)
					.await?,
				);
				e.insert(h.clone());
				h
			}
		};
		Ok(ix)
	}

	/// Removes an HNSW index from the registry.
	pub(super) async fn remove(&self, tb: TableId, ikb: &IndexKeyBase) -> Result<()> {
		let key = (tb, ikb.index());
		self.0.write().await.remove(&key);
		Ok(())
	}
}
