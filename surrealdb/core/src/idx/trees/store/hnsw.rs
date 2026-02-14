use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::catalog::{HnswParams, IndexId, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::index::HnswIndex;

pub(crate) type SharedHnswIndex = Arc<HnswIndex>;

pub(crate) struct HnswIndexes(Arc<RwLock<HashMap<(TableId, IndexId), SharedHnswIndex>>>);

impl Default for HnswIndexes {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

impl HnswIndexes {
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

	pub(super) async fn remove(&self, tb: TableId, ikb: &IndexKeyBase) -> Result<()> {
		let key = (tb, ikb.index());
		self.0.write().await.remove(&key);
		Ok(())
	}
}
