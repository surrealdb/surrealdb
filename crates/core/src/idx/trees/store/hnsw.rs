use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::catalog::HnswParams;
use crate::ctx::Context;
use crate::idx::IndexKeyBase;
use crate::idx::trees::hnsw::index::HnswIndex;
use crate::kvs::{KVKey, Key};

pub(crate) type SharedHnswIndex = Arc<RwLock<HnswIndex>>;

pub(crate) struct HnswIndexes(Arc<RwLock<HashMap<Key, SharedHnswIndex>>>);

impl Default for HnswIndexes {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

impl HnswIndexes {
	pub(super) async fn get(
		&self,
		ctx: &Context,
		tb: &str,
		ikb: &IndexKeyBase,
		p: &HnswParams,
	) -> Result<SharedHnswIndex> {
		let key = ikb.new_vm_root_key().encode_key()?;
		let h = self.0.read().await.get(&key).cloned();
		if let Some(h) = h {
			return Ok(h);
		}
		let mut w = self.0.write().await;
		let ix = match w.entry(key) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let h = Arc::new(RwLock::new(
					HnswIndex::new(&ctx.tx(), ikb.clone(), tb.to_string(), p).await?,
				));
				e.insert(h.clone());
				h
			}
		};
		Ok(ix)
	}

	pub(super) async fn remove(&self, ikb: &IndexKeyBase) -> Result<()> {
		let key = ikb.new_vm_root_key().encode_key()?;
		self.0.write().await.remove(&key);
		Ok(())
	}
}
