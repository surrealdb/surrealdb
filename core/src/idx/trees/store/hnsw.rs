use crate::ctx::Context;
use crate::err::Error;
use crate::idx::trees::hnsw::index::HnswIndex;
use crate::idx::IndexKeyBase;
use crate::kvs::Key;
use crate::sql::index::HnswParams;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
	) -> Result<SharedHnswIndex, Error> {
		let key = ikb.new_vm_key(None);
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

	pub(super) async fn remove(&self, ikb: &IndexKeyBase) {
		let key = ikb.new_vm_key(None);
		self.0.write().await.remove(&key);
	}

	pub(super) async fn is_empty(&self) -> bool {
		self.0.read().await.is_empty()
	}
}
