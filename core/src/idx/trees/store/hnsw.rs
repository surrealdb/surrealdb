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
	pub(super) async fn get(&self, ikb: &IndexKeyBase, p: &HnswParams) -> SharedHnswIndex {
		let key = ikb.new_vm_key(None);
		let r = self.0.read().await;
		let h = r.get(&key).cloned();
		drop(r);
		if let Some(h) = h {
			return h;
		}
		let mut w = self.0.write().await;
		let ix = match w.entry(key) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let h = Arc::new(RwLock::new(HnswIndex::new(p)));
				e.insert(h.clone());
				h
			}
		};
		drop(w);
		ix
	}

	pub(super) async fn remove(&self, ikb: &IndexKeyBase) {
		let key = ikb.new_vm_key(None);
		let mut w = self.0.write().await;
		w.remove(&key);
		drop(w);
	}

	pub(super) async fn is_empty(&self) -> bool {
		let h = self.0.read().await;
		let r = h.is_empty();
		drop(h);
		r
	}
}
