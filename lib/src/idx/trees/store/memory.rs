use crate::err::Error;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::Key;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub(super) type TreeMemoryMap<N> = HashMap<NodeId, Arc<StoredNode<N>>>;

#[derive(Default)]
pub struct TreeMemoryProvider<N>
where
	N: TreeNode + Debug,
{
	map: Arc<Mutex<HashMap<Key, Arc<RwLock<TreeMemoryMap<N>>>>>>,
}

impl<N> TreeMemoryProvider<N>
where
	N: TreeNode + Debug,
{
	pub(super) fn new() -> Self {
		Self {
			map: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	pub(super) async fn get(&self, keys: TreeNodeProvider) -> Arc<RwLock<TreeMemoryMap<N>>> {
		let mut m = self.map.lock().await;
		match m.entry(keys.get_key(0)) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let t = Arc::new(RwLock::new(TreeMemoryMap::new()));
				e.insert(t.clone());
				t
			}
		}
	}
}
