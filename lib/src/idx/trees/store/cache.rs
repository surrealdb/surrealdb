use crate::err::Error;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use quick_cache::sync::Cache;
use quick_cache::GuardResult;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) struct TreeCaches<N>(Arc<RwLock<HashMap<Key, TreeCache<N>>>>)
where
	N: TreeNode + Debug + Clone;

impl<N> TreeCaches<N>
where
	N: TreeNode + Debug + Clone,
{
	pub(super) async fn get_cache(
		&self,
		generation: u64,
		keys: &TreeNodeProvider,
		cache_size: usize,
	) -> TreeCache<N> {
		#[cfg(debug_assertions)]
		debug!("get_cache {generation}");
		// We take the key from the node 0 as the key identifier for the cache
		let key = keys.get_key(0);
		match self.0.write().await.entry(key) {
			Entry::Occupied(mut e) => {
				let c = e.get_mut();
				// The cache and the store are matching, we can send a clone of the cache.
				match generation.cmp(&c.generation) {
					Ordering::Less => {
						// The store generation is older than the current cache,
						// we return an empty cache, but we don't hold it
						TreeCache::new(generation, keys.clone(), cache_size)
					}
					Ordering::Equal => c.clone(),
					Ordering::Greater => {
						// The store generation is more recent than the cache,
						// we create a new one and hold it
						let c = TreeCache::new(generation, keys.clone(), cache_size);
						e.insert(c.clone());
						c
					}
				}
			}
			Entry::Vacant(e) => {
				// There is no cache for index, we create one and hold it
				let c = TreeCache::new(generation, keys.clone(), cache_size);
				e.insert(c.clone());
				c
			}
		}
	}
}

impl<N> Default for TreeCaches<N>
where
	N: TreeNode + Debug + Clone,
{
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}
pub struct TreeCache<N>
where
	N: TreeNode + Debug + Clone,
{
	generation: u64,
	keys: TreeNodeProvider,
	cache: Arc<Cache<NodeId, Arc<StoredNode<N>>>>,
}

impl<N> TreeCache<N>
where
	N: TreeNode + Debug + Clone,
{
	pub fn new(generation: u64, keys: TreeNodeProvider, cache_size: usize) -> Self {
		Self {
			generation,
			keys,
			cache: Arc::new(Cache::new(cache_size)),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self.cache.get_value_or_guard(&node_id, None) {
			GuardResult::Value(v) => Ok(v),
			GuardResult::Guard(g) => {
				let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
				g.insert(n.clone()).ok();
				Ok(n)
			}
			GuardResult::Timeout => Err(Error::Unreachable("TreeCache::get_node")),
		}
	}
}

impl<N> Clone for TreeCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn clone(&self) -> Self {
		Self {
			generation: self.generation,
			keys: self.keys.clone(),
			cache: self.cache.clone(),
		}
	}
}
