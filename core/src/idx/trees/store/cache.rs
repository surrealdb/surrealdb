use crate::err::Error;
use crate::idx::trees::store::{NodeId, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use quick_cache::sync::Cache;
use quick_cache::GuardResult;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type CacheGen = u64;

pub(super) struct TreeCaches<N>(Arc<RwLock<HashMap<Key, TreeCache<N>>>>)
where
	N: TreeNode + Debug + Clone + Display;

impl<N> TreeCaches<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	pub(super) async fn get_cache(
		&self,
		generation: CacheGen,
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
				match generation.cmp(&c.generation()) {
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

	pub(super) async fn remove_cache(&self, keys: &TreeNodeProvider) {
		let key = keys.get_key(0);
		self.0.write().await.remove(&key);
	}

	pub(crate) async fn is_empty(&self) -> bool {
		self.0.read().await.is_empty()
	}
}

impl<N> Default for TreeCaches<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

#[derive(Clone)]
#[non_exhaustive]
pub enum TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	Lru(CacheGen, TreeLruCache<N>),
	Full(CacheGen, TreeFullCache<N>),
}

impl<N> TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	pub fn new(generation: CacheGen, keys: TreeNodeProvider, cache_size: usize) -> Self {
		if cache_size == 0 {
			TreeCache::Full(generation, TreeFullCache::new(keys))
		} else {
			TreeCache::Lru(generation, TreeLruCache::new(keys, cache_size))
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			TreeCache::Lru(_, c) => c.get_node(tx, node_id).await,
			TreeCache::Full(_, c) => c.get_node(tx, node_id).await,
		}
	}

	fn generation(&self) -> CacheGen {
		match self {
			TreeCache::Lru(gen, _) | TreeCache::Full(gen, _) => *gen,
		}
	}
}

#[non_exhaustive]
pub struct TreeLruCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	keys: TreeNodeProvider,
	cache: Arc<Cache<NodeId, Arc<StoredNode<N>>>>,
}

impl<N> TreeLruCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn new(keys: TreeNodeProvider, cache_size: usize) -> Self {
		Self {
			keys,
			cache: Arc::new(Cache::new(cache_size)),
		}
	}

	async fn get_node(
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

impl<N> Clone for TreeLruCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn clone(&self) -> Self {
		Self {
			keys: self.keys.clone(),
			cache: self.cache.clone(),
		}
	}
}

#[non_exhaustive]
pub struct TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	keys: TreeNodeProvider,
	cache: Arc<RwLock<HashMap<NodeId, Arc<StoredNode<N>>>>>,
}

impl<N> TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	pub fn new(keys: TreeNodeProvider) -> Self {
		Self {
			keys,
			cache: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		// Let's first try with the read lock
		if let Some(n) = self.cache.read().await.get(&node_id).cloned() {
			return Ok(n);
		}
		match self.cache.write().await.entry(node_id) {
			Entry::Occupied(e) => Ok(e.get().clone()),
			Entry::Vacant(e) => {
				let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
				e.insert(n.clone());
				Ok(n)
			}
		}
	}
}

impl<N> Clone for TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn clone(&self) -> Self {
		Self {
			keys: self.keys.clone(),
			cache: self.cache.clone(),
		}
	}
}
