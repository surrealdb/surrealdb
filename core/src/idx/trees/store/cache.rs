use crate::err::Error;
use crate::idx::trees::store::{NodeId, StoreGeneration, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use quick_cache::sync::Cache;
use quick_cache::GuardResult;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tokio::sync::RwLock;
pub(super) struct TreeCaches<N>(Arc<RwLock<HashMap<Key, Arc<TreeCache<N>>>>>)
where
	N: TreeNode + Debug + Clone + Display;

impl<N> TreeCaches<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	pub(super) async fn get_cache(
		&self,
		generation: StoreGeneration,
		keys: &TreeNodeProvider,
		cache_size: usize,
	) -> Arc<TreeCache<N>> {
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
						Arc::new(TreeCache::new(generation, keys.clone(), cache_size))
					}
					Ordering::Equal => c.clone(),
					Ordering::Greater => {
						// The store generation is more recent than the cache,
						// we create a new one and hold it
						let c = Arc::new(TreeCache::new(generation, keys.clone(), cache_size));
						e.insert(c.clone());
						c
					}
				}
			}
			Entry::Vacant(e) => {
				// There is no cache for index, we create one and hold it
				let c = Arc::new(TreeCache::new(generation, keys.clone(), cache_size));
				e.insert(c.clone());
				c
			}
		}
	}

	pub(super) async fn new_cache(&self, keys: &TreeNodeProvider, new_cache: TreeCache<N>) {
		let key = keys.get_key(0);
		match self.0.write().await.entry(key) {
			Entry::Occupied(mut e) => {
				let old_cache = e.get();
				// We only store the cache if it is a newer generation
				if new_cache.generation() > old_cache.generation() {
					e.insert(Arc::new(new_cache));
				}
			}
			Entry::Vacant(e) => {
				e.insert(Arc::new(new_cache));
			}
		}
	}

	pub(super) async fn remove_caches(&self, keys: &TreeNodeProvider) {
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

#[non_exhaustive]
pub enum TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	Lru(StoreGeneration, TreeLruCache<N>),
	Full(StoreGeneration, TreeFullCache<N>),
}

impl<N> TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	pub fn new(generation: StoreGeneration, keys: TreeNodeProvider, cache_size: usize) -> Self {
		if cache_size == 0 {
			Self::Full(generation, TreeFullCache::new(keys))
		} else {
			Self::Lru(generation, TreeLruCache::new(keys, cache_size))
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			Self::Lru(_, c) => c.get_node(tx, node_id).await,
			Self::Full(_, c) => c.get_node(tx, node_id).await,
		}
	}

	pub(super) async fn set_node(&self, node: StoredNode<N>) {
		match self {
			Self::Lru(_, c) => c.set_node(node),
			Self::Full(_, c) => c.set_node(node).await,
		}
	}

	pub(super) async fn remove_node(&self, node_id: NodeId) {
		match self {
			Self::Lru(_, c) => c.remove_node(node_id),
			Self::Full(_, c) => c.remove_node(node_id).await,
		}
	}

	fn generation(&self) -> StoreGeneration {
		match self {
			Self::Lru(gen, _) | TreeCache::Full(gen, _) => *gen,
		}
	}

	pub(super) fn new_generation(&self, new_gen: StoreGeneration) -> TreeCache<N> {
		match self {
			Self::Lru(_, c) => Self::Lru(new_gen, c.clone()),
			Self::Full(_, c) => Self::Full(new_gen, c.clone()),
		}
	}
}

#[non_exhaustive]
#[derive(Clone)]
pub struct TreeLruCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	keys: TreeNodeProvider,
	cache: Cache<NodeId, Arc<StoredNode<N>>>,
}

impl<N> TreeLruCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn new(keys: TreeNodeProvider, cache_size: usize) -> Self {
		Self {
			keys,
			cache: Cache::new(cache_size),
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

	fn set_node(&self, node: StoredNode<N>) {
		self.cache.insert(node.id, Arc::new(node));
	}

	fn remove_node(&self, node_id: NodeId) {
		self.cache.remove(&node_id);
	}
}

#[non_exhaustive]
#[derive(Clone)]
pub struct TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	keys: TreeNodeProvider,
	cache: RwLock<HashMap<NodeId, Arc<StoredNode<N>>>>,
}

impl<N> TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	pub fn new(keys: TreeNodeProvider) -> Self {
		Self {
			keys,
			cache: RwLock::new(HashMap::new()),
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

	pub(super) async fn set_node(&self, node: StoredNode<N>) {
		self.cache.write().await.insert(node.id, Arc::new(node));
	}

	pub(super) async fn remove_node(&self, node_id: NodeId) {
		self.cache.write().await.remove(&node_id);
	}
}
