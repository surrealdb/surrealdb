use crate::err::Error;
use crate::idx::trees::store::lru::{CacheKey, ConcurrentLru};
use crate::idx::trees::store::{NodeId, StoreGeneration, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use ahash::{HashMap, HashSet};
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::sync::Arc;

pub(super) struct TreeCaches<N>(Arc<Inner<N>>)
where
	N: TreeNode + Debug + Clone + Display;

struct Inner<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	current: DashMap<Key, Arc<TreeCache<N>>>,
	modified: DashSet<Key>,
}

impl<N> Default for TreeCaches<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	fn default() -> Self {
		Self(Arc::new(Inner {
			current: DashMap::new(),
			modified: DashSet::new(),
		}))
	}
}

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
		let cache_key = keys.get_key(0);
		match self.0.current.entry(cache_key.clone()) {
			Entry::Occupied(mut e) => {
				let c = e.get_mut();
				// The cache and the store are matching, we can send a clone of the cache.
				match generation.cmp(&c.generation()) {
					Ordering::Less => {
						// The store generation is older than the current cache,
						// we return an empty cache, but we don't hold it
						Arc::new(TreeCache::new(generation, cache_key, keys.clone(), cache_size))
					}
					Ordering::Equal => c.clone(),
					Ordering::Greater => {
						// The store generation is more recent than the cache,
						// we create a new one and hold it
						let c = Arc::new(TreeCache::new(
							generation,
							cache_key,
							keys.clone(),
							cache_size,
						));
						e.insert(c.clone());
						c
					}
				}
			}
			Entry::Vacant(e) => {
				// There is no cache for index, we create one and hold it
				let c = Arc::new(TreeCache::new(generation, cache_key, keys.clone(), cache_size));
				e.insert(c.clone());
				c
			}
		}
	}

	pub(super) fn new_cache(&self, new_cache: TreeCache<N>) {
		match self.0.current.entry(new_cache.cache_key().clone()) {
			Entry::Occupied(mut e) => {
				let old_cache = e.get();
				// We only store the cache if it is a newer generation
				if new_cache.generation() > old_cache.generation() {
					self.0.modified.insert(e.key().clone());
					e.insert(Arc::new(new_cache));
				}
			}
			Entry::Vacant(e) => {
				self.0.modified.insert(e.key().clone());
				e.insert(Arc::new(new_cache));
			}
		}
	}

	pub(super) fn remove_caches(&self, keys: &TreeNodeProvider) {
		let key = keys.get_key(0);
		self.0.current.remove(&key);
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.0.current.is_empty()
	}
}

#[non_exhaustive]
pub enum TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	Lru(Key, StoreGeneration, TreeLruCache<N>),
	Full(Key, StoreGeneration, TreeFullCache<N>),
}

impl<N> TreeCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	pub fn new(
		generation: StoreGeneration,
		cache_key: Key,
		keys: TreeNodeProvider,
		cache_size: usize,
	) -> Self {
		if cache_size == 0 {
			Self::Full(cache_key, generation, TreeFullCache::new(keys))
		} else {
			Self::Lru(cache_key, generation, TreeLruCache::with_capacity(keys, cache_size))
		}
	}

	#[cfg(test)]
	pub(in crate::idx) fn len(&self) -> usize {
		match self {
			Self::Lru(_, _, c) => c.lru.len(),
			Self::Full(_, _, c) => c.cache.len(),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			Self::Lru(_, _, c) => c.get_node(tx, node_id).await,
			Self::Full(_, _, c) => c.get_node(tx, node_id).await,
		}
	}

	pub(super) async fn set_node(&self, node: StoredNode<N>) {
		match self {
			Self::Lru(_, _, c) => c.set_node(node).await,
			Self::Full(_, _, c) => c.set_node(node),
		}
	}

	pub(super) async fn remove_node(&self, node_id: &NodeId) {
		match self {
			Self::Lru(_, _, c) => c.remove_node(node_id).await,
			Self::Full(_, _, c) => c.remove_node(node_id),
		}
	}

	pub(super) fn cache_key(&self) -> &Key {
		match self {
			Self::Lru(k, _, _) => k,
			Self::Full(k, _, _) => k,
		}
	}

	fn generation(&self) -> StoreGeneration {
		match self {
			Self::Lru(_, gen, _) | TreeCache::Full(_, gen, _) => *gen,
		}
	}

	/// Creates a copy of the cache, with a generation number incremented by one.
	/// The new cache does not contain the NodeID contained in `updated` and `removed`.
	pub(super) async fn next_generation(
		&self,
		updated: &HashSet<NodeId>,
		removed: &HashMap<NodeId, Key>,
	) -> Self {
		match self {
			Self::Lru(k, g, c) => {
				Self::Lru(k.clone(), *g + 1, c.next_generation(updated, removed).await)
			}
			Self::Full(k, g, c) => {
				Self::Full(k.clone(), *g + 1, c.next_generation(updated, removed))
			}
		}
	}
}

#[non_exhaustive]
pub struct TreeLruCache<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	keys: TreeNodeProvider,
	lru: ConcurrentLru<Arc<StoredNode<N>>>,
}

impl<N> TreeLruCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn with_capacity(keys: TreeNodeProvider, size: usize) -> Self {
		let lru = ConcurrentLru::with_capacity(size);
		Self {
			keys,
			lru,
		}
	}

	async fn get_node(
		&self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		if let Some(n) = self.lru.get(node_id).await {
			return Ok(n);
		}
		let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
		self.lru.insert(node_id as CacheKey, n.clone()).await;
		Ok(n)
	}

	async fn set_node(&self, node: StoredNode<N>) {
		self.lru.insert(node.id as CacheKey, node.into()).await;
	}
	async fn remove_node(&self, node_id: &NodeId) {
		self.lru.remove(*node_id as CacheKey).await;
	}

	async fn next_generation(
		&self,
		updated: &HashSet<NodeId>,
		removed: &HashMap<NodeId, Key>,
	) -> Self {
		Self {
			keys: self.keys.clone(),
			lru: self.lru.duplicate(|id| !removed.contains_key(id) || !updated.contains(id)).await,
		}
	}
}

#[non_exhaustive]
pub struct TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	keys: TreeNodeProvider,
	cache: DashMap<NodeId, Arc<StoredNode<N>>>,
}

impl<N> TreeFullCache<N>
where
	N: TreeNode + Debug + Clone,
{
	pub fn new(keys: TreeNodeProvider) -> Self {
		Self {
			keys,
			cache: DashMap::new(),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self.cache.entry(node_id) {
			Entry::Occupied(e) => Ok(e.get().clone()),
			Entry::Vacant(e) => {
				let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
				e.insert(n.clone());
				Ok(n)
			}
		}
	}

	pub(super) fn set_node(&self, node: StoredNode<N>) {
		self.cache.insert(node.id, node.into());
	}

	pub(super) fn remove_node(&self, node_id: &NodeId) {
		self.cache.remove(node_id);
	}

	fn next_generation(&self, updated: &HashSet<NodeId>, removed: &HashMap<NodeId, Key>) -> Self {
		let new_cache = Self::new(self.keys.clone());
		self.cache
			.iter()
			.filter(|r| !removed.contains_key(r.key()))
			.filter(|r| !updated.contains(r.key()))
			.for_each(|r| {
				new_cache.cache.insert(r.id, r.value().clone());
			});
		new_cache
	}
}
