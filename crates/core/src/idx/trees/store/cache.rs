use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use ahash::{HashMap, HashSet};
use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::{BTreeNode, BTreeStore};
use crate::idx::trees::mtree::{MTreeNode, MTreeStore};
use crate::idx::trees::store::lru::{CacheKey, ConcurrentLru};
use crate::idx::trees::store::{
	NodeId, StoreGeneration, StoredNode, TreeNode, TreeNodeProvider, TreeStore,
};
use crate::kvs::{Key, Transaction, TransactionType};

#[derive(Default)]
pub(crate) struct IndexTreeCaches {
	btree_fst_caches: TreeCaches<BTreeNode<FstKeys>>,
	btree_trie_caches: TreeCaches<BTreeNode<TrieKeys>>,
	mtree_caches: TreeCaches<MTreeNode>,
}

impl IndexTreeCaches {
	pub(crate) async fn get_store_btree_fst(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> Result<BTreeStore<FstKeys>> {
		let cache = self.btree_fst_caches.get_cache(generation, &keys, cache_size).await?;
		Ok(TreeStore::new(keys, cache, tt).await)
	}

	pub(crate) fn advance_store_btree_fst(&self, new_cache: TreeCache<BTreeNode<FstKeys>>) {
		self.btree_fst_caches.new_cache(new_cache);
	}

	pub(crate) async fn get_store_btree_trie(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> Result<BTreeStore<TrieKeys>> {
		let cache = self.btree_trie_caches.get_cache(generation, &keys, cache_size).await?;
		Ok(TreeStore::new(keys, cache, tt).await)
	}

	pub(crate) fn advance_store_btree_trie(&self, new_cache: TreeCache<BTreeNode<TrieKeys>>) {
		self.btree_trie_caches.new_cache(new_cache);
	}

	pub async fn get_store_mtree(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> Result<MTreeStore> {
		let cache = self.mtree_caches.get_cache(generation, &keys, cache_size).await?;
		Ok(TreeStore::new(keys, cache, tt).await)
	}

	pub(crate) fn advance_store_mtree(&self, new_cache: TreeCache<MTreeNode>) {
		self.mtree_caches.new_cache(new_cache);
	}
}

pub(super) struct TreeCaches<N>(Arc<DashMap<Key, Arc<TreeCache<N>>>>)
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
	) -> Result<Arc<TreeCache<N>>> {
		#[cfg(debug_assertions)]
		debug!("get_cache {generation}");
		// We take the key from the node 0 as the key identifier for the cache
		let cache_key = keys.get_key(0)?;
		match self.0.entry(cache_key.clone()) {
			Entry::Occupied(mut e) => {
				let c = e.get_mut();
				// The cache and the store are matching, we can send a clone of the cache.
				match generation.cmp(&c.generation()) {
					Ordering::Less => {
						// The store generation is older than the current cache,
						// we return an empty cache, but we don't hold it
						Ok(Arc::new(TreeCache::new(
							generation,
							cache_key,
							keys.clone(),
							cache_size,
						)))
					}
					Ordering::Equal => Ok(c.clone()),
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
						Ok(c)
					}
				}
			}
			Entry::Vacant(e) => {
				// There is no cache for index, we create one and hold it
				let c = Arc::new(TreeCache::new(generation, cache_key, keys.clone(), cache_size));
				e.insert(c.clone());
				Ok(c)
			}
		}
	}

	pub(super) fn new_cache(&self, new_cache: TreeCache<N>) {
		match self.0.entry(new_cache.cache_key().clone()) {
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
}

impl<N> Default for TreeCaches<N>
where
	N: TreeNode + Debug + Clone + Display,
{
	fn default() -> Self {
		Self(Arc::new(DashMap::new()))
	}
}

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
	) -> Result<Arc<StoredNode<N>>> {
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
			Self::Lru(_, r#gen, _) | TreeCache::Full(_, r#gen, _) => *r#gen,
		}
	}

	/// Creates a copy of the cache, with a generation number incremented by
	/// one. The new cache does not contain the NodeID contained in `updated`
	/// and `removed`.
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

	async fn get_node(&self, tx: &Transaction, node_id: NodeId) -> Result<Arc<StoredNode<N>>> {
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
	) -> Result<Arc<StoredNode<N>>> {
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
