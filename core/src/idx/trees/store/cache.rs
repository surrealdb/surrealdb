use crate::err::Error;
use crate::idx::trees::store::{NodeId, StoreGeneration, StoredNode, TreeNode, TreeNodeProvider};
use crate::kvs::{Key, Transaction};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;

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
	) -> Arc<TreeCache<N>> {
		#[cfg(debug_assertions)]
		debug!("get_cache {generation}");
		// We take the key from the node 0 as the key identifier for the cache
		let cache_key = keys.get_key(0);
		match self.0.entry(cache_key.clone()) {
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

	pub(super) fn remove_caches(&self, keys: &TreeNodeProvider) {
		let key = keys.get_key(0);
		self.0.remove(&key);
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.0.is_empty()
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
			Self::Lru(cache_key, generation, TreeLruCache::new(keys, cache_size))
		}
	}

	#[cfg(test)]
	pub(in crate::idx) async fn len(&self) -> usize {
		match self {
			Self::Lru(_, _, c) => c.lru.lock().await.len(),
			Self::Full(_, _, c) => c.cache.len(),
		}
	}

	pub(super) async fn get_node(
		&self,
		tx: &mut Transaction,
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
	lru: Mutex<TreeLru<NodeId, Arc<StoredNode<N>>>>,
}

impl<N> TreeLruCache<N>
where
	N: TreeNode + Debug + Clone,
{
	fn new(keys: TreeNodeProvider, size: usize) -> Self {
		let lru = Mutex::new(TreeLru::new(size));
		Self {
			keys,
			lru,
		}
	}

	async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		let mut lru = self.lru.lock().await;
		if let Some(n) = lru.get(&node_id) {
			return Ok(n);
		}
		let n = Arc::new(self.keys.load::<N>(tx, node_id).await?);
		lru.insert(node_id, n.clone());
		Ok(n)
	}

	async fn set_node(&self, node: StoredNode<N>) {
		self.lru.lock().await.insert(node.id, node.into());
	}
	async fn remove_node(&self, node_id: &NodeId) {
		self.lru.lock().await.remove(node_id);
	}

	async fn next_generation(
		&self,
		updated: &HashSet<NodeId>,
		removed: &HashMap<NodeId, Key>,
	) -> Self {
		let lru = self
			.lru
			.lock()
			.await
			.duplicate(|id| !removed.contains_key(id) || !updated.contains(id));
		Self {
			keys: self.keys.clone(),
			lru: Mutex::new(lru),
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
		tx: &mut Transaction,
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

struct TreeLru<K, V>
where
	K: Clone + PartialEq + Eq + Hash,
	V: Clone,
{
	map: HashMap<K, usize>,
	vec: Vec<Option<(K, V)>>,
	capacity: usize,
}

impl<K, V> TreeLru<K, V>
where
	K: Clone + PartialEq + Eq + Hash,
	V: Clone,
{
	fn new(capacity: usize) -> Self {
		Self {
			map: HashMap::with_capacity(capacity),
			vec: Vec::with_capacity(capacity),
			capacity,
		}
	}

	fn get(&mut self, key: &K) -> Option<V> {
		if let Some(pos) = self.map.get(key).copied() {
			let val = self.vec[pos].clone();
			if pos > 0 {
				self.promote(key.clone(), pos);
			}
			val.map(|(_, v)| v.clone())
		} else {
			None
		}
	}

	fn promote(&mut self, key: K, pos: usize) {
		let new_pos = pos - 1;
		let flip_key = self.vec[new_pos].as_ref().map(|(k, _)| k).cloned();
		self.vec.swap(pos, new_pos);
		self.map.insert(key, new_pos);
		if let Some(flip_key) = flip_key {
			self.map.insert(flip_key, pos);
		} else {
			if pos == self.vec.len() - 1 {
				self.vec.remove(pos);
			}
		}
	}

	fn insert(&mut self, key: K, val: V) {
		if let Some(pos) = self.map.get(&key).copied() {
			// If the entry is already there, just update it
			self.vec[pos] = Some((key, val));
		} else {
			// If we reached the capacity
			if self.map.len() >= self.capacity {
				// Find the last entry...
				while !self.vec.is_empty() {
					if let Some(remove) = self.vec.pop() {
						if let Some((k, _v)) = remove {
							// ... and remove it
							self.map.remove(&k);
							break;
						}
					}
				}
			}
			// Now we can insert the new entry
			let pos = self.vec.len();
			self.vec.push(Some((key.clone(), val)));
			// If it is the head
			if pos == 0 {
				// ...we just insert it
				self.map.insert(key, pos);
			} else {
				// or we promote it
				self.promote(key, pos);
			}
		}
	}

	fn remove(&mut self, key: &K) {
		if let Some(pos) = self.map.remove(key) {
			if pos == self.vec.len() - 1 {
				// If it is the last element, we can just remove it from the vec
				self.vec.pop();
			} else {
				// Otherwise we set a placeholder
				self.vec[pos] = None;
			}
		}
	}

	#[cfg(test)]
	fn len(&self) -> usize {
		self.map.len()
	}

	fn duplicate<F>(&self, filter: F) -> Self
	where
		F: Fn(&K) -> bool,
	{
		let mut map = HashMap::with_capacity(self.capacity);
		let mut vec = Vec::with_capacity(self.capacity);
		self.map.iter().filter(|&(k, _pos)| filter(k)).for_each(|(k, pos)| {
			let new_pos = vec.len();
			map.insert(k.clone(), new_pos);
			vec.push(self.vec[*pos].clone());
		});
		Self {
			map,
			vec,
			capacity: self.capacity,
		}
	}
}
