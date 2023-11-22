use crate::err::Error;
use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::BTreeNode;
use crate::idx::trees::mtree::MTreeNode;
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, Val};
use lru::LruCache;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type NodeId = u64;

#[derive(Clone, Copy, PartialEq)]
pub enum TreeStoreType {
	Write,
	Read,
	MemoryRead,
	MemoryWrite,
}

pub enum TreeNodeStore<N>
where
	N: TreeNode + Debug,
{
	/// caches every read nodes, and keeps track of updated and created nodes
	Write(TreeWriteCache<N>),
	/// Uses an LRU cache to keep in memory the last node read
	Read(TreeReadCache<N>),
	/// Nodes are stored in memory with a read guard lock
	MemoryRead(TreeMemory<N>),
	/// Nodes are stored in memory with a write guard lock
	MemoryWrite(TreeMemory<N>),
}

impl<N> TreeNodeStore<N>
where
	N: TreeNode + Debug,
{
	pub async fn new(
		keys: TreeNodeProvider,
		store_type: TreeStoreType,
		read_size: usize,
		in_memory_provider: &InMemoryProvider<N>,
	) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(match store_type {
			TreeStoreType::Write => Self::Write(TreeWriteCache::new(keys)),
			TreeStoreType::Read => Self::Read(TreeReadCache::new(keys, read_size)),
			TreeStoreType::MemoryRead => {
				Self::MemoryRead(TreeMemory::new(in_memory_provider.get(keys).await))
			}
			TreeStoreType::MemoryWrite => {
				Self::MemoryWrite(TreeMemory::new(in_memory_provider.get(keys).await))
			}
		}))
	}

	pub(super) async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		match self {
			TreeNodeStore::Write(w) => w.get_node(tx, node_id).await,
			TreeNodeStore::Read(r) => r.get_node(tx, node_id).await,
			TreeNodeStore::MemoryRead(t) => t.get_node(node_id).await,
			TreeNodeStore::MemoryWrite(t) => t.get_node(node_id).await,
		}
	}

	pub(super) async fn set_node(
		&mut self,
		node: StoredNode<N>,
		updated: bool,
	) -> Result<(), Error> {
		match self {
			TreeNodeStore::Write(w) => w.set_node(node, updated),
			TreeNodeStore::Read(r) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					r.set_node(node)
				}
			}
			TreeNodeStore::MemoryRead(t) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					t.set_node(node).await
				}
			}
			TreeNodeStore::MemoryWrite(t) => t.set_node(node).await,
		}
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: N) -> Result<StoredNode<N>, Error> {
		match self {
			TreeNodeStore::Write(w) => Ok(w.new_node(id, node)),
			TreeNodeStore::MemoryWrite(t) => Ok(t.new_node(id, node)),
			_ => Err(Error::Unreachable),
		}
	}

	pub(super) async fn remove_node(
		&mut self,
		node_id: NodeId,
		node_key: Key,
	) -> Result<(), Error> {
		match self {
			TreeNodeStore::Write(w) => w.remove_node(node_id, node_key),
			TreeNodeStore::MemoryWrite(t) => t.remove_node(node_id).await,
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		match self {
			TreeNodeStore::Write(w) => w.finish(tx).await,
			TreeNodeStore::Read(r) => r.finish(),
			TreeNodeStore::MemoryRead(t) => t.finish(),
			TreeNodeStore::MemoryWrite(t) => t.finish(),
		}
	}
}

pub struct TreeWriteCache<N>
where
	N: TreeNode + Debug,
{
	np: TreeNodeProvider,
	nodes: HashMap<NodeId, StoredNode<N>>,
	updated: HashSet<NodeId>,
	removed: HashMap<NodeId, Key>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<N: Debug> TreeWriteCache<N>
where
	N: TreeNode,
{
	fn new(keys: TreeNodeProvider) -> Self {
		Self {
			np: keys,
			nodes: HashMap::new(),
			updated: HashSet::new(),
			removed: HashMap::new(),
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		if let Some(n) = self.nodes.remove(&node_id) {
			return Ok(n);
		}
		self.np.load::<N>(tx, node_id).await
	}

	fn set_node(&mut self, node: StoredNode<N>, updated: bool) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {} {:?}", node.id, updated, node.n);
			self.out.remove(&node.id);
		}
		if updated {
			self.updated.insert(node.id);
		}
		if self.removed.contains_key(&node.id) {
			return Err(Error::Unreachable);
		}
		self.nodes.insert(node.id, node);
		Ok(())
	}

	fn new_node(&mut self, id: NodeId, node: N) -> StoredNode<N> {
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		StoredNode::new(node, id, self.np.get_key(id), 0)
	}

	fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("REMOVE: {}", node_id);
			if self.nodes.contains_key(&node_id) {
				return Err(Error::Unreachable);
			}
			self.out.remove(&node_id);
		}
		self.updated.remove(&node_id);
		self.removed.insert(node_id, node_key);
		Ok(())
	}

	async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		let update = !self.updated.is_empty() || !self.removed.is_empty();
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable);
			}
		}
		for node_id in &self.updated {
			if let Some(node) = self.nodes.remove(node_id) {
				self.np.save(tx, node).await?;
			} else {
				return Err(Error::Unreachable);
			}
		}
		self.updated.clear();
		let node_ids: Vec<NodeId> = self.removed.keys().copied().collect();
		for node_id in node_ids {
			if let Some(node_key) = self.removed.remove(&node_id) {
				tx.del(node_key).await?;
			}
		}
		Ok(update)
	}
}

pub struct TreeReadCache<N>
where
	N: TreeNode,
{
	keys: TreeNodeProvider,
	nodes: LruCache<NodeId, StoredNode<N>>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<N> TreeReadCache<N>
where
	N: TreeNode + Debug,
{
	fn new(keys: TreeNodeProvider, size: usize) -> Self {
		Self {
			keys,
			nodes: LruCache::new(NonZeroUsize::new(size).unwrap()),
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		if let Some(n) = self.nodes.pop(&node_id) {
			return Ok(n);
		}
		self.keys.load::<N>(tx, node_id).await
	}

	fn set_node(&mut self, node: StoredNode<N>) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {:?}", node.id, node.n);
			self.out.remove(&node.id);
		}
		self.nodes.put(node.id, node);
		Ok(())
	}

	fn finish(&mut self) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable);
			}
		}
		Ok(true)
	}
}

pub(super) type TreeMemoryMap<N> = HashMap<NodeId, StoredNode<N>>;

pub struct TreeMemory<N>
where
	N: TreeNode + Debug,
{
	nodes: Arc<Mutex<TreeMemoryMap<N>>>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<N> TreeMemory<N>
where
	N: TreeNode + Debug,
{
	fn new(nodes: Arc<Mutex<TreeMemoryMap<N>>>) -> Self {
		Self {
			nodes,
			#[cfg(debug_assertions)]
			out: HashSet::new(),
		}
	}

	async fn get_node(&mut self, node_id: NodeId) -> Result<StoredNode<N>, Error> {
		#[cfg(debug_assertions)]
		{
			debug!("GET: {}", node_id);
			self.out.insert(node_id);
		}
		self.nodes.lock().await.remove(&node_id).ok_or(Error::Unreachable)
	}

	async fn set_node(&mut self, node: StoredNode<N>) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
			debug!("SET: {} {:?}", node.id, node.n);
			self.out.remove(&node.id);
		}
		self.nodes.lock().await.insert(node.id, node);
		Ok(())
	}

	fn new_node(&mut self, id: NodeId, node: N) -> StoredNode<N> {
		#[cfg(debug_assertions)]
		{
			debug!("NEW: {}", id);
			self.out.insert(id);
		}
		StoredNode::new(node, id, vec![], 0)
	}

	async fn remove_node(&mut self, node_id: NodeId) -> Result<(), Error> {
		let mut nodes = self.nodes.lock().await;
		#[cfg(debug_assertions)]
		{
			debug!("REMOVE: {}", node_id);
			if nodes.contains_key(&node_id) {
				return Err(Error::Unreachable);
			}
			self.out.remove(&node_id);
		}
		nodes.remove(&node_id);
		Ok(())
	}

	fn finish(&mut self) -> Result<bool, Error> {
		#[cfg(debug_assertions)]
		{
			if !self.out.is_empty() {
				debug!("OUT: {:?}", self.out);
				return Err(Error::Unreachable);
			}
		}
		Ok(true)
	}
}

#[derive(Clone)]
pub enum TreeNodeProvider {
	DocIds(IndexKeyBase),
	DocLengths(IndexKeyBase),
	Postings(IndexKeyBase),
	Terms(IndexKeyBase),
	Vector(IndexKeyBase),
	Debug,
}

impl TreeNodeProvider {
	pub(in crate::idx) fn get_key(&self, node_id: NodeId) -> Key {
		match self {
			TreeNodeProvider::DocIds(ikb) => ikb.new_bd_key(Some(node_id)),
			TreeNodeProvider::DocLengths(ikb) => ikb.new_bl_key(Some(node_id)),
			TreeNodeProvider::Postings(ikb) => ikb.new_bp_key(Some(node_id)),
			TreeNodeProvider::Terms(ikb) => ikb.new_bt_key(Some(node_id)),
			TreeNodeProvider::Vector(ikb) => ikb.new_vm_key(Some(node_id)),
			TreeNodeProvider::Debug => node_id.to_be_bytes().to_vec(),
		}
	}

	async fn load<N>(&self, tx: &mut Transaction, id: NodeId) -> Result<StoredNode<N>, Error>
	where
		N: TreeNode,
	{
		let key = self.get_key(id);
		if let Some(val) = tx.get(key.clone()).await? {
			let size = val.len() as u32;
			let node = N::try_from_val(val)?;
			Ok(StoredNode::new(node, id, key, size))
		} else {
			Err(Error::CorruptedIndex)
		}
	}

	async fn save<N>(&self, tx: &mut Transaction, mut node: StoredNode<N>) -> Result<(), Error>
	where
		N: TreeNode,
	{
		let val = node.n.try_into_val()?;
		tx.set(node.key, val).await?;
		Ok(())
	}
}

pub(super) struct StoredNode<N> {
	pub(super) n: N,
	pub(super) id: NodeId,
	pub(super) key: Key,
	pub(super) size: u32,
}

impl<N> StoredNode<N> {
	pub(super) fn new(n: N, id: NodeId, key: Key, size: u32) -> Self {
		Self {
			n,
			id,
			key,
			size,
		}
	}
}

pub trait TreeNode {
	fn try_from_val(val: Val) -> Result<Self, Error>
	where
		Self: Sized;
	fn try_into_val(&mut self) -> Result<Val, Error>;
}

#[derive(Default)]
pub struct InMemoryProvider<N>
where
	N: TreeNode + 'static,
{
	map: Arc<Mutex<HashMap<Key, Arc<Mutex<TreeMemoryMap<N>>>>>>,
}

impl<N> InMemoryProvider<N>
where
	N: TreeNode + 'static,
{
	pub(super) fn new() -> Self {
		Self {
			map: Arc::new(Mutex::new(HashMap::new())),
		}
	}

	async fn get(&self, keys: TreeNodeProvider) -> Arc<Mutex<TreeMemoryMap<N>>> {
		let mut m = self.map.lock().await;
		match m.entry(keys.get_key(0)) {
			Entry::Occupied(e) => e.get().clone(),
			Entry::Vacant(e) => {
				let t = Arc::new(Mutex::new(TreeMemoryMap::new()));
				e.insert(t.clone());
				t
			}
		}
	}
}

#[derive(Clone)]
pub(crate) struct IndexStores(Arc<Inner>);

struct Inner {
	in_memory_btree_fst: InMemoryProvider<BTreeNode<FstKeys>>,
	in_memory_btree_trie: InMemoryProvider<BTreeNode<TrieKeys>>,
	in_memory_mtree: InMemoryProvider<MTreeNode>,
}
impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			in_memory_btree_fst: InMemoryProvider::new(),
			in_memory_btree_trie: InMemoryProvider::new(),
			in_memory_mtree: InMemoryProvider::new(),
		}))
	}
}

impl IndexStores {
	pub(in crate::idx) fn in_memory_btree_fst(&self) -> &InMemoryProvider<BTreeNode<FstKeys>> {
		&self.0.in_memory_btree_fst
	}

	pub(in crate::idx) fn in_memory_btree_trie(&self) -> &InMemoryProvider<BTreeNode<TrieKeys>> {
		&self.0.in_memory_btree_trie
	}

	pub(in crate::idx) fn in_memory_mtree(&self) -> &InMemoryProvider<MTreeNode> {
		&self.0.in_memory_mtree
	}
}
