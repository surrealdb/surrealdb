use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, Val};
use lru::LruCache;
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
	Traversal,
}

pub enum TreeNodeStore<N>
where
	N: TreeNode + Debug,
{
	/// caches every read nodes, and keeps track of updated and created nodes
	Write(TreeWriteCache<N>),
	/// Uses an LRU cache to keep in memory the last node read
	Read(TreeReadCache<N>),
	/// Read the nodes from the KV store without any cache
	Traversal(TreeNodeProvider),
}

impl<N> TreeNodeStore<N>
where
	N: TreeNode + Debug,
{
	pub fn new(
		keys: TreeNodeProvider,
		store_type: TreeStoreType,
		read_size: usize,
	) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(match store_type {
			TreeStoreType::Write => Self::Write(TreeWriteCache::new(keys)),
			TreeStoreType::Read => Self::Read(TreeReadCache::new(keys, read_size)),
			TreeStoreType::Traversal => Self::Traversal(keys),
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
			TreeNodeStore::Traversal(keys) => keys.load::<N>(tx, node_id).await,
		}
	}

	pub(super) fn set_node(&mut self, node: StoredNode<N>, updated: bool) -> Result<(), Error> {
		match self {
			TreeNodeStore::Write(w) => w.set_node(node, updated),
			TreeNodeStore::Read(r) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					r.set_node(node);
					Ok(())
				}
			}
			TreeNodeStore::Traversal(_) => Ok(()),
		}
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: N) -> Result<StoredNode<N>, Error> {
		match self {
			TreeNodeStore::Write(w) => Ok(w.new_node(id, node)),
			_ => Err(Error::Unreachable),
		}
	}

	pub(super) fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		match self {
			TreeNodeStore::Write(w) => w.remove_node(node_id, node_key),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		if let TreeNodeStore::Write(w) = self {
			w.finish(tx).await
		} else {
			Err(Error::Unreachable)
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
		StoredNode {
			n: node,
			id,
			key: self.np.get_key(id),
			size: 0,
		}
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
}

impl<N> TreeReadCache<N>
where
	N: TreeNode,
{
	fn new(keys: TreeNodeProvider, size: usize) -> Self {
		Self {
			keys,
			nodes: LruCache::new(NonZeroUsize::new(size).unwrap()),
		}
	}

	async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		if let Some(n) = self.nodes.pop(&node_id) {
			return Ok(n);
		}
		self.keys.load::<N>(tx, node_id).await
	}

	fn set_node(&mut self, node: StoredNode<N>) {
		self.nodes.put(node.id, node);
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
			Ok(StoredNode {
				n: node,
				id,
				key,
				size,
			})
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

pub trait TreeNode
where
	Self: Sized,
{
	fn try_from_val(val: Val) -> Result<Self, Error>;
	fn try_into_val(&mut self) -> Result<Val, Error>;
}
