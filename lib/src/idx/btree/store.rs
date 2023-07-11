use crate::err::Error;
use crate::idx::bkeys::BKeys;
use crate::idx::btree::{Node, NodeId};
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction};
use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Copy)]
pub enum BTreeStoreType {
	Write,
	Read,
	Traversal,
}

pub enum BTreeNodeStore<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	/// caches every read nodes, and keeps track of updated and created nodes
	Write(BTreeWriteCache<BK>),
	/// Uses an LRU cache to keep in memory the last node read
	Read(BTreeReadCache<BK>),
	/// Read the nodes from the KV store without any cache
	Traversal(KeyProvider),
}

impl<BK> BTreeNodeStore<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	pub fn new(
		keys: KeyProvider,
		store_type: BTreeStoreType,
		read_size: usize,
	) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(match store_type {
			BTreeStoreType::Write => Self::Write(BTreeWriteCache::new(keys)),
			BTreeStoreType::Read => Self::Read(BTreeReadCache::new(keys, read_size)),
			BTreeStoreType::Traversal => Self::Traversal(keys),
		}))
	}

	pub(super) async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<BK>, Error> {
		match self {
			BTreeNodeStore::Write(w) => w.get_node(tx, node_id).await,
			BTreeNodeStore::Read(r) => r.get_node(tx, node_id).await,
			BTreeNodeStore::Traversal(keys) => keys.load_node::<BK>(tx, node_id).await,
		}
	}

	pub(super) fn set_node(&mut self, node: StoredNode<BK>, updated: bool) -> Result<(), Error> {
		match self {
			BTreeNodeStore::Write(w) => w.set_node(node, updated),
			BTreeNodeStore::Read(r) => {
				if updated {
					Err(Error::Unreachable)
				} else {
					r.set_node(node);
					Ok(())
				}
			}
			BTreeNodeStore::Traversal(_) => Ok(()),
		}
	}

	pub(super) fn new_node(&mut self, id: NodeId, node: Node<BK>) -> Result<StoredNode<BK>, Error> {
		match self {
			BTreeNodeStore::Write(w) => Ok(w.new_node(id, node)),
			_ => Err(Error::Unreachable),
		}
	}

	pub(super) fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		match self {
			BTreeNodeStore::Write(w) => w.remove_node(node_id, node_key),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		if let BTreeNodeStore::Write(w) = self {
			w.finish(tx).await
		} else {
			Err(Error::Unreachable)
		}
	}
}

pub struct BTreeWriteCache<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	keys: KeyProvider,
	nodes: HashMap<NodeId, StoredNode<BK>>,
	updated: HashSet<NodeId>,
	removed: HashMap<NodeId, Key>,
	#[cfg(debug_assertions)]
	out: HashSet<NodeId>,
}

impl<BK> BTreeWriteCache<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	fn new(keys: KeyProvider) -> Self {
		Self {
			keys,
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
	) -> Result<StoredNode<BK>, Error> {
		#[cfg(debug_assertions)]
		self.out.insert(node_id);
		if let Some(n) = self.nodes.remove(&node_id) {
			return Ok(n);
		}
		self.keys.load_node::<BK>(tx, node_id).await
	}

	fn set_node(&mut self, mut node: StoredNode<BK>, updated: bool) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		self.out.remove(&node.id);
		if updated {
			node.node.keys_mut().compile();
			self.updated.insert(node.id);
		}
		if self.removed.contains_key(&node.id) {
			return Err(Error::Unreachable);
		}
		self.nodes.insert(node.id, node);
		Ok(())
	}

	fn new_node(&mut self, id: NodeId, node: Node<BK>) -> StoredNode<BK> {
		#[cfg(debug_assertions)]
		self.out.insert(id);
		StoredNode {
			node,
			id,
			key: self.keys.get_node_key(id),
			size: 0,
		}
	}

	fn remove_node(&mut self, node_id: NodeId, node_key: Key) -> Result<(), Error> {
		#[cfg(debug_assertions)]
		{
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
				return Err(Error::Unreachable);
			}
		}
		for node_id in &self.updated {
			if let Some(mut node) = self.nodes.remove(node_id) {
				node.node.write(tx, node.key).await?;
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

pub struct BTreeReadCache<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	keys: KeyProvider,
	nodes: LruCache<NodeId, StoredNode<BK>>,
}

impl<BK> BTreeReadCache<BK>
where
	BK: BKeys + Serialize + DeserializeOwned,
{
	fn new(keys: KeyProvider, size: usize) -> Self {
		Self {
			keys,
			nodes: LruCache::new(NonZeroUsize::new(size).unwrap()),
		}
	}

	async fn get_node(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<BK>, Error> {
		if let Some(n) = self.nodes.pop(&node_id) {
			return Ok(n);
		}
		self.keys.load_node::<BK>(tx, node_id).await
	}

	fn set_node(&mut self, node: StoredNode<BK>) {
		self.nodes.put(node.id, node);
	}
}

#[derive(Clone)]
pub enum KeyProvider {
	DocIds(IndexKeyBase),
	DocLengths(IndexKeyBase),
	Postings(IndexKeyBase),
	Terms(IndexKeyBase),
	Debug,
}

impl KeyProvider {
	pub(in crate::idx) fn get_node_key(&self, node_id: NodeId) -> Key {
		match self {
			KeyProvider::DocIds(ikb) => ikb.new_bd_key(Some(node_id)),
			KeyProvider::DocLengths(ikb) => ikb.new_bl_key(Some(node_id)),
			KeyProvider::Postings(ikb) => ikb.new_bp_key(Some(node_id)),
			KeyProvider::Terms(ikb) => ikb.new_bt_key(Some(node_id)),
			KeyProvider::Debug => node_id.to_be_bytes().to_vec(),
		}
	}

	async fn load_node<BK>(&self, tx: &mut Transaction, id: NodeId) -> Result<StoredNode<BK>, Error>
	where
		BK: BKeys + Serialize + DeserializeOwned,
	{
		let key = self.get_node_key(id);
		let (node, size) = Node::<BK>::read(tx, key.clone()).await?;
		Ok(StoredNode {
			node,
			id,
			key,
			size,
		})
	}
}

pub(super) struct StoredNode<BK>
where
	BK: BKeys,
{
	pub(super) node: Node<BK>,
	pub(super) id: NodeId,
	pub(super) key: Key,
	pub(super) size: u32,
}
