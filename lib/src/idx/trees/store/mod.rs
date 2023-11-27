mod cache;
pub(crate) mod tree;

use crate::err::Error;
use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::{BTreeNode, BTreeStore};
use crate::idx::trees::mtree::{MTreeNode, MTreeStore};
use crate::idx::trees::store::cache::{TreeCache, TreeCaches};
use crate::idx::trees::store::tree::{TreeRead, TreeWrite};
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, TransactionType, Val};
use once_cell::sync::Lazy;
use std::fmt::Debug;
use std::sync::Arc;

pub type NodeId = u64;

pub enum TreeStore<N>
where
	N: TreeNode + Debug + Clone,
{
	/// caches every read nodes, and keeps track of updated and created nodes
	Write(TreeWrite<N>),
	/// caches read nodes in an LRU cache
	Read(TreeRead<N>),
}

impl<N> TreeStore<N>
where
	N: TreeNode + Debug + Clone,
{
	pub async fn new(keys: TreeNodeProvider, cache: TreeCache<N>, tt: TransactionType) -> Self {
		match tt {
			TransactionType::Read => Self::Read(TreeRead::new(cache)),
			TransactionType::Write => Self::Write(TreeWrite::new(keys, cache)),
		}
	}

	pub(in crate::idx) async fn get_node_mut(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		match self {
			TreeStore::Write(w) => w.get_node_mut(tx, node_id).await,
			_ => Err(Error::Unreachable("TreeStore::get_node_mut")),
		}
	}

	pub(in crate::idx) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			TreeStore::Read(r) => r.get_node(tx, node_id).await,
			_ => Err(Error::Unreachable("TreeStore::get_node")),
		}
	}

	pub(in crate::idx) async fn set_node(
		&mut self,
		node: StoredNode<N>,
		updated: bool,
	) -> Result<(), Error> {
		match self {
			TreeStore::Write(w) => w.set_node(node, updated),
			_ => Err(Error::Unreachable("TreeStore::set_node")),
		}
	}

	pub(in crate::idx) fn new_node(&mut self, id: NodeId, node: N) -> Result<StoredNode<N>, Error> {
		match self {
			TreeStore::Write(w) => Ok(w.new_node(id, node)),
			_ => Err(Error::Unreachable("TreeStore::new_node")),
		}
	}

	pub(in crate::idx) async fn remove_node(
		&mut self,
		node_id: NodeId,
		node_key: Key,
	) -> Result<(), Error> {
		match self {
			TreeStore::Write(w) => w.remove_node(node_id, node_key),
			_ => Err(Error::Unreachable("TreeStore::remove_node")),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		match self {
			TreeStore::Write(w) => w.finish(tx).await,
			_ => Ok(false),
		}
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
		N: TreeNode + Clone,
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
		N: TreeNode + Clone,
	{
		let val = node.n.try_into_val()?;
		tx.set(node.key, val).await?;
		Ok(())
	}
}

pub struct StoredNode<N>
where
	N: Clone,
{
	pub(super) n: N,
	pub(super) id: NodeId,
	pub(super) key: Key,
	pub(super) size: u32,
}

impl<N> StoredNode<N>
where
	N: Clone,
{
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

#[derive(Clone)]
pub(crate) struct IndexStores(Arc<Inner>);

struct Inner {
	btree_fst_caches: TreeCaches<BTreeNode<FstKeys>>,
	btree_trie_caches: TreeCaches<BTreeNode<TrieKeys>>,
	mtree_caches: TreeCaches<MTreeNode>,
}
impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			btree_fst_caches: TreeCaches::default(),
			btree_trie_caches: TreeCaches::default(),
			mtree_caches: TreeCaches::default(),
		}))
	}
}

pub(crate) static INDEX_STORES: Lazy<IndexStores> = Lazy::new(|| IndexStores::default());
impl IndexStores {
	pub(in crate::idx) async fn get_store_btree_fst(
		&self,
		keys: TreeNodeProvider,
		generation: u64,
		tt: TransactionType,
		cache_size: usize,
	) -> BTreeStore<FstKeys> {
		let cache = self.0.btree_fst_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}

	pub(in crate::idx) async fn get_store_btree_trie(
		&self,
		keys: TreeNodeProvider,
		generation: u64,
		tt: TransactionType,
		cache_size: usize,
	) -> BTreeStore<TrieKeys> {
		let cache = self.0.btree_trie_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}

	pub(in crate::idx) async fn get_store_mtree(
		&self,
		keys: TreeNodeProvider,
		generation: u64,
		tt: TransactionType,
		cache_size: usize,
	) -> MTreeStore {
		let cache = self.0.mtree_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}
}
