mod memory;
pub mod read;
pub mod write;

use crate::err::Error;
use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::BTreeNode;
use crate::idx::trees::mtree::MTreeNode;
use crate::idx::trees::store::memory::TreeMemoryProvider;
use crate::idx::trees::store::read::{TreeMemoryRead, TreeTransactionRead};
use crate::idx::trees::store::write::{TreeMemoryWrite, TreeTransactionWrite};
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, Val};
use std::fmt::Debug;
use std::sync::Arc;

pub type NodeId = u64;

#[derive(Clone, Copy)]
pub(in crate::idx) enum StoreRights {
	Write,
	Read,
}

#[derive(Clone, Copy)]
pub(in crate::idx) enum StoreProvider {
	Transaction,
	Memory,
}

#[derive(Clone, Copy, PartialEq)]
enum TreeStoreType {
	TransactionWrite,
	TransactionRead,
	MemoryWrite,
	MemoryRead,
}

pub enum TreeStore<'a, N>
where
	N: TreeNode + Debug,
{
	/// caches every read nodes, and keeps track of updated and created nodes
	TransactionWrite(TreeTransactionWrite<N>),
	TransactionRead(TreeTransactionRead<N>),
	MemoryWrite(TreeMemoryWrite<'a, N>),
	/// Nodes are stored in memory with a read guard lock
	MemoryRead(TreeMemoryRead<'a, N>),
}

impl<'a, N> TreeStore<'a, N>
where
	N: TreeNode + Debug,
{
	pub async fn new(
		keys: TreeNodeProvider,
		prov: StoreProvider,
		rights: StoreRights,
		cache_size: usize,
		tree_memory_provider: &TreeMemoryProvider<N>,
	) -> Self {
		match prov {
			StoreProvider::Transaction => match rights {
				StoreRights::Write => Self::Transaction(TreeTransactionWrite::new(keys)),
				StoreRights::Read => Self::Memory(TreeTransactionRead::new(keys, cache_size)),
			},
			StoreProvider::Memory => {
				let keys = tree_memory_provider.get(keys).await;
				match rights {
					StoreRights::Write => Self::Memory(TreeMemoryWrite::new(keys)),
					StoreRights::Read => Self::Memory(TreeMemoryRead::new(keys)),
				}
			}
		}
	}

	pub(in crate::idx) async fn get_node_mut(
		&mut self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			TreeStore::TransactionWrite(w) => w.get_node(tx, node_id).await,
			TreeStore::MemoryWrite(t) => t.get_node(node_id),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn get_node(
		&self,
		tx: &mut Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			TreeStore::TransactionRead(r) => r.get_node(tx, node_id).await,
			TreeStore::MemoryRead(t) => t.get_node(node_id),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn set_node(
		&mut self,
		node: Arc<StoredNode<N>>,
		updated: bool,
	) -> Result<(), Error> {
		match self {
			TreeStore::TransactionWrite(w) => w.set_node(node, updated),
			TreeStore::MemoryWrite(t) => t.set_node(node),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) fn new_node(
		&mut self,
		id: NodeId,
		node: N,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			TreeStore::TransactionWrite(w) => Ok(w.new_node(id, node)),
			TreeStore::MemoryWrite(t) => Ok(t.new_node(id, node)),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn remove_node(
		&mut self,
		node: Arc<StoredNode<N>>,
	) -> Result<(), Error> {
		match self {
			TreeStore::TransactionWrite(w) => w.remove_node(node),
			TreeStore::MemoryWrite(t) => t.remove_node(node.id),
			_ => Err(Error::Unreachable),
		}
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		match self {
			TreeStore::TransactionWrite(w) => w.finish(tx).await,
			TreeStore::MemoryWrite(t) => t.finish(),
			_ => Err(Error::Unreachable),
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

#[derive(Clone)]
pub(crate) struct IndexStores(Arc<Inner>);

struct Inner {
	in_memory_btree_fst: TreeMemoryProvider<BTreeNode<FstKeys>>,
	in_memory_btree_trie: TreeMemoryProvider<BTreeNode<TrieKeys>>,
	in_memory_mtree: TreeMemoryProvider<MTreeNode>,
}
impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			in_memory_btree_fst: TreeMemoryProvider::new(),
			in_memory_btree_trie: TreeMemoryProvider::new(),
			in_memory_mtree: TreeMemoryProvider::new(),
		}))
	}
}

impl IndexStores {
	pub(in crate::idx) async fn get_store_btree_fst(
		&self,
		keys: TreeNodeProvider,
		prov: StoreProvider,
		rights: StoreRights,
		cache_size: usize,
	) -> TreeStore<BTreeNode<FstKeys>> {
		TreeStore::new(keys, prov, rights, cache_size, &self.0.in_memory_btree_fst).await
	}

	pub(in crate::idx) async fn get_store_btree_trie(
		&self,
		keys: TreeNodeProvider,
		prov: StoreProvider,
		rights: StoreRights,
		cache_size: usize,
	) -> TreeStore<BTreeNode<TrieKeys>> {
		TreeStore::new(keys, prov, rights, cache_size, &self.0.in_memory_btree_trie).await
	}

	pub(in crate::idx) async fn get_store_mtree(
		&self,
		keys: TreeNodeProvider,
		prov: StoreProvider,
		rights: StoreRights,
		cache_size: usize,
	) -> TreeStore<MTreeNode> {
		TreeStore::new(keys, prov, rights, cache_size, &self.0.in_memory_mtree).await
	}
}
