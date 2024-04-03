pub mod cache;
pub(crate) mod tree;

use crate::dbs::Options;
use crate::err::Error;
use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::{BTreeNode, BTreeStore};
use crate::idx::trees::mtree::{MTreeNode, MTreeStore};
use crate::idx::trees::store::cache::{TreeCache, TreeCaches};
use crate::idx::trees::store::tree::{TreeRead, TreeWrite};
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, TransactionType, Val};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Index;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

pub type NodeId = u64;

#[non_exhaustive]
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
	N: TreeNode + Debug + Display + Clone,
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

	pub async fn finish(&mut self, tx: &mut Transaction) -> Result<bool, Error> {
		match self {
			TreeStore::Write(w) => w.finish(tx).await,
			_ => Ok(false),
		}
	}
}

#[derive(Clone)]
#[non_exhaustive]
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
			Err(Error::CorruptedIndex("TreeStore::load"))
		}
	}

	async fn save<N>(&self, tx: &mut Transaction, mut node: StoredNode<N>) -> Result<(), Error>
	where
		N: TreeNode + Clone + Display,
	{
		let val = node.n.try_into_val()?;
		tx.set(node.key, val).await?;
		Ok(())
	}
}

#[non_exhaustive]
pub struct StoredNode<N>
where
	N: Clone + Display,
{
	pub(super) n: N,
	pub(super) id: NodeId,
	pub(super) key: Key,
	pub(super) size: u32,
}

impl<N> StoredNode<N>
where
	N: Clone + Display,
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

impl<N> Display for StoredNode<N>
where
	N: Clone + Display,
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "node_id: {} - {}", self.id, self.n)
	}
}

pub trait TreeNode: Debug + Clone + Display {
	fn try_from_val(val: Val) -> Result<Self, Error>
	where
		Self: Sized;
	fn try_into_val(&mut self) -> Result<Val, Error>;
}

#[derive(Clone)]
#[non_exhaustive]
pub struct IndexStores(Arc<Inner>);

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

	pub(crate) async fn index_removed(
		&self,
		opt: &Options,
		tx: &mut Transaction,
		tb: &str,
		ix: &str,
	) -> Result<(), Error> {
		self.remove_index(
			opt,
			tx.get_and_cache_tb_index(opt.ns(), opt.db(), tb, ix).await?.as_ref(),
		)
		.await
	}

	pub(crate) async fn namespace_removed(
		&self,
		opt: &Options,
		tx: &mut Transaction,
	) -> Result<(), Error> {
		for tb in tx.all_tb(opt.ns(), opt.db()).await?.iter() {
			self.table_removed(opt, tx, &tb.name).await?;
		}
		Ok(())
	}

	pub(crate) async fn table_removed(
		&self,
		opt: &Options,
		tx: &mut Transaction,
		tb: &str,
	) -> Result<(), Error> {
		for ix in tx.all_tb_indexes(opt.ns(), opt.db(), tb).await?.iter() {
			self.remove_index(opt, ix).await?;
		}
		Ok(())
	}

	async fn remove_index(&self, opt: &Options, ix: &DefineIndexStatement) -> Result<(), Error> {
		let ikb = IndexKeyBase::new(opt, ix);
		match ix.index {
			Index::Search(_) => {
				self.remove_search_cache(ikb).await;
			}
			Index::MTree(_) => {
				self.remove_mtree_cache(ikb).await;
			}
			_ => {}
		}
		Ok(())
	}

	async fn remove_search_cache(&self, ikb: IndexKeyBase) {
		self.0.btree_trie_caches.remove_cache(&TreeNodeProvider::DocIds(ikb.clone())).await;
		self.0.btree_trie_caches.remove_cache(&TreeNodeProvider::DocLengths(ikb.clone())).await;
		self.0.btree_trie_caches.remove_cache(&TreeNodeProvider::Postings(ikb.clone())).await;
		self.0.btree_fst_caches.remove_cache(&TreeNodeProvider::Terms(ikb)).await;
	}

	async fn remove_mtree_cache(&self, ikb: IndexKeyBase) {
		self.0.btree_trie_caches.remove_cache(&TreeNodeProvider::DocIds(ikb.clone())).await;
		self.0.mtree_caches.remove_cache(&TreeNodeProvider::Vector(ikb.clone())).await;
	}

	pub async fn is_empty(&self) -> bool {
		self.0.mtree_caches.is_empty().await
			&& self.0.btree_fst_caches.is_empty().await
			&& self.0.btree_trie_caches.is_empty().await
	}
}
