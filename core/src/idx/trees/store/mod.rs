pub mod cache;
pub(crate) mod hnsw;
mod lru;
pub(crate) mod tree;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::trees::bkeys::{FstKeys, TrieKeys};
use crate::idx::trees::btree::{BTreeNode, BTreeStore};
use crate::idx::trees::mtree::{MTreeNode, MTreeStore};
use crate::idx::trees::store::cache::{TreeCache, TreeCaches};
use crate::idx::trees::store::hnsw::{HnswIndexes, SharedHnswIndex};
use crate::idx::trees::store::tree::{TreeRead, TreeWrite};
use crate::idx::IndexKeyBase;
use crate::kvs::{Key, Transaction, TransactionType, Val};
use crate::sql::index::HnswParams;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Index;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

pub type NodeId = u64;
pub type StoreGeneration = u64;

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
	pub async fn new(np: TreeNodeProvider, cache: Arc<TreeCache<N>>, tt: TransactionType) -> Self {
		match tt {
			TransactionType::Read => Self::Read(TreeRead::new(cache)),
			TransactionType::Write => Self::Write(TreeWrite::new(np, cache)),
		}
	}

	pub(in crate::idx) async fn get_node_mut(
		&mut self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<StoredNode<N>, Error> {
		match self {
			Self::Write(w) => w.get_node_mut(tx, node_id).await,
			_ => Err(Error::Unreachable("TreeStore::get_node_mut")),
		}
	}

	pub(in crate::idx) async fn get_node(
		&self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			Self::Read(r) => r.get_node(tx, node_id).await,
			_ => Err(Error::Unreachable("TreeStore::get_node")),
		}
	}

	pub(in crate::idx) async fn get_node_txn(
		&self,
		ctx: &Context,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			Self::Read(r) => {
				let tx = ctx.tx();
				r.get_node(&tx, node_id).await
			}
			_ => Err(Error::Unreachable("TreeStore::get_node_txn")),
		}
	}

	pub(in crate::idx) async fn set_node(
		&mut self,
		node: StoredNode<N>,
		updated: bool,
	) -> Result<(), Error> {
		match self {
			Self::Write(w) => w.set_node(node, updated),
			_ => Err(Error::Unreachable("TreeStore::set_node")),
		}
	}

	pub(in crate::idx) fn new_node(&mut self, id: NodeId, node: N) -> Result<StoredNode<N>, Error> {
		match self {
			Self::Write(w) => Ok(w.new_node(id, node)),
			_ => Err(Error::Unreachable("TreeStore::new_node")),
		}
	}

	pub(in crate::idx) async fn remove_node(
		&mut self,
		node_id: NodeId,
		node_key: Key,
	) -> Result<(), Error> {
		match self {
			Self::Write(w) => w.remove_node(node_id, node_key),
			_ => Err(Error::Unreachable("TreeStore::remove_node")),
		}
	}

	pub async fn finish(&mut self, tx: &Transaction) -> Result<Option<TreeCache<N>>, Error> {
		match self {
			Self::Write(w) => w.finish(tx).await,
			_ => Ok(None),
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
	pub fn get_key(&self, node_id: NodeId) -> Key {
		match self {
			TreeNodeProvider::DocIds(ikb) => ikb.new_bd_key(Some(node_id)),
			TreeNodeProvider::DocLengths(ikb) => ikb.new_bl_key(Some(node_id)),
			TreeNodeProvider::Postings(ikb) => ikb.new_bp_key(Some(node_id)),
			TreeNodeProvider::Terms(ikb) => ikb.new_bt_key(Some(node_id)),
			TreeNodeProvider::Vector(ikb) => ikb.new_vm_key(Some(node_id)),
			TreeNodeProvider::Debug => node_id.to_be_bytes().to_vec(),
		}
	}

	async fn load<N>(&self, tx: &Transaction, id: NodeId) -> Result<StoredNode<N>, Error>
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

	async fn save<N>(&self, tx: &Transaction, node: &mut StoredNode<N>) -> Result<(), Error>
	where
		N: TreeNode + Clone + Display,
	{
		let val = node.n.try_into_val()?;
		node.size = val.len() as u32;
		tx.set(node.key.clone(), val).await?;
		Ok(())
	}
}

#[non_exhaustive]
#[derive(Debug)]
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
	fn prepare_save(&mut self) {}
	fn try_from_val(val: Val) -> Result<Self, Error>
	where
		Self: Sized;
	fn try_into_val(&self) -> Result<Val, Error>;
}

#[derive(Clone)]
#[non_exhaustive]
pub struct IndexStores(Arc<Inner>);

struct Inner {
	btree_fst_caches: TreeCaches<BTreeNode<FstKeys>>,
	btree_trie_caches: TreeCaches<BTreeNode<TrieKeys>>,
	mtree_caches: TreeCaches<MTreeNode>,
	hnsw_indexes: HnswIndexes,
}
impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			btree_fst_caches: TreeCaches::default(),
			btree_trie_caches: TreeCaches::default(),
			mtree_caches: TreeCaches::default(),
			hnsw_indexes: HnswIndexes::default(),
		}))
	}
}

impl IndexStores {
	pub async fn get_store_btree_fst(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> BTreeStore<FstKeys> {
		let cache = self.0.btree_fst_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}

	pub fn advance_store_btree_fst(&self, new_cache: TreeCache<BTreeNode<FstKeys>>) {
		self.0.btree_fst_caches.new_cache(new_cache);
	}

	pub async fn get_store_btree_trie(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> BTreeStore<TrieKeys> {
		let cache = self.0.btree_trie_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}

	pub fn advance_cache_btree_trie(&self, new_cache: TreeCache<BTreeNode<TrieKeys>>) {
		self.0.btree_trie_caches.new_cache(new_cache);
	}

	pub async fn get_store_mtree(
		&self,
		keys: TreeNodeProvider,
		generation: StoreGeneration,
		tt: TransactionType,
		cache_size: usize,
	) -> MTreeStore {
		let cache = self.0.mtree_caches.get_cache(generation, &keys, cache_size).await;
		TreeStore::new(keys, cache, tt).await
	}

	pub fn advance_store_mtree(&self, new_cache: TreeCache<MTreeNode>) {
		self.0.mtree_caches.new_cache(new_cache);
	}

	pub(crate) async fn get_index_hnsw(
		&self,
		opt: &Options,
		ix: &DefineIndexStatement,
		p: &HnswParams,
	) -> Result<SharedHnswIndex, Error> {
		let ikb = IndexKeyBase::new(opt.ns()?, opt.db()?, ix)?;
		Ok(self.0.hnsw_indexes.get(&ikb, p).await)
	}

	pub(crate) async fn index_removed(
		&self,
		tx: &Transaction,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<(), Error> {
		self.remove_index(ns, db, tx.get_tb_index(ns, db, tb, ix).await?.as_ref()).await
	}

	pub(crate) async fn namespace_removed(&self, tx: &Transaction, ns: &str) -> Result<(), Error> {
		for db in tx.all_db(ns).await?.iter() {
			self.database_removed(tx, ns, &db.name).await?;
		}
		Ok(())
	}

	pub(crate) async fn database_removed(
		&self,
		tx: &Transaction,
		ns: &str,
		db: &str,
	) -> Result<(), Error> {
		for tb in tx.all_tb(ns, db).await?.iter() {
			self.table_removed(tx, ns, db, &tb.name).await?;
		}
		Ok(())
	}

	pub(crate) async fn table_removed(
		&self,
		tx: &Transaction,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<(), Error> {
		for ix in tx.all_tb_indexes(ns, db, tb).await?.iter() {
			self.remove_index(ns, db, ix).await?;
		}
		Ok(())
	}

	async fn remove_index(
		&self,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Result<(), Error> {
		let ikb = IndexKeyBase::new(ns, db, ix)?;
		match ix.index {
			Index::Search(_) => {
				self.remove_search_caches(ikb);
			}
			Index::MTree(_) => {
				self.remove_mtree_caches(ikb);
			}
			Index::Hnsw(_) => {
				self.remove_hnsw_index(ikb).await;
			}
			_ => {}
		}
		Ok(())
	}

	fn remove_search_caches(&self, ikb: IndexKeyBase) {
		self.0.btree_trie_caches.remove_caches(&TreeNodeProvider::DocIds(ikb.clone()));
		self.0.btree_trie_caches.remove_caches(&TreeNodeProvider::DocLengths(ikb.clone()));
		self.0.btree_trie_caches.remove_caches(&TreeNodeProvider::Postings(ikb.clone()));
		self.0.btree_fst_caches.remove_caches(&TreeNodeProvider::Terms(ikb));
	}

	fn remove_mtree_caches(&self, ikb: IndexKeyBase) {
		self.0.btree_trie_caches.remove_caches(&TreeNodeProvider::DocIds(ikb.clone()));
		self.0.mtree_caches.remove_caches(&TreeNodeProvider::Vector(ikb.clone()));
	}

	async fn remove_hnsw_index(&self, ikb: IndexKeyBase) {
		self.0.hnsw_indexes.remove(&ikb).await;
	}

	pub async fn is_empty(&self) -> bool {
		self.0.mtree_caches.is_empty()
			&& self.0.btree_fst_caches.is_empty()
			&& self.0.btree_trie_caches.is_empty()
			&& self.0.hnsw_indexes.is_empty().await
	}
}
