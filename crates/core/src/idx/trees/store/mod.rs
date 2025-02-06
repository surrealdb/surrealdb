pub mod cache;
pub(crate) mod hnsw;
mod lru;
mod mapper;
pub(crate) mod tree;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::trees::store::cache::TreeCache;
use crate::idx::trees::store::hnsw::{HnswIndexes, SharedHnswIndex};
use crate::idx::trees::store::mapper::Mappers;
use crate::idx::trees::store::tree::{TreeRead, TreeWrite};
use crate::idx::IndexKeyBase;
use crate::kvs::{IndexBuilder, Key, Transaction, TransactionType, Val};
use crate::sql::index::HnswParams;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Index;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

pub type NodeId = u64;
pub type StoreGeneration = u64;

#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
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
			_ => Err(fail!("TreeStore::get_node_mut")),
		}
	}

	pub(in crate::idx) async fn get_node(
		&self,
		tx: &Transaction,
		node_id: NodeId,
	) -> Result<Arc<StoredNode<N>>, Error> {
		match self {
			Self::Read(r) => r.get_node(tx, node_id).await,
			_ => Err(fail!("TreeStore::get_node")),
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
			_ => Err(fail!("TreeStore::get_node_txn")),
		}
	}

	pub(in crate::idx) async fn set_node(
		&mut self,
		node: StoredNode<N>,
		updated: bool,
	) -> Result<(), Error> {
		match self {
			Self::Write(w) => w.set_node(node, updated),
			_ => Err(fail!("TreeStore::set_node")),
		}
	}

	pub(in crate::idx) fn new_node(&mut self, id: NodeId, node: N) -> Result<StoredNode<N>, Error> {
		match self {
			Self::Write(w) => Ok(w.new_node(id, node)?),
			_ => Err(fail!("TreeStore::new_node")),
		}
	}

	pub(in crate::idx) async fn remove_node(
		&mut self,
		node_id: NodeId,
		node_key: Key,
	) -> Result<(), Error> {
		match self {
			Self::Write(w) => w.remove_node(node_id, node_key),
			_ => Err(fail!("TreeStore::remove_node")),
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
	pub fn get_key(&self, node_id: NodeId) -> Result<Key, Error> {
		match self {
			TreeNodeProvider::DocIds(ikb) => ikb.new_bd_key(Some(node_id)),
			TreeNodeProvider::DocLengths(ikb) => ikb.new_bl_key(Some(node_id)),
			TreeNodeProvider::Postings(ikb) => ikb.new_bp_key(Some(node_id)),
			TreeNodeProvider::Terms(ikb) => ikb.new_bt_key(Some(node_id)),
			TreeNodeProvider::Vector(ikb) => ikb.new_vm_key(Some(node_id)),
			TreeNodeProvider::Debug => Ok(node_id.to_be_bytes().to_vec()),
		}
	}

	async fn load<N>(&self, tx: &Transaction, id: NodeId) -> Result<StoredNode<N>, Error>
	where
		N: TreeNode + Clone,
	{
		let key = self.get_key(id)?;
		if let Some(val) = tx.get(key.clone(), None).await? {
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
		tx.set(node.key.clone(), val, None).await?;
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
	hnsw_indexes: HnswIndexes,
	mappers: Mappers,
}

impl Default for IndexStores {
	fn default() -> Self {
		Self(Arc::new(Inner {
			hnsw_indexes: HnswIndexes::default(),
			mappers: Mappers::default(),
		}))
	}
}

impl IndexStores {
	pub(crate) async fn get_index_hnsw(
		&self,
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		p: &HnswParams,
	) -> Result<SharedHnswIndex, Error> {
		let (ns, db) = opt.ns_db()?;
		let ikb = IndexKeyBase::new(ns, db, ix)?;
		self.0.hnsw_indexes.get(ctx, &ix.what, &ikb, p).await
	}

	pub(crate) async fn index_removed(
		&self,
		#[cfg(not(target_family = "wasm"))] ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<(), Error> {
		#[cfg(not(target_family = "wasm"))]
		if let Some(ib) = ib {
			ib.remove_index(ns, db, tb, ix)?;
		}
		self.remove_index(ns, db, tx.get_tb_index(ns, db, tb, ix).await?.as_ref()).await
	}

	pub(crate) async fn namespace_removed(
		&self,
		#[cfg(not(target_family = "wasm"))] ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: &str,
	) -> Result<(), Error> {
		for db in tx.all_db(ns).await?.iter() {
			#[cfg(not(target_family = "wasm"))]
			self.database_removed(ib, tx, ns, &db.name).await?;
			#[cfg(target_family = "wasm")]
			self.database_removed(tx, ns, &db.name).await?;
		}
		Ok(())
	}

	pub(crate) async fn database_removed(
		&self,
		#[cfg(not(target_family = "wasm"))] ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: &str,
		db: &str,
	) -> Result<(), Error> {
		for tb in tx.all_tb(ns, db, None).await?.iter() {
			#[cfg(not(target_family = "wasm"))]
			self.table_removed(ib, tx, ns, db, &tb.name).await?;
			#[cfg(target_family = "wasm")]
			self.table_removed(tx, ns, db, &tb.name).await?;
		}
		Ok(())
	}

	pub(crate) async fn table_removed(
		&self,
		#[cfg(not(target_family = "wasm"))] ib: Option<&IndexBuilder>,
		tx: &Transaction,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<(), Error> {
		for ix in tx.all_tb_indexes(ns, db, tb).await?.iter() {
			#[cfg(not(target_family = "wasm"))]
			if let Some(ib) = ib {
				ib.remove_index(ns, db, tb, &ix.name)?;
			}
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
		if matches!(ix.index, Index::Hnsw(_)) {
			let ikb = IndexKeyBase::new(ns, db, ix)?;
			self.remove_hnsw_index(ikb).await?;
		}
		Ok(())
	}

	async fn remove_hnsw_index(&self, ikb: IndexKeyBase) -> Result<(), Error> {
		self.0.hnsw_indexes.remove(&ikb).await?;
		Ok(())
	}

	pub(crate) fn mappers(&self) -> &Mappers {
		&self.0.mappers
	}
}
