use crate::err::Error;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BStatistics, BTree, BTreeNode};
use crate::idx::trees::store::memory::ShardedTreeMemoryMap;
use crate::idx::trees::store::{
	IndexStores, StoreProvider, StoreRights, TreeNodeProvider, TreeStore,
};
use crate::idx::{trees, IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

pub type DocId = u64;

pub(crate) const NO_DOC_ID: u64 = u64::MAX;

pub(crate) struct DocIds {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	index_stores: IndexStores,
	mem_store: Option<ShardedTreeMemoryMap<BTreeNode<TrieKeys>>>,
	tree_node_provider: TreeNodeProvider,
	store_provider: StoreProvider,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
	updated: bool,
}

impl DocIds {
	pub(in crate::idx) async fn new(
		ixs: IndexStores,
		tx: &mut Transaction,
		sp: StoreProvider,
		ikb: IndexKeyBase,
		default_btree_order: u32,
	) -> Result<Self, Error> {
		let state_key: Key = ikb.new_bd_key(None);
		let state: State = if let Some(val) = tx.get(state_key.clone()).await? {
			State::try_from_val(val)?
		} else {
			State::new(default_btree_order)
		};
		let tree_node_provider = TreeNodeProvider::DocIds(ikb.clone());
		let mem_store = ixs.get_mem_store_btree_trie(&tree_node_provider, sp).await;
		Ok(Self {
			state_key,
			index_key_base: ikb,
			btree: BTree::new(state.btree),
			index_stores: ixs,
			mem_store,
			tree_node_provider,
			store_provider: sp,
			available_ids: state.available_ids,
			next_doc_id: state.next_doc_id,
			updated: false,
		})
	}

	async fn get_store(&self, rights: StoreRights) -> TreeStore<BTreeNode<TrieKeys>> {
		self.index_stores
			.get_store_btree_trie(
				self.tree_node_provider.clone(),
				self.store_provider,
				rights,
				20, // TODO: Replace by configuration
			)
			.await
	}

	fn get_next_doc_id(&mut self) -> DocId {
		// We check first if there is any available id
		if let Some(available_ids) = &mut self.available_ids {
			if let Some(available_id) = available_ids.iter().next() {
				available_ids.remove(available_id);
				if available_ids.is_empty() {
					self.available_ids = None;
				}
				return available_id;
			}
		}
		// If not, we use the sequence
		let doc_id = self.next_doc_id;
		self.next_doc_id += 1;
		doc_id
	}

	pub(crate) async fn get_doc_id(
		&self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Option<DocId>, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		let mem = if let Some(mem_store) = &self.mem_store {
			Some(mem_store.read().await)
		} else {
			None
		};
		let res = self.btree.search(tx, &mem, &mut store, &doc_key).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	/// Returns the doc_id for the given doc_key.
	/// If the doc_id does not exists, a new one is created, and associated to the given key.
	pub(in crate::idx) async fn resolve_doc_id(
		&mut self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Resolved, Error> {
		{
			let mut store = self.get_store(StoreRights::Read).await;
			let mem = if let Some(mem_store) = &self.mem_store {
				Some(mem_store.read().await)
			} else {
				None
			};
			if let Some(doc_id) = self.btree.search(tx, &mem, &mut store, &doc_key).await? {
				return Ok(Resolved::Existing(doc_id));
			}
			store.finish(tx).await?;
		}
		let doc_id = self.get_next_doc_id();
		tx.set(self.index_key_base.new_bi_key(doc_id), doc_key.clone()).await?;
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = if let Some(mem_store) = &self.mem_store {
			Some(mem_store.write().await)
		} else {
			None
		};
		self.btree.insert(tx, &mut mem, &mut store, doc_key, doc_id).await?;
		store.finish(tx).await?;
		self.updated = true;
		Ok(Resolved::New(doc_id))
	}

	pub(in crate::idx) async fn remove_doc(
		&mut self,
		tx: &mut Transaction,
		doc_key: Key,
	) -> Result<Option<DocId>, Error> {
		let mut store = self.get_store(StoreRights::Write).await;
		let mut mem = if let Some(mem_store) = &self.mem_store {
			Some(mem_store.write().await)
		} else {
			None
		};
		let res = if let Some(doc_id) = self.btree.delete(tx, &mut mem, &mut store, doc_key).await?
		{
			tx.del(self.index_key_base.new_bi_key(doc_id)).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(doc_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(doc_id);
				self.available_ids = Some(available_ids);
			}
			self.updated = true;
			Some(doc_id)
		} else {
			None
		};
		store.finish(tx).await?;
		Ok(res)
	}

	pub(in crate::idx) async fn get_doc_key(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<Key>, Error> {
		let doc_id_key = self.index_key_base.new_bi_key(doc_id);
		if let Some(val) = tx.get(doc_id_key).await? {
			Ok(Some(val))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn statistics(
		&self,
		tx: &mut Transaction,
	) -> Result<BStatistics, Error> {
		let mut store = self.get_store(StoreRights::Read).await;
		let mem = if let Some(mem_store) = &self.mem_store {
			Some(mem_store.read().await)
		} else {
			None
		};
		let res = self.btree.statistics(tx, &mem, &mut store).await?;
		store.finish(tx).await?;
		Ok(res)
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		if self.updated {
			// TODO: The state should be handled by the IndexStores too
			let state = State {
				btree: self.btree.get_state().clone(),
				available_ids: self.available_ids.take(),
				next_doc_id: self.next_doc_id,
			};
			tx.set(self.state_key.clone(), state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
#[revisioned(revision = 1)]
struct State {
	btree: trees::btree::BState,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl VersionedSerdeState for State {}

impl State {
	fn new(default_btree_order: u32) -> Self {
		Self {
			btree: trees::btree::BState::new(default_btree_order),
			available_ids: None,
			next_doc_id: 0,
		}
	}
}

#[derive(Debug, PartialEq)]
pub(in crate::idx) enum Resolved {
	New(DocId),
	Existing(DocId),
}

impl Resolved {
	pub(in crate::idx) fn doc_id(&self) -> &DocId {
		match self {
			Resolved::New(doc_id) => doc_id,
			Resolved::Existing(doc_id) => doc_id,
		}
	}

	pub(in crate::idx) fn was_existing(&self) -> bool {
		match self {
			Resolved::New(_) => false,
			Resolved::Existing(_) => true,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::docids::{DocIds, Resolved};
	use crate::idx::trees::store::{IndexStores, StoreProvider};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType::*};

	const BTREE_ORDER: u32 = 7;

	async fn get_doc_ids(
		ds: &Datastore,
		ixs: IndexStores,
		st: StoreProvider,
	) -> (Transaction, DocIds) {
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let d = DocIds::new(ixs, &mut tx, st, IndexKeyBase::default(), BTREE_ORDER).await.unwrap();
		(tx, d)
	}

	async fn finish(mut tx: Transaction, mut d: DocIds) {
		d.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_resolve_doc_id() {
		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			// Resolve a first doc key
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				let doc_id = d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap();
				assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
				assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
				finish(tx, d).await;
				assert_eq!(doc_id, Resolved::New(0));
			}

			// Resolve the same doc key
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				let doc_id = d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap();
				assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 1);
				assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
				finish(tx, d).await;
				assert_eq!(doc_id, Resolved::Existing(0));
			}

			// Resolve another single doc key
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				let doc_id = d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap();
				assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 2);
				assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".into()));
				finish(tx, d).await;
				assert_eq!(doc_id, Resolved::New(1));
			}

			// Resolve another two existing doc keys and two new doc keys (interlaced)
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(),
					Resolved::Existing(0)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(),
					Resolved::New(2)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(),
					Resolved::Existing(1)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(),
					Resolved::New(3)
				);
				assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
				finish(tx, d).await;
			}

			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(),
					Resolved::Existing(0)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(),
					Resolved::Existing(1)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(),
					Resolved::Existing(2)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(),
					Resolved::Existing(3)
				);
				assert_eq!(d.get_doc_key(&mut tx, 0).await.unwrap(), Some("Foo".into()));
				assert_eq!(d.get_doc_key(&mut tx, 1).await.unwrap(), Some("Bar".into()));
				assert_eq!(d.get_doc_key(&mut tx, 2).await.unwrap(), Some("Hello".into()));
				assert_eq!(d.get_doc_key(&mut tx, 3).await.unwrap(), Some("World".into()));
				assert_eq!(d.statistics(&mut tx).await.unwrap().keys_count, 4);
				finish(tx, d).await;
			}
		}
	}

	#[tokio::test]
	async fn test_remove_doc() {
		for sp in [StoreProvider::Transaction, StoreProvider::Memory] {
			let ds = Datastore::new("memory").await.unwrap();
			let ixs = IndexStores::default();

			// Create two docs
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Foo".into()).await.unwrap(),
					Resolved::New(0)
				);
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Bar".into()).await.unwrap(),
					Resolved::New(1)
				);
				finish(tx, d).await;
			}

			// Remove doc 1
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(d.remove_doc(&mut tx, "Dummy".into()).await.unwrap(), None);
				assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), Some(0));
				finish(tx, d).await;
			}

			// Check 'Foo' has been removed
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), None);
				finish(tx, d).await;
			}

			// Insert a new doc - should take the available id 1
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(
					d.resolve_doc_id(&mut tx, "Hello".into()).await.unwrap(),
					Resolved::New(0)
				);
				finish(tx, d).await;
			}

			// Remove doc 2
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(d.remove_doc(&mut tx, "Dummy".into()).await.unwrap(), None);
				assert_eq!(d.remove_doc(&mut tx, "Bar".into()).await.unwrap(), Some(1));
				finish(tx, d).await;
			}

			// Check 'Bar' has been removed
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(d.remove_doc(&mut tx, "Foo".into()).await.unwrap(), None);
				finish(tx, d).await;
			}

			// Insert a new doc - should take the available id 2
			{
				let (mut tx, mut d) = get_doc_ids(&ds, ixs.clone(), sp).await;
				assert_eq!(
					d.resolve_doc_id(&mut tx, "World".into()).await.unwrap(),
					Resolved::New(1)
				);
				finish(tx, d).await;
			}
		}
	}
}
