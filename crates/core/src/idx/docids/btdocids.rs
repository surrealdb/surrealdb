use crate::err::Error;
use crate::idx::docids::{DocId, Resolved};
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BState1, BState1skip, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::TreeNodeProvider;
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction, TransactionType, Val};
use revision::{Revisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

/// BTree based DocIds store
pub(crate) struct BTreeDocIds {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	store: BTreeStore<TrieKeys>,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl BTreeDocIds {
	pub async fn new(
		tx: &Transaction,
		tt: TransactionType,
		ikb: IndexKeyBase,
		default_btree_order: u32,
		cache_size: u32,
	) -> anyhow::Result<Self> {
		let state_key: Key = ikb.new_bd_key(None)?;
		let state: State = if let Some(val) = tx.get(state_key.clone(), None).await? {
			VersionedStore::try_from(val)?
		} else {
			State::new(default_btree_order)
		};
		let store = tx
			.index_caches()
			.get_store_btree_trie(
				TreeNodeProvider::DocIds(ikb.clone()),
				state.btree.generation(),
				tt,
				cache_size as usize,
			)
			.await?;
		Ok(Self {
			state_key,
			index_key_base: ikb,
			btree: BTree::new(state.btree),
			store,
			available_ids: state.available_ids,
			next_doc_id: state.next_doc_id,
		})
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
		tx: &Transaction,
		doc_key: Key,
	) -> anyhow::Result<Option<DocId>> {
		self.btree.search(tx, &self.store, &doc_key).await
	}

	/// Returns the doc_id for the given doc_key.
	/// If the doc_id does not exists, a new one is created, and associated with the given key.
	pub(in crate::idx) async fn resolve_doc_id(
		&mut self,
		tx: &Transaction,
		doc_key: Key,
	) -> anyhow::Result<Resolved> {
		{
			if let Some(doc_id) = self.btree.search_mut(tx, &mut self.store, &doc_key).await? {
				return Ok(Resolved::Existing(doc_id));
			}
		}
		let doc_id = self.get_next_doc_id();
		let bi = self.index_key_base.new_bi_key(doc_id);
		tx.set(bi, doc_key.clone(), None).await?;
		self.btree.insert(tx, &mut self.store, doc_key, doc_id).await?;
		Ok(Resolved::New(doc_id))
	}

	pub(in crate::idx) async fn remove_doc(
		&mut self,
		tx: &Transaction,
		doc_key: Key,
	) -> anyhow::Result<Option<DocId>> {
		if let Some(doc_id) = self.btree.delete(tx, &mut self.store, doc_key).await? {
			let bi = self.index_key_base.new_bi_key(doc_id);
			tx.del(bi).await?;
			if let Some(available_ids) = &mut self.available_ids {
				available_ids.insert(doc_id);
			} else {
				let mut available_ids = RoaringTreemap::new();
				available_ids.insert(doc_id);
				self.available_ids = Some(available_ids);
			}
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn get_doc_key(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> anyhow::Result<Option<Key>> {
		let doc_id_key = self.index_key_base.new_bi_key(doc_id);
		if let Some(val) = tx.get(doc_id_key, None).await? {
			Ok(Some(val))
		} else {
			Ok(None)
		}
	}

	pub(in crate::idx) async fn statistics(&self, tx: &Transaction) -> anyhow::Result<BStatistics> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(in crate::idx) async fn finish(&mut self, tx: &Transaction) -> anyhow::Result<()> {
		if let Some(new_cache) = self.store.finish(tx).await? {
			let btree = self.btree.inc_generation().clone();
			let state = State {
				btree,
				available_ids: self.available_ids.take(),
				next_doc_id: self.next_doc_id,
			};
			tx.set(self.state_key.clone(), VersionedStore::try_into(&state)?, None).await?;
			tx.index_caches().advance_store_btree_trie(new_cache);
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State {
	btree: BState,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl VersionedStore for State {
	fn try_from(val: Val) -> anyhow::Result<Self> {
		match Self::deserialize_revisioned(&mut val.as_slice()) {
			Ok(r) => Ok(r),
			// If it fails here, there is the chance it was an old version of BState
			// that included the #[serde[skip]] updated parameter
			Err(e) => match State1skip::deserialize_revisioned(&mut val.as_slice()) {
				Ok(b_old) => Ok(b_old.into()),
				Err(_) => match State1::deserialize_revisioned(&mut val.as_slice()) {
					Ok(b_old) => Ok(b_old.into()),
					// Otherwise we return the initial error
					Err(_) => Err(anyhow::Error::new(Error::Revision(e))),
				},
			},
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State1 {
	btree: BState1,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl From<State1> for State {
	fn from(s: State1) -> Self {
		Self {
			btree: s.btree.into(),
			available_ids: s.available_ids,
			next_doc_id: s.next_doc_id,
		}
	}
}

impl VersionedStore for State1 {}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State1skip {
	btree: BState1skip,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl From<State1skip> for State {
	fn from(s: State1skip) -> Self {
		Self {
			btree: s.btree.into(),
			available_ids: s.available_ids,
			next_doc_id: s.next_doc_id,
		}
	}
}

impl VersionedStore for State1skip {}

impl State {
	fn new(default_btree_order: u32) -> Self {
		Self {
			btree: BState::new(default_btree_order),
			available_ids: None,
			next_doc_id: 0,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::btdocids::{BTreeDocIds, Resolved};
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType};

	const BTREE_ORDER: u32 = 7;

	async fn new_operation(ds: &Datastore, tt: TransactionType) -> (Transaction, BTreeDocIds) {
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let d = BTreeDocIds::new(&tx, tt, IndexKeyBase::default(), BTREE_ORDER, 100).await.unwrap();
		(tx, d)
	}

	async fn finish(tx: Transaction, mut d: BTreeDocIds) {
		d.finish(&tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		// Resolve a first doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Foo".into()).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 1);
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some("Foo".into()));
			assert_eq!(doc_id, Resolved::New(0));
		}

		// Resolve the same doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Foo".into()).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 1);
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some("Foo".into()));
			assert_eq!(doc_id, Resolved::Existing(0));
		}

		// Resolve another single doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, "Bar".into()).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 2);
			assert_eq!(d.get_doc_key(&tx, 1).await.unwrap(), Some("Bar".into()));
			assert_eq!(doc_id, Resolved::New(1));
		}

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::New(2));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::New(3));
			finish(tx, d).await;
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 4);
		}

		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::Existing(2));
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::Existing(3));
			finish(tx, d).await;
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some("Foo".into()));
			assert_eq!(d.get_doc_key(&tx, 1).await.unwrap(), Some("Bar".into()));
			assert_eq!(d.get_doc_key(&tx, 2).await.unwrap(), Some("Hello".into()));
			assert_eq!(d.get_doc_key(&tx, 3).await.unwrap(), Some("World".into()));
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 4);
		}
	}

	#[tokio::test]
	async fn test_remove_doc() {
		let ds = Datastore::new("memory").await.unwrap();

		// Create two docs
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Foo".into()).await.unwrap(), Resolved::New(0));
			assert_eq!(d.resolve_doc_id(&tx, "Bar".into()).await.unwrap(), Resolved::New(1));
			finish(tx, d).await;
		}

		// Remove doc 1
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, "Dummy".into()).await.unwrap(), None);
			assert_eq!(d.remove_doc(&tx, "Foo".into()).await.unwrap(), Some(0));
			finish(tx, d).await;
		}

		// Check 'Foo' has been removed
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, "Foo".into()).await.unwrap(), None);
			finish(tx, d).await;
		}

		// Insert a new doc - should take the available id 1
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "Hello".into()).await.unwrap(), Resolved::New(0));
			finish(tx, d).await;
		}

		// Remove doc 2
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, "Dummy".into()).await.unwrap(), None);
			assert_eq!(d.remove_doc(&tx, "Bar".into()).await.unwrap(), Some(1));
			finish(tx, d).await;
		}

		// Check 'Bar' has been removed
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, "Foo".into()).await.unwrap(), None);
			finish(tx, d).await;
		}

		// Insert a new doc - should take the available id 2
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, "World".into()).await.unwrap(), Resolved::New(1));
			finish(tx, d).await;
		}
	}
}
