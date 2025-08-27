use revision::{Revisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, Resolved};
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BState1, BState1skip, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::TreeNodeProvider;
use crate::kvs::{KVValue, Key, Transaction, TransactionType};
use crate::val::RecordId;

/// BTree based DocIds store
pub(crate) struct BTreeDocIds {
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
		let state_key = ikb.new_bd_root_key();
		let state: BTreeDocIdsState = if let Some(val) = tx.get(&state_key, None).await? {
			val
		} else {
			BTreeDocIdsState::new(default_btree_order)
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
	/// If the doc_id does not exists, a new one is created, and associated with
	/// the given key.
	pub(in crate::idx) async fn resolve_doc_id(
		&mut self,
		tx: &Transaction,
		doc_key: &RecordId,
	) -> anyhow::Result<Resolved> {
		let doc_key_bytes = doc_key.kv_encode_value()?;
		{
			if let Some(doc_id) = self.btree.search_mut(tx, &mut self.store, &doc_key_bytes).await?
			{
				return Ok(Resolved::Existing(doc_id));
			}
		}
		let doc_id = self.get_next_doc_id();
		let bi = self.index_key_base.new_bi_key(doc_id);
		tx.set(&bi, doc_key, None).await?;
		self.btree.insert(tx, &mut self.store, doc_key_bytes, doc_id).await?;
		Ok(Resolved::New(doc_id))
	}

	pub(in crate::idx) async fn remove_doc(
		&mut self,
		tx: &Transaction,
		doc_key: &RecordId,
	) -> anyhow::Result<Option<DocId>> {
		let doc_key_bytes = doc_key.kv_encode_value()?;
		if let Some(doc_id) = self.btree.delete(tx, &mut self.store, doc_key_bytes).await? {
			let bi = self.index_key_base.new_bi_key(doc_id);
			tx.del(&bi).await?;
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
	) -> anyhow::Result<Option<RecordId>> {
		let doc_id_key = self.index_key_base.new_bi_key(doc_id);
		if let Some(val) = tx.get(&doc_id_key, None).await? {
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
			let state_key = self.index_key_base.new_bd_root_key();
			let state = BTreeDocIdsState {
				btree,
				available_ids: self.available_ids.take(),
				next_doc_id: self.next_doc_id,
			};
			tx.set(&state_key, &state, None).await?;
			tx.index_caches().advance_store_btree_trie(new_cache);
		}
		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(crate) struct BTreeDocIdsState {
	btree: BState,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl KVValue for BTreeDocIdsState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
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

impl From<State1> for BTreeDocIdsState {
	fn from(s: State1) -> Self {
		Self {
			btree: s.btree.into(),
			available_ids: s.available_ids,
			next_doc_id: s.next_doc_id,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
struct State1skip {
	btree: BState1skip,
	available_ids: Option<RoaringTreemap>,
	next_doc_id: DocId,
}

impl From<State1skip> for BTreeDocIdsState {
	fn from(s: State1skip) -> Self {
		Self {
			btree: s.btree.into(),
			available_ids: s.available_ids,
			next_doc_id: s.next_doc_id,
		}
	}
}

impl BTreeDocIdsState {
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
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::btdocids::{BTreeDocIds, Resolved};
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, LockType, Transaction, TransactionType};
	use crate::val::RecordId;

	const BTREE_ORDER: u32 = 7;

	async fn new_operation(ds: &Datastore, tt: TransactionType) -> (Transaction, BTreeDocIds) {
		let tx = ds.transaction(tt, LockType::Optimistic).await.unwrap();
		let d = BTreeDocIds::new(
			&tx,
			tt,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
			BTREE_ORDER,
			100,
		)
		.await
		.unwrap();
		(tx, d)
	}

	async fn finish(tx: Transaction, mut d: BTreeDocIds) {
		d.finish(&tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[tokio::test]
	async fn test_resolve_doc_id() {
		let ds = Datastore::new("memory").await.unwrap();

		let foo_thing = RecordId::new("Foo".to_owned(), strand!("").to_owned());
		let bar_thing = RecordId::new("Bar".to_owned(), strand!("").to_owned());
		let hello_thing = RecordId::new("Hello".to_owned(), strand!("").to_owned());
		let world_thing = RecordId::new("World".to_owned(), strand!("").to_owned());

		// Resolve a first doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, &foo_thing).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 1);
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some(foo_thing.clone()));
			assert_eq!(doc_id, Resolved::New(0));
		}

		// Resolve the same doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, &foo_thing).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 1);
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some(foo_thing.clone()));
			assert_eq!(doc_id, Resolved::Existing(0));
		}

		// Resolve another single doc key
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			let doc_id = d.resolve_doc_id(&tx, &bar_thing).await.unwrap();
			finish(tx, d).await;

			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 2);
			assert_eq!(d.get_doc_key(&tx, 1).await.unwrap(), Some(bar_thing.clone()));
			assert_eq!(doc_id, Resolved::New(1));
		}

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, &foo_thing).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, &hello_thing).await.unwrap(), Resolved::New(2));
			assert_eq!(d.resolve_doc_id(&tx, &bar_thing).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, &world_thing).await.unwrap(), Resolved::New(3));
			finish(tx, d).await;
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 4);
		}

		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, &foo_thing).await.unwrap(), Resolved::Existing(0));
			assert_eq!(d.resolve_doc_id(&tx, &bar_thing).await.unwrap(), Resolved::Existing(1));
			assert_eq!(d.resolve_doc_id(&tx, &hello_thing).await.unwrap(), Resolved::Existing(2));
			assert_eq!(d.resolve_doc_id(&tx, &world_thing).await.unwrap(), Resolved::Existing(3));
			finish(tx, d).await;
			let (tx, d) = new_operation(&ds, Read).await;
			assert_eq!(d.get_doc_key(&tx, 0).await.unwrap(), Some(foo_thing.clone()));
			assert_eq!(d.get_doc_key(&tx, 1).await.unwrap(), Some(bar_thing.clone()));
			assert_eq!(d.get_doc_key(&tx, 2).await.unwrap(), Some(hello_thing.clone()));
			assert_eq!(d.get_doc_key(&tx, 3).await.unwrap(), Some(world_thing.clone()));
			assert_eq!(d.statistics(&tx).await.unwrap().keys_count, 4);
		}
	}

	#[tokio::test]
	async fn test_remove_doc() {
		let ds = Datastore::new("memory").await.unwrap();

		let foo_thing = RecordId::new("Foo".to_owned(), strand!("").to_owned());
		let bar_thing = RecordId::new("Bar".to_owned(), strand!("").to_owned());
		let dummy_thing = RecordId::new("Dummy".to_owned(), strand!("").to_owned());
		let hello_thing = RecordId::new("Hello".to_owned(), strand!("").to_owned());
		let world_thing = RecordId::new("World".to_owned(), strand!("").to_owned());

		// Create two docs
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, &foo_thing).await.unwrap(), Resolved::New(0));
			assert_eq!(d.resolve_doc_id(&tx, &bar_thing).await.unwrap(), Resolved::New(1));
			finish(tx, d).await;
		}

		// Remove doc 1
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, &dummy_thing).await.unwrap(), None);
			assert_eq!(d.remove_doc(&tx, &foo_thing).await.unwrap(), Some(0));
			finish(tx, d).await;
		}

		// Check 'Foo' has been removed
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, &foo_thing).await.unwrap(), None);
			finish(tx, d).await;
		}

		// Insert a new doc - should take the available id 1
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, &hello_thing).await.unwrap(), Resolved::New(0));
			finish(tx, d).await;
		}

		// Remove doc 2
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, &dummy_thing).await.unwrap(), None);
			assert_eq!(d.remove_doc(&tx, &bar_thing).await.unwrap(), Some(1));
			finish(tx, d).await;
		}

		// Check 'Bar' has been removed
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.remove_doc(&tx, &foo_thing).await.unwrap(), None);
			finish(tx, d).await;
		}

		// Insert a new doc - should take the available id 2
		{
			let (tx, mut d) = new_operation(&ds, Write).await;
			assert_eq!(d.resolve_doc_id(&tx, &world_thing).await.unwrap(), Resolved::New(1));
			finish(tx, d).await;
		}
	}
}
