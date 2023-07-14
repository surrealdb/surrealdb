use crate::err::Error;
use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::store::{BTreeNodeStore, BTreeStoreType, KeyProvider};
use crate::idx::btree::{BTree, Payload, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) type DocLength = u64;

pub(super) struct DocLengths {
	state_key: Key,
	btree: BTree<TrieKeys>,
	store: Arc<Mutex<BTreeNodeStore<TrieKeys>>>,
}

impl DocLengths {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: u32,
		store_type: BTreeStoreType,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bl_key(None);
		let state: btree::State = if let Some(val) = tx.get(state_key.clone()).await? {
			btree::State::try_from_val(val)?
		} else {
			btree::State::new(default_btree_order)
		};
		let store = BTreeNodeStore::new(KeyProvider::DocLengths(index_key_base), store_type, 20);
		Ok(Self {
			state_key,
			btree: BTree::new(state),
			store,
		})
	}

	pub(super) async fn get_doc_length(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<DocLength>, Error> {
		let mut store = self.store.lock().await;
		self.btree.search(tx, &mut store, &doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn set_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
		doc_length: DocLength,
	) -> Result<(), Error> {
		let mut store = self.store.lock().await;
		self.btree.insert(tx, &mut store, doc_id.to_be_bytes().to_vec(), doc_length).await
	}

	pub(super) async fn remove_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<Payload>, Error> {
		let mut store = self.store.lock().await;
		self.btree.delete(tx, &mut store, doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		let mut store = self.store.lock().await;
		self.btree.statistics(tx, &mut store).await
	}

	pub(super) async fn finish(&self, tx: &mut Transaction) -> Result<(), Error> {
		if self.store.lock().await.finish(tx).await? {
			tx.set(self.state_key.clone(), self.btree.get_state().try_to_val()?).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::btree::store::BTreeStoreType;
	use crate::idx::ft::doclength::DocLengths;
	use crate::idx::IndexKeyBase;
	use crate::kvs::Datastore;

	#[tokio::test]
	async fn test_doc_lengths() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();

		// Check empty state
		let mut tx = ds.transaction(true, false).await.unwrap();
		let l = DocLengths::new(
			&mut tx,
			IndexKeyBase::default(),
			BTREE_ORDER,
			BTreeStoreType::Traversal,
		)
		.await
		.unwrap();
		assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 0);
		let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
		assert_eq!(dl, None);

		// Set a doc length
		let mut l =
			DocLengths::new(&mut tx, IndexKeyBase::default(), BTREE_ORDER, BTreeStoreType::Write)
				.await
				.unwrap();
		l.set_doc_length(&mut tx, 99, 199).await.unwrap();
		assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
		let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
		l.finish(&mut tx).await.unwrap();
		assert_eq!(dl, Some(199));

		// Update doc length
		let mut l =
			DocLengths::new(&mut tx, IndexKeyBase::default(), BTREE_ORDER, BTreeStoreType::Write)
				.await
				.unwrap();
		l.set_doc_length(&mut tx, 99, 299).await.unwrap();
		assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
		let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
		l.finish(&mut tx).await.unwrap();
		assert_eq!(dl, Some(299));

		// Remove doc lengths
		let mut l =
			DocLengths::new(&mut tx, IndexKeyBase::default(), BTREE_ORDER, BTreeStoreType::Write)
				.await
				.unwrap();
		assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), Some(299));
		assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), None);
	}
}
