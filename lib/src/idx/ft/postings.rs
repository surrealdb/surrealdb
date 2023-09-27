use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeNodeStore};
use crate::idx::trees::store::{TreeNodeProvider, TreeNodeStore, TreeStoreType};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	store: Arc<Mutex<BTreeNodeStore<TrieKeys>>>,
}

impl Postings {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		order: u32,
		store_type: TreeStoreType,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bp_key(None);
		let state: BState = if let Some(val) = tx.get(state_key.clone()).await? {
			BState::try_from_val(val)?
		} else {
			BState::new(order)
		};
		let store =
			TreeNodeStore::new(TreeNodeProvider::Postings(index_key_base.clone()), store_type, 20);
		Ok(Self {
			state_key,
			index_key_base,
			btree: BTree::new(state),
			store,
		})
	}

	pub(super) async fn update_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) -> Result<(), Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.store.lock().await;
		self.btree.insert(tx, &mut store, key, term_freq).await
	}

	pub(super) async fn get_term_frequency(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.store.lock().await;
		self.btree.search(tx, &mut store, &key).await
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		let mut store = self.store.lock().await;
		self.btree.delete(tx, &mut store, key).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<BStatistics, Error> {
		let mut store = self.store.lock().await;
		self.btree.statistics(tx, &mut store).await
	}

	pub(super) async fn finish(&self, tx: &mut Transaction) -> Result<(), Error> {
		self.store.lock().await.finish(tx).await?;
		self.btree.get_state().finish(tx, &self.state_key).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::trees::store::TreeStoreType;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};
	use test_log::test;

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: u32 = 5;

		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		// Check empty state
		let mut p = Postings::new(
			&mut tx,
			IndexKeyBase::default(),
			DEFAULT_BTREE_ORDER,
			TreeStoreType::Write,
		)
		.await
		.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

		p.update_posting(&mut tx, 1, 2, 3).await.unwrap();
		p.update_posting(&mut tx, 1, 4, 5).await.unwrap();

		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let mut p = Postings::new(
			&mut tx,
			IndexKeyBase::default(),
			DEFAULT_BTREE_ORDER,
			TreeStoreType::Write,
		)
		.await
		.unwrap();
		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 2);

		assert_eq!(p.get_term_frequency(&mut tx, 1, 2).await.unwrap(), Some(3));
		assert_eq!(p.get_term_frequency(&mut tx, 1, 4).await.unwrap(), Some(5));

		// Check removal of doc 2
		assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), Some(3));
		// Again the same
		assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), None);
		// Remove doc 4
		assert_eq!(p.remove_posting(&mut tx, 1, 4).await.unwrap(), Some(5));

		// The underlying b-tree should be empty now
		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);
		tx.commit().await.unwrap();
	}
}
