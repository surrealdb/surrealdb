use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::{IndexStores, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction, TransactionType};

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	store: BTreeStore<TrieKeys>,
}

impl Postings {
	pub(super) async fn new(
		ixs: &IndexStores,
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		order: u32,
		tt: TransactionType,
		cache_size: u32,
	) -> Result<Self, Error> {
		let state_key: Key = index_key_base.new_bp_key(None);
		let state: BState = if let Some(val) = tx.get(state_key.clone()).await? {
			BState::try_from_val(val)?
		} else {
			BState::new(order)
		};
		let store = ixs
			.get_store_btree_trie(
				TreeNodeProvider::Postings(index_key_base.clone()),
				state.generation(),
				tt,
				cache_size as usize,
			)
			.await;
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
		self.btree.insert(tx, &mut self.store, key, term_freq).await
	}

	pub(super) async fn get_term_frequency(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		self.btree.search(tx, &self.store, &key).await
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		self.btree.delete(tx, &mut self.store, key).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<BStatistics, Error> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(super) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		if self.store.finish(tx).await? {
			let state = self.btree.inc_generation();
			tx.set(self.state_key.clone(), state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType, TransactionType::*};
	use test_log::test;

	async fn new_operation(
		ds: &Datastore,
		order: u32,
		tt: TransactionType,
	) -> (Transaction, Postings) {
		let mut tx = ds.transaction(tt, Optimistic).await.unwrap();
		let p = Postings::new(ds.index_store(), &mut tx, IndexKeyBase::default(), order, tt, 100)
			.await
			.unwrap();
		(tx, p)
	}

	async fn finish(mut tx: Transaction, mut p: Postings) {
		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: u32 = 5;

		let ds = Datastore::new("memory").await.unwrap();

		{
			// Check empty state
			let (tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Write).await;
			finish(tx, p).await;

			let (mut tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

			// Add postings
			let (mut tx, mut p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Write).await;
			p.update_posting(&mut tx, 1, 2, 3).await.unwrap();
			p.update_posting(&mut tx, 1, 4, 5).await.unwrap();
			finish(tx, p).await;

			let (mut tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 2);

			assert_eq!(p.get_term_frequency(&mut tx, 1, 2).await.unwrap(), Some(3));
			assert_eq!(p.get_term_frequency(&mut tx, 1, 4).await.unwrap(), Some(5));

			let (mut tx, mut p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Write).await;
			// Check removal of doc 2
			assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), Some(3));
			// Again the same
			assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), None);
			// Remove doc 4
			assert_eq!(p.remove_posting(&mut tx, 1, 4).await.unwrap(), Some(5));
			finish(tx, p).await;

			// The underlying b-tree should be empty now
			let (mut tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);
		}
	}
}
