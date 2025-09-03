use anyhow::Result;

use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::ft::TermFrequency;
use crate::idx::ft::search::terms::TermId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore};
use crate::idx::trees::store::TreeNodeProvider;
use crate::kvs::{KVKey, Transaction, TransactionType};

pub(super) struct Postings {
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
	store: BTreeStore<TrieKeys>,
}

impl Postings {
	pub(super) async fn new(
		tx: &Transaction,
		index_key_base: IndexKeyBase,
		order: u32,
		tt: TransactionType,
		cache_size: u32,
	) -> Result<Self> {
		let state_key = index_key_base.new_bp_root_key();
		let state: BState = if let Some(val) = tx.get(&state_key, None).await? {
			val
		} else {
			BState::new(order)
		};
		let store = tx
			.index_caches()
			.get_store_btree_trie(
				TreeNodeProvider::Postings(index_key_base.clone()),
				state.generation(),
				tt,
				cache_size as usize,
			)
			.await?;
		Ok(Self {
			index_key_base,
			btree: BTree::new(state),
			store,
		})
	}

	pub(super) async fn update_posting(
		&mut self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) -> Result<()> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id).encode_key()?;
		self.btree.insert(tx, &mut self.store, key, term_freq).await
	}

	pub(super) async fn get_term_frequency(
		&self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id).encode_key()?;
		self.btree.search(tx, &self.store, &key).await
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id).encode_key()?;
		self.btree.delete(tx, &mut self.store, key).await
	}

	pub(super) async fn statistics(&self, tx: &Transaction) -> Result<BStatistics> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(super) async fn finish(&mut self, tx: &Transaction) -> Result<()> {
		if let Some(new_cache) = self.store.finish(tx).await? {
			let state = self.btree.inc_generation();
			let state_key = self.index_key_base.new_bp_root_key();
			tx.set(&state_key, state, None).await?;
			tx.index_caches().advance_store_btree_trie(new_cache);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use test_log::test;

	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::idx::IndexKeyBase;
	use crate::idx::ft::search::postings::Postings;
	use crate::kvs::LockType::*;
	use crate::kvs::TransactionType::*;
	use crate::kvs::{Datastore, Transaction, TransactionType};

	async fn new_operation(
		ds: &Datastore,
		order: u32,
		tt: TransactionType,
	) -> (Transaction, Postings) {
		let tx = ds.transaction(tt, Optimistic).await.unwrap();
		let p = Postings::new(
			&tx,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
			order,
			tt,
			100,
		)
		.await
		.unwrap();
		(tx, p)
	}

	async fn finish(tx: Transaction, mut p: Postings) {
		p.finish(&tx).await.unwrap();
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

			let (tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&tx).await.unwrap().keys_count, 0);

			// Add postings
			let (tx, mut p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Write).await;
			p.update_posting(&tx, 1, 2, 3).await.unwrap();
			p.update_posting(&tx, 1, 4, 5).await.unwrap();
			finish(tx, p).await;

			let (tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&tx).await.unwrap().keys_count, 2);

			assert_eq!(p.get_term_frequency(&tx, 1, 2).await.unwrap(), Some(3));
			assert_eq!(p.get_term_frequency(&tx, 1, 4).await.unwrap(), Some(5));

			let (tx, mut p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Write).await;
			// Check removal of doc 2
			assert_eq!(p.remove_posting(&tx, 1, 2).await.unwrap(), Some(3));
			// Again the same
			assert_eq!(p.remove_posting(&tx, 1, 2).await.unwrap(), None);
			// Remove doc 4
			assert_eq!(p.remove_posting(&tx, 1, 4).await.unwrap(), Some(5));
			finish(tx, p).await;

			// The underlying b-tree should be empty now
			let (tx, p) = new_operation(&ds, DEFAULT_BTREE_ORDER, Read).await;
			assert_eq!(p.statistics(&tx).await.unwrap().keys_count, 0);
		}
	}
}
