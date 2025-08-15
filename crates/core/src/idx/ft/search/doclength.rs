use anyhow::Result;

use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
use crate::idx::ft::DocLength;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore, Payload};
use crate::idx::trees::store::TreeNodeProvider;
use crate::kvs::{Transaction, TransactionType};

pub(super) struct DocLengths {
	ikb: IndexKeyBase,
	btree: BTree<TrieKeys>,
	store: BTreeStore<TrieKeys>,
}

impl DocLengths {
	pub(super) async fn new(
		tx: &Transaction,
		ikb: IndexKeyBase,
		default_btree_order: u32,
		tt: TransactionType,
		cache_size: u32,
	) -> Result<Self> {
		let state_key = ikb.new_bl_root_key();
		let state: BState = if let Some(val) = tx.get(&state_key, None).await? {
			val
		} else {
			BState::new(default_btree_order)
		};
		let store = tx
			.index_caches()
			.get_store_btree_trie(
				TreeNodeProvider::DocLengths(ikb.clone()),
				state.generation(),
				tt,
				cache_size as usize,
			)
			.await?;
		Ok(Self {
			ikb,
			btree: BTree::new(state),
			store,
		})
	}

	pub(super) async fn get_doc_length(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<DocLength>> {
		self.btree.search(tx, &self.store, &doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn get_doc_length_mut(
		&mut self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<DocLength>> {
		self.btree.search_mut(tx, &mut self.store, &doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn set_doc_length(
		&mut self,
		tx: &Transaction,
		doc_id: DocId,
		doc_length: DocLength,
	) -> Result<()> {
		self.btree.insert(tx, &mut self.store, doc_id.to_be_bytes().to_vec(), doc_length).await?;
		Ok(())
	}

	pub(super) async fn remove_doc_length(
		&mut self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<Payload>> {
		self.btree.delete(tx, &mut self.store, doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn statistics(&self, tx: &Transaction) -> Result<BStatistics> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(super) async fn finish(&mut self, tx: &Transaction) -> Result<()> {
		if let Some(new_cache) = self.store.finish(tx).await? {
			let state = self.btree.inc_generation();
			let state_key = self.ikb.new_bl_root_key();
			tx.set(&state_key, state, None).await?;
			tx.index_caches().advance_store_btree_trie(new_cache);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::idx::IndexKeyBase;
	use crate::idx::ft::search::doclength::DocLengths;
	use crate::kvs::LockType::*;
	use crate::kvs::{Datastore, Transaction, TransactionType};

	async fn doc_length(
		ds: &Datastore,
		order: u32,
		tt: TransactionType,
	) -> (Transaction, DocLengths) {
		let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
		let dl = DocLengths::new(
			&tx,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
			order,
			tt,
			100,
		)
		.await
		.unwrap();
		(tx, dl)
	}

	async fn finish(mut l: DocLengths, tx: Transaction) {
		l.finish(&tx).await.unwrap();
		tx.commit().await.unwrap()
	}

	#[tokio::test]
	async fn test_doc_lengths() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();

		{
			// Check empty state
			let (tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&tx).await.unwrap().keys_count, 0);
			let dl = l.get_doc_length(&tx, 99).await.unwrap();
			assert_eq!(dl, None);
			tx.cancel().await.unwrap();
		}

		{
			// Set a doc length
			let (tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			l.set_doc_length(&tx, 99, 199).await.unwrap();
			finish(l, tx).await;
		}

		{
			let (tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&tx, 99).await.unwrap();
			assert_eq!(dl, Some(199));
			tx.cancel().await.unwrap();
		}

		{
			// Update doc length
			let (tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			l.set_doc_length(&tx, 99, 299).await.unwrap();
			finish(l, tx).await;
		}

		{
			let (tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&tx, 99).await.unwrap();
			assert_eq!(dl, Some(299));
			tx.cancel().await.unwrap();
		}

		{
			// Remove doc lengths
			let (tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			assert_eq!(l.remove_doc_length(&tx, 99).await.unwrap(), Some(299));
			assert_eq!(l.remove_doc_length(&tx, 99).await.unwrap(), None);
			finish(l, tx).await;
		}

		{
			let (tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			let dl = l.get_doc_length(&tx, 99).await.unwrap();
			assert_eq!(dl, None);
			tx.cancel().await.unwrap();
		}
	}
}
