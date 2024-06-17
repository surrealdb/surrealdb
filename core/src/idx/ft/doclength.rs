use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore, Payload};
use crate::idx::trees::store::{IndexStores, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedStore};
use crate::kvs::{Key, Transaction, TransactionType};

pub(super) type DocLength = u64;

pub(super) struct DocLengths {
	ixs: IndexStores,
	state_key: Key,
	btree: BTree<TrieKeys>,
	store: BTreeStore<TrieKeys>,
}

impl DocLengths {
	pub(super) async fn new(
		ixs: &IndexStores,
		tx: &mut Transaction,
		ikb: IndexKeyBase,
		default_btree_order: u32,
		tt: TransactionType,
		cache_size: u32,
	) -> Result<Self, Error> {
		let state_key: Key = ikb.new_bl_key(None);
		let state: BState = if let Some(val) = tx.get(state_key.clone()).await? {
			VersionedStore::try_from(val)?
		} else {
			BState::new(default_btree_order)
		};
		let store = ixs
			.get_store_btree_trie(
				TreeNodeProvider::DocLengths(ikb),
				state.generation(),
				tt,
				cache_size as usize,
			)
			.await;
		Ok(Self {
			ixs: ixs.clone(),
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
		self.btree.search(tx, &self.store, &doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn get_doc_length_mut(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<DocLength>, Error> {
		self.btree.search_mut(tx, &mut self.store, &doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn set_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
		doc_length: DocLength,
	) -> Result<(), Error> {
		self.btree.insert(tx, &mut self.store, doc_id.to_be_bytes().to_vec(), doc_length).await?;
		Ok(())
	}

	pub(super) async fn remove_doc_length(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
	) -> Result<Option<Payload>, Error> {
		self.btree.delete(tx, &mut self.store, doc_id.to_be_bytes().to_vec()).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<BStatistics, Error> {
		self.btree.statistics(tx, &self.store).await
	}

	pub(super) async fn finish(&mut self, tx: &mut Transaction) -> Result<(), Error> {
		if let Some(new_cache) = self.store.finish(tx).await? {
			let state = self.btree.inc_generation();
			tx.set(self.state_key.clone(), VersionedStore::try_into(state)?).await?;
			self.ixs.advance_cache_btree_trie(new_cache);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::doclength::DocLengths;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType};

	async fn doc_length(
		ds: &Datastore,
		order: u32,
		tt: TransactionType,
	) -> (Transaction, DocLengths) {
		let mut tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
		let dl =
			DocLengths::new(ds.index_store(), &mut tx, IndexKeyBase::default(), order, tt, 100)
				.await
				.unwrap();
		(tx, dl)
	}

	async fn finish(mut l: DocLengths, mut tx: Transaction) {
		l.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap()
	}

	#[tokio::test]
	async fn test_doc_lengths() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();

		{
			// Check empty state
			let (mut tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 0);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			assert_eq!(dl, None);
			tx.cancel().await.unwrap();
		}

		{
			// Set a doc length
			let (mut tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			l.set_doc_length(&mut tx, 99, 199).await.unwrap();
			finish(l, tx).await;
		}

		{
			let (mut tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			assert_eq!(dl, Some(199));
			tx.cancel().await.unwrap();
		}

		{
			// Update doc length
			let (mut tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			l.set_doc_length(&mut tx, 99, 299).await.unwrap();
			finish(l, tx).await;
		}

		{
			let (mut tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			assert_eq!(dl, Some(299));
			tx.cancel().await.unwrap();
		}

		{
			// Remove doc lengths
			let (mut tx, mut l) = doc_length(&ds, BTREE_ORDER, TransactionType::Write).await;
			assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), Some(299));
			assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), None);
			finish(l, tx).await;
		}

		{
			let (mut tx, l) = doc_length(&ds, BTREE_ORDER, TransactionType::Read).await;
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			assert_eq!(dl, None);
			tx.cancel().await.unwrap();
		}
	}
}
