use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::bkeys::TrieKeys;
use crate::idx::trees::btree::{BState, BStatistics, BTree, BTreeStore, Payload};
use crate::idx::trees::store::{IndexStores, TreeNodeProvider};
use crate::idx::{IndexKeyBase, VersionedSerdeState};
use crate::kvs::{Key, Transaction, TransactionType};

pub(super) type DocLength = u64;

pub(super) struct DocLengths {
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
			BState::try_from_val(val)?
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
		let updated = self.store.finish(tx).await?;
		if updated {
			let state = self.btree.finish();
			tx.set(self.state_key.clone(), state.try_to_val()?).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::doclength::DocLengths;
	use crate::idx::trees::store::IndexStores;
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, LockType::*, TransactionType, TransactionType::*};

	#[tokio::test]
	async fn test_doc_lengths() {
		const BTREE_ORDER: u32 = 7;

		let ds = Datastore::new("memory").await.unwrap();
		let ixs = IndexStores::default();

		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();

		{
			// Check empty state
			let l = DocLengths::new(
				&ixs,
				&mut tx,
				IndexKeyBase::default(),
				BTREE_ORDER,
				TransactionType::Read,
				100,
			)
			.await
			.unwrap();
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 0);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			assert_eq!(dl, None);
		}

		{
			// Set a doc length
			let mut l = DocLengths::new(
				&ixs,
				&mut tx,
				IndexKeyBase::default(),
				BTREE_ORDER,
				TransactionType::Write,
				100,
			)
			.await
			.unwrap();
			l.set_doc_length(&mut tx, 99, 199).await.unwrap();
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			l.finish(&mut tx).await.unwrap();
			assert_eq!(dl, Some(199));
		}

		{
			// Update doc length
			let mut l = DocLengths::new(
				&ixs,
				&mut tx,
				IndexKeyBase::default(),
				BTREE_ORDER,
				TransactionType::Write,
				100,
			)
			.await
			.unwrap();
			l.set_doc_length(&mut tx, 99, 299).await.unwrap();
			assert_eq!(l.statistics(&mut tx).await.unwrap().keys_count, 1);
			let dl = l.get_doc_length(&mut tx, 99).await.unwrap();
			l.finish(&mut tx).await.unwrap();
			assert_eq!(dl, Some(299));
		}

		{
			// Remove doc lengths
			let mut l = DocLengths::new(
				&ixs,
				&mut tx,
				IndexKeyBase::default(),
				BTREE_ORDER,
				TransactionType::Write,
				100,
			)
			.await
			.unwrap();
			assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), Some(299));
			assert_eq!(l.remove_doc_length(&mut tx, 99).await.unwrap(), None);
		}
		tx.commit().await.unwrap()
	}
}
