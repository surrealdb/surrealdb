use crate::err::Error;
use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::{BTree, KeyProvider, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::kvs::{Key, Transaction};

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<TrieKeys>,
}

impl Postings {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		order: u32,
	) -> Result<Self, Error> {
		let keys = KeyProvider::Postings(index_key_base.clone());
		let state_key: Key = keys.get_state_key();
		let state: btree::State = if let Some(val) = tx.get(state_key.clone()).await? {
			btree::State::try_from_val(val)?
		} else {
			btree::State::new(order)
		};
		Ok(Self {
			state_key,
			index_key_base,
			btree: BTree::new(keys, state),
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
		self.btree.insert(tx, key, term_freq).await
	}

	pub(super) async fn get_term_frequency(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		self.btree.search(tx, &key).await
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		self.btree.delete(tx, key).await
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.btree.statistics(tx).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.btree.is_updated() {
			tx.set(self.state_key, self.btree.get_state().try_to_val()?).await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::IndexKeyBase;
	use crate::kvs::Datastore;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: u32 = 5;

		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

		// Check empty state
		let mut p =
			Postings::new(&mut tx, IndexKeyBase::default(), DEFAULT_BTREE_ORDER).await.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

		p.update_posting(&mut tx, 1, 2, 3).await.unwrap();
		p.update_posting(&mut tx, 1, 4, 5).await.unwrap();

		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(true, false).await.unwrap();
		let mut p =
			Postings::new(&mut tx, IndexKeyBase::default(), DEFAULT_BTREE_ORDER).await.unwrap();
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
	}
}
