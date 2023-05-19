use crate::err::Error;
use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::{BTree, BTreeIterator, KeyProvider, NodeId, Payload, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::key::bf::Bf;
use crate::kvs::{Key, Transaction};

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<PostingsKeyProvider>,
}

impl Postings {
	pub(super) async fn new(
		tx: &mut Transaction,
		index_key_base: IndexKeyBase,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let keys = PostingsKeyProvider {
			index_key_base: index_key_base.clone(),
		};
		let state_key: Key = keys.get_state_key();
		let state: btree::State = if let Some(val) = tx.get(state_key.clone()).await? {
			btree::State::try_from_val(val)?
		} else {
			btree::State::new(default_btree_order)
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
		self.btree.insert::<TrieKeys>(tx, key, term_freq).await
	}

	pub(super) async fn remove_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
	) -> Result<Option<TermFrequency>, Error> {
		let key = self.index_key_base.new_bf_key(term_id, doc_id);
		self.btree.delete::<TrieKeys>(tx, key).await
	}

	pub(super) fn new_postings_iterator(&self, term_id: TermId) -> PostingsIterator {
		let prefix_key = self.index_key_base.new_bf_prefix_key(term_id);
		let i = self.btree.search_by_prefix(prefix_key);
		PostingsIterator::new(i)
	}

	pub(super) async fn count_postings(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<usize, Error> {
		let mut count = 0;
		let mut i = self.new_postings_iterator(term_id);
		while i.next(tx).await?.is_some() {
			count += 1;
		}
		Ok(count)
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.btree.statistics::<TrieKeys>(tx).await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.btree.is_updated() {
			tx.set(self.state_key, self.btree.get_state().try_to_val()?).await?;
		}
		Ok(())
	}
}

#[derive(Clone)]
pub(super) struct PostingsKeyProvider {
	index_key_base: IndexKeyBase,
}

impl KeyProvider for PostingsKeyProvider {
	fn get_node_key(&self, node_id: NodeId) -> Key {
		self.index_key_base.new_bp_key(Some(node_id))
	}
	fn get_state_key(&self) -> Key {
		self.index_key_base.new_bp_key(None)
	}
}

pub(super) struct PostingsIterator {
	btree_iterator: BTreeIterator<PostingsKeyProvider>,
}

impl PostingsIterator {
	fn new(btree_iterator: BTreeIterator<PostingsKeyProvider>) -> Self {
		Self {
			btree_iterator,
		}
	}

	pub(super) async fn next(
		&mut self,
		tx: &mut Transaction,
	) -> Result<Option<(DocId, Payload)>, Error> {
		Ok(self.btree_iterator.next::<TrieKeys>(tx).await?.map(|(k, p)| {
			let posting_key: Bf = (&k).into();
			(posting_key.doc_id, p)
		}))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::btree::Payload;
	use crate::idx::ft::docids::DocId;
	use crate::idx::ft::postings::{Postings, PostingsIterator};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use std::collections::HashMap;
	use test_log::test;

	async fn check_postings(
		mut i: PostingsIterator,
		tx: &mut Transaction,
		e: Vec<(DocId, Payload)>,
	) {
		let mut map = HashMap::new();
		while let Some((d, p)) = i.next(tx).await.unwrap() {
			map.insert(d, p);
		}
		assert_eq!(map.len(), e.len());
		for (k, p) in e {
			assert_eq!(map.get(&k), Some(&p));
		}
	}

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: usize = 5;

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

		let i = p.new_postings_iterator(1);
		check_postings(i, &mut tx, vec![(2, 3), (4, 5)]).await;

		// Check removal of doc 2
		assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), Some(3));
		assert_eq!(p.count_postings(&mut tx, 1).await.unwrap(), 1);
		// Again the same
		assert_eq!(p.remove_posting(&mut tx, 1, 2).await.unwrap(), None);
		assert_eq!(p.count_postings(&mut tx, 1).await.unwrap(), 1);
		// Remove doc 4
		assert_eq!(p.remove_posting(&mut tx, 1, 4).await.unwrap(), Some(5));
		assert_eq!(p.count_postings(&mut tx, 1).await.unwrap(), 0);

		// The underlying b-tree should be empty now
		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);
	}
}
