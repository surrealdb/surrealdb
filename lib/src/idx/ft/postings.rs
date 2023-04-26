use crate::err::Error;
use crate::idx::bkeys::{KeyVisitor, TrieKeys};
use crate::idx::btree::{BTree, KeyProvider, NodeId, Payload, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::{btree, IndexKeyBase, SerdeState};
use crate::key::bf::Bf;
use crate::kvs::{Key, Transaction};
use async_trait::async_trait;

pub(super) type TermFrequency = u64;

pub(super) struct Postings {
	state_key: Key,
	index_key_base: IndexKeyBase,
	btree: BTree<PostingsKeyProvider>,
}

#[async_trait]
pub(super) trait PostingsVisitor {
	async fn visit(
		&mut self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_frequency: TermFrequency,
	) -> Result<(), Error>;
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

	pub(super) async fn get_doc_count(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<u64, Error> {
		let prefix_key = self.index_key_base.new_bf_prefix_key(term_id);
		let mut counter = PostingsDocCount::default();
		self.btree.search_by_prefix::<TrieKeys, _>(tx, &prefix_key, &mut counter).await?;
		Ok(counter.doc_count)
	}

	pub(super) async fn collect_postings<V>(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
		visitor: &mut V,
	) -> Result<(), Error>
	where
		V: PostingsVisitor + Send,
	{
		let prefix_key = self.index_key_base.new_bf_prefix_key(term_id);
		let mut key_visitor = PostingsAdapter {
			visitor,
		};
		self.btree.search_by_prefix::<TrieKeys, _>(tx, &prefix_key, &mut key_visitor).await
	}

	pub(super) async fn count_postings(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<usize, Error> {
		let mut counter = PostingCounter::default();
		self.collect_postings(tx, term_id, &mut counter).await?;
		Ok(counter.count)
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

#[derive(Default)]
struct PostingCounter {
	count: usize,
}

#[async_trait]
impl PostingsVisitor for PostingCounter {
	async fn visit(
		&mut self,
		_tx: &mut Transaction,
		_doc_id: DocId,
		_term_frequency: TermFrequency,
	) -> Result<(), Error> {
		self.count += 1;
		Ok(())
	}
}

struct PostingsKeyProvider {
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

struct PostingsAdapter<'a, V>
where
	V: PostingsVisitor,
{
	visitor: &'a mut V,
}

#[async_trait]
impl<'a, V> KeyVisitor for PostingsAdapter<'a, V>
where
	V: PostingsVisitor + Send,
{
	async fn visit(
		&mut self,
		tx: &mut Transaction,
		key: Key,
		payload: Payload,
	) -> Result<(), Error> {
		let posting_key: Bf = key.into();
		self.visitor.visit(tx, posting_key.doc_id, payload).await
	}
}

#[derive(Default)]
struct PostingsDocCount {
	doc_count: u64,
}

#[async_trait]
impl KeyVisitor for PostingsDocCount {
	async fn visit(
		&mut self,
		_tx: &mut Transaction,
		_key: Key,
		_payload: Payload,
	) -> Result<(), Error> {
		self.doc_count += 1;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::ft::docids::DocId;
	use crate::idx::ft::postings::{Postings, PostingsVisitor, TermFrequency};
	use crate::idx::IndexKeyBase;
	use crate::kvs::{Datastore, Transaction};
	use async_trait::async_trait;
	use std::collections::HashMap;
	use test_log::test;

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

		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();

		let mut tx = ds.transaction(false, false).await.unwrap();
		let p = Postings::new(&mut tx, IndexKeyBase::default(), DEFAULT_BTREE_ORDER).await.unwrap();
		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 1);

		let mut v = TestPostingVisitor::default();
		p.collect_postings(&mut tx, 1, &mut v).await.unwrap();
		v.check_len(1, "Postings");
		v.check(vec![(2, 3)], "Postings");
	}

	#[derive(Default)]
	pub(super) struct TestPostingVisitor {
		map: HashMap<DocId, TermFrequency>,
	}

	#[async_trait]
	impl PostingsVisitor for TestPostingVisitor {
		async fn visit(
			&mut self,
			_tx: &mut Transaction,
			doc_id: DocId,
			term_frequency: TermFrequency,
		) -> Result<(), Error> {
			assert_eq!(self.map.insert(doc_id, term_frequency), None);
			Ok(())
		}
	}

	impl TestPostingVisitor {
		pub(super) fn check_len(&self, len: usize, info: &str) {
			assert_eq!(self.map.len(), len, "len issue: {}", info);
		}
		pub(super) fn check(&self, res: Vec<(DocId, TermFrequency)>, info: &str) {
			self.check_len(res.len(), info);
			for (d, f) in res {
				assert_eq!(self.map.get(&d), Some(&f));
			}
		}
	}
}
