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
	use crate::idx::ft::postings::Postings;
	use crate::idx::IndexKeyBase;
	use crate::kvs::Datastore;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: usize = 75;

		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

		// Check empty state
		let mut p =
			Postings::new(&mut tx, IndexKeyBase::default(), DEFAULT_BTREE_ORDER).await.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

		p.update_posting(&mut tx, 1, 2, 3).await.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 1);
		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}
}
