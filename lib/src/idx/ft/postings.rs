use crate::err::Error;
use crate::idx::bkeys::{KeyVisitor, TrieKeys};
use crate::idx::btree::{BTree, Payload, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::{BaseStateKey, Domain, IndexId, POSTING_DOMAIN};
use crate::kvs::{Key, Transaction, Val};
use async_trait::async_trait;
use derive::Key;
use serde::{Deserialize, Serialize};

pub(super) type TermFrequency = u64;

#[derive(Serialize, Deserialize, Key)]
struct PostingKey {
	domain: Domain,
	index_id: IndexId,
	term_id: TermId,
	doc_id: DocId,
}

#[derive(Serialize, Deserialize, Key)]
struct PostingPrefixKey {
	domain: Domain,
	index_id: IndexId,
	term_id: TermId,
}

pub(super) struct Postings {
	index_id: IndexId,
	state_key: Key,
	btree: BTree,
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
		index_id: IndexId,
		default_btree_order: usize,
	) -> Result<Self, Error> {
		let state_key: Key = BaseStateKey::new(POSTING_DOMAIN, index_id).into();
		let btree = if let Some(val) = tx.get(state_key.clone()).await? {
			BTree::try_from(val)?
		} else {
			BTree::new(POSTING_DOMAIN, index_id, default_btree_order)
		};
		Ok(Self {
			index_id,
			btree,
			state_key,
		})
	}

	pub(super) async fn update_posting(
		&mut self,
		tx: &mut Transaction,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) -> Result<(), Error> {
		let key = self.posting_key(term_id, doc_id);
		self.btree.insert::<TrieKeys>(tx, key.into(), term_freq).await
	}

	fn posting_key(&self, term_id: TermId, doc_id: DocId) -> PostingKey {
		PostingKey {
			domain: POSTING_DOMAIN,
			index_id: self.index_id,
			term_id,
			doc_id,
		}
	}

	pub(super) async fn get_doc_count(
		&self,
		tx: &mut Transaction,
		term_id: TermId,
	) -> Result<u64, Error> {
		let prefix_key = self.posting_prefix_key(term_id).into();
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
		let prefix_key = self.posting_prefix_key(term_id).into();
		let mut key_visitor = PostingsAdapter {
			visitor,
		};
		self.btree.search_by_prefix::<TrieKeys, _>(tx, &prefix_key, &mut key_visitor).await
	}

	fn posting_prefix_key(&self, term_id: TermId) -> PostingPrefixKey {
		PostingPrefixKey {
			domain: POSTING_DOMAIN,
			index_id: self.index_id,
			term_id,
		}
	}

	pub(super) async fn statistics(&self, tx: &mut Transaction) -> Result<Statistics, Error> {
		self.btree.statistics::<TrieKeys>(tx).await
	}

	pub(super) async fn debug(&self, tx: &mut Transaction) -> Result<(), Error> {
		debug!("POSTINGS {}", self.index_id);
		self.btree
			.debug::<_, TrieKeys>(tx, |k| {
				let k: PostingKey = k.into();
				Ok(format!("({}-{})", k.term_id, k.doc_id))
			})
			.await
	}

	pub(super) async fn finish(self, tx: &mut Transaction) -> Result<(), Error> {
		if self.btree.is_updated() {
			let val: Val = self.btree.try_into()?;
			tx.set(self.state_key, val).await?;
		}
		Ok(())
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
		let posting_key: PostingKey = key.into();
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
	use crate::kvs::Datastore;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_postings() {
		const DEFAULT_BTREE_ORDER: usize = 75;

		let ds = Datastore::new("memory").await.unwrap();
		let mut tx = ds.transaction(true, false).await.unwrap();

		// Check empty state
		let mut p = Postings::new(&mut tx, 0, DEFAULT_BTREE_ORDER).await.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 0);

		p.update_posting(&mut tx, 1, 2, 3).await.unwrap();

		assert_eq!(p.statistics(&mut tx).await.unwrap().keys_count, 1);
		p.debug(&mut tx).await.unwrap();
		p.finish(&mut tx).await.unwrap();
		tx.commit().await.unwrap();
	}
}
