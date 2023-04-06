use crate::idx::bkeys::{KeyVisitor, TrieKeys};
use crate::idx::btree::{BTree, Payload, Statistics};
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{BaseStateKey, Domain, IndexId, POSTING_DOMAIN};
use crate::kvs::Key;
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
	state: State,
	updated: bool,
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: BTree,
}

impl State {
	fn new(index_id: IndexId, btree_order: usize) -> Self {
		Self {
			btree: BTree::new(POSTING_DOMAIN, index_id, btree_order),
		}
	}
}

pub(super) trait PostingsVisitor {
	fn visit(&mut self, kv: &mut KVSimulator, doc_id: DocId, term_frequency: TermFrequency);
}

impl Postings {
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId, default_btree_order: usize) -> Self {
		let state_key = BaseStateKey::new(POSTING_DOMAIN, index_id).into();
		Self {
			index_id,
			state: kv.get(&state_key).unwrap_or_else(|| State::new(index_id, default_btree_order)),
			updated: false,
			state_key,
		}
	}

	pub(super) fn update_posting(
		&mut self,
		kv: &mut KVSimulator,
		term_id: TermId,
		doc_id: DocId,
		term_freq: TermFrequency,
	) {
		let key = self.posting_key(term_id, doc_id);
		self.state.btree.insert::<TrieKeys>(kv, key.into(), term_freq);
		self.updated = true;
	}

	fn posting_key(&self, term_id: TermId, doc_id: DocId) -> PostingKey {
		PostingKey {
			domain: POSTING_DOMAIN,
			index_id: self.index_id,
			term_id,
			doc_id,
		}
	}

	pub(super) fn get_doc_count(&self, kv: &mut KVSimulator, term_id: TermId) -> u64 {
		let prefix_key = self.posting_prefix_key(term_id).into();
		let mut counter = PostingsDocCount::default();
		self.state.btree.search_by_prefix::<TrieKeys, _>(kv, &prefix_key, &mut counter);
		counter.doc_count
	}

	pub(super) fn collect_postings<V>(&self, kv: &mut KVSimulator, term_id: TermId, visitor: &mut V)
	where
		V: PostingsVisitor,
	{
		let prefix_key = self.posting_prefix_key(term_id).into();
		let mut key_visitor = PostingsAdapter {
			visitor,
		};
		self.state.btree.search_by_prefix::<TrieKeys, _>(kv, &prefix_key, &mut key_visitor);
	}

	fn posting_prefix_key(&self, term_id: TermId) -> PostingPrefixKey {
		PostingPrefixKey {
			domain: POSTING_DOMAIN,
			index_id: self.index_id,
			term_id,
		}
	}

	pub(super) fn statistics(&self, kv: &mut KVSimulator) -> Statistics {
		self.state.btree.statistics::<TrieKeys>(kv)
	}

	pub(super) fn debug(&self, kv: &mut KVSimulator) {
		let state_key: BaseStateKey = self.state_key.clone().into();
		debug!("POSTINGS {:?}", state_key);
		self.state.btree.debug::<_, TrieKeys>(kv, |k| {
			let k: PostingKey = k.into();
			format!("({}-{})", k.term_id, k.doc_id)
		});
	}

	pub(super) fn finish(self, kv: &mut KVSimulator) {
		if self.updated {
			kv.set(self.state_key, &self.state);
		}
	}
}

struct PostingsAdapter<'a, V>
where
	V: PostingsVisitor,
{
	visitor: &'a mut V,
}

impl<'a, V> KeyVisitor for PostingsAdapter<'a, V>
where
	V: PostingsVisitor,
{
	fn visit(&mut self, kv: &mut KVSimulator, key: Key, payload: Payload) {
		let posting_key: PostingKey = key.into();
		self.visitor.visit(kv, posting_key.doc_id, payload);
	}
}

#[derive(Default)]
struct PostingsDocCount {
	doc_count: u64,
}

impl KeyVisitor for PostingsDocCount {
	fn visit(&mut self, _kv: &mut KVSimulator, _key: Key, _payload: Payload) {
		self.doc_count += 1;
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::kvsim::KVSimulator;
	use test_log::test;

	#[test]
	fn test_postings() {
		const DEFAULT_BTREE_ORDER: usize = 75;

		let mut kv = KVSimulator::default();

		// Check empty state
		let mut p = Postings::new(&mut kv, 0, DEFAULT_BTREE_ORDER);
		assert_eq!(p.statistics(&mut kv).keys_count, 0);
		p.update_posting(&mut kv, 1, 2, 3);
		assert_eq!(p.statistics(&mut kv).keys_count, 1);
		p.debug(&mut kv);
		p.finish(&mut kv);
	}
}
