use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::BTree;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{IndexId, StateKey, POSTING_DOMAIN};
use crate::kvs::Key;
use derive::Key;
use serde::{Deserialize, Serialize};

pub(super) type TermFrequency = u64;

#[derive(Serialize, Deserialize, Key)]
struct PostingKey {
	domain: u8,
	index_id: u64,
	term_id: TermId,
	doc_id: DocId,
}

#[derive(Serialize, Deserialize, Key)]
struct PostingPrefixKey {
	domain: u8,
	index_id: u64,
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

impl Postings {
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId, default_btree_order: usize) -> Self {
		let state_key = StateKey::new(POSTING_DOMAIN, index_id).into();
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

	pub(super) fn get_postings(
		&self,
		kv: &mut KVSimulator,
		term_id: TermId,
	) -> Vec<(DocId, TermFrequency)> {
		let prefix_key = self.posting_prefix_key(term_id).into();
		let key_payload_vec = self.state.btree.search_by_prefix::<TrieKeys>(kv, &prefix_key);
		let mut res = Vec::with_capacity(key_payload_vec.len());
		for (key, payload) in key_payload_vec {
			let posting_key: PostingKey = key.into();
			res.push((posting_key.doc_id, payload));
		}
		res
	}

	fn posting_prefix_key(&self, term_id: TermId) -> PostingPrefixKey {
		PostingPrefixKey {
			domain: POSTING_DOMAIN,
			index_id: self.index_id,
			term_id,
		}
	}

	pub(super) fn count(&self, kv: &mut KVSimulator) -> usize {
		self.state.btree.count::<TrieKeys>(kv)
	}

	pub(super) fn finish(self, kv: &mut KVSimulator) {
		if self.updated {
			kv.set(self.state_key, &self.state);
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::postings::Postings;
	use crate::idx::kvsim::KVSimulator;

	#[test]
	fn test_doc_lengths() {
		const DEFAULT_BTREE_ORDER: usize = 75;

		let mut kv = KVSimulator::default();

		// Check empty state
		let mut p = Postings::new(&mut kv, 0, DEFAULT_BTREE_ORDER);
		assert_eq!(p.count(&mut kv), 0);
		p.update_posting(&mut kv, 1, 2, 3);
		assert_eq!(p.count(&mut kv), 1);
		p.finish(&mut kv);
	}
}
