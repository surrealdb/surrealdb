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

impl PostingKey {
	fn new(index_id: IndexId, term_id: TermId, doc_id: DocId) -> Self {
		Self {
			domain: POSTING_DOMAIN,
			index_id,
			term_id,
			doc_id,
		}
	}
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
		let key = PostingKey::new(self.index_id, term_id, doc_id);
		self.state.btree.insert::<TrieKeys>(kv, key.into(), term_freq);
		self.updated = true;
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
