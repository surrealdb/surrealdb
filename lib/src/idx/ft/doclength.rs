use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::BTree;
use crate::idx::ft::docids::DocId;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{BaseStateKey, IndexId, DOC_LENGTHS_DOMAIN};
use crate::kvs::Key;
use serde::{Deserialize, Serialize};

pub(super) type DocLength = u64;

pub(super) struct DocLengths {
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
			btree: BTree::new(DOC_LENGTHS_DOMAIN, index_id, btree_order),
		}
	}
}

impl DocLengths {
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId, default_btree_order: usize) -> Self {
		let state_key = BaseStateKey::new(DOC_LENGTHS_DOMAIN, index_id).into();
		Self {
			state: kv.get(&state_key).unwrap_or_else(|| State::new(index_id, default_btree_order)),
			updated: false,
			state_key,
		}
	}

	pub(super) fn get_doc_length(&self, kv: &mut KVSimulator, doc_id: DocId) -> Option<DocLength> {
		self.state.btree.search::<TrieKeys>(kv, &doc_id.to_be_bytes().to_vec())
	}

	pub(super) fn set_doc_length(
		&mut self,
		kv: &mut KVSimulator,
		doc_id: DocId,
		doc_length: DocLength,
	) {
		self.state.btree.insert::<TrieKeys>(kv, doc_id.to_be_bytes().to_vec(), doc_length);
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
	use crate::idx::ft::doclength::DocLengths;
	use crate::idx::kvsim::KVSimulator;

	#[test]
	fn test_doc_lengths() {
		const BTREE_ORDER: usize = 75;

		let mut kv = KVSimulator::default();

		// Check empty state
		let l = DocLengths::new(&mut kv, 0, BTREE_ORDER);
		assert_eq!(l.count(&mut kv), 0);
		let dl = l.get_doc_length(&mut kv, 99);
		l.finish(&mut kv);
		assert_eq!(dl, None);

		// Set a doc length
		let mut l = DocLengths::new(&mut kv, 0, BTREE_ORDER);
		l.set_doc_length(&mut kv, 99, 199);
		assert_eq!(l.count(&mut kv), 1);
		let dl = l.get_doc_length(&mut kv, 99);
		l.finish(&mut kv);
		assert_eq!(dl, Some(199));

		// Update doc length
		let mut l = DocLengths::new(&mut kv, 0, BTREE_ORDER);
		l.set_doc_length(&mut kv, 99, 299);
		assert_eq!(l.count(&mut kv), 1);
		let dl = l.get_doc_length(&mut kv, 99);
		l.finish(&mut kv);
		assert_eq!(dl, Some(299));
	}
}
