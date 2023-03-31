use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::BTree;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{IndexId, StateKey, DOC_IDS_DOMAIN};
use crate::kvs::Key;
use serde::{Deserialize, Serialize};

pub(super) type DocId = u64;

pub(super) struct DocIds {
	state_key: Key,
	state: State,
	updated: bool,
}

#[derive(Serialize, Deserialize)]
struct State {
	btree: BTree,
	next_doc_id: DocId,
}

impl State {
	fn new(index_id: IndexId, btree_order: usize) -> Self {
		Self {
			btree: BTree::new(DOC_IDS_DOMAIN, index_id, btree_order),
			next_doc_id: 0,
		}
	}
}

impl DocIds {
	pub(super) fn new(kv: &mut KVSimulator, index_id: IndexId, default_btree_order: usize) -> Self {
		let state_key = StateKey::new(DOC_IDS_DOMAIN, index_id).into();
		Self {
			state: kv.get(&state_key).unwrap_or_else(|| State::new(index_id, default_btree_order)),
			updated: false,
			state_key,
		}
	}

	pub(super) fn resolve_doc_id(&mut self, kv: &mut KVSimulator, key: &str) -> DocId {
		let key = key.into();
		if let Some(doc_id) = self.state.btree.search::<TrieKeys>(kv, &key) {
			doc_id
		} else {
			let doc_id = self.state.next_doc_id;
			self.state.btree.insert::<TrieKeys>(kv, key, doc_id);
			self.state.next_doc_id += 1;
			self.updated = true;
			doc_id
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
	use crate::idx::ft::docids::DocIds;
	use crate::idx::kvsim::KVSimulator;

	#[test]
	fn test_resolve_doc_id() {
		const BTREE_ORDER: usize = 75;

		let mut kv = KVSimulator::default();

		// Resolve a first doc key
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Foo");
		assert_eq!(d.count(&mut kv), 1);
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve the same doc key
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Foo");
		assert_eq!(d.count(&mut kv), 1);
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve another single doc key
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Bar");
		assert_eq!(d.count(&mut kv), 2);
		d.finish(&mut kv);
		assert_eq!(doc_id, 1);

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		assert_eq!(d.resolve_doc_id(&mut kv, "Foo"), 0);
		assert_eq!(d.resolve_doc_id(&mut kv, "Hello"), 2);
		assert_eq!(d.resolve_doc_id(&mut kv, "Bar"), 1);
		assert_eq!(d.resolve_doc_id(&mut kv, "World"), 3);
		assert_eq!(d.count(&mut kv), 4);
		d.finish(&mut kv);

		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		assert_eq!(d.resolve_doc_id(&mut kv, "Foo"), 0);
		assert_eq!(d.resolve_doc_id(&mut kv, "Bar"), 1);
		assert_eq!(d.resolve_doc_id(&mut kv, "Hello"), 2);
		assert_eq!(d.resolve_doc_id(&mut kv, "World"), 3);
		assert_eq!(d.count(&mut kv), 4);
	}
}
