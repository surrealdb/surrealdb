use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::BTree;
use crate::idx::kvsim::KVSimulator;
use crate::kvs::Key;
use serde::{Deserialize, Serialize};

pub(super) type DocId = u64;

pub(super) struct DocIds {
	state_key: Key,
	state: DocIdsState,
	updated: bool,
}

#[derive(Serialize, Deserialize)]
struct DocIdsState {
	btree: BTree,
	next_doc_id: DocId,
}

impl DocIdsState {
	fn new(btree_order: usize) -> Self {
		Self {
			btree: BTree::new(btree_order),
			next_doc_id: 0,
		}
	}
}

impl DocIds {
	pub(super) fn new(kv: &mut KVSimulator, state_key: Key, btree_order: usize) -> Self {
		Self {
			state: kv.get(&state_key).unwrap_or_else(|| DocIdsState::new(btree_order)),
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
		let mut d = DocIds::new(&mut kv, "D".into(), BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Foo");
		assert_eq!(d.count(&mut kv), 1);
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve the same doc key
		let mut d = DocIds::new(&mut kv, "D".into(), BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Foo");
		assert_eq!(d.count(&mut kv), 1);
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve another single doc key
		let mut d = DocIds::new(&mut kv, "D".into(), BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Bar");
		assert_eq!(d.count(&mut kv), 2);
		d.finish(&mut kv);
		assert_eq!(doc_id, 1);

		// Resolve another two existing doc keys and two new doc keys (interlaced)
		let mut d = DocIds::new(&mut kv, "D".into(), BTREE_ORDER);
		assert_eq!(d.resolve_doc_id(&mut kv, "Foo"), 0);
		assert_eq!(d.resolve_doc_id(&mut kv, "Hello"), 2);
		assert_eq!(d.resolve_doc_id(&mut kv, "Bar"), 1);
		assert_eq!(d.resolve_doc_id(&mut kv, "World"), 3);
		assert_eq!(d.count(&mut kv), 4);
		d.finish(&mut kv);

		let mut d = DocIds::new(&mut kv, "D".into(), BTREE_ORDER);
		assert_eq!(d.resolve_doc_id(&mut kv, "Foo"), 0);
		assert_eq!(d.resolve_doc_id(&mut kv, "Bar"), 1);
		assert_eq!(d.resolve_doc_id(&mut kv, "Hello"), 2);
		assert_eq!(d.resolve_doc_id(&mut kv, "World"), 3);
		assert_eq!(d.count(&mut kv), 4);
	}
}
