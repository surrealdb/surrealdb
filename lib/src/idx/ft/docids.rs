use crate::idx::bkeys::TrieKeys;
use crate::idx::btree::BTree;
use crate::idx::kvsim::KVSimulator;
use crate::idx::{Domain, IndexId, StateKey, DOC_IDS_DOMAIN, DOC_KEYS_DOMAIN};
use crate::kvs::Key;
use derive::Key;
use serde::{Deserialize, Serialize};

pub(super) type DocId = u64;

pub(super) struct DocIds {
	state_key: Key,
	index_id: IndexId,
	state: State,
	updated: bool,
}

#[derive(Serialize, Deserialize, Key)]
struct DocKey {
	domain: Domain,
	index_id: IndexId,
	doc_id: DocId,
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
			index_id,
		}
	}

	pub(super) fn resolve_doc_id(&mut self, kv: &mut KVSimulator, key: &str) -> DocId {
		let key = key.into();
		if let Some(doc_id) = self.state.btree.search::<TrieKeys>(kv, &key) {
			doc_id
		} else {
			let doc_id = self.state.next_doc_id;
			let doc_key = DocKey {
				domain: DOC_KEYS_DOMAIN,
				index_id: self.index_id,
				doc_id,
			};
			kv.set(doc_key.into(), &key);
			self.state.btree.insert::<TrieKeys>(kv, key, doc_id);
			self.state.next_doc_id += 1;
			self.updated = true;
			doc_id
		}
	}

	pub(super) fn get_doc_key(&self, kv: &mut KVSimulator, doc_id: DocId) -> Option<String> {
		let doc_key = DocKey {
			domain: DOC_KEYS_DOMAIN,
			index_id: self.index_id,
			doc_id,
		};
		kv.get::<Key>(&doc_key.into()).map(|v| String::from_utf8(v).unwrap())
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
		assert_eq!(d.get_doc_key(&mut kv, 0), Some("Foo".to_string()));
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve the same doc key
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Foo");
		assert_eq!(d.count(&mut kv), 1);
		assert_eq!(d.get_doc_key(&mut kv, 0), Some("Foo".to_string()));
		d.finish(&mut kv);
		assert_eq!(doc_id, 0);

		// Resolve another single doc key
		let mut d = DocIds::new(&mut kv, 0, BTREE_ORDER);
		let doc_id = d.resolve_doc_id(&mut kv, "Bar");
		assert_eq!(d.count(&mut kv), 2);
		assert_eq!(d.get_doc_key(&mut kv, 1), Some("Bar".to_string()));
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
		assert_eq!(d.get_doc_key(&mut kv, 0), Some("Foo".to_string()));
		assert_eq!(d.get_doc_key(&mut kv, 1), Some("Bar".to_string()));
		assert_eq!(d.get_doc_key(&mut kv, 2), Some("Hello".to_string()));
		assert_eq!(d.get_doc_key(&mut kv, 3), Some("World".to_string()));
		assert_eq!(d.count(&mut kv), 4);
	}
}
