use crate::idx::docids::DocId;
use crate::kvs::Key;
use crate::sql::Thing;
use radix_trie::Trie;
use roaring::RoaringTreemap;

#[derive(Default)]
pub(super) struct HnswDocs {
	doc_ids: Trie<Key, DocId>,
	ids_doc: Vec<Option<Thing>>,
	available: RoaringTreemap,
}

impl HnswDocs {
	pub(super) fn resolve(&mut self, rid: &Thing) -> DocId {
		let doc_key: Key = rid.into();
		if let Some(doc_id) = self.doc_ids.get(&doc_key) {
			*doc_id
		} else {
			let doc_id = self.next_doc_id();
			self.ids_doc.push(Some(rid.clone()));
			self.doc_ids.insert(doc_key, doc_id);
			doc_id
		}
	}

	fn next_doc_id(&mut self) -> DocId {
		if let Some(doc_id) = self.available.iter().next() {
			self.available.remove(doc_id);
			doc_id
		} else {
			self.ids_doc.len() as DocId
		}
	}

	pub(super) fn get_thing(&self, doc_id: DocId) -> Option<&Thing> {
		if let Some(r) = self.ids_doc.get(doc_id as usize) {
			r.as_ref()
		} else {
			None
		}
	}
	pub(super) fn remove(&mut self, rid: &Thing) -> Option<DocId> {
		let doc_key: Key = rid.into();
		if let Some(doc_id) = self.doc_ids.remove(&doc_key) {
			let n = doc_id as usize;
			if n < self.ids_doc.len() {
				self.ids_doc[n] = None;
			}
			self.available.insert(doc_id);
			Some(doc_id)
		} else {
			None
		}
	}
}
