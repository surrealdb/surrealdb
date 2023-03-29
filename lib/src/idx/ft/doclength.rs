use crate::idx::bkeys::{BKeys, FstKeys};
use crate::idx::docids::DocId;
use roaring::RoaringTreemap;

pub(super) type DocLength = u64;

#[derive(Default)]
pub(super) struct DocLengths(Vec<DocLengthPartition>);

struct DocLengthPartition {
	docs: FstKeys,
}

impl DocLengths {
	pub(super) fn _get_docs_lengths(&self, docs: RoaringTreemap) -> Vec<DocLength> {
		for _ in docs {
			for _ in &self.0 {}
		}
		vec![]
	}

	pub(super) fn set_doc_length(&self, doc_id: &DocId, doc_length: DocLength) {
		for p in &self.0 {
			if p.set_doc_length(doc_id, doc_length) {
				return;
			}
		}
	}
}

impl DocLengthPartition {
	fn set_doc_length(&self, doc_id: &DocId, doc_length: DocLength) -> bool {
		if let Some(old_doc_length) = self.docs.get(&doc_id.to_vec()) {
			if old_doc_length == doc_length {
				return true;
			}
		}
		return false;
	}
}
