use crate::idx::ft::docids::DocId;
use roaring::RoaringTreemap;

pub(super) type DocLength = u64;

#[derive(Default)]
pub(super) struct DocLengths {}

impl DocLengths {
	pub(super) fn _get_docs_lengths(&self, _docs: RoaringTreemap) -> Vec<DocLength> {
		todo!()
	}

	pub(super) fn set_doc_length(&self, _doc_id: DocId, _doc_length: DocLength) {
		todo!()
	}
}
