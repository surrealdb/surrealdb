use fst::Map;

pub(super) struct DocId(u64);

impl From<u64> for DocId {
	fn from(id: u64) -> Self {
		Self(id)
	}
}

impl DocId {
	pub(super) fn to_vec(&self) -> Vec<u8> {
		self.0.to_be_bytes().to_vec()
	}
}

pub(super) struct _DocIdsPartition {
	docs: Map<Vec<u8>>,
	highest_doc_id: DocId,
	size: usize,
}

impl _DocIdsPartition {}
