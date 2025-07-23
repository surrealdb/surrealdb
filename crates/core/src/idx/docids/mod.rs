pub(crate) mod btdocids;
pub(crate) mod seqdocids;

pub type DocId = u64;

#[derive(Debug, PartialEq)]
pub(in crate::idx) enum Resolved {
	New(DocId),
	Existing(DocId),
}

impl Resolved {
	pub(in crate::idx) fn doc_id(&self) -> DocId {
		match self {
			Resolved::New(doc_id) => *doc_id,
			Resolved::Existing(doc_id) => *doc_id,
		}
	}

	pub(in crate::idx) fn was_existing(&self) -> bool {
		match self {
			Resolved::New(_) => false,
			Resolved::Existing(_) => true,
		}
	}
}
