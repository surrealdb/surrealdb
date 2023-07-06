use crate::idx::ft::docids::DocId;
use crate::sql::{Thing, Value};

pub struct CursorDoc<'a> {
	rid: Option<&'a Thing>,
	doc: Option<&'a Value>,
	doc_id: Option<DocId>,
}

impl<'a> CursorDoc<'a> {
	pub(crate) const NONE: CursorDoc<'a> = Self {
		rid: None,
		doc_id: None,
		doc: None,
	};

	pub(crate) fn new(
		rid: Option<&'a Thing>,
		doc_id: Option<DocId>,
		doc: Option<&'a Value>,
	) -> Self {
		Self {
			rid,
			doc,
			doc_id,
		}
	}

	pub(crate) fn rid(&self) -> Option<&'a Thing> {
		self.rid
	}

	pub(crate) fn doc_id(&self) -> Option<DocId> {
		self.doc_id
	}

	pub(crate) fn doc(&self) -> Option<&'a Value> {
		self.doc
	}
}
