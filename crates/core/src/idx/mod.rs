pub(crate) mod ft;
pub(crate) mod index;
pub mod planner;
pub(super) mod seqdocids;
pub mod trees;

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::store::NodeId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::index::dc::DocumentFrequencyKey;
use crate::key::index::dl::DocLengthKey;
use crate::key::index::hd::{HdRoot, HnswDocIdKey};
use crate::key::index::he::HnswIndexKey;
use crate::key::index::hi::HnswRecordKey;
use crate::key::index::hl::HnswChunkedLayerKey;
use crate::key::index::hs::HnswStateKey;
use crate::key::index::hv::HnswVectorDocsKey;
#[cfg(not(target_family = "wasm"))]
use crate::key::index::ia::IndexAppendingKey;
use crate::key::index::ib::SequenceBatchKey;
use crate::key::index::id::InvertedIndexDocIdKey as IdKey;
use crate::key::index::ii::InvertedIdKey;
#[cfg(not(target_family = "wasm"))]
use crate::key::index::ip::IndexPreviousRecordIdKey;
use crate::key::index::is::IndexSequenceStateKey;
use crate::key::index::td::{TdRoot, TermDocumentKey};
use crate::key::index::tt::TermDocumentFrequencyKey;
use crate::key::index::vm::{MtreeNodeStateKey, MtreeStateKey};
use crate::key::root::ic::IndexCompactionKey;
use crate::kvs::Key;
use crate::val::RecordIdKey;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct IndexKeyBase(Arc<Inner>);

#[derive(Debug, Hash, PartialEq, Eq)]
struct Inner {
	ns: NamespaceId,
	db: DatabaseId,
	tb: String,
	ix: IndexId,
}

impl IndexKeyBase {
	pub fn new(
		ns: impl Into<NamespaceId>,
		db: impl Into<DatabaseId>,
		tb: &str,
		ix: impl Into<IndexId>,
	) -> Self {
		Self(Arc::new(Inner {
			ns: ns.into(),
			db: db.into(),
			tb: tb.to_string(),
			ix: ix.into(),
		}))
	}

	fn new_hd_root_key(&self) -> HdRoot<'_> {
		HdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_hd_key(&self, doc_id: DocId) -> HnswDocIdKey<'_> {
		HnswDocIdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_he_key(&self, element_id: ElementId) -> HnswIndexKey<'_> {
		HnswIndexKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, element_id)
	}

	fn new_hi_key(&self, id: RecordIdKey) -> HnswRecordKey<'_> {
		HnswRecordKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> HnswChunkedLayerKey<'_> {
		HnswChunkedLayerKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer, chunk)
	}

	fn new_hv_key<'a>(&'a self, vec: &'a SerializedVector) -> HnswVectorDocsKey<'a> {
		HnswVectorDocsKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, vec)
	}

	fn new_hs_key(&self) -> HnswStateKey<'_> {
		HnswStateKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_vm_root_key(&self) -> MtreeStateKey<'_> {
		MtreeStateKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_vm_key(&self, node_id: NodeId) -> MtreeNodeStateKey<'_> {
		MtreeNodeStateKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, node_id)
	}

	fn new_ii_key(&self, doc_id: DocId) -> InvertedIdKey<'_> {
		InvertedIdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_id_key(&self, id: RecordIdKey) -> IdKey<'_> {
		IdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_ia_key(&self, i: u32) -> IndexAppendingKey<'_> {
		IndexAppendingKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, i)
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_ip_key(&self, id: RecordIdKey) -> IndexPreviousRecordIdKey<'_> {
		IndexPreviousRecordIdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	pub(crate) fn new_ib_key(&self, start: i64) -> SequenceBatchKey<'_> {
		SequenceBatchKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, start)
	}

	pub(crate) fn new_ic_key(&self, nid: Uuid) -> IndexCompactionKey<'_> {
		IndexCompactionKey::new(
			self.0.ns,
			self.0.db,
			Cow::Borrowed(&self.0.tb),
			self.0.ix,
			nid,
			Uuid::now_v7(),
		)
	}

	pub(crate) fn new_ib_range(&self) -> Result<Range<Key>> {
		SequenceBatchKey::new_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	pub(crate) fn new_is_key(&self, nid: Uuid) -> IndexSequenceStateKey<'_> {
		IndexSequenceStateKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, nid)
	}

	fn new_td_root<'a>(&'a self, term: &'a str) -> TdRoot<'a> {
		TdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> TermDocumentKey<'a> {
		TermDocumentKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id)
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> TermDocumentFrequencyKey<'a> {
		TermDocumentFrequencyKey::new(
			self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id, nid, uid, add,
		)
	}

	fn new_tt_term_range(&self, term: &str) -> Result<(Key, Key)> {
		TermDocumentFrequencyKey::term_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_tt_terms_range(&self) -> Result<(Key, Key)> {
		TermDocumentFrequencyKey::terms_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> DocumentFrequencyKey<'_> {
		DocumentFrequencyKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id, nid, uid)
	}

	fn new_dc_compacted(&self) -> Result<Key> {
		DocumentFrequencyKey::new_root(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_range(&self) -> Result<(Key, Key)> {
		DocumentFrequencyKey::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dl(&self, doc_id: DocId) -> DocLengthKey<'_> {
		DocLengthKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	pub(crate) fn table(&self) -> &str {
		&self.0.tb
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn index(&self) -> IndexId {
		self.0.ix
	}
}
