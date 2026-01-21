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
use crate::idx::trees::vector::SerializedVectorHash;
use crate::key::index::dc::Dc;
use crate::key::index::dl::Dl;
use crate::key::index::hd::{Hd, HdRoot};
use crate::key::index::he::He;
use crate::key::index::hi::Hi;
use crate::key::index::hl::Hl;
use crate::key::index::hs::Hs;
use crate::key::index::hv::Hv;
use crate::key::index::ia::Ia;
use crate::key::index::ib::Ib;
use crate::key::index::id::Id as IdKey;
use crate::key::index::ii::Ii;
use crate::key::index::ip::Ip;
use crate::key::index::is::Is;
use crate::key::index::td::{Td, TdRoot};
use crate::key::index::tt::Tt;
use crate::key::root::ic::IndexCompactionKey;
use crate::kvs::Key;
use crate::val::{RecordIdKey, TableName};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
#[repr(transparent)]
pub struct IndexKeyBase(Arc<Inner>);

#[derive(Debug, Hash, PartialEq, Eq)]
struct Inner {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ix: IndexId,
}

impl IndexKeyBase {
	pub fn new(ns: NamespaceId, db: DatabaseId, tb: TableName, ix: IndexId) -> Self {
		Self(Arc::new(Inner {
			ns,
			db,
			tb,
			ix,
		}))
	}

	fn new_hd_root_key(&self) -> HdRoot<'_> {
		HdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_hd_key(&self, doc_id: DocId) -> Hd<'_> {
		Hd::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_he_key(&self, element_id: ElementId) -> He<'_> {
		He::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, element_id)
	}

	fn new_hi_key(&self, id: RecordIdKey) -> Hi<'_> {
		Hi::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> Hl<'_> {
		Hl::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, layer, chunk)
	}

	fn new_hv_key(&self, hash: SerializedVectorHash) -> Hv<'_> {
		Hv::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, hash)
	}

	fn new_hs_key(&self) -> Hs<'_> {
		Hs::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_ii_key(&self, doc_id: DocId) -> Ii<'_> {
		Ii::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	fn new_id_key(&self, id: RecordIdKey) -> IdKey<'_> {
		IdKey::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	pub(crate) fn new_ia_key(&self, i: u32) -> Ia<'_> {
		Ia::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, i)
	}

	pub(crate) fn new_ip_key(&self, id: RecordIdKey) -> Ip<'_> {
		Ip::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, id)
	}

	pub(crate) fn new_ib_key(&self, start: i64) -> Ib<'_> {
		Ib::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, start)
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
		Ib::new_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	pub(crate) fn new_is_key(&self, nid: Uuid) -> Is<'_> {
		Is::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, nid)
	}

	fn new_td_root<'a>(&'a self, term: &'a str) -> TdRoot<'a> {
		TdRoot::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> Td<'a> {
		Td::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id)
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> Tt<'a> {
		Tt::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term, doc_id, nid, uid, add)
	}

	fn new_tt_term_range(&self, term: &str) -> Result<(Key, Key)> {
		Tt::term_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix, term)
	}

	fn new_tt_terms_range(&self) -> Result<(Key, Key)> {
		Tt::terms_range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> Dc<'_> {
		Dc::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id, nid, uid)
	}

	fn new_dc_compacted(&self) -> Result<Key> {
		Dc::new_root(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dc_range(&self) -> Result<(Key, Key)> {
		Dc::range(self.0.ns, self.0.db, &self.0.tb, self.0.ix)
	}

	fn new_dl(&self, doc_id: DocId) -> Dl<'_> {
		Dl::new(self.0.ns, self.0.db, &self.0.tb, self.0.ix, doc_id)
	}

	pub(crate) fn table(&self) -> &TableName {
		&self.0.tb
	}

	pub(crate) fn index(&self) -> IndexId {
		self.0.ix
	}
}
