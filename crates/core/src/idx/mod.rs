pub mod docids;
pub(crate) mod ft;
pub(crate) mod index;
pub mod planner;
pub mod trees;

use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::idx::docids::DocId;
use crate::idx::ft::search::terms::TermId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::store::NodeId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::index::bc::Bc;
use crate::key::index::bd::{Bd, BdRoot};
use crate::key::index::bf::Bf;
use crate::key::index::bi::Bi;
use crate::key::index::bk::Bk;
use crate::key::index::bl::{Bl, BlRoot};
use crate::key::index::bo::Bo;
use crate::key::index::bp::{Bp, BpRoot};
use crate::key::index::bs::Bs;
use crate::key::index::bt::{Bt, BtRoot};
use crate::key::index::bu::Bu;
use crate::key::index::dc::Dc;
use crate::key::index::dl::Dl;
use crate::key::index::hd::{Hd, HdRoot};
use crate::key::index::he::He;
use crate::key::index::hi::Hi;
use crate::key::index::hl::Hl;
use crate::key::index::hs::Hs;
use crate::key::index::hv::Hv;
#[cfg(not(target_family = "wasm"))]
use crate::key::index::ia::Ia;
use crate::key::index::ib::Ib;
use crate::key::index::id::Id as IdKey;
use crate::key::index::ii::Ii;
#[cfg(not(target_family = "wasm"))]
use crate::key::index::ip::Ip;
use crate::key::index::is::Is;
use crate::key::index::td::{Td, TdRoot};
use crate::key::index::tt::Tt;
use crate::key::index::vm::{Vm, VmRoot};
use crate::key::root::ic::Ic;
use crate::kvs::Key;
use crate::val::RecordIdKey;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct IndexKeyBase(Arc<Inner>);

#[derive(Debug, Hash, PartialEq, Eq)]
struct Inner {
	ns: NamespaceId,
	db: DatabaseId,
	tb: String,
	ix: String,
}

impl IndexKeyBase {
	pub fn new(ns: impl Into<NamespaceId>, db: impl Into<DatabaseId>, tb: &str, ix: &str) -> Self {
		Self(Arc::new(Inner {
			ns: ns.into(),
			db: db.into(),
			tb: tb.to_string(),
			ix: ix.to_string(),
		}))
	}

	pub(crate) fn from_ic(ic: &Ic) -> Self {
		Self(Arc::new(Inner {
			ns: ic.ns,
			db: ic.db,
			tb: ic.tb.to_string(),
			ix: ic.ix.to_string(),
		}))
	}

	pub(crate) fn match_ic(&self, ic: &Ic) -> bool {
		self.0.ix == ic.ix && self.0.tb == ic.tb && self.0.db == ic.db && self.0.ns == ic.ns
	}

	fn new_bc_key(&self, term_id: TermId) -> Bc<'_> {
		Bc::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term_id)
	}

	fn new_bd_root_key(&self) -> BdRoot<'_> {
		BdRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_bd_key(&self, node_id: NodeId) -> Bd<'_> {
		Bd::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, node_id)
	}

	fn new_bk_key(&self, doc_id: DocId) -> Bk<'_> {
		Bk::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn new_bl_root_key(&self) -> BlRoot<'_> {
		BlRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_bl_key(&self, node_id: NodeId) -> Bl<'_> {
		Bl::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, node_id)
	}

	fn new_bo_key(&self, doc_id: DocId, term_id: TermId) -> Bo<'_> {
		Bo::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id, term_id)
	}

	fn new_bp_root_key(&self) -> BpRoot<'_> {
		BpRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_bp_key(&self, node_id: NodeId) -> Bp<'_> {
		Bp::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, node_id)
	}

	fn new_bf_key(&self, term_id: TermId, doc_id: DocId) -> Bf<'_> {
		Bf::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term_id, doc_id)
	}

	fn new_bt_root_key(&self) -> BtRoot<'_> {
		BtRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_bt_key(&self, node_id: NodeId) -> Bt<'_> {
		Bt::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, node_id)
	}

	fn new_bs_key(&self) -> Bs<'_> {
		Bs::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_bu_key(&self, term_id: TermId) -> Bu<'_> {
		Bu::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term_id)
	}

	fn new_hd_root_key(&self) -> HdRoot<'_> {
		HdRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_hd_key(&self, doc_id: DocId) -> Hd<'_> {
		Hd::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn new_he_key(&self, element_id: ElementId) -> He<'_> {
		He::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, element_id)
	}

	fn new_hi_key(&self, id: RecordIdKey) -> Hi<'_> {
		Hi::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, id)
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> Hl<'_> {
		Hl::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, layer, chunk)
	}

	fn new_hv_key(&self, vec: Arc<SerializedVector>) -> Hv<'_> {
		Hv::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, vec)
	}

	fn new_hs_key(&self) -> Hs<'_> {
		Hs::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_vm_root_key(&self) -> VmRoot<'_> {
		VmRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_vm_key(&self, node_id: NodeId) -> Vm<'_> {
		Vm::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, node_id)
	}

	fn new_bi_key(&self, doc_id: DocId) -> Bi<'_> {
		Bi::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn new_ii_key(&self, doc_id: DocId) -> Ii<'_> {
		Ii::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn new_id_key(&self, id: RecordIdKey) -> IdKey {
		IdKey::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, id)
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_ia_key(&self, i: u32) -> Ia<'_> {
		Ia::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, i)
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn new_ip_key(&self, id: RecordIdKey) -> Ip {
		Ip::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, id)
	}

	pub(crate) fn new_ib_key(&self, start: i64) -> Ib<'_> {
		Ib::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, start)
	}

	pub(crate) fn new_ic_key(&self, nid: Uuid) -> Ic {
		Ic::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, nid, Uuid::now_v7())
	}

	pub(crate) fn new_ib_range(&self) -> Result<Range<Key>> {
		Ib::new_range(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	pub(crate) fn new_is_key(&self, nid: Uuid) -> Is<'_> {
		Is::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, nid)
	}

	fn new_td_root<'a>(&'a self, term: &'a str) -> TdRoot<'a> {
		TdRoot::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term)
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> Td<'a> {
		Td::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term, doc_id)
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> Tt<'a> {
		Tt::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term, doc_id, nid, uid, add)
	}

	fn new_tt_term_range(&self, term: &str) -> Result<(Key, Key)> {
		Tt::term_range(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, term)
	}

	fn new_tt_terms_range(&self) -> Result<(Key, Key)> {
		Tt::terms_range(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> Dc {
		Dc::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id, nid, uid)
	}

	fn new_dc_compacted(&self) -> Result<Key> {
		Dc::new_root(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_dc_range(&self) -> Result<(Key, Key)> {
		Dc::range(self.0.ns, self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_dl(&self, doc_id: DocId) -> Dl<'_> {
		Dl::new(self.0.ns, self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	pub(crate) fn table(&self) -> &str {
		&self.0.tb
	}

	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn index(&self) -> &str {
		&self.0.ix
	}
}
