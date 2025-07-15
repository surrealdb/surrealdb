pub mod docids;
pub(crate) mod ft;
pub(crate) mod index;
pub mod planner;
pub mod trees;

use crate::expr::{Id, Thing};
use crate::idx::docids::DocId;
use crate::idx::ft::search::terms::TermId;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::store::NodeId;
use crate::idx::trees::vector::SerializedVector;
use crate::key::index::bc::Bc;
use crate::key::index::bd::Bd;
use crate::key::index::bf::Bf;
use crate::key::index::bi::Bi;
use crate::key::index::bk::Bk;
use crate::key::index::bl::Bl;
use crate::key::index::bo::Bo;
use crate::key::index::bp::Bp;
use crate::key::index::bs::Bs;
use crate::key::index::bt::Bt;
use crate::key::index::bu::Bu;
use crate::key::index::hd::Hd;
use crate::key::index::he::He;
use crate::key::index::hi::Hi;
use crate::key::index::hl::Hl;
use crate::key::index::hs::Hs;
use crate::key::index::hv::Hv;
use crate::key::index::id::Id as IdKey;
use crate::key::index::vm::Vm;

use crate::key::index::dc::Dc;
use crate::key::index::dl::Dl;
use crate::key::index::ib::Ib;
use crate::key::index::is::Is;
use crate::key::index::td::Td;
use crate::key::index::tt::Tt;
use crate::kvs::{Key, KeyEncode as _, Val};
use anyhow::Result;
use revision::Revisioned;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexKeyBase(Arc<Inner>);

#[derive(Debug, Default, Hash, PartialEq, Eq)]
struct Inner {
	ns: String,
	db: String,
	tb: String,
	ix: String,
}

impl IndexKeyBase {
	pub(crate) fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Self> {
		Ok(Self(Arc::new(Inner {
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
			ix: ix.to_string(),
		})))
	}

	fn new_bc_key(&self, term_id: TermId) -> Result<Key> {
		Bc::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term_id).encode()
	}

	fn new_bd_key(&self, node_id: Option<NodeId>) -> Result<Key> {
		Bd::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, node_id).encode()
	}

	fn new_bk_key(&self, doc_id: DocId) -> Result<Key> {
		Bk::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, doc_id).encode()
	}

	fn new_bl_key(&self, node_id: Option<NodeId>) -> Result<Key> {
		Bl::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, node_id).encode()
	}

	fn new_bo_key(&self, doc_id: DocId, term_id: TermId) -> Result<Key> {
		Bo::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, doc_id, term_id).encode()
	}

	fn new_bp_key(&self, node_id: Option<NodeId>) -> Result<Key> {
		Bp::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, node_id).encode()
	}

	fn new_bf_key(&self, term_id: TermId, doc_id: DocId) -> Result<Key> {
		Bf::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term_id, doc_id).encode()
	}

	fn new_bt_key(&self, node_id: Option<NodeId>) -> Result<Key> {
		Bt::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, node_id).encode()
	}

	fn new_bs_key(&self) -> Result<Key> {
		Bs::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix).encode()
	}

	fn new_bu_key(&self, term_id: TermId) -> Result<Key> {
		Bu::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term_id).encode()
	}

	fn new_hd_key(&self, doc_id: Option<DocId>) -> Result<Key> {
		Hd::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, doc_id).encode()
	}

	fn new_he_key(&self, element_id: ElementId) -> Result<Key> {
		He::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, element_id).encode()
	}

	fn new_hi_key(&self, id: Id) -> Result<Key> {
		Hi::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, id).encode()
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> Result<Key> {
		Hl::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, layer, chunk).encode()
	}

	fn new_hv_key(&self, vec: Arc<SerializedVector>) -> Result<Key> {
		Hv::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, vec).encode()
	}

	fn new_hs_key(&self) -> Result<Key> {
		Hs::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix).encode()
	}

	fn new_vm_key(&self, node_id: Option<NodeId>) -> Result<Key> {
		Vm::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, node_id).encode()
	}

	fn new_bi_key(&self, doc_id: DocId) -> Bi {
		Bi::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn new_id_key(&self, id: Id) -> IdKey {
		IdKey::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, id)
	}

	pub(crate) fn new_ib_key(&self, start: i64) -> Result<Key> {
		Ib::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, start).encode()
	}

	pub(crate) fn new_ib_range(&self) -> Result<(Key, Key)> {
		Ib::new_range(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix)
	}

	pub(crate) fn new_is_key(&self, nid: Uuid) -> Result<Key> {
		Is::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, nid).encode()
	}

	fn new_td<'a>(&'a self, term: &'a str, doc_id: DocId) -> Td<'a> {
		Td::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term, doc_id)
	}

	fn new_tt<'a>(
		&'a self,
		term: &'a str,
		doc_id: DocId,
		nid: Uuid,
		uid: Uuid,
		add: bool,
	) -> Tt<'a> {
		Tt::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term, Some((doc_id, nid, uid, add)))
	}

	fn new_tt_compacted<'a>(&'a self, term: &'a str) -> Tt<'a> {
		Tt::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term, None)
	}

	fn new_tt_range(&self, term: &str) -> Result<(Key, Key)> {
		Tt::range(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, term)
	}

	fn new_dc_with_id(&self, doc_id: DocId, nid: Uuid, uid: Uuid) -> Dc {
		Dc::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, Some((doc_id, nid, uid)))
	}

	fn _new_dc_compacted(&self) -> Dc {
		Dc::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, None)
	}

	fn new_dc_range(&self) -> Result<(Key, Key)> {
		Dc::range(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix)
	}

	fn new_dl(&self, doc_id: DocId) -> Dl {
		Dl::new(&self.0.ns, &self.0.db, &self.0.tb, &self.0.ix, doc_id)
	}

	fn table(&self) -> &str {
		&self.0.tb
	}
}

/// This trait provides `Revision` based default implementations for serialization/deserialization
trait VersionedStore
where
	Self: Sized + Serialize + DeserializeOwned + Revisioned,
{
	fn try_into(&self) -> Result<Val> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	fn try_from(val: Val) -> Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

impl VersionedStore for Thing {}
