pub mod docids;
pub(crate) mod ft;
pub(crate) mod index;
pub mod planner;
pub mod trees;

use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::terms::TermId;
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
use crate::key::index::vm::Vm;
use crate::kvs::{Key, KeyEncode as _, Val};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Thing};
use revision::Revisioned;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct IndexKeyBase {
	inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
	ns: String,
	db: String,
	tb: String,
	ix: String,
}

impl IndexKeyBase {
	pub(crate) fn new(ns: &str, db: &str, ix: &DefineIndexStatement) -> Result<Self, Error> {
		Ok(Self {
			inner: Arc::new(Inner {
				ns: ns.to_string(),
				db: db.to_string(),
				tb: ix.what.to_string(),
				ix: ix.name.to_string(),
			}),
		})
	}

	fn new_bc_key(&self, term_id: TermId) -> Result<Key, Error> {
		Bc::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
		)
		.encode()
	}

	fn new_bd_key(&self, node_id: Option<NodeId>) -> Result<Key, Error> {
		Bd::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.encode()
	}

	fn new_bi_key(&self, doc_id: DocId) -> Result<Key, Error> {
		Bi::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
		)
		.encode()
	}

	fn new_bk_key(&self, doc_id: DocId) -> Result<Key, Error> {
		Bk::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
		)
		.encode()
	}

	fn new_bl_key(&self, node_id: Option<NodeId>) -> Result<Key, Error> {
		Bl::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.encode()
	}

	fn new_bo_key(&self, doc_id: DocId, term_id: TermId) -> Result<Key, Error> {
		Bo::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
			term_id,
		)
		.encode()
	}

	fn new_bp_key(&self, node_id: Option<NodeId>) -> Result<Key, Error> {
		Bp::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.encode()
	}

	fn new_bf_key(&self, term_id: TermId, doc_id: DocId) -> Result<Key, Error> {
		Bf::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
			doc_id,
		)
		.encode()
	}

	fn new_bt_key(&self, node_id: Option<NodeId>) -> Result<Key, Error> {
		Bt::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.encode()
	}

	fn new_bs_key(&self) -> Result<Key, Error> {
		Bs::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
		)
		.encode()
	}

	fn new_bu_key(&self, term_id: TermId) -> Result<Key, Error> {
		Bu::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
		)
		.encode()
	}

	fn new_hd_key(&self, doc_id: Option<DocId>) -> Result<Key, Error> {
		Hd::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
		)
		.encode()
	}

	fn new_he_key(&self, element_id: ElementId) -> Result<Key, Error> {
		He::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			element_id,
		)
		.encode()
	}

	fn new_hi_key(&self, id: Id) -> Result<Key, Error> {
		Hi::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			id,
		)
		.encode()
	}

	fn new_hl_key(&self, layer: u16, chunk: u32) -> Result<Key, Error> {
		Hl::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			layer,
			chunk,
		)
		.encode()
	}

	fn new_hv_key(&self, vec: Arc<SerializedVector>) -> Result<Key, Error> {
		Hv::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			vec,
		)
		.encode()
	}

	fn new_hs_key(&self) -> Result<Key, Error> {
		Hs::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
		)
		.encode()
	}

	fn new_vm_key(&self, node_id: Option<NodeId>) -> Result<Key, Error> {
		Vm::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.encode()
	}
}

/// This trait provides `Revision` based default implementations for serialization/deserialization
trait VersionedStore
where
	Self: Sized + Serialize + DeserializeOwned + Revisioned,
{
	fn try_into(&self) -> Result<Val, Error> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	fn try_from(val: Val) -> Result<Self, Error> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

impl VersionedStore for Thing {}
