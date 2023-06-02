mod bkeys;
pub(crate) mod btree;
pub(crate) mod ft;
pub(crate) mod planner;

use crate::dbs::Options;
use crate::err::Error;
use crate::idx::btree::NodeId;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::key::bc::Bc;
use crate::key::bd::Bd;
use crate::key::bf::Bf;
use crate::key::bi::Bi;
use crate::key::bk::Bk;
use crate::key::bl::Bl;
use crate::key::bp::Bp;
use crate::key::bs::Bs;
use crate::key::bt::Bt;
use crate::key::bu::Bu;
use crate::kvs::{Key, Val};
use crate::sql::statements::DefineIndexStatement;
use roaring::RoaringTreemap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(crate) struct IndexKeyBase {
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
	pub(crate) fn new(opt: &Options, ix: &DefineIndexStatement) -> Self {
		Self {
			inner: Arc::new(Inner {
				ns: opt.ns().to_string(),
				db: opt.db().to_string(),
				tb: ix.what.to_string(),
				ix: ix.name.to_string(),
			}),
		}
	}

	fn new_bc_key(&self, term_id: TermId) -> Key {
		Bc::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
		)
		.into()
	}

	fn new_bd_key(&self, node_id: Option<NodeId>) -> Key {
		Bd::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.into()
	}

	fn new_bi_key(&self, doc_id: DocId) -> Key {
		Bi::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
		)
		.into()
	}

	fn new_bk_key(&self, doc_id: DocId) -> Key {
		Bk::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			doc_id,
		)
		.into()
	}

	fn new_bl_key(&self, node_id: Option<NodeId>) -> Key {
		Bl::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.into()
	}

	fn new_bp_key(&self, node_id: Option<NodeId>) -> Key {
		Bp::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.into()
	}

	fn new_bf_key(&self, term_id: TermId, doc_id: DocId) -> Key {
		Bf::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
			doc_id,
		)
		.into()
	}

	fn new_bt_key(&self, node_id: Option<NodeId>) -> Key {
		Bt::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			node_id,
		)
		.into()
	}

	fn new_bs_key(&self) -> Key {
		Bs::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
		)
		.into()
	}

	fn new_bu_key(&self, term_id: TermId) -> Key {
		Bu::new(
			self.inner.ns.as_str(),
			self.inner.db.as_str(),
			self.inner.tb.as_str(),
			self.inner.ix.as_str(),
			term_id,
		)
		.into()
	}
}

/// This trait provides `bincode` based default implementations for serialization/deserialization
trait SerdeState
where
	Self: Sized + Serialize + DeserializeOwned,
{
	fn try_to_val(&self) -> Result<Val, Error> {
		Ok(bincode::serialize(self)?)
	}

	fn try_from_val(val: Val) -> Result<Self, Error> {
		Ok(bincode::deserialize(&val)?)
	}
}

impl SerdeState for RoaringTreemap {}
