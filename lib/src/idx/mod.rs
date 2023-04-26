mod bkeys;
pub(crate) mod btree;
pub(crate) mod ft;
mod kvsim;

use crate::dbs::Options;
use crate::err::Error;
use crate::idx::btree::NodeId;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::key::bd::Bd;
use crate::key::bf::{Bf, BfPrefix};
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
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexKeyBase {
	ns: String,
	db: String,
	tb: String,
	ix: String,
}

impl IndexKeyBase {
	pub(crate) fn new(opt: &Options, ix: &DefineIndexStatement) -> Self {
		Self {
			ns: opt.ns().to_string(),
			db: opt.db().to_string(),
			tb: ix.what.to_string(),
			ix: ix.name.to_string(),
		}
	}

	fn new_bd_key(&self, node_id: Option<NodeId>) -> Key {
		Bd::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), node_id).into()
	}

	fn new_bi_key(&self, doc_id: DocId) -> Key {
		Bi::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), doc_id).into()
	}

	fn new_bk_key(&self, doc_id: DocId) -> Key {
		Bk::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), doc_id).into()
	}

	fn new_bl_key(&self, node_id: Option<NodeId>) -> Key {
		Bl::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), node_id).into()
	}

	fn new_bp_key(&self, node_id: Option<NodeId>) -> Key {
		Bp::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), node_id).into()
	}

	fn new_bf_key(&self, term_id: TermId, doc_id: DocId) -> Key {
		Bf::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), term_id, doc_id)
			.into()
	}

	fn new_bf_prefix_key(&self, term_id: TermId) -> Key {
		BfPrefix::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), term_id)
			.into()
	}

	fn new_bt_key(&self, node_id: Option<NodeId>) -> Key {
		Bt::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), node_id).into()
	}

	fn new_bs_key(&self) -> Key {
		Bs::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone()).into()
	}

	fn new_bu_key(&self, term_id: TermId) -> Key {
		Bu::new(self.ns.clone(), self.db.clone(), self.tb.clone(), self.ix.clone(), term_id).into()
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

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::bkeys::KeyVisitor;
	use crate::idx::btree::Payload;
	use crate::kvs::{Key, Transaction};
	use async_trait::async_trait;
	use std::collections::HashMap;

	#[derive(Default)]
	pub(super) struct HashVisitor {
		map: HashMap<Key, Payload>,
	}

	#[async_trait]
	impl KeyVisitor for HashVisitor {
		async fn visit(
			&mut self,
			_tx: &mut Transaction,
			key: Key,
			payload: Payload,
		) -> Result<(), Error> {
			self.map.insert(key, payload);
			Ok(())
		}
	}

	impl HashVisitor {
		pub(super) fn check_len(&self, len: usize, info: &str) {
			assert_eq!(self.map.len(), len, "len issue: {}", info);
		}
		pub(super) fn check(&self, res: Vec<(Key, Payload)>, info: &str) {
			self.check_len(res.len(), info);
			for (k, p) in res {
				assert_eq!(self.map.get(&k), Some(&p));
			}
		}
	}
}
