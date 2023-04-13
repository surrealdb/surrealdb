use crate::kvs::kv::Key;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineLoginStatement;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::statements::DefineParamStatement;
use crate::sql::statements::DefineScopeStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::DefineTokenStatement;
use crate::sql::statements::LiveStatement;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub enum Entry {
	// Single definitions
	Db(Arc<DefineDatabaseStatement>),
	Ns(Arc<DefineNamespaceStatement>),
	Tb(Arc<DefineTableStatement>),
	// Multi definitions
	Dbs(Arc<[DefineDatabaseStatement]>),
	Dls(Arc<[DefineLoginStatement]>),
	Dts(Arc<[DefineTokenStatement]>),
	Evs(Arc<[DefineEventStatement]>),
	Fcs(Arc<[DefineFunctionStatement]>),
	Fds(Arc<[DefineFieldStatement]>),
	Fts(Arc<[DefineTableStatement]>),
	Ixs(Arc<[DefineIndexStatement]>),
	Lvs(Arc<[LiveStatement]>),
	Nls(Arc<[DefineLoginStatement]>),
	Nss(Arc<[DefineNamespaceStatement]>),
	Nts(Arc<[DefineTokenStatement]>),
	Pas(Arc<[DefineParamStatement]>),
	Scs(Arc<[DefineScopeStatement]>),
	Sts(Arc<[DefineTokenStatement]>),
	Tbs(Arc<[DefineTableStatement]>),
}

#[derive(Default)]
pub struct Cache(pub HashMap<Key, Entry>);

impl Cache {
	/// Set a key in the cache
	pub fn set(&mut self, key: Key, val: Entry) {
		self.0.insert(key, val);
	}
	/// Get a key from the cache
	pub fn get(&mut self, key: &Key) -> Option<Entry> {
		self.0.get(key).cloned()
	}
	/// Delete a key from the cache
	pub fn del(&mut self, key: &Key) -> Option<Entry> {
		self.0.remove(key)
	}
}
