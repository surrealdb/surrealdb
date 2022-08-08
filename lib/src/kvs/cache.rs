use crate::kvs::kv::Key;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineLoginStatement;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::statements::DefineScopeStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::DefineTokenStatement;
use crate::sql::statements::LiveStatement;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub enum Entry {
	Ns(Arc<DefineNamespaceStatement>),
	Db(Arc<DefineDatabaseStatement>),
	Tb(Arc<DefineTableStatement>),
	Nss(Arc<Vec<DefineNamespaceStatement>>),
	Nls(Arc<Vec<DefineLoginStatement>>),
	Nts(Arc<Vec<DefineTokenStatement>>),
	Dbs(Arc<Vec<DefineDatabaseStatement>>),
	Dls(Arc<Vec<DefineLoginStatement>>),
	Dts(Arc<Vec<DefineTokenStatement>>),
	Scs(Arc<Vec<DefineScopeStatement>>),
	Sts(Arc<Vec<DefineTokenStatement>>),
	Tbs(Arc<Vec<DefineTableStatement>>),
	Evs(Arc<Vec<DefineEventStatement>>),
	Fds(Arc<Vec<DefineFieldStatement>>),
	Ixs(Arc<Vec<DefineIndexStatement>>),
	Fts(Arc<Vec<DefineTableStatement>>),
	Lvs(Arc<Vec<LiveStatement>>),
}

#[derive(Default)]
pub struct Cache(pub HashMap<Key, Entry>);

impl Cache {
	// Check if key exists
	pub fn exi(&mut self, key: &Key) -> bool {
		self.0.contains_key(key)
	}
	// Set a key in the cache
	pub fn set(&mut self, key: Key, val: Entry) {
		self.0.insert(key, val);
	}
	// get a key from the cache
	pub fn get(&mut self, key: &Key) -> Option<Entry> {
		self.0.get(key).cloned()
	}
}
