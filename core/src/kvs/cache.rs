use crate::idg::u32::U32;
use crate::kvs::kv::Key;
use crate::sql::statements::AccessGrant;
use crate::sql::statements::DefineAccessStatement;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::statements::DefineEventStatement;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::statements::DefineModelStatement;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::statements::DefineParamStatement;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::DefineUserStatement;
use crate::sql::statements::LiveStatement;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
#[non_exhaustive]
pub enum Entry {
	// Single definitions
	Db(Arc<DefineDatabaseStatement>),
	Fc(Arc<DefineFunctionStatement>),
	Ix(Arc<DefineIndexStatement>),
	Ml(Arc<DefineModelStatement>),
	Ns(Arc<DefineNamespaceStatement>),
	Pa(Arc<DefineParamStatement>),
	Tb(Arc<DefineTableStatement>),
	// Multi definitions
	Ags(Arc<[AccessGrant]>),
	Azs(Arc<[DefineAnalyzerStatement]>),
	Dbs(Arc<[DefineDatabaseStatement]>),
	Das(Arc<[DefineAccessStatement]>),
	Dus(Arc<[DefineUserStatement]>),
	Evs(Arc<[DefineEventStatement]>),
	Fcs(Arc<[DefineFunctionStatement]>),
	Fds(Arc<[DefineFieldStatement]>),
	Fts(Arc<[DefineTableStatement]>),
	Ixs(Arc<[DefineIndexStatement]>),
	Lvs(Arc<[LiveStatement]>),
	Mls(Arc<[DefineModelStatement]>),
	Nss(Arc<[DefineNamespaceStatement]>),
	Nas(Arc<[DefineAccessStatement]>),
	Nus(Arc<[DefineUserStatement]>),
	Pas(Arc<[DefineParamStatement]>),
	Tbs(Arc<[DefineTableStatement]>),
	// Sequences
	Seq(U32),
}

#[derive(Default)]
#[non_exhaustive]
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
	/// Clears a cache completely
	pub fn clear(&mut self) {
		self.0.clear()
	}
}
