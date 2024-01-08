use crate::idg::u32::U32;
use crate::kvs::kv::Key;
use crate::statements::DefineAnalyzerStatement;
use crate::statements::DefineDatabaseStatement;
use crate::statements::DefineEventStatement;
use crate::statements::DefineFieldStatement;
use crate::statements::DefineFunctionStatement;
use crate::statements::DefineIndexStatement;
use crate::statements::DefineModelStatement;
use crate::statements::DefineNamespaceStatement;
use crate::statements::DefineParamStatement;
use crate::statements::DefineScopeStatement;
use crate::statements::DefineTableStatement;
use crate::statements::DefineTokenStatement;
use crate::statements::DefineUserStatement;
use crate::statements::LiveStatement;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
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
	Azs(Arc<[DefineAnalyzerStatement]>),
	Dbs(Arc<[DefineDatabaseStatement]>),
	Dts(Arc<[DefineTokenStatement]>),
	Dus(Arc<[DefineUserStatement]>),
	Evs(Arc<[DefineEventStatement]>),
	Fcs(Arc<[DefineFunctionStatement]>),
	Fds(Arc<[DefineFieldStatement]>),
	Fts(Arc<[DefineTableStatement]>),
	Ixs(Arc<[DefineIndexStatement]>),
	Lvs(Arc<[LiveStatement]>),
	Mls(Arc<[DefineModelStatement]>),
	Nss(Arc<[DefineNamespaceStatement]>),
	Nts(Arc<[DefineTokenStatement]>),
	Nus(Arc<[DefineUserStatement]>),
	Pas(Arc<[DefineParamStatement]>),
	Scs(Arc<[DefineScopeStatement]>),
	Sts(Arc<[DefineTokenStatement]>),
	Tbs(Arc<[DefineTableStatement]>),
	// Sequences
	Seq(U32),
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
	/// Clears a cache completely
	pub fn clear(&mut self) {
		self.0.clear()
	}
}
