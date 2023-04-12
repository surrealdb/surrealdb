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
use async_trait_fn::async_trait;
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

pub trait SyncCache {
	// Check if key exists
	fn exi(&mut self, key: &Key) -> bool;
	// Set a key in the cache
	fn set(&mut self, key: Key, val: Entry);
	// Get a key from the cache
	fn get(&mut self, key: &Key) -> Option<Entry>;
	// Delete a key from the cache
	fn del(&mut self, key: &Key) -> Option<Entry>;
}

#[async_trait]
pub trait Cache: Send {
	// Check if key exists
	async fn exi(&mut self, key: &Key) -> bool;
	// Set a key in the cache
	async fn set(&mut self, key: Key, val: Entry);
	// Get a key from the cache
	async fn get(&mut self, key: &Key) -> Option<Entry>;
	// Delete a key from the cache
	async fn del(&mut self, key: &Key) -> Option<Entry>;
}

#[async_trait]
impl<T: SyncCache + Send> Cache for T {
	async fn exi(&mut self, key: &Key) -> bool {
		(self as &mut T).exi(key)
	}
	async fn set(&mut self, key: Key, val: Entry) {
		(self as &mut T).set(key, val)
	}
	async fn get(&mut self, key: &Key) -> Option<Entry> {
		(self as &mut T).get(key)
	}
	async fn del(&mut self, key: &Key) -> Option<Entry> {
		(self as &mut T).del(key)
	}
}

pub mod btreemap;
pub mod hashmap;
pub mod moka;
