use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use futures::lock::{Mutex, MutexGuard};
use futures::stream::Stream;
use uuid::Uuid;

use super::batch::Batch;
use super::tr::Check;
use super::{Key, Val, Version, util};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, NamespaceDefinition, NamespaceId, TableDefinition,
};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::expr::statements::access::AccessGrantStore;
use crate::expr::statements::define::config::ConfigStore;
use crate::expr::statements::define::{ApiDefinition, BucketDefinition, DefineSequenceStatement};
use crate::expr::statements::{
	DefineAccessStatement, DefineAnalyzerStatement, DefineEventStatement, DefineFieldStatement,
	DefineFunctionStatement, DefineIndexStatement, DefineModelStatement, DefineParamStore,
	DefineUserStatement, LiveStatement,
};
use crate::idx::planner::ScanDirection;
use crate::idx::trees::store::cache::IndexTreeCaches;
use crate::key::database::sq::Sq;
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::key::KVKey;
use crate::kvs::scanner::Scanner;
use crate::kvs::{Transactor, cache};
use crate::val::record::Record;
use crate::val::{RecordId, RecordIdKey};

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The underlying transactor
	tx: Mutex<Transactor>,
	/// The query cache for this store
	cache: TransactionCache,
	/// Cache the index updates
	index_caches: IndexTreeCaches,
	/// Does this support reverse scan?
	reverse_scan: bool,
}

impl Transaction {
	/// Create a new query store
	pub fn new(local: bool, tx: Transactor) -> Transaction {
		Transaction {
			local,
			reverse_scan: tx.inner.supports_reverse_scan(),
			tx: Mutex::new(tx),
			cache: TransactionCache::new(),
			index_caches: IndexTreeCaches::default(),
		}
	}

	/// Retrieve the underlying transaction
	pub fn inner(self) -> Transactor {
		self.tx.into_inner()
	}

	/// Enclose this transaction in an [`Arc`]
	pub fn enclose(self) -> Arc<Transaction> {
		Arc::new(self)
	}

	/// Retrieve the underlying transaction
	pub async fn lock(&self) -> MutexGuard<'_, Transactor> {
		self.tx.lock().await
	}

	/// Check if the transaction is local or remote
	pub fn local(&self) -> bool {
		self.local
	}

	/// Check if the transaction supports reverse scan
	pub fn reverse_scan(&self) -> bool {
		self.reverse_scan
	}

	/// Check if the transaction is finished.
	///
	/// If the transaction has been canceled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	pub async fn closed(&self) -> bool {
		self.lock().await.closed()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn cancel(&self) -> Result<()> {
		self.lock().await.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<()> {
		self.lock().await.commit().await
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn exists<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
	where
		K: KVKey + Debug,
	{
		self.lock().await.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get<K>(&self, key: &K, version: Option<u64>) -> Result<Option<K::ValueType>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.get(key, version).await
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<K::ValueType>>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.getm(keys).await
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in
	/// grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getp<K>(&self, key: &K) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.getp(key).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in
	/// grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr<K>(&self, rng: Range<K>, version: Option<u64>) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.getr(rng, version).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.delc(key, chk).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.delr(rng).await
	}

	/// Delete a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.delp(key).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.clrc(key, chk).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.clrr(rng).await
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.clrp(key).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.set(key, val, version).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.put(key, val, version).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn putc<K>(
		&self,
		key: &K,
		val: &K::ValueType,
		chk: Option<&K::ValueType>,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.lock().await.putc(key, val, chk).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys, in a single request to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keys<K>(&self, rng: Range<K>, limit: u32, version: Option<u64>) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.keys(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of keys, in a single request to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.keysr(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.scan(rng, limit, version).await
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.scanr(rng, limit, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn count<K>(&self, rng: Range<K>) -> Result<usize>
	where
		K: KVKey + Debug,
	{
		self.lock().await.count(rng).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the keys in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.batch_keys(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.batch_keys_vals(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value-version pairs in batches, with
	/// multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals_versions<K>(
		&self,
		rng: Range<K>,
		batch: u32,
	) -> Result<Batch<(Key, Val, Version, bool)>>
	where
		K: KVKey + Debug,
	{
		self.lock().await.batch_keys_vals_versions(rng, batch).await
	}

	/// Retrieve a stream over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream(
		&self,
		rng: Range<Vec<u8>>,
		version: Option<u64>,
		limit: Option<usize>,
		sc: ScanDirection,
	) -> impl Stream<Item = Result<(Key, Val)>> + '_ {
		Scanner::<(Key, Val)>::new(self, *NORMAL_FETCH_SIZE, rng, version, limit, sc)
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys(
		&self,
		rng: Range<Vec<u8>>,
		limit: Option<usize>,
		sc: ScanDirection,
	) -> impl Stream<Item = Result<Key>> + '_ {
		Scanner::<Key>::new(self, *NORMAL_FETCH_SIZE, rng, None, limit, sc)
	}

	// --------------------------------------------------
	// Rollback methods
	// --------------------------------------------------

	/// Warn if this transaction is dropped without proper handling.
	pub async fn rollback_with_warning(self) -> Self {
		self.tx.lock().await.check_level(Check::Warn);
		self
	}

	/// Error if this transaction is dropped without proper handling.
	pub async fn rollback_with_error(self) -> Self {
		self.tx.lock().await.check_level(Check::Error);
		self
	}

	/// Do nothing if this transaction is dropped without proper handling.
	pub async fn rollback_and_ignore(self) -> Self {
		self.tx.lock().await.check_level(Check::None);
		self
	}

	// --------------------------------------------------
	// Cache methods
	// --------------------------------------------------

	/// Retrieve all nodes belonging to this cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_nodes(&self) -> Result<Arc<[Node]>> {
		let qey = cache::tx::Lookup::Nds;
		match self.cache.get(&qey) {
			Some(val) => val.try_into_nds(),
			None => {
				let beg = crate::key::root::nd::prefix();
				let end = crate::key::root::nd::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Nds(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all ROOT level users in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_users(&self) -> Result<Arc<[DefineUserStatement]>> {
		let qey = cache::tx::Lookup::Rus;
		match self.cache.get(&qey) {
			Some(val) => val.try_into_rus(),
			None => {
				let beg = crate::key::root::us::prefix();
				let end = crate::key::root::us::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Rus(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all ROOT level accesses in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_accesses(&self) -> Result<Arc<[DefineAccessStatement]>> {
		let qey = cache::tx::Lookup::Ras;
		match self.cache.get(&qey) {
			Some(val) => val.try_into_ras(),
			None => {
				let beg = crate::key::root::ac::prefix();
				let end = crate::key::root::ac::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Ras(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all root access grants in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[AccessGrantStore]>> {
		let qey = cache::tx::Lookup::Rgs(ra);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_rag(),
			None => {
				let beg = crate::key::root::access::gr::prefix(ra)?;
				let end = crate::key::root::access::gr::suffix(ra)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Rag(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace definitions in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns(&self) -> Result<Arc<[NamespaceDefinition]>> {
		let qey = cache::tx::Lookup::Nss;
		match self.cache.get(&qey) {
			Some(val) => val.try_into_nss(),
			None => {
				let beg = crate::key::root::ns::prefix();
				let end = crate::key::root::ns::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Nss(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_users(&self, ns: NamespaceId) -> Result<Arc<[DefineUserStatement]>> {
		let qey = cache::tx::Lookup::Nus(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_nus(),
			None => {
				let beg = crate::key::namespace::us::prefix(ns)?;
				let end = crate::key::namespace::us::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Nus(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_accesses(&self, ns: NamespaceId) -> Result<Arc<[DefineAccessStatement]>> {
		let qey = cache::tx::Lookup::Nas(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_nas(),
			None => {
				let beg = crate::key::namespace::ac::prefix(ns)?;
				let end = crate::key::namespace::ac::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Nas(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace access grants for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_access_grants(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Arc<[AccessGrantStore]>> {
		let qey = cache::tx::Lookup::Ngs(ns, na);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_nag(),
			None => {
				let beg = crate::key::namespace::access::gr::prefix(ns, na)?;
				let end = crate::key::namespace::access::gr::suffix(ns, na)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Nag(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all database definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db(&self, ns: NamespaceId) -> Result<Arc<[DatabaseDefinition]>> {
		let qey = cache::tx::Lookup::Dbs(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_dbs(),
			None => {
				let beg = crate::key::namespace::db::prefix(ns)?;
				let end = crate::key::namespace::db::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Dbs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all database user definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_users(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineUserStatement]>> {
		let qey = cache::tx::Lookup::Dus(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_dus(),
			None => {
				let beg = crate::key::database::us::prefix(ns, db)?;
				let end = crate::key::database::us::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Dus(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all database access definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineAccessStatement]>> {
		let qey = cache::tx::Lookup::Das(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_das(),
			None => {
				let beg = crate::key::database::ac::prefix(ns, db)?;
				let end = crate::key::database::ac::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Das(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all database access grants for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_access_grants(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Arc<[AccessGrantStore]>> {
		let qey = cache::tx::Lookup::Dgs(ns, db, da);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_dag(),
			None => {
				let beg = crate::key::database::access::gr::prefix(ns, db, da)?;
				let end = crate::key::database::access::gr::suffix(ns, db, da)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Dag(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all api definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_apis(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[ApiDefinition]>> {
		let qey = cache::tx::Lookup::Aps(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::ap::prefix(ns, db)?;
				let end = crate::key::database::ap::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let val = cache::tx::Entry::Aps(Arc::clone(&val));
				self.cache.insert(qey, val.clone());
				val
			}
		}
		.try_into_aps()
	}

	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineAnalyzerStatement]>> {
		let qey = cache::tx::Lookup::Azs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_azs(),
			None => {
				let beg = crate::key::database::az::prefix(ns, db)?;
				let end = crate::key::database::az::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Azs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[BucketDefinition]>> {
		let qey = cache::tx::Lookup::Bus(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_bus(),
			None => {
				let beg = crate::key::database::bu::prefix(ns, db)?;
				let end = crate::key::database::bu::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Bus(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all sequences definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineSequenceStatement]>> {
		let qey = cache::tx::Lookup::Sqs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_sqs(),
			None => {
				let beg = crate::key::database::sq::prefix(ns, db)?;
				let end = crate::key::database::sq::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Sqs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all function definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineFunctionStatement]>> {
		let qey = cache::tx::Lookup::Fcs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_fcs(),
			None => {
				let beg = crate::key::database::fc::prefix(ns, db)?;
				let end = crate::key::database::fc::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Fcs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all param definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineParamStore]>> {
		let qey = cache::tx::Lookup::Pas(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_pas(),
			None => {
				let beg = crate::key::database::pa::prefix(ns, db)?;
				let end = crate::key::database::pa::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Pas(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all model definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[DefineModelStatement]>> {
		let qey = cache::tx::Lookup::Mls(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_mls(),
			None => {
				let beg = crate::key::database::ml::prefix(ns, db)?;
				let end = crate::key::database::ml::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Mls(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all model definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[ConfigStore]>> {
		let qey = cache::tx::Lookup::Cgs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_cgs(),
			None => {
				let beg = crate::key::database::cg::prefix(ns, db)?;
				let end = crate::key::database::cg::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Cgs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all table definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> Result<Arc<[TableDefinition]>> {
		let qey = cache::tx::Lookup::Tbs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_tbs(),
			None => {
				let beg = crate::key::database::tb::prefix(ns, db)?;
				let end = crate::key::database::tb::suffix(ns, db)?;
				let val = self.getr(beg..end, version).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Tbs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all event definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_events(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>> {
		let qey = cache::tx::Lookup::Evs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_evs(),
			None => {
				let beg = crate::key::table::ev::prefix(ns, db, tb)?;
				let end = crate::key::table::ev::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Evs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all field definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		version: Option<u64>,
	) -> Result<Arc<[DefineFieldStatement]>> {
		let qey = cache::tx::Lookup::Fds(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_fds(),
			None => {
				let beg = crate::key::table::fd::prefix(ns, db, tb)?;
				let end = crate::key::table::fd::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, version).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Fds(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all index definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_indexes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>> {
		let qey = cache::tx::Lookup::Ixs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_ixs(),
			None => {
				let beg = crate::key::table::ix::prefix(ns, db, tb)?;
				let end = crate::key::table::ix::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Ixs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all view definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_views(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[TableDefinition]>> {
		let qey = cache::tx::Lookup::Fts(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_fts(),
			None => {
				let beg = crate::key::table::ft::prefix(ns, db, tb)?;
				let end = crate::key::table::ft::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Fts(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all live definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_lives(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>> {
		let qey = cache::tx::Lookup::Lvs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_lvs(),
			None => {
				let beg = crate::key::table::lq::prefix(ns, db, tb)?;
				let end = crate::key::table::lq::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Lvs(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific node in the cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_node(&self, id: Uuid) -> Result<Arc<Node>> {
		let qey = cache::tx::Lookup::Nd(id);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::nd::new(id);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::NdNotFound {
					uuid: id.to_string(),
				})?;
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey, val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific root user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_user(&self, us: &str) -> Result<Option<Arc<DefineUserStatement>>> {
		let qey = cache::tx::Lookup::Ru(us);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::root::us::new(us);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	pub async fn expect_root_user(&self, us: &str) -> Result<Arc<DefineUserStatement>> {
		match self.get_root_user(us).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::UserRootNotFound {
				name: us.to_owned(),
			}),
		}
	}

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access(&self, ra: &str) -> Result<Option<Arc<DefineAccessStatement>>> {
		let qey = cache::tx::Lookup::Ra(ra);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::root::ac::new(ra);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	pub async fn expect_root_access(&self, ra: &str) -> Result<Arc<DefineAccessStatement>> {
		match self.get_root_access(ra).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::AccessRootNotFound {
				ac: ra.to_owned(),
			}),
		}
	}

	/// Retrieve a specific root access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access_grant(
		&self,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<AccessGrantStore>>> {
		let qey = cache::tx::Lookup::Rg(ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::root::access::gr::new(ac, gr);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific namespace definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns(&self, ns: NamespaceId) -> Result<Option<Arc<NamespaceDefinition>>> {
		let qey = cache::tx::Lookup::NsById(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::root::ns::new(ns);
				let Some(ns_def) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let ns_def = Arc::new(ns_def);
				let entr = cache::tx::Entry::Any(ns_def.clone());
				self.cache.insert(qey, entr);
				Ok(Some(ns_def))
			}
		}
	}

	pub async fn get_ns_by_name(&self, ns: &str) -> Result<Option<Arc<NamespaceDefinition>>> {
		let qey = cache::tx::Lookup::NsByName(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::catalog::ns::new(ns);
				let Some(ns) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let ns = Arc::new(ns);
				let entr = cache::tx::Entry::Any(ns.clone());
				self.cache.insert(qey, entr);
				Ok(Some(ns))
			}
		}
	}

	pub async fn expect_ns_by_name(&self, ns: &str) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::NsNotFound {
				name: ns.to_owned(),
			}),
		}
	}

	pub(crate) async fn put_ns(&self, ns: NamespaceDefinition) -> Result<Arc<NamespaceDefinition>> {
		let key = crate::key::catalog::ns::new(&ns.name);
		self.put(&key, &ns, None).await?;

		let key = crate::key::root::ns::new(ns.namespace_id);
		self.put(&key, &ns, None).await?;

		// Populate cache
		let cached_ns = Arc::new(ns.clone());

		let qey = cache::tx::Lookup::NsById(ns.namespace_id);
		let entry = cache::tx::Entry::Any(Arc::clone(&cached_ns) as Arc<dyn Any + Send + Sync>);
		self.cache.insert(qey, entry.clone());

		let qey = cache::tx::Lookup::NsByName(&ns.name);
		self.cache.insert(qey, entry);

		Ok(cached_ns)
	}

	pub(crate) async fn put_db(
		&self,
		ns: &str,
		db: DatabaseDefinition,
	) -> Result<Arc<DatabaseDefinition>> {
		let key = crate::key::catalog::db::new(ns, &db.name);
		self.put(&key, &db, None).await?;

		let key = crate::key::namespace::db::new(db.namespace_id, db.database_id);
		self.put(&key, &db, None).await?;

		// Populate cache
		let cached_db = Arc::new(db.clone());

		let qey = cache::tx::Lookup::DbById(db.namespace_id, db.database_id);
		let entry = cache::tx::Entry::Any(Arc::clone(&cached_db) as Arc<dyn Any + Send + Sync>);
		self.cache.insert(qey, entry.clone());

		let qey = cache::tx::Lookup::DbByName(ns, &db.name);
		self.cache.insert(qey, entry);

		Ok(cached_db)
	}

	pub async fn get_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>> {
		let qey = cache::tx::Lookup::TbByName(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::catalog::tb::new(ns, db, tb);
				let Some(tb) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let tb = Arc::new(tb);
				let entr = cache::tx::Entry::Any(tb.clone());
				self.cache.insert(qey, entr);
				Ok(Some(tb))
			}
		}
	}

	pub(crate) async fn put_tb(
		&self,
		ns: &str,
		db: &str,
		tb: TableDefinition,
	) -> Result<Arc<TableDefinition>> {
		let key = crate::key::catalog::tb::new(ns, db, &tb.name);
		self.set(&key, &tb, None).await?;

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.set(&key, &tb, None).await?;

		// Populate cache
		let cached_tb = Arc::new(tb.clone());
		let cached_entry =
			cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);

		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.insert(qey, cached_entry.clone());

		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.insert(qey, cached_entry);

		Ok(cached_tb)
	}

	pub(crate) async fn del_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.to_string(),
			}
			.into());
		};

		{
			let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
			self.del(&key).await?;
		}
		{
			let key = crate::key::catalog::tb::new(ns, db, &tb.name);
			self.del(&key).await?;
		}

		// Clear the cache
		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.remove(qey);
		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.remove(qey);

		Ok(())
	}

	pub(crate) async fn clr_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.to_string(),
			}
			.into());
		};

		{
			let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
			self.clr(&key).await?;
		}
		{
			let key = crate::key::catalog::tb::new(ns, db, &tb.name);
			self.clr(&key).await?;
		}

		// Clear the cache
		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.remove(qey);
		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.remove(qey);

		Ok(())
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_user(
		&self,
		ns: NamespaceId,
		us: &str,
	) -> Result<Option<Arc<DefineUserStatement>>> {
		let qey = cache::tx::Lookup::Nu(ns, us);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::namespace::us::new(ns, us);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific namespace access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Option<Arc<DefineAccessStatement>>> {
		let qey = cache::tx::Lookup::Na(ns, na);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::namespace::ac::new(ns, na);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific namespace access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access_grant(
		&self,
		ns: NamespaceId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<AccessGrantStore>>> {
		let qey = cache::tx::Lookup::Ng(ns, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::namespace::access::gr::new(ns, ac, gr);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Option<Arc<DatabaseDefinition>>> {
		let qey = cache::tx::Lookup::DbById(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::namespace::db::new(ns, db);
				let Some(db_def) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(db_def);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_by_name(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Option<Arc<DatabaseDefinition>>> {
		let qey = cache::tx::Lookup::DbByName(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let catalog_key = crate::key::catalog::db::new(ns, db);
				let Some(db) = self.get(&catalog_key, None).await? else {
					return Ok(None);
				};

				let key = crate::key::namespace::db::new(db.namespace_id, db.database_id);
				let Some(db_def) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(db_def);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	pub async fn expect_db_by_name(&self, ns: &str, db: &str) -> Result<Arc<DatabaseDefinition>> {
		match self.get_db_by_name(ns, db).await? {
			Some(val) => Ok(val),
			None => {
				// Check if the namespace exists.
				// If it doesn't, return a namespace not found error.
				self.expect_ns_by_name(ns).await?;

				// Return a database not found error.
				Err(anyhow::anyhow!(Error::DbNotFound {
					name: db.to_owned()
				}))
			}
		}
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<DefineUserStatement>>> {
		let qey = cache::tx::Lookup::Du(ns, db, us);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::us::new(ns, db, us);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific database access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Option<Arc<DefineAccessStatement>>> {
		let qey = cache::tx::Lookup::Da(ns, db, da);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::ac::new(ns, db, da);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific database access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access_grant(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<AccessGrantStore>>> {
		let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::access::gr::new(ns, db, ac, gr);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific model definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_model(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &str,
		vn: &str,
	) -> Result<Option<Arc<DefineModelStatement>>> {
		let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::ml::new(ns, db, ml, vn);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &str,
	) -> Result<Arc<ApiDefinition>> {
		let qey = cache::tx::Lookup::Ap(ns, db, ap);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::ap::new(ns, db, ap);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::ApNotFound {
					value: ap.to_owned(),
				})?;
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey, val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_bucket(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Option<Arc<BucketDefinition>>> {
		let qey = cache::tx::Lookup::Bu(ns, db, bu);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::bu::new(ns, db, bu);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let bucket_def = Arc::new(val);
				let entr = cache::tx::Entry::Any(bucket_def.clone());
				self.cache.insert(qey, entr);
				Ok(Some(bucket_def))
			}
		}
	}

	pub async fn expect_db_bucket(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Arc<BucketDefinition>> {
		match self.get_db_bucket(ns, db, bu).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::BuNotFound {
				name: bu.to_owned(),
			}),
		}
	}

	/// Retrieve a specific analyzer definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_analyzer(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &str,
	) -> Result<Arc<DefineAnalyzerStatement>> {
		let qey = cache::tx::Lookup::Az(ns, db, az);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::az::new(ns, db, az);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::AzNotFound {
					name: az.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<Arc<DefineSequenceStatement>> {
		let qey = cache::tx::Lookup::Sq(ns, db, sq);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = Sq::new(ns, db, sq);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::SeqNotFound {
					name: sq.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_function(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>> {
		let qey = cache::tx::Lookup::Fc(ns, db, fc);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::fc::new(ns, db, fc);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::FcNotFound {
					name: fc.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &str,
	) -> Result<Arc<DefineParamStore>> {
		let qey = cache::tx::Lookup::Pa(ns, db, pa);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::pa::new(ns, db, pa);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::PaNotFound {
					name: pa.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Arc<ConfigStore>> {
		if let Some(val) = self.get_db_optional_config(ns, db, cg).await? {
			Ok(val)
		} else {
			Err(anyhow::Error::new(Error::CgNotFound {
				name: cg.to_owned(),
			}))
		}
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_optional_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Option<Arc<ConfigStore>>> {
		let qey = cache::tx::Lookup::Cg(ns, db, cg);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Option::Some),
			None => {
				let key = crate::key::database::cg::new(ns, db, cg);
				if let Some(val) = self.get(&key, None).await? {
					let val = Arc::new(val);
					let entr = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entr);
					Ok(Some(val))
				} else {
					Ok(None)
				}
			}
		}
	}

	/// Retrieve a specific table definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>> {
		let qey = cache::tx::Lookup::Tb(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::tb::new(ns, db, tb);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	pub async fn check_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		strict: bool,
	) -> Result<()> {
		if !strict {
			return Ok(());
		}
		self.expect_tb(ns, db, tb).await?;
		Ok(())
	}

	pub async fn expect_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<TableDefinition>> {
		match self.get_tb(ns, db, tb).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::TbNotFound {
				name: tb.to_owned(),
			}),
		}
	}

	/// Retrieve an event for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_event(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ev: &str,
	) -> Result<Arc<DefineEventStatement>> {
		let qey = cache::tx::Lookup::Ev(ns, db, tb, ev);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::table::ev::new(ns, db, tb, ev);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::EvNotFound {
					name: ev.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a field for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		fd: &str,
	) -> Result<Option<Arc<DefineFieldStatement>>> {
		let qey = cache::tx::Lookup::Fd(ns, db, tb, fd);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::table::fd::new(ns, db, tb, fd);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(Some(val))
			}
		}
	}

	/// Retrieve an index for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<Arc<DefineIndexStatement>> {
		let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::table::ix::new(ns, db, tb, ix);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::IxNotFound {
					name: ix.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Fetch a specific record value.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Arc<Record>> {
		// Cache is not versioned
		if version.is_some() {
			// Fetch the record from the datastore
			let key = crate::key::thing::new(ns, db, tb, id);
			match self.get(&key, version).await? {
				// The value exists in the datastore
				Some(mut record) => {
					// Inject the id field into the document
					let rid = RecordId {
						table: tb.to_owned(),
						key: id.clone(),
					};
					record.data.to_mut().def(&rid);
					// Convert to read-only format for better sharing and performance
					Ok(record.into_read_only())
				}
				// The value is not in the datastore
				None => Ok(Arc::new(Default::default())),
			}
		} else {
			let qey = cache::tx::Lookup::Record(ns, db, tb, id);
			match self.cache.get(&qey) {
				// The entry is in the cache
				Some(val) => val.try_into_record(),
				// The entry is not in the cache
				None => {
					// Fetch the record from the datastore
					let key = crate::key::thing::new(ns, db, tb, id);
					match self.get(&key, None).await? {
						// The value exists in the datastore
						Some(mut record) => {
							// Inject the id field into the document
							let rid = RecordId {
								table: tb.to_owned(),
								key: id.clone(),
							};
							record.data.to_mut().def(&rid);
							// Convert to read-only format for better sharing and performance
							let record = record.into_read_only();
							let entry = cache::tx::Entry::Val(record.clone());
							self.cache.insert(qey, entry);
							Ok(record)
						}
						// The value is not in the datastore
						None => Ok(Arc::new(Default::default())),
					}
				}
			}
		}
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn set_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Record,
	) -> Result<()> {
		// Set the value in the datastore
		let key = crate::key::thing::new(ns, db, tb, id);
		self.set(&key, &record, None).await?;
		// Set the value in the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(qey, cache::tx::Entry::Val(record.into_read_only()));
		// Return nothing
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn set_record_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
	) -> Result<()> {
		// Set the value in the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(qey, cache::tx::Entry::Val(record));
		// Return nothing
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn del_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<()> {
		// Delete the value in the datastore
		let key = crate::key::thing::new(ns, db, tb, id);
		self.del(&key).await?;
		// Clear the value from the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.remove(qey);
		// Return nothing
		Ok(())
	}

	/// Get or add a namespace with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_ns(&self, ns: &str, strict: bool) -> Result<Arc<NamespaceDefinition>> {
		self.get_or_add_ns_upwards(ns, strict).await
	}

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ns, db, strict, false).await
	}

	pub async fn ensure_ns_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		self.get_or_add_db_upwards(ns, db, strict, true).await
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, false).await
	}

	/// Ensures that a table, database, and namespace are all fully defined.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn ensure_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<TableDefinition>> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, true).await
	}

	/// Ensure a specific table (and database, and namespace) exist.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub(crate) async fn check_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<()> {
		if !strict {
			return Ok(());
		}

		let db = match self.get_db_by_name(ns, db).await? {
			Some(db) => db,
			None => {
				return Err(Error::DbNotFound {
					name: db.to_owned(),
				}
				.into());
			}
		};

		match self.get_tb(db.namespace_id, db.database_id, tb).await? {
			Some(tb) => tb,
			None => {
				return Err(Error::TbNotFound {
					name: tb.to_owned(),
				}
				.into());
			}
		};
		Ok(())
	}

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}

	// --------------------------------------------------
	// Private methods
	// --------------------------------------------------

	/// Get or add a namespace with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_ns_upwards(
		&self,
		ns: &str,
		strict: bool,
	) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			// The entry is not in the database
			None => {
				if strict {
					return Err(Error::NsNotFound {
						name: ns.to_owned(),
					}
					.into());
				}

				let ns = NamespaceDefinition {
					namespace_id: self.lock().await.get_next_ns_id().await?,
					name: ns.to_owned(),
					comment: None,
				};

				return self.put_ns(ns).await;
			}
		}
	}

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_db_upwards(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		let qey = cache::tx::Lookup::DbByName(ns, db);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				let db_def = self.get_db_by_name(ns, db).await?;
				if let Some(db_def) = db_def {
					return Ok(db_def);
				}

				// Database does not exist
				if !strict {
					let ns_def = if upwards {
						self.get_or_add_ns_upwards(ns, strict).await?
					} else {
						match self.get_ns_by_name(ns).await? {
							Some(ns_def) => ns_def,
							None => {
								return Err(Error::NsNotFound {
									name: ns.to_owned(),
								}
								.into());
							}
						}
					};

					let db_def = DatabaseDefinition {
						namespace_id: ns_def.namespace_id,
						database_id: self.lock().await.get_next_db_id(ns_def.namespace_id).await?,
						name: db.to_string(),
						comment: None,
						changefeed: None,
					};

					return self.put_db(&ns_def.name, db_def).await;
				}

				// Ensure the namespace exists
				if self.get_ns_by_name(ns).await?.is_none() {
					return Err(Error::NsNotFound {
						name: ns.to_owned(),
					}
					.into());
				}

				return Err(Error::DbNotFound {
					name: db.to_owned(),
				}
				.into());
			}
		}
		.try_into_type()
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_tb_upwards(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<TableDefinition>> {
		let qey = cache::tx::Lookup::TbByName(ns, db, tb);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val.try_into_type(),
			// The entry is not in the cache
			None => {
				let key = crate::key::catalog::tb::new(ns, db, tb);
				if let Some(tb_def) = self.get(&key, None).await? {
					let cached_tb = Arc::new(tb_def);
					let cached_entry =
						cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);
					self.cache.insert(qey, cached_entry);
					return Ok(cached_tb);
				}

				if strict {
					return Err(Error::TbNotFound {
						name: tb.to_owned(),
					}
					.into());
				}

				let db_def = if upwards {
					self.get_or_add_db_upwards(ns, db, strict, upwards).await?
				} else {
					if self.get_ns_by_name(ns).await?.is_none() {
						return Err(Error::NsNotFound {
							name: ns.to_owned(),
						}
						.into());
					}
					match self.get_db_by_name(ns, db).await? {
						Some(db_def) => db_def,
						None => {
							return Err(Error::DbNotFound {
								name: db.to_owned(),
							}
							.into());
						}
					}
				};

				let tb_def = TableDefinition::new(
					db_def.namespace_id,
					db_def.database_id,
					self.lock()
						.await
						.get_next_tb_id(db_def.namespace_id, db_def.database_id)
						.await?,
					tb.to_owned(),
				);
				self.put_tb(ns, db, tb_def).await
			}
		}
	}

	pub(crate) fn index_caches(&self) -> &IndexTreeCaches {
		&self.index_caches
	}
}
