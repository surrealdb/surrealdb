use super::batch::Batch;
use super::tr::Check;
use super::Convert;
use super::Key;
use super::Val;
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::idx::trees::store::cache::IndexTreeCaches;
use crate::kvs::cache;
use crate::kvs::cache::tx::Cache;
use crate::kvs::scanner::Scanner;
use crate::kvs::Transactor;
use crate::sql::statements::define::DefineConfigStatement;
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
use crate::sql::Id;
use crate::sql::Permissions;
use crate::sql::Value;
use futures::lock::Mutex;
use futures::lock::MutexGuard;
use futures::stream::Stream;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use uuid::Uuid;

#[non_exhaustive]
pub struct Transaction {
	/// The underlying transactor
	tx: Mutex<Transactor>,
	/// The query cache for this store
	cache: Cache,
	/// Tracks the index cache updates occurring during this transaction
	index_caches: IndexTreeCaches,
}

impl Transaction {
	/// Create a new query store
	pub fn new(tx: Transactor) -> Transaction {
		Transaction {
			tx: Mutex::new(tx),
			cache: cache::tx::new(),
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

	/// Check if the transaction is finished.
	///
	/// If the transaction has been canceled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	pub async fn closed(&self) -> bool {
		self.lock().await.closed().await
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn cancel(&self) -> Result<(), Error> {
		self.lock().await.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<(), Error> {
		self.lock().await.commit().await
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn exists<K>(&self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get<K>(&self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.get(key, version).await
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<Val>>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.getm(keys).await
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getp<K>(&self, key: K) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.getp(key).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.getr(rng, version).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del<K>(&self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delc<K, V>(&self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.delc(key, chk).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delr<K>(&self, rng: Range<K>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.delr(rng).await
	}

	/// Delete a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delp<K>(&self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.delp(key).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr<K>(&self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrc<K, V>(&self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.clrc(key, chk).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrr<K>(&self, rng: Range<K>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.clrr(rng).await
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrp<K>(&self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.clrp(key).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.set(key, val, version).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace<K, V>(&self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.put(key, val, version).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn putc<K, V>(&self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.putc(key, val, chk).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys, in a single request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keys<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.keys(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.scan(rng, limit, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		values: bool,
		version: Option<u64>,
	) -> Result<Batch, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.batch(rng, batch, values, version).await
	}

	/// Retrieve a batched scan to scan all versions over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_versions<K>(&self, rng: Range<K>, batch: u32) -> Result<Batch, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.batch_versions(rng, batch).await
	}

	/// Retrieve a stream over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
	) -> impl Stream<Item = Result<(Key, Val), Error>> + '_
	where
		K: Into<Key> + Debug,
	{
		Scanner::<(Key, Val)>::new(
			self,
			*NORMAL_FETCH_SIZE,
			Range {
				start: rng.start.into(),
				end: rng.end.into(),
			},
			version,
		)
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys<K>(&self, rng: Range<K>) -> impl Stream<Item = Result<Key, Error>> + '_
	where
		K: Into<Key> + Debug,
	{
		Scanner::<Key>::new(
			self,
			*NORMAL_FETCH_SIZE,
			Range {
				start: rng.start.into(),
				end: rng.end.into(),
			},
			None,
		)
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
	pub async fn all_nodes(&self) -> Result<Arc<[Node]>, Error> {
		let qey = cache::tx::Lookup::Nds;
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::root::nd::prefix();
				let end = crate::key::root::nd::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Nds(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_nds()
	}

	/// Retrieve all ROOT level users in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_users(&self) -> Result<Arc<[DefineUserStatement]>, Error> {
		let qey = cache::tx::Lookup::Rus;
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::root::us::prefix();
				let end = crate::key::root::us::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Rus(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_rus()
	}

	/// Retrieve all ROOT level accesses in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_accesses(&self) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let qey = cache::tx::Lookup::Ras;
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::root::ac::prefix();
				let end = crate::key::root::ac::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Ras(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_ras()
	}

	/// Retrieve all root access grants in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[AccessGrant]>, Error> {
		let qey = cache::tx::Lookup::Rgs(ra);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::root::access::gr::prefix(ra);
				let end = crate::key::root::access::gr::suffix(ra);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Rag(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_rag()
	}

	/// Retrieve all namespace definitions in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns(&self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		let qey = cache::tx::Lookup::Nss;
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::root::ns::prefix();
				let end = crate::key::root::ns::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Nss(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_nss()
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_users(&self, ns: &str) -> Result<Arc<[DefineUserStatement]>, Error> {
		let qey = cache::tx::Lookup::Nus(ns);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::namespace::us::prefix(ns);
				let end = crate::key::namespace::us::suffix(ns);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Nus(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_nus()
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_accesses(&self, ns: &str) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let qey = cache::tx::Lookup::Nas(ns);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::namespace::ac::prefix(ns);
				let end = crate::key::namespace::ac::suffix(ns);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Nas(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_nas()
	}

	/// Retrieve all namespace access grants for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_access_grants(
		&self,
		ns: &str,
		na: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		let qey = cache::tx::Lookup::Ngs(ns, na);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::namespace::access::gr::prefix(ns, na);
				let end = crate::key::namespace::access::gr::suffix(ns, na);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Nag(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_nag()
	}

	/// Retrieve all database definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db(&self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		let qey = cache::tx::Lookup::Dbs(ns);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::namespace::db::prefix(ns);
				let end = crate::key::namespace::db::suffix(ns);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Dbs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_dbs()
	}

	/// Retrieve all database user definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_users(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineUserStatement]>, Error> {
		let qey = cache::tx::Lookup::Dus(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::us::prefix(ns, db);
				let end = crate::key::database::us::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Dus(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_dus()
	}

	/// Retrieve all database access definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_accesses(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let qey = cache::tx::Lookup::Das(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::ac::prefix(ns, db);
				let end = crate::key::database::ac::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Das(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_das()
	}

	/// Retrieve all database access grants for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_access_grants(
		&self,
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		let qey = cache::tx::Lookup::Dgs(ns, db, da);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::access::gr::prefix(ns, db, da);
				let end = crate::key::database::access::gr::suffix(ns, db, da);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Dag(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_dag()
	}

	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_analyzers(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		let qey = cache::tx::Lookup::Azs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::az::prefix(ns, db);
				let end = crate::key::database::az::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Azs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_azs()
	}

	/// Retrieve all function definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_functions(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		let qey = cache::tx::Lookup::Fcs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::fc::prefix(ns, db);
				let end = crate::key::database::fc::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Fcs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_fcs()
	}

	/// Retrieve all param definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_params(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		let qey = cache::tx::Lookup::Pas(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::pa::prefix(ns, db);
				let end = crate::key::database::pa::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Pas(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_pas()
	}

	/// Retrieve all model definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_models(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineModelStatement]>, Error> {
		let qey = cache::tx::Lookup::Mls(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::ml::prefix(ns, db);
				let end = crate::key::database::ml::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Mls(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_mls()
	}

	/// Retrieve all model definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_configs(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineConfigStatement]>, Error> {
		let qey = cache::tx::Lookup::Cgs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::cg::prefix(ns, db);
				let end = crate::key::database::cg::suffix(ns, db);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Cgs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_cgs()
	}

	/// Retrieve all table definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb(
		&self,
		ns: &str,
		db: &str,
		version: Option<u64>,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let qey = cache::tx::Lookup::Tbs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::database::tb::prefix(ns, db);
				let end = crate::key::database::tb::suffix(ns, db);
				let val = self.getr(beg..end, version).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Tbs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_tbs()
	}

	/// Retrieve all event definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_events(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		let qey = cache::tx::Lookup::Evs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::table::ev::prefix(ns, db, tb);
				let end = crate::key::table::ev::suffix(ns, db, tb);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Evs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_evs()
	}

	/// Retrieve all field definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_fields(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		version: Option<u64>,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		let qey = cache::tx::Lookup::Fds(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::table::fd::prefix(ns, db, tb);
				let end = crate::key::table::fd::suffix(ns, db, tb);
				let val = self.getr(beg..end, version).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Fds(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_fds()
	}

	/// Retrieve all index definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_indexes(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		let qey = cache::tx::Lookup::Ixs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::table::ix::prefix(ns, db, tb);
				let end = crate::key::table::ix::suffix(ns, db, tb);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Ixs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_ixs()
	}

	/// Retrieve all view definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_views(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let qey = cache::tx::Lookup::Fts(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::table::ft::prefix(ns, db, tb);
				let end = crate::key::table::ft::suffix(ns, db, tb);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Fts(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_fts()
	}

	/// Retrieve all live definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_lives(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		let qey = cache::tx::Lookup::Lvs(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let beg = crate::key::table::lq::prefix(ns, db, tb);
				let end = crate::key::table::lq::suffix(ns, db, tb);
				let val = self.getr(beg..end, None).await?;
				let val = val.convert().into();
				let val = cache::tx::Entry::Lvs(Arc::clone(&val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_lvs()
	}

	/// Retrieve a specific node in the cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_node(&self, id: Uuid) -> Result<Arc<Node>, Error> {
		let qey = cache::tx::Lookup::Nd(id);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::nd::new(id).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::NdNotFound {
					value: id.to_string(),
				})?;
				let val: Node = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific root user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_user(&self, us: &str) -> Result<Arc<DefineUserStatement>, Error> {
		let qey = cache::tx::Lookup::Ru(us);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::us::new(us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserRootNotFound {
					value: us.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access(&self, ra: &str) -> Result<Arc<DefineAccessStatement>, Error> {
		let qey = cache::tx::Lookup::Ra(ra);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::ac::new(ra).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessRootNotFound {
					ac: ra.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific root access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access_grant(
		&self,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		let qey = cache::tx::Lookup::Rg(ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::access::gr::new(ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantRootNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
					})?;
				let val: AccessGrant = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific namespace definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns(&self, ns: &str) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let qey = cache::tx::Lookup::Ns(ns);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::ns::new(ns).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::NsNotFound {
					value: ns.to_owned(),
				})?;
				let val: DefineNamespaceStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_user(&self, ns: &str, us: &str) -> Result<Arc<DefineUserStatement>, Error> {
		let qey = cache::tx::Lookup::Nu(ns, us);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::namespace::us::new(ns, us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserNsNotFound {
					value: us.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific namespace access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access(
		&self,
		ns: &str,
		na: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let qey = cache::tx::Lookup::Na(ns, na);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::namespace::ac::new(ns, na).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessNsNotFound {
					ac: na.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific namespace access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access_grant(
		&self,
		ns: &str,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		let qey = cache::tx::Lookup::Ng(ns, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::namespace::access::gr::new(ns, ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantNsNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_owned(),
					})?;
				let val: AccessGrant = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db(&self, ns: &str, db: &str) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let qey = cache::tx::Lookup::Db(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::namespace::db::new(ns, db).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::DbNotFound {
					value: db.to_owned(),
				})?;
				let val: DefineDatabaseStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_user(
		&self,
		ns: &str,
		db: &str,
		us: &str,
	) -> Result<Arc<DefineUserStatement>, Error> {
		let qey = cache::tx::Lookup::Du(ns, db, us);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::us::new(ns, db, us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserDbNotFound {
					value: us.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific database access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access(
		&self,
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let qey = cache::tx::Lookup::Da(ns, db, da);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::ac::new(ns, db, da).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessDbNotFound {
					ac: da.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific database access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access_grant(
		&self,
		ns: &str,
		db: &str,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::access::gr::new(ns, db, ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantDbNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_owned(),
						db: db.to_owned(),
					})?;
				let val: AccessGrant = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific model definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_model(
		&self,
		ns: &str,
		db: &str,
		ml: &str,
		vn: &str,
	) -> Result<Arc<DefineModelStatement>, Error> {
		let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::ml::new(ns, db, ml, vn).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::MlNotFound {
					value: format!("{ml}<{vn}>"),
				})?;
				let val: DefineModelStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific analyzer definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_analyzer(
		&self,
		ns: &str,
		db: &str,
		az: &str,
	) -> Result<Arc<DefineAnalyzerStatement>, Error> {
		let qey = cache::tx::Lookup::Az(ns, db, az);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::az::new(ns, db, az).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AzNotFound {
					value: az.to_owned(),
				})?;
				let val: DefineAnalyzerStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_function(
		&self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>, Error> {
		let qey = cache::tx::Lookup::Fc(ns, db, fc);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::fc::new(ns, db, fc).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::FcNotFound {
					value: fc.to_owned(),
				})?;
				let val: DefineFunctionStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_param(
		&self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<Arc<DefineParamStatement>, Error> {
		let qey = cache::tx::Lookup::Pa(ns, db, pa);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::pa::new(ns, db, pa).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::PaNotFound {
					value: pa.to_owned(),
				})?;
				let val: DefineParamStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_config(
		&self,
		ns: &str,
		db: &str,
		cg: &str,
	) -> Result<Arc<DefineConfigStatement>, Error> {
		let qey = cache::tx::Lookup::Cg(ns, db, cg);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::cg::new(ns, db, cg).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::CgNotFound {
					value: cg.to_owned(),
				})?;
				let val: DefineConfigStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a specific table definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let qey = cache::tx::Lookup::Tb(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::tb::new(ns, db, tb).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::TbNotFound {
					value: tb.to_owned(),
				})?;
				let val: DefineTableStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve an event for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_event(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		ev: &str,
	) -> Result<Arc<DefineEventStatement>, Error> {
		let qey = cache::tx::Lookup::Ev(ns, db, tb, ev);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::table::ev::new(ns, db, tb, ev).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::EvNotFound {
					value: ev.to_owned(),
				})?;
				let val: DefineEventStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve a field for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_field(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		fd: &str,
	) -> Result<Arc<DefineFieldStatement>, Error> {
		let qey = cache::tx::Lookup::Fd(ns, db, tb, fd);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::table::fd::new(ns, db, tb, fd).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::FdNotFound {
					value: fd.to_owned(),
				})?;
				let val: DefineFieldStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Retrieve an index for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb_index(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		ix: &str,
	) -> Result<Arc<DefineIndexStatement>, Error> {
		let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::table::ix::new(ns, db, tb, ix).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::IxNotFound {
					value: ix.to_owned(),
				})?;
				let val: DefineIndexStatement = val.into();
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey.into(), val.clone());
				val
			}
		}
		.try_into_type()
	}

	/// Fetch a specific record value.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_record(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		version: Option<u64>,
	) -> Result<Arc<Value>, Error> {
		// Cache is not versioned
		if version.is_some() {
			// Fetch the record from the datastore
			let key = crate::key::thing::new(ns, db, tb, id).encode()?;
			match self.get(key, version).await? {
				// The value exists in the datastore
				Some(val) => {
					let val = cache::tx::Entry::Val(Arc::new(val.into()));
					val.try_into_val()
				}
				// The value is not in the datastore
				None => Ok(Arc::new(Value::None)),
			}
		} else {
			let qey = cache::tx::Lookup::Record(ns, db, tb, id);
			match self.cache.get(&qey) {
				// The entry is in the cache
				Some(val) => val.try_into_val(),
				// The entry is not in the cache
				None => {
					// Fetch the record from the datastore
					let key = crate::key::thing::new(ns, db, tb, id).encode()?;
					match self.get(key, None).await? {
						// The value exists in the datastore
						Some(val) => {
							let val = cache::tx::Entry::Val(Arc::new(val.into()));
							self.cache.insert(qey.into(), val.clone());
							val.try_into_val()
						}
						// The value is not in the datastore
						None => Ok(Arc::new(Value::None)),
					}
				}
			}
		}
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn set_record(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		val: Value,
	) -> Result<(), Error> {
		// Set the value in the datastore
		let key = crate::key::thing::new(ns, db, tb, id);
		self.set(&key, &val, None).await?;
		// Set the value in the cache
		let key = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(key.into(), cache::tx::Entry::Val(Arc::new(val)));
		// Return nothing
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn set_record_cache(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		val: Arc<Value>,
	) -> Result<(), Error> {
		// Set the value in the cache
		let key = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(key.into(), cache::tx::Entry::Val(val));
		// Return nothing
		Ok(())
	}

	/// Get or add a namespace with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_ns(
		&self,
		ns: &str,
		strict: bool,
	) -> Result<Arc<DefineNamespaceStatement>, Error> {
		self.get_or_add_ns_upwards(ns, strict, false).await
	}

	/// Get or add a database with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		self.get_or_add_db_upwards(ns, db, strict, false).await
	}

	/// Get or add a table with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_or_add_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, false).await
	}

	/// Ensures that a table, database, and namespace are all fully defined.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	#[inline(always)]
	pub async fn ensure_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		self.get_or_add_tb_upwards(ns, db, tb, strict, true).await
	}

	/// Ensure a specific table (and database, and namespace) exist.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	#[inline(always)]
	pub(crate) async fn check_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<(), Error> {
		match strict {
			// Strict mode is disabled
			false => Ok(()),
			// Strict mode is enabled
			true => {
				// Check that the table exists
				match self.get_tb(ns, db, tb).await {
					Err(Error::TbNotFound {
						value: tb,
					}) => {
						// If not, check the database exists
						match self.get_db(ns, db).await {
							Err(Error::DbNotFound {
								value: db,
							}) => {
								// If not, check the namespace exists
								match self.get_ns(ns).await {
									Err(Error::NsNotFound {
										value: ns,
									}) => Err(Error::NsNotFound {
										value: ns,
									}),
									// Return any other errors
									Err(err) => Err(err),
									// Namespace does exist
									Ok(_) => Err(Error::DbNotFound {
										value: db,
									}),
								}
							}
							// Return any other errors
							Err(err) => Err(err),
							// Database does exist
							Ok(_) => Err(Error::TbNotFound {
								value: tb,
							}),
						}
					}
					// Return any other errors
					Err(err) => Err(err),
					// Table does exist
					Ok(_) => Ok(()),
				}
			}
		}
	}

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	#[inline(always)]
	pub fn clear(&self) {
		self.cache.clear()
	}

	// --------------------------------------------------
	// Private methods
	// --------------------------------------------------

	/// Get or add a namespace with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_ns_upwards(
		&self,
		ns: &str,
		strict: bool,
		_upwards: bool,
	) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let qey = cache::tx::Lookup::Ns(ns);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				// Try to fetch the value from the datastore
				let key = crate::key::root::ns::new(ns);
				let res = self.get(&key, None).await?.ok_or_else(|| Error::NsNotFound {
					value: ns.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::NsNotFound {
						..
					}) if !strict => {
						let val = DefineNamespaceStatement {
							name: ns.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineNamespaceStatement = val.into();
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.try_into_type()
	}

	/// Get or add a database with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_db_upwards(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let qey = cache::tx::Lookup::Db(ns, db);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				// Try to fetch the value from the datastore
				let key = crate::key::namespace::db::new(ns, db);
				let res = self.get(&key, None).await?.ok_or_else(|| Error::DbNotFound {
					value: db.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::DbNotFound {
						..
					}) if !strict => {
						// First ensure that a namespace exists
						if upwards {
							self.get_or_add_ns_upwards(ns, strict, upwards).await?;
						}
						// Next, dynamically define the database
						let val = DefineDatabaseStatement {
							name: db.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Check to see that the hierarchy exists
					Err(Error::DbNotFound {
						value,
					}) if strict => {
						self.get_ns(ns).await?;
						Err(Error::DbNotFound {
							value,
						})?
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineDatabaseStatement = val.into();
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.try_into_type()
	}

	/// Get or add a table with a default configuration, only if we are in dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_or_add_tb_upwards(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
		upwards: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let qey = cache::tx::Lookup::Tb(ns, db, tb);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				// Try to fetch the value from the datastore
				let key = crate::key::database::tb::new(ns, db, tb);
				let res = self.get(&key, None).await?.ok_or_else(|| Error::TbNotFound {
					value: tb.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::TbNotFound {
						..
					}) if !strict => {
						// First ensure that a database exists
						if upwards {
							self.get_or_add_db_upwards(ns, db, strict, upwards).await?;
						}
						// Next, dynamically define the table
						let val = DefineTableStatement {
							name: tb.to_owned().into(),
							permissions: Permissions::none(),
							..Default::default()
						};
						let val = {
							self.put(&key, &val, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Check to see that the hierarchy exists
					Err(Error::TbNotFound {
						value,
					}) if strict => {
						self.get_ns(ns).await?;
						self.get_db(ns, db).await?;
						Err(Error::TbNotFound {
							value,
						})?
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineTableStatement = val.into();
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey.into(), val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.try_into_type()
	}

	pub(crate) fn index_caches(&self) -> &IndexTreeCaches {
		&self.index_caches
	}
}
