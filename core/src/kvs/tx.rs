use super::batch::Batch;
use super::tr::Check;
use super::Convert;
use super::Key;
use super::Val;
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::cnf::TRANSACTION_CACHE_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::kvs::cache::Entry;
use crate::kvs::cache::EntryWeighter;
use crate::kvs::scanner::Scanner;
use crate::kvs::Transactor;
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
use quick_cache::sync::Cache;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use uuid::Uuid;

#[non_exhaustive]
pub struct Transaction {
	/// The underlying transactor
	tx: Mutex<Transactor>,
	/// The query cache for this store
	cache: Cache<Key, Entry, EntryWeighter>,
}

impl Transaction {
	/// Create a new query store
	pub fn new(tx: Transactor) -> Transaction {
		Transaction {
			tx: Mutex::new(tx),
			cache: Cache::with_weighter(
				*TRANSACTION_CACHE_SIZE,
				*TRANSACTION_CACHE_SIZE as u64,
				EntryWeighter,
			),
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

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
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
	pub async fn exists<K>(&self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.exists(key).await
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
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Val>, Error>
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
	pub async fn getr<K>(&self, rng: Range<K>) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.getr(rng).await
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

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K, V>(&self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.set(key, val).await
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
	pub async fn keys<K>(&self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.keys(rng, limit).await
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
	pub async fn batch<K>(&self, rng: Range<K>, batch: u32, values: bool) -> Result<Batch, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.batch(rng, batch, values).await
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
		Scanner::new(
			self,
			*NORMAL_FETCH_SIZE,
			Range {
				start: rng.start.into(),
				end: rng.end.into(),
			},
			version,
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

	/// Panic if this transaction is dropped without proper handling.
	pub async fn rollback_with_panic(self) -> Self {
		self.tx.lock().await.check_level(Check::Panic);
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
		let key = crate::key::root::nd::prefix();
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::root::nd::suffix();
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Nds(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_nds())
	}

	/// Retrieve all ROOT level users in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_users(&self) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::root::us::prefix();
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::root::us::suffix();
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Rus(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_rus())
	}

	/// Retrieve all ROOT level accesses in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_accesses(&self) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::root::access::ac::prefix();
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::root::access::ac::suffix();
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Ras(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_ras())
	}

	/// Retrieve all root access grants in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[AccessGrant]>, Error> {
		let key = crate::key::root::access::gr::prefix(ra);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::root::access::gr::suffix(ra);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Rag(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_rag())
	}

	/// Retrieve all namespace definitions in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns(&self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
		let key = crate::key::root::ns::prefix();
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::root::ns::suffix();
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Nss(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_nss())
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_users(&self, ns: &str) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::namespace::us::prefix(ns);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::namespace::us::suffix(ns);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Nus(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_nus())
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_accesses(&self, ns: &str) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::namespace::access::ac::prefix(ns);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::namespace::access::ac::suffix(ns);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Nas(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_nas())
	}

	/// Retrieve all namespace access grants for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_ns_access_grants(
		&self,
		ns: &str,
		na: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		let key = crate::key::namespace::access::gr::prefix(ns, na);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::namespace::access::gr::suffix(ns, na);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Nag(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_nag())
	}

	/// Retrieve all database definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db(&self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		let key = crate::key::namespace::db::prefix(ns);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::namespace::db::suffix(ns);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Dbs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_dbs())
	}

	/// Retrieve all database user definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_users(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineUserStatement]>, Error> {
		let key = crate::key::database::us::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::us::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Dus(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_dus())
	}

	/// Retrieve all database access definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_accesses(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		let key = crate::key::database::access::ac::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::access::ac::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Das(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_das())
	}

	/// Retrieve all database access grants for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_access_grants(
		&self,
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		let key = crate::key::database::access::gr::prefix(ns, db, da);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::access::gr::suffix(ns, db, da);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Dag(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_dag())
	}

	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_analyzers(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		let key = crate::key::database::az::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::az::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Azs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_azs())
	}

	/// Retrieve all function definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_functions(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		let key = crate::key::database::fc::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::fc::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Fcs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_fcs())
	}

	/// Retrieve all param definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_params(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		let key = crate::key::database::pa::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::pa::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Pas(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_pas())
	}

	/// Retrieve all model definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_models(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineModelStatement]>, Error> {
		let key = crate::key::database::ml::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::ml::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Mls(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_mls())
	}

	/// Retrieve all table definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb(&self, ns: &str, db: &str) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::database::tb::prefix(ns, db);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::database::tb::suffix(ns, db);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Tbs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_tbs())
	}

	/// Retrieve all event definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_events(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		let key = crate::key::table::ev::prefix(ns, db, tb);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::table::ev::suffix(ns, db, tb);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Evs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_evs())
	}

	/// Retrieve all field definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_fields(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		let key = crate::key::table::fd::prefix(ns, db, tb);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::table::fd::suffix(ns, db, tb);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Fds(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_fds())
	}

	/// Retrieve all index definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_indexes(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		let key = crate::key::table::ix::prefix(ns, db, tb);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::table::ix::suffix(ns, db, tb);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Ixs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_ixs())
	}

	/// Retrieve all view definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_views(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		let key = crate::key::table::ft::prefix(ns, db, tb);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::table::ft::suffix(ns, db, tb);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Fts(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_fts())
	}

	/// Retrieve all live definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_tb_lives(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		let key = crate::key::table::lq::prefix(ns, db, tb);
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let end = crate::key::table::lq::suffix(ns, db, tb);
				let val = self.getr(key..end).await?;
				let val = val.convert().into();
				let val = Entry::Lvs(Arc::clone(&val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_lvs())
	}

	/// Retrieve a specific namespace definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_node(&self, id: Uuid) -> Result<Arc<Node>, Error> {
		let key = crate::key::root::nd::new(id).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::NdNotFound {
					value: id.to_string(),
				})?;
				let val: Node = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific root user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_user(&self, us: &str) -> Result<Arc<DefineUserStatement>, Error> {
		let key = crate::key::root::us::new(us).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::UserRootNotFound {
					value: us.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access(&self, ra: &str) -> Result<Arc<DefineAccessStatement>, Error> {
		let key = crate::key::root::access::ac::new(ra).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessRootNotFound {
					ac: ra.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific root access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access_grant(
		&self,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		let key = crate::key::root::access::gr::new(ac, gr).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessGrantRootNotFound {
					ac: ac.to_owned(),
					gr: gr.to_owned(),
				})?;
				let val: AccessGrant = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns(&self, ns: &str) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let key = crate::key::root::ns::new(ns).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::NsNotFound {
					value: ns.to_owned(),
				})?;
				let val: DefineNamespaceStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_user(&self, ns: &str, us: &str) -> Result<Arc<DefineUserStatement>, Error> {
		let key = crate::key::namespace::us::new(ns, us).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::UserNsNotFound {
					value: us.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access(
		&self,
		ns: &str,
		na: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let key = crate::key::namespace::access::ac::new(ns, na).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessNsNotFound {
					ac: na.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific namespace access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access_grant(
		&self,
		ns: &str,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		let key = crate::key::namespace::access::gr::new(ns, ac, gr).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessGrantNsNotFound {
					ac: ac.to_owned(),
					gr: gr.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: AccessGrant = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db(&self, ns: &str, db: &str) -> Result<Arc<DefineDatabaseStatement>, Error> {
		let key = crate::key::namespace::db::new(ns, db).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::DbNotFound {
					value: db.to_owned(),
				})?;
				let val: DefineDatabaseStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_user(
		&self,
		ns: &str,
		db: &str,
		us: &str,
	) -> Result<Arc<DefineUserStatement>, Error> {
		let key = crate::key::database::us::new(ns, db, us).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::UserDbNotFound {
					value: us.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineUserStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific database access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access(
		&self,
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		let key = crate::key::database::access::ac::new(ns, db, da).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessDbNotFound {
					ac: da.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineAccessStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
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
		let key = crate::key::database::access::gr::new(ns, db, ac, gr).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AccessGrantDbNotFound {
					ac: ac.to_owned(),
					gr: gr.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: AccessGrant = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
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
		let key = crate::key::database::ml::new(ns, db, ml, vn).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::MlNotFound {
					value: format!("{ml}<{vn}>"),
				})?;
				let val: DefineModelStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific analyzer definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_analyzer(
		&self,
		ns: &str,
		db: &str,
		az: &str,
	) -> Result<Arc<DefineAnalyzerStatement>, Error> {
		let key = crate::key::database::az::new(ns, db, az).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::AzNotFound {
					value: az.to_owned(),
				})?;
				let val: DefineAnalyzerStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_function(
		&self,
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>, Error> {
		let key = crate::key::database::fc::new(ns, db, fc).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::FcNotFound {
					value: fc.to_owned(),
				})?;
				let val: DefineFunctionStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_param(
		&self,
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<Arc<DefineParamStatement>, Error> {
		let key = crate::key::database::pa::new(ns, db, pa).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::PaNotFound {
					value: pa.to_owned(),
				})?;
				let val: DefineParamStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Retrieve a specific table definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<DefineTableStatement>, Error> {
		let key = crate::key::database::tb::new(ns, db, tb).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::TbNotFound {
					value: tb.to_owned(),
				})?;
				let val: DefineTableStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
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
		let key = crate::key::table::ev::new(ns, db, tb, ev).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::EvNotFound {
					value: ev.to_owned(),
				})?;
				let val: DefineEventStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
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
		let key = crate::key::table::fd::new(ns, db, tb, fd).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::FdNotFound {
					value: fd.to_owned(),
				})?;
				let val: DefineFieldStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
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
		let key = crate::key::table::ix::new(ns, db, tb, ix).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		Ok(match res {
			Ok(val) => val,
			Err(cache) => {
				let val = self.get(key, None).await?.ok_or(Error::IxNotFound {
					value: ix.to_owned(),
				})?;
				let val: DefineIndexStatement = val.into();
				let val = Entry::Any(Arc::new(val));
				let _ = cache.insert(val.clone());
				val
			}
		}
		.into_type())
	}

	/// Fetch a specific record value.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_record(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
	) -> Result<Arc<Value>, Error> {
		let key = crate::key::thing::new(ns, db, tb, id).encode()?;
		let res = self.cache.get_value_or_guard_async(&key).await;
		match res {
			// The entry is in the cache
			Ok(val) => Ok(val.into_val()),
			// The entry is not in the cache
			Err(cache) => match self.get(key, None).await? {
				// The value exists in the datastore
				Some(val) => {
					let val = Entry::Val(Arc::new(val.into()));
					let _ = cache.insert(val.clone());
					Ok(val.into_val())
				}
				// The value is not in the datastore
				None => Ok(Arc::new(Value::None)),
			},
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
		let key = crate::key::thing::new(ns, db, tb, id);
		let enc = crate::key::thing::new(ns, db, tb, id).encode()?;
		// Set the value in the datastore
		self.set(&key, &val).await?;
		// Set the value in the cache
		self.cache.insert(enc, Entry::Val(Arc::new(val)));
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
		let key = crate::key::root::ns::new(ns);
		let enc = crate::key::root::ns::new(ns).encode()?;
		let res = self.cache.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(cache) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key, None).await?.ok_or(Error::NsNotFound {
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
							Entry::Any(Arc::new(val))
						};
						let _ = cache.insert(val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineNamespaceStatement = val.into();
						let val = Entry::Any(Arc::new(val));
						let _ = cache.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
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
		let key = crate::key::namespace::db::new(ns, db);
		let enc = crate::key::namespace::db::new(ns, db).encode()?;
		let res = self.cache.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(cache) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key, None).await?.ok_or(Error::DbNotFound {
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
							Entry::Any(Arc::new(val))
						};
						let _ = cache.insert(val.clone());
						val
					}
					// Check to see that the hierarchy exists
					Err(Error::TbNotFound {
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
						let val = Entry::Any(Arc::new(val));
						let _ = cache.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
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
		let key = crate::key::database::tb::new(ns, db, tb);
		let enc = crate::key::database::tb::new(ns, db, tb).encode()?;
		let res = self.cache.get_value_or_guard_async(&enc).await;
		Ok(match res {
			// The entry is in the cache
			Ok(val) => val,
			// The entry is not in the cache
			Err(cache) => {
				// Try to fetch the value from the datastore
				let res = self.get(&key, None).await?.ok_or(Error::TbNotFound {
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
							Entry::Any(Arc::new(val))
						};
						let _ = cache.insert(val.clone());
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
						let val = Entry::Any(Arc::new(val));
						let _ = cache.insert(val.clone());
						val
					}
					// Throw any received errors
					Err(err) => Err(err)?,
				}
			}
		}
		.into_type())
	}
}
