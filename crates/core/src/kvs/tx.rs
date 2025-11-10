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
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, CatalogProvider, DatabaseProvider,
	NamespaceProvider, NodeProvider, TableProvider, UserProvider,
};
use crate::catalog::{
	self, ApiDefinition, ConfigDefinition, DatabaseDefinition, DatabaseId, IndexId,
	NamespaceDefinition, NamespaceId, Record, TableDefinition, TableId,
};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::ctx::MutableContext;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::idx::planner::ScanDirection;
use crate::key::database::sq::Sq;
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::key::KVKey;
use crate::kvs::scanner::Scanner;
use crate::kvs::sequences::Sequences;
use crate::kvs::{Transactor, cache};
use crate::val::{RecordId, RecordIdKey};

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The underlying transactor
	tx: Mutex<Transactor>,
	/// The query cache for this store
	cache: TransactionCache,
	/// Does this support reverse scan?
	has_reverse_scan: bool,
	/// The sequences for this store
	sequences: Sequences,
}

impl Transaction {
	/// Create a new query store
	pub fn new(local: bool, tx: Transactor, sequences: Sequences) -> Transaction {
		Transaction {
			local,
			has_reverse_scan: tx.inner.supports_reverse_scan(),
			tx: Mutex::new(tx),
			cache: TransactionCache::new(),
			sequences,
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
	pub fn has_reverse_scan(&self) -> bool {
		self.has_reverse_scan
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

	#[inline]
	fn set_record_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
	) {
		// Set the value in the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(qey, cache::tx::Entry::Val(record));
	}

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl NodeProvider for Transaction {
	/// Retrieve all nodes belonging to this cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_nodes(&self) -> Result<Arc<[Node]>> {
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

	/// Retrieve a specific node in the cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_node(&self, id: Uuid) -> Result<Arc<Node>> {
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
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl NamespaceProvider for Transaction {
	/// Retrieve all namespace definitions in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns(&self) -> Result<Arc<[NamespaceDefinition]>> {
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

	async fn get_ns_by_name(&self, ns: &str) -> Result<Option<Arc<NamespaceDefinition>>> {
		let qey = cache::tx::Lookup::NsByName(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::root::ns::new(ns);
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

	async fn expect_ns_by_name(&self, ns: &str) -> Result<Arc<NamespaceDefinition>> {
		match self.get_ns_by_name(ns).await? {
			Some(val) => Ok(val),
			None => anyhow::bail!(Error::NsNotFound {
				name: ns.to_owned(),
			}),
		}
	}

	async fn put_ns(&self, ns: NamespaceDefinition) -> Result<Arc<NamespaceDefinition>> {
		let key = crate::key::root::ns::new(&ns.name);
		self.set(&key, &ns, None).await?;

		// Populate cache
		let cached_ns = Arc::new(ns.clone());

		let entry = cache::tx::Entry::Any(Arc::clone(&cached_ns) as Arc<dyn Any + Send + Sync>);
		let qey = cache::tx::Lookup::NsByName(&ns.name);
		self.cache.insert(qey, entry);

		Ok(cached_ns)
	}

	async fn get_next_ns_id(&self, ctx: Option<&MutableContext>) -> Result<NamespaceId> {
		self.sequences.next_namespace_id(ctx).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl DatabaseProvider for Transaction {
	/// Retrieve all database definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db(&self, ns: NamespaceId) -> Result<Arc<[DatabaseDefinition]>> {
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

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_by_name(&self, ns: &str, db: &str) -> Result<Option<Arc<DatabaseDefinition>>> {
		let qey = cache::tx::Lookup::DbByName(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let Some(ns) = self.get_ns_by_name(ns).await? else {
					return Ok(None);
				};

				let key = crate::key::namespace::db::new(ns.namespace_id, db);
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

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self, ctx))]
	async fn get_or_add_db_upwards(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		let qey = cache::tx::Lookup::DbByName(ns, db);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => {
				let t = val.try_into_type()?;
				Ok(t)
			}
			// The entry is not in the cache
			None => {
				let db_def = self.get_db_by_name(ns, db).await?;
				if let Some(db_def) = db_def {
					return Ok(db_def);
				}

				let ns_def = if upwards {
					self.get_or_add_ns(ctx, ns).await?
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
					database_id: self.get_next_db_id(ctx, ns_def.namespace_id).await?,
					name: db.to_string(),
					comment: None,
					changefeed: None,
					strict: false,
				};

				return self.put_db(&ns_def.name, db_def).await;
			}
		}
	}

	async fn get_next_db_id(
		&self,
		ctx: Option<&MutableContext>,
		ns: NamespaceId,
	) -> Result<DatabaseId> {
		self.sequences.next_database_id(ctx, ns).await
	}

	async fn put_db(&self, ns: &str, db: DatabaseDefinition) -> Result<Arc<DatabaseDefinition>> {
		let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
		self.set(&key, &db, None).await?;

		// Populate cache
		let cached_db = Arc::new(db.clone());

		let entry = cache::tx::Entry::Any(Arc::clone(&cached_db) as Arc<dyn Any + Send + Sync>);
		let qey = cache::tx::Lookup::DbByName(ns, &db.name);
		self.cache.insert(qey, entry);

		Ok(cached_db)
	}

	async fn del_db(&self, ns: &str, db: &str, expunge: bool) -> Result<Option<()>> {
		let Some(db) = self.get_db_by_name(ns, db).await? else {
			return Ok(None);
		};
		let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
		let database_root = crate::key::database::all::new(db.namespace_id, db.database_id);
		if expunge {
			self.clr(&key).await?;
			self.clrp(&database_root).await?;
		} else {
			self.del(&key).await?;
			self.delp(&database_root).await?
		};

		Ok(Some(()))
	}

	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::AnalyzerDefinition]>> {
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

	/// Retrieve all sequences definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::SequenceDefinition]>> {
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
	async fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::FunctionDefinition]>> {
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

	/// Retrieve all module definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_modules(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::ModuleDefinition]>> {
		let qey = cache::tx::Lookup::Mds(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_mds(),
			None => {
				let beg = crate::key::database::md::prefix(ns, db)?;
				let end = crate::key::database::md::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				let entry = cache::tx::Entry::Mds(val.clone());
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// Retrieve all param definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::ParamDefinition]>> {
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
	async fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::MlModelDefinition]>> {
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
	async fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[ConfigDefinition]>> {
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

	/// Retrieve a specific model definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_model(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &str,
		vn: &str,
	) -> Result<Option<Arc<catalog::MlModelDefinition>>> {
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

	/// Retrieve a specific analyzer definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_analyzer(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &str,
	) -> Result<Arc<catalog::AnalyzerDefinition>> {
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
	async fn get_db_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<Arc<catalog::SequenceDefinition>> {
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
	async fn get_db_function(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &str,
	) -> Result<Arc<catalog::FunctionDefinition>> {
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

	async fn put_db_function(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &catalog::FunctionDefinition,
	) -> Result<()> {
		let key = crate::key::database::fc::new(ns, db, &fc.name);
		self.set(&key, fc, None).await?;
		Ok(())
	}

	/// Retrieve a specific module definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_module(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &str,
	) -> Result<Arc<catalog::ModuleDefinition>> {
		let qey = cache::tx::Lookup::Md(ns, db, md);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::md::new(ns, db, md);
				let val = self.get(&key, None).await?.ok_or_else(|| Error::MdNotFound {
					name: md.to_owned(),
				})?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	async fn put_db_module(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &catalog::ModuleDefinition,
	) -> Result<()> {
		let name = md.get_storage_name()?;
		let key = crate::key::database::md::new(ns, db, &name);
		self.set(&key, md, None).await?;
		Ok(())
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &str,
	) -> Result<Arc<catalog::ParamDefinition>> {
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

	async fn put_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &catalog::ParamDefinition,
	) -> Result<()> {
		let key = crate::key::database::pa::new(ns, db, &pa.name);
		self.set(&key, pa, None).await?;
		Ok(())
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Option<Arc<ConfigDefinition>>> {
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
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl TableProvider for Transaction {
	/// Retrieve all table definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_tb(
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

	/// Retrieve all view definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_tb_views(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::TableDefinition]>> {
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

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self, ctx))]
	async fn get_or_add_tb_upwards(
		&self,
		ctx: Option<&MutableContext>,
		ns: &str,
		db: &str,
		tb: &str,
		upwards: bool,
	) -> Result<Arc<TableDefinition>> {
		let qey = cache::tx::Lookup::TbByName(ns, db, tb);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val.try_into_type(),
			// The entry is not in the cache
			None => {
				let db_def = if upwards {
					self.get_or_add_db_upwards(ctx, ns, db, upwards).await?
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

				let table_key =
					crate::key::database::tb::new(db_def.namespace_id, db_def.database_id, tb);
				if let Some(tb_def) = self.get(&table_key, None).await? {
					let cached_tb = Arc::new(tb_def);
					let cached_entry =
						cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);
					self.cache.insert(qey, cached_entry);
					return Ok(cached_tb);
				}

				if db_def.strict {
					return Err(Error::TbNotFound {
						name: tb.to_owned(),
					}
					.into());
				}

				let tb_def = TableDefinition::new(
					db_def.namespace_id,
					db_def.database_id,
					self.get_next_tb_id(ctx, db_def.namespace_id, db_def.database_id).await?,
					tb.to_owned(),
				);
				self.put_tb(ns, db, &tb_def).await
			}
		}
	}

	async fn get_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Option<Arc<TableDefinition>>> {
		let qey = cache::tx::Lookup::TbByName(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let Some(db) = self.get_db_by_name(ns, db).await? else {
					return Ok(None);
				};

				let key = crate::key::database::tb::new(db.namespace_id, db.database_id, tb);
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

	async fn put_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &TableDefinition,
	) -> Result<Arc<TableDefinition>> {
		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.set(&key, tb, None).await?;

		// Populate cache
		let cached_tb = Arc::new(tb.clone());
		let cached_entry =
			cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);

		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.insert(qey, cached_entry.clone());

		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.insert(qey, cached_entry.clone());

		Ok(cached_tb)
	}

	async fn del_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.to_string(),
			}
			.into());
		};

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.del(&key).await?;

		// Clear the cache
		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.remove(qey);
		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.remove(qey);

		Ok(())
	}

	async fn clr_tb(&self, ns: &str, db: &str, tb: &str) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.to_string(),
			}
			.into());
		};

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.clr(&key).await?;

		// Clear the cache
		let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, &tb.name);
		self.cache.remove(qey);
		let qey = cache::tx::Lookup::TbByName(ns, db, &tb.name);
		self.cache.remove(qey);

		Ok(())
	}

	/// Retrieve all event definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_tb_events(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::EventDefinition]>> {
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
	async fn all_tb_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		version: Option<u64>,
	) -> Result<Arc<[catalog::FieldDefinition]>> {
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
	async fn all_tb_indexes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::IndexDefinition]>> {
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

	/// Retrieve all live definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_tb_lives(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
	) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
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

	/// Retrieve a specific table definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_tb(
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

	/// Retrieve an event for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_tb_event(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ev: &str,
	) -> Result<Arc<catalog::EventDefinition>> {
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
	async fn get_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		fd: &str,
	) -> Result<Option<Arc<catalog::FieldDefinition>>> {
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

	async fn put_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		fd: &catalog::FieldDefinition,
	) -> Result<()> {
		let name = fd.name.to_raw_string();
		let key = crate::key::table::fd::new(ns, db, tb, &name);
		self.set(&key, fd, None).await?;
		Ok(())
	}

	/// Retrieve an index for a table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<Option<Arc<catalog::IndexDefinition>>> {
		let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::table::ix::new(ns, db, tb, ix);
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

	async fn get_tb_index_by_id(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: IndexId,
	) -> Result<Option<Arc<catalog::IndexDefinition>>> {
		let key = crate::key::table::ix::IndexNameLookupKey::new(ns, db, tb, ix);
		let Some(index_name) = self.get(&key, None).await? else {
			return Ok(None);
		};

		self.get_tb_index(ns, db, tb, &index_name).await
	}

	async fn put_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &catalog::IndexDefinition,
	) -> Result<()> {
		let key = crate::key::table::ix::new(ns, db, tb, &ix.name);
		self.set(&key, ix, None).await?;

		let name_lookup_key =
			crate::key::table::ix::IndexNameLookupKey::new(ns, db, tb, ix.index_id);
		self.set(&name_lookup_key, &ix.name, None).await?;

		// Set the entry in the cache
		let qey = cache::tx::Lookup::Ix(ns, db, tb, &ix.name);
		let entry = cache::tx::Entry::Any(Arc::new(ix.clone()));
		self.cache.insert(qey, entry);
		Ok(())
	}

	async fn del_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		ix: &str,
	) -> Result<()> {
		// Get the index definition
		let Some(ix) = self.get_tb_index(ns, db, tb, ix).await? else {
			return Ok(());
		};

		// Remove the index data
		let key = crate::key::index::all::new(ns, db, tb, ix.index_id);
		self.delp(&key).await?;

		// Delete the definition
		let key = crate::key::table::ix::new(ns, db, tb, &ix.name);
		self.del(&key).await?;

		Ok(())
	}

	/// Fetch a specific record value.
	///
	/// This function will return a new default initialized record if non exists.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_record(
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
			let key = crate::key::record::new(ns, db, tb, id);
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
					let key = crate::key::record::new(ns, db, tb, id);
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

	async fn record_exists(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<bool> {
		let key = crate::key::record::new(ns, db, tb, id);
		Ok(self.exists(&key, None).await?)
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn put_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
		version: Option<u64>,
	) -> Result<()> {
		let key = crate::key::record::new(ns, db, tb, id);
		self.put(&key, &record, version).await?;
		self.set_record_cache(ns, db, tb, id, record);
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn set_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		record: Arc<Record>,
		version: Option<u64>,
	) -> Result<()> {
		// Set the value in the datastore
		let key = crate::key::record::new(ns, db, tb, id);
		self.set(&key, &record, version).await?;
		// Set the value in the cache
		self.set_record_cache(ns, db, tb, id, record);
		// Return nothing
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn del_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
	) -> Result<()> {
		// Delete the value in the datastore
		let key = crate::key::record::new(ns, db, tb, id);
		self.del(&key).await?;
		// Clear the value from the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.remove(qey);
		// Return nothing
		Ok(())
	}

	async fn get_next_tb_id(
		&self,
		ctx: Option<&MutableContext>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<TableId> {
		self.sequences.next_table_id(ctx, ns, db).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl UserProvider for Transaction {
	/// Retrieve all ROOT level users in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_root_users(&self) -> Result<Arc<[catalog::UserDefinition]>> {
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

	/// Retrieve all namespace user definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns_users(&self, ns: NamespaceId) -> Result<Arc<[catalog::UserDefinition]>> {
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

	/// Retrieve all database user definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_users(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::UserDefinition]>> {
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

	/// Retrieve a specific root user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_user(&self, us: &str) -> Result<Option<Arc<catalog::UserDefinition>>> {
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

	async fn put_root_user(&self, us: &catalog::UserDefinition) -> Result<()> {
		let key = crate::key::root::us::new(&us.name);
		self.set(&key, us, None).await?;
		Ok(())
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_ns_user(
		&self,
		ns: NamespaceId,
		us: &str,
	) -> Result<Option<Arc<catalog::UserDefinition>>> {
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

	async fn put_ns_user(&self, ns: NamespaceId, us: &catalog::UserDefinition) -> Result<()> {
		let key = crate::key::namespace::us::new(ns, &us.name);
		self.set(&key, us, None).await?;
		Ok(())
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<catalog::UserDefinition>>> {
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

	async fn put_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &catalog::UserDefinition,
	) -> Result<()> {
		let key = crate::key::database::us::new(ns, db, &us.name);
		self.set(&key, us, None).await?;
		Ok(())
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl AuthorisationProvider for Transaction {
	/// Retrieve all ROOT level accesses in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_root_accesses(&self) -> Result<Arc<[catalog::AccessDefinition]>> {
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
	async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[catalog::AccessGrant]>> {
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

	/// Retrieve all namespace access definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns_accesses(&self, ns: NamespaceId) -> Result<Arc<[catalog::AccessDefinition]>> {
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
	async fn all_ns_access_grants(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Arc<[catalog::AccessGrant]>> {
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

	/// Retrieve all database access definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::AccessDefinition]>> {
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
	async fn all_db_access_grants(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Arc<[catalog::AccessGrant]>> {
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

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_access(&self, ra: &str) -> Result<Option<Arc<catalog::AccessDefinition>>> {
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

	/// Retrieve a specific root access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_access_grant(
		&self,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>> {
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

	/// Retrieve a specific namespace access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_ns_access(
		&self,
		ns: NamespaceId,
		na: &str,
	) -> Result<Option<Arc<catalog::AccessDefinition>>> {
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
	async fn get_ns_access_grant(
		&self,
		ns: NamespaceId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>> {
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

	/// Retrieve a specific database access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_access(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &str,
	) -> Result<Option<Arc<catalog::AccessDefinition>>> {
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
	async fn get_db_access_grant(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &str,
		gr: &str,
	) -> Result<Option<Arc<catalog::AccessGrant>>> {
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

	async fn del_root_access(&self, ra: &str) -> Result<()> {
		// Delete the definition
		let key = crate::key::root::ac::new(ra);
		self.del(&key).await?;
		// Delete any associated data including access grants.
		let key = crate::key::root::access::all::new(ra);
		self.delp(&key).await?;

		// Return result
		Ok(())
	}

	async fn del_ns_access(&self, ns: NamespaceId, na: &str) -> Result<()> {
		// Delete the definition
		let key = crate::key::namespace::ac::new(ns, na);
		self.del(&key).await?;
		// Delete any associated data including access grants.
		let key = crate::key::namespace::access::all::new(ns, na);
		self.delp(&key).await?;

		// Return result
		Ok(())
	}

	async fn del_db_access(&self, ns: NamespaceId, db: DatabaseId, da: &str) -> Result<()> {
		// Delete the definition
		let key = crate::key::database::ac::new(ns, db, da);
		self.del(&key).await?;
		// Delete any associated data including access grants.
		let key = crate::key::database::access::all::new(ns, db, da);
		self.delp(&key).await?;

		// Return result
		Ok(())
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl ApiProvider for Transaction {
	/// Retrieve all api definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_apis(&self, ns: NamespaceId, db: DatabaseId) -> Result<Arc<[ApiDefinition]>> {
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

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &str,
	) -> Result<Option<Arc<ApiDefinition>>> {
		let qey = cache::tx::Lookup::Ap(ns, db, ap);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Some),
			None => {
				let key = crate::key::database::ap::new(ns, db, ap);
				let Some(val) = self.get(&key, None).await? else {
					return Ok(None);
				};
				let api_def = Arc::new(val);
				let val = cache::tx::Entry::Any(api_def.clone());
				self.cache.insert(qey, val.clone());
				Ok(Some(api_def))
			}
		}
	}

	async fn put_db_api(&self, ns: NamespaceId, db: DatabaseId, ap: &ApiDefinition) -> Result<()> {
		let name = ap.path.to_string();
		let key = crate::key::database::ap::new(ns, db, &name);
		self.set(&key, ap, None).await?;

		// Return result
		Ok(())
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl BucketProvider for Transaction {
	/// Retrieve all analyzer definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[catalog::BucketDefinition]>> {
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

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_bucket(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &str,
	) -> Result<Option<Arc<catalog::BucketDefinition>>> {
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
}

impl CatalogProvider for Transaction {}
