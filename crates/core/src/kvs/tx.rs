use super::batch::Batch;
use super::tr::Check;
use super::util;
use super::Key;
use super::KeyEncode;
use super::Val;
use super::Version;
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::idx::planner::ScanDirection;
use crate::idx::trees::store::cache::IndexTreeCaches;
use crate::kvs::cache;
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::index::{BatchId, BatchIdsCleanQueue, SharedIndexKey};
use crate::kvs::scanner::Scanner;
use crate::kvs::Transactor;
use crate::sql::statements::define::ApiDefinition;
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
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

// The number of keys one page of a name-reuse reclaim deletes.
// A page is a scan followed by that many deletes, all inside the caller's
// transaction, so this bounds how much is read in one request rather than how
// much the transaction may write.
const REUSE_BATCH_SIZE: u32 = 1_000;

// The number of keys the name-reuse reclaims of one transaction may delete between
// them. Reusing the name of a removed namespace or database has to destroy whatever
// its pending reclaim still holds, and that destruction shares the reusing
// statement's transaction. The cap is what keeps that transaction bounded, so it is
// spent from an allowance the transaction carries rather than granted per reuse: one
// statement can reach several names, and one transaction can hold many statements.
// A reuse with nothing left to spend fails rather than growing the write set without
// limit, and the name becomes available again once the background reclaim task has
// drained the job.
const REUSE_KEY_BUDGET: u64 = 10_000;

// The number of reclaim queue entries read in one page while finishing a reuse.
// The queue holds one entry per removed object, and a reuse only ever finishes the
// handful naming the object being reused, so a page this size is read once.
const REUSE_QUEUE_PAGE_SIZE: u32 = 1_000;

#[non_exhaustive]
pub struct Transaction {
	/// Is this is a local datastore transaction
	local: bool,
	/// The underlying transactor
	tx: Mutex<Transactor>,
	/// The query cache for this store
	cache: TransactionCache,
	/// Cache the index updates
	index_caches: IndexTreeCaches,
	/// Does this supports reverse scan
	has_reverse_scan: bool,
	/// The transaction is writeable
	writeable: bool,
	/// For each index, track the pending append batch for cleanup on cancel.
	pending_index_batches: Mutex<HashMap<SharedIndexKey, (BatchId, BatchIdsCleanQueue)>>,
	/// What is left of this transaction's name-reuse reclaim allowance.
	reuse_budget: AtomicU64,
	/// Whether this transaction has deferred the data of a removed namespace or
	/// database to the reclaim task.
	deferred_reclaim: AtomicBool,
}

impl Transaction {
	/// Create a new query store
	pub fn new(local: bool, writeable: bool, tx: Transactor) -> Transaction {
		Transaction {
			local,
			has_reverse_scan: tx.supports_reverse_scan(),
			writeable,
			tx: Mutex::new(tx),
			cache: TransactionCache::new(),
			index_caches: IndexTreeCaches::default(),
			pending_index_batches: Mutex::new(HashMap::new()),
			reuse_budget: AtomicU64::new(REUSE_KEY_BUDGET),
			deferred_reclaim: AtomicBool::new(false),
		}
	}

	pub(super) async fn lock_pending_index_batches<'a>(
		&'a self,
	) -> MutexGuard<'a, HashMap<SharedIndexKey, (BatchId, BatchIdsCleanQueue)>> {
		self.pending_index_batches.lock().await
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

	pub fn writeable(&self) -> bool {
		self.writeable
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
		// Enqueue pending index batches for deferred cleanup after rollback (cancel or failed
		// commit).
		self.cleanup_index_batches().await;
		// Cancel the transaction
		self.lock().await.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<(), Error> {
		// Commit the transaction
		if let Err(e) = self.lock().await.commit().await {
			// Enqueue pending index batches for deferred cleanup after commit failure.
			self.cleanup_index_batches().await;
			return Err(e);
		}
		Ok(())
	}

	/// Enqueue pending index batches for deferred cleanup after rollback (cancel or failed commit).
	async fn cleanup_index_batches(&self) {
		let batches = {
			let mut pending = self.lock_pending_index_batches().await;
			std::mem::take(&mut *pending)
		};
		for (_, (batch_id, clean_queue)) in batches {
			// Enqueue batch ids for cleaning
			clean_queue.lock().await.push(batch_id);
		}
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn exists<K>(&self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get<K>(&self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.get(key, version).await
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<Val>>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.getm(keys).await
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getp<K>(&self, key: K) -> Result<Vec<(Key, Val)>, Error>
	where
		K: KeyEncode + Debug,
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
		K: KeyEncode + Debug,
	{
		self.lock().await.getr(rng, version).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del<K>(&self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delc<K, V>(&self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
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
		K: KeyEncode + Debug,
	{
		self.lock().await.delr(rng).await
	}

	/// Delete a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delp<K>(&self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.delp(key).await
	}

	/// Delete at most `limit` keys from a prefix range, resuming after a cursor.
	///
	/// Scans one page of at most `limit` keys from `rng`, starting strictly
	/// after `start_after` when it is given, and deletes exactly those keys.
	///
	/// Unlike [`Self::delp`] and [`Self::delr`], this issues a bounded number
	/// of writes and never loops to exhaustion, so a caller can page a prefix
	/// of any size without the transaction growing without limit.
	///
	/// Returns the last key deleted, how many were deleted, and whether the
	/// range is now drained. The caller must commit that key as the resume
	/// cursor in the same transaction as these deletions, so the cursor can
	/// never claim progress that was rolled back.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delp_bounded(
		&self,
		rng: Range<Key>,
		start_after: Option<&[u8]>,
		limit: u32,
		expunge: bool,
	) -> Result<(Option<Key>, u64, bool), Error> {
		self.lock().await.delp_bounded(rng, start_after, limit, expunge).await
	}

	/// Whether a range holds anything at all, counting the versions of keys
	/// that a visible scan does not report.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn any_version_in(&self, rng: Range<Key>) -> Result<bool, Error> {
		self.lock().await.any_version_in(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr<K>(&self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrc<K, V>(&self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
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
		K: KeyEncode + Debug,
	{
		self.lock().await.clrr(rng).await
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrp<K>(&self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.clrp(key).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.set(key, val, version).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace<K, V>(&self, key: K, val: V) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
		V: Into<Val> + Debug,
	{
		self.lock().await.put(key, val, version).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn putc<K, V>(&self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Debug,
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
		K: KeyEncode + Debug,
	{
		self.lock().await.keys(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys, in a single request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.keysr(rng, limit, version).await
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
		K: KeyEncode + Debug,
	{
		self.lock().await.scan(rng, limit, version).await
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key> + Debug,
	{
		self.lock().await.scanr(rng, limit, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn count<K>(&self, rng: Range<K>) -> Result<usize, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.count(rng).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the keys in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.batch_keys(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.batch_keys_vals(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value-version pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals_versions<K>(
		&self,
		rng: Range<K>,
		batch: u32,
	) -> Result<Batch<(Key, Val, Version, bool)>, Error>
	where
		K: KeyEncode + Debug,
	{
		self.lock().await.batch_keys_vals_versions(rng, batch).await
	}

	/// Retrieve a stream over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream(
		&self,
		rng: Range<Vec<u8>>,
		version: Option<u64>,
		limit: Option<usize>,
		sc: ScanDirection,
	) -> impl Stream<Item = Result<(Key, Val), Error>> + '_ {
		Scanner::<(Key, Val)>::new(self, *NORMAL_FETCH_SIZE, rng, version, limit, sc)
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys(
		&self,
		rng: Range<Vec<u8>>,
		limit: Option<usize>,
		sc: ScanDirection,
	) -> impl Stream<Item = Result<Key, Error>> + '_ {
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
	pub async fn all_nodes(&self) -> Result<Arc<[Node]>, Error> {
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
	pub async fn all_root_users(&self) -> Result<Arc<[DefineUserStatement]>, Error> {
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
	pub async fn all_root_accesses(&self) -> Result<Arc<[DefineAccessStatement]>, Error> {
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
	pub async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[AccessGrant]>, Error> {
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
	pub async fn all_ns(&self) -> Result<Arc<[DefineNamespaceStatement]>, Error> {
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
	pub async fn all_ns_users(&self, ns: &str) -> Result<Arc<[DefineUserStatement]>, Error> {
		// Definitions inside a namespace the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
	pub async fn all_ns_accesses(&self, ns: &str) -> Result<Arc<[DefineAccessStatement]>, Error> {
		// Definitions inside a namespace the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		na: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		// Definitions inside a namespace the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
	pub async fn all_db(&self, ns: &str) -> Result<Arc<[DefineDatabaseStatement]>, Error> {
		// Definitions inside a namespace the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineUserStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAccessStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<[AccessGrant]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
	pub async fn all_db_apis(&self, ns: &str, db: &str) -> Result<Arc<[ApiDefinition]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineAnalyzerStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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

	/// Retrieve all function definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn all_db_functions(
		&self,
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineFunctionStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineParamStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineModelStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
	) -> Result<Arc<[DefineConfigStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		version: Option<u64>,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineEventStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		tb: &str,
		version: Option<u64>,
	) -> Result<Arc<[DefineFieldStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineIndexStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[DefineTableStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<[LiveStatement]>, Error> {
		// Definitions inside a database the catalog no longer holds are not
		// definitions: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::from(Vec::new()));
		}
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
	pub async fn get_node(&self, id: Uuid) -> Result<Arc<Node>, Error> {
		let qey = cache::tx::Lookup::Nd(id);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::root::nd::new(id).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::NdNotFound {
					uuid: id.to_string(),
				})?;
				let val: Node = revision::from_slice(&val)?;
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey, val.clone());
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
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::root::us::new(us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserRootNotFound {
					name: us.to_owned(),
				})?;
				let val: DefineUserStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_root_access(&self, ra: &str) -> Result<Arc<DefineAccessStatement>, Error> {
		let qey = cache::tx::Lookup::Ra(ra);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::root::ac::new(ra).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessRootNotFound {
					ac: ra.to_owned(),
				})?;
				let val: DefineAccessStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
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
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::root::access::gr::new(ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantRootNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
					})?;
				let val: AccessGrant = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific namespace definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns(&self, ns: &str) -> Result<Arc<DefineNamespaceStatement>, Error> {
		let qey = cache::tx::Lookup::Ns(ns);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::root::ns::new(ns).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::NsNotFound {
					name: ns.to_owned(),
				})?;
				let val: DefineNamespaceStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_user(&self, ns: &str, us: &str) -> Result<Arc<DefineUserStatement>, Error> {
		// A credential inside a namespace the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Err(Error::UserNsNotFound {
				name: us.to_owned(),
				ns: ns.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Nu(ns, us);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::namespace::us::new(ns, us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserNsNotFound {
					name: us.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineUserStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific namespace access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access(
		&self,
		ns: &str,
		na: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		// A credential inside a namespace the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Err(Error::AccessNsNotFound {
				ac: na.to_owned(),
				ns: ns.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Na(ns, na);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::namespace::ac::new(ns, na).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessNsNotFound {
					ac: na.to_owned(),
					ns: ns.to_owned(),
				})?;
				let val: DefineAccessStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific namespace access grant.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_ns_access_grant(
		&self,
		ns: &str,
		ac: &str,
		gr: &str,
	) -> Result<Arc<AccessGrant>, Error> {
		// A credential inside a namespace the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.ns_exists(ns).await? {
			return Err(Error::AccessGrantNsNotFound {
				ac: ac.to_owned(),
				gr: gr.to_owned(),
				ns: ns.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Ng(ns, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::namespace::access::gr::new(ns, ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantNsNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_owned(),
					})?;
				let val: AccessGrant = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific database definition.
	///
	/// A database definition lives inside its namespace's key prefix, which
	/// outlives the removal of that namespace, so reading one back is not
	/// evidence that the database exists: the namespace is resolved through the
	/// catalog first, and a database inside an absent namespace is absent.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db(&self, ns: &str, db: &str) -> Result<Arc<DefineDatabaseStatement>, Error> {
		if !self.ns_exists(ns).await? {
			return Err(Error::DbNotFound {
				name: db.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Db(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::namespace::db::new(ns, db).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::DbNotFound {
					name: db.to_owned(),
				})?;
				let val: DefineDatabaseStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_user(
		&self,
		ns: &str,
		db: &str,
		us: &str,
	) -> Result<Arc<DefineUserStatement>, Error> {
		// A credential inside a database the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::UserDbNotFound {
				name: us.to_owned(),
				ns: ns.to_owned(),
				db: db.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Du(ns, db, us);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::us::new(ns, db, us).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::UserDbNotFound {
					name: us.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineUserStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific database access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_access(
		&self,
		ns: &str,
		db: &str,
		da: &str,
	) -> Result<Arc<DefineAccessStatement>, Error> {
		// A credential inside a database the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::AccessDbNotFound {
				ac: da.to_owned(),
				ns: ns.to_owned(),
				db: db.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Da(ns, db, da);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::ac::new(ns, db, da).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AccessDbNotFound {
					ac: da.to_owned(),
					ns: ns.to_owned(),
					db: db.to_owned(),
				})?;
				let val: DefineAccessStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
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
		// A credential inside a database the catalog no longer holds must not
		// authenticate: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::AccessGrantDbNotFound {
				ac: ac.to_owned(),
				gr: gr.to_owned(),
				ns: ns.to_owned(),
				db: db.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::access::gr::new(ns, db, ac, gr).encode()?;
				let val =
					self.get(key, None).await?.ok_or_else(|| Error::AccessGrantDbNotFound {
						ac: ac.to_owned(),
						gr: gr.to_owned(),
						ns: ns.to_owned(),
						db: db.to_owned(),
					})?;
				let val: AccessGrant = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
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
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::MlNotFound {
				name: ml.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::ml::new(ns, db, ml, vn).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::MlNotFound {
					name: format!("{ml}<{vn}>"),
				})?;
				let val: DefineModelStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
	}

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_api(
		&self,
		ns: &str,
		db: &str,
		ap: &str,
	) -> Result<Arc<ApiDefinition>, Error> {
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::ApNotFound {
				value: ap.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Ap(ns, db, ap);
		match self.cache.get(&qey) {
			Some(val) => val,
			None => {
				let key = crate::key::database::ap::new(ns, db, ap).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::ApNotFound {
					value: ap.to_owned(),
				})?;
				let val: ApiDefinition = revision::from_slice(&val)?;
				let val = cache::tx::Entry::Any(Arc::new(val));
				self.cache.insert(qey, val.clone());
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
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::AzNotFound {
				name: az.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Az(ns, db, az);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::az::new(ns, db, az).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::AzNotFound {
					name: az.to_owned(),
				})?;
				let val: DefineAnalyzerStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		fc: &str,
	) -> Result<Arc<DefineFunctionStatement>, Error> {
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::FcNotFound {
				name: fc.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Fc(ns, db, fc);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::fc::new(ns, db, fc).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::FcNotFound {
					name: fc.to_owned(),
				})?;
				let val: DefineFunctionStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		pa: &str,
	) -> Result<Arc<DefineParamStatement>, Error> {
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::PaNotFound {
				name: pa.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Pa(ns, db, pa);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::pa::new(ns, db, pa).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::PaNotFound {
					name: pa.to_owned(),
				})?;
				let val: DefineParamStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		cg: &str,
	) -> Result<Arc<DefineConfigStatement>, Error> {
		if let Some(val) = self.get_db_optional_config(ns, db, cg).await? {
			Ok(val)
		} else {
			Err(Error::CgNotFound {
				name: cg.to_owned(),
			})
		}
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn get_db_optional_config(
		&self,
		ns: &str,
		db: &str,
		cg: &str,
	) -> Result<Option<Arc<DefineConfigStatement>>, Error> {
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(None);
		}
		let qey = cache::tx::Lookup::Cg(ns, db, cg);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type().map(Option::Some),
			None => {
				let key = crate::key::database::cg::new(ns, db, cg).encode()?;
				if let Some(val) = self.get(key, None).await? {
					let val: DefineConfigStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Arc<DefineTableStatement>, Error> {
		// A table definition inside a database the catalog no longer holds is not
		// a definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::TbNotFound {
				name: tb.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Tb(ns, db, tb);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::database::tb::new(ns, db, tb).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::TbNotFound {
					name: tb.to_owned(),
				})?;
				let val: DefineTableStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
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
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::EvNotFound {
				name: ev.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Ev(ns, db, tb, ev);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::table::ev::new(ns, db, tb, ev).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::EvNotFound {
					name: ev.to_owned(),
				})?;
				let val: DefineEventStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		tb: &str,
		fd: &str,
	) -> Result<Arc<DefineFieldStatement>, Error> {
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::FdNotFound {
				name: fd.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Fd(ns, db, tb, fd);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::table::fd::new(ns, db, tb, fd).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::FdNotFound {
					name: fd.to_owned(),
				})?;
				let val: DefineFieldStatement = revision::from_slice(&val)?;
				let val = Arc::new(val);
				let entr = cache::tx::Entry::Any(val.clone());
				self.cache.insert(qey, entr);
				Ok(val)
			}
		}
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
		// A definition inside a database the catalog no longer holds is not a
		// definition: the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Err(Error::IxNotFound {
				name: ix.to_owned(),
			});
		}
		let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_type(),
			None => {
				let key = crate::key::table::ix::new(ns, db, tb, ix).encode()?;
				let val = self.get(key, None).await?.ok_or_else(|| Error::IxNotFound {
					name: ix.to_owned(),
				})?;
				let val: DefineIndexStatement = revision::from_slice(&val)?;
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
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		version: Option<u64>,
	) -> Result<Arc<Value>, Error> {
		// A record inside a database the catalog no longer holds is not a record:
		// the removed prefix outlives the removal itself
		if !self.db_exists(ns, db).await? {
			return Ok(Arc::new(Value::None));
		}
		// Cache is not versioned
		if version.is_some() {
			// Fetch the record from the datastore
			let key = crate::key::thing::new(ns, db, tb, id).encode()?;
			match self.get(key, version).await? {
				// The value exists in the datastore
				Some(val) => {
					let val = cache::tx::Entry::Val(Arc::new(revision::from_slice(&val)?));
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
							let val = cache::tx::Entry::Val(Arc::new(revision::from_slice(&val)?));
							self.cache.insert(qey, val.clone());
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
		self.set(&key, revision::to_vec(&val)?, None).await?;
		// Set the value in the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(qey, cache::tx::Entry::Val(Arc::new(val)));
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
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.insert(qey, cache::tx::Entry::Val(val));
		// Return nothing
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn del_record(&self, ns: &str, db: &str, tb: &str, id: &Id) -> Result<(), Error> {
		// Set the value in the datastore
		let key = crate::key::thing::new(ns, db, tb, id);
		self.del(&key).await?;
		// Set the value in the cache
		let qey = cache::tx::Lookup::Record(ns, db, tb, id);
		self.cache.remove(qey);
		// Return nothing
		Ok(())
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
		self.get_or_add_tb(ns, db, tb, strict).await
	}

	/// Whether the catalog still holds a namespace.
	///
	/// A namespace that is absent gives `false`; every other read failure is
	/// propagated, so a store that cannot be read is never mistaken for one
	/// whose namespace has been removed.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub(crate) async fn ns_exists(&self, ns: &str) -> Result<bool, Error> {
		match self.get_ns(ns).await {
			Ok(_) => Ok(true),
			Err(Error::NsNotFound {
				..
			}) => Ok(false),
			Err(e) => Err(e),
		}
	}

	/// Whether the catalog still holds a database, and the namespace above it.
	///
	/// This is the gate on every name-addressed access to a database's data. A
	/// removal deletes only the catalog definition, so the definitions and
	/// records inside the removed prefix keep resolving by key until the
	/// background reclaim task destroys them: reading one is not evidence that
	/// the object it describes still exists, and only the catalog can answer
	/// that.
	///
	/// A namespace or database that is absent gives `false`; every other read
	/// failure is propagated.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub(crate) async fn db_exists(&self, ns: &str, db: &str) -> Result<bool, Error> {
		match self.get_db(ns, db).await {
			Ok(_) => Ok(true),
			Err(Error::DbNotFound {
				..
			}) => Ok(false),
			Err(e) => Err(e),
		}
	}

	/// Whether the data of a table may be read through its name.
	///
	/// Returns `false` when the namespace or the database is absent from the
	/// catalog, which is what keeps the data of a removed object unreachable
	/// between the removal committing and the reclaim task destroying it.
	/// Callers must produce the same result they would for an empty range
	/// rather than reading one.
	///
	/// In strict mode the table itself must be defined as well, and an absent
	/// namespace, database or table is an error naming the outermost one that
	/// is missing.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	#[inline(always)]
	pub(crate) async fn check_ns_db_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<bool, Error> {
		match strict {
			// Strict mode is disabled
			false => self.db_exists(ns, db).await,
			// Strict mode is enabled
			true => {
				// Check that the table exists
				match self.get_tb(ns, db, tb).await {
					Err(Error::TbNotFound {
						name: tb,
					}) => {
						// If not, check the database exists
						match self.get_db(ns, db).await {
							Err(Error::DbNotFound {
								name: db,
							}) => {
								// If not, check the namespace exists
								match self.get_ns(ns).await {
									Err(Error::NsNotFound {
										name: ns,
									}) => Err(Error::NsNotFound {
										name: ns,
									}),
									// Return any other errors
									Err(err) => Err(err),
									// Namespace does exist
									Ok(_) => Err(Error::DbNotFound {
										name: db,
									}),
								}
							}
							// Return any other errors
							Err(err) => Err(err),
							// Database does exist
							Ok(_) => Err(Error::TbNotFound {
								name: tb,
							}),
						}
					}
					// Return any other errors
					Err(err) => Err(err),
					// Table does exist
					Ok(_) => Ok(true),
				}
			}
		}
	}

	/// Removes a namespace from the catalog and enqueues its data for reclaim.
	///
	/// Only the catalog definition is deleted, so the namespace stops existing
	/// as soon as this transaction commits: every name-addressed read and write
	/// resolves the namespace through the catalog first, so its data prefix
	/// becomes unreachable even though it is still on disk. The prefix is
	/// destroyed later by the background reclaim task, which pages through it
	/// in bounded batches instead of writing the whole prefix into one
	/// transaction.
	///
	/// Both writes land in the caller's transaction, so a rollback leaves
	/// neither the removal nor the reclaim job, and no data is ever orphaned
	/// without a job naming it. Those two keys are the whole write set,
	/// whatever the namespace held.
	///
	/// The catalog definition is cleared rather than deleted, so no version of
	/// it survives: a namespace of the same name owns the same key, and a
	/// versioned read of the new one must not return the old definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn del_ns_deferred(&self, ns: &str) -> Result<(), Error> {
		// Drop what this transaction already resolved through the catalog, so that a
		// later statement in it cannot reach the removed namespace from cache
		self.cache.clear();
		// The datastore cache is cleared once this commits, not here: a query
		// running alongside this one resolves from a snapshot taken before the
		// removal and would put what it found back
		self.deferred_reclaim.store(true, Ordering::Release);
		// Delete the catalog definition, making the namespace unreachable
		self.clr(crate::key::root::ns::new(ns)).await?;
		// Enqueue the orphaned data prefix for the background reclaim task
		let job = crate::key::root::rc::ReclaimKey::namespace(Cow::Borrowed(ns), Uuid::now_v7());
		let val = revision::to_vec(&crate::key::root::rc::ReclaimState::enqueued())?;
		self.set(&job, val, None).await?;
		Ok(())
	}

	/// Removes a database from the catalog and enqueues its data for reclaim.
	///
	/// Only the catalog definition is deleted, so the database stops existing
	/// as soon as this transaction commits: every name-addressed read and write
	/// resolves the database through the catalog first, so its data prefix
	/// becomes unreachable even though it is still on disk. The prefix is
	/// destroyed later by the background reclaim task, which pages through it
	/// in bounded batches instead of writing the whole prefix into one
	/// transaction.
	///
	/// Both writes land in the caller's transaction, so a rollback leaves
	/// neither the removal nor the reclaim job, and no data is ever orphaned
	/// without a job naming it. Those two keys are the whole write set,
	/// whatever the database held.
	///
	/// The catalog definition is cleared rather than deleted, so no version of
	/// it survives: a database of the same name owns the same key, and a
	/// versioned read of the new one must not return the old definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn del_db_deferred(&self, ns: &str, db: &str) -> Result<(), Error> {
		// Drop what this transaction already resolved through the catalog, so that a
		// later statement in it cannot reach the removed database from cache
		self.cache.clear();
		// The datastore cache is cleared once this commits, not here: a query
		// running alongside this one resolves from a snapshot taken before the
		// removal and would put what it found back
		self.deferred_reclaim.store(true, Ordering::Release);
		// Delete the catalog definition, making the database unreachable
		self.clr(crate::key::namespace::db::new(ns, db)).await?;
		// Enqueue the orphaned data prefix for the background reclaim task
		let job = crate::key::root::rc::ReclaimKey::database(
			Cow::Borrowed(ns),
			Cow::Borrowed(db),
			Uuid::now_v7(),
		);
		let val = revision::to_vec(&crate::key::root::rc::ReclaimState::enqueued())?;
		self.set(&job, val, None).await?;
		Ok(())
	}

	/// The part of `rng` that a paged delete resuming after `cursor` still
	/// covers, matching how [`Transaction::delp_bounded`] resumes.
	///
	/// A cursor from outside the range is ignored rather than followed, so a
	/// stale one can never widen the window beyond the prefix it belongs to.
	fn resume_after(rng: &Range<Key>, cursor: Option<&[u8]>) -> Range<Key> {
		let start = match cursor {
			Some(k) if k >= rng.start.as_slice() => {
				let mut k = k.to_vec();
				k.push(0x00);
				k
			}
			_ => rng.start.clone(),
		};
		start..rng.end.clone()
	}

	/// Empties the prefix a namespace name addresses, or a database name when
	/// `db` is given, so that the name may be used again.
	///
	/// A namespace or database addresses its data by name, so bytes left under
	/// that name are bytes a new object of the same name would otherwise inherit.
	/// Taking a name therefore destroys its prefix in the transaction that takes
	/// it: either the new object exists and the old bytes are gone, or neither
	/// happened.
	///
	/// The prefix is destroyed whether or not a queue entry still describes it.
	/// An entry is retired once the prefix reads as empty, and a removal writes
	/// only the catalog row and the entry, so a transaction holding a snapshot
	/// from before the removal conflicts with neither and may commit into the
	/// prefix after the entry is gone. Nothing names such a key afterwards, so
	/// this is the only guard that can catch it. Any entry naming the prefix is
	/// finished here too, because a reclaim task that kept one would later
	/// destroy a range inside an object that exists again.
	///
	/// Taking either name also finishes any entry naming the namespace itself,
	/// because a namespace entry's prefix covers every database in it, so an
	/// entry left behind describes a range inside the database being taken. An
	/// entry naming a database is left to the reuse of that database's own name,
	/// or to the reclaim task, which addresses the database rather than the
	/// namespace and so is unaffected by the namespace existing.
	///
	/// Every prefix destroyed here is one the catalog says is orphaned, an
	/// entry's as much as the name's own. An entry that outlives its object's
	/// return describes bytes that belong to the live object, and a namespace
	/// entry covers every database in it, so destroying one would take a whole
	/// live namespace. Such an entry is left in place: the reclaim task makes
	/// the same check before it acts on one.
	///
	/// This destruction shares the caller's transaction, so it is capped at
	/// [`REUSE_KEY_BUDGET`] keys, read and deleted in pages of
	/// [`REUSE_BATCH_SIZE`]. The cap belongs to the transaction rather than to
	/// the call: one statement can reuse several names, and one transaction can
	/// hold many statements, so each reuse spends what the ones before it left.
	/// A reuse which would exceed what is left fails with
	/// [`Error::ReclaimPending`] rather than growing the caller's transaction
	/// without limit; the name becomes available again as the background reclaim
	/// task drains the job, which by default starts on the next tick after the
	/// removal is first observed.
	///
	/// Callers must reach this before writing anything into the prefix, which
	/// is why every path that resolves a namespace or database name does so
	/// through the catalog before it constructs a data key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub async fn finish_pending_reclaim(&self, ns: &str, db: Option<&str>) -> Result<(), Error> {
		// Taken in full for the duration of the call, so that what it spends is
		// never lost and nothing else can spend the same allowance alongside it
		let mut budget = self.reuse_budget.swap(0, Ordering::AcqRel);
		let res = self.finish_pending_reclaim_with(ns, db, &mut budget).await;
		self.reuse_budget.store(budget, Ordering::Release);
		res
	}

	/// [`Self::finish_pending_reclaim`] with this transaction's remaining key
	/// budget set first, so that a test can reach the cap without writing
	/// [`REUSE_KEY_BUDGET`] keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub(crate) async fn finish_pending_reclaim_within(
		&self,
		ns: &str,
		db: Option<&str>,
		budget: u64,
	) -> Result<(), Error> {
		self.reuse_budget.store(budget, Ordering::Release);
		self.finish_pending_reclaim(ns, db).await
	}

	/// The body of [`Self::finish_pending_reclaim`], spending `budget` in place
	/// so that what a failed reuse destroyed before it gave up is still counted.
	async fn finish_pending_reclaim_with(
		&self,
		ns: &str,
		db: Option<&str>,
		budget: &mut u64,
	) -> Result<(), Error> {
		use crate::key::root::rc::ReclaimKey;
		// A job naming the namespace covers every database inside it, so it has
		// to be finished whichever of the two is being reused
		let mut ranges = vec![ReclaimKey::namespace_range(ns)?];
		if let Some(db) = db {
			ranges.push(ReclaimKey::database_range(ns, db)?);
		}
		let mut described = false;
		for rng in ranges {
			let jobs = self.keys(rng, REUSE_QUEUE_PAGE_SIZE, None).await?;
			// A full page may not be all of them, and a job left behind would name a
			// range inside an object that exists again
			if jobs.len() >= REUSE_QUEUE_PAGE_SIZE as usize {
				return Err(Error::ReclaimPending {
					name: match db {
						Some(db) => format!("{ns}/{db}"),
						None => ns.to_owned(),
					},
				});
			}
			for key in jobs {
				let job = ReclaimKey::decode_key(&key)?;
				let data = job.prefix_range()?;
				let (job_ns, job_db) = job.target();
				let name = match job_db {
					Some(db) => format!("{job_ns}/{db}"),
					None => job_ns.to_owned(),
				};
				// The catalog is the authority on whether the entry's prefix is
				// orphaned, as it is for the reclaim task, which leaves an entry
				// alone rather than destroying a prefix whose object exists
				// again. An entry outliving its object's return describes bytes
				// that belong to the live object, and a namespace entry's prefix
				// covers every database in it, so destroying one here would take
				// a whole live namespace. Leaving it costs nothing: the task
				// makes the same check before it acts.
				if self.name_is_taken(job_ns, job_db).await? {
					warn!(
						target: "surrealdb::core::kvs::tx",
						"Reclaim entry naming {name}, which exists again, left alone while its name is taken",
					);
					continue;
				}
				self.destroy_orphaned(data, &name, budget).await?;
				// Dropping the job in the same transaction as the data it named
				// keeps the queue from outliving what it describes. Its key holds
				// a version for every page the reaper committed against it, and
				// its uid is never reused, so a tombstone would leave that whole
				// history on disk for good.
				self.clr(key).await?;
				// Definitions read out of the destroyed prefix earlier in this
				// transaction no longer describe anything. Clearing here rather
				// than once at the end keeps the catalog reads around it from
				// being answered out of a cache this has just invalidated.
				self.cache.clear();
				// An entry destroyed here named a prefix covering the one this
				// name addresses, so the sweep below has nothing left to do. One
				// left alone destroyed nothing, so it does not speak for it.
				described = true;
			}
		}
		// A prefix nothing describes may still hold bytes. A job is retired once
		// its prefix reads as empty, and a removal writes only the catalog row
		// and the queue entry, so a transaction holding a snapshot from before
		// the removal conflicts with neither and may commit into the prefix after
		// the job is gone. Nothing names such a key afterwards, which leaves this
		// as the only side that can catch it.
		//
		// The catalog decides whether the prefix is orphaned, exactly as it does
		// for the reclaim task: this is reached from `DEFINE ... OVERWRITE` on an
		// object that exists, whose data is live and must not be touched.
		if !described && !self.name_is_taken(ns, db).await? {
			let name = match db {
				Some(db) => format!("{ns}/{db}"),
				None => ns.to_owned(),
			};
			if self.destroy_orphaned(ReclaimKey::prefix_range_of(ns, db)?, &name, budget).await? {
				self.cache.clear();
			}
		}
		Ok(())
	}

	/// Whether the catalog holds the namespace, or the database when `db` is
	/// given, that a name addresses.
	///
	/// A namespace or database row is the only authority on whether the prefix
	/// under that name is live or orphaned, because the data is addressed by the
	/// name rather than by anything the prefix itself carries.
	async fn name_is_taken(&self, ns: &str, db: Option<&str>) -> Result<bool, Error> {
		match db {
			Some(db) => self.db_exists(ns, db).await,
			None => self.ns_exists(ns).await,
		}
	}

	/// Destroys every version of every key under an orphaned prefix, spending
	/// `budget` in place, and reports whether anything was there to destroy.
	///
	/// Names are reused on this line, so a prefix that was only tombstoned would
	/// be inherited version-for-version by the object about to take its name: a
	/// versioned read of the new object would return records that belonged to
	/// the old one. Every version goes, not just the visible one.
	///
	/// This shares the caller's transaction, so it spends `budget` and refuses
	/// with [`Error::ReclaimPending`] rather than growing the write set without
	/// limit. `name` is the object named in that refusal.
	async fn destroy_orphaned(
		&self,
		data: Range<Key>,
		name: &str,
		budget: &mut u64,
	) -> Result<bool, Error> {
		// Ask before writing. A prefix with more visible keys than the budget
		// has more distinct keys than the budget, so the pages below would refuse
		// after destroying what they were allowed and rolling all of it back.
		// This path is reached from the implicit creation every write does
		// outside strict mode, so that would be paid by each of them, and again
		// by every retry, for as long as the name is held. Only a refusal is
		// decided here: a prefix that fits may still hold versions a visible scan
		// does not report, and it is the pages below that count those.
		let ask = (*budget).saturating_add(1).min(u32::MAX as u64) as u32;
		if self.keys(data.clone(), ask, None).await?.len() as u64 > *budget {
			return Err(Error::ReclaimPending {
				name: name.to_owned(),
			});
		}
		// Destroy the prefix a page at a time, never exceeding what this
		// transaction is allowed to write
		let mut destroyed = false;
		let mut cursor: Option<Key> = None;
		loop {
			let limit = (*budget).min(REUSE_BATCH_SIZE as u64) as u32;
			if limit == 0 {
				// A page that exactly drains the range spends its whole limit
				// and so reads as a full one, which would refuse a prefix that
				// had in fact already been destroyed. The budget is only exceeded
				// if something is still there. The pages above destroy versions,
				// so this asks about versions too: a prefix holding only
				// tombstoned keys looks empty to a visible scan and would be
				// inherited whole by the object about to take the name.
				let rest = Self::resume_after(&data, cursor.as_deref());
				if !self.any_version_in(rest).await? {
					break;
				}
				return Err(Error::ReclaimPending {
					name: name.to_owned(),
				});
			}
			let (last, count, drained) =
				self.delp_bounded(data.clone(), cursor.as_deref(), limit, true).await?;
			*budget = budget.saturating_sub(count);
			destroyed |= count > 0;
			if drained {
				break;
			}
			// Resume strictly after the last key this page deleted
			cursor = last;
		}
		Ok(destroyed)
	}

	/// Whether this transaction has deferred the data of a removed namespace or
	/// database to the reclaim task.
	///
	/// The datastore cache holds database and table definitions under keys that
	/// carry no version of their own, so nothing invalidates them but a clear.
	/// Clearing inside the removing transaction is not enough: a query running
	/// alongside it resolves those definitions from a snapshot taken before the
	/// removal and puts them back. A caller which commits this transaction must
	/// therefore clear the cache again afterwards, so that what a removal leaves
	/// behind is bounded by the removal rather than by the next unrelated clear.
	pub fn deferred_reclaim(&self) -> bool {
		self.deferred_reclaim.load(Ordering::Acquire)
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
	pub async fn get_or_add_ns(
		&self,
		ns: &str,
		strict: bool,
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
					name: ns.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::NsNotFound {
						..
					}) if !strict => {
						// The name is being reused, so anything a pending
						// reclaim still holds under it must go first
						self.finish_pending_reclaim(ns, None).await?;
						let val = DefineNamespaceStatement {
							name: ns.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, revision::to_vec(&val)?, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey, val.clone());
						val
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineNamespaceStatement = revision::from_slice(&val)?;
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
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
	pub async fn get_or_add_db(
		&self,
		ns: &str,
		db: &str,
		strict: bool,
	) -> Result<Arc<DefineDatabaseStatement>, Error> {
		// A database definition lives inside the namespace prefix, which outlives
		// the removal of its namespace, so reading one back is not evidence that
		// the database exists. The namespace is resolved through the catalog
		// first, which is also what finishes a reclaim still pending for its name
		// before this transaction writes anything into that prefix — and what
		// destroys the stale definition below, so the read that follows sees the
		// namespace as it is now rather than as it was before the removal.
		//
		// This is not the caller's choice: a database row is never trusted on
		// its own. In strict mode it is also what reports an absent namespace as
		// such rather than as an absent database.
		self.get_or_add_ns(ns, strict).await?;
		let qey = cache::tx::Lookup::Db(ns, db);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				// Try to fetch the value from the datastore
				let key = crate::key::namespace::db::new(ns, db);
				let res = self.get(&key, None).await?.ok_or_else(|| Error::DbNotFound {
					name: db.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::DbNotFound {
						..
					}) if !strict => {
						// The name is being reused, so anything a pending
						// reclaim still holds under it must go first
						self.finish_pending_reclaim(ns, Some(db)).await?;
						// Next, dynamically define the database
						let val = DefineDatabaseStatement {
							name: db.to_owned().into(),
							..Default::default()
						};
						let val = {
							self.put(&key, revision::to_vec(&val)?, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey, val.clone());
						val
					}
					// The namespace is already resolved above, so an absent
					// database in strict mode is exactly what it says
					Err(Error::DbNotFound {
						name,
					}) if strict => Err(Error::DbNotFound {
						name,
					})?,
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineDatabaseStatement = revision::from_slice(&val)?;
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
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
	pub async fn get_or_add_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		strict: bool,
	) -> Result<Arc<DefineTableStatement>, Error> {
		// A table definition lives inside the database prefix, which outlives the
		// removal of its database, so reading one back is not evidence that the
		// table exists. The database is resolved through the catalog first, which
		// is also what finishes a reclaim still pending for its name before this
		// transaction writes anything into that prefix.
		self.get_or_add_db(ns, db, strict).await?;
		let qey = cache::tx::Lookup::Tb(ns, db, tb);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val,
			// The entry is not in the cache
			None => {
				// Try to fetch the value from the datastore
				let key = crate::key::database::tb::new(ns, db, tb);
				let res = self.get(&key, None).await?.ok_or_else(|| Error::TbNotFound {
					name: tb.to_owned(),
				});
				// Check whether the value exists in the datastore
				match res {
					// Store a new default value in the datastore
					Err(Error::TbNotFound {
						..
					}) if !strict => {
						// Dynamically define the table
						let val = DefineTableStatement {
							name: tb.to_owned().into(),
							permissions: Permissions::none(),
							..Default::default()
						};
						let val = {
							self.put(&key, revision::to_vec(&val)?, None).await?;
							cache::tx::Entry::Any(Arc::new(val))
						};
						self.cache.insert(qey, val.clone());
						val
					}
					// Check to see that the hierarchy exists
					Err(Error::TbNotFound {
						name,
					}) if strict => {
						self.get_ns(ns).await?;
						self.get_db(ns, db).await?;
						Err(Error::TbNotFound {
							name,
						})?
					}
					// Store the fetched value in the cache
					Ok(val) => {
						let val: DefineTableStatement = revision::from_slice(&val)?;
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
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
