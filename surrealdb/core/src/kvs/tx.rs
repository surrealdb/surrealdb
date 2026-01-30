use std::fmt::Debug;
use std::ops::{Deref, Range};
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use futures::future::try_join_all;
use futures::stream::Stream;
use priority_lfu::CacheKeyLookup;
use uuid::Uuid;

use super::batch::Batch;
use super::{Key, Val, Version, util};
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, CatalogProvider, DatabaseProvider,
	NamespaceProvider, NodeProvider, RootProvider, TableProvider, UserProvider,
};
use crate::catalog::{
	self, ApiDefinition, ConfigDefinition, DatabaseDefinition, DatabaseId, DefaultConfig, IndexId,
	NamespaceDefinition, NamespaceId, Record, TableDefinition, TableId,
};
use crate::ctx::Context;
use crate::dbs::node::Node;
use crate::doc::CursorRecord;
use crate::err::Error;
use crate::idx::planner::ScanDirection;
use crate::key::database::sq::Sq;
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::scanner::Direction;
use crate::kvs::sequences::Sequences;
use crate::kvs::{KVKey, KVValue, Transactor, cache};
use crate::val::{RecordId, RecordIdKey, TableName};

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The underlying transactor
	tr: Transactor,
	/// The query cache for this store
	cache: TransactionCache,
	/// The sequences for this store
	sequences: Sequences,
	// The changefeed buffer
	cf: crate::cf::Writer,
}

impl Deref for Transaction {
	type Target = Transactor;

	fn deref(&self) -> &Self::Target {
		&self.tr
	}
}

impl Transaction {
	/// Create a new query store
	pub fn new(local: bool, sequences: Sequences, tr: Transactor) -> Transaction {
		Transaction {
			local,
			tr,
			cache: TransactionCache::new(),
			sequences,
			cf: crate::cf::Writer::new(),
		}
	}

	/// Check if the transaction is local or remote
	pub fn is_local(&self) -> bool {
		self.local
	}

	/// Enclose this transaction in an [`Arc`]
	pub fn enclose(self) -> Arc<Transaction> {
		Arc::new(self)
	}

	/// Check if the transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`kvs::Error::TransactionFinished`] error.
	pub fn closed(&self) -> bool {
		self.tr.closed()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn cancel(&self) -> Result<()> {
		// Clear any buffered changefeed entries
		self.cf.clear();
		// Cancel the transaction
		Ok(self.tr.cancel().await.map_err(Error::from)?)
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<()> {
		// Store any buffered changefeed entries
		if let Err(e) = self.store_changes().await {
			// Cancel the transaction if failure
			let _ = self.cancel().await;
			// Return the error
			return Err(e);
		}
		// Commit the transaction
		Ok(self.tr.commit().await.map_err(Error::from)?)
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn exists<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		Ok(self.tr.exists(key, version).await.map_err(Error::from)?)
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get<K>(&self, key: &K, version: Option<u64>) -> Result<Option<K::ValueType>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = self.tr.get(key, version).await.map_err(Error::from)?;
		val.map(K::ValueType::kv_decode_value).transpose()
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm<K>(
		&self,
		keys: Vec<K>,
		version: Option<u64>,
	) -> Result<Vec<Option<K::ValueType>>>
	where
		K: KVKey + Debug,
	{
		let keys = keys.iter().map(|k| k.encode_key()).collect::<Result<Vec<_>>>()?;
		self.tr
			.getm(keys, version)
			.await
			.map_err(Error::from)?
			.into_iter()
			.map(|v| match v {
				Some(v) => K::ValueType::kv_decode_value(v).map(Some),
				None => Ok(None),
			})
			.collect()
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in
	/// grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getp<K>(&self, key: &K) -> Result<Vec<(Key, K::ValueType)>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		self.tr
			.getp(key)
			.await
			.map_err(Error::from)?
			.into_iter()
			.map(|(k, v)| Ok((k, K::ValueType::kv_decode_value(v)?)))
			.collect()
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches key-value pairs from the underlying datastore in
	/// grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
	) -> Result<Vec<(Key, K::ValueType)>>
	where
		K: KVKey + Debug,
	{
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		self.tr
			.getr(beg..end, version)
			.await
			.map_err(Error::from)?
			.into_iter()
			.map(|(k, v)| Ok((k, K::ValueType::kv_decode_value(v)?)))
			.collect()
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		Ok(self.tr.del(key).await.map_err(Error::from)?)
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		Ok(self.tr.delc(key, chk).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.delr(beg..end).await.map_err(Error::from)?)
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
		let key = key.encode_key()?;
		Ok(self.tr.delp(key).await.map_err(Error::from)?)
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		Ok(self.tr.clr(key).await.map_err(Error::from)?)
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		Ok(self.tr.clrc(key, chk).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.clrr(beg..end).await.map_err(Error::from)?)
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
		let key = key.encode_key()?;
		Ok(self.tr.clrp(key).await.map_err(Error::from)?)
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		Ok(self.tr.set(key, val, version).await.map_err(Error::from)?)
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		Ok(self.tr.put(key, val, version).await.map_err(Error::from)?)
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
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		Ok(self.tr.putc(key, val, chk).await.map_err(Error::from)?)
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		Ok(self.tr.replace(key, val).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.keys(beg..end, limit, version).await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.keysr(beg..end, limit, version).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.scan(beg..end, limit, version).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.scanr(beg..end, limit, version).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.count(beg..end).await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.batch_keys(beg..end, batch, version).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.batch_keys_vals(beg..end, batch, version).await.map_err(Error::from)?)
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
		let beg = rng.start.encode_key()?;
		let end = rng.end.encode_key()?;
		Ok(self.tr.batch_keys_vals_versions(beg..end, batch).await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Stream functions
	// --------------------------------------------------

	/// Retrieve a stream over a specific range of keys in the datastore.
	///
	/// This function fetches keys in batches, with multiple requests to the
	/// underlying datastore. The Scanner uses adaptive batch sizing, starting
	/// at 100 items and doubling up to MAX_BATCH_SIZE. Prefetching is enabled
	/// by default for optimal read throughput.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys(
		&self,
		rng: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: ScanDirection,
	) -> impl Stream<Item = Result<Key>> + '_ {
		self.tr
			.stream_keys(
				rng,
				version,
				limit,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
			)
			.map_err(Error::from)
			.map_err(Into::into)
	}

	/// Retrieve a stream over a specific range of key-value pairs in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore. The Scanner uses adaptive batch
	/// sizing, starting at 100 items and doubling up to MAX_BATCH_SIZE.
	/// Prefetching is enabled by default for optimal read throughput.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys_vals(
		&self,
		rng: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: ScanDirection,
	) -> impl Stream<Item = Result<(Key, Val)>> + '_ {
		self.tr
			.stream_keys_vals(
				rng,
				version,
				limit,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
			)
			.map_err(Error::from)
			.map_err(Into::into)
	}

	/// Retrieve a stream over a specific range of keys in the datastore without
	/// prefetching.
	///
	/// This variant disables prefetching, making it more suitable for scenarios
	/// where each key will be processed with write operations (e.g., delete, update)
	/// and prefetching would waste work on errors.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys_no_prefetch(
		&self,
		rng: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: ScanDirection,
	) -> impl Stream<Item = Result<Key>> + '_ {
		self.tr
			.stream_keys_no_prefetch(
				rng,
				version,
				limit,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
			)
			.map_err(Error::from)
			.map_err(Into::into)
	}

	/// Retrieve a stream over a specific range of key-value pairs in the datastore without
	/// prefetching.
	///
	/// This variant disables prefetching, making it more suitable for scenarios
	/// where each key will be processed with write operations (e.g., delete, update)
	/// and prefetching would waste work on errors.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub fn stream_keys_vals_no_prefetch(
		&self,
		rng: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: ScanDirection,
	) -> impl Stream<Item = Result<(Key, Val)>> + '_ {
		self.tr
			.stream_keys_vals_no_prefetch(
				rng,
				version,
				limit,
				match dir {
					ScanDirection::Forward => Direction::Forward,
					ScanDirection::Backward => Direction::Backward,
				},
			)
			.map_err(Error::from)
			.map_err(Into::into)
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	pub async fn new_save_point(&self) -> Result<()> {
		Ok(self.inner.new_save_point().await.map_err(Error::from)?)
	}

	/// Release the last save point.
	pub async fn release_last_save_point(&self) -> Result<()> {
		Ok(self.inner.release_last_save_point().await.map_err(Error::from)?)
	}

	/// Rollback to the last save point.
	pub async fn rollback_to_save_point(&self) -> Result<()> {
		Ok(self.inner.rollback_to_save_point().await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	async fn timestamp(&self) -> Result<Box<dyn crate::kvs::Timestamp>> {
		Ok(self.tr.timestamp().await.map_err(Error::from)?)
	}

	/// Convert a versionstamp to timestamp bytes for this storage engine
	pub async fn timestamp_bytes_from_versionstamp(&self, version: u128) -> Result<Vec<u8>> {
		Ok(self.tr.timestamp_bytes_from_versionstamp(version).await.map_err(Error::from)?)
	}

	/// Convert a datetime to timestamp bytes for this storage engine
	pub async fn timestamp_bytes_from_datetime(&self, datetime: DateTime<Utc>) -> Result<Vec<u8>> {
		Ok(self.tr.timestamp_bytes_from_datetime(datetime).await.map_err(Error::from)?)
	}

	// --------------------------------------------------
	// Changefeed functions
	// --------------------------------------------------

	// Records the table (re)definition in the changefeed if enabled.
	pub(crate) fn changefeed_buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		dt: &TableDefinition,
	) {
		self.cf.changefeed_buffer_table_change(ns, db, tb, dt)
	}

	// change will record the change in the changefeed if enabled.
	// To actually persist the record changes into the underlying kvs,
	// you must call the `complete_changes` function and then commit the
	// transaction.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn changefeed_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: CursorRecord,
		current: CursorRecord,
		store_difference: bool,
	) {
		self.cf.changefeed_buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous,
			current,
			store_difference,
		)
	}

	// complete_changes will complete the changefeed recording for the given
	// namespace and database.
	//
	// This function writes all buffered changefeed entries to the datastore
	// with the current transaction timestamp. Every change must be recorded by
	// calling this struct's `changefeed_buffer_record_change` function beforehand.
	// If there were no preceding calls for this transaction, this function
	// will do nothing.
	//
	// This function should be called only after all the changes have been made to
	// the transaction. Otherwise, changes are missed in the change feed.
	//
	// This function should be called immediately before calling the commit function
	// to ensure the timestamp reflects the actual commit time.
	pub(crate) async fn store_changes(&self) -> Result<()> {
		// Get the changes from the changefeed
		let changes = self.cf.changes()?;
		// For zero-length changes, return early
		if changes.is_empty() {
			return Ok(());
		}
		// Get the current transaction timestamp
		let ts = self.timestamp().await?.to_ts_bytes();
		// Convert the timestamp bytes to a slice
		let ts = ts.as_slice();
		// Collect all changefeed write operations as futures
		let futures = changes.into_iter().map(|(ns, db, tb, value)| async move {
			// Create the changefeed key with the current timestamp
			let key = crate::key::change::new(ns, db, ts, &tb).encode_key()?;
			// Write the changefeed entry using the raw transactor API
			self.tr.set(key, value, None).await.map_err(Error::from)?;
			// Everything succeeded
			Ok::<(), anyhow::Error>(())
		});
		// Execute all write operations concurrently
		try_join_all(futures).await?;
		// All good
		Ok(())
	}

	// --------------------------------------------------
	// Cache functions
	// --------------------------------------------------

	#[inline]
	fn set_record_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		record: Arc<Record>,
	) {
		// Set the value in the cache
		let lookup = cache::tx::key::RecordCacheKeyRef(ns, db, tb.as_str(), id);
		self.cache.insert(lookup.to_owned_key(), Arc::clone(&record));
	}

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn compact<K>(&self, prefix_key: Option<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let rng = match prefix_key {
			Some(prefix_key) => Some(util::to_prefix_range(prefix_key)?),
			None => None,
		};
		self.tr.inner.compact(rng).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl NodeProvider for Transaction {
	/// Retrieve all nodes belonging to this cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_nodes(&self) -> Result<Arc<[Node]>> {
		let key = cache::tx::key::NodesCacheKey;
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::root::nd::prefix();
				let end = crate::key::root::nd::suffix();
				let raw_val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(raw_val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific node in the cluster.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_node(&self, id: Uuid) -> Result<Arc<Node>> {
		let key = cache::tx::key::NodeCacheKey(id);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let node_key = crate::key::root::nd::new(id);
				let val = self.get(&node_key, None).await?.ok_or_else(|| Error::NdNotFound {
					uuid: id.to_string(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl RootProvider for Transaction {
	async fn get_default_config(&self) -> Result<Option<Arc<DefaultConfig>>> {
		let lookup = cache::tx::key::RootConfigCacheKeyRef("default");
		match self.cache.get_clone_by(&lookup) {
			Some(val) => {
				// Since we store ConfigDefinition but need DefaultConfig, extract it
				match &*val {
					ConfigDefinition::Default(default) => Ok(Some(Arc::new(default.clone()))),
					_ => fail!("Expected a default config but found something else"),
				}
			}
			None => {
				let cfg_key = crate::key::root::root_config::new("default");
				let Some(val) = self.get(&cfg_key, None).await? else {
					return Ok(None);
				};
				let ConfigDefinition::Default(default_config) = &val else {
					fail!("Expected a default config but found {val:?} instead");
				};
				let result = Arc::new(default_config.clone());
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(result))
			}
		}
	}

	/// Retrieve a specific config definition from the root.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_config(&self, cg: &str) -> Result<Option<Arc<ConfigDefinition>>> {
		let lookup = cache::tx::key::RootConfigCacheKeyRef(cg);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let cfg_key = crate::key::root::root_config::new(cg);
				if let Some(val) = self.get(&cfg_key, None).await? {
					let val = Arc::new(val);
					self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
impl NamespaceProvider for Transaction {
	/// Retrieve all namespace definitions in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns(&self) -> Result<Arc<[NamespaceDefinition]>> {
		let key = cache::tx::key::NamespacesCacheKey;
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::root::ns::prefix();
				let end = crate::key::root::ns::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;

				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	async fn get_ns_by_name(&self, ns: &str) -> Result<Option<Arc<NamespaceDefinition>>> {
		let lookup = cache::tx::key::NamespaceByNameCacheKeyRef(ns);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ns_key = crate::key::root::ns::new(ns);
				let Some(ns) = self.get(&ns_key, None).await? else {
					return Ok(None);
				};

				let ns = Arc::new(ns);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&ns));
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
		let cached_ns = Arc::new(ns);
		let lookup = cache::tx::key::NamespaceByNameCacheKeyRef(&cached_ns.name);
		self.cache.insert(lookup.to_owned_key(), Arc::clone(&cached_ns));

		Ok(cached_ns)
	}

	async fn get_next_ns_id(&self, ctx: Option<&Context>) -> Result<NamespaceId> {
		self.sequences.next_namespace_id(ctx).await
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl DatabaseProvider for Transaction {
	/// Retrieve all database definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db(&self, ns: NamespaceId) -> Result<Arc<[DatabaseDefinition]>> {
		let key = cache::tx::key::DatabasesCacheKey(ns);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::namespace::db::prefix(ns)?;
				let end = crate::key::namespace::db::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific database definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_by_name(&self, ns: &str, db: &str) -> Result<Option<Arc<DatabaseDefinition>>> {
		let lookup = cache::tx::key::DatabaseByNameCacheKeyRef(ns, db);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let Some(ns) = self.get_ns_by_name(ns).await? else {
					return Ok(None);
				};

				let db_key = crate::key::namespace::db::new(ns.namespace_id, db);
				let Some(db_def) = self.get(&db_key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(db_def);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self, ctx))]
	async fn get_or_add_db_upwards(
		&self,
		ctx: Option<&Context>,
		ns: &str,
		db: &str,
		upwards: bool,
	) -> Result<Arc<DatabaseDefinition>> {
		let lookup = cache::tx::key::DatabaseByNameCacheKeyRef(ns, db);
		match self.cache.get_clone_by(&lookup) {
			// The entry is in the cache
			Some(val) => Ok(val),
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

	async fn get_next_db_id(&self, ctx: Option<&Context>, ns: NamespaceId) -> Result<DatabaseId> {
		self.sequences.next_database_id(ctx, ns).await
	}

	async fn put_db(&self, ns: &str, db: DatabaseDefinition) -> Result<Arc<DatabaseDefinition>> {
		let key = crate::key::namespace::db::new(db.namespace_id, &db.name);
		self.set(&key, &db, None).await?;

		// Populate cache
		let cached_db = Arc::new(db);
		let lookup = cache::tx::key::DatabaseByNameCacheKeyRef(ns, &cached_db.name);
		self.cache.insert(lookup.to_owned_key(), Arc::clone(&cached_db));

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
		let key = cache::tx::key::AnalyzersCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::az::prefix(ns, db)?;
				let end = crate::key::database::az::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::SequencesCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::sq::prefix(ns, db)?;
				let end = crate::key::database::sq::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::FunctionsCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::fc::prefix(ns, db)?;
				let end = crate::key::database::fc::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::ModulesCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::md::prefix(ns, db)?;
				let end = crate::key::database::md::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::ParamsCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::pa::prefix(ns, db)?;
				let end = crate::key::database::pa::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::ModelsCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::ml::prefix(ns, db)?;
				let end = crate::key::database::ml::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve all config definitions for a specific database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[ConfigDefinition]>> {
		let key = cache::tx::key::ConfigsCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::cg::prefix(ns, db)?;
				let end = crate::key::database::cg::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let lookup = cache::tx::key::ModelCacheKeyRef(ns, db, ml, vn);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ml_key = crate::key::database::ml::new(ns, db, ml, vn);
				let Some(val) = self.get(&ml_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific analyzer definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_analyzer(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &str,
	) -> Result<Arc<catalog::AnalyzerDefinition>> {
		let lookup = cache::tx::key::AnalyzerCacheKeyRef(ns, db, az);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let az_key = crate::key::database::az::new(ns, db, az);
				let val = self.get(&az_key, None).await?.ok_or_else(|| Error::AzNotFound {
					name: az.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific sequence definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Result<Arc<catalog::SequenceDefinition>> {
		let lookup = cache::tx::key::SequenceCacheKeyRef(ns, db, sq);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let sq_key = Sq::new(ns, db, sq);
				let val = self.get(&sq_key, None).await?.ok_or_else(|| Error::SeqNotFound {
					name: sq.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::FunctionCacheKeyRef(ns, db, fc);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let fc_key = crate::key::database::fc::new(ns, db, fc);
				let val = self.get(&fc_key, None).await?.ok_or_else(|| Error::FcNotFound {
					name: fc.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific module definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_module(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &str,
	) -> Result<Arc<catalog::ModuleDefinition>> {
		let lookup = cache::tx::key::ModuleCacheKeyRef(ns, db, md);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let md_key = crate::key::database::md::new(ns, db, md);
				let val = self.get(&md_key, None).await?.ok_or_else(|| Error::MdNotFound {
					name: md.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific function definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_param(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &str,
	) -> Result<Arc<catalog::ParamDefinition>> {
		let lookup = cache::tx::key::ParamCacheKeyRef(ns, db, pa);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let pa_key = crate::key::database::pa::new(ns, db, pa);
				let val = self.get(&pa_key, None).await?.ok_or_else(|| Error::PaNotFound {
					name: pa.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific config definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_config(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &str,
	) -> Result<Option<Arc<ConfigDefinition>>> {
		let lookup = cache::tx::key::ConfigCacheKeyRef(ns, db, cg);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let cg_key = crate::key::database::cg::new(ns, db, cg);
				if let Some(val) = self.get(&cg_key, None).await? {
					let val = Arc::new(val);
					self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
					Ok(Some(val))
				} else {
					Ok(None)
				}
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
		let key = cache::tx::key::TablesCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::tb::prefix(ns, db)?;
				let end = crate::key::database::tb::suffix(ns, db)?;
				let val = self.getr(beg..end, version).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		tb: &TableName,
	) -> Result<Arc<[TableDefinition]>> {
		let lookup = cache::tx::key::TableViewsCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::table::ft::prefix(ns, db, tb)?;
				let end = crate::key::table::ft::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self, ctx))]
	async fn get_or_add_tb(
		&self,
		ctx: Option<&Context>,
		ns: &str,
		db: &str,
		tb: &TableName,
	) -> Result<Arc<TableDefinition>> {
		let lookup = cache::tx::key::TableByNameCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			// The entry is in the cache
			Some(val) => Ok(val),
			// The entry is not in the cache
			None => {
				let Some(db_def) = self.get_db_by_name(ns, db).await? else {
					return Err(anyhow::anyhow!(Error::DbNotFound {
						name: db.to_owned(),
					}));
				};

				let table_key =
					crate::key::database::tb::new(db_def.namespace_id, db_def.database_id, tb);
				if let Some(tb_def) = self.get(&table_key, None).await? {
					let cached_tb = Arc::new(tb_def);
					self.cache.insert(lookup.to_owned_key(), Arc::clone(&cached_tb));
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
					tb.clone(),
				);
				self.put_tb(ns, db, &tb_def).await
			}
		}
	}

	async fn get_tb_by_name(
		&self,
		ns: &str,
		db: &str,
		tb: &TableName,
	) -> Result<Option<Arc<TableDefinition>>> {
		let lookup = cache::tx::key::TableByNameCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let Some(db) = self.get_db_by_name(ns, db).await? else {
					return Ok(None);
				};

				let tb_key = crate::key::database::tb::new(db.namespace_id, db.database_id, tb);
				let Some(tb) = self.get(&tb_key, None).await? else {
					return Ok(None);
				};

				let tb = Arc::new(tb);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&tb));
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
		match self.set(&key, tb, None).await {
			Ok(_) => {}
			Err(e) => {
				if matches!(
					e.downcast_ref(),
					Some(Error::Kvs(crate::kvs::Error::TransactionReadonly))
				) {
					return Err(Error::TbNotFound {
						name: tb.name.clone(),
					}
					.into());
				}
				return Err(e);
			}
		}

		// Populate cache
		let cached_tb = Arc::new(tb.clone());

		let lookup1 =
			cache::tx::key::TableCacheKeyRef(tb.namespace_id, tb.database_id, tb.name.as_str());
		self.cache.insert(lookup1.to_owned_key(), Arc::clone(&cached_tb));

		let lookup2 = cache::tx::key::TableByNameCacheKeyRef(ns, db, tb.name.as_str());
		self.cache.insert(lookup2.to_owned_key(), Arc::clone(&cached_tb));

		Ok(cached_tb)
	}

	async fn del_tb(&self, ns: &str, db: &str, tb: &TableName) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.clone(),
			}
			.into());
		};

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.del(&key).await?;

		// Clear the cache
		let lookup1 =
			cache::tx::key::TableCacheKeyRef(tb.namespace_id, tb.database_id, tb.name.as_str());
		self.cache.remove(&lookup1.to_owned_key());
		let lookup2 = cache::tx::key::TableByNameCacheKeyRef(ns, db, tb.name.as_str());
		self.cache.remove(&lookup2.to_owned_key());

		Ok(())
	}

	async fn clr_tb(&self, ns: &str, db: &str, tb: &TableName) -> Result<()> {
		let Some(tb) = self.get_tb_by_name(ns, db, tb).await? else {
			return Err(Error::TbNotFound {
				name: tb.clone(),
			}
			.into());
		};

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		self.clr(&key).await?;

		// Clear the cache
		let lookup1 =
			cache::tx::key::TableCacheKeyRef(tb.namespace_id, tb.database_id, tb.name.as_str());
		self.cache.remove(&lookup1.to_owned_key());
		let lookup2 = cache::tx::key::TableByNameCacheKeyRef(ns, db, tb.name.as_str());
		self.cache.remove(&lookup2.to_owned_key());

		Ok(())
	}

	/// Retrieve all event definitions for a specific table.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_tb_events(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Arc<[catalog::EventDefinition]>> {
		let lookup = cache::tx::key::TableEventsCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::table::ev::prefix(ns, db, tb)?;
				let end = crate::key::table::ev::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
		version: Option<u64>,
	) -> Result<Arc<[catalog::FieldDefinition]>> {
		let lookup = cache::tx::key::TableFieldsCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::table::fd::prefix(ns, db, tb)?;
				let end = crate::key::table::fd::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, version).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
	) -> Result<Arc<[catalog::IndexDefinition]>> {
		let lookup = cache::tx::key::TableIndexesCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::table::ix::prefix(ns, db, tb)?;
				let end = crate::key::table::ix::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
	) -> Result<Arc<[catalog::SubscriptionDefinition]>> {
		let lookup = cache::tx::key::TableLivesCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::table::lq::prefix(ns, db, tb)?;
				let end = crate::key::table::lq::suffix(ns, db, tb)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
	) -> Result<Option<Arc<TableDefinition>>> {
		let lookup = cache::tx::key::TableCacheKeyRef(ns, db, tb.as_str());
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let tb_key = crate::key::database::tb::new(ns, db, tb);
				let Some(val) = self.get(&tb_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
		ev: &str,
	) -> Result<Arc<catalog::EventDefinition>> {
		let lookup = cache::tx::key::EventCacheKeyRef(ns, db, tb.as_str(), ev);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let ev_key = crate::key::table::ev::new(ns, db, tb, ev);
				let val = self.get(&ev_key, None).await?.ok_or_else(|| Error::EvNotFound {
					name: ev.to_owned(),
				})?;
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		tb: &TableName,
		fd: &str,
	) -> Result<Option<Arc<catalog::FieldDefinition>>> {
		let lookup = cache::tx::key::FieldCacheKeyRef(ns, db, tb.as_str(), fd);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let fd_key = crate::key::table::fd::new(ns, db, tb, fd);
				let Some(val) = self.get(&fd_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	async fn put_tb_field(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
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
		tb: &TableName,
		ix: &str,
	) -> Result<Option<Arc<catalog::IndexDefinition>>> {
		let lookup = cache::tx::key::IndexCacheKeyRef(ns, db, tb.as_str(), ix);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ix_key = crate::key::table::ix::new(ns, db, tb, ix);
				let Some(val) = self.get(&ix_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	async fn get_tb_index_by_id(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
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
		tb: &TableName,
		ix: &catalog::IndexDefinition,
	) -> Result<()> {
		let key = crate::key::table::ix::new(ns, db, tb, &ix.name);
		self.set(&key, ix, None).await?;

		let name_lookup_key =
			crate::key::table::ix::IndexNameLookupKey::new(ns, db, tb, ix.index_id);
		self.set(&name_lookup_key, &ix.name, None).await?;

		// Set the entry in the cache
		let lookup = cache::tx::key::IndexCacheKeyRef(ns, db, tb.as_str(), &ix.name);
		self.cache.insert(lookup.to_owned_key(), Arc::new(ix.clone()));
		Ok(())
	}

	async fn del_tb_index(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
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
		tb: &TableName,
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
			let lookup = cache::tx::key::RecordCacheKeyRef(ns, db, tb.as_str(), id);
			match self.cache.get_clone_by(&lookup) {
				// The entry is in the cache
				Some(val) => Ok(val),
				// The entry is not in the cache
				None => {
					// Fetch the record from the datastore
					let rec_key = crate::key::record::new(ns, db, tb, id);
					match self.get(&rec_key, None).await? {
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
							self.cache.insert(lookup.to_owned_key(), Arc::clone(&record));
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
		tb: &TableName,
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
		tb: &TableName,
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
		tb: &TableName,
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
		tb: &TableName,
		id: &RecordIdKey,
	) -> Result<()> {
		// Delete the value in the datastore
		let key = crate::key::record::new(ns, db, tb, id);
		self.del(&key).await?;
		// Clear the value from the cache
		let lookup = cache::tx::key::RecordCacheKeyRef(ns, db, tb.as_str(), id);
		self.cache.remove(&lookup.to_owned_key());
		// Return nothing
		Ok(())
	}

	async fn get_next_tb_id(
		&self,
		ctx: Option<&Context>,
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
		let key = cache::tx::key::RootUsersCacheKey;
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::root::us::prefix();
				let end = crate::key::root::us::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns_users(&self, ns: NamespaceId) -> Result<Arc<[catalog::UserDefinition]>> {
		let key = cache::tx::key::NamespaceUsersCacheKey(ns);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::namespace::us::prefix(ns)?;
				let end = crate::key::namespace::us::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let key = cache::tx::key::DatabaseUsersCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::us::prefix(ns, db)?;
				let end = crate::key::database::us::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific root user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_user(&self, us: &str) -> Result<Option<Arc<catalog::UserDefinition>>> {
		let lookup = cache::tx::key::RootUserCacheKeyRef(us);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let us_key = crate::key::root::us::new(us);
				let Some(val) = self.get(&us_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific namespace user definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_ns_user(
		&self,
		ns: NamespaceId,
		us: &str,
	) -> Result<Option<Arc<catalog::UserDefinition>>> {
		let lookup = cache::tx::key::NamespaceUserCacheKeyRef(ns, us);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let us_key = crate::key::namespace::us::new(ns, us);
				let Some(val) = self.get(&us_key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	/// Retrieve a specific user definition from a database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_user(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &str,
	) -> Result<Option<Arc<catalog::UserDefinition>>> {
		let lookup = cache::tx::key::DatabaseUserCacheKeyRef(ns, db, us);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let us_key = crate::key::database::us::new(ns, db, us);
				let Some(val) = self.get(&us_key, None).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
			}
		}
	}

	async fn put_root_user(&self, us: &catalog::UserDefinition) -> Result<()> {
		let key = crate::key::root::us::new(&us.name);
		self.set(&key, us, None).await?;
		Ok(())
	}

	async fn put_ns_user(&self, ns: NamespaceId, us: &catalog::UserDefinition) -> Result<()> {
		let key = crate::key::namespace::us::new(ns, &us.name);
		self.set(&key, us, None).await?;
		Ok(())
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
		let key = cache::tx::key::RootAccessesCacheKey;
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::root::ac::prefix();
				let end = crate::key::root::ac::suffix();
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve all root access grants in a datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_root_access_grants(&self, ra: &str) -> Result<Arc<[catalog::AccessGrant]>> {
		let lookup = cache::tx::key::RootAccessGrantsCacheKeyRef(ra);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::root::access::gr::prefix(ra)?;
				let end = crate::key::root::access::gr::suffix(ra)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn all_ns_accesses(&self, ns: NamespaceId) -> Result<Arc<[catalog::AccessDefinition]>> {
		let key = cache::tx::key::NamespaceAccessesCacheKey(ns);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::namespace::ac::prefix(ns)?;
				let end = crate::key::namespace::ac::suffix(ns)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let lookup = cache::tx::key::NamespaceAccessGrantsCacheKeyRef(ns, na);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::namespace::access::gr::prefix(ns, na)?;
				let end = crate::key::namespace::access::gr::suffix(ns, na)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let key = cache::tx::key::DatabaseAccessesCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::ac::prefix(ns, db)?;
				let end = crate::key::database::ac::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let lookup = cache::tx::key::DatabaseAccessGrantsCacheKeyRef(ns, db, da);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::access::gr::prefix(ns, db, da)?;
				let end = crate::key::database::access::gr::suffix(ns, db, da)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific root access definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_root_access(&self, ra: &str) -> Result<Option<Arc<catalog::AccessDefinition>>> {
		let lookup = cache::tx::key::RootAccessCacheKeyRef(ra);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ac_key = crate::key::root::ac::new(ra);
				let Some(val) = self.get(&ac_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::RootAccessGrantCacheKeyRef(ac, gr);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let gr_key = crate::key::root::access::gr::new(ac, gr);
				let Some(val) = self.get(&gr_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::NamespaceAccessCacheKeyRef(ns, na);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ac_key = crate::key::namespace::ac::new(ns, na);
				let Some(val) = self.get(&ac_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::NamespaceAccessGrantCacheKeyRef(ns, ac, gr);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let gr_key = crate::key::namespace::access::gr::new(ns, ac, gr);
				let Some(val) = self.get(&gr_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::DatabaseAccessCacheKeyRef(ns, db, da);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ac_key = crate::key::database::ac::new(ns, db, da);
				let Some(val) = self.get(&ac_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let lookup = cache::tx::key::DatabaseAccessGrantCacheKeyRef(ns, db, ac, gr);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let gr_key = crate::key::database::access::gr::new(ns, db, ac, gr);
				let Some(val) = self.get(&gr_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
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
		let key = cache::tx::key::ApisCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::ap::prefix(ns, db)?;
				let end = crate::key::database::ap::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
				Ok(val)
			}
		}
	}

	/// Retrieve a specific api definition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	async fn get_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &str,
	) -> Result<Option<Arc<ApiDefinition>>> {
		let lookup = cache::tx::key::ApiCacheKeyRef(ns, db, ap);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let ap_key = crate::key::database::ap::new(ns, db, ap);
				let Some(val) = self.get(&ap_key, None).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&val));
				Ok(Some(val))
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
		let key = cache::tx::key::BucketsCacheKey(ns, db);
		match self.cache.get_clone(&key) {
			Some(val) => Ok(val),
			None => {
				let beg = crate::key::database::bu::prefix(ns, db)?;
				let end = crate::key::database::bu::suffix(ns, db)?;
				let val = self.getr(beg..end, None).await?;
				let val = util::deserialize_cache(val.iter().map(|x| x.1.as_slice()))?;
				self.cache.insert(key, Arc::clone(&val));
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
		let lookup = cache::tx::key::BucketCacheKeyRef(ns, db, bu);
		match self.cache.get_clone_by(&lookup) {
			Some(val) => Ok(Some(val)),
			None => {
				let bu_key = crate::key::database::bu::new(ns, db, bu);
				let Some(val) = self.get(&bu_key, None).await? else {
					return Ok(None);
				};
				let bucket_def = Arc::new(val);
				self.cache.insert(lookup.to_owned_key(), Arc::clone(&bucket_def));
				Ok(Some(bucket_def))
			}
		}
	}
}

impl CatalogProvider for Transaction {}
