use std::fmt;
use std::fmt::Debug;
use std::ops::Range;

use anyhow::Result;

#[allow(unused_imports, reason = "Not used when none of the storage backends are enabled.")]
use super::api::Transaction;
use super::{Key, Val, Version};
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::cf;
use crate::doc::CursorRecord;
use crate::key::database::vs::VsKey;
use crate::key::debug::Sprintable;
use crate::kvs::KVValue;
use crate::kvs::batch::Batch;
use crate::kvs::key::KVKey;
use crate::val::RecordId;
use crate::vs::VersionStamp;

const TARGET: &str = "surrealdb::core::kvs::tr";

/// Specifies whether the transaction is read-only or writeable.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum TransactionType {
	Read,
	Write,
}

/// Specifies whether the transaction is optimistic or pessimistic.
#[derive(Copy, Clone)]
pub enum LockType {
	Pessimistic,
	Optimistic,
}

impl From<bool> for LockType {
	fn from(value: bool) -> Self {
		match value {
			true => LockType::Pessimistic,
			false => LockType::Optimistic,
		}
	}
}

/// A set of undoable updates and requests against a dataset.
pub struct Transactor {
	// The underlying transaction
	pub(super) inner: Box<dyn super::api::Transaction>,
	// The changefeed buffer
	pub(super) cf: cf::Writer,
}

impl fmt::Display for Transactor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.kind())
	}
}

impl Drop for Transactor {
	fn drop(&mut self) {
		if !self.closed() && self.writeable() {
			// Warn when running in test mode
			#[cfg(test)]
			warn!("A transaction was dropped without being committed or cancelled");
			// Panic when running in debug mode
			#[cfg(not(test))]
			#[cfg(debug_assertions)]
			panic!("A transaction was dropped without being committed or cancelled");
			// Error when running in release mode
			#[cfg(not(test))]
			#[cfg(not(debug_assertions))]
			error!("A transaction was dropped without being committed or cancelled");
		}
	}
}

impl Transactor {
	/// Get the underlying datastore kind.
	fn kind(&self) -> &'static str {
		self.inner.kind()
	}

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`Error::TxFinished`] error.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) fn closed(&self) -> bool {
		self.inner.closed()
	}

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This fuction can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return an [`Error::TxReadonly`] error when attempting
	/// to modify any data within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) fn writeable(&self) -> bool {
		self.inner.writeable()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) async fn cancel(&self) -> Result<()> {
		self.inner.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) async fn commit(&self) -> Result<()> {
		self.inner.commit().await
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn exists<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), version = version, "Exists");
		self.inner.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn get<K>(&self, key: &K, version: Option<u64>) -> Result<Option<K::ValueType>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), version = version, "Get");
		let bytes = self.inner.get(key, version).await?;
		bytes.map(K::ValueType::kv_decode_value).transpose()
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<K::ValueType>>>
	where
		K: KVKey + Debug,
	{
		let keys = keys.iter().map(|k| k.encode_key()).collect::<Result<Vec<_>>>()?;
		trace!(target: TARGET, keys = keys.sprint(), "GetM");
		let vals = self.inner.getm(keys).await?;

		vals.into_iter()
			.map(|v| match v {
				Some(v) => K::ValueType::kv_decode_value(v).map(Some),
				None => Ok(None),
			})
			.collect()
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getr<K>(&self, rng: Range<K>, version: Option<u64>) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), version = version, "GetR");
		self.inner.getr(rng, version).await
	}

	/// Retrieve a specific prefixed range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getp<K>(&self, key: &K) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "GetP");
		self.inner.getp(key).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn set<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), version = version, "Set");
		self.inner.set(key, val.kv_encode_value()?, version).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn replace<K>(&self, key: &K, val: &K::ValueType) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "Replace");
		self.inner.replace(key, val.kv_encode_value()?).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn put<K>(&self, key: &K, val: &K::ValueType, version: Option<u64>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), version = version, "Put");
		self.inner.put(key, val.kv_encode_value()?, version).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
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
		trace!(target: TARGET, key = key.sprint(), "PutC");
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.inner.putc(key, val.kv_encode_value()?, chk).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn del<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "Del");
		self.inner.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "DelC");
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.inner.delc(key, chk).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), "DelR");
		self.inner.delr(rng).await
	}

	/// Delete a prefixed range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "DelP");
		self.inner.delp(key).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clr<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "Clr");
		self.inner.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrc<K>(&self, key: &K, chk: Option<&K::ValueType>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "ClrC");
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.inner.clrc(key, chk).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), "ClrR");
		self.inner.clrr(rng).await
	}

	/// Delete all versions of a prefixed range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrp<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key: Key = key.encode_key()?;
		trace!(target: TARGET, key = key.sprint(), "ClrP");
		self.inner.clrp(key).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn keys<K>(&self, rng: Range<K>, limit: u32, version: Option<u64>) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), limit = limit, version = version, "Keys");
		if rng.start > rng.end {
			return Ok(vec![]);
		}
		self.inner.keys(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn keysr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), limit = limit, version = version, "Keysr");
		if rng.start > rng.end {
			return Ok(vec![]);
		}
		self.inner.keysr(rng, limit, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn scan<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), limit = limit, version = version, "Scan");
		if rng.start > rng.end {
			return Ok(vec![]);
		}
		self.inner.scan(rng, limit, version).await
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn scanr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), limit = limit, version = version, "Scanr");
		if rng.start > rng.end {
			return Ok(vec![]);
		}
		self.inner.scanr(rng, limit, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn count<K>(&self, rng: Range<K>) -> Result<usize>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), "Count");
		self.inner.count(rng).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys, in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn batch_keys<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Key>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), version = version, "Batch");
		self.inner.batch_keys(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn batch_keys_vals<K>(
		&self,
		rng: Range<K>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Key, Val)>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), version = version, "Batch");
		self.inner.batch_keys_vals(rng, batch, version).await
	}

	/// Retrieve a batched scan of all versions over a specific range of keys in
	/// the datastore.
	///
	/// This function fetches key-value-version pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn batch_keys_vals_versions<K>(
		&self,
		rng: Range<K>,
		batch: u32,
	) -> Result<Batch<(Key, Val, Version, bool)>>
	where
		K: KVKey + Debug,
	{
		let beg: Key = rng.start.encode_key()?;
		let end: Key = rng.end.encode_key()?;
		let rng = beg..end;
		trace!(target: TARGET, rng = rng.sprint(), "BatchVersions");
		self.inner.batch_keys_vals_versions(rng, batch).await
	}

	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is
	/// committed. NOTE: This should be called when composing the change feed
	/// entries for this transaction, which should be done immediately before
	/// the transaction commit. That is to keep other transactions commit
	/// delay(pessimistic) or conflict(optimistic) as less as possible.
	pub(crate) async fn get_versionstamp(&self, key: VsKey) -> Result<VersionStamp> {
		self.inner.get_versionstamp(key).await
	}

	/// Insert or update a key in the datastore.
	pub(crate) async fn set_versionstamp<K>(
		&self,
		ts_key: VsKey,
		prefix: K,
		suffix: K,
		val: K::ValueType,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let prefix = prefix.encode_key()?;
		let suffix = suffix.encode_key()?;
		let value = val.kv_encode_value()?;
		self.inner.set_versionstamp(ts_key, prefix, suffix, value).await
	}
}

// --------------------------------------------------
// Savepoint functions
// --------------------------------------------------

impl Transactor {
	/// Set a new save point on the transaction.
	pub(crate) async fn new_save_point(&self) -> Result<()> {
		self.inner.new_save_point().await
	}

	/// Release the last save point.
	pub(crate) async fn release_last_save_point(&self) -> Result<()> {
		self.inner.release_last_save_point().await
	}

	/// Rollback to the last save point.
	pub(crate) async fn rollback_to_save_point(&self) -> Result<()> {
		self.inner.rollback_to_save_point().await
	}
}

// --------------------------------------------------
// Changefeed functions
// --------------------------------------------------

impl Transactor {
	// Records the table (re)definition in the changefeed if enabled.
	pub(crate) fn record_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		dt: &TableDefinition,
	) {
		self.cf.record_table_change(ns, db, tb, dt)
	}

	// change will record the change in the changefeed if enabled.
	// To actually persist the record changes into the underlying kvs,
	// you must call the `complete_changes` function and then commit the
	// transaction.
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordId,
		previous: CursorRecord,
		current: CursorRecord,
		store_difference: bool,
	) {
		self.cf.record_change(ns, db, tb, id.clone(), previous, current, store_difference)
	}

	// complete_changes will complete the changefeed recording for the given
	// namespace and database.
	//
	// Under the hood, this function calls the transaction's
	// `set_versionstamped_key` for each change. Every change must be recorded by
	// calling this struct's `record_change` function beforehand. If there were no
	// preceding `record_change` function calls for this transaction, this function
	// will do nothing.
	//
	// This function should be called only after all the changes have been made to
	// the transaction. Otherwise, changes are missed in the change feed.
	//
	// This function should be called immediately before calling the commit function
	// to guarantee that the lock, if needed by lock=true, is held only for the
	// duration of the commit, not the entire transaction.
	//
	// This function is here because it needs access to mutably borrow the
	// transaction.
	//
	// Lastly, you should set lock=true if you want the changefeed to be correctly
	// ordered for non-FDB backends.
	pub(crate) async fn complete_changes(&self, _lock: bool) -> Result<()> {
		let changes = self.cf.get()?;
		for (tskey, prefix, suffix, v) in changes {
			self.set_versionstamp(tskey, prefix, suffix, v).await?
		}
		Ok(())
	}

	// set_timestamp_for_versionstamp correlates the given timestamp with the
	// current versionstamp. This allows get_versionstamp_from_timestamp to obtain
	// the versionstamp from the timestamp later.
	pub(crate) async fn set_timestamp_for_versionstamp(
		&self,
		ts: u64,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<VersionStamp> {
		// This also works as an advisory lock on the ts keys so that there is
		// on other concurrent transactions that can write to the ts_key or the keys
		// after it.
		let key = crate::key::database::vs::new(ns, db);
		let vst = self.get_versionstamp(key).await?;
		trace!(
			target: TARGET,
			"Setting timestamp {} for versionstamp {:?} in ns: {}, db: {}",
			ts,
			vst.into_u64_lossy(),
			ns,
			db
		);

		// Ensure there are no keys after the ts_key
		// Otherwise we can go back in time!
		let mut ts_key = crate::key::database::ts::new(ns, db, ts);
		let beg = ts_key.encode_key()?;
		let end = crate::key::database::ts::suffix(ns, db)?;
		let ts_pairs: Vec<(Vec<u8>, Vec<u8>)> = self.getr(beg..end, None).await?;
		let latest_ts_pair = ts_pairs.last();
		if let Some((k, _)) = latest_ts_pair {
			trace!(
				target: TARGET,
				"There already was a greater committed timestamp {} in ns: {}, db: {} found: {}",
				ts,
				ns,
				db,
				k.sprint()
			);
			let k = crate::key::database::ts::Ts::decode_key(k)?;
			let latest_ts = k.ts;
			if latest_ts >= ts {
				warn!("ts {ts} is less than the latest ts {latest_ts}");
				ts_key = crate::key::database::ts::new(ns, db, latest_ts + 1);
			}
		}
		self.replace(&ts_key, &vst).await?;
		Ok(vst)
	}

	pub(crate) async fn get_versionstamp_from_timestamp(
		&self,
		ts: u64,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Option<VersionStamp>> {
		let ts_key = crate::key::database::ts::new(ns, db, ts + 1).encode_key()?;
		let beg = crate::key::database::ts::prefix(ns, db)?;
		let end = ts_key.encode_key()?;
		let ts = self.scanr(beg..end, 1, None).await?.pop().map(|x| x.1);
		if let Some(v) = ts {
			return Ok(Some(VersionStamp::from_slice(&v)?));
		}
		Ok(None)
	}
}
