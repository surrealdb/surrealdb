use std::fmt;
use std::fmt::Debug;

use surrealdb_kvs::api::{
	Batch, GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanResult, Transactable,
};
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{Direction, Key, KeyRange};

use crate::{IntoBytes, Result, Val};

/// A set of undoable updates and requests against a dataset.
pub struct Transactor {
	// The underlying transaction
	pub inner: Box<dyn Transactable>,
}

impl fmt::Display for Transactor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.kind())
	}
}

impl Drop for Transactor {
	fn drop(&mut self) {
		if !self.closed() && self.writeable() {
			// A test may drop a transaction deliberately, so this is only a
			// warning there; anywhere else it is a leak.
			#[cfg(any(test, feature = "test-hooks"))]
			warn!("A transaction was dropped without being committed or cancelled");
			#[cfg(not(any(test, feature = "test-hooks")))]
			error!("A transaction was dropped without being committed or cancelled");
		}
	}
}

impl Transactor {
	/// Get the underlying datastore kind.
	pub fn kind(&self) -> &'static str {
		self.inner.kind()
	}

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`surrealdb_kvs::Error::TransactionFinished`] error.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn closed(&self) -> bool {
		self.inner.closed()
	}

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This fuction can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return a [`surrealdb_kvs::Error::TransactionReadonly`] error when
	/// attempting to modify any data within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn writeable(&self) -> bool {
		self.inner.writeable()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn cancel(&self) -> Result<()> {
		self.inner.cancel().await
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn commit(&self) -> Result<()> {
		self.inner.commit().await
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn exists(&self, key: Key<'_>, version: Option<u64>) -> Result<bool> {
		self.inner.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn get(&self, key: Key<'_>, version: Option<u64>) -> Result<Option<Val>> {
		self.inner.get(key, version).await
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getm(&self, keys: &[Key<'_>], version: Option<u64>) -> Result<GetMultiResult> {
		self.inner.getm(keys, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getr(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<ScanResult> {
		self.inner.getr(rng, version).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn set<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		self.inner.set(key, val).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn replace<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		self.inner.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn put<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		self.inner.put(key, val).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn putc<V>(&self, key: Key<'_>, val: V, chk: Option<V>) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		let chk = chk.map(|v| v.into_vec());
		self.inner.putc(key, val, chk).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		self.inner.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delc(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		self.inner.delc(key, chk).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delr(&self, rng: KeyRange<'_>) -> Result<()> {
		self.inner.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clr(&self, key: Key<'_>) -> Result<()> {
		self.inner.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrc(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		self.inner.clrc(key, chk).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrr(&self, rng: KeyRange<'_>) -> Result<()> {
		self.inner.clrr(rng).await
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. The returned [`Key<'_>sResult`] also
	/// reports the total key bytes scanned, accumulated by the backend during
	/// the same iteration that produced the keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn keys(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<KeysResult> {
		if rng.is_empty() {
			return Ok(KeysResult::default());
		}
		self.inner.keys(rng, limit, skip, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. The returned [`Key<'_>sResult`] also
	/// reports the total key bytes scanned, accumulated by the backend during
	/// the same iteration that produced the keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn keysr(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<KeysResult> {
		if rng.is_empty() {
			return Ok(KeysResult::default());
		}
		self.inner.keysr(rng, limit, skip, version).await
	}

	/// Retrieve a specific range of key-value pairs from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. The returned [`ScanResult`] also
	/// reports the total value bytes scanned, accumulated by the backend
	/// during the same iteration that produced the values.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn scan(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<ScanResult> {
		if rng.is_empty() {
			return Ok(ScanResult::default());
		}
		self.inner.scan(rng, limit, skip, version).await
	}

	/// Retrieve a specific range of key-value pairs from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. The returned [`ScanResult`] also
	/// reports the total value bytes scanned, accumulated by the backend
	/// during the same iteration that produced the values.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn scanr(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<ScanResult> {
		if rng.is_empty() {
			return Ok(ScanResult::default());
		}
		self.inner.scanr(rng, limit, skip, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn count(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<usize> {
		self.inner.count(rng, version).await
	}

	// --------------------------------------------------
	// Cursor functions
	// --------------------------------------------------

	/// Open a stateful keys-only scan cursor over a range.
	///
	/// The cursor lives for the duration of one logical scan (e.g. an
	/// outer table walk or one prefix of a graph-edge traversal). Each
	/// `next_batch` call advances the same underlying iterator instead of
	/// re-seeking from scratch, which is the primary cost on RocksDB
	/// paged scans. `skip` is applied once on the first batch. See
	/// [`ScanCursorKey<'_>s`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<Box<dyn ScanCursorKeys + 'a>> {
		self.inner.open_keys_cursor(rng, dir, skip, version).await
	}

	/// Open a stateful key+value scan cursor over a range. See
	/// [`Self::open_keys_cursor`] for the rationale.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<Box<dyn ScanCursorVals + 'a>> {
		self.inner.open_vals_cursor(rng, dir, skip, version).await
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys, in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn batch_keys(
		&self,
		rng: KeyRange<'_>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		self.inner.batch_keys(rng, batch, version).await
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn batch_keys_vals(
		&self,
		rng: KeyRange<'_>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		self.inner.batch_keys_vals(rng, batch, version).await
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	pub async fn new_save_point(&self) -> Result<()> {
		self.inner.new_save_point().await
	}

	/// Release the last save point.
	pub async fn release_last_save_point(&self) -> Result<()> {
		self.inner.release_last_save_point().await
	}

	/// Rollback to the last save point.
	pub async fn rollback_to_save_point(&self) -> Result<()> {
		self.inner.rollback_to_save_point().await
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	pub async fn timestamp(&self) -> Result<BoxTimeStamp> {
		self.inner.timestamp().await
	}

	/// Get the current safe (closed) watermark timestamp — the versionstamp at or
	/// below which every committed transaction is final and visible. Defaults to
	/// the monotonic timestamp; distributed backends override it.
	pub async fn safe_timestamp(&self) -> Result<BoxTimeStamp> {
		self.inner.safe_timestamp().await
	}

	/// Returns the implementation of timestamp that this transaction uses.
	pub fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.inner.timestamp_impl()
	}
}
