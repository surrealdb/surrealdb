use std::fmt;
use std::fmt::Debug;
use std::ops::Range;

use futures::stream::Stream;

use super::Result;
use super::api::Transactable;
use super::batch::Batch;
use super::scanner::{Direction, Scanner};
use super::{IntoBytes, Key, Val, Version};

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
	pub(super) inner: Box<dyn Transactable>,
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
	/// in a [`kvs::Error::TransactionFinished`] error.
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
	/// will return a [`kvs::Error::TransactionReadonly`] error w
	/// hen attempting to modify any data within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) fn writeable(&self) -> bool {
		self.inner.writeable()
	}

	/// Get the current monotonic timestamp
	pub(crate) async fn timestamp(&self) -> Result<Box<dyn super::Timestamp>> {
		self.inner.timestamp().await
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
	pub async fn exists<K>(&self, key: K, version: Option<u64>) -> Result<bool>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn get<K>(&self, key: K, version: Option<u64>) -> Result<Option<Val>>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.get(key, version).await
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<Val>>>
	where
		K: IntoBytes + Debug,
	{
		let keys = keys.iter().map(IntoBytes::into_vec).collect();
		self.inner.getm(keys).await
	}

	/// Retrieve a specific prefixed range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getp<K>(&self, key: K) -> Result<Vec<(Key, Val)>>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.getp(key).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getr<K>(&self, rng: Range<K>, version: Option<u64>) -> Result<Vec<(Key, Val)>>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.getr(beg..end, version).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn set<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let val = val.into_vec();
		self.inner.set(key, val, version).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn replace<K, V>(&self, key: K, val: V) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let val = val.into_vec();
		self.inner.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn put<K, V>(&self, key: K, val: V, version: Option<u64>) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let val = val.into_vec();
		self.inner.put(key, val, version).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn putc<K, V>(&self, key: K, val: V, chk: Option<V>) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let val = val.into_vec();
		let chk = chk.map(|v| v.into_vec());
		self.inner.putc(key, val, chk).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn del<K>(&self, key: K) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delc<K, V>(&self, key: K, chk: Option<V>) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let chk = chk.map(|v| v.into_vec());
		self.inner.delc(key, chk).await
	}

	/// Delete a prefixed range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delp<K>(&self, key: K) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.delp(key).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.delr(beg..end).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clr<K>(&self, key: K) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrc<K, V>(&self, key: K, chk: Option<V>) -> Result<()>
	where
		K: IntoBytes + Debug,
		V: IntoBytes + Debug,
	{
		let key = key.into_vec();
		let chk = chk.map(|v| v.into_vec());
		self.inner.clrc(key, chk).await
	}

	/// Delete all versions of a prefixed range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrp<K>(&self, key: K) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let key = key.into_vec();
		self.inner.clrp(key).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrr<K>(&self, rng: Range<K>) -> Result<()>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.clrr(beg..end).await
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn keys<K>(&self, rng: Range<K>, limit: u32, version: Option<u64>) -> Result<Vec<Key>>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		if beg > end {
			return Ok(vec![]);
		}
		self.inner.keys(beg..end, limit, version).await
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
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		if beg > end {
			return Ok(vec![]);
		}
		self.inner.keysr(beg..end, limit, version).await
	}

	/// Retrieve a specific range of key-value pairs from the datastore.
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
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		if beg > end {
			return Ok(vec![]);
		}
		self.inner.scan(beg..end, limit, version).await
	}

	/// Retrieve a specific range of key-value pairs from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn scanr<K>(
		&self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		if beg > end {
			return Ok(vec![]);
		}
		self.inner.scanr(beg..end, limit, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn count<K>(&self, rng: Range<K>) -> Result<usize>
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.count(beg..end).await
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

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
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.batch_keys(beg..end, batch, version).await
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
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.batch_keys_vals(beg..end, batch, version).await
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
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		self.inner.batch_keys_vals_versions(beg..end, batch).await
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
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn stream_keys<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: Direction,
	) -> impl Stream<Item = Result<Key>> + '_
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		Scanner::<Key>::new(self, beg..end, version, limit, dir, true)
	}

	/// Retrieve a stream over a specific range of key-value pairs in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore. The Scanner uses adaptive batch
	/// sizing, starting at 100 items and doubling up to MAX_BATCH_SIZE.
	/// Prefetching is enabled by default for optimal read throughput.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn stream_keys_vals<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: Direction,
	) -> impl Stream<Item = Result<(Key, Val)>> + '_
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		Scanner::<(Key, Val)>::new(self, beg..end, version, limit, dir, true)
	}

	/// Retrieve a stream over a specific range of keys in the datastore without
	/// prefetching.
	///
	/// This variant disables prefetching, making it more suitable for scenarios
	/// where each key will be processed with write operations (e.g., delete, update)
	/// and prefetching would waste work on errors.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn stream_keys_no_prefetch<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: Direction,
	) -> impl Stream<Item = Result<Key>> + '_
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		Scanner::<Key>::new(self, beg..end, version, limit, dir, false)
	}

	/// Retrieve a stream over a specific range of keys in the datastore without
	/// prefetching.
	///
	/// This variant disables prefetching, making it more suitable for scenarios
	/// where each key will be processed with write operations (e.g., delete, update)
	/// and prefetching would waste work on errors.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub fn stream_keys_vals_no_prefetch<K>(
		&self,
		rng: Range<K>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: Direction,
	) -> impl Stream<Item = Result<(Key, Val)>> + '_
	where
		K: IntoBytes + Debug,
	{
		let beg = rng.start.into_vec();
		let end = rng.end.into_vec();
		Scanner::<(Key, Val)>::new(self, beg..end, version, limit, dir, false)
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
}
