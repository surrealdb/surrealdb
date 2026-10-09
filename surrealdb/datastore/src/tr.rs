use std::fmt;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use surrealdb_kvs::api::{
	Batch, GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanResult, Transactable,
};
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{Direction, Key, KeyRange};

use crate::speculation::{Held, HeldCursor, Overlay};
use crate::{IntoBytes, Result, Val};

/// A set of undoable updates and requests against a dataset.
pub struct Transactor {
	/// The underlying transaction.
	///
	/// Crate-private, because reaching it is reaching past what
	/// [`crate::Transaction`] wraps around it: the write-cardinality guard, the
	/// poison checks, the buffered index deltas and the save-point frames that
	/// mirror this transaction's own save-point stack. `Transaction` derefs to
	/// `Transactor`, so a public field here is a public bypass of all of them.
	pub(crate) inner: Box<dyn Transactable>,
	/// Whether this is a speculation's view of a transaction
	/// ([`crate::speculation`]): its writes land in [`Self::overlay`] and its
	/// reads of the current state see them. Fixed when the transactor is made,
	/// so no other transactor's reads ever pass through an overlay.
	pub(crate) speculating: bool,
	/// A speculation's writes; `None` once the speculation has ended, when
	/// the view refuses every write.
	pub(crate) overlay: Arc<Mutex<Option<Overlay>>>,
	/// How many writes the speculation has taken.
	pub(crate) writes: Arc<std::sync::atomic::AtomicU64>,
}

impl fmt::Display for Transactor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.kind())
	}
}

impl Drop for Transactor {
	fn drop(&mut self) {
		// A speculation's view commits nothing, and holds nothing to cancel.
		if !self.speculating && !self.closed() && self.writeable() {
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
	/// A transactor over `inner`, speculating nothing.
	pub(crate) fn new(inner: Box<dyn Transactable>) -> Self {
		Self {
			inner,
			speculating: false,
			overlay: Arc::new(Mutex::new(None)),
			writes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
		}
	}

	/// A speculation's view over `inner`, which reads the transaction it
	/// speculates on, holding the writes in `overlay`.
	pub(crate) fn speculative(
		inner: Box<dyn Transactable>,
		overlay: Arc<Mutex<Option<Overlay>>>,
	) -> Self {
		Self {
			inner,
			speculating: true,
			overlay,
			writes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
		}
	}

	/// Whether this is a speculation's view: reads and writes of the current
	/// state go through the speculation's overlay.
	#[inline]
	pub fn speculating(&self) -> bool {
		self.speculating
	}

	/// End the speculation, answering the writes it holds. The view refuses
	/// every write after, so a task still holding it once the speculation has
	/// ended reaches neither the overlay nor the storage.
	pub(crate) fn end_speculation(&self) -> Overlay {
		self.overlay
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.take()
			.unwrap_or_default()
	}

	/// What the speculation in progress answers for `key`.
	fn held(&self, key: &[u8]) -> Held {
		match self.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref() {
			Some(overlay) => overlay.get(key),
			None => Held::Storage,
		}
	}

	/// Whether the speculation in progress holds anything inside `rng`.
	fn touches(&self, rng: &KeyRange<'_>) -> bool {
		self.overlay
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.as_ref()
			.is_some_and(|overlay| overlay.touches(rng))
	}

	/// Take a write into the speculation's overlay, refused once the
	/// speculation has ended.
	fn overlay_mut(&self, f: impl FnOnce(&mut Overlay)) -> Result<()> {
		match self.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut() {
			Some(overlay) => {
				f(overlay);
				self.writes.fetch_add(1, std::sync::atomic::Ordering::Release);
				Ok(())
			}
			None => Err(crate::Error::TransactionFinished),
		}
	}

	/// Whether a read of the current state within `rng` must merge the
	/// speculation's writes.
	fn speculated(&self, rng: &KeyRange<'_>, version: Option<u64>) -> bool {
		version.is_none() && self.speculating() && self.touches(rng)
	}

	/// The current value of `key` as the speculation leaves it.
	async fn speculative_value(&self, key: &Key<'_>) -> Result<Option<Val>> {
		match self.held(key.as_slice()) {
			Held::Value(value) => Ok(Some(value)),
			Held::Deleted => Ok(None),
			Held::Storage => self.inner.get(key.as_borrowed(), None).await,
		}
	}

	/// The pairs inside `rng`, in key order, as the speculation leaves them.
	async fn speculative_pairs(
		&self,
		rng: &KeyRange<'_>,
		values: bool,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let stored = if values {
			self.inner.scan(rng.as_borrowed(), u32::MAX, 0, None).await?.values
		} else {
			self.inner
				.keys(rng.as_borrowed(), u32::MAX, 0, None)
				.await?
				.keys
				.into_iter()
				.map(|key| (key, Vec::new()))
				.collect()
		};
		let overlay = self.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		Ok(match overlay.as_ref() {
			Some(overlay) => overlay.merge(rng, stored),
			None => stored,
		})
	}

	/// Get the underlying datastore kind.
	pub fn kind(&self) -> &'static str {
		self.inner.kind()
	}

	/// Whether [`Self::count`] reads off the calling thread, in parallel where
	/// the transaction allows. A speculating transaction counts what it
	/// speculated on the calling thread, so it answers `false`.
	///
	/// See [`surrealdb_kvs::api::Transactable::parallel_count`].
	pub fn parallel_count(&self) -> bool {
		!self.speculating() && self.inner.parallel_count()
	}

	/// Whether the backend bounds the memory its write buffer takes. A
	/// speculating transaction holds its writes in memory itself, so it
	/// answers `false`.
	///
	/// See [`surrealdb_kvs::api::Transactable::bounds_write_buffer`].
	pub fn bounds_write_buffer(&self) -> bool {
		!self.speculating() && self.inner.bounds_write_buffer()
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

	/// Whether a locked read ([`Self::getu`]) conflicts only with writes,
	/// never with another transaction's locked read of the same key.
	///
	/// See [`surrealdb_kvs::api::Transactable::shared_locked_reads`].
	pub fn shared_locked_reads(&self) -> bool {
		self.inner.shared_locked_reads()
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
	///
	/// Crate-private: [`crate::Transaction::commit`] is the commit a caller wants.
	/// It refuses a poisoned transaction and flushes the buffered index deltas into
	/// the same transaction as the writes they describe, and reaching this one past
	/// it — which `Transaction`'s `Deref` would otherwise allow — skips both.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub(crate) async fn commit(&self) -> Result<()> {
		self.inner.commit().await
	}

	/// Check if a key exists in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn exists(&self, key: Key<'_>, version: Option<u64>) -> Result<bool> {
		if version.is_none() && self.speculating() {
			match self.held(key.as_slice()) {
				Held::Value(_) => return Ok(true),
				Held::Deleted => return Ok(false),
				Held::Storage => {}
			}
		}
		self.inner.exists(key, version).await
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn get(&self, key: Key<'_>, version: Option<u64>) -> Result<Option<Val>> {
		if version.is_none() && self.speculating() {
			match self.held(key.as_slice()) {
				Held::Value(value) => return Ok(Some(value)),
				Held::Deleted => return Ok(None),
				Held::Storage => {}
			}
		}
		self.inner.get(key, version).await
	}

	/// Fetch a key from the datastore, registering the key for
	/// commit-time conflict detection.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getu(&self, key: Key<'_>) -> Result<Option<Val>> {
		if self.speculating() {
			match self.held(key.as_slice()) {
				Held::Value(value) => return Ok(Some(value)),
				Held::Deleted => return Ok(None),
				Held::Storage => {}
			}
		}
		self.inner.getu(key).await
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getm(&self, keys: &[Key<'_>], version: Option<u64>) -> Result<GetMultiResult> {
		if version.is_none() && self.speculating() {
			let mut result = GetMultiResult::default();
			for key in keys {
				let value = self.speculative_value(key).await?;
				if let Some(value) = &value {
					result.records += 1;
					result.value_bytes += value.len() as u64;
				}
				result.values.push(value);
			}
			return Ok(result);
		}
		self.inner.getm(keys, version).await
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn getr(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<ScanResult> {
		if self.speculated(&rng, version) {
			return Ok(scan_result(self.speculative_pairs(&rng, true).await?));
		}
		self.inner.getr(rng, version).await
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn set<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.set(&key, val));
		}
		self.inner.set(key, val).await
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn replace<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.set(&key, val));
		}
		self.inner.replace(key, val).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn put<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let val = val.into_vec();
		if self.speculating() {
			if self.speculative_value(&key).await?.is_some() {
				return Err(crate::Error::TransactionKeyAlreadyExists);
			}
			return self.overlay_mut(|overlay| overlay.set(&key, val));
		}
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
		if self.speculating() {
			if self.speculative_value(&key).await? != chk {
				return Err(crate::Error::TransactionConditionNotMet);
			}
			return self.overlay_mut(|overlay| overlay.set(&key, val));
		}
		self.inner.putc(key, val, chk).await
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delc(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		if self.speculating() {
			if self.speculative_value(&key).await?.as_deref() != chk {
				return Err(crate::Error::TransactionConditionNotMet);
			}
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.delc(key, chk).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn delr(&self, rng: KeyRange<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.clear(&rng));
		}
		self.inner.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clr(&self, key: Key<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrc(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		if self.speculating() {
			if self.speculative_value(&key).await?.as_deref() != chk {
				return Err(crate::Error::TransactionConditionNotMet);
			}
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.clrc(key, chk).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn clrr(&self, rng: KeyRange<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.clear(&rng));
		}
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, false).await?;
			return Ok(keys_result(pairs.into_iter().skip(skip as usize).take(limit as usize)));
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, false).await?;
			return Ok(keys_result(
				pairs.into_iter().rev().skip(skip as usize).take(limit as usize),
			));
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, true).await?;
			return Ok(scan_result(pairs.into_iter().skip(skip as usize).take(limit as usize)));
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, true).await?;
			return Ok(scan_result(
				pairs.into_iter().rev().skip(skip as usize).take(limit as usize),
			));
		}
		self.inner.scanr(rng, limit, skip, version).await
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn count(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<usize> {
		if self.speculated(&rng, version) {
			return Ok(self.speculative_pairs(&rng, false).await?.len());
		}
		self.inner.count(rng, version).await
	}

	/// Count the keys within a range, examining at most `budget` of them, and
	/// answer the key to resume at when the range holds more.
	///
	/// A range the speculation touches is counted whole, from what the
	/// speculation holds over what the storage does.
	///
	/// See [`surrealdb_kvs::api::Transactable::count_bounded`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)]
	pub async fn count_bounded(
		&self,
		rng: KeyRange<'_>,
		version: Option<u64>,
		budget: u32,
	) -> Result<(usize, Option<Key<'static>>)> {
		if self.speculated(&rng, version) {
			return Ok((self.speculative_pairs(&rng, false).await?.len(), None));
		}
		self.inner.count_bounded(rng, version, budget).await
	}

	/// How the work of a count grows with the keys a range holds.
	///
	/// See [`surrealdb_kvs::api::Transactable::count_cost`].
	pub fn count_cost(&self) -> surrealdb_kvs::CountCost {
		self.inner.count_cost()
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, false).await?;
			return Ok(Box::new(HeldCursor::new(ordered(pairs, dir, skip))));
		}
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, true).await?;
			return Ok(Box::new(HeldCursor::new(ordered(pairs, dir, skip))));
		}
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
		if self.speculated(&rng, version) {
			let pairs = self.speculative_pairs(&rng, false).await?;
			return Ok(Batch::new(None, pairs.into_iter().map(|(key, _)| key).collect()));
		}
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
		if self.speculated(&rng, version) {
			return Ok(Batch::new(None, self.speculative_pairs(&rng, true).await?));
		}
		self.inner.batch_keys_vals(rng, batch, version).await
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------
	//
	// A save point scopes more than the storage layer's write set: the buffered
	// index deltas describe writes this transaction has made, so they are framed
	// alongside it. Only [`crate::Transaction`] holds those buffers, so it owns
	// the save-point API and reaches the engine through `inner` directly.
	//
	// Deliberately no wrapper here. `Transaction` derefs to `Transactor`, so one
	// would resolve on a `Transaction` for any caller who reached for it, and move
	// the storage stack without the frames that mirror it. Reaching `inner` still
	// does that — the field is what `Transaction` itself needs — so this is one
	// fewer way in rather than none.

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

/// A [`KeysResult`] of `pairs`' keys.
fn keys_result(pairs: impl Iterator<Item = (Vec<u8>, Val)>) -> KeysResult {
	let mut result = KeysResult::default();
	for (key, _) in pairs {
		result.key_bytes += key.len() as u64;
		result.keys.push(key);
	}
	result
}

/// A [`ScanResult`] of `pairs`.
fn scan_result(pairs: impl IntoIterator<Item = (Vec<u8>, Val)>) -> ScanResult {
	let mut result = ScanResult::default();
	for (key, value) in pairs {
		result.key_bytes += key.len() as u64;
		result.value_bytes += value.len() as u64;
		result.values.push((key, value));
	}
	result
}

/// `pairs`, in key order, in the order a scan in direction `dir` yields them,
/// the first `skip` left out.
fn ordered(mut pairs: Vec<(Vec<u8>, Val)>, dir: Direction, skip: u32) -> Vec<(Vec<u8>, Val)> {
	if matches!(dir, Direction::Backward) {
		pairs.reverse();
	}
	pairs.drain(..(skip as usize).min(pairs.len()));
	pairs
}
