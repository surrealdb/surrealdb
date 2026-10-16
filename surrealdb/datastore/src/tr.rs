use std::fmt;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use surrealdb_kvs::api::{
	Batch, GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanResult, Transactable,
};
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::{Direction, Estimate, Key, KeyRange, Snapshot, SplitTarget};

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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub fn closed(&self) -> bool {
		self.inner.closed()
	}

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This function can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return a [`surrealdb_kvs::Error::TransactionReadonly`] error when
	/// attempting to modify any data within the transaction.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub fn writeable(&self) -> bool {
		self.inner.writeable()
	}

	/// Whether every transaction committed to the store is committed through
	/// the datastore this transaction belongs to, and this transaction reads
	/// the snapshot the store held when it opened.
	///
	/// See [`surrealdb_kvs::api::Transactable::sole_writer`].
	pub fn sole_writer(&self) -> bool {
		self.inner.sole_writer()
	}

	/// Whether a write transaction's commit fails if any key it read was
	/// written since it opened.
	///
	/// See [`surrealdb_kvs::api::Transactable::validates_reads`].
	pub fn validates_reads(&self) -> bool {
		self.inner.validates_reads()
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub(crate) async fn commit(&self) -> Result<()> {
		self.inner.commit().await
	}

	/// Commit the transaction without waiting for the store's own sync.
	///
	/// See [`surrealdb_kvs::api::Transactable::commit_unsynced`]. Crate-private:
	/// only for a commit a later synced commit through the same store makes
	/// durable.
	#[cfg_attr(
		not(diskann),
		allow(dead_code, reason = "only DiskANN guard coverings and steps commit unsynced")
	)]
	pub(crate) async fn commit_unsynced(&self) -> Result<()> {
		self.inner.commit_unsynced().await
	}

	/// Check if a key exists in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn getr(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<ScanResult> {
		if self.speculated(&rng, version) {
			return Ok(scan_result(self.speculative_pairs(&rng, true).await?));
		}
		self.inner.getr(rng, version).await
	}

	/// Insert or update a key in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.del(key).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn delr(&self, rng: KeyRange<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.clear(&rng));
		}
		self.inner.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn clr(&self, key: Key<'_>) -> Result<()> {
		if self.speculating() {
			return self.overlay_mut(|overlay| overlay.delete(&key));
		}
		self.inner.clr(key).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	/// request to the underlying datastore. The returned [`KeysResult`] also
	/// reports the total key bytes scanned, accumulated by the backend during
	/// the same iteration that produced the keys.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	/// request to the underlying datastore. The returned [`KeysResult`] also
	/// reports the total key bytes scanned, accumulated by the backend during
	/// the same iteration that produced the keys.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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

	/// This transaction's owned read view, or `None` when it cannot give one.
	///
	/// A speculation's view answers `None`: its reads pass through an overlay of writes that
	/// the storage's view does not hold.
	///
	/// See [`surrealdb_kvs::api::Transactable::snapshot`].
	pub fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		if self.speculating() {
			return None;
		}
		self.inner.snapshot()
	}

	/// What read views the storage can hand out: a fact of the datastore, the same for every
	/// transaction opened on it, and an upper bound on what [`Self::snapshot`] returns.
	///
	/// See [`surrealdb_kvs::api::Transactable::view_capabilities`].
	pub fn view_capabilities(&self) -> ViewCapabilities {
		self.inner.view_capabilities()
	}

	/// What a range holds, exactly when it holds fewer keys than the estimate budget and
	/// extrapolated otherwise.
	///
	/// A speculating transaction estimates what the storage holds: the writes the speculation
	/// has taken are not counted.
	///
	/// See [`surrealdb_kvs::api::Transactable::estimate`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn estimate(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<Estimate> {
		self.inner.estimate(rng, version).await
	}

	/// Boundary keys that divide a range into parts, strictly inside it and ascending.
	///
	/// A range the speculation touches is not divided: the storage cannot see the keys the
	/// speculation holds, so the answer is no boundaries.
	///
	/// See [`surrealdb_kvs::api::Transactable::split`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
	pub async fn split(
		&self,
		rng: KeyRange<'_>,
		target: SplitTarget,
		version: Option<u64>,
	) -> Result<Vec<Key<'static>>> {
		if self.speculated(&rng, version) {
			return Ok(Vec::new());
		}
		self.inner.split(rng, target, version).await
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
	/// [`ScanCursorKeys`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tr", skip_all)
	)]
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
	// A save point covers the storage write set and the buffered index deltas
	// together, so both are framed at once. Only [`crate::Transaction`] holds
	// those buffers, so it owns the save-point API and calls the engine through
	// `inner` directly.
	//
	// There is no save-point wrapper here. `Transaction` derefs to `Transactor`,
	// so a wrapper would be reachable from any `Transaction` and could roll back
	// storage without the index frames. `inner` stays reachable because
	// `Transaction` needs it, so callers must not use it to skip those frames.

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

	/// Returns the timestamp implementation of the stamps [`Self::timestamp`]
	/// mints, which key the change feed and the live-query events.
	pub fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.inner.timestamp_impl()
	}

	/// Returns the timestamp implementation of the version a versioned read
	/// takes, through which a `VERSION <datetime>` clause is converted.
	pub fn version_impl(&self) -> BoxTimeStampImpl {
		self.inner.version_impl()
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

#[cfg(test)]
mod tests {
	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::api::BoxFut;
	use surrealdb_kvs::{RangeCursor, Result as KvsResult, TransactionType};

	use super::*;

	/// A view that opens nothing: only its presence is observed.
	struct StubView;

	impl Snapshot for StubView {
		fn open<'a>(
			&'a self,
			_: KeyRange<'a>,
			_: Direction,
			_: Option<u64>,
		) -> BoxFut<'a, KvsResult<Box<dyn RangeCursor + 'a>>> {
			unreachable!("the view is only asked for, never read")
		}
	}

	/// A transaction on the in-memory backend that, unlike the backend, can give a view.
	struct Viewing(Box<dyn Transactable>);

	impl Transactable for Viewing {
		fn kind(&self) -> &'static str {
			self.0.kind()
		}
		fn closed(&self) -> bool {
			self.0.closed()
		}
		fn writeable(&self) -> bool {
			self.0.writeable()
		}
		fn cancel(&self) -> BoxFut<'_, KvsResult<()>> {
			self.0.cancel()
		}
		fn commit(&self) -> BoxFut<'_, KvsResult<()>> {
			self.0.commit()
		}
		fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<bool>> {
			self.0.exists(key, version)
		}
		fn get<'a>(
			&'a self,
			key: Key<'a>,
			version: Option<u64>,
		) -> BoxFut<'a, KvsResult<Option<Val>>> {
			self.0.get(key, version)
		}
		fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
			self.0.set(key, val)
		}
		fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
			self.0.put(key, val)
		}
		fn putc<'a>(
			&'a self,
			key: Key<'a>,
			val: Val,
			chk: Option<Val>,
		) -> BoxFut<'a, KvsResult<()>> {
			self.0.putc(key, val, chk)
		}
		fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
			self.0.del(key)
		}
		fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
			self.0.delc(key, chk)
		}
		fn keys<'a>(
			&'a self,
			rng: KeyRange<'a>,
			limit: u32,
			skip: u32,
			version: Option<u64>,
		) -> BoxFut<'a, KvsResult<KeysResult>> {
			self.0.keys(rng, limit, skip, version)
		}
		fn keysr<'a>(
			&'a self,
			rng: KeyRange<'a>,
			limit: u32,
			skip: u32,
			version: Option<u64>,
		) -> BoxFut<'a, KvsResult<KeysResult>> {
			self.0.keysr(rng, limit, skip, version)
		}
		fn scan<'a>(
			&'a self,
			rng: KeyRange<'a>,
			limit: u32,
			skip: u32,
			version: Option<u64>,
		) -> BoxFut<'a, KvsResult<ScanResult>> {
			self.0.scan(rng, limit, skip, version)
		}
		fn scanr<'a>(
			&'a self,
			rng: KeyRange<'a>,
			limit: u32,
			skip: u32,
			version: Option<u64>,
		) -> BoxFut<'a, KvsResult<ScanResult>> {
			self.0.scanr(rng, limit, skip, version)
		}
		fn new_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
			self.0.new_save_point()
		}
		fn release_last_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
			self.0.release_last_save_point()
		}
		fn rollback_to_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
			self.0.rollback_to_save_point()
		}
		fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
			Some(Arc::new(StubView))
		}
	}

	async fn viewing_storage() -> Box<dyn Transactable> {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let (tx, _) = builder.new_transaction(TransactionType::Read).await.unwrap();
		Box::new(Viewing(tx))
	}

	/// A speculation's reads pass through an overlay the storage's view does not hold, so its
	/// transactor offers no view even over a backend that gives one.
	#[tokio::test]
	async fn a_speculating_transaction_gives_no_view() {
		let plain = Transactor::new(viewing_storage().await);
		assert!(plain.snapshot().is_some(), "the storage's own view is offered");
		let speculating =
			Transactor::speculative(viewing_storage().await, Arc::new(Mutex::new(None)));
		assert!(speculating.speculating());
		assert!(speculating.snapshot().is_none());
	}
}
