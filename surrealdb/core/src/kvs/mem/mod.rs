#![cfg(feature = "kv-mem")]

mod cnf;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
pub use cnf::MemoryConfig;
use surrealmx::{Database, DatabaseOptions, KeyIterator, ScanIterator, Transaction as Tx};
use tokio::sync::RwLock;
use tracing::info;

const TARGET: &str = "surrealdb::core::kvs::mem";

use super::api::{
	BoxFut, GetMultiResult, KeyValSpan, KeysResult, ScanChunkStats, ScanCursorVals, ScanResult,
	ValVisitor, ValsBatch,
};
#[cfg(not(target_family = "wasm"))]
use super::config::{AolMode, SnapshotMode, SyncMode};
use super::cursor::fill_vals_batch;
use super::direction::Direction;
use super::err::{Error, Result};
use super::util::update_range;
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
use crate::kvs::{Key, Val};

pub struct Datastore {
	db: Database,
	/// Whether user-defined timestamps (versioning) are enabled
	versioned: bool,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
	/// Copied from the datastore at transaction creation.
	versioned: bool,
}

impl Transaction {
	fn ensure_versioned(&self, version: Option<u64>) -> Result<()> {
		if !self.versioned && version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		Ok(())
	}
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(config: MemoryConfig) -> Result<Datastore> {
		info!(
			target: TARGET,
			"Versioning enabled: {} with retention period: {}ns",
			config.versioned,
			config.retention_ns
		);
		#[cfg(not(target_family = "wasm"))]
		match &config.persist_path {
			Some(path) => {
				info!(target: TARGET, "Persistence path: {path}");
				info!(target: TARGET, "Append-only log mode: {}", config.aol_mode);
				info!(target: TARGET, "Snapshot mode: {}", config.snapshot_mode);
				info!(target: TARGET, "Sync mode: {}", config.sync_mode);
			}
			None => info!(target: TARGET, "Storage mode: in-memory only (no persist path)"),
		}
		// Create new configuration options
		let opts = DatabaseOptions {
			enable_gc: config.retention_ns > 0,
			enable_cleanup: true,
			..Default::default()
		};
		// Create the database, optionally with persistence
		#[cfg(not(target_family = "wasm"))]
		let db = if let Some(ref persist_path) = config.persist_path {
			// Build persistence options from config
			let mut persistence_opts = surrealmx::PersistenceOptions::new(persist_path);
			// Map AOL mode
			persistence_opts.aol_mode = match config.aol_mode {
				AolMode::Never => surrealmx::AolMode::Never,
				AolMode::Sync => surrealmx::AolMode::SynchronousOnCommit,
				AolMode::Async => surrealmx::AolMode::AsynchronousAfterCommit,
			};
			// Map snapshot mode
			persistence_opts.snapshot_mode = match config.snapshot_mode {
				SnapshotMode::Never => surrealmx::SnapshotMode::Never,
				SnapshotMode::Interval(interval) => surrealmx::SnapshotMode::Interval(interval),
			};
			// Map sync mode to fsync mode
			persistence_opts.fsync_mode = match config.sync_mode {
				SyncMode::Never => surrealmx::FsyncMode::Never,
				SyncMode::Every => surrealmx::FsyncMode::EveryAppend,
				SyncMode::Interval(d) => surrealmx::FsyncMode::Interval(d),
			};
			// Create a persistent database
			Database::new_with_persistence(opts, persistence_opts)
				.map_err(|e| Error::Datastore(e.to_string()))?
		} else {
			// Create a non-persistent database
			Database::new_with_options(opts)
		};
		#[cfg(target_family = "wasm")]
		let db = Database::new_with_options(opts);
		// Configure GC retention if a retention period is specified
		let db = if config.retention_ns > 0 {
			db.with_gc_history(Duration::from_nanos(config.retention_ns))
		} else {
			db
		};
		// Return the new datastore
		Ok(Datastore {
			db,
			versioned: config.versioned,
		})
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
		// Create a new transaction
		let txn = self.db.transaction(write).with_snapshot_isolation();
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			inner: RwLock::new(txn),
			versioned: self.versioned,
		}))
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"memory"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancels the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Rollback this transaction
			inner.cancel()?;
			// Continue
			Ok(())
		})
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Commit this transaction
			inner.commit()?;
			// Continue
			Ok(())
		})
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<bool>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at_version(key, ts)?.is_some(),
				None => inner.get(key)?.is_some(),
			};
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<Option<Val>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at_version(key, ts)?,
				None => inner.get(key)?,
			};
			// Return result
			Ok(res.map(Val::from))
		})
	}

	/// Fetch multiple keys from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	fn getm(&self, keys: Vec<Key>, version: Option<u64>) -> BoxFut<'_, Result<GetMultiResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the keys
			let res = match version {
				Some(ts) => inner.getm_at_version(keys, ts)?,
				None => inner.getm(keys)?,
			};
			// Materialise once, accumulating the hit count and value bytes during
			// the same pass so callers do not need to re-walk the result.
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			let values = res
				.into_iter()
				.map(|opt| {
					opt.map(|v| {
						records += 1;
						value_bytes += v.len() as u64;
						Val::from(v)
					})
				})
				.collect();
			Ok(GetMultiResult {
				values,
				records,
				value_bytes,
			})
		})
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn set(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key
			inner.set(key, val)?;
			// Return result
			Ok(())
		})
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn replace(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Replace the key
			inner.set(key, val)?;
			// Return result
			Ok(())
		})
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn put(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key if empty
			// `put` checks the base store directly and would reject a key deleted
			// earlier in this transaction; the current view must decide existence.
			match inner.get(&key)? {
				None => inner.set(key, val)?,
				_ => return Err(Error::TransactionKeyAlreadyExists),
			}
			// Return result
			Ok(())
		})
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Set the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.set(key, val)?,
				(None, None) => inner.set(key, val)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Delete a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn del(&self, key: Key) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Remove the key
			inner.del(key)?;
			// Return result
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Delete the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.del(key)?,
				(None, None) => inner.del(key)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn clr(&self, key: Key) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Remove the key (use del since delete doesn't exist in SurrealMX)
			inner.del(key)?;
			// Return result
			Ok(())
		})
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn clrc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Delete the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.del(key)?,
				(None, None) => inner.del(key)?,
				_ => return Err(Error::TransactionConditionNotMet),
			};
			// Return result
			Ok(())
		})
	}

	/// Count the total number of keys within a range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn count(&self, rng: Range<Key>, version: Option<u64>) -> BoxFut<'_, Result<usize>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Execute on the blocking threadpool
			let res = affinitypool::spawn_local(move || -> Result<_> {
				// Count the items in the range
				let res = match version {
					Some(ts) => inner.total_at_version(beg..end, None, None, ts)?,
					None => inner.total(beg..end, None, None)?,
				};
				// Return result
				Ok(res)
			})
			.await?;
			// Return result
			Ok(res)
		})
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keys(
		&self,
		rng: Range<Key>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a forward iterator
			let mut iter = match version {
				Some(ts) => inner.keys_iter_at_version(beg..end, ts)?,
				None => inner.keys_iter(beg..end)?,
			};
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keysr(
		&self,
		rng: Range<Key>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a reverse iterator
			let mut iter = match version {
				Some(ts) => inner.keys_iter_at_version_reverse(beg..end, ts)?,
				None => inner.keys_iter_reverse(beg..end)?,
			};
			// Consume the iterator
			Ok(consume_keys(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scan(
		&self,
		rng: Range<Key>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a forward iterator
			let mut iter = match version {
				Some(ts) => inner.scan_iter_at_version(beg..end, ts)?,
				None => inner.scan_iter(beg..end)?,
			};
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit, skip))
		})
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scanr(
		&self,
		rng: Range<Key>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let beg = rng.start;
			let end = rng.end;
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Create a reverse iterator
			let mut iter = match version {
				Some(ts) => inner.scan_iter_at_version_reverse(beg..end, ts)?,
				None => inner.scan_iter_reverse(beg..end)?,
			};
			// Consume the iterator
			Ok(consume_vals(&mut iter, limit, skip))
		})
	}

	/// Open a stateful key+value scan cursor. Overrides the default
	/// (double-copying) cursor with a resume-by-bound cursor whose `for_each`
	/// streams the engine's refcounted `Bytes` to the visitor with no payload
	/// copy. See [`MemValsCursor`].
	fn open_vals_cursor<'a>(
		&'a self,
		rng: Range<Key>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(MemValsCursor {
				tx: self,
				rng,
				dir,
				version,
				skip,
				exhausted: false,
				key_buf: Vec::new(),
				val_buf: Vec::new(),
				spans: Vec::new(),
			}) as Box<dyn ScanCursorVals + 'a>)
		})
	}

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.write().await.set_savepoint()?;
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.write().await.rollback_to_savepoint()?;
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { Ok(()) })
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		Box::new(SurrealMxTimeStampImpl)
	}
}

struct SurrealMxTimeStamp(u64);

impl TimeStamp for SurrealMxTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		Some(DateTime::from_timestamp_nanos(self.0 as i64))
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let nanos: u64 = duration.as_nanos().try_into().ok()?;
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(self.0.checked_sub(nanos)?)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		bytes[..8].copy_from_slice(&self.0.to_be_bytes());
		&bytes[..8]
	}
}

struct SurrealMxTimeStampImpl;

impl TimeStampImpl for SurrealMxTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(SurrealMxTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let nanos = dt.timestamp_nanos_opt()?;
		if nanos < 0 {
			return None;
		}
		Some(BoxTimeStamp::new(SurrealMxTimeStamp(nanos as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(SurrealMxTimeStamp(u64::from_be_bytes(bytes))))
	}
}

// Consume and iterate over only keys
fn consume_keys(cursor: &mut KeyIterator<'_>, limit: u32, skip: u32) -> KeysResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	// Create the result set
	let mut keys = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while keys.len() < limit as usize {
		if let Some(k) = cursor.next() {
			key_bytes += k.len() as u64;
			keys.push(k.to_vec());
		} else {
			break;
		}
	}
	KeysResult {
		keys,
		key_bytes,
	}
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut ScanIterator<'_>, limit: u32, skip: u32) -> ScanResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return ScanResult::default();
		}
	}
	// Track the cumulative key/value bytes for the scan metrics.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	// Create the result set
	let mut values = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while values.len() < limit as usize {
		if let Some((k, v)) = cursor.next() {
			key_bytes += k.len() as u64;
			value_bytes += v.len() as u64;
			values.push((k.to_vec(), v.to_vec()));
		} else {
			break;
		}
	}
	ScanResult {
		values,
		key_bytes,
		value_bytes,
	}
}

/// Stateful, resume-by-bound key+value scan cursor for the in-memory engine.
///
/// surrealmx scan iterators borrow the transaction's `RwLock` read guard, so a
/// cursor cannot hold one across calls without a self-referential borrow.
/// Instead this cursor re-seeks the B-tree from the current range bound on each
/// call (a cheap operation) and advances the bound past the last visited key.
/// `for_each` hands the engine's refcounted `Bytes` to the visitor by
/// reference — zero payload copy — while `next_batch` fills the reusable
/// buffers for callers that need an owned-in-buffer batch.
struct MemValsCursor<'a> {
	/// The parent transaction (source of the `RwLock`-guarded inner tx).
	tx: &'a Transaction,
	/// Remaining range to scan; advanced past the last visited key each call.
	rng: Range<Key>,
	/// Fixed scan direction.
	dir: Direction,
	/// Optional historical version timestamp.
	version: Option<u64>,
	/// Entries still to skip before the first visited row (burned once).
	skip: u32,
	/// Set once the range is exhausted; further calls are cheap no-ops.
	exhausted: bool,
	/// Reusable concatenated key buffer for the `next_batch` path.
	key_buf: Vec<u8>,
	/// Reusable concatenated value buffer for the `next_batch` path.
	val_buf: Vec<u8>,
	/// Reusable per-pair spans for the `next_batch` path.
	spans: Vec<KeyValSpan>,
}

impl MemValsCursor<'_> {
	/// Build a fresh forward/backward (optionally versioned) iterator over the
	/// current range, borrowing the supplied read guard.
	fn build_iter<'g>(&self, inner: &'g Tx) -> Result<ScanIterator<'g>> {
		let rng = self.rng.start.clone()..self.rng.end.clone();
		Ok(match (self.version, self.dir) {
			(Some(ts), Direction::Forward) => inner.scan_iter_at_version(rng, ts)?,
			(None, Direction::Forward) => inner.scan_iter(rng)?,
			(Some(ts), Direction::Backward) => inner.scan_iter_at_version_reverse(rng, ts)?,
			(None, Direction::Backward) => inner.scan_iter_reverse(rng)?,
		})
	}
}

impl ScanCursorVals for MemValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		// The materialising path shares the default cursor's `scan`-based fill;
		// the zero-copy path that borrows the engine's `Bytes` is `for_each`.
		Box::pin(async move {
			if self.tx.closed() {
				return Err(Error::TransactionFinished);
			}
			let (key_bytes, value_bytes) = fill_vals_batch(
				self.tx,
				&mut self.rng,
				self.dir,
				self.version,
				&mut self.skip,
				&mut self.exhausted,
				&mut self.key_buf,
				&mut self.val_buf,
				&mut self.spans,
				limit,
			)
			.await?;
			Ok(ValsBatch::from_parts(
				&self.key_buf,
				&self.val_buf,
				&self.spans,
				key_bytes,
				value_bytes,
			))
		})
	}

	/// Zero-copy chunk drive. Note: the transaction's `RwLock` read guard is
	/// held for the whole chunk — including the visitor's per-row work (e.g.
	/// record decode) — so a same-transaction writer queues behind it for a
	/// bounded stretch: ≤ `skip + limit` rows on the first call (the leading
	/// skip burns under the same guard), ≤ `limit` rows thereafter. Concurrent
	/// readers are unaffected unless a writer is already queued (the lock is
	/// FIFO-fair).
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			if self.tx.closed() {
				return Err(Error::TransactionFinished);
			}
			let mut stats = ScanChunkStats::default();
			// A zero-budget call must be a pure no-op: bail before burning
			// `skip` or touching the iterator so the cursor state is intact
			// for the next (non-zero) call.
			if limit == 0 || self.exhausted || self.rng.start >= self.rng.end {
				return Ok(stats);
			}
			let tx = self.tx;
			let mut last = None;
			let mut broke = false;
			{
				let inner = tx.inner.read().await;
				let mut iter = self.build_iter(&inner)?;
				for _ in 0..std::mem::take(&mut self.skip) {
					if iter.next().is_none() {
						self.exhausted = true;
						return Ok(stats);
					}
				}
				while stats.rows < limit as u64 {
					let Some((k, v)) = iter.next() else {
						self.exhausted = true;
						break;
					};
					// Count only after the visitor accepts the row, so a
					// visitor error never records an unresumed row.
					let flow = f(&k[..], &v[..])?;
					stats.rows += 1;
					stats.key_bytes += k.len() as u64;
					stats.value_bytes += v.len() as u64;
					last = Some(k);
					if let std::ops::ControlFlow::Break(()) = flow {
						broke = true;
						break;
					}
				}
			}
			update_range(&mut self.rng, self.dir, last.as_deref());
			// A short chunk without an early `Break` means the iterator dried
			// up before the row budget: the range is exhausted.
			if !broke && stats.rows < limit as u64 {
				self.exhausted = true;
			}
			Ok(stats)
		})
	}
}
