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

use super::api::{BoxFut, GetMultiResult, KeysResult, ScanLimit, ScanResult};
#[cfg(not(target_family = "wasm"))]
use super::config::{AolMode, SnapshotMode, SyncMode};
use super::err::{Error, Result};
use super::{ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV};
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
			inner.put(key, val)?;
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
		limit: ScanLimit,
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
		limit: ScanLimit,
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
		limit: ScanLimit,
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
		limit: ScanLimit,
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
fn consume_keys(cursor: &mut KeyIterator<'_>, limit: ScanLimit, skip: u32) -> KeysResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	let keys = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				if let Some(k) = cursor.next() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KEY).min(4096) as usize);
			// Check that we don't exceed the byte limit
			while key_bytes < b as u64 {
				if let Some(k) = cursor.next() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && key_bytes < b as u64 {
				if let Some(k) = cursor.next() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			res
		}
	};
	KeysResult {
		keys,
		key_bytes,
	}
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut ScanIterator<'_>, limit: ScanLimit, skip: u32) -> ScanResult {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return ScanResult::default();
		}
	}
	// Track the cumulative key/value bytes for the metric. The byte-bounded limit
	// branches still rely on `bytes_fetched` (key + value bytes) to decide
	// when to stop, so the two counters are kept separate.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	let values = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				if let Some((k, v)) = cursor.next() {
					key_bytes += k.len() as u64;
					value_bytes += v.len() as u64;
					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KV).min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as u64 {
				if let Some((k, v)) = cursor.next() {
					let key_len = k.len() as u64;
					let value_len = v.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as u64 {
				if let Some((k, v)) = cursor.next() {
					let key_len = k.len() as u64;
					let value_len = v.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			res
		}
	};
	ScanResult {
		values,
		key_bytes,
		value_bytes,
	}
}
