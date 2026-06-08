#![cfg(feature = "kv-surrealkv")]

mod background_flusher;
mod cnf;
mod commit_coordinator;

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use background_flusher::BackgroundFlusher;
use chrono::{DateTime, Utc};
pub use cnf::SurrealKvConfig;
use commit_coordinator::CommitCoordinator;
use surrealkv::{
	Durability, HistoryOptions, LSMIterator, Mode, Transaction as Tx, Tree, TreeBuilder,
};
use tokio::sync::RwLock;

use super::api::{BoxFut, KeysResult, ScanLimit, ScanResult};
use super::config::SyncMode;
use super::err::{Error, Result};
use super::{Direction, ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

pub struct Datastore {
	db: Tree,
	/// Whether the datastore supports transaction versioning
	versioned: bool,
	/// Commit coordinator for batching transaction commits when sync=every
	commit_coordinator: Option<Arc<CommitCoordinator>>,
	/// Background flusher for periodically flushing WAL when sync=<interval>
	background_flusher: Option<Arc<BackgroundFlusher>>,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// Whether the datastore supports transaction versioning
	versioned: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
	/// Commit coordinator for grouped fsync (when sync=every)
	commit_coordinator: Option<Arc<CommitCoordinator>>,
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
	pub(crate) async fn new(path: &str, config: SurrealKvConfig) -> Result<Datastore> {
		// Configure custom options
		let builder = TreeBuilder::new();

		// Enable separated keys and values
		// Determine if vlog should be enabled
		// - Required when versioning is enabled
		// - Can be explicitly enabled via env var even without versioning
		info!(target: TARGET, "Enabling value log separation: {}", config.enable_vlog);
		let builder = builder.with_enable_vlog(config.enable_vlog);

		// Configure the maximum value log file size
		info!(target: TARGET, "Setting value log max file size: {}", config.vlog_max_file_size);
		let builder = builder.with_vlog_max_file_size(config.vlog_max_file_size);

		// Configure value log threshold
		info!(target: TARGET, "Setting value log threshold: {}", config.vlog_threshold);
		let builder = builder.with_vlog_value_threshold(config.vlog_threshold);

		// Configure versioned queries with retention period
		let retention_ns = config.retention.as_nanos().try_into().unwrap_or(u64::MAX);
		info!(target: TARGET, "Versioning enabled: {} with retention period: {}ns", config.versioned, retention_ns);
		let builder = builder.with_versioning(config.versioned, retention_ns);

		// Configure optional bplustree index for versioned queries
		let versioned_index = config.versioned && config.versioned_index;
		info!(target: TARGET, "Versioning with versioned_index: {}", versioned_index);
		let builder = builder.with_versioned_index(versioned_index);

		// Configure the maximum memtable size
		info!(target: TARGET, "Setting max memtable size: {}", config.max_memtable_size);
		let builder = builder.with_max_memtable_size(config.max_memtable_size);
		// Enable the block cache capacity
		info!(target: TARGET, "Setting block cache capacity: {}", config.block_cache_capacity);
		let builder = builder.with_block_cache_capacity(config.block_cache_capacity);
		// Set the block size
		info!(target: TARGET, "Setting block size: {}", config.block_size);
		let builder = builder.with_block_size(config.block_size);
		// Set the data storage directory
		let builder = builder.with_path(path.to_string().into());
		// Build the database
		let db = builder.build().map_err(|e| Error::Datastore(e.to_string()))?;

		// Create sync components based on sync mode
		let (commit_coordinator, background_flusher) = match config.sync_mode {
			SyncMode::Every => {
				info!(target: TARGET, "Sync mode: every transaction commit");
				let coordinator = Arc::new(CommitCoordinator::new(db.clone(), &config)?);
				(Some(coordinator), None)
			}
			SyncMode::Interval(interval) => {
				info!(target: TARGET, "Sync mode: background syncing on interval ({}ms)", interval.as_millis());
				let flusher = Arc::new(BackgroundFlusher::new(db.clone(), interval)?);
				(None, Some(flusher))
			}
			SyncMode::Never => {
				info!(target: TARGET, "Sync mode: never (handled by the OS)");
				(None, None)
			}
		};

		// Create and return the datastore
		Ok(Datastore {
			db,
			versioned: config.versioned,
			commit_coordinator,
			background_flusher,
		})
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Wait for the background flusher to finish
		if let Some(background_flusher) = &self.background_flusher {
			background_flusher.shutdown()?;
		}
		// Wait for the commit coordinator to finish
		if let Some(commit_coordinator) = &self.commit_coordinator {
			commit_coordinator.shutdown()?;
		}
		// Flush WAL before closing
		if let Err(e) = self.db.flush_wal(true) {
			error!(target: TARGET, "An error occurred flushing the WAL buffer to disk: {e}");
		}
		// Close the database
		if let Err(e) = self.db.close().await {
			error!(target: TARGET, "An error occurred closing the database: {e}");
		}
		// Nothing to do here
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
		// Create a new transaction
		let mut txn = match write {
			true => self.db.begin_with_mode(Mode::ReadWrite),
			false => self.db.begin_with_mode(Mode::ReadOnly),
		}?;
		// For sync=every mode, use Eventual durability and let coordinator handle fsync
		// For sync=never/interval modes, also use Eventual (OS or background thread handles sync)
		txn.set_durability(Durability::Eventual);
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			versioned: self.versioned,
			inner: RwLock::new(txn),
			commit_coordinator: self.commit_coordinator.clone(),
		}))
	}
}

impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"surrealkv"
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
			inner.rollback();
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
			// Commit the transaction (writes to WAL)
			inner.commit().await?;
			// If we have a coordinator, wait for the grouped fsync
			if let Some(coordinator) = &self.commit_coordinator {
				coordinator.wait_for_sync().await?;
			}
			// Continue
			Ok(())
		})
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<bool>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at(&key, ts)?.is_some(),
				None => inner.get(&key)?.is_some(),
			};
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<Option<Val>>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at(&key, ts)?,
				None => inner.get(&key)?,
			};
			// Return result
			Ok(res)
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
			inner.set(&key, &val)?;
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
			inner.replace(&key, &val)?;
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
			match inner.get(&key)? {
				None => inner.set(&key, &val)?,
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
				(Some(v), Some(w)) if v == w => inner.set(&key, &val)?,
				(None, None) => inner.set(&key, &val)?,
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
			// Delete the key
			if self.versioned {
				inner.soft_delete(&key)?;
			} else {
				inner.delete(&key)?;
			}
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
			if self.versioned {
				match (inner.get(&key)?, chk) {
					(Some(v), Some(w)) if v == w => inner.soft_delete(&key)?,
					(None, None) => inner.soft_delete(&key)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
			} else {
				match (inner.get(&key)?, chk) {
					(Some(v), Some(w)) if v == w => inner.delete(&key)?,
					(None, None) => inner.delete(&key)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
			}
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
			// Delete the key
			inner.delete(&key)?;
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
				(Some(v), Some(w)) if v == w => inner.delete(&key)?,
				(None, None) => inner.delete(&key)?,
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
			// Versioned queries require a versioned datastore
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
				// Store the count
				let mut count = 0;
				//
				match version {
					Some(ts) => {
						// Include tombstones so we can detect deleted keys
						let opts = HistoryOptions::new().with_tombstones(true);
						// Create the iterator with tombstone visibility
						let mut iter = inner.history_with_options(beg, end, &opts)?;
						// Seek to the first key
						iter.seek_first()?;
						// History entries are sorted (key ASC, timestamp DESC),
						// so the first entry with timestamp <= ts is the latest
						// version for each key. We skip newer versions and only
						// count non-tombstone entries.
						while iter.valid() {
							let key_ref = iter.key();
							// This is the latest relevant version for this key
							if key_ref.timestamp() <= ts {
								// Store the current user key
								let user_key = key_ref.user_key().to_vec();
								// Check if this is a tombstone
								let is_tombstone = key_ref.is_tombstone();
								// Skip remaining older versions of this key
								loop {
									iter.next()?;
									if !iter.valid() || iter.key().user_key() != user_key {
										break;
									}
								}
								// Count values which are not deletes
								if !is_tombstone {
									count += 1;
								}
							} else {
								// This version is newer, skip it
								iter.next()?;
							}
						}
					}
					None => {
						// Create the iterator
						let mut iter = inner.range(beg, end)?;
						// Seek to the first key
						iter.seek_first()?;
						// Loop over all keys
						while iter.valid() {
							count += 1;
							iter.next()?;
						}
					}
				}
				// Return result
				Ok(count)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = inner.history(&beg, &end)?;
					// Seek to the first key
					iter.seek_first()?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
						ts,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter = inner.range(&beg, &end)?;
					// Seek to the first key
					iter.seek_first()?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = inner.history(&beg, &end)?;
					// Seek to the last key
					iter.seek_last()?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
						ts,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter = inner.range(&beg, &end)?;
					// Seek to the last key
					iter.seek_last()?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
					};
					consume_keys(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = inner.history(&beg, &end)?;
					// Seek to the first key
					iter.seek_first()?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
						ts,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter = inner.range(&beg, &end)?;
					// Seek to the first key
					iter.seek_first()?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Forward,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
			// Versioned queries require a versioned datastore
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
			// Retrieve the scan range
			let res = match version {
				Some(ts) => {
					// Create the iterator
					let mut iter = inner.history(&beg, &end)?;
					// Seek to the last key
					iter.seek_last()?;
					// Consume the iterator
					let mut cursor = HistoryCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
						ts,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
				None => {
					// Create the iterator
					let mut iter = inner.range(&beg, &end)?;
					// Seek to the last key
					iter.seek_last()?;
					// Consume the iterator
					let mut cursor = RangeCursor {
						inner: Box::new(iter),
						dir: Direction::Backward,
					};
					consume_vals(&mut cursor, limit, skip)?
				}
			};
			// Return result
			Ok(res)
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
		Box::new(SurrealKvTimeStampImpl)
	}
}

struct SurrealKvTimeStamp(u64);

impl TimeStamp for SurrealKvTimeStamp {
	fn as_versionstamp(&self) -> u128 {
		self.0 as u128
	}

	fn as_datetime(&self) -> Option<DateTime<Utc>> {
		Some(DateTime::from_timestamp_nanos(self.0 as i64))
	}

	fn sub_checked(&self, duration: Duration) -> Option<BoxTimeStamp> {
		let nanos: u64 = duration.as_nanos().try_into().ok()?;
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(self.0.checked_sub(nanos)?)))
	}

	fn encode<'a>(&self, bytes: &'a mut [u8; MAX_TIMESTAMP_BYTES]) -> &'a [u8] {
		bytes[..8].copy_from_slice(&self.0.to_be_bytes());
		&bytes[..8]
	}
}

struct SurrealKvTimeStampImpl;

impl TimeStampImpl for SurrealKvTimeStampImpl {
	fn earliest(&self) -> BoxTimeStamp {
		BoxTimeStamp::new(SurrealKvTimeStamp(0))
	}

	fn create_from_versionstamp(&self, version: u128) -> Option<BoxTimeStamp> {
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(version.try_into().ok()?)))
	}

	fn create_from_datetime(&self, dt: DateTime<Utc>) -> Option<BoxTimeStamp> {
		let nanos = dt.timestamp_nanos_opt()?;
		if nanos < 0 {
			return None;
		}
		Some(BoxTimeStamp::new(SurrealKvTimeStamp(nanos as u64)))
	}

	fn decode(&self, bytes: &[u8]) -> Result<BoxTimeStamp> {
		let bytes = <[u8; 8]>::try_from(bytes).map_err(|_| {
			Error::TimestampInvalid("encoded timestamp not a valid length".to_string())
		})?;
		Ok(BoxTimeStamp::new(SurrealKvTimeStamp(u64::from_be_bytes(bytes))))
	}
}

// A cursor advances through entries and returns the next key or key-value pair.
// The cursor abstraction allows consume_keys and consume_vals to work with
// both range iterators and history iterators with timestamp filtering.
trait Cursor {
	/// Returns the next key from the cursor, or None if exhausted
	fn next_key(&mut self) -> Result<Option<Key>>;
	/// Returns the next key-value pair from the cursor, or None if exhausted
	fn next_entry(&mut self) -> Result<Option<(Key, Val)>>;
}

// A cursor wrapping a range iterator
struct RangeCursor<'a> {
	inner: Box<dyn LSMIterator + 'a>,
	dir: Direction,
}

impl Cursor for RangeCursor<'_> {
	fn next_key(&mut self) -> Result<Option<Key>> {
		if self.inner.valid() {
			let key = self.inner.key().user_key().to_vec();
			match self.dir {
				Direction::Forward => self.inner.next()?,
				Direction::Backward => self.inner.prev()?,
			};
			return Ok(Some(key));
		}
		Ok(None)
	}

	fn next_entry(&mut self) -> Result<Option<(Key, Val)>> {
		if self.inner.valid() {
			let key = self.inner.key().user_key().to_vec();
			let value = self.inner.value()?;
			match self.dir {
				Direction::Forward => self.inner.next()?,
				Direction::Backward => self.inner.prev()?,
			};
			return Ok(Some((key, value)));
		}
		Ok(None)
	}
}

// A cursor wrapping a history iterator with timestamp filtering
struct HistoryCursor<'a> {
	inner: Box<dyn LSMIterator + 'a>,
	dir: Direction,
	ts: u64,
}

impl Cursor for HistoryCursor<'_> {
	fn next_key(&mut self) -> Result<Option<Key>> {
		// History entries are sorted (key ASC, timestamp DESC), so
		// forward iteration yields newest versions first per key,
		// and backward iteration yields oldest versions first per key.
		match self.dir {
			Direction::Forward => {
				// Newest version first: the first entry with ts <= self.ts
				// is the latest version. Then skip older versions of same key.
				while self.inner.valid() {
					let key_ref = self.inner.key();
					if key_ref.timestamp() <= self.ts {
						// Store the current user key
						let user_key = key_ref.user_key().to_vec();
						// Skip remaining older versions of this key
						loop {
							// Continue to the next version
							self.inner.next()?;
							// Check if we have proceeded to a new key
							if !self.inner.valid() || self.inner.key().user_key() != user_key {
								break;
							}
						}
						// Return the key
						return Ok(Some(user_key));
					}
					// Continue to the next version
					self.inner.next()?;
				}
				// Return None if no key was matched
				Ok(None)
			}
			Direction::Backward => {
				// Oldest version first: scan all versions of the current
				// key and keep the latest one with ts <= self.ts.
				while self.inner.valid() {
					// Track if matched
					let mut matched = false;
					let user_key = self.inner.key().user_key().to_vec();
					// Scan all versions of the current key
					while self.inner.valid() && self.inner.key().user_key() == user_key {
						// Check the first version at or before the timestamp
						if self.inner.key().timestamp() <= self.ts {
							matched = true;
						}
						// Continue to the previous version
						self.inner.prev()?;
					}
					// Return the key if matched
					if matched {
						return Ok(Some(user_key));
					}
				}
				// Return None if no key was matched
				Ok(None)
			}
		}
	}

	fn next_entry(&mut self) -> Result<Option<(Key, Val)>> {
		// History entries are sorted (key ASC, timestamp DESC), so
		// forward iteration yields newest versions first per key,
		// and backward iteration yields oldest versions first per key.
		match self.dir {
			Direction::Forward => {
				// Newest version first: the first entry with ts <= self.ts
				// is the latest version. Then skip older versions of same key.
				while self.inner.valid() {
					let key_ref = self.inner.key();
					if key_ref.timestamp() <= self.ts {
						// Store the current user key
						let user_key = key_ref.user_key().to_vec();
						// Store the current value
						let value = self.inner.value()?;
						// Skip remaining older versions of this key
						loop {
							// Continue to the next version
							self.inner.next()?;
							// Check if we have proceeded to a new key
							if !self.inner.valid() || self.inner.key().user_key() != user_key {
								break;
							}
						}
						return Ok(Some((user_key, value)));
					}
					// Continue to the next version
					self.inner.next()?;
				}
				// Return None if no entry was matched
				Ok(None)
			}
			Direction::Backward => {
				// Oldest version first: scan all versions of the current
				// key and keep the latest one with ts <= self.ts.
				while self.inner.valid() {
					// Extract user key once (owned for comparison across iterations)
					let user_key = self.inner.key().user_key().to_vec();
					// Store the current value
					let mut value: Option<Val> = None;
					// Scan all versions of the current key
					while self.inner.valid() && self.inner.key().user_key() == user_key {
						// Check the first version at or before the timestamp
						if self.inner.key().timestamp() <= self.ts {
							// Store the current value
							value = Some(self.inner.value()?);
						}
						// Continue to the previous version
						self.inner.prev()?;
					}
					// Return the entry if matched
					if let Some(value) = value {
						return Ok(Some((user_key, value)));
					}
				}
				// Return None if no entry was matched
				Ok(None)
			}
		}
	}
}

// Consume and iterate over only keys
fn consume_keys(cursor: &mut impl Cursor, limit: ScanLimit, skip: u32) -> Result<KeysResult> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_key()?.is_none() {
			return Ok(KeysResult::default());
		}
	}
	let mut key_bytes = 0u64;
	let keys = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key
				if let Some(key) = cursor.next_key()? {
					key_bytes += key.len() as u64;
					res.push(key);
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
				// Check the key
				if let Some(key) = cursor.next_key()? {
					key_bytes += key.len() as u64;
					res.push(key);
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
				// Check the key
				if let Some(key) = cursor.next_key()? {
					key_bytes += key.len() as u64;
					res.push(key);
				} else {
					break;
				}
			}
			res
		}
	};
	Ok(KeysResult {
		keys,
		key_bytes,
	})
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut impl Cursor, limit: ScanLimit, skip: u32) -> Result<ScanResult> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_entry()?.is_none() {
			return Ok(ScanResult::default());
		}
	}
	// Track the cumulative key/value bytes for the metric. The byte-bounded
	// limit branches still rely on `bytes_fetched` (key + value bytes) to
	// decide when to stop, so the two counters are kept separate.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	let values = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key and value
				if let Some((key, value)) = cursor.next_entry()? {
					key_bytes += key.len() as u64;
					value_bytes += value.len() as u64;
					res.push((key, value));
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
				// Check the key and value
				if let Some((key, value)) = cursor.next_entry()? {
					let key_len = key.len() as u64;
					let value_len = value.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((key, value));
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
				// Check the key and value
				if let Some((key, value)) = cursor.next_entry()? {
					let key_len = key.len() as u64;
					let value_len = value.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((key, value));
				} else {
					break;
				}
			}
			res
		}
	};
	Ok(ScanResult {
		values,
		key_bytes,
		value_bytes,
	})
}
