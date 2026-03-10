#![cfg(feature = "kv-surrealkv")]

mod cnf;
mod sync;

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealkv::{
	Durability, HistoryOptions, LSMIterator, Mode, Transaction as Tx, Tree, TreeBuilder,
};
use sync::{BackgroundFlusher, CommitCoordinator};
use tokio::sync::RwLock;

use super::Direction;
use super::api::ScanLimit;
use super::config::{SurrealKvConfig, SyncMode};
use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

pub struct Datastore {
	db: Tree,
	enable_versions: bool,
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
	/// Is versioning enabled?
	enable_versions: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
	/// Commit coordinator for grouped fsync (when sync=every)
	commit_coordinator: Option<Arc<CommitCoordinator>>,
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
		let enable_vlog = *cnf::SURREALKV_ENABLE_VLOG || config.versioned;
		info!(target: TARGET, "Enabling value log separation: {}", enable_vlog);
		let builder = builder.with_enable_vlog(enable_vlog);

		// Configure the maximum value log file size
		info!(target: TARGET, "Setting value log max file size: {}", *cnf::SURREALKV_VLOG_MAX_FILE_SIZE);
		let builder = builder.with_vlog_max_file_size(*cnf::SURREALKV_VLOG_MAX_FILE_SIZE);

		// Configure value log threshold
		info!(target: TARGET, "Setting value log threshold: {}", *cnf::SURREALKV_VLOG_THRESHOLD);
		let builder = builder.with_vlog_value_threshold(*cnf::SURREALKV_VLOG_THRESHOLD);

		// Configure versioned queries with retention period
		info!(target: TARGET, "Versioning enabled: {} with retention period: {}ns", config.versioned, config.retention_ns);
		let builder = builder.with_versioning(config.versioned, config.retention_ns);

		// Configure optional bplustree index for versioned queries
		let versioned_index = config.versioned && *cnf::SURREALKV_VERSIONED_INDEX;
		info!(target: TARGET, "Versioning with versioned_index: {}", versioned_index);
		let builder = builder.with_versioned_index(versioned_index);

		// Enable the block cache capacity
		info!(target: TARGET, "Setting block cache capacity: {}", *cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		let builder = builder.with_block_cache_capacity(*cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		// Set the block size
		info!(target: TARGET, "Setting block size: {}", *cnf::SURREALKV_BLOCK_SIZE);
		let builder = builder.with_block_size(*cnf::SURREALKV_BLOCK_SIZE);
		// Set the data storage directory
		let builder = builder.with_path(path.to_string().into());
		// Build the database
		let db = builder.build().map_err(|e| Error::Datastore(e.to_string()))?;

		// Create sync components based on sync mode
		let (commit_coordinator, background_flusher) = match config.sync_mode {
			SyncMode::Every => {
				info!(target: TARGET, "Sync mode: every transaction commit");
				let coordinator = Arc::new(CommitCoordinator::new(db.clone())?);
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
			enable_versions: config.versioned,
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
			enable_versions: self.enable_versions,
			inner: RwLock::new(txn),
			commit_coordinator: self.commit_coordinator.clone(),
		}))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
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
	async fn cancel(&self) -> Result<()> {
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
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&self) -> Result<()> {
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
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&self, key: Key, version: Option<u64>) -> Result<bool> {
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
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
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
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
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
		match version {
			Some(ts) => inner.set_at(&key, &val, ts)?,
			None => inner.set(&key, &val)?,
		}
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace(&self, key: Key, val: Val) -> Result<()> {
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
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
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
		if let Some(ts) = version {
			inner.set_at(&key, &val, ts)?;
		} else {
			match inner.get(&key)? {
				None => inner.set(&key, &val)?,
				_ => return Err(Error::TransactionKeyAlreadyExists),
			}
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
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
	}

	/// Delete a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&self, key: Key) -> Result<()> {
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
		if self.enable_versions {
			inner.soft_delete(&key)?;
		} else {
			inner.delete(&key)?;
		}
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&self, key: Key, chk: Option<Val>) -> Result<()> {
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
		if self.enable_versions {
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
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr(&self, key: Key) -> Result<()> {
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
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc(&self, key: Key, chk: Option<Val>) -> Result<()> {
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
	}

	/// Count the total number of keys within a range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn count(&self, rng: Range<Key>, version: Option<u64>) -> Result<usize> {
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
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
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
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
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
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
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
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
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
	}

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		self.inner.write().await.set_savepoint()?;
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		self.inner.write().await.rollback_to_savepoint()?;
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		Ok(())
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
fn consume_keys(cursor: &mut impl Cursor, limit: ScanLimit, skip: u32) -> Result<Vec<Key>> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_key()?.is_none() {
			return Ok(Vec::new());
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key
				if let Some(key) = cursor.next_key()? {
					res.push(key);
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b as usize / 128).min(4096)); // Assuming 128 bytes per entry
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				// Check the key
				if let Some(key) = cursor.next_key()? {
					bytes_fetched += key.len();
					res.push(key);
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as usize {
				// Check the key
				if let Some(key) = cursor.next_key()? {
					bytes_fetched += key.len();
					res.push(key);
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
	}
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut impl Cursor, limit: ScanLimit, skip: u32) -> Result<Vec<(Key, Val)>> {
	// Skip entries efficiently by discarding cursor results
	for _ in 0..skip {
		if cursor.next_entry()?.is_none() {
			return Ok(Vec::new());
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key and value
				if let Some(entry) = cursor.next_entry()? {
					res.push(entry);
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b as usize / 512).min(4096)); // Assuming 512 bytes per entry
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				// Check the key and value
				if let Some((key, value)) = cursor.next_entry()? {
					bytes_fetched += key.len() + value.len();
					res.push((key, value));
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as usize {
				// Check the key and value
				if let Some((key, value)) = cursor.next_entry()? {
					bytes_fetched += key.len() + value.len();
					res.push((key, value));
				} else {
					break;
				}
			}
			// Return the result
			Ok(res)
		}
	}
}
