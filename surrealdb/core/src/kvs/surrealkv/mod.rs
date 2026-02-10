#![cfg(feature = "kv-surrealkv")]

mod cnf;

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealkv::{Durability, Mode, Transaction as Tx, Tree, TreeBuilder};
use tokio::sync::RwLock;

use super::Direction;
use super::api::ScanLimit;
use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::surrealkv";

pub struct Datastore {
	db: Tree,
	enable_versions: bool,
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
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str, enable_versions: bool) -> Result<Datastore> {
		// Configure custom options
		let builder = TreeBuilder::new();
		// Enable separated keys and values
		info!(target: TARGET, "Enabling value log separation: {}", *cnf::SURREALKV_ENABLE_VLOG);
		let builder = builder.with_enable_vlog(*cnf::SURREALKV_ENABLE_VLOG);
		// Configure the maximum value log file size
		info!(target: TARGET, "Setting value log max file size: {}", *cnf::SURREALKV_VLOG_MAX_FILE_SIZE);
		let builder = builder.with_vlog_max_file_size(*cnf::SURREALKV_VLOG_MAX_FILE_SIZE);
		// Enable the block cache capacity
		info!(target: TARGET, "Setting block cache capacity: {}", *cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		let builder = builder.with_block_cache_capacity(*cnf::SURREALKV_BLOCK_CACHE_CAPACITY);
		// Configure versioned queries
		info!(target: TARGET, "Versioning enabled: {} with unlimited retention period", enable_versions);
		let builder = builder.with_versioning(enable_versions, 0);
		// Set the block size
		info!(target: TARGET, "Setting block size: {}", *cnf::SURREALKV_BLOCK_SIZE);
		let builder = builder.with_block_size(*cnf::SURREALKV_BLOCK_SIZE);
		// Log if writes should be synced
		info!(target: TARGET, "Wait for disk sync acknowledgement: {}", *cnf::SYNC_DATA);
		// Set the data storage directory
		let builder = builder.with_path(path.to_string().into());
		// Create a new datastore
		match builder.build() {
			Ok(db) => Ok(Datastore {
				db,
				enable_versions,
			}),
			Err(e) => Err(Error::Datastore(e.to_string())),
		}
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Shutdown the database
		if let Err(e) = self.db.close().await {
			error!("An error occured closing the database: {e}");
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
		// Set the transaction durability
		match *cnf::SYNC_DATA {
			true => txn.set_durability(Durability::Immediate),
			false => txn.set_durability(Durability::Eventual),
		};
		// Return the new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			enable_versions: self.enable_versions,
			inner: RwLock::new(txn),
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
		// Commit this transaction
		inner.commit().await?;
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
	async fn count(&self, rng: Range<Key>) -> Result<usize> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Count items using range iterator
		let mut iter = inner.range(&beg, &end)?;
		let mut count = 0;
		iter.seek_first()?;
		while iter.valid() {
			count += 1;
			iter.next()?;
		}
		// Return result
		Ok(count)
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
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
					inner: iter,
					dir: Direction::Forward,
					ts,
				};
				consume_keys(&mut cursor, limit)?
			}
			None => {
				// Create the iterator
				let mut iter = inner.range(&beg, &end)?;
				// Seek to the first key
				iter.seek_first()?;
				// Consume the iterator
				let mut cursor = RangeCursor {
					inner: iter,
					dir: Direction::Forward,
				};
				consume_keys(&mut cursor, limit)?
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
					inner: iter,
					dir: Direction::Backward,
					ts,
				};
				consume_keys(&mut cursor, limit)?
			}
			None => {
				// Create the iterator
				let mut iter = inner.range(&beg, &end)?;
				// Seek to the last key
				iter.seek_last()?;
				// Consume the iterator
				let mut cursor = RangeCursor {
					inner: iter,
					dir: Direction::Backward,
				};
				consume_keys(&mut cursor, limit)?
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
					inner: iter,
					dir: Direction::Forward,
					ts,
				};
				consume_vals(&mut cursor, limit)?
			}
			None => {
				// Create the iterator
				let mut iter = inner.range(&beg, &end)?;
				// Seek to the first key
				iter.seek_first()?;
				// Consume the iterator
				let mut cursor = RangeCursor {
					inner: iter,
					dir: Direction::Forward,
				};
				consume_vals(&mut cursor, limit)?
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
					inner: iter,
					dir: Direction::Backward,
					ts,
				};
				consume_vals(&mut cursor, limit)?
			}
			None => {
				// Create the iterator
				let mut iter = inner.range(&beg, &end)?;
				// Seek to the last key
				iter.seek_last()?;
				// Consume the iterator
				let mut cursor = RangeCursor {
					inner: iter,
					dir: Direction::Backward,
				};
				consume_vals(&mut cursor, limit)?
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
	inner: surrealkv::TransactionIterator<'a>,
	dir: Direction,
}

impl Cursor for RangeCursor<'_> {
	fn next_key(&mut self) -> Result<Option<Key>> {
		if self.inner.valid() {
			let key = self.inner.key();
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
			let key = self.inner.key();
			let value = self.inner.value()?.unwrap_or_default();
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
	inner: surrealkv::TransactionHistoryIterator<'a>,
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
					if self.inner.timestamp() <= self.ts {
						// Store the current key
						let key = self.inner.key();
						// Skip remaining older versions of this key
						loop {
							// Continue to the next version
							self.inner.next()?;
							// Check if we have proceeded to a new key
							if !self.inner.valid() || self.inner.key() != key {
								break;
							}
						}
						// Return the key
						return Ok(Some(key));
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
					// Store the current key
					let key = self.inner.key();
					// Scan all versions of the current key
					while self.inner.valid() && self.inner.key() == key {
						// Check the first version at or before the timestamp
						if self.inner.timestamp() <= self.ts {
							matched = true;
						}
						// Continue to the previous version
						self.inner.prev()?;
					}
					// Return the key if matched
					if matched {
						return Ok(Some(key));
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
					if self.inner.timestamp() <= self.ts {
						// Store the current key
						let key = self.inner.key();
						// Store the current value
						let value = self.inner.value()?;
						// Skip remaining older versions of this key
						loop {
							// Continue to the next version
							self.inner.next()?;
							// Check if we have proceeded to a new key
							if !self.inner.valid() || self.inner.key() != key {
								break;
							}
						}
						return Ok(Some((key, value)));
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
					// Store the current key
					let key = self.inner.key();
					// Store the current value
					let mut value: Option<Val> = None;
					// Scan all versions of the current key
					while self.inner.valid() && self.inner.key() == key {
						// Check the first version at or before the timestamp
						if self.inner.timestamp() <= self.ts {
							// Store the current value
							value = Some(self.inner.value()?);
						}
						// Continue to the previous version
						self.inner.prev()?;
					}
					// Return the entry if matched
					if let Some(value) = value {
						return Ok(Some((key, value)));
					}
				}
				// Return None if no entry was matched
				Ok(None)
			}
		}
	}
}

// Consume and iterate over only keys
fn consume_keys(cursor: &mut impl Cursor, limit: ScanLimit) -> Result<Vec<Key>> {
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
fn consume_vals(cursor: &mut impl Cursor, limit: ScanLimit) -> Result<Vec<(Key, Val)>> {
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
