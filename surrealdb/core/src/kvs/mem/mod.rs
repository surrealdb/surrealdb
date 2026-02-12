#![cfg(feature = "kv-mem")]

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealmx::{Database, DatabaseOptions, KeyIterator, ScanIterator, Transaction as Tx};
use tokio::sync::RwLock;

use super::api::ScanLimit;
use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

pub struct Datastore {
	db: Database,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx>,
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new() -> Result<Datastore> {
		// Create new configuration options
		let opts = DatabaseOptions {
			enable_gc: true,
			enable_cleanup: true,
			..Default::default()
		};
		// Create a new in-memory database
		let db = Database::new_with_options(opts);
		// Return the new datastore
		Ok(Datastore {
			db,
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
		}))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
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
	async fn cancel(&self) -> Result<()> {
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
		inner.commit()?;
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
			Some(ts) => inner.get_at_version(key, ts)?.is_some(),
			None => inner.get(key)?.is_some(),
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
			Some(ts) => inner.get_at_version(key, ts)?,
			None => inner.get(key)?,
		};
		// Return result
		Ok(res.map(Val::from))
	}

	/// Fetch multiple keys from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	async fn getm(&self, keys: Vec<Key>, version: Option<u64>) -> Result<Vec<Option<Val>>> {
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
		// Return result
		Ok(res.into_iter().map(|opt| opt.map(Val::from)).collect())
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// SurrealMX does not support versioned set queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
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
		inner.set(key, val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// SurrealMX does not support versioned put queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
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
			(Some(v), Some(w)) if v == w => inner.set(key, val)?,
			(None, None) => inner.set(key, val)?,
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
		// Remove the key
		inner.del(key)?;
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
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => inner.del(key)?,
			(None, None) => inner.del(key)?,
			_ => return Err(Error::TransactionConditionNotMet),
		};
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
		// Remove the key (use del since delete doesn't exist in SurrealMX)
		inner.del(key)?;
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
			(Some(v), Some(w)) if v == w => inner.del(key)?,
			(None, None) => inner.del(key)?,
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
		// Create a forward iterator
		let mut iter = match version {
			Some(ts) => inner.keys_iter_at_version(beg..end, ts)?,
			None => inner.keys_iter(beg..end)?,
		};
		// Consume the iterator
		let res = consume_keys(&mut iter, limit, skip);
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
		// Create a reverse iterator
		let mut iter = match version {
			Some(ts) => inner.keys_iter_at_version_reverse(beg..end, ts)?,
			None => inner.keys_iter_reverse(beg..end)?,
		};
		// Consume the iterator
		let res = consume_keys(&mut iter, limit, skip);
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
		// Create a forward iterator
		let mut iter = match version {
			Some(ts) => inner.scan_iter_at_version(beg..end, ts)?,
			None => inner.scan_iter(beg..end)?,
		};
		// Consume the iterator
		let res = consume_vals(&mut iter, limit, skip);
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
		// Create a reverse iterator
		let mut iter = match version {
			Some(ts) => inner.scan_iter_at_version_reverse(beg..end, ts)?,
			None => inner.scan_iter_reverse(beg..end)?,
		};
		// Consume the iterator
		let res = consume_vals(&mut iter, limit, skip);
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

// Consume and iterate over only keys
fn consume_keys(cursor: &mut KeyIterator<'_>, limit: ScanLimit, skip: u32) -> Vec<Key> {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return Vec::new();
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				if let Some(k) = cursor.next() {
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b as usize / 128).min(4096)); // Assuming 128 bytes per entry
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				if let Some(k) = cursor.next() {
					bytes_fetched += k.len();
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as usize {
				if let Some(k) = cursor.next() {
					bytes_fetched += k.len();
					res.push(k.to_vec());
				} else {
					break;
				}
			}
			// Return the result
			res
		}
	}
}

// Consume and iterate over keys and values
fn consume_vals(cursor: &mut ScanIterator<'_>, limit: ScanLimit, skip: u32) -> Vec<(Key, Val)> {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if cursor.next().is_none() {
			return Vec::new();
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				if let Some((k, v)) = cursor.next() {
					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b as usize / 512).min(4096)); // Assuming 512 bytes per entry
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				if let Some((k, v)) = cursor.next() {
					bytes_fetched += k.len() + v.len();
					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as usize {
				if let Some((k, v)) = cursor.next() {
					bytes_fetched += k.len() + v.len();
					res.push((k.to_vec(), v.to_vec()));
				} else {
					break;
				}
			}
			// Return the result
			res
		}
	}
}
