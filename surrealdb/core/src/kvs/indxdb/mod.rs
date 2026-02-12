#![cfg(feature = "kv-indxdb")]

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use indxdb::{Database as Db, Transaction as Tx};
use tokio::sync::RwLock;

use super::api::ScanLimit;
use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

const ESTIMATED_BYTES_PER_KEY: u32 = 128;
const ESTIMATED_BYTES_PER_VAL: u32 = 512;

pub struct Datastore {
	db: Db,
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
	pub async fn new(path: &str) -> Result<Datastore> {
		match indxdb::Database::new(path).await {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Datastore(e.to_string())),
		}
	}
	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
		// Create a new transaction
		match self.db.begin(write).await {
			Ok(txn) => Ok(Box::new(Transaction {
				done: AtomicBool::new(false),
				write,
				inner: RwLock::new(txn),
			})),
			Err(e) => Err(Error::from(e)),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"indxdb"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&self) -> Result<()> {
		// Atomically mark transaction as done and check if it was already closed
		if self.done.swap(true, Ordering::AcqRel) {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Cancel this transaction
		inner.cancel().await?;
		// Continue
		Ok(())
	}

	/// Commit a transaction
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
		// Cancel this transaction
		inner.commit().await?;
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&self, key: Key, version: Option<u64>) -> Result<bool> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Check the key
		let res = inner.exists(key).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Get the key
		let res = inner.get(key).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// IndxDB does not support versioned queries.
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
		inner.set(key, val).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// IndxDB does not support versioned queries.
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
		inner.put(key, val).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
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
		// Set the key
		inner.putc(key, val, chk.map(Into::into)).await?;
		// Return result
		Ok(())
	}

	/// Delete a key
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
		let res = inner.del(key).await?;
		// Return result
		Ok(res)
	}

	/// Delete a key if the current value matches a condition
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
		// Remove the key
		let res = inner.delc(key, chk.map(Into::into)).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count, adding skip to fetch enough entries
		let count = match limit {
			ScanLimit::Count(c) => c.saturating_add(skip),
			ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KEY).max(1).saturating_add(skip),
			ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
		};
		// Scan the keys
		let res = inner.keys(rng, count).await?;
		// Consume the results
		let res = consume_keys(&mut res.into_iter(), limit, skip);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count, adding skip to fetch enough entries
		let count = match limit {
			ScanLimit::Count(c) => c.saturating_add(skip),
			ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_KEY).max(1).saturating_add(skip),
			ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
		};
		// Scan the keys
		let res = inner.keysr(rng, count).await?;
		// Consume the results
		let res = consume_keys(&mut res.into_iter(), limit, skip);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count, adding skip to fetch enough entries
		let count = match limit {
			ScanLimit::Count(c) => c.saturating_add(skip),
			ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_VAL).max(1).saturating_add(skip),
			ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
		};
		// Scan the keys
		let res = inner.scan(rng, count).await?;
		// Consume the results
		let res = consume_vals(&mut res.into_iter(), limit, skip);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// IndxDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Load the inner transaction
		let inner = self.inner.read().await;
		// Extract the limit count, adding skip to fetch enough entries
		let count = match limit {
			ScanLimit::Count(c) => c.saturating_add(skip),
			ScanLimit::Bytes(b) => (b / ESTIMATED_BYTES_PER_VAL).max(1).saturating_add(skip),
			ScanLimit::BytesOrCount(_, c) => c.saturating_add(skip),
		};
		// Scan the keys in reverse
		let res = inner.scanr(rng, count).await?;
		// Consume the results
		let res = consume_vals(&mut res.into_iter(), limit, skip);
		// Return result
		Ok(res)
	}

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		self.inner.write().await.set_savepoint().await?;
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		self.inner.write().await.rollback_to_savepoint().await?;
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		Ok(())
	}
}

// Consume and iterate over keys
fn consume_keys<I: Iterator<Item = Key>>(iter: &mut I, limit: ScanLimit, skip: u32) -> Vec<Key> {
	// Skip entries from the pre-fetched iterator
	for _ in 0..skip {
		if iter.next().is_none() {
			return Vec::new();
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key
				if let Some(k) = iter.next() {
					res.push(k);
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KEY).min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				// Check the key
				if let Some(k) = iter.next() {
					bytes_fetched += k.len();
					res.push(k);
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
				// Check the key
				if let Some(k) = iter.next() {
					bytes_fetched += k.len();
					res.push(k);
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
fn consume_vals<I: Iterator<Item = (Key, Val)>>(
	iter: &mut I,
	limit: ScanLimit,
	skip: u32,
) -> Vec<(Key, Val)> {
	// Skip entries from the pre-fetched iterator
	for _ in 0..skip {
		if iter.next().is_none() {
			return Vec::new();
		}
	}
	match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key and value
				if let Some((k, v)) = iter.next() {
					res.push((k, v));
				} else {
					break;
				}
			}
			// Return the result
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_VAL).min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0usize;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as usize {
				// Check the key and value
				if let Some((k, v)) = iter.next() {
					bytes_fetched += k.len() + v.len();
					res.push((k, v));
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
				// Check the key and value
				if let Some((k, v)) = iter.next() {
					bytes_fetched += k.len() + v.len();
					res.push((k, v));
				} else {
					break;
				}
			}
			// Return the result
			res
		}
	}
}
