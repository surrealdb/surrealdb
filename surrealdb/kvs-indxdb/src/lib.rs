//! The IndexedDB (browser/WASM) key-value store backend for SurrealDB.
//!
//! This backend only exists on WASM targets: the underlying `indxdb` crate
//! wraps the browser's IndexedDB API and is not thread-safe, so on native
//! targets this crate compiles to nothing.
#![cfg(target_family = "wasm")]

use std::sync::atomic::{AtomicBool, Ordering};

use indxdb::{Database as Db, Transaction as Tx};
use surrealdb_kvs::api::{BoxFut, KeysResult, ScanResult, Transactable};
use surrealdb_kvs::err::{Error, Result};
use surrealdb_kvs::{Key, KeyRange, TransactionType, Val};
use tokio::sync::RwLock;
use tracing::instrument;

/// Convert an IndxDB engine error into the generic KVS error type.
#[expect(
	clippy::needless_pass_by_value,
	reason = "by-value so it can be passed directly to `Result::map_err`"
)]
fn kvs_error(e: indxdb::Error) -> Error {
	match e {
		indxdb::Error::DbError => Error::Datastore(e.to_string()),
		indxdb::Error::TxError => Error::Transaction(e.to_string()),
		indxdb::Error::TxClosed => Error::TransactionFinished,
		indxdb::Error::TxNotWritable => Error::TransactionReadonly,
		indxdb::Error::KeyAlreadyExists => Error::TransactionKeyAlreadyExists,
		indxdb::Error::ValNotExpectedValue => Error::TransactionConditionNotMet,
		_ => Error::Transaction(e.to_string()),
	}
}

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
	pub async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: TransactionType) -> Result<Box<dyn Transactable>> {
		let write = matches!(write, TransactionType::Write);
		// Create a new transaction
		match self.db.begin(write).await {
			Ok(txn) => Ok(Box::new(Transaction {
				done: AtomicBool::new(false),
				write,
				inner: RwLock::new(txn),
			})),
			Err(e) => Err(kvs_error(e)),
		}
	}
}

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
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already closed
			if self.done.swap(true, Ordering::AcqRel) {
				return Err(Error::TransactionFinished);
			}
			// Load the inner transaction
			let mut inner = self.inner.write().await;
			// Cancel this transaction
			inner.cancel().await.map_err(kvs_error)?;
			// Continue
			Ok(())
		})
	}

	/// Commit a transaction
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
			// Cancel this transaction
			inner.commit().await.map_err(kvs_error)?;
			// Continue
			Ok(())
		})
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		Box::pin(async move {
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
			let res = inner.exists(key.to_vec()).await.map_err(kvs_error)?;
			// Return result
			Ok(res)
		})
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		Box::pin(async move {
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
			let res = inner.get(key.to_vec()).await.map_err(kvs_error)?;
			// Return result
			Ok(res)
		})
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
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
			inner.set(key.to_vec(), val).await.map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
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
			inner.put(key.to_vec(), val).await.map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
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
			inner.putc(key.to_vec(), val, chk).await.map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
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
			inner.del(key.to_vec()).await.map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
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
			inner.delc(key.to_vec(), chk.map(|x| x.to_vec())).await.map_err(kvs_error)?;
			// Return result
			Ok(())
		})
	}

	/// Retrieve a range of keys
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			// IndxDB does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Add skip to the row budget so enough entries are fetched
			let count = limit.saturating_add(skip);
			// Scan the keys
			let res = inner.keys(rng, count).await.map_err(kvs_error)?;
			// Consume the results
			Ok(consume_keys(&mut res.into_iter(), limit, skip))
		})
	}

	/// Retrieve a range of keys, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		Box::pin(async move {
			// IndxDB does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Add skip to the row budget so enough entries are fetched
			let count = limit.saturating_add(skip);
			// Scan the keys
			let res = inner.keysr(rng, count).await.map_err(kvs_error)?;
			// Consume the results
			Ok(consume_keys(&mut res.into_iter(), limit, skip))
		})
	}

	/// Retrieve a range of key-value pairs
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			// IndxDB does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped = inner.keys(rng.clone(), skip).await.map_err(kvs_error)?;
				match skipped.last() {
					Some(last) => {
						let new_start = Key::from(last).next().to_vec();
						new_start..rng.end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Scan the keys
			let res = inner.scan(rng, limit).await.map_err(kvs_error)?;
			// Consume the results
			Ok(consume_vals(&mut res.into_iter(), limit))
		})
	}

	/// Retrieve a range of key-value pairs, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			// IndxDB does not support versioned queries.
			if version.is_some() {
				return Err(Error::UnsupportedVersionedQueries);
			}
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Set the key range
			let rng = rng.start.to_vec()..rng.end.to_vec();
			// Load the inner transaction
			let inner = self.inner.read().await;
			// Skip entries using keys-only scan to avoid fetching values
			let rng = if skip > 0 {
				let skipped = inner.keysr(rng.clone(), skip).await.map_err(kvs_error)?;
				match skipped.last() {
					Some(last) => {
						let end = last.clone();
						rng.start..end
					}
					// Fewer entries than skip -- nothing to return
					None => return Ok(ScanResult::default()),
				}
			} else {
				rng
			};
			// Scan the keys in reverse
			let res = inner.scanr(rng, limit).await.map_err(kvs_error)?;
			// Consume the results
			Ok(consume_vals(&mut res.into_iter(), limit))
		})
	}

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.write().await.set_savepoint().await.map_err(kvs_error)?;
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			self.inner.write().await.rollback_to_savepoint().await.map_err(kvs_error)?;
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { Ok(()) })
	}
}

// Consume and iterate over keys
fn consume_keys<I: Iterator<Item = Vec<u8>>>(iter: &mut I, limit: u32, skip: u32) -> KeysResult {
	// Skip entries from the pre-fetched iterator
	for _ in 0..skip {
		if iter.next().is_none() {
			return KeysResult::default();
		}
	}
	let mut key_bytes = 0u64;
	// Create the result set
	let mut keys = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while keys.len() < limit as usize {
		// Check the key
		if let Some(k) = iter.next() {
			key_bytes += k.len() as u64;
			keys.push(k);
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
fn consume_vals<I: Iterator<Item = (Vec<u8>, Val)>>(iter: &mut I, limit: u32) -> ScanResult {
	// Track the cumulative key/value bytes for the scan metrics.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	// Create the result set
	let mut values = Vec::with_capacity(limit.min(4096) as usize);
	// Check that we don't exceed the count limit
	while values.len() < limit as usize {
		// Check the key and value
		if let Some((k, v)) = iter.next() {
			key_bytes += k.len() as u64;
			value_bytes += v.len() as u64;
			values.push((k, v));
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
