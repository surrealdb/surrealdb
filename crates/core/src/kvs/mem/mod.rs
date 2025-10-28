#![cfg(feature = "kv-mem")]

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail, ensure};
use surrealmx::{Database, DatabaseOptions, Transaction as Tx};
use tokio::sync::RwLock;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Key, Val, Version};

pub struct Datastore {
	db: Database<Vec<u8>, Vec<u8>>,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: RwLock<Tx<Vec<u8>, Vec<u8>>>,
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
	pub(crate) async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
		// Create a new transaction
		let txn = self.db.transaction(write);
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
impl super::api::Transaction for Transaction {
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
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Mark the transaction as done.
		self.done.store(true, Ordering::Release);
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
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Mark the transaction as done.
		self.done.store(true, Ordering::Release);
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
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at_version(key.clone(), ts)?.is_some(),
			None => inner.get(key.clone())?.is_some(),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the key
		let res = match version {
			Some(ts) => inner.get_at_version(key.clone(), ts)?,
			None => inner.get(key.clone())?,
		};
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// SurrealMX does not support versioned set queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key
		inner.set(key.clone(), val.clone())?;
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace(&mut self, key: Key, val: Val) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Replace the key (use set since insert_or_replace doesn't exist in SurrealMX)
		inner.set(key.clone(), val.clone())?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// SurrealMX does not support versioned put queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if empty
		inner.put(key.clone(), val.clone())?;
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Set the key if valid
		match (inner.get(key.clone())?, chk) {
			(Some(v), Some(w)) if v == w => inner.set(key.clone(), val.clone())?,
			(None, None) => inner.set(key.clone(), val.clone())?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Remove the key
		inner.del(key.clone())?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key if valid
		match (inner.get(key.clone())?, chk) {
			(Some(v), Some(w)) if v == w => inner.del(key.clone())?,
			(None, None) => inner.del(key.clone())?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Remove the key (use del since delete doesn't exist in SurrealMX)
		inner.del(key.clone())?;
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Delete the key if valid
		match (inner.get(key.clone())?, chk) {
			(Some(v), Some(w)) if v == w => inner.del(key.clone())?,
			(None, None) => inner.del(key.clone())?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner.keys_at_version(beg..end, None, Some(limit as usize), ts)?,
			None => inner.keys(beg..end, None, Some(limit as usize))?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner.keys_at_version_reverse(beg..end, None, Some(limit as usize), ts)?,
			None => inner.keys_reverse(beg..end, None, Some(limit as usize))?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner.scan_at_version(beg..end, None, Some(limit as usize), ts)?,
			None => inner.scan(beg..end, None, Some(limit as usize))?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => inner.scan_at_version_reverse(beg..end, None, Some(limit as usize), ts)?,
			None => inner.scan_reverse(beg..end, None, Some(limit as usize))?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve all the versions from a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan_all_versions(
		&mut self,
		rng: Range<Key>,
		limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Set the key range
		let beg = rng.start;
		let end = rng.end;
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Retrieve the scan range
		let res = inner
			.scan_all_versions(beg..end, None, Some(limit as usize))?
			.into_iter()
			.map(|(k, ts, v)| match v {
				Some(v) => (k.to_vec(), v.to_vec(), ts, false),
				None => (k.to_vec(), vec![], ts, true),
			})
			.collect::<Result<_>>()?;
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
