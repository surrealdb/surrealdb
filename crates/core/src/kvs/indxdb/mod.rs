#![cfg(feature = "kv-indxdb")]

use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, ensure};
use indxdb::Tx;
use tokio::sync::RwLock;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Key, Val};

pub struct Datastore {
	db: indxdb::Db,
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
		match indxdb::db::new(path).await {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(anyhow::Error::new(Error::Ds(e.to_string()))),
		}
	}
	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Nothing to do here
		Ok(())
	}
	/// Start a new transaction
	pub async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
		// Create a new transaction
		match self.db.begin(write).await {
			Ok(inner) => Ok(Box::new(Transaction {
				done: AtomicBool::new(false),
				write,
				inner,
			})),
			Err(e) => Err(anyhow::Error::new(Error::Tx(e.to_string()))),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
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
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
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
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Check the key
		let res = inner.exi(key).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Get the key
		let res = inner.get(key).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
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
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
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
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
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
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
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
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.writeable(), Error::TxReadonly);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Remove the key
		let res = inner.delc(key, chk.map(Into::into)).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.keys(rng, limit).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.keysr(rng, limit).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.scan(rng, limit).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Load the inner transaction
		let mut inner = self.inner.write().await;
		// Scan the keys
		let res = inner.scanr(rng, limit).await?;
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
