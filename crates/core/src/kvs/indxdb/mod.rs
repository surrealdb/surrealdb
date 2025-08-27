#![cfg(feature = "kv-indxdb")]

use std::ops::Range;

use anyhow::{Result, ensure};

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::savepoint::SavePoints;
use crate::kvs::{Check, Key, Val};

pub struct Datastore {
	db: indxdb::Db,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: indxdb::Tx,
	/// The save point implementation
	save_points: SavePoints,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.done && self.write {
			match self.check {
				Check::None => {
					trace!("A transaction was dropped without being committed or cancelled");
				}
				Check::Warn => {
					warn!("A transaction was dropped without being committed or cancelled");
				}
				Check::Error => {
					error!("A transaction was dropped without being committed or cancelled");
				}
			}
		}
	}
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
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		match self.db.begin(write).await {
			Ok(inner) => Ok(Box::new(Transaction {
				done: false,
				check,
				write,
				inner,
				save_points: Default::default(),
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

	fn supports_reverse_scan(&self) -> bool {
		false
	}

	/// Behaviour if unclosed
	fn check_level(&mut self, check: Check) {
		self.check = check;
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		self.inner.cancel().await?;
		// Continue
		Ok(())
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		self.inner.commit().await?;
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&mut self, key: Key, version: Option<u64>) -> Result<bool> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check the key
		let res = self.inner.exi(key).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&mut self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the key
		let res = self.inner.get(key).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key
		self.inner.set(key, val).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key
		self.inner.put(key, val).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key
		self.inner.putc(key, val, chk.map(Into::into)).await?;
		// Return result
		Ok(())
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Remove the key
		let res = self.inner.del(key).await?;
		// Return result
		Ok(res)
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Remove the key
		let res = self.inner.delc(key, chk.map(Into::into)).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Scan the keys
		let res = self.inner.keys(rng, limit).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// IndxDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Scan the keys
		let res = self.inner.scan(rng, limit).await?;
		// Return result
		Ok(res)
	}

	fn get_save_points(&mut self) -> &mut SavePoints {
		&mut self.save_points
	}
}
