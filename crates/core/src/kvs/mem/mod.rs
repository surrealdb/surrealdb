#![cfg(feature = "kv-mem")]

use std::ops::Range;

use anyhow::{Result, bail, ensure};
use surrealkv::{Transaction as Tx, Tree, TreeBuilder};

use super::Check;
use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::savepoint::SavePoints;
use crate::kvs::{Key, Val};

pub struct Datastore {
	db: Tree,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Option<Tx>,
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
	pub(crate) async fn new() -> Result<Datastore> {
		// Create new configuration options
		let builder = TreeBuilder::new();
		// Create a new datastore
		match builder.build() {
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
	pub(crate) async fn transaction(
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
		match self.db.begin() {
			Ok(inner) => Ok(Box::new(Transaction {
				done: false,
				check,
				write,
				inner: Some(inner),
			})),
			Err(e) => Err(anyhow::Error::new(Error::Tx(e.to_string()))),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
	fn kind(&self) -> &'static str {
		"memory"
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

	/// Cancels the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Mark the transaction as done.
		self.done = true;
		// Rollback this transaction
		if let Some(inner) = &mut self.inner {
			inner.rollback();
		}
		// Continue
		Ok(())
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Mark the transaction as done.
		self.done = true;

		// Take ownership of the inner transaction
		let mut inner =
			self.inner.take().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Commit this transaction directly (memory backend doesn't need thread pool)
		inner.commit().await?;

		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&mut self, key: Key, _version: Option<u64>) -> Result<bool> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Get the key
		let res = inner.get(&key)?.is_some();

		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&mut self, key: Key, _version: Option<u64>) -> Result<Option<Val>> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Get the key
		let res = inner.get(&key)?.map(|v| v.to_vec());

		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&mut self, key: Key, val: Val, _version: Option<u64>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);

		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Set the key
		inner.set(&key, &val)?;

		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&mut self, key: Key, val: Val, _version: Option<u64>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Set the key if empty
		match inner.get(&key)? {
			None => inner.set(&key, &val)?,
			_ => bail!(Error::TxKeyAlreadyExists),
		}

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
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Set the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v.as_ref() == w.as_slice() => inner.set(&key, &val)?,
			(None, None) => inner.set(&key, &val)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Remove the key
		inner.delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]

	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);

		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Set the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v.as_ref() == w.as_slice() => inner.delete(&key)?,
			(None, None) => inner.delete(&key)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]

	async fn clr(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Remove the key
		inner.delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]

	async fn clrc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Set the key if valid
		match (inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v.as_ref() == w.as_slice() => inner.delete(&key)?,
			(None, None) => inner.delete(&key)?,
			_ => bail!(Error::TxConditionNotMet),
		};

		// Return result
		Ok(())
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]

	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		_version: Option<u64>,
	) -> Result<Vec<Key>> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();

		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Retrieve the scan range
		let res = inner
			.keys(beg, end, Some(limit as usize))?
			.map(|r| r.map(|(k, _)| k.to_vec()))
			.collect::<Result<Vec<_>, _>>()?
			.into_iter()
			.filter(|k| k.as_slice() < end) // Filter out keys equal to end bound
			.collect();

		// Return result
		Ok(res)
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		_version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();

		// Get the inner transaction
		let inner =
			self.inner.as_mut().ok_or_else(|| Error::Tx("Transaction inner is None".into()))?;

		// Retrieve the scan range
		let res = inner
			.range(beg, end, Some(limit as usize))?
			.map(|r| r.map(|(k, v)| (k.to_vec(), v.map(|v| v.to_vec()).unwrap_or_default())))
			.collect::<Result<Vec<_>, _>>()?
			.into_iter()
			.filter(|(k, _)| k.as_slice() < end) // Filter out keys equal to end bound
			.collect();

		// Return result
		Ok(res)
	}

	fn get_save_points(&mut self) -> &mut SavePoints {
		unimplemented!("Get save points not implemented for the memory backend");
	}

	fn new_save_point(&mut self) {
		if let Some(inner) = &mut self.inner {
			let _ = inner.set_savepoint();
		}
	}

	async fn rollback_to_save_point(&mut self) -> Result<()> {
		if let Some(inner) = &mut self.inner {
			inner.rollback_to_savepoint()?;
		}
		Ok(())
	}

	fn release_last_save_point(&mut self) -> Result<()> {
		Ok(())
	}
}
