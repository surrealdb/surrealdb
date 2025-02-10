#![cfg(feature = "kv-mem")]

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Key, Val, Version};
use std::fmt::Debug;
use std::ops::Range;
use surrealkv::Options;
use surrealkv::Store;
use surrealkv::Transaction as Tx;

use super::{Check, KeyEncode};

pub struct Datastore {
	db: Store,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Tx,
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
	pub(crate) async fn new() -> Result<Datastore, Error> {
		// Create new configuration options
		let mut opts = Options::new();
		// Ensure versions are disabled
		opts.enable_versions = false;
		// Ensure persistence is disabled
		opts.disk_persistence = false;
		// Create a new datastore
		match Store::new(opts) {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<(), Error> {
		// Nothing to do here
		Ok(())
	}
	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		match self.db.begin() {
			Ok(inner) => Ok(Transaction {
				done: false,
				check,
				write,
				inner,
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl super::api::Transaction for Transaction {
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
	async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Mark the transaction as done.
		self.done = true;
		// Rollback this transaction
		self.inner.rollback();
		// Continue
		Ok(())
	}

	/// Commits the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Mark the transaction as done.
		self.done = true;
		// Commit this transaction.
		self.inner.commit().await?;
		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists<K>(&mut self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		// Get the key
		let res = match version {
			Some(ts) => self.inner.get_at_version(&key, ts)?.is_some(),
			None => self.inner.get(&key)?.is_some(),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		// Get the key
		let res = match version {
			Some(ts) => self.inner.get_at_version(&key, ts)?,
			None => self.inner.get(&key)?,
		};
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let val = val.into();
		// Set the key
		match version {
			Some(ts) => self.inner.set_at_ts(&key, &val, ts)?,
			None => self.inner.set(&key, &val)?,
		}
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let val = val.into();
		// Replace the key
		self.inner.insert_or_replace(&key, &val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let val = val.into();
		// Set the key if empty
		if let Some(ts) = version {
			self.inner.set_at_ts(&key, &val, ts)?;
		} else {
			match self.inner.get(&key)? {
				None => self.inner.set(&key, &val)?,
				_ => return Err(Error::TxKeyAlreadyExists),
			}
		}
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (self.inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => self.inner.set(&key, &val)?,
			(None, None) => self.inner.set(&key, &val)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		// Remove the key
		self.inner.soft_delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (self.inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => self.inner.soft_delete(&key)?,
			(None, None) => self.inner.soft_delete(&key)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		// Remove the key
		self.inner.delete(&key)?;
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: KeyEncode + Sprintable + Debug,
		V: Into<Val> + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the arguments
		let key = key.encode_owned()?;
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (self.inner.get(&key)?, chk) {
			(Some(v), Some(w)) if v == w => self.inner.delete(&key)?,
			(None, None) => self.inner.delete(&key)?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.encode_owned()?;
		let end = rng.end.encode_owned()?;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => self
				.inner
				.keys_at_version(beg.as_slice()..end.as_slice(), ts, Some(limit as usize))
				.map(Key::from)
				.collect(),
			None => self
				.inner
				.keys(beg.as_slice()..end.as_slice(), Some(limit as usize))
				.map(Key::from)
				.collect(),
		};
		// Return result
		Ok(res)
	}

	/// Retrieves a range of key-value pairs from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.encode_owned()?;
		let end = rng.end.encode_owned()?;
		// Retrieve the scan range
		let res = match version {
			Some(ts) => self
				.inner
				.scan_at_version(beg.as_slice()..end.as_slice(), ts, Some(limit as usize))
				.map(|r| r.map(|(k, v)| (k.to_vec(), v)).map_err(Into::<Error>::into))
				.collect::<Result<_, Error>>()?,
			None => self
				.inner
				.scan(beg.as_slice()..end.as_slice(), Some(limit as usize))
				.map(|r| r.map(|(k, v, _)| (k.to_vec(), v)).map_err(Into::into))
				.collect::<Result<_, Error>>()?,
		};
		// Return result
		Ok(res)
	}

	/// Retrieve all the versions from a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan_all_versions<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> Result<Vec<(Key, Val, Version, bool)>, Error>
	where
		K: KeyEncode + Sprintable + Debug,
	{
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.encode_owned()?;
		let end = rng.end.encode_owned()?;
		// Retrieve the scan range
		let res = self
			.inner
			.scan_all_versions(beg.as_slice()..end.as_slice(), Some(limit as usize))
			.map(|r| r.map(|(k, v, ts, del)| (k.to_vec(), v, ts, del)).map_err(Into::into))
			.collect::<Result<_, Error>>()?;
		// Return result
		Ok(res)
	}
}

impl Transaction {
	pub(crate) fn new_save_point(&mut self) {
		let _ = self.inner.set_savepoint();
	}

	pub(crate) async fn rollback_to_save_point(&mut self) -> Result<(), Error> {
		self.inner.rollback_to_savepoint()?;
		Ok(())
	}

	pub(crate) fn release_last_save_point(&mut self) -> Result<(), Error> {
		Ok(())
	}
}
