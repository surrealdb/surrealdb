#![cfg(feature = "kv-mem")]

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Check, Key, Val, Version};
use std::fmt::Debug;
use std::ops::Range;
use surrealkv::Options;
use surrealkv::Store;
use surrealkv::Transaction as Tx;

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
				inner: Some(inner),
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
		match self.inner.take() {
			Some(mut inner) => {
				// Execute on the blocking threadpool
				affinitypool::execute(move || -> Result<_, Error> {
					inner.rollback();
					Ok(())
				})
				.await?;
			}
			None => return Err(fail!("Unable to cancel an already taken transaction")),
		};
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
		// Commit this transaction
		match self.inner.take() {
			Some(mut inner) => {
				// Execute on the blocking threadpool
				/*affinitypool::execute(move || -> Result<_, Error> {
					inner.commit();
					Ok(())
				})
				.await?;*/
				inner.commit().await?;
			}
			None => return Err(fail!("Unable to commit an already taken transaction")),
		};
		// Continue
		Ok(())
	}

	/// Checks if a key exists in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists<K>(&mut self, key: K, version: Option<u64>) -> Result<bool, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the arguments
		let key = key.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let (inner, res) = affinitypool::execute(|| -> Result<_, Error> {
			// Get the key
			let res = inner.get(&key)?.is_some();
			// Return result
			Ok((inner, res))
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the arguments
		let key = key.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let (inner, res) = affinitypool::execute(|| -> Result<_, Error> {
			// Get the key
			let res = match version {
				Some(ts) => inner.get_at_version(&key, ts)?,
				None => inner.get(&key)?,
			};
			// Return result
			Ok((inner, res))
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let val = val.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Set the key
			match version {
				Some(ts) => inner.set_at_ts(&key, &val, ts)?,
				None => inner.set(&key, &val)?,
			}
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Insert or replace a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn replace<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let val = val.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Replace the key
			inner.insert_or_replace(&key, &val)?;
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put<K, V>(&mut self, key: K, val: V, version: Option<u64>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let val = val.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Set the key if empty
			if let Some(ts) = version {
				inner.set_at_ts(&key, &val, ts)?;
			} else {
				match inner.get(&key)? {
					None => inner.set(&key, &val)?,
					_ => return Err(Error::TxKeyAlreadyExists),
				}
			}
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Set the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.set(&key, &val)?,
				(None, None) => inner.set(&key, &val)?,
				_ => return Err(Error::TxConditionNotMet),
			};
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Deletes a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Remove the key
			inner.soft_delete(&key)?;
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let chk = chk.map(Into::into);
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Set the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.soft_delete(&key)?,
				(None, None) => inner.soft_delete(&key)?,
				_ => return Err(Error::TxConditionNotMet),
			};
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Deletes all versions of a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clr<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Remove the key
			inner.delete(&key)?;
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(())
	}

	/// Delete all versions of a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn clrc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key> + Sprintable + Debug,
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
		let key = key.into();
		let chk = chk.map(Into::into);
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let inner = affinitypool::execute(|| -> Result<_, Error> {
			// Set the key if valid
			match (inner.get(&key)?, chk) {
				(Some(v), Some(w)) if v == w => inner.delete(&key)?,
				(None, None) => inner.delete(&key)?,
				_ => return Err(Error::TxConditionNotMet),
			};
			// Return result
			Ok(inner)
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
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
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get the transaction
		let inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let (inner, res) = affinitypool::execute(|| -> Result<_, Error> {
			// Retrieve the scan range
			let res = match version {
				Some(ts) => inner
					.keys_at_version(beg.as_slice()..end.as_slice(), ts, Some(limit as usize))
					.into_iter()
					.map(Key::from)
					.collect(),
				None => inner
					.keys(beg.as_slice()..end.as_slice(), Some(limit as usize))
					.into_iter()
					.map(Key::from)
					.collect(),
			};
			// Return result
			Ok((inner, res))
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
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
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get the transaction
		let mut inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let (inner, res) = affinitypool::execute(|| -> Result<_, Error> {
			// Retrieve the scan range
			let res = match version {
				Some(ts) => inner
					.scan_at_version(beg.as_slice()..end.as_slice(), ts, Some(limit as usize))
					.map(|r| r.map(|(k, v)| (k.to_vec(), v)).map_err(Into::<Error>::into))
					.collect::<Result<_, Error>>()?,
				None => inner
					.scan(beg.as_slice()..end.as_slice(), Some(limit as usize))
					.map(|r| r.map(|(k, v, _)| (k.to_vec(), v)).map_err(Into::into))
					.collect::<Result<_, Error>>()?,
			};
			// Return result
			Ok((inner, res))
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
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
		K: Into<Key> + Sprintable + Debug,
	{
		if self.done {
			return Err(Error::TxFinished);
		}
		// Set the key range
		let beg = rng.start.into();
		let end = rng.end.into();
		// Get the transaction
		let inner = match self.inner.take() {
			Some(inner) => inner,
			None => return Err(fail!("Unable to use an already taken transaction")),
		};
		// Execute on the blocking threadpool
		let (inner, res) = affinitypool::execute(|| -> Result<_, Error> {
			// Retrieve the scan range
			let res = inner
				.scan_all_versions(beg.as_slice()..end.as_slice(), Some(limit as usize))
				.into_iter()
				.map(|r| r.map(|(k, v, ts, del)| (k.to_vec(), v, ts, del)).map_err(Into::into))
				.collect::<Result<_, Error>>()?;
			// Return result
			Ok((inner, res))
		})
		.await?;
		// Restore the transaction
		self.inner = Some(inner);
		// Return result
		Ok(res)
	}
}

impl Transaction {
	pub(crate) fn new_save_point(&mut self) {
		if let Some(inner) = self.inner.as_mut() {
			let _ = inner.set_savepoint();
		}
	}

	pub(crate) async fn rollback_to_save_point(&mut self) -> Result<(), Error> {
		if let Some(inner) = self.inner.as_mut() {
			inner.rollback_to_savepoint()?;
		}
		Ok(())
	}

	pub(crate) fn release_last_save_point(&mut self) -> Result<(), Error> {
		Ok(())
	}
}
