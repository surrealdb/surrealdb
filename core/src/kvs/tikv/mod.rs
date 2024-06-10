#![cfg(feature = "kv-tikv")]

use crate::err::Error;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{u64_to_versionstamp, Versionstamp};
use std::ops::Range;
use tikv::CheckLevel;
use tikv::TimestampExt;
use tikv::TransactionOptions;

#[non_exhaustive]
pub struct Datastore {
	db: tikv::TransactionClient,
}

#[non_exhaustive]
pub struct Transaction {
	// Is the transaction complete?
	done: bool,
	// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: tikv::Transaction,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.done && self.write {
			// Check if already panicking
			if std::thread::panicking() {
				return;
			}
			// Handle the behaviour
			match self.check {
				Check::None => {
					trace!("A transaction was dropped without being committed or cancelled");
				}
				Check::Warn => {
					warn!("A transaction was dropped without being committed or cancelled");
				}
				Check::Panic => {
					#[cfg(debug_assertions)]
					{
						let backtrace = std::backtrace::Backtrace::force_capture();
						if let std::backtrace::BacktraceStatus::Captured = backtrace.status() {
							println!("{}", backtrace);
						}
					}
					panic!("A transaction was dropped without being committed or cancelled");
				}
			}
		}
	}
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		match tikv::TransactionClient::new(vec![path]).await {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		// TiKV currently has issues with pessimistic locks. Panic in development.
		#[cfg(debug_assertions)]
		if lock {
			panic!("There are issues with pessimistic locking in TiKV");
		}
		// Set whether this should be an optimistic or pessimistic transaction
		let mut opt = if lock {
			TransactionOptions::new_pessimistic()
		} else {
			TransactionOptions::new_optimistic()
		};
		// Set the behaviour when dropping an unfinished transaction
		opt = opt.drop_check(CheckLevel::Warn);
		// Set this transaction as read only if possible
		if !write {
			opt = opt.read_only();
		}
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		match self.db.begin_with_options(opt).await {
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
	/// Check if closed
	fn closed(&self) -> bool {
		self.done
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction
	async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		if self.write {
			self.inner.rollback().await?;
		}
		// Continue
		Ok(())
	}

	/// Commit a transaction
	async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Mark this transaction as done
		self.done = true;
		// Commit this transaction
		if let Err(err) = self.inner.commit().await {
			if let Err(inner_err) = self.inner.rollback().await {
				error!("Transaction commit failed {} and rollback failed: {}", err, inner_err);
			}
			return Err(err.into());
		}
		// Continue
		Ok(())
	}

	/// Check if a key exists
	async fn exists<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let res = self.inner.key_exists(key.into()).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the key
		let res = self.inner.get(key.into()).await?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Set the key
		self.inner.put(key.into(), val.into()).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the key
		let key = key.into();
		// Get the val
		let val = val.into();
		// Set the key if empty
		match self.inner.key_exists(key.clone()).await? {
			false => self.inner.put(key, val).await?,
			_ => return Err(Error::TxKeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the key
		let key = key.into();
		// Get the val
		let val = val.into();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		match (self.inner.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.inner.put(key, val).await?,
			(None, None) => self.inner.put(key, val).await?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Delete the key
		self.inner.delete(key.into()).await?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the key
		let key = key.into();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		match (self.inner.get(key.clone()).await?, chk) {
			(Some(v), Some(w)) if v == w => self.inner.delete(key).await?,
			(None, None) => self.inner.delete(key).await?,
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the database
	async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Scan the keys
		let res = self.inner.scan_keys(rng, limit).await?.map(|k| Key::from(k)).collect();
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Scan the keys
		let res = self.inner.scan(rng, limit).await?.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}

	/// Obtain a new change timestamp for a key
	#[allow(unused)]
	async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Calculate the version key
		let key = key.into();
		// Get the transaction version
		let ver = self.inner.get_current_timestamp().await?.version();
		// Convert the value to a versionstamp
		let verbytes = u64_to_versionstamp(ver);
		// Calculate the previous version value
		if let Some(prev) = self.get(key.as_slice()).await? {
			let res: Result<[u8; 10], Error> = match prev.as_slice().try_into() {
				Ok(ba) => Ok(ba),
				Err(e) => Err(Error::Tx(e.to_string())),
			};
			let prev = crate::vs::try_to_u64_be(res?)?;
			if prev >= ver {
				return Err(Error::TxFailure);
			}
		};
		// Convert the
		let verbytes = crate::vs::u64_to_versionstamp(ver);
		// Store the timestamp to prevent other transactions from committing
		self.set(key.as_slice(), verbytes.to_vec()).await?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
}
