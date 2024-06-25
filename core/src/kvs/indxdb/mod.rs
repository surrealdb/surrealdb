#![cfg(feature = "kv-indxdb")]

use crate::err::Error;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};
use std::ops::Range;

#[non_exhaustive]
pub struct Datastore {
	db: indxdb::Db,
}

#[non_exhaustive]
pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: indxdb::Tx,
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
		match indxdb::db::new(path).await {
			Ok(db) => Ok(Datastore {
				db,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		match self.db.begin(write).await {
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

impl Transaction {
	/// Check if closed
	pub(crate) fn closed(&self) -> bool {
		self.done
	}

	/// Cancel a transaction
	pub(crate) async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		self.inner.cancel().await?;
		// Continue
		Ok(())
	}

	/// Commit a transaction
	pub(crate) async fn commit(&mut self) -> Result<(), Error> {
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
		// Cancel this transaction
		self.inner.commit().await?;
		// Continue
		Ok(())
	}

	/// Check if a key exists
	pub(crate) async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let res = self.inner.exi(key.into()).await?;
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub(crate) async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
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
	pub(crate) async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		self.inner.set(key.into(), val.into()).await?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub(crate) async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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

	/// Insert a key if the current value matches a condition
	pub(crate) async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
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
		self.inner.putc(key.into(), val.into(), chk.map(Into::into)).await?;
		// Return result
		Ok(())
	}

	/// Delete a key
	pub(crate) async fn del<K>(&mut self, key: K) -> Result<(), Error>
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
		// Remove the key
		let res = self.inner.del(key.into()).await?;
		// Return result
		Ok(res)
	}

	/// Delete a key if the current value matches a condition
	pub(crate) async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
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
		// Remove the key
		let res = self.inner.delc(key.into(), chk.map(Into::into)).await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	pub(crate) async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
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
		let res = self.inner.keys(rng, limit as usize)?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	pub(crate) async fn scan<K>(
		&mut self,
		rng: Range<K>,
		limit: u32,
	) -> Result<Vec<(Key, Val)>, Error>
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
		let res = self.inner.scan(rng, limit).await?;
		// Return result
		Ok(res)
	}
}
