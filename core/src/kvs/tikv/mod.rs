#![cfg(feature = "kv-tikv")]

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};
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

impl Transaction {
	/// Behaviour if unclosed
	pub(crate) fn check_level(&mut self, check: Check) {
		self.check = check;
	}
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
		if self.write {
			self.inner.rollback().await?;
		}
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
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub(crate) async fn get_timestamp<K>(
		&mut self,
		key: K,
		lock: bool,
	) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the current timestamp
		let res = self.inner.get_current_timestamp().await?;
		let ver = res.version();
		let verbytes = u64_to_versionstamp(ver);
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let k: Key = key.into();
		if lock {
			let prev = self.inner.get(k.clone()).await?;
			if let Some(prev) = prev {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev = try_to_u64_be(array)?;
				if prev >= ver {
					return Err(Error::TxFailure);
				}
			}

			self.inner.put(k, verbytes.to_vec()).await?;
		}
		// Return the uint64 representation of the timestamp as the result
		Ok(u64_to_versionstamp(ver))
	}
	/// Obtain a new key that is suffixed with the change timestamp
	#[allow(unused)]
	pub(crate) async fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
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
		let ts = self.get_timestamp(ts_key, false).await?;
		let mut k: Vec<u8> = prefix.into();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.into());
		Ok(k)
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
		let res = self.inner.key_exists(key.into()).await?;
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
		self.inner.put(key.into(), val.into()).await?;
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub(crate) async fn put<K, V>(
		&mut self,
		category: KeyCategory,
		key: K,
		val: V,
	) -> Result<(), Error>
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
			_ => return Err(Error::TxKeyAlreadyExistsCategory(category)),
		};
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
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
		// Delete the key
		self.inner.delete(key.into()).await?;
		// Return result
		Ok(())
	}
	/// Delete a key
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
		let res = res.map(|kv| (Key::from(kv.0), kv.1)).collect();
		// Return result
		Ok(res)
	}
	/// Delete a range of keys from the databases
	pub(crate) async fn delr<K>(&mut self, rng: Range<K>, limit: u32) -> Result<(), Error>
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
		let res = self.inner.scan_keys(rng, limit).await?;
		// Delete all the keys
		for key in res {
			self.inner.delete(key).await?;
		}
		// Return result
		Ok(())
	}
}
