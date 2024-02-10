#![cfg(feature = "kv-surrealkv")]

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};

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
	inner: Tx,
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
		let mut opts = Options::new();
		opts.dir = path.to_string().into();

		match Store::new(opts) {
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
		self.inner.rollback();

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
		self.inner
			.get(&key.into().as_slice())
			.map(|opt| opt.is_some())
			.map_err(|e| Error::Tx(format!("Unable to get kv from SurrealKV: {}", e)))
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
		let res = self.inner.get(&key.into().as_slice())?;

		// Return result
		Ok(res)
	}
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub(crate) async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let key_vec = key.into();
		let k = key_vec.as_slice();
		let prev = self.inner.get(k.clone())?;

		let ver = match prev {
			Some(prev) => {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev: u64 = try_to_u64_be(array)?;
				prev + 1
			}
			None => 1,
		};

		let verbytes = u64_to_versionstamp(ver);

		self.inner.set(k, verbytes.as_slice())?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
	/// Obtain a new key that is suffixed with the change timestamp
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
		let ts = self.get_timestamp(ts_key).await?;
		let mut k: Vec<u8> = prefix.into();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.into());
		Ok(k)
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

		let key_slice = key.into();
		// Set the key
		self.inner.set(&key_slice.as_slice(), &val.into())?;
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

		let key: Vec<u8> = key.into();
		if self.exi(key.clone().as_slice()).await? {
			return Err(Error::TxKeyAlreadyExistsCategory(category));
		}

		// Set the key
		let key: &[u8] = &key[..];
		self.inner.set(&key, &val.into())?;

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

		// Get the check
		let chk = chk.map(Into::into);

		// Set the key if not present
		let key_slice = key.into();
		let val_vec = val.into();

		let res = self
			.inner
			.get(&key_slice.as_slice())
			.map_err(|e| Error::Tx(format!("Unable to get kv from SurrealKV: {}", e)));

		match (res, chk) {
			(Ok(Some(v)), Some(w)) if v == w => self.inner.set(&key_slice.as_slice(), &val_vec),
			(Ok(None), None) => self.inner.set(&key_slice.as_slice(), &val_vec),
			(Err(e), _) => return Err(e),
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
		// Remove the key
		let key_slice = key.into();
		let res = self.inner.delete(&key_slice.as_slice())?;
		// Return result
		Ok(res)
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

		// Get the check
		let chk: Option<Val> = chk.map(Into::into);

		// Remove the key
		let key_slice = key.into();
		let res = self
			.inner
			.get(&key_slice.as_slice())
			.map_err(|e| Error::Tx(format!("Unable to get kv from SurrealKV: {}", e)));

		match (res, chk) {
			(Ok(Some(v)), Some(w)) if v == w => self.inner.delete(&key_slice.as_slice()),
			(Ok(None), None) => self.inner.delete(&key_slice.as_slice()),
			(Err(e), _) => return Err(e),
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
		let start_range = rng.start.into();
		let end_range = rng.end.into();

		// Scan the keys
		let res =
			self.inner.scan(start_range.as_slice()..end_range.as_slice(), Some(limit as usize))?;
		let res = res.into_iter().map(|kv| (Key::from(kv.0), kv.1)).collect();

		// Return result
		Ok(res)
	}
}
