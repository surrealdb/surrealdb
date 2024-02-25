#![cfg(feature = "kv-surrealkv")]

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Transactable;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};

use std::ops::Range;
use surrealkv::Options;
use surrealkv::Store;
use surrealkv::Transaction as Tx;

#[non_exhaustive]
pub struct Datastore {
	db: Store,
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
}

impl Transactable for Transaction {
	/// Checks if the transaction is closed.
	fn closed(&self) -> bool {
		self.done
	}

	/// Cancels the transaction.
	async fn cancel(&mut self) -> Result<(), Error> {
		// If the transaction is already closed, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		}

		// Mark the transaction as done.
		self.done = true;

		// Rollback the transaction.
		self.inner.rollback();

		Ok(())
	}

	/// Commits the transaction.
	async fn commit(&mut self) -> Result<(), Error> {
		// If the transaction is already closed or is read-only, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		} else if !self.write {
			return Err(Error::TxReadonly);
		}

		// Mark the transaction as done.
		self.done = true;

		// Commit the transaction.
		self.inner.commit().await.map_err(Into::into)
	}

	/// Checks if a key exists in the database.
	async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		}

		// Check if the key exists in the database.
		self.inner
			.get(key.into().as_slice())
			.map(|opt| opt.is_some())
			.map_err(|e| Error::Tx(format!("Unable to get kv from SurrealKV: {}", e)))
	}

	/// Fetches a value from the database by key.
	async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		}

		// Fetch the value from the database.
		let res = self.inner.get(key.into().as_slice())?;

		Ok(res)
	}

	/// Obtains a new change timestamp for a key.
	/// This timestamp is replaced with the current timestamp when the transaction is committed.
	/// This method should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// This is to minimize the delay or conflict of other transactions.
	#[allow(unused)]
	async fn get_timestamp<K>(&mut self, key: K, lock: bool) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		}

		// Convert the key into a vector.
		let key_vec = key.into();
		let k = key_vec.as_slice();

		// Get the previous value of the key.
		let prev = self.inner.get(k)?;

		// Calculate the new version.
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

		// Convert the version to a versionstamp.
		let verbytes = u64_to_versionstamp(ver);

		// Set the new versionstamp.
		self.inner.set(k, verbytes.as_slice())?;

		// Return the versionstamp.
		Ok(verbytes)
	}

	/// Obtains a new key that is suffixed with the change timestamp.
	async fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
	where
		K: Into<Key>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		} else if !self.write {
			return Err(Error::TxReadonly);
		}

		// Get the timestamp.
		let ts = self.get_timestamp(ts_key, false).await?;

		// Create the new key.
		let mut k: Vec<u8> = prefix.into();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.into());

		// Return the new key.
		Ok(k)
	}

	/// Inserts or updates a key in the database.
	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// If the transaction is already closed or is read-only, return an error.
		if self.closed() {
			return Err(Error::TxFinished);
		} else if !self.write {
			return Err(Error::TxReadonly);
		}

		// Set the key.
		self.inner.set(key.into().as_slice(), &val.into()).map_err(Into::into)
	}

	/// Inserts a key-value pair into the database if the key doesn't already exist.
	async fn put<K, V>(&mut self, category: KeyCategory, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Ensure the transaction is open and writable.
		if self.done {
			return Err(Error::TxFinished);
		}
		if !self.write {
			return Err(Error::TxReadonly);
		}

		// Check if the key already exists.
		let key: Vec<u8> = key.into();
		if self.exi(key.clone().as_slice()).await? {
			return Err(Error::TxKeyAlreadyExistsCategory(category));
		}

		// Insert the key-value pair.
		self.inner.set(&key, &val.into()).map_err(Into::into)
	}

	/// Inserts a key-value pair into the database if the key doesn't already exist,
	/// or if the existing value matches the provided check value.
	async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Ensure the transaction is open and writable.
		if self.done {
			return Err(Error::TxFinished);
		}
		if !self.write {
			return Err(Error::TxReadonly);
		}

		// Convert the check value.
		let chk = chk.map(Into::into);

		// Insert the key-value pair if the key doesn't exist or the existing value matches the check value.
		let key_slice = key.into();
		let val_vec = val.into();
		let res = self.inner.get(key_slice.as_slice())?;

		match (res, chk) {
			(Some(v), Some(w)) if v == w => self.inner.set(key_slice.as_slice(), &val_vec)?,
			(None, None) => self.inner.set(key_slice.as_slice(), &val_vec)?,
			_ => return Err(Error::TxConditionNotMet),
		};

		Ok(())
	}

	/// Deletes a key from the database.
	async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Ensure the transaction is open and writable.
		if self.done {
			return Err(Error::TxFinished);
		}
		if !self.write {
			return Err(Error::TxReadonly);
		}

		// Delete the key.
		let key_slice = key.into();
		self.inner.delete(key_slice.as_slice()).map_err(Into::into)
	}

	/// Deletes a key from the database if the existing value matches the provided check value.
	async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Ensure the transaction is open and writable.
		if self.done {
			return Err(Error::TxFinished);
		}
		if !self.write {
			return Err(Error::TxReadonly);
		}

		// Convert the check value.
		let chk: Option<Val> = chk.map(Into::into);

		// Delete the key if the existing value matches the check value.
		let key_slice = key.into();
		let res = self.inner.get(key_slice.as_slice())?;

		match (res, chk) {
			(Some(v), Some(w)) if v == w => self.inner.delete(key_slice.as_slice())?,
			(None, None) => self.inner.delete(key_slice.as_slice())?,
			_ => return Err(Error::TxConditionNotMet),
		};

		Ok(())
	}

	/// Retrieves a range of key-value pairs from the database.
	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Ensure the transaction is open.
		if self.done {
			return Err(Error::TxFinished);
		}

		// Convert the range to byte slices.
		let start_range = rng.start.into();
		let end_range = rng.end.into();

		// Retrieve the key-value pairs.
		let res =
			self.inner.scan(start_range.as_slice()..end_range.as_slice(), Some(limit as usize))?;
		let res = res.into_iter().map(|kv| (Key::from(kv.0), kv.1)).collect();

		Ok(res)
	}
}
