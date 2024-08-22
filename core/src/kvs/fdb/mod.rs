#![cfg(feature = "kv-fdb")]

mod cnf;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::Versionstamp;
use foundationdb::options::DatabaseOption;
use foundationdb::options::MutationType;
use foundationdb::Database;
use foundationdb::RangeOption;
use foundationdb::Transaction as Tx;
use futures::StreamExt;
use once_cell::sync::Lazy;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

const TIMESTAMP: [u8; 10] = [0x00; 10];

#[non_exhaustive]
pub struct Datastore {
	db: Database,
	// The Database stored above, relies on the
	// foundationdb network being booted before
	// the client can be used. The return result
	// of the foundationdb::boot method is a
	// handle which must be dropped before the
	// program exits. This handle is stored on
	// the database so that it is held for the
	// duration of the programme. This pointer must
	// be declared last, so that it is dropped last.
	_fdbnet: Arc<foundationdb::api::NetworkAutoStop>,
}

#[non_exhaustive]
pub struct Transaction {
	/// Is the transaction complete?
	done: bool,
	/// Should this transaction lock?
	lock: bool,
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
	///
	/// The `path` argument can be a local file path to a FoundationDB
	/// cluster file, or an empty string. If specified as an empty
	/// string, then the default cluster file placed at a system
	/// dependent location (defined by FoundationDB) will be used.
	/// See https://apple.github.io/foundationdb/administration.html
	/// for more information on cluster connection files.
	pub(crate) async fn new(path: &str) -> Result<Datastore, Error> {
		// Initialize the FoundationDB Client API
		static FDBNET: Lazy<Arc<foundationdb::api::NetworkAutoStop>> =
			Lazy::new(|| Arc::new(unsafe { foundationdb::boot() }));
		// Store the network cancellation handle
		let _fdbnet = (*FDBNET).clone();
		// Configure and setup the database
		match foundationdb::Database::from_path(path) {
			Ok(db) => {
				// Set the transaction timeout
				db.set_option(DatabaseOption::TransactionTimeout(
					*cnf::FOUNDATIONDB_TRANSACTION_TIMEOUT,
				))
				.map_err(|e| Error::Ds(format!("Unable to set transaction timeout: {e}")))?;
				// Set the transaction retry liimt
				db.set_option(DatabaseOption::TransactionRetryLimit(
					*cnf::FOUNDATIONDB_TRANSACTION_RETRY_LIMIT,
				))
				.map_err(|e| Error::Ds(format!("Unable to set transaction retry limit: {e}")))?;
				// Set the transaction max retry delay
				db.set_option(DatabaseOption::TransactionMaxRetryDelay(
					*cnf::FOUNDATIONDB_TRANSACTION_MAX_RETRY_DELAY,
				))
				.map_err(|e| {
					Error::Ds(format!("Unable to set transaction max retry delay: {e}"))
				})?;
				Ok(Datastore {
					db,
					_fdbnet,
				})
			}
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		match self.db.create_trx() {
			Ok(inner) => Ok(Transaction {
				done: false,
				lock,
				check,
				write,
				inner: Some(inner),
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Each transaction uses `lock=true` to behave similarly to pessimistic
	/// locks in the same way that pessimistic transactions work in TiKV.
	/// Standard transactions in FoundationDB (where `snapshot=false`) behave
	/// behaves like a TiKV pessimistic transaction, by automatically retrying
	/// on commit conflicts at the client layer. In FoundationDB we assume
	/// that `lock=true` is effectively specifying that we should ensure
	/// transactions are serializable. If the transaction is writeable, we also
	/// assume that the user never wants to lose serializability, so we go with
	/// the standard FoundationDB serializable more in that scenario.
	#[inline(always)]
	fn snapshot(&self) -> bool {
		!self.write && !self.lock
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

	/// Cancel a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		match self.inner.take() {
			Some(inner) => inner.cancel().reset(),
			None => unreachable!(),
		};
		// Continue
		Ok(())
	}

	/// Commit a transaction
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
		// Mark this transaction as done
		self.done = true;
		// Commit this transaction
		match self.inner.take() {
			Some(inner) => inner.commit().await?,
			None => unreachable!(),
		};
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let res = self.inner.as_ref().unwrap().get(&key.into(), self.snapshot()).await?.is_some();
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// FDB does not support verisoned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}

		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the key
		let res = self
			.inner
			.as_ref()
			.unwrap()
			.get(&key.into(), self.snapshot())
			.await?
			.map(|v| v.to_vec());
		// Return result
		Ok(res)
	}

	/// Inserts or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
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
		// Set the key
		self.inner.as_ref().unwrap().set(&key.into(), &val.into());
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
		// FDB does not support verisoned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}

		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxReadonly);
		}
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		// Set the key if empty
		match inner.get(&key, self.snapshot()).await? {
			None => inner.set(&key, &val),
			_ => return Err(Error::TxKeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
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
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (inner.get(&key, self.snapshot()).await?, chk) {
			(Some(v), Some(w)) if *v.as_ref() == w => inner.set(&key, &val),
			(None, None) => inner.set(&key, &val),
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key
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
		// Remove the key
		self.inner.as_ref().unwrap().clear(&key.into());
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
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
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let chk = chk.map(Into::into);
		// Delete the key if valid
		match (inner.get(&key, self.snapshot()).await?, chk) {
			(Some(v), Some(w)) if *v.as_ref() == w => inner.clear(&key),
			(None, None) => inner.clear(&key),
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn delr<K>(&mut self, rng: Range<K>) -> Result<(), Error>
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
		// Delete the key range
		self.inner.as_ref().unwrap().clear_range(&rng.start.into(), &rng.end.into());
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<Key>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Create result set
		let mut res = vec![];
		// Set the key range
		let opt = RangeOption {
			limit: Some(limit as usize),
			..RangeOption::from((rng.start.as_slice(), rng.end.as_slice()))
		};
		// Create the scan request
		let mut req = inner.get_ranges(opt, self.snapshot());
		// Scan the keys in the iterator
		while let Some(val) = req.next().await {
			for v in val?.into_iter() {
				res.push(Key::from(v.key()));
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
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
		// FDB does not support verisoned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}

		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Create result set
		let mut res = vec![];
		// Set the key range
		let opt = RangeOption {
			limit: Some(limit as usize),
			..RangeOption::from((rng.start.as_slice(), rng.end.as_slice()))
		};
		// Create the scan request
		let mut req = inner.get_ranges(opt, self.snapshot());
		// Scan the keys in the iterator
		while let Some(val) = req.next().await {
			for v in val?.into_iter() {
				res.push((Key::from(v.key()), Val::from(v.value())));
			}
		}
		// Return result
		Ok(res)
	}

	/// Obtain a new change timestamp for a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the current read version
		let res = self.inner.as_ref().unwrap().get_read_version().await?;
		// Convert to a version stamp
		let res = crate::vs::u64_to_versionstamp(res as u64);
		// Return result
		Ok(res)
	}

	// Sets the value for a versionstamped key prefixed with the user-supplied key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(ts_key = ts_key.sprint()))]
	async fn set_versionstamp<K, V>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
		val: V,
	) -> Result<(), Error>
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
		// Build the key starting with the prefix
		let mut key: Vec<u8> = prefix.into();
		// Get the position of the timestamp
		let pos = key.len() as u32;
		// Append the timestamp placeholder
		key.extend_from_slice(&TIMESTAMP);
		// Append the suffix to the key
		key.extend(suffix.into());
		// Append the 4 byte placeholder position in little endian
		key.append(&mut pos.to_le_bytes().to_vec());
		// Convert the value
		let val = val.into();
		// Set the versionstamp key
		self.inner.as_ref().unwrap().atomic_op(&key, &val, MutationType::SetVersionstampedKey);
		// Return result
		Ok(())
	}
}
