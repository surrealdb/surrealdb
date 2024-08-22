#![cfg(feature = "kv-rocksdb")]

mod cnf;

use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use rocksdb::{
	DBCompactionStyle, DBCompressionType, LogLevel, OptimisticTransactionDB,
	OptimisticTransactionOptions, Options, ReadOptions, WriteOptions,
};
use std::fmt::Debug;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
#[non_exhaustive]
pub struct Datastore {
	db: Pin<Arc<OptimisticTransactionDB>>,
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
	inner: Option<rocksdb::Transaction<'static, OptimisticTransactionDB>>,
	/// The read options containing the Snapshot
	ro: ReadOptions,
	// The above, supposedly 'static transaction
	// actually points here, so we need to ensure
	// the memory is kept alive. This pointer must
	// be declared last, so that it is dropped last.
	_db: Pin<Arc<OptimisticTransactionDB>>,
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
		// Configure custom options
		let mut opts = Options::default();
		// Ensure we use fdatasync
		opts.set_use_fsync(false);
		// Only use warning log level
		opts.set_log_level(LogLevel::Warn);
		// Set the number of log files to keep
		opts.set_keep_log_file_num(*cnf::ROCKSDB_KEEP_LOG_FILE_NUM);
		// Create database if missing
		opts.create_if_missing(true);
		// Create column families if missing
		opts.create_missing_column_families(true);
		// Set the datastore compaction style
		opts.set_compaction_style(DBCompactionStyle::Level);
		// Increase the background thread count
		opts.increase_parallelism(*cnf::ROCKSDB_THREAD_COUNT);
		// Set the maximum number of write buffers
		opts.set_max_write_buffer_number(*cnf::ROCKSDB_MAX_WRITE_BUFFER_NUMBER);
		// Set the amount of data to build up in memory
		opts.set_write_buffer_size(*cnf::ROCKSDB_WRITE_BUFFER_SIZE);
		// Set the target file size for compaction
		opts.set_target_file_size_base(*cnf::ROCKSDB_TARGET_FILE_SIZE_BASE);
		// Set minimum number of write buffers to merge
		opts.set_min_write_buffer_number_to_merge(*cnf::ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
		// Use separate write thread queues
		opts.set_enable_pipelined_write(*cnf::ROCKSDB_ENABLE_PIPELINED_WRITES);
		// Enable separation of keys and values
		opts.set_enable_blob_files(*cnf::ROCKSDB_ENABLE_BLOB_FILES);
		// Store 4KB values separate from keys
		opts.set_min_blob_size(*cnf::ROCKSDB_MIN_BLOB_SIZE);
		// Set specific compression levels
		opts.set_compression_per_level(&[
			DBCompressionType::None,
			DBCompressionType::None,
			DBCompressionType::Lz4hc,
			DBCompressionType::Lz4hc,
			DBCompressionType::Lz4hc,
		]);
		// Create the datastore
		Ok(Datastore {
			db: Arc::pin(OptimisticTransactionDB::open(&opts, path)?),
		})
	}
	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(false);
		// Create a new transaction
		let inner = self.db.transaction_opt(&wo, &to);
		// The database reference must always outlive
		// the transaction. If it doesn't then this
		// is undefined behaviour. This unsafe block
		// ensures that the transaction reference is
		// static, but will cause a crash if the
		// datastore is dropped prematurely.
		let inner = unsafe {
			std::mem::transmute::<
				rocksdb::Transaction<'_, OptimisticTransactionDB>,
				rocksdb::Transaction<'static, OptimisticTransactionDB>,
			>(inner)
		};
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		Ok(Transaction {
			done: false,
			write,
			check,
			inner: Some(inner),
			ro,
			_db: self.db.clone(),
		})
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
		match self.inner.as_ref() {
			Some(inner) => inner.rollback()?,
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
			Some(inner) => inner.commit()?,
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
		let res = self.inner.as_ref().unwrap().get_opt(key.into(), &self.ro)?.is_some();
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get<K>(&mut self, key: K, version: Option<u64>) -> Result<Option<Val>, Error>
	where
		K: Into<Key> + Sprintable + Debug,
	{
		// RocksDB does not support verisoned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}

		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the key
		let res = self.inner.as_ref().unwrap().get_opt(key.into(), &self.ro)?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
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
		self.inner.as_ref().unwrap().put(key.into(), val.into())?;
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
		// RocksDB does not support verisoned queries.
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
		match inner.get_opt(&key, &self.ro)? {
			None => inner.put(key, val)?,
			_ => return Err(Error::TxKeyAlreadyExists),
		};
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
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (inner.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => inner.put(key, val)?,
			(None, None) => inner.put(key, val)?,
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
		self.inner.as_ref().unwrap().delete(key.into())?;
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
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let chk = chk.map(Into::into);
		// Delete the key if valid
		match (inner.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => inner.delete(key)?,
			(None, None) => inner.delete(key)?,
			_ => return Err(Error::TxConditionNotMet),
		};
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
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create the iterator
		let mut iter = inner.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek(&rng.start);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some(k) = iter.key() {
				// Check the range validity
				if k >= beg && k < end {
					res.push(k.to_vec());
					iter.next();
					continue;
				}
			}
			// Exit
			break;
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
		// RocksDB does not support verisoned queries.
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
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create the iterator
		let mut iter = inner.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek(&rng.start);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some((k, v)) = iter.item() {
				// Check the range validity
				if k >= beg && k < end {
					res.push((k.to_vec(), v.to_vec()));
					iter.next();
					continue;
				}
			}
			// Exit
			break;
		}
		// Return result
		Ok(res)
	}
}
