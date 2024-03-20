#![cfg(feature = "kv-speedb")]

mod cnf;

use crate::err::Error;
use crate::key::error::KeyCategory;
use crate::kvs::Check;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};
use futures::lock::Mutex;
use speedb::{
	DBCompactionStyle, DBCompressionType, LogLevel, OptimisticTransactionDB,
	OptimisticTransactionOptions, Options, ReadOptions, WriteOptions,
};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
pub struct Datastore {
	db: Pin<Arc<OptimisticTransactionDB>>,
}

pub struct Transaction {
	// Is the transaction complete?
	done: bool,
	// Is the transaction writeable?
	write: bool,
	/// Should we check unhandled transactions?
	check: Check,
	/// The underlying datastore transaction
	inner: Arc<Mutex<Option<speedb::Transaction<'static, OptimisticTransactionDB>>>>,
	// The read options containing the Snapshot
	ro: ReadOptions,
	// The above, supposedly 'static transaction
	// actually points here, so we need to ensure
	// the memory is kept alive. This pointer must
	// be declared last, so that it is dropped last
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
		opts.set_keep_log_file_num(*cnf::SPEEDB_KEEP_LOG_FILE_NUM);
		// Create database if missing
		opts.create_if_missing(true);
		// Create column families if missing
		opts.create_missing_column_families(true);
		// Set the datastore compaction style
		opts.set_compaction_style(DBCompactionStyle::Level);
		// Increase the background thread count
		opts.increase_parallelism(*cnf::SPEEDB_THREAD_COUNT);
		// Set the maximum number of write buffers
		opts.set_max_write_buffer_number(*cnf::SPEEDB_MAX_WRITE_BUFFER_NUMBER);
		// Set the amount of data to build up in memory
		opts.set_write_buffer_size(*cnf::SPEEDB_WRITE_BUFFER_SIZE);
		// Set the target file size for compaction
		opts.set_target_file_size_base(*cnf::SPEEDB_TARGET_FILE_SIZE_BASE);
		// Set minimum number of write buffers to merge
		opts.set_min_write_buffer_number_to_merge(*cnf::SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
		// Use separate write thread queues
		opts.set_enable_pipelined_write(*cnf::SPEEDB_ENABLE_PIPELINED_WRITES);
		// Enable separation of keys and values
		opts.set_enable_blob_files(*cnf::SPEEDB_ENABLE_BLOB_FILES);
		// Store 4KB values separate from keys
		opts.set_min_blob_size(*cnf::SPEEDB_MIN_BLOB_SIZE);
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
				speedb::Transaction<'_, OptimisticTransactionDB>,
				speedb::Transaction<'static, OptimisticTransactionDB>,
			>(inner)
		};
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.fill_cache(true);
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Panic;
		// Create a new transaction
		Ok(Transaction {
			done: false,
			check,
			write,
			inner: Arc::new(Mutex::new(Some(inner))),
			ro,
			_db: self.db.clone(),
		})
	}
}

impl Transaction {
	/// Behaviour if unclosed
	pub(crate) fn check_level(&mut self, check: Check) {
		self.check = check;
	}
}

impl crate::kvs::api::Transaction for Transaction {
	/// Check if closed
	fn closed(&self) -> bool {
		self.done
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
		match self.inner.lock().await.take() {
			Some(inner) => inner.rollback()?,
			None => unreachable!(),
		};
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
		// Cancel this transaction
		match self.inner.lock().await.take() {
			Some(inner) => inner.commit()?,
			None => unreachable!(),
		};
		// Continue
		Ok(())
	}
	/// Check if a key exists
	async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Check the key
		let res =
			self.inner.lock().await.as_ref().unwrap().get_opt(key.into(), &self.ro)?.is_some();
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
		let res = self.inner.lock().await.as_ref().unwrap().get_opt(key.into(), &self.ro)?;
		// Return result
		Ok(res)
	}
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	async fn get_timestamp<K>(&mut self, key: K, lock: bool) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let k: Key = key.into();
		let prev = self.inner.lock().await.as_ref().unwrap().get_opt(k.clone(), &self.ro)?;
		let ver = match prev {
			Some(prev) => {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev = try_to_u64_be(array)?;
				prev + 1
			}
			None => 1,
		};

		let verbytes = u64_to_versionstamp(ver);

		self.inner.lock().await.as_ref().unwrap().put(k, verbytes)?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
	/// Obtain a new key that is suffixed with the change timestamp
	async fn get_versionstamped_key<K>(
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
		self.inner.lock().await.as_ref().unwrap().put(key.into(), val.into())?;
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	async fn put<K, V>(&mut self, category: KeyCategory, key: K, val: V) -> Result<(), Error>
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
		// Get the transaction
		let inner = self.inner.lock().await;
		let inner = inner.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		// Set the key if empty
		match inner.get_opt(&key, &self.ro)? {
			None => inner.put(key, val)?,
			_ => return Err(Error::TxKeyAlreadyExistsCategory(category)),
		};
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
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
		// Get the transaction
		let inner = self.inner.lock().await;
		let inner = inner.as_ref().unwrap();
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
		// Remove the key
		self.inner.lock().await.as_ref().unwrap().delete(key.into())?;
		// Return result
		Ok(())
	}
	/// Delete a key
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
		// Get the transaction
		let inner = self.inner.lock().await;
		let inner = inner.as_ref().unwrap();
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
	async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxFinished);
		}
		// Get the transaction
		let inner = self.inner.lock().await;
		let inner = inner.as_ref().unwrap();
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Create result set
		let mut res: Vec<(Key, Val)> = vec![];
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		// Create the iterator
		let mut iter = inner.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek(&rng.start);
		// Scan the keys in the iterator
		while iter.valid() {
			// Check the scan limit
			if res.len() < limit as usize {
				// Get the key and value
				let (k, v) = (iter.key(), iter.value());
				// Check the key and value
				if let (Some(k), Some(v)) = (k, v) {
					if k >= beg && k < end {
						res.push((k.to_vec(), v.to_vec()));
						iter.next();
						continue;
					}
				}
			}
			// Exit
			break;
		}
		// Return result
		Ok(res)
	}
}
