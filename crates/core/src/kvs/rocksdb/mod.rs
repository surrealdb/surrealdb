#![cfg(feature = "kv-rocksdb")]

mod cnf;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Result, bail, ensure};
use rocksdb::{
	BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, FlushOptions, LogLevel,
	OptimisticTransactionDB, OptimisticTransactionOptions, Options, ReadOptions, WriteOptions,
};

use super::savepoint::SavePoints;
use crate::err::Error;
use crate::key::debug::Sprintable;
use crate::kvs::{Check, Key, Val};

const TARGET: &str = "surrealdb::core::kvs::rocksdb";

pub struct Datastore {
	db: Pin<Arc<OptimisticTransactionDB>>,
}

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
	pub(crate) async fn new(path: &str) -> Result<Datastore> {
		// Configure custom options
		let mut opts = Options::default();
		// Ensure we use fdatasync
		opts.set_use_fsync(false);
		// Create database if missing
		opts.create_if_missing(true);
		// Create column families if missing
		opts.create_missing_column_families(true);
		// Increase the background thread count
		info!(target: TARGET, "Background thread count: {}", *cnf::ROCKSDB_THREAD_COUNT);
		opts.increase_parallelism(*cnf::ROCKSDB_THREAD_COUNT);
		// Specify the max concurrent background jobs
		info!(target: TARGET, "Maximum background jobs count: {}", *cnf::ROCKSDB_JOBS_COUNT);
		opts.set_max_background_jobs(*cnf::ROCKSDB_JOBS_COUNT);
		// Set the maximum number of open files that can be used by the database
		info!(target: TARGET, "Maximum number of open files: {}", *cnf::ROCKSDB_MAX_OPEN_FILES);
		opts.set_max_open_files(*cnf::ROCKSDB_MAX_OPEN_FILES);
		// Set the number of log files to keep
		info!(target: TARGET, "Number of log files to keep: {}", *cnf::ROCKSDB_KEEP_LOG_FILE_NUM);
		opts.set_keep_log_file_num(*cnf::ROCKSDB_KEEP_LOG_FILE_NUM);
		// Set the maximum number of write buffers
		info!(target: TARGET, "Maximum write buffers: {}", *cnf::ROCKSDB_MAX_WRITE_BUFFER_NUMBER);
		opts.set_max_write_buffer_number(*cnf::ROCKSDB_MAX_WRITE_BUFFER_NUMBER);
		// Set the amount of data to build up in memory
		info!(target: TARGET, "Write buffer size: {}", *cnf::ROCKSDB_WRITE_BUFFER_SIZE);
		opts.set_write_buffer_size(*cnf::ROCKSDB_WRITE_BUFFER_SIZE);
		// Set the target file size for compaction
		info!(target: TARGET, "Target file size for compaction: {}", *cnf::ROCKSDB_TARGET_FILE_SIZE_BASE);
		opts.set_target_file_size_base(*cnf::ROCKSDB_TARGET_FILE_SIZE_BASE);
		// Set the levelled target file size multipler
		info!(target: TARGET, "Target file size compaction multiplier: {}", *cnf::ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER);
		opts.set_target_file_size_multiplier(*cnf::ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER);
		// Set minimum number of write buffers to merge
		info!(target: TARGET, "Minimum write buffers to merge: {}", *cnf::ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
		opts.set_min_write_buffer_number_to_merge(*cnf::ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
		// Delay compaction until the minimum number of files accumulate
		info!(target: TARGET, "Number of files to trigger compaction: {}", *cnf::ROCKSDB_FILE_COMPACTION_TRIGGER);
		opts.set_level_zero_file_num_compaction_trigger(*cnf::ROCKSDB_FILE_COMPACTION_TRIGGER);
		// Set the compaction readahead size
		info!(target: TARGET, "Compaction readahead size: {}", *cnf::ROCKSDB_COMPACTION_READAHEAD_SIZE);
		opts.set_compaction_readahead_size(*cnf::ROCKSDB_COMPACTION_READAHEAD_SIZE);
		// Set the max number of subcompactions
		info!(target: TARGET, "Maximum concurrent subcompactions: {}", *cnf::ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS);
		opts.set_max_subcompactions(*cnf::ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS);
		// Use separate write thread queues
		info!(target: TARGET, "Use separate thread queues: {}", *cnf::ROCKSDB_ENABLE_PIPELINED_WRITES);
		opts.set_enable_pipelined_write(*cnf::ROCKSDB_ENABLE_PIPELINED_WRITES);
		// Enable separation of keys and values
		info!(target: TARGET, "Enable separation of keys and values: {}", *cnf::ROCKSDB_ENABLE_BLOB_FILES);
		opts.set_enable_blob_files(*cnf::ROCKSDB_ENABLE_BLOB_FILES);
		// Store large values separate from keys
		info!(target: TARGET, "Minimum blob value size: {}", *cnf::ROCKSDB_MIN_BLOB_SIZE);
		opts.set_min_blob_size(*cnf::ROCKSDB_MIN_BLOB_SIZE);
		// Set the write-ahead-log size limit in MB
		info!(target: TARGET, "Write-ahead-log file size limit: {}MB", *cnf::ROCKSDB_WAL_SIZE_LIMIT);
		opts.set_wal_size_limit_mb(*cnf::ROCKSDB_WAL_SIZE_LIMIT);
		// Allow multiple writers to update memtables in parallel
		info!(target: TARGET, "Allow concurrent memtable writes: true");
		opts.set_allow_concurrent_memtable_write(true);
		// Avoid unnecessary blocking io, preferring background threads
		info!(target: TARGET, "Avoid unnecessary blocking IO: true");
		opts.set_avoid_unnecessary_blocking_io(true);
		// Improve concurrency from write batch mutex
		info!(target: TARGET, "Allow adaptive write thread yielding: true");
		opts.set_enable_write_thread_adaptive_yield(true);
		// Log if writes should be synced
		info!(target: TARGET, "Wait for disk sync acknowledgement: {}", *cnf::SYNC_DATA);
		// Set the block cache size in bytes
		info!(target: TARGET, "Block cache size: {}", *cnf::ROCKSDB_BLOCK_CACHE_SIZE);
		// Configure the in-memory cache options
		let cache = Cache::new_lru_cache(*cnf::ROCKSDB_BLOCK_CACHE_SIZE);
		// Configure the block based file options
		let mut block_opts = BlockBasedOptions::default();
		block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
		block_opts.set_pin_top_level_index_and_filter(true);
		block_opts.set_bloom_filter(10.0, false);
		block_opts.set_block_size(*cnf::ROCKSDB_BLOCK_SIZE);
		block_opts.set_block_cache(&cache);
		// Configure the database with the cache
		opts.set_block_based_table_factory(&block_opts);
		opts.set_blob_cache(&cache);
		opts.set_row_cache(&cache);
		// Configure memory-mapped reads
		info!(target: TARGET, "Enable memory-mapped reads: {}", *cnf::ROCKSDB_ENABLE_MEMORY_MAPPED_READS);
		opts.set_allow_mmap_reads(*cnf::ROCKSDB_ENABLE_MEMORY_MAPPED_READS);
		// Configure memory-mapped writes
		info!(target: TARGET, "Enable memory-mapped writes: {}", *cnf::ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES);
		opts.set_allow_mmap_writes(*cnf::ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES);
		// Set the delete compaction factory
		info!(target: TARGET, "Setting delete compaction factory: {} / {} ({})",
			*cnf::ROCKSDB_DELETION_FACTORY_WINDOW_SIZE,
			*cnf::ROCKSDB_DELETION_FACTORY_DELETE_COUNT,
			*cnf::ROCKSDB_DELETION_FACTORY_RATIO,
		);
		opts.add_compact_on_deletion_collector_factory(
			*cnf::ROCKSDB_DELETION_FACTORY_WINDOW_SIZE,
			*cnf::ROCKSDB_DELETION_FACTORY_DELETE_COUNT,
			*cnf::ROCKSDB_DELETION_FACTORY_RATIO,
		);
		// Set the datastore compaction style
		info!(target: TARGET, "Setting compaction style: {}", *cnf::ROCKSDB_COMPACTION_STYLE);
		opts.set_compaction_style(
			match cnf::ROCKSDB_COMPACTION_STYLE.to_ascii_lowercase().as_str() {
				"universal" => DBCompactionStyle::Universal,
				_ => DBCompactionStyle::Level,
			},
		);
		// Set specific compression levels
		info!(target: TARGET, "Setting compression level");
		opts.set_compression_per_level(&[
			DBCompressionType::None,
			DBCompressionType::None,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
			DBCompressionType::Snappy,
		]);
		// Set specific storage log level
		info!(target: TARGET, "Setting storage engine log level: {}", *cnf::ROCKSDB_STORAGE_LOG_LEVEL);
		opts.set_log_level(match cnf::ROCKSDB_STORAGE_LOG_LEVEL.to_ascii_lowercase().as_str() {
			"debug" => LogLevel::Debug,
			"info" => LogLevel::Info,
			"warn" => LogLevel::Warn,
			"error" => LogLevel::Error,
			"fatal" => LogLevel::Fatal,
			l => {
				bail!(Error::Ds(format!("Invalid storage engine log level specified: {l}")));
			}
		});
		// Configure background WAL flush behaviour
		let db = match *cnf::ROCKSDB_BACKGROUND_FLUSH {
			// Beckground flush is disabled which
			// means that the WAL will be flushed
			// whenever a transaction is committed.
			false => {
				// Dispay the configuration setting
				info!(target: TARGET, "Background write-ahead-log flushing: disabled");
				// Enable manual WAL flush
				opts.set_manual_wal_flush(false);
				// Create the optimistic datastore
				Arc::pin(OptimisticTransactionDB::open(&opts, path)?)
			}
			// Background flush is enabled so we
			// spawn a background worker thread to
			// flush the WAL to disk periodically.
			true => {
				// Dispay the configuration setting
				info!(target: TARGET, "Background write-ahead-log flushing: enabled every {}ms", *cnf::ROCKSDB_BACKGROUND_FLUSH_INTERVAL);
				// Enable manual WAL flush
				opts.set_manual_wal_flush(true);
				// Create the optimistic datastore
				let db = Arc::pin(OptimisticTransactionDB::open(&opts, path)?);
				// Clone the database reference
				let dbc = db.clone();
				// Create a new background thread
				thread::spawn(move || {
					loop {
						// Get the specified flush interval
						let wait = *cnf::ROCKSDB_BACKGROUND_FLUSH_INTERVAL;
						// Wait for the specified interval
						thread::sleep(Duration::from_millis(wait));
						// Flush the WAL to disk periodically
						if let Err(err) = dbc.flush_wal(*cnf::SYNC_DATA) {
							error!("Failed to flush WAL: {err}");
						}
					}
				});
				// Return the datastore
				db
			}
		};
		// Return the datastore
		Ok(Datastore {
			db,
		})
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Create new flush options
		let mut opts = FlushOptions::default();
		// Wait for the sync to finish
		opts.set_wait(true);
		// Flush the WAL to storage
		if let Err(e) = self.db.flush_wal(true) {
			error!("An error occured flushing the WAL buffer to disk: {e}");
		}
		// Flush the memtables to SST
		if let Err(e) = self.db.flush_opt(&opts) {
			error!("An error occured flushing memtables to SST files: {e}");
		}
		// All good
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(
		&self,
		write: bool,
		_: bool,
	) -> Result<Box<dyn crate::kvs::api::Transaction>> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(*cnf::SYNC_DATA);
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
		// Set the read options
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Specify the check level
		#[cfg(not(debug_assertions))]
		let check = Check::Warn;
		#[cfg(debug_assertions)]
		let check = Check::Error;
		// Create a new transaction
		Ok(Box::new(Transaction {
			done: false,
			write,
			check,
			inner: Some(inner),
			ro,
			_db: self.db.clone(),
		}))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl super::api::Transaction for Transaction {
	fn kind(&self) -> &'static str {
		"rocksdb"
	}

	fn supports_reverse_scan(&self) -> bool {
		true
	}

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
	async fn cancel(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Mark this transaction as done
		self.done = true;
		// Cancel this transaction
		self.inner.as_ref().unwrap().rollback()?;
		// Continue
		Ok(())
	}

	/// Commit a transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&mut self) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Mark this transaction as done
		self.done = true;
		// Commit this transaction
		self.inner.take().unwrap().commit()?;
		// Continue
		Ok(())
	}

	/// Check if a key exists
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&mut self, key: Key, version: Option<u64>) -> Result<bool> {
		// RocksDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the key
		let res = self.inner.as_ref().unwrap().get_pinned_opt(key, &self.ro)?.is_some();
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&mut self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// RocksDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the key
		let res = self.inner.as_ref().unwrap().get_opt(key, &self.ro)?;
		// Return result
		Ok(res)
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	async fn getm(&mut self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
		// Check to see if transaction is closed
		ensure!(!self.closed(), Error::TxFinished);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Get the keys
		let res = self.inner.as_ref().unwrap().multi_get_opt(keys, &self.ro);
		// Convert result
		let res = res.into_iter().collect::<Result<_, _>>()?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key
		self.inner.as_ref().unwrap().put(key, val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&mut self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key if empty
		match self.inner.as_ref().unwrap().get_pinned_opt(&key, &self.ro)? {
			None => self.inner.as_ref().unwrap().put(key, val)?,
			_ => bail!(Error::TxKeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&mut self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Set the key if empty
		match (self.inner.as_ref().unwrap().get_pinned_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v.eq(&w) => self.inner.as_ref().unwrap().put(key, val)?,
			(None, None) => self.inner.as_ref().unwrap().put(key, val)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&mut self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Remove the key
		self.inner.as_ref().unwrap().delete(key)?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&mut self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		// Check to see if transaction is writable
		ensure!(self.write, Error::TxReadonly);
		// Delete the key if valid
		match (self.inner.as_ref().unwrap().get_pinned_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v.eq(&w) => self.inner.as_ref().unwrap().delete(key)?,
			(None, None) => self.inner.as_ref().unwrap().delete(key)?,
			_ => bail!(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		let rng = self.prepare_scan(rng, version).await?;
		// Execute on the blocking threadpool
		let res = affinitypool::spawn_local(move || {
			// Create result set
			let mut res = vec![];
			// Set the key range
			let beg = rng.start.as_slice();
			let end = rng.end.as_slice();
			// Set the ReadOptions with the snapshot
			let mut ro = ReadOptions::default();
			ro.set_snapshot(&self.inner.as_ref().unwrap().snapshot());
			ro.set_iterate_lower_bound(beg);
			ro.set_iterate_upper_bound(end);
			ro.set_async_io(true);
			ro.fill_cache(true);
			// Create the iterator
			let mut iter = self.inner.as_ref().unwrap().raw_iterator_opt(ro);
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
			// Drop the iterator
			drop(iter);
			// Return result
			res
		})
		.await;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the databases
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<Key>> {
		let rng = self.prepare_scan(rng, version).await?;
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Create result set
		let mut res = vec![];
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_iterate_lower_bound(beg);
		ro.set_iterate_upper_bound(end);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create the iterator
		let mut iter = inner.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek_for_prev(&rng.end);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some(k) = iter.key() {
				// Check the range validity
				if k >= beg && k < end {
					res.push(k.to_vec());
					iter.prev();
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
	async fn scan(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		let rng = self.prepare_scan(rng, version).await?;
		// Execute on the blocking threadpool
		let res = affinitypool::spawn_local(move || {
			// Create result set
			let mut res = vec![];
			// Set the key range
			let beg = rng.start.as_slice();
			let end = rng.end.as_slice();
			// Set the ReadOptions with the snapshot
			let mut ro = ReadOptions::default();
			ro.set_snapshot(&self.inner.as_ref().unwrap().snapshot());
			ro.set_iterate_lower_bound(beg);
			ro.set_iterate_upper_bound(end);
			ro.set_async_io(true);
			ro.fill_cache(true);
			// Create the iterator
			let mut iter = self.inner.as_ref().unwrap().raw_iterator_opt(ro);
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
			// Drop the iterator
			drop(iter);
			// Return result
			res
		})
		.await;
		// Return result
		Ok(res)
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&mut self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		let rng = self.prepare_scan(rng, version).await?;
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Create result set
		let mut res = vec![];
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot());
		ro.set_iterate_lower_bound(beg);
		ro.set_iterate_upper_bound(end);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// Create the iterator
		let mut iter = inner.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek_for_prev(&rng.end);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some((k, v)) = iter.item() {
				// Check the range validity
				if k >= beg && k < end {
					res.push((k.to_vec(), v.to_vec()));
					iter.prev();
					continue;
				}
			}
			// Exit
			break;
		}
		// Return result
		Ok(res)
	}

	fn get_save_points(&mut self) -> &mut SavePoints {
		unimplemented!("Get save points not implemented for for the RocksDB backend");
	}

	fn new_save_point(&mut self) {
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Set the save point
		inner.set_savepoint();
	}

	async fn rollback_to_save_point(&mut self) -> Result<()> {
		// Get the transaction
		let inner = self.inner.as_ref().unwrap();
		// Rollback
		inner.rollback_to_savepoint()?;
		//
		Ok(())
	}

	fn release_last_save_point(&mut self) -> Result<()> {
		Ok(())
	}
}

impl Transaction {
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn prepare_scan(&mut self, rng: Range<Key>, version: Option<u64>) -> Result<Range<Key>> {
		// RocksDB does not support versioned queries.
		ensure!(version.is_none(), Error::UnsupportedVersionedQueries);
		// Check to see if transaction is closed
		ensure!(!self.done, Error::TxFinished);
		Ok(rng)
	}
}
