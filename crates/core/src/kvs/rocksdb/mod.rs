#![cfg(feature = "kv-rocksdb")]

mod cnf;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use rocksdb::{
	BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, Env, FlushOptions, LogLevel,
	OptimisticTransactionDB, OptimisticTransactionOptions, Options, ReadOptions, SstFileManager,
	WriteOptions,
};
use tokio::sync::Mutex;

use super::err::{Error, Result};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::rocksdb";

pub struct Datastore {
	/// The underlying RocksDB optimistic transaction database
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// Disk space manager for monitoring space usage and enforcing space limits
	disk_space_manager: Option<DiskSpaceManager>,
}

/// Manages disk space monitoring and enforces space limits for the RocksDB datastore.
///
/// This manager tracks SST file space usage and implements a state machine to transition
/// the datastore between normal operation and read-and-deletion-only mode based on
/// configured space limits. It provides gradual degradation of service rather than
/// abrupt failures when disk space is constrained.
#[derive(Clone)]
struct DiskSpaceManager {
	/// SST file manager for monitoring space usage
	sst_file_manager: Arc<SstFileManager>,
	/// The maximum space usage allowed for the database.
	read_and_deletion_limit: u64,
	/// The number of bytes for 80% of the allowed space usage.
	limit_80: u64,
	/// Indicates if the warning for 80% full has been logged
	warn_80_percent_logged: Arc<AtomicBool>,
}

pub struct Transaction {
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// The underlying datastore transaction
	inner: Mutex<Option<rocksdb::Transaction<'static, OptimisticTransactionDB>>>,
	/// The read options containing the Snapshot
	ro: ReadOptions,
	// The above, supposedly 'static transaction
	// actually points here, so we need to ensure
	// the memory is kept alive. This pointer must
	// be declared last, so that it is dropped last.
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// The operational state when this transaction was created.
	/// If `true`, the datastore was in read-and-deletion-only mode, so only deletions are allowed.
	/// If `false`, all write operations are permitted.
	deletion_only: bool,
	/// Tracks the types of write operations performed in this transaction.
	/// - `None`: No write operations have been performed yet
	/// - `Some(true)`: Only deletion operations have been performed
	/// - `Some(false)`: At least one non-deletion write operation has been performed
	///
	/// Used during commit to validate transactions started before the datastore entered
	/// deletion-only mode.
	contains_only_deletions: Option<bool>,
	/// Reference to the disk space manager for checking current operational state during commit.
	disk_space_manager: Option<DiskSpaceManager>,
}

impl DiskSpaceManager {
	/// Creates a new disk space manager with the specified space limit.
	///
	/// # Parameters
	/// - `limit`: The maximum allowed SST file space usage in bytes
	/// - `opts`: RocksDB options to configure with the SST file manager
	///
	/// # Implementation Details
	/// This method disables RocksDB's built-in hard limit enforcement and instead
	/// implements application-level space management at the transaction level.
	/// This approach provides more graceful degradation and allows deletions to
	/// free space even when the limit is reached.
	fn new(limit: u64, opts: &mut Options) -> Result<Self> {
		let env = Env::new()?;
		let sst_file_manager = SstFileManager::new(&env)?;
		// Disable RocksDB's built-in hard limit (set to 0 = unlimited).
		// This prevents RocksDB from blocking writes due to temporary size spikes from
		// write buffering and pending compactions. Instead, the application manages space
		// restrictions at the transaction level through state transitions, providing more
		// graceful handling and allowing deletions to free space.
		sst_file_manager.set_max_allowed_space_usage(0);
		opts.set_sst_file_manager(&sst_file_manager);
		Ok(Self {
			sst_file_manager: Arc::new(sst_file_manager),
			read_and_deletion_limit: limit,
			limit_80: (limit as f64 * 0.8) as u64,
			warn_80_percent_logged: Arc::new(AtomicBool::new(false)),
		})
	}

	/// Checks the datastore operational state based on SST file space usage.
	///
	/// This method implements a state machine that transitions between two modes:
	/// - Normal: All operations allowed (write, read, delete)
	/// - ReadAndDeletionOnly: Only read and delete operations allowed, writes are blocked
	///
	/// State transitions:
	/// - Normal → ReadAndDeletionOnly: When SST file space usage reaches the configured limit
	/// - ReadAndDeletionOnly → Normal: When space usage drops below the configured limit (after
	///   deletions and compaction free up space)
	///
	/// Returns `true` if the datastore is in read-and-deletion-only mode, `false` otherwise.
	/// When `true`, write operations will be rejected with `Error::ReadAndDeleteOnly`.
	fn is_deletion_only(&self) -> bool {
		let current_size = self.sst_file_manager.get_total_size();
		if current_size < self.limit_80 {
			self.warn_80_percent_logged.store(false, Ordering::Relaxed);
			return false;
		}
		// Use compare_exchange to atomically check and set the flag, ensuring only one thread logs
		// the warning
		if self
			.warn_80_percent_logged
			.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
			.is_ok()
		{
			warn!(target: TARGET, "SST file space usage is at 80% of the limit ({})", current_size);
		}
		// Check current size against the application limit
		// Transition to read-and-deletion-only mode when the primary limit is exceeded
		if current_size < self.read_and_deletion_limit {
			return false;
		}
		warn!(
			target: TARGET,
			"Transitioning to read-and-deletion-only mode due to primary limit ({}) being reached",
			current_size
		);
		true
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
		// Additional blob file options
		info!(target: TARGET, "Target blob file size: {}", *cnf::ROCKSDB_BLOB_FILE_SIZE);
		opts.set_blob_file_size(*cnf::ROCKSDB_BLOB_FILE_SIZE);
		if let Some(c) = cnf::ROCKSDB_BLOB_COMPRESSION_TYPE.as_ref() {
			info!(target: TARGET, "Blob compression type: {c}");
			opts.set_blob_compression_type(match c.as_str() {
				"none" => DBCompressionType::None,
				"snappy" => DBCompressionType::Snappy,
				"lz4" => DBCompressionType::Lz4,
				"zstd" => DBCompressionType::Zstd,
				l => {
					return Err(Error::Datastore(format!("Invalid compression type: {l}")));
				}
			});
		}
		info!(target: TARGET, "Enable blob garbage collection: {}", *cnf::ROCKSDB_ENABLE_BLOB_GC);
		opts.set_enable_blob_gc(*cnf::ROCKSDB_ENABLE_BLOB_GC);
		info!(target: TARGET, "Blob GC age cutoff: {}", *cnf::ROCKSDB_BLOB_GC_AGE_CUTOFF);
		opts.set_blob_gc_age_cutoff(*cnf::ROCKSDB_BLOB_GC_AGE_CUTOFF);
		info!(target: TARGET, "Blob GC force threshold: {}", *cnf::ROCKSDB_BLOB_GC_FORCE_THRESHOLD);
		opts.set_blob_gc_force_threshold(*cnf::ROCKSDB_BLOB_GC_FORCE_THRESHOLD);
		info!(target: TARGET, "Blob compaction readahead size: {}", *cnf::ROCKSDB_BLOB_COMPACTION_READAHEAD_SIZE);
		opts.set_blob_compaction_readahead_size(
			(*cnf::ROCKSDB_BLOB_COMPACTION_READAHEAD_SIZE) as u64,
		);
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
			DBCompressionType::Lz4,
			DBCompressionType::Lz4,
			DBCompressionType::Lz4,
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
				return Err(Error::Datastore(format!(
					"Invalid storage engine log level specified: {l}"
				)));
			}
		});
		// Configure SST file manager for disk space monitoring and management.
		// The SST file manager tracks SST file sizes in real-time. When the configured
		// space limit is reached, the application transitions to read-and-deletion-only mode.
		let disk_space_manager = if *cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE > 0 {
			Some(DiskSpaceManager::new(*cnf::ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE, &mut opts)?)
		} else {
			None
		};
		// Open the database
		let db = Self::open(opts.clone(), path).await?;
		// Return the datastore
		Ok(Datastore {
			db,
			disk_space_manager,
		})
	}

	/// Open database with normal configuration
	async fn open(mut opts: Options, path: &str) -> Result<Pin<Arc<OptimisticTransactionDB>>> {
		if !*cnf::ROCKSDB_BACKGROUND_FLUSH {
			// Background flush is disabled which
			// means that the WAL will be flushed
			// whenever a transaction is committed.
			// Display the configuration setting
			info!(target: TARGET, "Background write-ahead-log flushing: disabled");
			// Enable manual WAL flush
			opts.set_manual_wal_flush(false);
			// Create the optimistic datastore
			Ok(Arc::pin(OptimisticTransactionDB::open(&opts, path)?))
		} else {
			// Background flush is enabled so we
			// spawn a background worker thread to
			// flush the WAL to disk periodically.
			// Display the configuration setting
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
			Ok(db)
		}
	}

	/// Shutdown the database
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// Create new flush options
		let mut opts = FlushOptions::default();
		// Wait for the sync to finish
		opts.set_wait(true);
		// Flush the WAL to storage
		if let Err(e) = self.db.flush_wal(true) {
			error!("An error occurred flushing the WAL buffer to disk: {e}");
		}
		// Flush the memtables to SST
		if let Err(e) = self.db.flush_opt(&opts) {
			error!("An error occurred flushing memtables to SST files: {e}");
		}
		// All good
		Ok(())
	}

	/// Checks if the datastore is currently in read-and-deletion-only mode.
	///
	/// Returns `true` if the SST file space usage has exceeded the configured limit,
	/// `false` otherwise. When no space limit is configured, always returns `false`.
	fn is_deletion_only(&self) -> bool {
		self.disk_space_manager.as_ref().map(|dsm| dsm.is_deletion_only()).unwrap_or(false)
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		wo.set_sync(*cnf::SYNC_DATA);
		// Create a new transaction
		let inner = self.db.transaction_opt(&wo, &to);
		// SAFETY: The transaction lifetime is tied to the database through the _db field.
		// The database is guaranteed to outlive the transaction because:
		// 1. The transaction holds a Pin<Arc<OptimisticTransactionDB>> reference
		// 2. The transaction struct ensures _db is dropped after inner
		// 3. The Pin ensures the database isn't moved or dropped while referenced
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
		// Create a new transaction
		Ok(Box::new(Transaction {
			done: AtomicBool::new(false),
			write,
			inner: Mutex::new(Some(inner)),
			ro,
			db: self.db.clone(),
			deletion_only: self.is_deletion_only(),
			contains_only_deletions: None,
			disk_space_manager: self.disk_space_manager.clone(),
		}))
	}
}

/*impl Transaction {
	/// Validates that a read operation can be performed in the current transaction.
	///
	/// This method checks:
	/// 1. Version parameter is None (RocksDB doesn't support versioned queries)
	/// 2. Transaction is still open (not committed or cancelled)
	///
	/// Read operations are allowed in all datastore states, so no state checking is needed.
	fn ensure_read(&self, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.done.load(Ordering::Relaxed) {
			return Err(Error::TransactionFinished);
		}
		// Continue
		Ok(())
	}

	/// Validates that a write operation can be performed in the current transaction.
	///
	/// This method checks multiple conditions before allowing a write:
	/// 1. Datastore state allows writes (not in read-only or read-and-deletion-only mode)
	/// 2. Version parameter is None (RocksDB doesn't support versioned queries)
	/// 3. Transaction is still open (not committed or cancelled)
	/// 4. Transaction was created as writable
	///
	/// Returns an appropriate error if any validation fails.
	fn ensure_write(&self, version: Option<u64>) -> Result<()> {
		// if self.deletion_only {
		// 	return Err(Error::ReadAndDeleteOnly);
		// }
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.done.load(Ordering::Relaxed) {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TransactionReadonly);
		}
		// Mark this transaction as containing non-deletion operations
		// self.contains_only_deletions = Some(false);
		// Continue
		Ok(())
	}

	/// Validates that a delete operation can be performed in the current transaction.
	///
	/// This method checks conditions before allowing a deletion:
	/// 1. Version parameter is None (RocksDB doesn't support versioned queries)
	/// 2. Transaction is still open (not committed or cancelled)
	/// 3. Transaction was created as writable
	///
	/// Unlike `ensure_write()`, this method does NOT check the `deletion_only` flag,
	/// allowing deletions even in read-and-deletion-only mode. This enables space recovery
	/// when the datastore is in a restricted state due to space limits.
	///
	/// Returns an appropriate error if any validation fails.
	fn ensure_deletion(&self, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.done.load(Ordering::Relaxed) {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TransactionReadonly);
		}
		// Mark this transaction as containing only deletions if no writes have been performed yet
		// if self.contains_only_deletions.is_none() {
		// 	self.contains_only_deletions = Some(true);
		// }
		// Continue
		Ok(())
	}
}*/

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl Transactable for Transaction {
	fn kind(&self) -> &'static str {
		"rocksdb"
	}

	/// Check if closed
	fn closed(&self) -> bool {
		self.done.load(Ordering::Relaxed)
	}

	/// Check if writeable
	fn writeable(&self) -> bool {
		self.write
	}

	/// Cancel a transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn cancel(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Cancel this transaction
		inner.rollback()?;
		// Continue
		Ok(())
	}

	/// Commit a transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	async fn commit(&self) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Mark this transaction as done
		self.done.store(true, Ordering::Release);
		// Check if we are in read-and-deletion-only mode
		// This is used for long duration transactions that would have started before disk
		// conditions changed
		if let Some(disk_space_manager) = self.disk_space_manager.as_ref() {
			if disk_space_manager.is_deletion_only() && self.contains_only_deletions == Some(false)
			{
				return Err(Error::ReadAndDeleteOnly);
			}
		}
		// Get the inner transaction
		let inner = self
			.inner
			.lock()
			.await
			.take()
			.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Commit this transaction
		inner.commit()?;
		// If transaction was created in read-and-deletion-only mode, trigger compaction to reclaim
		// disk space from deleted keys. This helps the datastore transition back to normal mode
		// when space usage drops below the limit.
		if self.deletion_only {
			self.db.compact_range::<&[u8], &[u8]>(None, None);
		}
		// Continue
		Ok(())
	}

	/// Check if a key exists.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn exists(&self, key: Key, version: Option<u64>) -> Result<bool> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the key
		let res = inner.get_pinned_opt(key, &self.ro)?.is_some();
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn get(&self, key: Key, version: Option<u64>) -> Result<Option<Val>> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the key
		let res = inner.get_opt(key, &self.ro)?;
		// Return result
		Ok(res)
	}

	/// Fetch many keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	async fn getm(&self, keys: Vec<Key>) -> Result<Vec<Option<Val>>> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the keys
		let res = inner.multi_get_opt(keys, &self.ro);
		// Convert result
		let res = res.into_iter().map(|r| r.map_err(Into::into)).collect::<Result<_>>()?;
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn set(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Set the key
		inner.put(key, val)?;
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn put(&self, key: Key, val: Val, version: Option<u64>) -> Result<()> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Set the key if empty
		match inner.get_pinned_opt(&key, &self.ro)? {
			None => inner.put(key, val)?,
			_ => return Err(Error::TransactionKeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Set the key if empty
		match (inner.get_pinned_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v.eq(&w) => inner.put(key, val)?,
			(None, None) => inner.put(key, val)?,
			_ => return Err(Error::TrandsactionConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Delete a key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn del(&self, key: Key) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Remove the key
		inner.delete(key)?;
		// Return result
		Ok(())
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	async fn delc(&self, key: Key, chk: Option<Val>) -> Result<()> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Check to see if transaction is writable
		if !self.writeable() {
			return Err(Error::TransactionReadonly);
		}
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Delete the key if valid
		match (inner.get_pinned_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v.eq(&w) => inner.delete(key)?,
			(None, None) => inner.delete(key)?,
			_ => return Err(Error::TrandsactionConditionNotMet),
		};
		// Return result
		Ok(())
	}

	/// Count the total number of keys within a range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn count(&self, rng: Range<Key>) -> Result<usize> {
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Execute on the blocking threadpool
		let res = affinitypool::spawn_local(move || -> Result<_> {
			// Set the key range
			let beg = rng.start.as_slice();
			let end = rng.end.as_slice();
			// Load the inner transaction
			let inner = self.inner.blocking_lock();
			// Get the inner transaction
			let inner =
				inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			// Set the ReadOptions with the snapshot
			let mut ro = ReadOptions::default();
			ro.set_snapshot(&inner.snapshot());
			ro.set_iterate_lower_bound(beg);
			ro.set_iterate_upper_bound(end);
			ro.set_async_io(true);
			ro.fill_cache(false);
			// Create the iterator
			let mut iter = inner.raw_iterator_opt(ro);
			// Seek to the start key
			iter.seek(&rng.start);
			// Count the items
			let mut res = 0;
			while iter.valid() {
				res += 1;
				iter.next();
			}
			// Drop the iterator
			drop(iter);
			// Return result
			Ok(res)
		})
		.await?;
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keys(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Create result set
		let mut res = Vec::with_capacity(limit as usize);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
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
		iter.seek(&rng.start);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some(k) = iter.key() {
				res.push(k.to_vec());
				iter.next();
			} else {
				break;
			}
		}
		// Drop the iterator
		drop(iter);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn keysr(&self, rng: Range<Key>, limit: u32, version: Option<u64>) -> Result<Vec<Key>> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Create result set
		let mut res = Vec::with_capacity(limit as usize);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
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
				res.push(k.to_vec());
				iter.prev();
			} else {
				break;
			}
		}
		// Drop the iterator
		drop(iter);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scan(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Create result set
		let mut res = Vec::with_capacity(limit as usize);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
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
		iter.seek(&rng.start);
		// Check the scan limit
		while res.len() < limit as usize {
			// Check the key and value
			if let Some((k, v)) = iter.item() {
				res.push((k.to_vec(), v.to_vec()));
				iter.next();
			} else {
				break;
			}
		}
		// Drop the iterator
		drop(iter);
		// Return result
		Ok(res)
	}

	/// Retrieve a range of key-value pairs, in reverse.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	async fn scanr(
		&self,
		rng: Range<Key>,
		limit: u32,
		version: Option<u64>,
	) -> Result<Vec<(Key, Val)>> {
		// RocksDB does not support versioned queries.
		if version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		// Check to see if transaction is closed
		if self.closed() {
			return Err(Error::TransactionFinished);
		}
		// Create result set
		let mut res = Vec::with_capacity(limit as usize);
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Lock the inner transaction
		let inner = self.inner.lock().await;
		// Get the inner transaction
		let inner =
			inner.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
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
				res.push((k.to_vec(), v.to_vec()));
				iter.prev();
			} else {
				break;
			}
		}
		// Drop the iterator
		drop(iter);
		// Return result
		Ok(res)
	}

	/// Set a new save point on the transaction.
	async fn new_save_point(&self) -> Result<()> {
		let inner = self.inner.lock().await;
		if let Some(inner) = inner.as_ref() {
			inner.set_savepoint();
		}
		Ok(())
	}

	/// Rollback to the last save point.
	async fn rollback_to_save_point(&self) -> Result<()> {
		let inner = self.inner.lock().await;
		if let Some(inner) = inner.as_ref() {
			inner.rollback_to_savepoint()?;
		}
		Ok(())
	}

	/// Release the last save point.
	async fn release_last_save_point(&self) -> Result<()> {
		Ok(())
	}
}

// impl Transaction {
// 	/// Check if an error is related to Out of Disk (OOD) conditions
// 	fn is_ood_error(error: &anyhow::Error) -> bool {
// 		let error_msg = error.to_string().to_lowercase();
// 		error_msg.contains("no space left on device")
// 			|| error_msg.contains("disk full")
// 			|| error_msg.contains("out of space")
// 			|| error_msg.contains("enospc")
// 	}

// 	/// Log OOD error with appropriate context
// 	fn log_ood_error(error: &anyhow::Error, context: &str) {
// 		error!(target: TARGET, "Out of Disk error during {}: {}", context, error);
// 		warn!(target: TARGET, "Database may enter read-only mode until disk space is available");
// 	}
// }
