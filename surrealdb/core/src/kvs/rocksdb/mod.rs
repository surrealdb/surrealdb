#![cfg(feature = "kv-rocksdb")]
// Belt-and-suspenders against regressing the per-Transaction `inner` mutex
// back to a synchronous type (parking_lot or std). Tokio mutex guards are not
// flagged by this lint, so the current `inner.lock().await` sites are fine.
#![deny(clippy::await_holding_lock)]

mod background_flusher;
mod cnf;
mod commit_coordinator;
mod comparator;
mod disk_space_manager;
mod garbage_collector;
mod inline_guard;
mod memory_manager;
mod prefix_extractor;
mod range_shard;
mod scan_cursor;
#[cfg(test)]
mod tests;

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use background_flusher::BackgroundFlusher;
pub use cnf::RocksDbConfig;
use commit_coordinator::CommitCoordinator;
use disk_space_manager::{DiskSpaceManager, DiskSpaceState, TransactionState};
use garbage_collector::GarbageCollector;
use inline_guard::InlineGuard;
use memory_manager::MemoryManager;
use range_shard::{COUNT_PARALLEL_MAX_SHARDS, shard_range};
use rocksdb::{
	BottommostLevelCompaction, ColumnFamilyDescriptor, CompactOptions, DBCompactionStyle,
	DBCompressionType, DBRawIteratorWithThreadMode, FlushOptions, LogLevel,
	OptimisticTransactionDB, OptimisticTransactionOptions, Options, ReadOptions,
	SnapshotWithThreadMode, UniversalCompactOptions, UniversalCompactionStopStyle,
	WaitForCompactOptions, WriteOptions, properties,
};
use scan_cursor::{
	AliveGuard, RocksDbKeysCursor, RocksDbValsCursor, ScanIter, ScanStateKeys, ScanStateVals,
};
use tokio::sync::{Mutex, MutexGuard};
use web_time::Instant;

use super::api::{
	BoxFut, GetMultiResult, KeySpan, KeyValSpan, KeysBatch, KeysResult, ScanCursorKeys,
	ScanCursorVals, ScanLimit, ScanResult, ValsBatch,
};
use super::config::SyncMode;
use super::err::{Error, Result};
use super::{Direction, ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV};
use crate::key::debug::Sprintable;
use crate::kvs::api::Transactable;
use crate::kvs::ds::{Metric, Metrics};
use crate::kvs::timestamp::HlcTimeStamp;
use crate::kvs::{Key, Val};

const TARGET: &str = "surrealdb::core::kvs::rocksdb";

pub struct Datastore {
	/// The underlying RocksDB optimistic transaction database
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// Whether user-defined timestamps (versioning) are enabled
	versioned: bool,
	/// Memory manager for managing memory usage
	memory_manager: Arc<MemoryManager>,
	/// Disk space manager for monitoring space usage and enforcing space limits
	disk_space_manager: Option<Arc<DiskSpaceManager>>,
	/// Commit coordinator for batching transaction commits when sync is enabled
	commit_coordinator: Option<Arc<CommitCoordinator>>,
	/// Background flusher for periodically flushing WAL to disk
	background_flusher: Option<Arc<BackgroundFlusher>>,
	/// Garbage collector for advancing the version GC watermark
	garbage_collector: Option<Arc<GarbageCollector>>,
	/// Whether the custom SurrealDB prefix extractor is enabled on the
	/// column family. Propagated to each transaction so range scans can
	/// pick the correct prefix-seek mode without consulting global state.
	prefix_extractor_enabled: bool,
	/// Whether scan/count `ReadOptions` set `verify_checksums(true)`.
	/// When false, CRC32C verification is skipped on cold block reads.
	scan_verify_checksums: bool,
	/// Whether `shutdown` should run a full-keyspace compaction (down to
	/// the bottommost level) after flushing memtables. Off by default —
	/// the operation is O(database size) and is intended for maintenance
	/// shutdowns rather than the SIGTERM-grace-period path.
	compact_on_shutdown: bool,
	/// Timeout (seconds) for the post-flush `wait_for_compact` step
	/// during shutdown. `0` waits indefinitely.
	shutdown_wait_for_compact_seconds: u64,
	/// Per-datastore starvation guard: bounds how many tokio workers
	/// may run a synchronous RocksDB call inline at any moment, sized
	/// from the configured tokio runtime worker count. Cloned (as an
	/// `Arc`) into every `Transaction` so the dispatch helpers can
	/// probe / divert without going through global state.
	inline_guard: Arc<InlineGuard>,
}

pub struct Transaction {
	/// Number of open caller-owned cursors. Each `open_*_cursor`
	/// increments after passing the `done` check; each cursor handle's
	/// `AliveGuard` decrements on `Drop`. `commit`/`cancel` drain-wait
	/// on this counter before consuming `inner` so that no cursor's
	/// iterator can outlive the snapshot/tx it references.
	///
	/// See `scan_cursor.rs` for the safety contract and SeqCst
	/// ordering rationale.
	pub(super) cursors_alive: AtomicUsize,
	/// Is the transaction complete?
	done: AtomicBool,
	/// Is the transaction writeable?
	write: bool,
	/// Whether user-defined timestamps (versioning) are enabled
	versioned: bool,
	/// The read options containing the snapshot
	read_options: ReadOptions,
	/// The inner transaction and the transaction snapshot.
	///
	/// `tokio::sync::Mutex` so a contending tokio worker yields (the runtime
	/// can run other tasks while it waits) rather than parks during slow
	/// critical sections — cold-cache `get_opt`, inline scans, and the
	/// writable `count` path can each hold the guard for tens of ms to
	/// seconds. With a synchronous mutex the contending worker would be
	/// blocked for that whole window; with this one it yields.
	///
	/// Tokio's `Mutex` is **not reentrant**: a recursive `self.inner.lock()`
	/// from inside a method already holding the guard will hang the future.
	/// Callers must still acquire the lock once and pass the guard down to
	/// helpers (`count_blocking`, `keys_blocking`, `scan_blocking`) rather
	/// than re-acquiring it.
	///
	/// For `affinitypool::spawn_local` dispatches: acquire `.lock().await`
	/// *before* `spawn_local(...)` and move the `MutexGuard` into the closure.
	/// `MutexGuard<'_, Option<TransactionInner>>` is `Send` (because
	/// `TransactionInner: Send`), and `spawn_local`'s `'pool` lifetime
	/// admits non-`'static` borrows from `&self`.
	inner: Mutex<Option<TransactionInner>>,
	/// The current transaction state
	transaction_state: Arc<AtomicU8>,
	/// Reference to the disk space manager for checking current operational state during commit.
	disk_space_manager: Option<Arc<DiskSpaceManager>>,
	/// Commit coordinator for batching transaction commits when sync writes are enabled
	commit_coordinator: Option<Arc<CommitCoordinator>>,
	/// The above, supposedly 'static transaction actually points here, so we
	/// need to ensure the memory is kept alive. This pointer must be declared
	/// last, so that it is dropped last.
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// Whether the custom SurrealDB prefix extractor is enabled on the
	/// column family. Controls whether `apply_prefix_mode` sets
	/// `prefix_same_as_start` / `total_order_seek` on scan `ReadOptions`.
	prefix_extractor_enabled: bool,
	/// Whether scan/count `ReadOptions` set `verify_checksums(true)`.
	scan_verify_checksums: bool,
	/// Per-datastore starvation guard (cloned from the parent
	/// `Datastore`). All dispatch helpers — `run_blocking`, the cursor
	/// batch pump, commit's fsync — route through this so the
	/// inline-vs-offload decision and metric counters are shared across
	/// every transaction on this datastore.
	inline_guard: Arc<InlineGuard>,
}

impl Transaction {
	fn ensure_versioned(&self, version: Option<u64>) -> Result<()> {
		if !self.versioned && version.is_some() {
			return Err(Error::UnsupportedVersionedQueries);
		}
		Ok(())
	}
}

/// The rocksdb transaction and its pre-captured snapshot, bundled together so
/// that the snapshot is always dropped before the transaction it borrows from.
///
/// `snapshot` holds a `'static` reference into the boxed transaction: the
/// RocksDB snapshot's `db` field points at the `Transaction` allocation **inside**
/// `tx: Box<...>`, which has a stable address while `TransactionInner` is moved
/// (only the `Box` pointer moves, not the heap allocation). `tx` must outlive
/// `snapshot`. Two paths guarantee this:
///
/// * On natural drop, struct fields drop in declaration order. `snapshot` is declared before `tx`
///   and therefore drops first — while `tx` is still alive.
/// * On commit, the commit path destructures this struct, drops `snapshot` first, then consumes the
///   boxed transaction with `(*inner).commit()` (where `inner` is the
///   `Box<rocksdb::Transaction<…>>`).
struct TransactionInner {
	/// The snapshot for the underlying datastore transaction.
	///
	/// Declared before `tx` so it drops first on natural drop: the snapshot
	/// must be released before the boxed `Transaction` is destroyed.
	snapshot:
		SnapshotWithThreadMode<'static, rocksdb::Transaction<'static, OptimisticTransactionDB>>,
	/// The underlying datastore transaction (boxed so `snapshot` can safely
	/// hold a `&Transaction` into a stable allocation when `TransactionInner` moves).
	tx: Box<rocksdb::Transaction<'static, OptimisticTransactionDB>>,
}

/// Apply the column-family-level RocksDB options derived from `config`
/// to `target`. Keeping these grouped in one helper means that when
/// versioning is enabled - and the default CF is opened via an
/// explicit `ColumnFamilyDescriptor` with its own options - the
/// explicit CF receives the same compaction, blob, compression and
/// prefix-filter settings as the implicit default CF would have
/// inherited from the main `opts`.
fn apply_cf_level_options(target: &mut Options, config: &RocksDbConfig) {
	// Set the target file size for compaction
	info!(target: TARGET, "Target file size for compaction: {}", config.target_file_size_base);
	target.set_target_file_size_base(config.target_file_size_base);
	// Set the levelled target file size multipler
	let size_multiplier = config.target_file_size_multiplier.min(i32::MAX as usize) as i32;
	info!(target: TARGET, "Target file size compaction multiplier: {size_multiplier}");
	target.set_target_file_size_multiplier(size_multiplier);
	// Delay compaction until the minimum number of files accumulate
	let compaction_trigger = config.file_compaction_trigger.min(i32::MAX as usize) as i32;
	info!(target: TARGET, "Number of files to trigger compaction: {compaction_trigger}");
	target.set_level_zero_file_num_compaction_trigger(compaction_trigger);
	// L0 file-count watermark at which writes start to self-throttle.
	info!(target: TARGET, "Level-0 slowdown writes trigger: {}", config.level0_slowdown_writes_trigger);
	target.set_level_zero_slowdown_writes_trigger(config.level0_slowdown_writes_trigger);
	// L0 file-count watermark at which writes are halted entirely.
	info!(target: TARGET, "Level-0 stop writes trigger: {}", config.level0_stop_writes_trigger);
	target.set_level_zero_stop_writes_trigger(config.level0_stop_writes_trigger);
	// Periodic compaction safety net for cold ranges.
	if config.periodic_compaction_seconds > 0 {
		info!(target: TARGET, "Periodic compaction seconds: {}", config.periodic_compaction_seconds);
		target.set_periodic_compaction_seconds(config.periodic_compaction_seconds);
	} else {
		info!(target: TARGET, "Periodic compaction: disabled");
	}
	// Enable separation of keys and values
	info!(target: TARGET, "Enable separation of keys and values: {}", config.enable_blob_files);
	target.set_enable_blob_files(config.enable_blob_files);
	// Store large values separate from keys
	info!(target: TARGET, "Minimum blob value size: {}", config.min_blob_size);
	target.set_min_blob_size(config.min_blob_size);
	// Additional blob file options
	info!(target: TARGET, "Target blob file size: {}", config.blob_file_size);
	target.set_blob_file_size(config.blob_file_size);
	// Set the blob compression type
	let (db_compression, name) = match config.blob_compression_type {
		cnf::BlobCompression::Snappy => (DBCompressionType::Snappy, "snappy"),
		cnf::BlobCompression::Lz4 => (DBCompressionType::Lz4, "lz4"),
		cnf::BlobCompression::Zstd => (DBCompressionType::Zstd, "zstd"),
		cnf::BlobCompression::None => (DBCompressionType::None, "none"),
	};
	info!(target: TARGET, "Blob compression type: {name}");
	target.set_blob_compression_type(db_compression);
	// Whether to enable blob garbage collection
	info!(target: TARGET, "Enable blob garbage collection: {}", config.enable_blob_gc);
	target.set_enable_blob_gc(config.enable_blob_gc);
	// Set the blob garbage collection age cutoff
	info!(target: TARGET, "Blob GC age cutoff: {}", config.blob_gc_age_cutoff);
	target.set_blob_gc_age_cutoff(config.blob_gc_age_cutoff);
	// Set the blob garbage collection force threshold
	info!(target: TARGET, "Blob GC force threshold: {}", config.blob_gc_force_threshold);
	target.set_blob_gc_force_threshold(config.blob_gc_force_threshold);
	// Set the blob compaction readahead size
	info!(target: TARGET, "Blob compaction readahead size: {}", config.blob_compaction_readahead_size);
	target.set_blob_compaction_readahead_size(config.blob_compaction_readahead_size);
	// Set the delete compaction factory
	info!(target: TARGET, "Setting delete compaction factory: {} / {} ({})",
		config.deletion_factory_window_size,
		config.deletion_factory_delete_count,
		config.deletion_factory_ratio,
	);
	target.add_compact_on_deletion_collector_factory(
		config.deletion_factory_window_size,
		config.deletion_factory_delete_count,
		config.deletion_factory_ratio,
	);
	// Set the datastore compaction style
	info!(target: TARGET, "Setting compaction style: {}", config.compaction_style);
	let style = match config.compaction_style.to_ascii_lowercase().as_str() {
		"universal" => DBCompactionStyle::Universal,
		_ => DBCompactionStyle::Level,
	};
	target.set_compaction_style(style);
	// Universal-compaction-specific options
	if matches!(style, DBCompactionStyle::Universal) {
		// Create the universal compaction options
		let mut uco = UniversalCompactOptions::default();
		// Set the intended size ratio
		info!(target: TARGET, "Universal compaction size ratio: {}", config.universal_size_ratio);
		uco.set_size_ratio(config.universal_size_ratio);
		// Set the minimum merge width
		let min = config.universal_min_merge_width.min(i32::MAX as u32);
		info!(target: TARGET, "Universal compaction min merge width: {min}");
		uco.set_min_merge_width(min as i32);
		// Set the maximum merge width
		let max = config.universal_max_merge_width.min(i32::MAX as u32);
		info!(target: TARGET, "Universal compaction max merge width: {max}");
		uco.set_max_merge_width(max as i32);
		// Set the max size amplification percentage
		let amp_pct = config.universal_max_size_amplification_percent.min(i32::MAX as u32);
		info!(target: TARGET, "Universal compaction max size amplification percent: {amp_pct}");
		uco.set_max_size_amplification_percent(amp_pct as i32);
		// Set the compression size percentage. Note this field is `i32`
		// rather than `u32` because RocksDB uses `-1` as the sentinel
		// meaning "compress all output"; no clamp is needed.
		let compress_pct = config.universal_compression_size_percent;
		info!(target: TARGET, "Universal compaction compression size percent: {compress_pct}");
		uco.set_compression_size_percent(compress_pct);
		// Set the compaction stop style
		let style = config.universal_stop_style.to_ascii_lowercase();
		info!(target: TARGET, "Universal compaction stop style: {style}");
		uco.set_stop_style(match style.as_str() {
			"similar_size" | "similar" => UniversalCompactionStopStyle::Similar,
			_ => UniversalCompactionStopStyle::Total,
		});
		// Set the universal compaction options
		target.set_universal_compaction_options(&uco);
	}
	// Set specific compression levels
	info!(target: TARGET, "Setting compression level");
	target.set_compression_per_level(&[
		DBCompressionType::None, // L0
		DBCompressionType::Lz4,  // L1
		DBCompressionType::Lz4,  // L2
		DBCompressionType::Lz4,  // L3
		DBCompressionType::Lz4,  // L4
		DBCompressionType::Zstd, // L5
		DBCompressionType::Zstd, // L6
		DBCompressionType::Zstd, // L7
	]);
	// Set the bottommost compression type
	info!(target: TARGET, "Setting bottommost compression type: Zstd");
	target.set_bottommost_compression_type(DBCompressionType::Zstd);
	// Use Zstd typed dictionary training
	info!(target: TARGET, "Using Zstd typed dictionary training");
	target.set_bottommost_zstd_max_train_bytes(0, true);
	// Configure the custom table-level prefix extractor. See
	// `prefix_extractor.rs` for the semantics.
	if config.prefix_extractor_enabled {
		info!(target: TARGET, "Prefix extractor: enabled ({})", prefix_extractor::NAME);
		target.set_prefix_extractor(prefix_extractor::build());
		let ratio = config.memtable_prefix_bloom_ratio;
		if ratio > 0.0 {
			info!(target: TARGET, "Memtable prefix bloom ratio: {ratio}");
			target.set_memtable_prefix_bloom_ratio(ratio);
		}
	} else {
		info!(target: TARGET, "Prefix extractor: disabled");
	}
}

impl Datastore {
	/// Open a new database
	pub(crate) async fn new(path: &str, config: RocksDbConfig) -> Result<Datastore> {
		// Configure custom options
		let mut opts = Options::default();
		// Ensure we use fdatasync
		opts.set_use_fsync(false);
		// Create database if missing
		opts.create_if_missing(true);
		// Create column families if missing
		opts.create_missing_column_families(true);
		// Default to WAL flush on every commit
		opts.set_manual_wal_flush(false);
		// Set incremental asynchronous bytes per sync to 2MiB
		opts.set_wal_bytes_per_sync(2 * 1024 * 1024);
		// Increase the background thread count
		let threads = config.thread_count.min(i32::MAX as usize) as i32;
		info!(target: TARGET, "Background thread count: {threads}");
		opts.increase_parallelism(threads);
		// Specify the max concurrent background jobs
		let background_jobs = config.jobs_count.min(i32::MAX as usize) as i32;
		info!(target: TARGET, "Maximum background jobs count: {background_jobs}");
		opts.set_max_background_jobs(background_jobs);
		// Set the maximum number of open files that can be used by the database
		let max_open_files = config.max_open_files.min(i32::MAX as usize) as i32;
		info!(target: TARGET, "Maximum number of open files: {max_open_files}");
		opts.set_max_open_files(max_open_files);
		// Set the number of log files to keep
		info!(target: TARGET, "Number of log files to keep: {}", config.keep_log_file_num);
		opts.set_keep_log_file_num(config.keep_log_file_num);
		// Set the compaction readahead size
		info!(target: TARGET, "Compaction readahead size: {}", config.compaction_readahead_size);
		opts.set_compaction_readahead_size(config.compaction_readahead_size);
		// Set the max number of subcompactions
		info!(target: TARGET, "Maximum concurrent subcompactions: {}", config.max_concurrent_subcompactions);
		opts.set_max_subcompactions(config.max_concurrent_subcompactions);
		// Use separate write thread queues
		info!(target: TARGET, "Use separate thread queues: {}", config.enable_pipelined_writes);
		opts.set_enable_pipelined_write(config.enable_pipelined_writes);
		// Set the write-ahead-log size limit in MB
		info!(target: TARGET, "Write-ahead-log file size limit: {}MB", config.wal_size_limit);
		opts.set_wal_size_limit_mb(config.wal_size_limit);
		// Allow multiple writers to update memtables in parallel
		info!(target: TARGET, "Allow concurrent memtable writes: true");
		opts.set_allow_concurrent_memtable_write(true);
		// Avoid unnecessary blocking io, preferring background threads
		info!(target: TARGET, "Avoid unnecessary blocking IO: true");
		opts.set_avoid_unnecessary_blocking_io(true);
		// Improve concurrency from write batch mutex
		info!(target: TARGET, "Allow adaptive write thread yielding: true");
		opts.set_enable_write_thread_adaptive_yield(true);
		// Set specific storage log level
		info!(target: TARGET, "Setting storage engine log level: {}", config.storage_log_level);
		opts.set_log_level(match config.storage_log_level.to_ascii_lowercase().as_str() {
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
		// Apply the column-family-level settings to the main `opts`. These
		// govern the implicit default CF used when versioning is disabled;
		// when versioning is enabled the same settings are mirrored onto
		// the explicit CF descriptor's options below.
		apply_cf_level_options(&mut opts, &config);
		// Configure and create the memory manager. This also applies its
		// per-CF settings (write buffer size, max/min memtables,
		// block-based table factory, blob cache) to `opts`.
		let memory_manager = Arc::new(MemoryManager::configure(&mut opts, &config)?);
		// Configure the timestamp-aware comparator for user-defined
		// timestamps. When versioning is enabled we open the database
		// with an explicit default CF descriptor; RocksDB uses this
		// descriptor's options for CF-level settings, so every CF-level
		// setting applied above on `opts` must also be applied here.
		let cf_opts = if config.versioned {
			info!(target: TARGET, "Enabling user-defined timestamps (versioning)");
			let mut cf_opts = Options::default();
			cf_opts.set_comparator_with_ts(
				comparator::NAME,
				comparator::TIMESTAMP_SIZE,
				Box::new(comparator::compare),
				Box::new(comparator::compare_ts),
				Box::new(comparator::compare_without_ts),
			);
			apply_cf_level_options(&mut cf_opts, &config);
			memory_manager.apply_to_cf_options(&mut cf_opts, &config);
			Some(cf_opts)
		} else {
			None
		};
		// Pre-configure the disk space manager
		let should_create_disk_space_manager = DiskSpaceManager::configure(&mut opts, &config)?;
		// Pre-configure WAL options based on the resolved sync mode
		match config.sync_mode {
			// Pre-configure the background flusher
			SyncMode::Interval(_) => BackgroundFlusher::configure(&mut opts, &config),
			// Pre-configure the commit coordinator
			SyncMode::Every => CommitCoordinator::configure(&mut opts, &config),
			// No configuration needed
			SyncMode::Never => {}
		};
		// Create the disk space manager if enabled
		let disk_space_manager = if should_create_disk_space_manager {
			Some(Arc::new(DiskSpaceManager::new(&mut opts, &config)?))
		} else {
			None
		};
		// Open the database, using an explicit "default" column family when
		// versioning is enabled so that cf_handle("default") is available
		// for the garbage collector to advance the full_history_ts_low watermark.
		let db = if let Some(cf_opts) = cf_opts {
			// Open the database with the "default" column family
			let descriptors = vec![ColumnFamilyDescriptor::new("default", cf_opts)];
			// Open the database with the "default" column family
			Arc::pin(OptimisticTransactionDB::open_cf_descriptors(&opts, path, descriptors)?)
		} else {
			// Open the database without a default column family
			Arc::pin(OptimisticTransactionDB::open(&opts, path)?)
		};
		// Create the commit coordinator if enabled
		let commit_coordinator = if let SyncMode::Every = config.sync_mode {
			Some(Arc::new(CommitCoordinator::new(db.clone(), &config)?))
		} else {
			None
		};
		// Create the background flusher if enabled
		let background_flusher = if let SyncMode::Interval(interval) = config.sync_mode {
			Some(Arc::new(BackgroundFlusher::new(db.clone(), interval)?))
		} else {
			None
		};
		// Create the garbage collector if versioning with a finite retention period
		let garbage_collector = if config.versioned && config.retention != Duration::ZERO {
			Some(Arc::new(GarbageCollector::new(db.clone(), config.retention)?))
		} else {
			None
		};
		// Defer to the operating system buffers for disk sync. This means that the
		// transaction commits are written to WAL on commit, but are then flushed
		// to disk by the operating system at an unspecified time. In the event of
		// a system crash, data may be lost if the operating system has not yet
		// synced the data to disk.
		if let SyncMode::Never = config.sync_mode {
			info!(target: TARGET, "Sync mode: never (handled by the OS");
			opts.set_manual_wal_flush(false);
		}
		// Register the memory manager with the global allocator tracker
		memory_manager.register_with_allocator_tracker();
		// Build the per-datastore starvation guard. The embedder
		// (server / SDK consumer) injects the worker count of the
		// tokio runtime it built via
		// `Datastore::builder().with_runtime_worker_threads(...)`, so
		// the inline cap follows the executor that will run the
		// storage ops. When the embedder hasn't injected a value the
		// field retains `default_runtime_worker_threads()`
		// (`max(4, num_cpus::get())`, matching the server's tokio
		// runtime sizing default), keeping the inline fast path
		// enabled out of the box.
		let inline_guard =
			Arc::new(InlineGuard::new(config.runtime_worker_threads, config.runtime_reserve));
		// Return the datastore
		Ok(Datastore {
			db,
			versioned: config.versioned,
			memory_manager,
			disk_space_manager,
			background_flusher,
			commit_coordinator,
			garbage_collector,
			prefix_extractor_enabled: config.prefix_extractor_enabled,
			scan_verify_checksums: config.scan_verify_checksums,
			compact_on_shutdown: config.compact_on_shutdown,
			shutdown_wait_for_compact_seconds: config.shutdown_wait_for_compact_seconds,
			inline_guard,
		})
	}

	const BLOCK_CACHE_USAGE: &str = "rocksdb.block_cache_usage";
	const BLOCK_CACHE_PINNED_USAGE: &str = "rocksdb.block_cache_pinned_usage";
	const ESTIMATE_TABLE_READERS_MEM: &str = "rocksdb.estimate_table_readers_mem";
	const CUR_SIZE_ALL_MEM_TABLES: &str = "rocksdb.cur_size_all_mem_tables";
	const TOTAL_SST_FILES_SIZE: &str = "rocksdb.total_sst_files_size";
	const LIVE_SST_FILES_SIZE: &str = "rocksdb.live_sst_files_size";
	const ESTIMATE_LIVE_DATA_SIZE: &str = "rocksdb.estimate_live_data_size";
	const ESTIMATE_NUM_KEYS: &str = "rocksdb.estimate_num_keys";
	const COMPACTION_PENDING: &str = "rocksdb.compaction_pending";
	const NUM_RUNNING_COMPACTIONS: &str = "rocksdb.num_running_compactions";
	const NUM_RUNNING_FLUSHES: &str = "rocksdb.num_running_flushes";
	/// Per-datastore counter: storage calls that ran inline on a tokio
	/// worker. Counts each `get`/`getm`/`set`/`put`/`putc`/`del`/`delc`/
	/// `exists`/`cancel`/`commit` invocation that successfully acquired
	/// an inline-blocking permit on this datastore's `InlineGuard`.
	const INLINE_BLOCKING_GRANTED: &str = "rocksdb.inline_blocking_granted";
	/// Per-datastore counter: storage calls diverted to the affinity pool
	/// because this datastore's inline-blocking cap was hit. A rising
	/// delta indicates the runtime would have been at risk of starvation
	/// without the guard.
	const INLINE_BLOCKING_DIVERTED: &str = "rocksdb.inline_blocking_diverted";

	/// Registers metrics for the RocksDB datastore.
	///
	/// The first four metrics capture memory usage; the remainder expose
	/// on-disk size, logical data size, compaction, and flush activity so
	/// operators can alert on stalled LSM trees. Every metric is a direct
	/// RocksDB `property_int_value` lookup and is therefore O(1).
	pub(crate) fn register_metrics(&self) -> Metrics {
		Metrics {
			name: "surrealdb.rocksdb",
			u64_metrics: vec![
				Metric {
					name: Self::BLOCK_CACHE_USAGE,
					description: "Returns the memory size (in bytes) for the entries residing in block cache.",
				},
				Metric {
					name: Self::BLOCK_CACHE_PINNED_USAGE,
					description: "Returns the memory size (in bytes) for the entries being pinned.",
				},
				Metric {
					name: Self::ESTIMATE_TABLE_READERS_MEM,
					description: "Returns estimated memory size (in bytes) used for reading SST tables, excluding memory used in block cache (e.g., filter and index blocks).",
				},
				Metric {
					name: Self::CUR_SIZE_ALL_MEM_TABLES,
					description: "Returns approximate size (in bytes) of active and unflushed immutable memtables",
				},
				Metric {
					name: Self::TOTAL_SST_FILES_SIZE,
					description: "Total on-disk size (bytes) of all SST files, including obsolete ones that are still referenced by snapshots.",
				},
				Metric {
					name: Self::LIVE_SST_FILES_SIZE,
					description: "On-disk size (bytes) of SST files referenced by the current LSM tree.",
				},
				Metric {
					name: Self::ESTIMATE_LIVE_DATA_SIZE,
					description: "Estimated logical live data size (bytes) after applying tombstones.",
				},
				Metric {
					name: Self::ESTIMATE_NUM_KEYS,
					description: "Estimated number of live keys in the LSM tree.",
				},
				Metric {
					name: Self::COMPACTION_PENDING,
					description: "1 if a compaction is pending, 0 otherwise.",
				},
				Metric {
					name: Self::NUM_RUNNING_COMPACTIONS,
					description: "Number of compactions currently running.",
				},
				Metric {
					name: Self::NUM_RUNNING_FLUSHES,
					description: "Number of memtable flushes currently running.",
				},
				Metric {
					name: Self::INLINE_BLOCKING_GRANTED,
					description: "Per-datastore count of storage calls that ran inline on a tokio worker (inline-blocking permit granted).",
				},
				Metric {
					name: Self::INLINE_BLOCKING_DIVERTED,
					description: "Per-datastore count of storage calls diverted to the affinity pool because the inline-blocking cap was hit.",
				},
			],
		}
	}

	/// Collects a specific u64 metric by name from the RocksDB datastore.
	pub(crate) fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		// Inline-blocking counters live on the per-datastore `InlineGuard`,
		// not on a RocksDB property. Resolve them before falling through to
		// the property lookup so the per-flavour registry exposes both
		// shapes uniformly.
		match metric {
			Self::INLINE_BLOCKING_GRANTED => return Some(self.inline_guard.granted()),
			Self::INLINE_BLOCKING_DIVERTED => return Some(self.inline_guard.diverted()),
			_ => {}
		}
		let metric = match metric {
			Self::BLOCK_CACHE_USAGE => Some(properties::BLOCK_CACHE_USAGE),
			Self::BLOCK_CACHE_PINNED_USAGE => Some(properties::BLOCK_CACHE_PINNED_USAGE),
			Self::ESTIMATE_TABLE_READERS_MEM => Some(properties::ESTIMATE_TABLE_READERS_MEM),
			Self::CUR_SIZE_ALL_MEM_TABLES => Some(properties::CUR_SIZE_ALL_MEM_TABLES),
			Self::TOTAL_SST_FILES_SIZE => Some(properties::TOTAL_SST_FILES_SIZE),
			Self::LIVE_SST_FILES_SIZE => Some(properties::LIVE_SST_FILES_SIZE),
			Self::ESTIMATE_LIVE_DATA_SIZE => Some(properties::ESTIMATE_LIVE_DATA_SIZE),
			Self::ESTIMATE_NUM_KEYS => Some(properties::ESTIMATE_NUM_KEYS),
			Self::COMPACTION_PENDING => Some(properties::COMPACTION_PENDING),
			Self::NUM_RUNNING_COMPACTIONS => Some(properties::NUM_RUNNING_COMPACTIONS),
			Self::NUM_RUNNING_FLUSHES => Some(properties::NUM_RUNNING_FLUSHES),
			_ => None,
		};
		metric.map(|metric| {
			self.db.property_int_value(metric).unwrap_or_default().unwrap_or_default()
		})
	}

	/// Gracefully shut down the database.
	///
	/// Order of operations matters and is documented inline. Roughly:
	///
	/// 1. Stop the application-level pumps (garbage collector, background flusher, commit
	///    coordinator) so nothing schedules new work.
	/// 2. Flush the WAL to storage and the memtables to L0 SSTs so the next startup needs minimal
	///    recovery.
	/// 3. *Optionally* run a full-keyspace compaction (`compact_on_shutdown`) down to the
	///    bottommost level, mirroring `ALTER SYSTEM COMPACT`.
	/// 4. Drain in-flight compactions with a bounded `wait_for_compact` so we do not abandon work
	///    that was already in progress (typical case: the flush above just produced an L0 file that
	///    tripped `level_zero_file_num_compaction_trigger`).
	/// 5. `cancel_all_background_work(wait=true)` so the bg thread pool is drained cleanly before
	///    the `Arc<OptimisticTransactionDB>` is dropped.
	/// 6. Shut down the memory manager.
	///
	/// Steps 1, 2, and 6 already existed; 3-5 are new. All errors are
	/// logged-and-continued rather than propagated: a partial shutdown is
	/// always better than panicking on the way out.
	pub(crate) async fn shutdown(&self) -> Result<()> {
		// (1) Stop the application-level pumps so nothing schedules new
		//     compactions / flushes / commits while we tear the LSM down.
		if let Some(garbage_collector) = &self.garbage_collector {
			garbage_collector.shutdown()?;
		}
		if let Some(background_flusher) = &self.background_flusher {
			background_flusher.shutdown()?;
		}
		if let Some(commit_coordinator) = &self.commit_coordinator {
			commit_coordinator.shutdown()?;
		}
		// (2) Build the flush options once: every flush waits for completion.
		let mut flush_opts = FlushOptions::default();
		flush_opts.set_wait(true);
		// (2a) Flush the WAL so anything sitting in the WAL buffer is
		//      fsynced to disk before we touch the memtable.
		if let Err(e) = self.db.flush_wal(true) {
			error!("An error occurred flushing the WAL buffer to disk: {e}");
		}
		// (2b) Flush the memtables so the next startup does not have to
		//      replay them from the WAL.
		if let Err(e) = self.db.flush_opt(&flush_opts) {
			error!("An error occurred flushing memtables to SST files: {e}");
		}
		// (3-5) Offload the LSM-cleanup steps to the affinity pool because
		//       `compact_range_opt` and `wait_for_compact` are synchronous
		//       and can block for a long time on large databases. We do not
		//       want to stall the async runtime for the duration.
		let compact_on_shutdown = self.compact_on_shutdown;
		let wait_for_compact_seconds = self.shutdown_wait_for_compact_seconds;
		let cleanup: anyhow::Result<()> = affinitypool::spawn_local(move || {
			// (3) Optional full-keyspace compaction. Mirrors the
			//     `Transactable::compact` impl: change_level=true,
			//     target_level=6, bottommost_level_compaction=Force so
			//     the data lands at the configured bottommost-Zstd
			//     level and superseded versions there are rewritten
			//     away. Skipped by default because it is O(database
			//     size).
			if compact_on_shutdown {
				info!(
					target: TARGET,
					"Running full-keyspace compaction on shutdown",
				);
				let mut copts = CompactOptions::default();
				copts.set_exclusive_manual_compaction(true);
				copts.set_change_level(true);
				copts.set_target_level(6);
				copts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
				// Turbofish required: with `None, None` the compiler
				// cannot otherwise infer the byte-slice types.
				self.db.compact_range_opt::<&[u8], &[u8]>(None, None, &copts);
			}
			// (4) Drain in-flight compactions. After the memtable flush
			//     above, the new L0 file may have crossed
			//     `level_zero_file_num_compaction_trigger`; waiting for
			//     that to settle means the next startup does not have
			//     to recover or re-perform the work.
			let mut wfco = WaitForCompactOptions::default();
			// `set_timeout` takes microseconds. `0` waits indefinitely.
			let timeout_us = wait_for_compact_seconds.saturating_mul(1_000_000);
			wfco.set_timeout(timeout_us);
			info!(
				target: TARGET,
				"Waiting for in-flight compactions to drain (timeout: {wait_for_compact_seconds}s)",
			);
			if let Err(e) = self.db.wait_for_compact(&wfco) {
				error!("An error occurred waiting for compactions to drain: {e}");
			}
			// (5) Cancel any remaining scheduled bg work and wait for
			//     currently-running jobs to finish. After this point
			//     RocksDB will not start any new flush or compaction.
			info!(target: TARGET, "Cancelling background work");
			self.db.cancel_all_background_work(true);
			Ok(())
		})
		.await;
		if let Err(e) = cleanup {
			error!("An error occurred during shutdown cleanup: {e}");
		}
		// (6) Shut down the memory manager last, after we are sure no
		//     bg thread is going to touch the write buffer manager.
		self.memory_manager.shutdown()?;
		// All good
		Ok(())
	}

	/// Start a new transaction
	pub(crate) async fn transaction(&self, write: bool, _: bool) -> Result<Box<dyn Transactable>> {
		// Set the transaction options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Set the write options
		let mut wo = WriteOptions::default();
		// Per-transaction sync is never used. When sync=every is configured, the commit
		// coordinator handles grouped fsync after parallel transaction commits. When
		// sync=<interval> or sync=never, no per-transaction fsync is needed either.
		wo.set_sync(false);
		// Create a new transaction
		let tx = self.db.transaction_opt(&wo, &to);
		// When versioning is enabled the default column family uses a
		// user-defined-timestamp (UDT) comparator. RocksDB then requires every
		// `OptimisticTransaction` to have a `read_timestamp_` strictly less
		// than `kMaxTxnTimestamp` (the default `u64::MAX` sentinel) and
		// strictly less than whatever `commit_timestamp_` we later set at
		// commit time. Specifically:
		//
		// - `DBImpl::GetLatestSequenceForKey`, invoked from the commit-time conflict check
		//   (`TransactionUtil::CheckKey`), asserts `timestamp != nullptr` whenever the column
		//   family's comparator has a non-zero timestamp size. `OptimisticTransaction` only
		//   populates that buffer when `read_timestamp_ < kMaxTxnTimestamp`, so without this call
		//   the first writeable commit aborts the process with `Assertion 'timestamp' failed`.
		// - `OptimisticTransaction::SetCommitTimestamp` rejects any `commit_ts <= read_timestamp_`
		//   with `Status::InvalidArgument`, which the Rust wrapper silently drops. A too-large
		//   `read_ts` (e.g. `u64::MAX - 1`) would therefore make the `set_commit_ timestamp` call
		//   in `commit()` a no-op, leading to a downstream `Must assign a commit timestamp` error.
		// - UDT-based validation fires when `read_ts < observed_ts`, so a too-small `read_ts` (e.g.
		//   `0`) would false-positive on every key whose latest version is visible through our
		//   snapshot.
		//
		// Seeding `read_ts` from the globally-monotonic HLC after
		// `transaction_opt()` (which captured the snapshot) satisfies all
		// three: every HLC assigned to a visible prior commit is strictly
		// smaller (their `set_commit_timestamp` + `db.Write` both happened
		// before our snapshot, hence before this call), and the HLC we use at
		// commit time via `HlcTimeStamp::next()` is strictly larger (the HLC
		// is a process-wide CAS counter).
		if self.versioned {
			let read_ts = HlcTimeStamp::next();
			tx.set_read_timestamp_for_validation(read_ts.0);
		}
		// SAFETY: The transaction lifetime is tied to the database through the db field.
		// The database is guaranteed to outlive the transaction because:
		// 1. The transaction holds a Pin<Arc<OptimisticTransactionDB>> reference
		// 2. The transaction struct ensures db is dropped after inner
		// 3. The Pin ensures the database isn't moved or dropped while referenced
		let tx = unsafe {
			std::mem::transmute::<
				rocksdb::Transaction<'_, OptimisticTransactionDB>,
				rocksdb::Transaction<'static, OptimisticTransactionDB>,
			>(tx)
		};
		// Heap-allocate before capturing the snapshot so `SnapshotWithThreadMode`'s
		// `db: &Transaction` points at a stable address: moving `TransactionInner`
		// only moves the `Box` pointer, not the transaction allocation.
		let tx = Box::new(tx);
		// Capture the transaction's internal snapshot (set by `set_snapshot(true)`
		// above) and extend its lifetime to `'static`. Using the transaction's
		// own snapshot ensures reads always see the sequence number used for
		// commit-time conflict detection; using a different snapshot (e.g. from
		// `db.snapshot()`) would allow read/conflict-detection to disagree and
		// break optimistic concurrency correctness.
		//
		// SAFETY: The snapshot borrows `&*tx` into the boxed transaction. The
		// transmute is sound because `tx` and `snapshot` live in the same
		// `TransactionInner` (with `snapshot` declared before `tx` so it drops
		// first), and the commit path drops `snapshot` before consuming the
		// boxed `Transaction` (see `Transactable::commit`).
		let snapshot = unsafe {
			std::mem::transmute::<
				SnapshotWithThreadMode<'_, rocksdb::Transaction<'static, OptimisticTransactionDB>>,
				SnapshotWithThreadMode<
					'static,
					rocksdb::Transaction<'static, OptimisticTransactionDB>,
				>,
			>(tx.as_ref().snapshot())
		};
		// Build the default read options pointing at the captured snapshot.
		// `set_snapshot` copies the internal snapshot pointer into the
		// `ReadOptions`, so `ReadOptions` remains valid as long as the
		// underlying rocksdb snapshot (owned by `inner`) is alive.
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&snapshot);
		ro.set_async_io(true);
		ro.fill_cache(true);
		// When versioned, default reads fetch the latest version
		if self.versioned {
			ro.set_timestamp(u64::MAX.to_le_bytes().to_vec());
		}
		// Create a new transaction
		Ok(Box::new(Transaction {
			cursors_alive: AtomicUsize::new(0),
			done: AtomicBool::new(false),
			write,
			versioned: self.versioned,
			read_options: ro,
			inner: Mutex::new(Some(TransactionInner {
				tx,
				snapshot,
			})),
			transaction_state: Arc::new(Default::default()),
			disk_space_manager: self.disk_space_manager.clone(),
			commit_coordinator: self.commit_coordinator.clone(),
			db: self.db.clone(),
			prefix_extractor_enabled: self.prefix_extractor_enabled,
			scan_verify_checksums: self.scan_verify_checksums,
			inline_guard: Arc::clone(&self.inline_guard),
		}))
	}
}

impl Transaction {
	/// Get the current transaction state
	fn current_state(&self) -> TransactionState {
		match self.transaction_state.load(Ordering::Acquire) {
			0 => TransactionState::ReadsOnly,
			1 => TransactionState::HasDeletes,
			2 => TransactionState::HasWrites,
			_ => unreachable!(),
		}
	}

	/// Mark the transaction as containing deletes
	fn store_deletes(&self) {
		if self.current_state() < TransactionState::HasDeletes {
			self.transaction_state.store(TransactionState::HasDeletes as u8, Ordering::Release);
		}
	}

	/// Mark the transaction as containing writes
	fn store_writes(&self) {
		if self.current_state() < TransactionState::HasWrites {
			self.transaction_state.store(TransactionState::HasWrites as u8, Ordering::Release);
		}
	}

	/// Check if the transaction contains writes
	fn contains_deletes(&self) -> bool {
		self.current_state() == TransactionState::HasDeletes
	}

	/// Check if the transaction contains writes
	fn contains_writes(&self) -> bool {
		self.current_state() == TransactionState::HasWrites
	}

	/// Check if disk space is restricted
	fn is_restricted(&self, recalculate: bool) -> bool {
		if let Some(dsm) = self.disk_space_manager.as_ref() {
			match recalculate {
				false => dsm.cached_state() == DiskSpaceState::ReadAndDeletionOnly,
				true => dsm.latest_state() == DiskSpaceState::ReadAndDeletionOnly,
			}
		} else {
			false
		}
	}

	/// Build a fresh `ReadOptions` for a versioned point read.
	///
	/// The caller must pass the already-borrowed `TransactionInner` so the read uses
	/// the transaction's captured snapshot (matching the conflict-detection
	/// view that will be used at commit time).
	fn versioned_read_options(
		&self,
		version: Option<u64>,
		inner: &TransactionInner,
	) -> ReadOptions {
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot);
		ro.set_async_io(true);
		ro.fill_cache(true);
		if self.versioned {
			let ts = version.unwrap_or(u64::MAX);
			ro.set_timestamp(ts.to_le_bytes().to_vec());
		}
		ro
	}

	/// Apply the appropriate prefix-seek setting to the read options used
	/// by a range scan.
	///
	/// With a prefix extractor configured on the column family, iterators
	/// have two safe modes:
	///
	/// * `prefix_same_as_start(true)` — the iterator stays within the extracted prefix of the seek
	///   key. This is what lets RocksDB skip SSTs via the per-SST prefix bloom filter and terminate
	///   `FindNextUserEntry` early at prefix boundaries. We enable it only when *both* range
	///   endpoints are in-domain AND resolve to the same extracted prefix (all common record /
	///   index / graph / ref scans satisfy this).
	/// * `total_order_seek(true)` — the iterator does a total-order seek and ignores the prefix
	///   extractor. This is required for catalog scans whose bounds live outside the prefix
	///   extractor's domain (e.g. listing tables, indexes, users, etc.); without it RocksDB
	///   documents the returned keys as "undefined" when the upper bound has a different prefix
	///   than the seek key.
	///
	/// When the prefix extractor is disabled at the datastore level,
	/// neither option is set (default behaviour).
	fn apply_prefix_mode(&self, ro: &mut ReadOptions, rng: &Range<Key>) {
		// Check if the prefix extractor is enabled
		if !self.prefix_extractor_enabled {
			return;
		}
		// Check if the bounds are in-domain and share the same extracted prefix
		match (prefix_extractor::extract(&rng.start), prefix_extractor::extract(&rng.end)) {
			// Both bounds are in-domain and share the same extracted prefix:
			// safe to enable prefix-scan optimisations. This is the hot path
			// for record and index scans.
			(Some(sp), Some(ep)) if sp == ep => {
				ro.set_prefix_same_as_start(true);
			}
			// Any other case: either one or both bounds are out-of-domain,
			// or they sit in different prefixes. Fall back to total-order
			// seek so we keep correctness at the cost of prefix bloom
			// filter optimisations.
			_ => {
				ro.set_total_order_seek(true);
			}
		}
	}

	/// Build a fresh `ReadOptions` for a scan over the given key range.
	/// Sets the iterate bounds, the captured snapshot, async-io, caching
	/// flags, and (when applicable) the version timestamp.
	fn scan_read_options(
		&self,
		rng: &Range<Key>,
		version: Option<u64>,
		inner: &TransactionInner,
	) -> ReadOptions {
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot);
		ro.set_iterate_lower_bound(rng.start.clone());
		ro.set_iterate_upper_bound(rng.end.clone());
		ro.set_auto_readahead_size(true);
		ro.set_async_io(true);
		ro.fill_cache(true);
		ro.set_verify_checksums(self.scan_verify_checksums);
		self.apply_prefix_mode(&mut ro, rng);
		if self.versioned {
			let ts = version.unwrap_or(u64::MAX);
			ro.set_timestamp(ts.to_le_bytes().to_vec());
		}
		ro
	}

	/// Build a fresh `ReadOptions` for a count operation.
	///
	/// Count is the known-bulk path: it walks the full range, never
	/// stopping early, and doesn't fill the block cache (`fill_cache =
	/// false`) so it can't pollute the cache with one-off data. We set
	/// an explicit `set_readahead_size` of 2 MiB so the dedicated
	/// prefetch buffer reads big chunks instead of waiting for the
	/// DB-level auto-readahead to ramp up from 8 KiB to its 256 KiB cap
	/// (the auto-readahead's exponential ramp pays a measurable
	/// per-block latency tax during the warm-up; explicit
	/// `set_readahead_size` skips the ramp). The explicit prefetch
	/// buffer is safe to combine with `fill_cache = false` — it's
	/// per-iterator and freed when the iterator drops, so it cannot
	/// outlive the count.
	fn count_read_options(
		&self,
		rng: &Range<Key>,
		version: Option<u64>,
		inner: &TransactionInner,
	) -> ReadOptions {
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&inner.snapshot);
		ro.set_iterate_lower_bound(rng.start.clone());
		ro.set_iterate_upper_bound(rng.end.clone());
		ro.set_auto_readahead_size(true);
		// 2 MiB per-iterator prefetch buffer for the known-bulk count
		// path. See function docstring for rationale.
		ro.set_readahead_size(2 * 1024 * 1024);
		ro.set_async_io(true);
		ro.fill_cache(false);
		ro.set_verify_checksums(self.scan_verify_checksums);
		self.apply_prefix_mode(&mut ro, rng);
		if self.versioned {
			let ts = version.unwrap_or(u64::MAX);
			ro.set_timestamp(ts.to_le_bytes().to_vec());
		}
		ro
	}

	/// Run a blocking RocksDB op under the inner transaction lock.
	///
	/// Acquires the inner mutex, then runs the closure through the
	/// process-wide inline-blocking permit: granted → inline on the calling
	/// tokio worker (cache-hit fast path, no thread hop); refused →
	/// dispatched to the affinity pool so `RUNTIME_RESERVE` tokio workers
	/// stay free for async work.
	///
	/// This is the canonical dispatch shape for every *bounded* RocksDB
	/// op — point reads/writes, scans/keys (read-only and writable),
	/// cancel, commit's fsync. Unbounded ops (`count`, `compact`) and the
	/// sharded read-only `count` fan-out bypass this helper and go to the
	/// pool unconditionally.
	async fn run_blocking<'a, F, R>(&'a self, op: F) -> Result<R>
	where
		F: FnOnce(MutexGuard<'a, Option<TransactionInner>>) -> Result<R> + Send + 'a,
		R: Send + 'a,
	{
		let guard = self.inner.lock().await;
		self.inline_guard.try_inline_or_offload(move || op(guard)).await
	}

	/// Synchronous implementation of `count` taking an already-acquired lock
	/// guard. Always dispatched via `affinitypool::spawn_local` from `count()`;
	/// this function is never called directly on the async executor thread.
	///
	/// Writeable transactions lock the transaction inner and iterate on the
	/// inner transaction so that pending writes in this transaction are
	/// visible to the iterator. Read-only transactions iterate directly on
	/// the database, holding the inner lock only long enough to build the
	/// `ReadOptions` (which copies out the raw snapshot pointer).
	fn count_blocking(
		&self,
		rng: Range<Key>,
		version: Option<u64>,
		guard: MutexGuard<'_, Option<TransactionInner>>,
	) -> Result<usize> {
		// Get the inner transaction state
		let inner =
			guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the ReadOptions with the snapshot and iterate bounds
		let ro = self.count_read_options(&rng, version, inner);
		// Initialize the result
		let mut res: usize = 0;
		// If the transaction is writable, we create the iterator on the
		// transaction. This ensures that all writes in this transaction
		// are merged with each iterator step, making the writes visible to
		// the iterator.
		if self.write {
			// Create the iterator on the transaction
			let mut iter = inner.tx.raw_iterator_opt(ro);
			// Seek to the start key
			iter.seek(&rng.start);
			// Count the items
			while iter.valid() {
				res += 1;
				iter.next();
			}
			// Catch any iterator errors
			iter.status()?;
		}
		// If the transaction is readonly, we iterate directly on the
		// database. This is faster than iterating on the transaction,
		// as it avoids the `BaseDeltaIterator` wrapper used by the
		// transactional iterator.
		else {
			// Release the inner lock before the scan: `ReadOptions` already
			// holds the raw snapshot pointer, and the underlying rocksdb
			// snapshot stays alive for as long as the boxed `inner.tx` (owned by this
			// `Transaction`) is alive. Read-only transactions never take the
			// inner out of the mutex, so the snapshot is safe to use unlocked.
			drop(guard);
			// Create the iterator on the database
			let mut iter = self.db.raw_iterator_opt(ro);
			// Seek to the start key
			iter.seek(&rng.start);
			// Count the items
			while iter.valid() {
				res += 1;
				iter.next();
			}
			// Catch any iterator errors
			iter.status()?;
		}
		// Return result
		Ok(res)
	}

	/// Synchronous implementation of `keys` and `keysr` taking an already-acquired
	/// lock guard.
	///
	/// Dispatches forward or backward iteration based on `dir`. Read-only
	/// transactions release the inner lock before iterating; writeable
	/// transactions hold the lock for the duration of the iterator so that
	/// pending writes are visible.
	fn keys_blocking(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
		dir: Direction,
		guard: MutexGuard<'_, Option<TransactionInner>>,
	) -> Result<KeysResult> {
		// Get the inner transaction state
		let inner =
			guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the ReadOptions with the snapshot and iterate bounds
		let ro = self.scan_read_options(&rng, version, inner);
		// If the transaction is writable, we create the iterator on the
		// transaction. This ensures that all writes in this transaction
		// are merged with each iterator step, making the writes visible to
		// the iterator.
		if self.write {
			// Create the iterator on the transaction
			let mut iter = inner.tx.raw_iterator_opt(ro);
			// Seek to the start (or end) key based on direction
			match dir {
				Direction::Forward => iter.seek(&rng.start),
				Direction::Backward => iter.seek_for_prev(&rng.end),
			}
			// Consume the iterator
			consume_keys(&mut iter, limit, skip, dir)
		}
		// If the transaction is readonly, we iterate directly on the
		// database. This is faster than iterating on the transaction,
		// as it avoids the `BaseDeltaIterator` wrapper used by the
		// transactional iterator.
		else {
			// Release the inner lock before the scan
			drop(guard);
			// Create the iterator on the database
			let mut iter = self.db.raw_iterator_opt(ro);
			// Seek to the start (or end) key based on direction
			match dir {
				Direction::Forward => iter.seek(&rng.start),
				Direction::Backward => iter.seek_for_prev(&rng.end),
			}
			// Consume the iterator
			consume_keys(&mut iter, limit, skip, dir)
		}
	}

	/// Synchronous implementation of `scan` and `scanr` taking an already-acquired
	/// lock guard.
	///
	/// Dispatches forward or backward iteration based on `dir`. Read-only
	/// transactions release the inner lock before iterating; writeable
	/// transactions hold the lock for the duration of the iterator so that
	/// pending writes are visible.
	fn scan_blocking(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
		dir: Direction,
		guard: MutexGuard<'_, Option<TransactionInner>>,
	) -> Result<ScanResult> {
		// Get the inner transaction state
		let inner =
			guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
		// Get the ReadOptions with the snapshot and iterate bounds
		let ro = self.scan_read_options(&rng, version, inner);
		// If the transaction is writable, we create the iterator on the
		// transaction. This ensures that all writes in this transaction
		// are merged with each iterator step, making the writes visible to
		// the iterator.
		if self.write {
			// Create the iterator on the transaction
			let mut iter = inner.tx.raw_iterator_opt(ro);
			// Seek to the start (or end) key based on direction
			match dir {
				Direction::Forward => iter.seek(&rng.start),
				Direction::Backward => iter.seek_for_prev(&rng.end),
			}
			// Consume the iterator
			consume_vals(&mut iter, limit, skip, dir)
		}
		// If the transaction is readonly, we iterate directly on the
		// database. This is faster than iterating on the transaction,
		// as it avoids the `BaseDeltaIterator` wrapper used by the
		// transactional iterator.
		else {
			// Release the inner lock before the scan
			drop(guard);
			// Create the iterator on the database
			let mut iter = self.db.raw_iterator_opt(ro);
			// Seek to the start (or end) key based on direction
			match dir {
				Direction::Forward => iter.seek(&rng.start),
				Direction::Backward => iter.seek_for_prev(&rng.end),
			}
			// Consume the iterator
			consume_vals(&mut iter, limit, skip, dir)
		}
	}

	/// Build a fresh `DBRawIterator` for a stateful scan and erase its
	/// borrow lifetime to `'static`. The variant is chosen by `self.write`:
	/// writable transactions iterate on `inner.tx` so pending writes in
	/// this logical tx are visible through `BaseDeltaIterator`; read-only
	/// transactions iterate on `self.db` directly to avoid the
	/// `BaseDeltaIterator` wrapper.
	///
	/// The iterator is **not** seeked here — the first `next_batch` call
	/// performs the seek so a caller that opens a cursor and never pumps
	/// pays no LSM-seek cost.
	///
	/// SAFETY: the `'static` lifetime on the returned iterator is an
	/// erasure of the borrow into `inner.tx` (writable) or `self.db`
	/// (read-only). Sound because the iterator is moved into a
	/// caller-owned cursor handle whose drop order is load-bearing:
	///
	/// * On the cursor handle (`RocksDbKeysCursor` / `RocksDbValsCursor`), the `ScanState` field
	///   (which carries this iterator) is declared before `_alive_guard`. Rust drops fields in
	///   declaration order, so the iterator's destructor runs while the parent (snapshot, tx, db)
	///   is still alive; only after that does `AliveGuard::drop` decrement `cursors_alive`.
	/// * `commit`/`cancel` set `done = true` (SeqCst) and then `drain_cursors` until `cursors_alive
	///   == 0` before consuming `inner`. Paired with the open-protocol `fetch_add(SeqCst)` then
	///   `load(done, SeqCst)` on `open_*_cursor`, any cursor either completes and drops (drain
	///   observes it and waits) or sees `done == true` and aborts before allocating its iterator.
	///
	/// Both invariants are documented at `Transaction::cursors_alive` and
	/// the cursor handle field order in `scan_cursor.rs`.
	fn build_scan_iter(
		&self,
		rng: &Range<Key>,
		version: Option<u64>,
		inner: &TransactionInner,
	) -> ScanIter {
		let ro = self.scan_read_options(rng, version, inner);
		if self.write {
			let iter = inner.tx.raw_iterator_opt(ro);
			// SAFETY: erasing the iterator's borrow into `inner.tx` to
			// `'static`. Sound iff the four invariants below all hold;
			// changing any of them silently produces use-after-free.
			//
			//   I1. CURSOR FIELD ORDER. `RocksDbValsCursor` / `RocksDbKeysCursor` declare
			//       `state` BEFORE `_alive_guard`. Rust drops fields in declaration order, so the
			//       iterator (inside `state`) is destroyed before `_alive_guard` decrements
			//       `cursors_alive`. Do not reorder these fields, do not extract `ScanState` into a
			//       separately-owned helper, and do not introduce any helper that moves the
			// iterator       out of the cursor.
			//
			//   I2. DRAIN ON COMMIT/CANCEL. `commit`/`cancel` set `done = true` (SeqCst), then
			//       `drain_cursors().await` until `cursors_alive == 0` BEFORE consuming `inner`.
			//       Without the drain, the boxed `rocksdb::Transaction` could be dropped while an
			//       iterator still references it.
			//
			//   I3. SEQCST OPEN PROTOCOL. `open_*_cursor` does `fetch_add(cursors_alive, SeqCst)`
			//       then `load(done, SeqCst)`. Paired against commit's `swap(done, SeqCst)` then
			//       `load(cursors_alive, SeqCst)`, the four-op SeqCst total order rules out the
			//       interleaving where a cursor opens AFTER commit has observed
			//       `cursors_alive == 0`.
			//
			//   I4. BORROW-CHECKER-TIED CURSOR LIFETIME. The cursor handle returned to the caller
			// is       `Box<dyn ScanCursorKeys + 'a>` where `'a` is the borrow of `&'a
			// Transaction`. The       cursor cannot outlive the `Transaction` at the type level.
			// If you ever expose a       way to upgrade this to `'static` (e.g. through
			// `Arc<Transaction>`), invariants       I1–I3 alone are no longer sufficient — you
			// must additionally hold the parent       alive past every cursor's drop.
			//
			// Migrating to `self_cell` or `ouroboros` would express the
			// self-referential borrow at the type level and remove this
			// transmute. See the module comment in `scan_cursor.rs`.
			let iter: DBRawIteratorWithThreadMode<
				'static,
				rocksdb::Transaction<'static, OptimisticTransactionDB>,
			> = unsafe {
				std::mem::transmute::<
					DBRawIteratorWithThreadMode<
						'_,
						rocksdb::Transaction<'static, OptimisticTransactionDB>,
					>,
					DBRawIteratorWithThreadMode<
						'static,
						rocksdb::Transaction<'static, OptimisticTransactionDB>,
					>,
				>(iter)
			};
			ScanIter::Tx(iter)
		} else {
			let iter = self.db.raw_iterator_opt(ro);
			// SAFETY: same four invariants as the writable branch above
			// (cursor field order, drain on commit/cancel, SeqCst open
			// protocol, borrow-checker-tied cursor lifetime). The erased
			// borrow here is into `self.db` (`Pin<Arc<OptimisticTransactionDB>>`)
			// rather than `inner.tx`; `db` is held by the `Transaction`
			// itself so the drain on `cursors_alive` is what keeps it
			// alive until every iterator has destructed.
			let iter: DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB> = unsafe {
				std::mem::transmute::<
					DBRawIteratorWithThreadMode<'_, OptimisticTransactionDB>,
					DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB>,
				>(iter)
			};
			ScanIter::Db(iter)
		}
	}
}

/// Drain wait: yield-loop until no caller-owned cursors are alive on
/// this transaction. Called as the first action of `commit`/`cancel`
/// (after `done` has been set to `true`, so no new cursors can open).
///
/// Uses `SeqCst` to participate in the same total order as the cursor's
/// open protocol (`fetch_add(SeqCst)` then `load(done, SeqCst)`) and
/// commit's own `done.swap(SeqCst)`. The four operations together rule
/// out the case where commit reads `count == 0` and cursor reads
/// `done == false`: in any SeqCst total order, either the cursor's
/// `fetch_add` precedes this load (commit sees `count >= 1`, waits), or
/// the cursor's `done` load follows commit's `done` swap (cursor sees
/// `done == true`, aborts). The SeqCst load is also at least as strong as
/// Acquire, so each cursor's `AliveGuard::drop` (`Release`) synchronises
/// with this load — any iterator-destructor side effects on the rocksdb
/// parent's refcount are visible to the commit thread before it proceeds
/// to consume `inner`.
///
/// In practice the wait is one or zero yields — callers conventionally
/// drop cursors before committing — but the loop is correct under
/// arbitrary scheduling.
///
/// # Stuck drain
///
/// We cannot time out: if `cursors_alive > 0`, an iterator destructor
/// has not yet decremented the rocksdb parent's refcount, and giving up
/// here would let `commit`/`cancel` consume `inner` underneath a live
/// iterator (use-after-free). So a stuck cursor-holding task causes the
/// drain to wait forever — silent hang.
///
/// To make that loud rather than silent, we emit one `warn!` per drain
/// once the wait exceeds [`DRAIN_STUCK_WARN_AFTER`]. The warning fires
/// at most once per `drain_cursors` call to avoid log spam, with an
/// escalating second warning at `10×` the threshold for confirmation.
/// The clock is consulted only every 1024 yields to keep the steady-state
/// (zero or one yield) cost unchanged.
async fn drain_cursors(tx: &Transaction) {
	if tx.cursors_alive.load(Ordering::SeqCst) == 0 {
		return;
	}
	let started = Instant::now();
	let mut warned = false;
	let mut escalated = false;
	let mut yields: u32 = 0;
	while tx.cursors_alive.load(Ordering::SeqCst) > 0 {
		tokio::task::yield_now().await;
		yields = yields.wrapping_add(1);
		// Cheap mask check — only consult the wall clock once per ~1024
		// yields so the hot path (drain completing in 0–1 yields) is
		// unaffected.
		if yields & 1023 == 0 {
			let elapsed = started.elapsed();
			if !warned && elapsed >= DRAIN_STUCK_WARN_AFTER {
				let alive = tx.cursors_alive.load(Ordering::SeqCst);
				warn!(
					target: TARGET,
					"drain_cursors waiting unusually long: {alive} cursor(s) still alive after {elapsed:?}; \
					possible stuck task holding a cursor handle past commit/cancel"
				);
				warned = true;
			}
			if !escalated && elapsed >= DRAIN_STUCK_WARN_AFTER * 10 {
				let alive = tx.cursors_alive.load(Ordering::SeqCst);
				warn!(
					target: TARGET,
					"drain_cursors still blocked: {alive} cursor(s) alive after {elapsed:?}; \
					commit/cancel will not progress until every cursor handle is dropped"
				);
				escalated = true;
			}
		}
	}
}

/// How long [`drain_cursors`] waits before logging a warning. Chosen so
/// the routine drain (single-digit microseconds) never trips it and a
/// truly stuck cursor produces a loud, single-line signal in logs.
const DRAIN_STUCK_WARN_AFTER: Duration = Duration::from_secs(5);

/// Perform the first-time seek (if not yet done), then iterate the
/// underlying `DBRawIterator`, copying each key into `state.key_buf` and
/// recording its `(offset, len)` in `state.key_spans`. Returns the total
/// key bytes for the batch. Both buffers are reused across calls — the
/// caller is responsible for clearing them before invoking this helper.
fn fill_keys_into_state(state: &mut ScanStateKeys, limit: ScanLimit) -> Result<u64> {
	if !state.started {
		match (&mut state.iter, state.dir) {
			(ScanIter::Db(iter), Direction::Forward) => iter.seek(&state.start),
			(ScanIter::Db(iter), Direction::Backward) => iter.seek_for_prev(&state.end),
			(ScanIter::Tx(iter), Direction::Forward) => iter.seek(&state.start),
			(ScanIter::Tx(iter), Direction::Backward) => iter.seek_for_prev(&state.end),
		}
		state.started = true;
		// Burn the pending skip directly against the iterator without
		// materialising or copying anything.
		let skip = std::mem::take(&mut state.skip);
		for _ in 0..skip {
			let still_valid = match &mut state.iter {
				ScanIter::Db(iter) => iter.valid().then(|| match state.dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				}),
				ScanIter::Tx(iter) => iter.valid().then(|| match state.dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				}),
			};
			if still_valid.is_none() {
				match &state.iter {
					ScanIter::Db(iter) => iter.status()?,
					ScanIter::Tx(iter) => iter.status()?,
				}
				return Ok(0);
			}
		}
	}
	match &mut state.iter {
		ScanIter::Db(iter) => {
			fill_keys_inner(iter, limit, state.dir, &mut state.key_buf, &mut state.key_spans)
		}
		ScanIter::Tx(iter) => {
			fill_keys_inner(iter, limit, state.dir, &mut state.key_buf, &mut state.key_spans)
		}
	}
}

/// Same as [`fill_keys_into_state`], for key+value pairs. Fills
/// `state.key_buf`, `state.val_buf`, and `state.spans`. Returns
/// `(key_bytes, value_bytes)` for the batch.
fn fill_vals_into_state(state: &mut ScanStateVals, limit: ScanLimit) -> Result<(u64, u64)> {
	if !state.started {
		match (&mut state.iter, state.dir) {
			(ScanIter::Db(iter), Direction::Forward) => iter.seek(&state.start),
			(ScanIter::Db(iter), Direction::Backward) => iter.seek_for_prev(&state.end),
			(ScanIter::Tx(iter), Direction::Forward) => iter.seek(&state.start),
			(ScanIter::Tx(iter), Direction::Backward) => iter.seek_for_prev(&state.end),
		}
		state.started = true;
		let skip = std::mem::take(&mut state.skip);
		for _ in 0..skip {
			let still_valid = match &mut state.iter {
				ScanIter::Db(iter) => iter.valid().then(|| match state.dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				}),
				ScanIter::Tx(iter) => iter.valid().then(|| match state.dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				}),
			};
			if still_valid.is_none() {
				match &state.iter {
					ScanIter::Db(iter) => iter.status()?,
					ScanIter::Tx(iter) => iter.status()?,
				}
				return Ok((0, 0));
			}
		}
	}
	match &mut state.iter {
		ScanIter::Db(iter) => fill_vals_inner(
			iter,
			limit,
			state.dir,
			&mut state.key_buf,
			&mut state.val_buf,
			&mut state.spans,
		),
		ScanIter::Tx(iter) => fill_vals_inner(
			iter,
			limit,
			state.dir,
			&mut state.key_buf,
			&mut state.val_buf,
			&mut state.spans,
		),
	}
}

/// Estimate the bytes/items a keys-only batch will need and reserve
/// capacity on the cursor's buffers up front. Saves the
/// `Vec::extend_from_slice` reallocations on the first batch of a fresh
/// cursor; subsequent batches already have capacity from peak usage.
#[inline]
fn reserve_for_keys(limit: ScanLimit, key_buf: &mut Vec<u8>, key_spans: &mut Vec<KeySpan>) {
	let (bytes, count) = match limit {
		ScanLimit::Count(c) => {
			let c = c as usize;
			(c.saturating_mul(ESTIMATED_BYTES_PER_KEY as usize), c)
		}
		ScanLimit::Bytes(b) => (b as usize, b as usize / ESTIMATED_BYTES_PER_KEY as usize),
		ScanLimit::BytesOrCount(b, c) => {
			let c = c as usize;
			((b as usize).min(c.saturating_mul(ESTIMATED_BYTES_PER_KEY as usize)), c)
		}
	};
	let need_buf = bytes.saturating_sub(key_buf.capacity());
	if need_buf > 0 {
		key_buf.reserve(need_buf);
	}
	let need_spans = count.saturating_sub(key_spans.capacity());
	if need_spans > 0 {
		key_spans.reserve(need_spans);
	}
}

/// Reserve capacity for a key+value batch. See [`reserve_for_keys`].
#[inline]
fn reserve_for_vals(
	limit: ScanLimit,
	key_buf: &mut Vec<u8>,
	val_buf: &mut Vec<u8>,
	spans: &mut Vec<KeyValSpan>,
) {
	let (bytes, count) = match limit {
		ScanLimit::Count(c) => {
			let c = c as usize;
			(c.saturating_mul(ESTIMATED_BYTES_PER_KV as usize), c)
		}
		ScanLimit::Bytes(b) => (b as usize, b as usize / ESTIMATED_BYTES_PER_KV as usize),
		ScanLimit::BytesOrCount(b, c) => {
			let c = c as usize;
			((b as usize).min(c.saturating_mul(ESTIMATED_BYTES_PER_KV as usize)), c)
		}
	};
	// Split the byte budget across key and value buffers roughly evenly;
	// the exact split is an estimate, growth past it is one extra
	// reallocation per buffer per batch in the worst case.
	//
	// TODO(perf): the 50/50 split is workload-agnostic. Record tables
	// in SurrealDB typically have values much larger than keys, so
	// `val_buf` is the buffer that actually grows. A weighted split
	// (e.g. 1/4 keys, 3/4 values) backed by per-cursor running averages
	// would eliminate the first-batch `val_buf` reallocation on
	// value-heavy scans. Holding off because the win is one
	// reallocation per cursor (not per batch — capacity persists), so
	// it's only worth doing once we have a benchmark that isolates it.
	let half = bytes / 2;
	let need_kbuf = half.saturating_sub(key_buf.capacity());
	if need_kbuf > 0 {
		key_buf.reserve(need_kbuf);
	}
	let need_vbuf = half.saturating_sub(val_buf.capacity());
	if need_vbuf > 0 {
		val_buf.reserve(need_vbuf);
	}
	let need_spans = count.saturating_sub(spans.capacity());
	if need_spans > 0 {
		spans.reserve(need_spans);
	}
}

/// Generic-over-`D: DBAccess` inner loop that writes each key into
/// `key_buf` and pushes its [`KeySpan`]. No per-item heap allocation;
/// the only allocation is any capacity growth on `key_buf` /
/// `key_spans` itself (avoided after the first batch since capacity
/// persists).
fn fill_keys_inner<D: rocksdb::DBAccess>(
	iter: &mut rocksdb::DBRawIteratorWithThreadMode<'_, D>,
	limit: ScanLimit,
	dir: Direction,
	key_buf: &mut Vec<u8>,
	key_spans: &mut Vec<KeySpan>,
) -> Result<u64> {
	let mut key_bytes: u64 = 0;
	// Pre-reserve from the limit hint so the first batch doesn't pay
	// log2(N) `Vec::reserve` reallocations as `extend_from_slice` grows
	// the buffer. Subsequent batches already hit the cached capacity.
	reserve_for_keys(limit, key_buf, key_spans);
	let push_key = |k: &[u8], key_buf: &mut Vec<u8>, key_spans: &mut Vec<KeySpan>| {
		let offset = key_buf.len();
		let len = k.len();
		key_buf.extend_from_slice(k);
		key_spans.push(KeySpan {
			offset,
			len,
		});
	};
	match limit {
		ScanLimit::Count(c) => {
			let c = c as u64;
			let mut count = 0u64;
			while count < c {
				let Some(k) = iter.key() else {
					break;
				};
				push_key(k, key_buf, key_spans);
				key_bytes += k.len() as u64;
				count += 1;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
		ScanLimit::Bytes(b) => {
			let b = b as u64;
			while key_bytes < b {
				let Some(k) = iter.key() else {
					break;
				};
				push_key(k, key_buf, key_spans);
				key_bytes += k.len() as u64;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
		ScanLimit::BytesOrCount(b, c) => {
			let b = b as u64;
			let c = c as u64;
			let mut count = 0u64;
			while count < c && key_bytes < b {
				let Some(k) = iter.key() else {
					break;
				};
				push_key(k, key_buf, key_spans);
				key_bytes += k.len() as u64;
				count += 1;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
	}
	iter.status()?;
	Ok(key_bytes)
}

/// Generic-over-`D: DBAccess` inner loop for key+value batches. See
/// [`fill_keys_inner`].
fn fill_vals_inner<D: rocksdb::DBAccess>(
	iter: &mut rocksdb::DBRawIteratorWithThreadMode<'_, D>,
	limit: ScanLimit,
	dir: Direction,
	key_buf: &mut Vec<u8>,
	val_buf: &mut Vec<u8>,
	spans: &mut Vec<KeyValSpan>,
) -> Result<(u64, u64)> {
	let mut key_bytes: u64 = 0;
	let mut value_bytes: u64 = 0;
	// See `reserve_for_keys` — same rationale for the key+value path.
	reserve_for_vals(limit, key_buf, val_buf, spans);
	let push_pair = |k: &[u8],
	                 v: &[u8],
	                 key_buf: &mut Vec<u8>,
	                 val_buf: &mut Vec<u8>,
	                 spans: &mut Vec<KeyValSpan>| {
		let key_offset = key_buf.len();
		let key_len = k.len();
		key_buf.extend_from_slice(k);
		let val_offset = val_buf.len();
		let val_len = v.len();
		val_buf.extend_from_slice(v);
		spans.push(KeyValSpan {
			key_offset,
			key_len,
			val_offset,
			val_len,
		});
	};
	match limit {
		ScanLimit::Count(c) => {
			let c = c as u64;
			let mut count = 0u64;
			while count < c {
				let Some((k, v)) = iter.item() else {
					break;
				};
				push_pair(k, v, key_buf, val_buf, spans);
				key_bytes += k.len() as u64;
				value_bytes += v.len() as u64;
				count += 1;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
		ScanLimit::Bytes(b) => {
			let b = b as u64;
			let mut bytes_fetched = 0u64;
			while bytes_fetched < b {
				let Some((k, v)) = iter.item() else {
					break;
				};
				let kl = k.len() as u64;
				let vl = v.len() as u64;
				push_pair(k, v, key_buf, val_buf, spans);
				bytes_fetched += kl + vl;
				key_bytes += kl;
				value_bytes += vl;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
		ScanLimit::BytesOrCount(b, c) => {
			let b = b as u64;
			let c = c as u64;
			let mut count = 0u64;
			let mut bytes_fetched = 0u64;
			while count < c && bytes_fetched < b {
				let Some((k, v)) = iter.item() else {
					break;
				};
				let kl = k.len() as u64;
				let vl = v.len() as u64;
				push_pair(k, v, key_buf, val_buf, spans);
				bytes_fetched += kl + vl;
				key_bytes += kl;
				value_bytes += vl;
				count += 1;
				match dir {
					Direction::Forward => iter.next(),
					Direction::Backward => iter.prev(),
				};
			}
		}
	}
	iter.status()?;
	Ok((key_bytes, value_bytes))
}

/// Advance the cursor by one batch of keys. Returns a [`KeysBatch`]
/// borrowed from the cursor's internal buffer for the lifetime of the
/// `&mut self` borrow. The cursor handle owns `state` directly (no
/// per-tx map lookup, no per-batch lock); the only synchronisation here
/// is the `done` check, which lets us bail cleanly if `commit`/`cancel`
/// raced ahead of us.
///
/// Dispatches via `try_inline_or_offload`: inline on the calling tokio
/// worker when a permit is granted, otherwise to
/// `affinitypool::spawn_local`. The first batch of a cursor with a
/// non-zero pending `skip` iterates the skip prefix synchronously inside
/// the closure (see `fill_keys_into_state`), so the permit guards the
/// worker against being pinned by both the skip walk and the batch fill.
/// After the first batch, `state.skip` is 0 and subsequent batches use
/// only `limit`.
pub(in crate::kvs::rocksdb) async fn cursor_next_keys<'s>(
	cursor: &'s mut RocksDbKeysCursor<'_>,
	limit: ScanLimit,
) -> Result<KeysBatch<'s>> {
	// Best-effort early-exit on a stale handle. Cursor-vs-commit safety
	// is already guaranteed by the `cursors_alive` drain + the borrow
	// checker on the cursor handle, so a stale read here only delays the
	// error by at most one batch — well worth the cheaper load.
	if cursor.tx.done.load(Ordering::Relaxed) {
		return Err(Error::TransactionFinished);
	}
	// Reset the reusable buffers before each batch. The allocations
	// themselves persist via Vec's capacity.
	cursor.state.key_buf.clear();
	cursor.state.key_spans.clear();
	let state: &mut ScanStateKeys = &mut cursor.state;
	let key_bytes = cursor
		.tx
		.inline_guard
		.try_inline_or_offload(move || -> Result<u64> { fill_keys_into_state(state, limit) })
		.await?;
	// Borrow the freshly-populated buffers back out as the batch. Zero
	// allocations here — the batch is just a `&[u8]` + `&[KeySpan]` over
	// the cursor's own storage.
	Ok(KeysBatch::from_parts(&cursor.state.key_buf, &cursor.state.key_spans, key_bytes))
}

/// Advance the cursor by one batch of key+value pairs. See
/// [`cursor_next_keys`].
pub(in crate::kvs::rocksdb) async fn cursor_next_vals<'s>(
	cursor: &'s mut RocksDbValsCursor<'_>,
	limit: ScanLimit,
) -> Result<ValsBatch<'s>> {
	// Best-effort early-exit on a stale handle. Cursor-vs-commit safety
	// is already guaranteed by the `cursors_alive` drain + the borrow
	// checker on the cursor handle, so a stale read here only delays the
	// error by at most one batch — well worth the cheaper load.
	if cursor.tx.done.load(Ordering::Relaxed) {
		return Err(Error::TransactionFinished);
	}
	cursor.state.key_buf.clear();
	cursor.state.val_buf.clear();
	cursor.state.spans.clear();
	let state: &mut ScanStateVals = &mut cursor.state;
	let (key_bytes, value_bytes) = cursor
		.tx
		.inline_guard
		.try_inline_or_offload(move || -> Result<(u64, u64)> { fill_vals_into_state(state, limit) })
		.await?;
	// Borrow the freshly-populated buffers back out as the batch. Zero
	// allocations here.
	Ok(ValsBatch::from_parts(
		&cursor.state.key_buf,
		&cursor.state.val_buf,
		&cursor.state.spans,
		key_bytes,
		value_bytes,
	))
}

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
	fn cancel(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already
			// closed. SeqCst pairs with the cursor open protocol — see
			// `drain_cursors` for the full ordering argument.
			if self.done.swap(true, Ordering::SeqCst) {
				return Err(Error::TransactionFinished);
			}
			// Wait for any caller-owned cursors to drop before we consume
			// `inner`. Each cursor's `AliveGuard` decrements `cursors_alive`
			// only AFTER its iterator's destructor runs (field-declaration
			// order on the cursor handle), so by the time we observe
			// `cursors_alive == 0` every iterator has decremented its
			// parent's refcount. `done` was already swapped to `true`
			// above, so no new cursor can open. See `scan_cursor.rs` for
			// the SeqCst ordering rationale.
			drain_cursors(self).await;
			// `rollback` on an OptimisticTransaction just clears the in-memory
			// WriteBatchWithIndex and resets the tracked-keys set — it never
			// touches disk. We therefore take the inner lock and run inline
			// rather than dispatching through the inline-blocking guard.
			let guard = self.inner.lock().await;
			let inner =
				guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			inner.tx.rollback()?;
			Ok(())
		})
	}

	/// Commit a transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Atomically mark transaction as done and check if it was already
			// closed. SeqCst pairs with the cursor open protocol — see
			// `drain_cursors` for the full ordering argument.
			if self.done.swap(true, Ordering::SeqCst) {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Check if we are in read-and-deletion-only mode
			if self.is_restricted(true) && self.contains_writes() {
				return Err(Error::ReadAndDeleteOnly);
			}
			// Wait for any caller-owned cursors to drop before we consume
			// `inner`. See the matching comment in `cancel` for rationale.
			drain_cursors(self).await;
			// Take ownership of the transaction state. The tokio mutex guard
			// is dropped at the end of this statement, so no lock is held
			// across subsequent awaits (e.g. the commit coordinator wait below).
			let TransactionInner {
				tx: inner,
				snapshot,
			} = self
				.inner
				.lock()
				.await
				.take()
				.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			// Explicitly drop the snapshot before consuming the boxed transaction.
			// `snapshot` borrows the transaction inside `inner`, and `commit` consumes
			// it, so the snapshot must be released first.
			drop(snapshot);
			// When versioned, stamp all writes with the current HLC timestamp
			if self.versioned {
				let ts = HlcTimeStamp::next();
				inner.set_commit_timestamp(ts.0);
			}
			// RocksDB commit may invoke `fsync` when sync writes are
			// enabled, so it is the highest-latency synchronous call in
			// the transaction lifecycle and must respect the
			// runtime-headroom guard. The helper probes the inline-blocking
			// permit: granted → run on the calling tokio worker; refused →
			// dispatched to the affinity pool.
			self.inline_guard.try_inline_or_offload(move || (*inner).commit()).await?;
			// If we have a coordinator, wait for the grouped fsync
			if let Some(coordinator) = &self.commit_coordinator {
				coordinator.wait_for_sync().await?;
			}
			// Perform compaction if necessary
			if self.is_restricted(true) && self.contains_deletes() {
				self.compact(None).await?;
			}
			// Continue
			Ok(())
		})
	}

	/// Check if a key exists.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<bool>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				let res = if version.is_some() {
					inner.tx.get_pinned_opt(key, &self.versioned_read_options(version, inner))
				} else {
					inner.tx.get_pinned_opt(key, &self.read_options)
				}?
				.is_some();
				Ok(res)
			})
			.await
		})
	}

	/// Fetch a key from the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<Option<Val>>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				let res = if version.is_some() {
					inner.tx.get_opt(key, &self.versioned_read_options(version, inner))
				} else {
					inner.tx.get_opt(key, &self.read_options)
				}?;
				Ok(res)
			})
			.await
		})
	}

	/// Fetch many keys from the datastore.
	///
	/// Bounded by `keys.len()`, so the call flows through the same
	/// inline-blocking permit path as `get`. Granted → inline on the
	/// calling tokio worker; refused → dispatched to the affinity pool.
	/// Larger key lists naturally bias toward the divert branch as the
	/// global concurrent inline budget is consumed.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	fn getm(&self, keys: Vec<Key>, version: Option<u64>) -> BoxFut<'_, Result<GetMultiResult>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				let res = if version.is_some() {
					inner.tx.multi_get_opt(keys, &self.versioned_read_options(version, inner))
				} else {
					inner.tx.multi_get_opt(keys, &self.read_options)
				};
				// Convert result, accumulating the hit count and value bytes during
				// the same pass so callers do not need to re-walk the result.
				let mut records = 0u64;
				let mut value_bytes = 0u64;
				let values = res
					.into_iter()
					.map(|r| match r {
						Ok(Some(v)) => {
							records += 1;
							value_bytes += v.len() as u64;
							Ok(Some(v))
						}
						Ok(None) => Ok(None),
						Err(e) => Err(e.into()),
					})
					.collect::<Result<Vec<Option<Val>>>>()?;
				Ok(GetMultiResult {
					values,
					records,
					value_bytes,
				})
			})
			.await
		})
	}

	/// Insert or update a key in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn set(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Check if we are in read-and-deletion-only mode
			if self.is_restricted(false) {
				return Err(Error::ReadAndDeleteOnly);
			}
			// `put` on an OptimisticTransaction only buffers into the in-memory
			// WriteBatchWithIndex and records the key for commit-time conflict
			// detection — it never touches disk. We therefore take the inner
			// lock and run inline rather than dispatching through the
			// inline-blocking guard, which would only add a function-call hop
			// (granted) or a wasteful thread hop (diverted) for a pure-memory op.
			let guard = self.inner.lock().await;
			let inner =
				guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			inner.tx.put(key, val)?;
			self.store_writes();
			Ok(())
		})
	}

	/// Insert a key if it doesn't exist in the database.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn put(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Check if we are in read-and-deletion-only mode
			if self.is_restricted(false) {
				return Err(Error::ReadAndDeleteOnly);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				match inner.tx.get_pinned_opt(&key, &self.read_options)? {
					None => inner.tx.put(key, val)?,
					_ => return Err(Error::TransactionKeyAlreadyExists),
				};
				self.store_writes();
				Ok(())
			})
			.await
		})
	}

	/// Insert a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Check if we are in read-and-deletion-only mode
			if self.is_restricted(false) {
				return Err(Error::ReadAndDeleteOnly);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				match (inner.tx.get_pinned_opt(&key, &self.read_options)?, chk) {
					(Some(v), Some(w)) if v.eq(&w) => inner.tx.put(key, val)?,
					(None, None) => inner.tx.put(key, val)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
				self.store_writes();
				Ok(())
			})
			.await
		})
	}

	/// Delete a key.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn del(&self, key: Key) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// `delete` on an OptimisticTransaction only buffers a tombstone into
			// the in-memory WriteBatchWithIndex and records the key for
			// commit-time conflict detection — it never touches disk. We
			// therefore take the inner lock and run inline rather than
			// dispatching through the inline-blocking guard.
			let guard = self.inner.lock().await;
			let inner =
				guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			inner.tx.delete(key)?;
			self.store_deletes();
			Ok(())
		})
	}

	/// Delete a key if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			self.run_blocking(move |guard| {
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				match (inner.tx.get_pinned_opt(&key, &self.read_options)?, chk) {
					(Some(v), Some(w)) if v.eq(&w) => inner.tx.delete(key)?,
					(None, None) => inner.tx.delete(key)?,
					_ => return Err(Error::TransactionConditionNotMet),
				};
				self.store_deletes();
				Ok(())
			})
			.await
		})
	}

	/// Count the total number of keys within a range.
	///
	/// `count()` has no `ScanLimit` parameter, so it always iterates the
	/// entire provided key range without an early-exit limit. It is therefore
	/// always offloaded to the blocking threadpool to avoid stalling the
	/// async executor during the full range scan.
	///
	/// Writable transactions iterate the inner transaction so that pending
	/// writes are merged into the count, and run as a single serial scan.
	/// Read-only transactions shard the range across the affinitypool and
	/// scan all shards in parallel against a shared snapshot, which is a
	/// significant speed-up on large ranges (e.g. full-table `COUNT(*)`).
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn count(&self, rng: Range<Key>, version: Option<u64>) -> BoxFut<'_, Result<usize>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Writable transactions iterate on the inner transaction so that
			// pending writes are visible; this cannot safely be sharded across
			// threads, so we fall back to a single serial scan.
			if self.write {
				// Acquire the lock before dispatching: tokio Mutex requires an
				// async context, which `spawn_local`'s closure does not have.
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					// Run the serial count
					self.count_blocking(rng, version, guard)
				})
				.await;
			}
			// Decide how many shards to dispatch, bounded by the machine
			// parallelism so we don't oversubscribe the affinitypool, and
			// further bounded by `COUNT_PARALLEL_MAX_SHARDS` so very large
			// CPU counts don't produce many tiny shards that all touch the
			// same SSTs.
			let desired = std::thread::available_parallelism()
				.map(|n| n.get())
				.unwrap_or(8)
				.min(COUNT_PARALLEL_MAX_SHARDS);
			// Compute the disjoint sub-ranges that together cover `[start, end)`.
			// Returns a single shard when the range is too narrow to split
			// meaningfully along its first differing byte.
			let sub_ranges = shard_range(&rng.start, &rng.end, desired);
			// Fall back to a single serial scan when the range is too narrow
			// to split: dispatching a single shard would just add the overhead
			// of an extra task hop with no parallelism benefit.
			if sub_ranges.len() <= 1 {
				// Acquire the lock before dispatching: tokio Mutex requires an
				// async context, which `spawn_local`'s closure does not have.
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					// Run the serial count
					self.count_blocking(rng, version, guard)
				})
				.await;
			}
			// Build all shard `ReadOptions` under the inner lock so each shard
			// captures the same snapshot pointer. The lock is dropped before
			// dispatching shards: the snapshot stays alive for as long as
			// `inner.tx` is alive (owned by this `Transaction`), and read-only
			// transactions never take the inner out of the mutex.
			let scans = {
				// Acquire the inner lock
				let guard = self.inner.lock().await;
				// Get the inner transaction state
				let inner = guard
					.as_ref()
					.ok_or_else(|| Error::Internal("expected a transaction".into()))?;
				// Build one (sub-range, ReadOptions) pair per shard
				sub_ranges
					.into_iter()
					.map(|(lo, hi)| {
						// Construct the shard's sub-range
						let sub_rng = lo..hi;
						// Build the ReadOptions for this shard
						let ro = self.count_read_options(&sub_rng, version, inner);
						(sub_rng, ro)
					})
					.collect::<Vec<_>>()
			};
			let mut tasks = Vec::with_capacity(scans.len());
			for (sub_rng, ro) in scans {
				// Clone the Arc'd handle so each shard owns its own reference
				let db = self.db.clone();
				tasks.push(affinitypool::spawn_local(move || -> Result<usize> {
					// Create the iterator on the database
					let mut iter = db.raw_iterator_opt(ro);
					// Seek to the start key
					iter.seek(&sub_rng.start);
					// Initialize the per-shard count
					let mut res: usize = 0;
					// Count the items
					while iter.valid() {
						res += 1;
						iter.next();
					}
					// Catch any iterator errors
					iter.status()?;
					// Return the per-shard count
					Ok(res)
				}));
			}
			// Run all shard scans concurrently and sum the per-shard counts
			let counts = futures::future::try_join_all(tasks).await?;
			// Return result
			Ok(counts.into_iter().sum())
		})
	}

	/// Retrieve a range of keys.
	///
	/// Bounded by `limit`, so the call runs through the standard
	/// inline-blocking permit. Granted → run on the calling tokio worker;
	/// refused → dispatch to the affinity pool. Applies to both read-only
	/// and writable transactions; writable iteration on `inner.tx`
	/// (`BaseDeltaIterator`) holds the inner Mutex for the iter window,
	/// but that already serialises ops on the same transaction regardless
	/// of dispatch shape — and the global permit cap bounds how many
	/// tokio workers can be pinned at once.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Writable transactions always offload range scans to the
			// affinity pool. The scan iterator holds the inner Mutex for
			// the whole iter window (`BaseDeltaIterator` over a writable
			// `Transaction`), so keeping it inline would pin a tokio
			// worker for the entire scan — exactly the starvation mode
			// the inline-blocking permit was built to avoid. The permit
			// already bounds *one* call, but a writable scan is the
			// worst-case occupant: long, locked, and chained through the
			// cursor loop. Send it to the pool unconditionally and keep
			// the permit-aware path for short bounded ops only.
			if self.write {
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					self.keys_blocking(rng, limit, skip, version, Direction::Forward, guard)
				})
				.await;
			}
			self.run_blocking(move |guard| {
				self.keys_blocking(rng, limit, skip, version, Direction::Forward, guard)
			})
			.await
		})
	}

	/// Retrieve a range of keys, in reverse. See [`Self::keys`] for the
	/// dispatch policy.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Writable transactions always offload range scans to the
			// affinity pool. The scan iterator holds the inner Mutex for
			// the whole iter window (`BaseDeltaIterator` over a writable
			// `Transaction`), so keeping it inline would pin a tokio
			// worker for the entire scan — exactly the starvation mode
			// the inline-blocking permit was built to avoid. The permit
			// already bounds *one* call, but a writable scan is the
			// worst-case occupant: long, locked, and chained through the
			// cursor loop. Send it to the pool unconditionally and keep
			// the permit-aware path for short bounded ops only.
			if self.write {
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					self.keys_blocking(rng, limit, skip, version, Direction::Backward, guard)
				})
				.await;
			}
			self.run_blocking(move |guard| {
				self.keys_blocking(rng, limit, skip, version, Direction::Backward, guard)
			})
			.await
		})
	}

	/// Retrieve a range of key-value pairs. See [`Self::keys`] for the
	/// dispatch policy.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Writable transactions always offload range scans to the
			// affinity pool. The scan iterator holds the inner Mutex for
			// the whole iter window (`BaseDeltaIterator` over a writable
			// `Transaction`), so keeping it inline would pin a tokio
			// worker for the entire scan — exactly the starvation mode
			// the inline-blocking permit was built to avoid. The permit
			// already bounds *one* call, but a writable scan is the
			// worst-case occupant: long, locked, and chained through the
			// cursor loop. Send it to the pool unconditionally and keep
			// the permit-aware path for short bounded ops only.
			if self.write {
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					self.scan_blocking(rng, limit, skip, version, Direction::Forward, guard)
				})
				.await;
			}
			self.run_blocking(move |guard| {
				self.scan_blocking(rng, limit, skip, version, Direction::Forward, guard)
			})
			.await
		})
	}

	/// Retrieve a range of key-value pairs, in reverse. See [`Self::keys`] for
	/// the dispatch policy.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// Versioned queries require a versioned datastore
			self.ensure_versioned(version)?;
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Writable transactions always offload range scans to the
			// affinity pool. The scan iterator holds the inner Mutex for
			// the whole iter window (`BaseDeltaIterator` over a writable
			// `Transaction`), so keeping it inline would pin a tokio
			// worker for the entire scan — exactly the starvation mode
			// the inline-blocking permit was built to avoid. The permit
			// already bounds *one* call, but a writable scan is the
			// worst-case occupant: long, locked, and chained through the
			// cursor loop. Send it to the pool unconditionally and keep
			// the permit-aware path for short bounded ops only.
			if self.write {
				let guard = self.inner.lock().await;
				return affinitypool::spawn_local(move || {
					self.scan_blocking(rng, limit, skip, version, Direction::Backward, guard)
				})
				.await;
			}
			self.run_blocking(move |guard| {
				self.scan_blocking(rng, limit, skip, version, Direction::Backward, guard)
			})
			.await
		})
	}

	/// Open a stateful keys-only scan cursor over `rng`.
	///
	/// Builds the iterator under the `inner` lock so the snapshot pointer
	/// and (for writable transactions) the `Transaction` allocation are
	/// observed consistently. The iterator itself is **not** seeked here;
	/// the first [`ScanCursorKeys::next_batch`] call performs the seek.
	/// This keeps a cursor that is opened then aborted before pumping
	/// (e.g. early `LIMIT 0` paths) free of the LSM-seek cost.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn open_keys_cursor<'a>(
		&'a self,
		rng: Range<Key>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// Open protocol: increment `cursors_alive` (SeqCst), then claim
			// the slot via `AliveGuard` so every subsequent exit — the
			// `done == true` re-check, the future being dropped at the
			// `self.inner.lock().await` below (query cancel / timeout), the
			// `?` early-out on `guard.as_ref()` — decrements via Drop.
			// Without the guard, those error/cancel paths would leak the
			// increment and stall `drain_cursors` forever.
			//
			// The SeqCst increment + SeqCst `done` load paired against
			// commit's `done.store` + `cursors_alive.load` rule out the
			// race where a cursor proceeds against a transaction that's
			// already mid-commit. See `scan_cursor.rs` for the proof.
			//
			// `AliveGuard::drop` decrements with Release. That's safe on
			// the error/cancel paths (no iterator has been built, so there
			// are no destructor side-effects to publish) and on the
			// success path (the iterator destructor runs *before* the
			// guard drops, because the guard is the last field in the
			// cursor struct, so the Release publishes its side-effects).
			self.cursors_alive.fetch_add(1, Ordering::SeqCst);
			let alive_guard = AliveGuard::new(self);
			if self.done.load(Ordering::SeqCst) {
				return Err(Error::TransactionFinished);
			}
			// Build the iterator under the inner lock so we observe a
			// consistent (tx, snapshot) pair. Once built, the iterator
			// owns the snapshot pointer via its captured `ReadOptions`;
			// no further inner access is needed for cursor advance.
			let guard = self.inner.lock().await;
			let inner =
				guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			let iter = self.build_scan_iter(&rng, version, inner);
			drop(guard);
			Ok(Box::new(RocksDbKeysCursor {
				tx: self,
				state: ScanStateKeys {
					iter,
					dir,
					started: false,
					skip,
					start: rng.start,
					end: rng.end,
					key_buf: Vec::new(),
					key_spans: Vec::new(),
				},
				_alive_guard: alive_guard,
			}) as Box<dyn ScanCursorKeys + 'a>)
		})
	}

	/// Open a stateful key+value scan cursor over `rng`. See
	/// [`Transactable::open_keys_cursor`] for the rationale.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn open_vals_cursor<'a>(
		&'a self,
		rng: Range<Key>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			self.ensure_versioned(version)?;
			// See `open_keys_cursor` for the open-protocol rationale and
			// why the `AliveGuard` must be bound before the `lock().await`.
			self.cursors_alive.fetch_add(1, Ordering::SeqCst);
			let alive_guard = AliveGuard::new(self);
			if self.done.load(Ordering::SeqCst) {
				return Err(Error::TransactionFinished);
			}
			let guard = self.inner.lock().await;
			let inner =
				guard.as_ref().ok_or_else(|| Error::Internal("expected a transaction".into()))?;
			let iter = self.build_scan_iter(&rng, version, inner);
			drop(guard);
			Ok(Box::new(RocksDbValsCursor {
				tx: self,
				state: ScanStateVals {
					iter,
					dir,
					started: false,
					skip,
					start: rng.start,
					end: rng.end,
					key_buf: Vec::new(),
					val_buf: Vec::new(),
					spans: Vec::new(),
				},
				_alive_guard: alive_guard,
			}) as Box<dyn ScanCursorVals + 'a>)
		})
	}

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			let guard = self.inner.lock().await;
			if let Some(state) = guard.as_ref() {
				state.tx.set_savepoint();
			}
			Ok(())
		})
	}

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move {
			let guard = self.inner.lock().await;
			if let Some(state) = guard.as_ref() {
				state.tx.rollback_to_savepoint()?;
			}
			Ok(())
		})
	}

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { Ok(()) })
	}

	fn compact(&self, range: Option<Range<Key>>) -> BoxFut<'_, anyhow::Result<()>> {
		Box::pin(async move {
			// Create new flush options
			let mut fopts = FlushOptions::default();
			// Wait for the sync to finish
			fopts.set_wait(true);
			// Create new compact options
			let mut copts = CompactOptions::default();
			// Set the exclusive manual compaction flag
			copts.set_exclusive_manual_compaction(true);
			// Allow files to move to a higher level
			copts.set_change_level(true);
			// Set the target level for SSTs
			copts.set_target_level(6);
			// Force the bottommost SSTs to be rewritten
			copts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
			// Spawn a new task to compact the range
			affinitypool::spawn_local(move || {
				// Flush the WAL to storage
				self.db.flush_wal(true)?;
				// Flush the memtables to SST
				self.db.flush_opt(&fopts)?;
				// Get the compaction range
				let (start, end) = match range {
					Some(r) => (Some(r.start), Some(r.end)),
					None => (None, None),
				};
				// Compact the specified range with the bottommost target.
				self.db.compact_range_opt(start, end, &copts);
				// All ok
				Ok(())
			})
			.await
		})
	}
}

// Consume and iterate over only keys
fn consume_keys<D: rocksdb::DBAccess>(
	iter: &mut rocksdb::DBRawIteratorWithThreadMode<'_, D>,
	limit: ScanLimit,
	skip: u32,
	dir: Direction,
) -> Result<KeysResult> {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if iter.valid() {
			match dir {
				Direction::Forward => iter.next(),
				Direction::Backward => iter.prev(),
			}
		} else {
			// Catch any iterator errors
			iter.status()?;
			// Return an empty result
			return Ok(KeysResult::default());
		}
	}
	let mut key_bytes = 0u64;
	let keys = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key
				if let Some(k) = iter.key() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KEY).min(4096) as usize);
			// Check that we don't exceed the byte limit
			while key_bytes < b as u64 {
				// Check the key
				if let Some(k) = iter.key() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && key_bytes < b as u64 {
				// Check the key
				if let Some(k) = iter.key() {
					key_bytes += k.len() as u64;
					res.push(k.to_vec());
					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
	};
	// Catch any iterator errors
	iter.status()?;
	// Return the result
	Ok(KeysResult {
		keys,
		key_bytes,
	})
}

// Consume and iterate over keys and values
fn consume_vals<D: rocksdb::DBAccess>(
	iter: &mut rocksdb::DBRawIteratorWithThreadMode<'_, D>,
	limit: ScanLimit,
	skip: u32,
	dir: Direction,
) -> Result<ScanResult> {
	// Skip entries efficiently without allocation
	for _ in 0..skip {
		if iter.valid() {
			match dir {
				Direction::Forward => iter.next(),
				Direction::Backward => iter.prev(),
			}
		} else {
			// Catch any iterator errors
			iter.status()?;
			// Return an empty result
			return Ok(ScanResult::default());
		}
	}
	// Track the cumulative bytes for the metric. The byte-bounded limit
	// branches still rely on `bytes_fetched` (key + value bytes) to decide
	// when to stop, so the two counters are kept separate.
	let mut key_bytes = 0u64;
	let mut value_bytes = 0u64;
	let values = match limit {
		ScanLimit::Count(c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Check that we don't exceed the count limit
			while res.len() < c as usize {
				// Check the key and value
				if let Some((k, v)) = iter.item() {
					key_bytes += k.len() as u64;
					value_bytes += v.len() as u64;
					res.push((k.to_vec(), v.to_vec()));
					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::Bytes(b) => {
			// Create the result set
			let mut res = Vec::with_capacity((b / ESTIMATED_BYTES_PER_KV).min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the byte limit
			while bytes_fetched < b as u64 {
				// Check the key and value
				if let Some((k, v)) = iter.item() {
					let key_len = k.len() as u64;
					let value_len = v.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((k.to_vec(), v.to_vec()));

					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
		ScanLimit::BytesOrCount(b, c) => {
			// Create the result set
			let mut res = Vec::with_capacity(c.min(4096) as usize);
			// Count the bytes fetched
			let mut bytes_fetched = 0u64;
			// Check that we don't exceed the count limit AND the byte limit
			while res.len() < c as usize && bytes_fetched < b as u64 {
				// Check the key and value
				if let Some((k, v)) = iter.item() {
					let key_len = k.len() as u64;
					let value_len = v.len() as u64;

					bytes_fetched += key_len + value_len;
					key_bytes += key_len;
					value_bytes += value_len;

					res.push((k.to_vec(), v.to_vec()));

					match dir {
						Direction::Forward => iter.next(),
						Direction::Backward => iter.prev(),
					};
				} else {
					break;
				}
			}
			res
		}
	};
	// Catch any iterator errors
	iter.status()?;
	// Return the result
	Ok(ScanResult {
		values,
		key_bytes,
		value_bytes,
	})
}
