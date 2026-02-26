use std::cmp::max;

#[cfg(feature = "cli")]
use crate::parsers::{parse_bytes_u64, parse_bytes_usize, parse_duration_nanos};
use crate::system::SYSTEM_MEMORY;

const DEFAULT_MAX_OPEN_FILES: usize = 1024;
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;
const DEFAULT_WAL_SIZE_LIMIT: u64 = 0;
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 64 * 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE_MULTIPLIER: usize = 2;
const DEFAULT_FILE_COMPACTION_TRIGGER: usize = 4;
const DEFAULT_MAX_CONCURRENT_SUBCOMPACTIONS: u32 = 4;
const DEFAULT_ENABLE_PIPELINED_WRITES: bool = true;
const DEFAULT_KEEP_LOG_FILE_NUM: usize = 10;
const DEFAULT_DELETION_FACTORY_WINDOW_SIZE: usize = 1000;
const DEFAULT_DELETION_FACTORY_DELETE_COUNT: usize = 50;
const DEFAULT_DELETION_FACTORY_RATIO: f64 = 0.5;
const DEFAULT_ENABLE_BLOB_FILES: bool = true;
const DEFAULT_MIN_BLOB_SIZE: u64 = 4 * 1024;
const DEFAULT_BLOB_FILE_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_ENABLE_BLOB_GC: bool = true;
const DEFAULT_BLOB_GC_AGE_CUTOFF: f64 = 0.5;
const DEFAULT_BLOB_GC_FORCE_THRESHOLD: f64 = 0.5;
const DEFAULT_BLOB_COMPACTION_READAHEAD_SIZE: u64 = 0;
const DEFAULT_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: usize = 2;
const DEFAULT_SST_MAX_ALLOWED_SPACE_USAGE: u64 = 0;
const DEFAULT_GROUPED_COMMIT_TIMEOUT: u64 = 5_000_000; // 5ms in nanos
const DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD: usize = 12;
const DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE: usize = 4096;

fn default_compaction_readahead_size() -> usize {
	let memory = *SYSTEM_MEMORY;
	if memory < 4 * 1024 * 1024 * 1024 {
		4 * 1024 * 1024
	} else if memory < 16 * 1024 * 1024 * 1024 {
		8 * 1024 * 1024
	} else {
		16 * 1024 * 1024
	}
}

fn default_block_cache_size() -> usize {
	let memory = *SYSTEM_MEMORY;
	let memory = memory.saturating_div(2);
	let memory = memory.saturating_sub(1024 * 1024 * 1024);
	max(memory as usize, 16 * 1024 * 1024)
}

fn default_write_buffer_size() -> usize {
	let memory = *SYSTEM_MEMORY;
	if memory < 1024 * 1024 * 1024 {
		32 * 1024 * 1024
	} else if memory < 16 * 1024 * 1024 * 1024 {
		64 * 1024 * 1024
	} else {
		128 * 1024 * 1024
	}
}

fn default_max_write_buffer_number() -> usize {
	let memory = *SYSTEM_MEMORY;
	if memory < 4 * 1024 * 1024 * 1024 {
		2
	} else if memory < 16 * 1024 * 1024 * 1024 {
		4
	} else if memory < 64 * 1024 * 1024 * 1024 {
		8
	} else {
		32
	}
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct RocksDbEngineConfig {
	/// The number of threads to start for flushing and compaction
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_THREAD_COUNT",
		long = "rocksdb-thread-count",
		default_value_t = num_cpus::get(),
		hide = true,
	))]
	pub thread_count: usize,
	/// The maximum number of threads to use for flushing and compaction
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_JOBS_COUNT",
		long = "rocksdb-jobs-count",
		default_value_t = num_cpus::get() * 2,
		hide = true,
	))]
	pub jobs_count: usize,
	/// The maximum number of open files which can be opened by RocksDB
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_MAX_OPEN_FILES",
		long = "rocksdb-max-open-files",
		default_value_t = DEFAULT_MAX_OPEN_FILES,
		hide = true,
	))]
	pub max_open_files: usize,
	/// The size of each uncompressed data block in bytes
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOCK_SIZE",
		long = "rocksdb-block-size",
		default_value_t = DEFAULT_BLOCK_SIZE,
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub block_size: usize,
	/// The write-ahead-log size limit in MiB (0 = unlimited)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_WAL_SIZE_LIMIT",
		long = "rocksdb-wal-size-limit",
		default_value_t = DEFAULT_WAL_SIZE_LIMIT,
		hide = true,
	))]
	pub wal_size_limit: u64,
	/// The target file size for compaction in bytes
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_TARGET_FILE_SIZE_BASE",
		long = "rocksdb-target-file-size-base",
		default_value_t = DEFAULT_TARGET_FILE_SIZE_BASE,
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub target_file_size_base: u64,
	/// The target file size multiplier for each compaction level
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER",
		long = "rocksdb-target-file-size-multiplier",
		default_value_t = DEFAULT_TARGET_FILE_SIZE_MULTIPLIER,
		hide = true,
	))]
	pub target_file_size_multiplier: usize,
	/// The number of files needed to trigger level 0 compaction
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_FILE_COMPACTION_TRIGGER",
		long = "rocksdb-file-compaction-trigger",
		default_value_t = DEFAULT_FILE_COMPACTION_TRIGGER,
		hide = true,
	))]
	pub file_compaction_trigger: usize,
	/// The readahead buffer size used during compaction (dynamic based on system memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_COMPACTION_READAHEAD_SIZE",
		long = "rocksdb-compaction-readahead-size",
		default_value_t = default_compaction_readahead_size(),
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub compaction_readahead_size: usize,
	/// The maximum number of threads which will perform compactions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS",
		long = "rocksdb-max-concurrent-subcompactions",
		default_value_t = DEFAULT_MAX_CONCURRENT_SUBCOMPACTIONS,
		hide = true,
	))]
	pub max_concurrent_subcompactions: u32,
	/// Use separate queues for WAL writes and memtable writes
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_ENABLE_PIPELINED_WRITES",
		long = "rocksdb-enable-pipelined-writes",
		default_value_t = DEFAULT_ENABLE_PIPELINED_WRITES,
		hide = true,
	))]
	pub enable_pipelined_writes: bool,
	/// The maximum number of information log files to keep
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_KEEP_LOG_FILE_NUM",
		long = "rocksdb-keep-log-file-num",
		default_value_t = DEFAULT_KEEP_LOG_FILE_NUM,
		hide = true,
	))]
	pub keep_log_file_num: usize,
	/// The information log level of the RocksDB library
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_ROCKSDB_STORAGE_LOG_LEVEL",
			long = "rocksdb-storage-log-level",
			default_value = "warn",
			hide = true,
		)
	)]
	pub storage_log_level: String,
	/// The database compaction style
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_ROCKSDB_COMPACTION_STYLE",
			long = "rocksdb-compaction-style",
			default_value = "level",
			hide = true,
		)
	)]
	pub compaction_style: String,
	/// The size of the window used to track deletions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_DELETION_FACTORY_WINDOW_SIZE",
		long = "rocksdb-deletion-factory-window-size",
		default_value_t = DEFAULT_DELETION_FACTORY_WINDOW_SIZE,
		hide = true,
	))]
	pub deletion_factory_window_size: usize,
	/// The number of deletions to track in the window
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_DELETION_FACTORY_DELETE_COUNT",
		long = "rocksdb-deletion-factory-delete-count",
		default_value_t = DEFAULT_DELETION_FACTORY_DELETE_COUNT,
		hide = true,
	))]
	pub deletion_factory_delete_count: usize,
	/// The ratio of deletions to track in the window
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_DELETION_FACTORY_RATIO",
		long = "rocksdb-deletion-factory-ratio",
		default_value_t = DEFAULT_DELETION_FACTORY_RATIO,
		hide = true,
	))]
	pub deletion_factory_ratio: f64,
	/// Whether to enable separate key and value file storage
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_ENABLE_BLOB_FILES",
		long = "rocksdb-enable-blob-files",
		default_value_t = DEFAULT_ENABLE_BLOB_FILES,
		hide = true,
	))]
	pub enable_blob_files: bool,
	/// The minimum size of a value for it to be stored in blob files
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_MIN_BLOB_SIZE",
		long = "rocksdb-min-blob-size",
		default_value_t = DEFAULT_MIN_BLOB_SIZE,
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub min_blob_size: u64,
	/// The target blob file size
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOB_FILE_SIZE",
		long = "rocksdb-blob-file-size",
		default_value_t = DEFAULT_BLOB_FILE_SIZE,
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub blob_file_size: u64,
	/// Compression type used for blob files ("none", "snappy", "lz4", "zstd")
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_ROCKSDB_BLOB_COMPRESSION_TYPE",
			long = "rocksdb-blob-compression-type",
			hide = true,
		)
	)]
	pub blob_compression_type: Option<String>,
	/// Whether to enable blob garbage collection
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_ENABLE_BLOB_GC",
		long = "rocksdb-enable-blob-gc",
		default_value_t = DEFAULT_ENABLE_BLOB_GC,
		hide = true,
	))]
	pub enable_blob_gc: bool,
	/// Fractional age cutoff for blob GC eligibility (0.0 to 1.0)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOB_GC_AGE_CUTOFF",
		long = "rocksdb-blob-gc-age-cutoff",
		default_value_t = DEFAULT_BLOB_GC_AGE_CUTOFF,
		hide = true,
	))]
	pub blob_gc_age_cutoff: f64,
	/// Discardable ratio threshold to force GC (0.0 to 1.0)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOB_GC_FORCE_THRESHOLD",
		long = "rocksdb-blob-gc-force-threshold",
		default_value_t = DEFAULT_BLOB_GC_FORCE_THRESHOLD,
		hide = true,
	))]
	pub blob_gc_force_threshold: f64,
	/// Readahead size for blob compaction/GC
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOB_COMPACTION_READAHEAD_SIZE",
		long = "rocksdb-blob-compaction-readahead-size",
		default_value_t = DEFAULT_BLOB_COMPACTION_READAHEAD_SIZE,
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub blob_compaction_readahead_size: u64,
	/// The size of the least-recently-used block cache (dynamic based on system memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_BLOCK_CACHE_SIZE",
		long = "rocksdb-block-cache-size",
		default_value_t = default_block_cache_size(),
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub block_cache_size: usize,
	/// The amount of data each write buffer can build up in memory (dynamic based on system
	/// memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_WRITE_BUFFER_SIZE",
		long = "rocksdb-write-buffer-size",
		default_value_t = default_write_buffer_size(),
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub write_buffer_size: usize,
	/// The maximum number of write buffers which can be used (dynamic based on system memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER",
		long = "rocksdb-max-write-buffer-number",
		default_value_t = default_max_write_buffer_number(),
		hide = true,
	))]
	pub max_write_buffer_number: usize,
	/// The minimum number of write buffers to merge before writing to disk
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE",
		long = "rocksdb-min-write-buffer-number-to-merge",
		default_value_t = DEFAULT_MIN_WRITE_BUFFER_NUMBER_TO_MERGE,
		hide = true,
	))]
	pub min_write_buffer_number_to_merge: usize,
	/// The maximum allowed space usage for SST files in bytes (0 = unlimited).
	/// When reached, only read and delete operations are allowed.
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE",
		long = "rocksdb-sst-max-allowed-space-usage",
		default_value_t = DEFAULT_SST_MAX_ALLOWED_SPACE_USAGE,
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub sst_max_allowed_space_usage: u64,
	/// The maximum wait time in nanoseconds before forcing a grouped commit
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_GROUPED_COMMIT_TIMEOUT",
		long = "rocksdb-grouped-commit-timeout",
		default_value_t = DEFAULT_GROUPED_COMMIT_TIMEOUT,
		hide = true,
		value_parser = parse_duration_nanos,
	))]
	pub grouped_commit_timeout: u64,
	/// Threshold for deciding whether to wait for more transactions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_GROUPED_COMMIT_WAIT_THRESHOLD",
		long = "rocksdb-grouped-commit-wait-threshold",
		default_value_t = DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD,
		hide = true,
	))]
	pub grouped_commit_wait_threshold: usize,
	/// The maximum number of transactions in a single grouped commit batch
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_ROCKSDB_GROUPED_COMMIT_MAX_BATCH_SIZE",
		long = "rocksdb-grouped-commit-max-batch-size",
		default_value_t = DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE,
		hide = true,
	))]
	pub grouped_commit_max_batch_size: usize,
}

impl Default for RocksDbEngineConfig {
	fn default() -> Self {
		Self {
			thread_count: num_cpus::get(),
			jobs_count: num_cpus::get() * 2,
			max_open_files: DEFAULT_MAX_OPEN_FILES,
			block_size: DEFAULT_BLOCK_SIZE,
			wal_size_limit: DEFAULT_WAL_SIZE_LIMIT,
			target_file_size_base: DEFAULT_TARGET_FILE_SIZE_BASE,
			target_file_size_multiplier: DEFAULT_TARGET_FILE_SIZE_MULTIPLIER,
			file_compaction_trigger: DEFAULT_FILE_COMPACTION_TRIGGER,
			compaction_readahead_size: default_compaction_readahead_size(),
			max_concurrent_subcompactions: DEFAULT_MAX_CONCURRENT_SUBCOMPACTIONS,
			enable_pipelined_writes: DEFAULT_ENABLE_PIPELINED_WRITES,
			keep_log_file_num: DEFAULT_KEEP_LOG_FILE_NUM,
			storage_log_level: "warn".to_string(),
			compaction_style: "level".to_string(),
			deletion_factory_window_size: DEFAULT_DELETION_FACTORY_WINDOW_SIZE,
			deletion_factory_delete_count: DEFAULT_DELETION_FACTORY_DELETE_COUNT,
			deletion_factory_ratio: DEFAULT_DELETION_FACTORY_RATIO,
			enable_blob_files: DEFAULT_ENABLE_BLOB_FILES,
			min_blob_size: DEFAULT_MIN_BLOB_SIZE,
			blob_file_size: DEFAULT_BLOB_FILE_SIZE,
			blob_compression_type: None,
			enable_blob_gc: DEFAULT_ENABLE_BLOB_GC,
			blob_gc_age_cutoff: DEFAULT_BLOB_GC_AGE_CUTOFF,
			blob_gc_force_threshold: DEFAULT_BLOB_GC_FORCE_THRESHOLD,
			blob_compaction_readahead_size: DEFAULT_BLOB_COMPACTION_READAHEAD_SIZE,
			block_cache_size: default_block_cache_size(),
			write_buffer_size: default_write_buffer_size(),
			max_write_buffer_number: default_max_write_buffer_number(),
			min_write_buffer_number_to_merge: DEFAULT_MIN_WRITE_BUFFER_NUMBER_TO_MERGE,
			sst_max_allowed_space_usage: DEFAULT_SST_MAX_ALLOWED_SPACE_USAGE,
			grouped_commit_timeout: DEFAULT_GROUPED_COMMIT_TIMEOUT,
			grouped_commit_wait_threshold: DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD,
			grouped_commit_max_batch_size: DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE,
		}
	}
}
