use std::cmp::max;
use std::sync::LazyLock;

use sysinfo::System;

/// Should we sync writes to disk before acknowledgement
pub(super) static SYNC_DATA: LazyLock<bool> = lazy_env_parse!("SURREAL_SYNC_DATA", bool, false);

/// Whether to enable background WAL file flushing (default: false)
pub(super) static ROCKSDB_BACKGROUND_FLUSH: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_BACKGROUND_FLUSH", bool, false);

/// The interval in milliseconds between background flushes (default: 200)
pub(super) static ROCKSDB_BACKGROUND_FLUSH_INTERVAL: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_BACKGROUND_FLUSH_INTERVAL", u64, 200);

/// The number of threads to start for flushing and compaction (default: number
/// of CPUs)
pub(super) static ROCKSDB_THREAD_COUNT: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_THREAD_COUNT", i32, || num_cpus::get() as i32);

/// The maximum number of threads to use for flushing and compaction (default:
/// number of CPUs * 2)
pub(super) static ROCKSDB_JOBS_COUNT: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_JOBS_COUNT", i32, || num_cpus::get() as i32 * 2);

/// The maximum number of open files which can be opened by RocksDB (default:
/// 1024)
pub(super) static ROCKSDB_MAX_OPEN_FILES: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_OPEN_FILES", i32, 1024);

/// The size of each uncompressed data block in bytes (default: 64 KiB)
pub(super) static ROCKSDB_BLOCK_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_BLOCK_SIZE", usize, 64 * 1024);

/// The write-ahead-log size limit in MiB (default: 0)
pub(super) static ROCKSDB_WAL_SIZE_LIMIT: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_WAL_SIZE_LIMIT", u64, 0);

/// The maximum number of write buffers which can be used (default: dynamic from
/// 2 to 32)
pub(super) static ROCKSDB_MAX_WRITE_BUFFER_NUMBER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER", i32, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Dynamically set the number of write buffers
		if memory < 4 * 1024 * 1024 * 1024 {
			2 // For systems with < 4 GiB, use 2 buffers
		} else if memory < 16 * 1024 * 1024 * 1024 {
			4 // For systems with < 16 GiB, use 4 buffers
		} else if memory < 64 * 1024 * 1024 * 1024 {
			8 // For systems with < 64 GiB, use 8 buffers
		} else {
			32 // For systems with > 64 GiB, use 32 buffers
		}
	});

/// The amount of data each write buffer can build up in memory (default:
/// dynamic from 32 MiB to 128 MiB)
pub(super) static ROCKSDB_WRITE_BUFFER_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_WRITE_BUFFER_SIZE", usize, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Dynamically set the number of write buffers
		if memory < 1024 * 1024 * 1024 {
			32 * 1024 * 1024 // For systems with < 1 GiB, use 32 MiB
		} else if memory < 16 * 1024 * 1024 * 1024 {
			64 * 1024 * 1024 // For systems with < 16 GiB, use 64 MiB
		} else {
			128 * 1024 * 1024 // For all other systems, use 128 MiB
		}
	});

/// The target file size for compaction in bytes (default: 64 MiB)
pub(super) static ROCKSDB_TARGET_FILE_SIZE_BASE: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_TARGET_FILE_SIZE_BASE", u64, 64 * 1024 * 1024);

/// The target file size multiplier for each compaction level (default: 2)
pub(super) static ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER", i32, 2);

/// The minimum number of write buffers to merge before writing to disk
/// (default: 2)
pub(super) static ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE", i32, 2);

/// The number of files needed to trigger level 0 compaction (default: 4)
pub(super) static ROCKSDB_FILE_COMPACTION_TRIGGER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_FILE_COMPACTION_TRIGGER", i32, 4);

/// The readahead buffer size used during compaction (default: dynamic from 4
/// MiB to 16 MiB)
pub(super) static ROCKSDB_COMPACTION_READAHEAD_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_COMPACTION_READAHEAD_SIZE", usize, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Dynamically set the compaction readahead size
		if memory < 4 * 1024 * 1024 * 1024 {
			4 * 1024 * 1024 // For systems with < 4 GiB, use 4 MiB
		} else if memory < 16 * 1024 * 1024 * 1024 {
			8 * 1024 * 1024 // For systems with < 16 GiB, use 8 MiB
		} else {
			16 * 1024 * 1024 // For all other systems, use 16 MiB
		}
	});

/// The maximum number threads which will perform compactions (default: 4)
pub(super) static ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS: LazyLock<u32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS", u32, 4);

/// Whether to use separate queues for WAL writes and memtable writes (default:
/// true)
pub(super) static ROCKSDB_ENABLE_PIPELINED_WRITES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_PIPELINED_WRITES", bool, true);

/// Whether to enable separate key and value file storage (default: true)
pub(super) static ROCKSDB_ENABLE_BLOB_FILES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_BLOB_FILES", bool, true);

/// The minimum size of a value for it to be stored in blob files (default: 4
/// KiB)
pub(super) static ROCKSDB_MIN_BLOB_SIZE: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_MIN_BLOB_SIZE", u64, 4 * 1024);

/// The size of the least-recently-used block cache (default: 16 MiB)
pub(super) static ROCKSDB_BLOCK_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_ROCKSDB_BLOCK_CACHE_SIZE", usize, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Divide the total memory by 2
		let memory = memory.saturating_div(2);
		// Subtract 1 GiB from the memory size
		let memory = memory.saturating_sub(1024 * 1024 * 1024);
		// Take the larger of 16MiB or available memory
		max(memory as usize, 16 * 1024 * 1024)
	});

/// Whether to enable memory-mapped reads (default: false)
pub(super) static ROCKSDB_ENABLE_MEMORY_MAPPED_READS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_MEMORY_MAPPED_READS", bool, false);

/// Whether to enable memory-mapped writes (default: false)
pub(super) static ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES", bool, false);

/// The maximum number of information log files to keep (default: 10)
pub(super) static ROCKSDB_KEEP_LOG_FILE_NUM: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_KEEP_LOG_FILE_NUM", usize, 10);

/// The information log level of the RocksDB library (default: "warn")
pub(super) static ROCKSDB_STORAGE_LOG_LEVEL: LazyLock<String> =
	lazy_env_parse!("SURREAL_ROCKSDB_STORAGE_LOG_LEVEL", String, "warn".to_string());

/// Use to specify the database compaction style (default: "level")
pub(super) static ROCKSDB_COMPACTION_STYLE: LazyLock<String> =
	lazy_env_parse!("SURREAL_ROCKSDB_COMPACTION_STYLE", String, "level".to_string());

/// The size of the window used to track deletions (default: 1000)
pub(super) static ROCKSDB_DELETION_FACTORY_WINDOW_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_WINDOW_SIZE", usize, 1000);

/// The number of deletions to track in the window (default: 50)
pub(super) static ROCKSDB_DELETION_FACTORY_DELETE_COUNT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_DELETE_COUNT", usize, 50);

/// The ratio of deletions to track in the window (default: 0.5)
pub(super) static ROCKSDB_DELETION_FACTORY_RATIO: LazyLock<f64> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_RATIO", f64, 0.5);
