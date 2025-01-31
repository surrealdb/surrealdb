use std::cmp::max;
use std::sync::LazyLock;
use sysinfo::System;

pub static SYNC_DATA: LazyLock<bool> = lazy_env_parse!("SURREAL_SYNC_DATA", bool, false);

pub static ROCKSDB_BACKGROUND_FLUSH: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_BACKGROUND_FLUSH", bool, false);

pub static ROCKSDB_BACKGROUND_FLUSH_INTERVAL: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_BACKGROUND_FLUSH_INTERVAL", u64, 200);

pub static ROCKSDB_THREAD_COUNT: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_ROCKSDB_THREAD_COUNT", i32, |_| num_cpus::get() as i32);

pub static ROCKSDB_JOBS_COUNT: LazyLock<i32> =
	lazy_env_parse_or_else!("SURREAL_ROCKSDB_JOBS_COUNT", i32, |_| num_cpus::get() as i32 * 2);

pub static ROCKSDB_MAX_OPEN_FILES: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_OPEN_FILES", i32, 1024);

pub static ROCKSDB_BLOCK_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_BLOCK_SIZE", usize, 64 * 1024);

pub static ROCKSDB_WAL_SIZE_LIMIT: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_WAL_SIZE_LIMIT", u64, 1024);

pub static ROCKSDB_MAX_WRITE_BUFFER_NUMBER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER", i32, 32);

pub static ROCKSDB_WRITE_BUFFER_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_WRITE_BUFFER_SIZE", usize, 256 * 1024 * 1024);

pub static ROCKSDB_TARGET_FILE_SIZE_BASE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_TARGET_FILE_SIZE_BASE", u64, 128 * 1024 * 1024);

pub static ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER", i32, 10);

pub static ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE", i32, 6);

pub static ROCKSDB_FILE_COMPACTION_TRIGGER: LazyLock<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_FILE_COMPACTION_TRIGGER", i32, 16);

pub static ROCKSDB_COMPACTION_READAHEAD_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_COMPACTION_READAHEAD_SIZE", usize, 16 * 1024 * 1024);

pub static ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS: LazyLock<u32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_CONCURRENT_SUBCOMPACTIONS", u32, 4);

pub static ROCKSDB_ENABLE_PIPELINED_WRITES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_PIPELINED_WRITES", bool, true);

pub static ROCKSDB_ENABLE_BLOB_FILES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_BLOB_FILES", bool, true);

pub static ROCKSDB_MIN_BLOB_SIZE: LazyLock<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_MIN_BLOB_SIZE", u64, 4 * 1024);

pub static ROCKSDB_BLOCK_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse_or_else!("SURREAL_ROCKSDB_BLOCK_CACHE_SIZE", usize, |_| {
		// Load the system attributes
		let system = System::new_all();
		// Get the total system memory
		let memory = system.total_memory();
		// Divide the total system memory by 2
		let memory = memory.saturating_div(2);
		// Subtract 1 GiB from the memory size
		let memory = memory.saturating_sub(1024 * 1024 * 1024);
		// Take the larger of 512MiB or available memory
		max(memory as usize, 512 * 1024 * 1024)
	});

pub static ROCKSDB_ENABLE_MEMORY_MAPPED_READS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_MEMORY_MAPPED_READS", bool, false);

pub static ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_MEMORY_MAPPED_WRITES", bool, false);

pub static ROCKSDB_KEEP_LOG_FILE_NUM: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_KEEP_LOG_FILE_NUM", usize, 20);

pub static ROCKSDB_STORAGE_LOG_LEVEL: LazyLock<String> =
	lazy_env_parse!("SURREAL_ROCKSDB_STORAGE_LOG_LEVEL", String, "warn".to_string());

pub static ROCKSDB_COMPACTION_STYLE: LazyLock<String> =
	lazy_env_parse!("SURREAL_ROCKSDB_COMPACTION_STYLE", String, "level".to_string());

pub static ROCKSDB_DELETION_FACTORY_WINDOW_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_WINDOW_SIZE", usize, 1000);

pub static ROCKSDB_DELETION_FACTORY_DELETE_COUNT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_DELETE_COUNT", usize, 50);

pub static ROCKSDB_DELETION_FACTORY_RATIO: LazyLock<f64> =
	lazy_env_parse!("SURREAL_ROCKSDB_DELETION_FACTORY_RATIO", f64, 0.5);
