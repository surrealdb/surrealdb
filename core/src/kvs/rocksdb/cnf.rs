use crate::{lazy_env_parse, lazy_env_parse_or_else};
use once_cell::sync::Lazy;

pub static ROCKSDB_THREAD_COUNT: Lazy<i32> =
	lazy_env_parse_or_else!("SURREAL_ROCKSDB_THREAD_COUNT", i32, |_| num_cpus::get() as i32);

pub static ROCKSDB_WRITE_BUFFER_SIZE: Lazy<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_WRITE_BUFFER_SIZE", usize, 256 * 1024 * 1024);

pub static ROCKSDB_TARGET_FILE_SIZE_BASE: Lazy<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_TARGET_FILE_SIZE_BASE", u64, 512 * 1024 * 1024);

pub static ROCKSDB_MAX_WRITE_BUFFER_NUMBER: Lazy<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER", i32, 32);

pub static ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: Lazy<i32> =
	lazy_env_parse!("SURREAL_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE", i32, 4);

pub static ROCKSDB_ENABLE_PIPELINED_WRITES: Lazy<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_PIPELINED_WRITES", bool, true);

pub static ROCKSDB_ENABLE_BLOB_FILES: Lazy<bool> =
	lazy_env_parse!("SURREAL_ROCKSDB_ENABLE_BLOB_FILES", bool, true);

pub static ROCKSDB_MIN_BLOB_SIZE: Lazy<u64> =
	lazy_env_parse!("SURREAL_ROCKSDB_MIN_BLOB_SIZE", u64, 4 * 1024);

pub static ROCKSDB_KEEP_LOG_FILE_NUM: Lazy<usize> =
	lazy_env_parse!("SURREAL_ROCKSDB_KEEP_LOG_FILE_NUM", usize, 20);
