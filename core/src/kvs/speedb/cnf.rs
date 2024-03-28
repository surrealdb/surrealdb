use crate::{lazy_env_parse, lazy_env_parse_or_else};
use once_cell::sync::Lazy;

pub static SPEEDB_THREAD_COUNT: Lazy<i32> =
	lazy_env_parse_or_else!("SURREAL_SPEEDB_THREAD_COUNT", i32, |_| num_cpus::get() as i32);

pub static SPEEDB_WRITE_BUFFER_SIZE: Lazy<usize> =
	lazy_env_parse!("SURREAL_SPEEDB_WRITE_BUFFER_SIZE", usize, 256 * 1024 * 1024);

pub static SPEEDB_TARGET_FILE_SIZE_BASE: Lazy<u64> =
	lazy_env_parse!("SURREAL_SPEEDB_TARGET_FILE_SIZE_BASE", u64, 512 * 1024 * 1024);

pub static SPEEDB_MAX_WRITE_BUFFER_NUMBER: Lazy<i32> =
	lazy_env_parse!("SURREAL_SPEEDB_MAX_WRITE_BUFFER_NUMBER", i32, 32);

pub static SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: Lazy<i32> =
	lazy_env_parse!("SURREAL_SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE", i32, 4);

pub static SPEEDB_ENABLE_PIPELINED_WRITES: Lazy<bool> =
	lazy_env_parse!("SURREAL_SPEEDB_ENABLE_PIPELINED_WRITES", bool, true);

pub static SPEEDB_ENABLE_BLOB_FILES: Lazy<bool> =
	lazy_env_parse!("SURREAL_SPEEDB_ENABLE_BLOB_FILES", bool, true);

pub static SPEEDB_MIN_BLOB_SIZE: Lazy<u64> =
	lazy_env_parse!("SURREAL_SPEEDB_MIN_BLOB_SIZE", u64, 4 * 1024);

pub static SPEEDB_KEEP_LOG_FILE_NUM: Lazy<usize> =
	lazy_env_parse!("SURREAL_SPEEDB_KEEP_LOG_FILE_NUM", usize, 20);
