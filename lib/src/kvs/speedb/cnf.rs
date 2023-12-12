use once_cell::sync::Lazy;

pub static SPEEDB_THREAD_COUNT: Lazy<i32> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_THREAD_COUNT")
		.and_then(|s| s.parse::<i32>().ok())
		.unwrap_or(num_cpus::get() as i32)
});

pub static SPEEDB_WRITE_BUFFER_SIZE: Lazy<usize> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_WRITE_BUFFER_SIZE")
		.and_then(|s| s.parse::<usize>().ok())
		.unwrap_or(256 * 1024 * 1024)
});

pub static SPEEDB_TARGET_FILE_SIZE_BASE: Lazy<u64> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_TARGET_FILE_SIZE_BASE")
		.and_then(|s| s.parse::<u64>().ok())
		.unwrap_or(512 * 1024 * 1024)
});

pub static SPEEDB_MAX_WRITE_BUFFER_NUMBER: Lazy<i32> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_MAX_WRITE_BUFFER_NUMBER")
		.and_then(|s| s.parse::<i32>().ok())
		.unwrap_or(32)
});

pub static SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: Lazy<i32> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE")
		.and_then(|s| s.parse::<i32>().ok())
		.unwrap_or(4)
});

pub static SPEEDB_ENABLE_PIPELINED_WRITES: Lazy<bool> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_ENABLE_PIPELINED_WRITES")
		.and_then(|s| s.parse::<bool>().ok())
		.unwrap_or(true)
});

pub static SPEEDB_ENABLE_BLOB_FILES: Lazy<bool> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_ENABLE_BLOB_FILES")
		.and_then(|s| s.parse::<bool>().ok())
		.unwrap_or(true)
});

pub static SPEEDB_MIN_BLOB_SIZE: Lazy<u64> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_MIN_BLOB_SIZE")
		.and_then(|s| s.parse::<u64>().ok())
		.unwrap_or(4 * 1024)
});

pub static SPEEDB_KEEP_LOG_FILE_NUM: Lazy<usize> = Lazy::new(|| {
	option_env!("SURREAL_SPEEDB_KEEP_LOG_FILE_NUM")
		.and_then(|s| s.parse::<usize>().ok())
		.unwrap_or(20)
});
