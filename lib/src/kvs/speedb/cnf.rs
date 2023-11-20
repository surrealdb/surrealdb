use once_cell::sync::Lazy;

pub static SPEEDB_THREAD_COUNT: Lazy<i32> = Lazy::new(|| {
	let default = num_cpus::get() as i32;
	std::env::var("SURREAL_SPEEDB_THREAD_COUNT")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_WRITE_BUFFER_SIZE: Lazy<usize> = Lazy::new(|| {
	let default = 256 * 1024 * 1024;
	std::env::var("SURREAL_SPEEDB_WRITE_BUFFER_SIZE")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_TARGET_FILE_SIZE_BASE: Lazy<u64> = Lazy::new(|| {
	let default = 512 * 1024 * 1024;
	std::env::var("SURREAL_SPEEDB_TARGET_FILE_SIZE_BASE")
		.map(|v| v.parse::<u64>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_MAX_WRITE_BUFFER_NUMBER: Lazy<i32> = Lazy::new(|| {
	let default = 32;
	std::env::var("SURREAL_SPEEDB_MAX_WRITE_BUFFER_NUMBER")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: Lazy<i32> = Lazy::new(|| {
	let default = 4;
	std::env::var("SURREAL_SPEEDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_ENABLE_PIPELINED_WRITES: Lazy<bool> = Lazy::new(|| {
	let default = true;
	std::env::var("SURREAL_SPEEDB_ENABLE_PIPELINED_WRITES")
		.map(|v| v.parse::<bool>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_ENABLE_BLOB_FILES: Lazy<bool> = Lazy::new(|| {
	let default = true;
	std::env::var("SURREAL_SPEEDB_ENABLE_BLOB_FILES")
		.map(|v| v.parse::<bool>().unwrap_or(default))
		.unwrap_or(default)
});

pub static SPEEDB_MIN_BLOB_SIZE: Lazy<u64> = Lazy::new(|| {
	let default = 4 * 1024;
	std::env::var("SURREAL_SPEEDB_ENABLE_BLOB_FILES")
		.map(|v| v.parse::<u64>().unwrap_or(default))
		.unwrap_or(default)
});
