use once_cell::sync::Lazy;

pub static ROCKSDB_THREAD_COUNT: Lazy<i32> = Lazy::new(|| {
	let default = num_cpus::get() as i32;
	std::env::var("SURREAL_ROCKSDB_THREAD_COUNT")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_WRITE_BUFFER_SIZE: Lazy<usize> = Lazy::new(|| {
	let default = 256 * 1024 * 1024;
	std::env::var("SURREAL_ROCKSDB_WRITE_BUFFER_SIZE")
		.map(|v| v.parse::<usize>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_TARGET_FILE_SIZE_BASE: Lazy<u64> = Lazy::new(|| {
	let default = 512 * 1024 * 1024;
	std::env::var("SURREAL_ROCKSDB_TARGET_FILE_SIZE_BASE")
		.map(|v| v.parse::<u64>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_MAX_WRITE_BUFFER_NUMBER: Lazy<i32> = Lazy::new(|| {
	let default = 32;
	std::env::var("SURREAL_ROCKSDB_MAX_WRITE_BUFFER_NUMBER")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE: Lazy<i32> = Lazy::new(|| {
	let default = 4;
	std::env::var("SURREAL_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE")
		.map(|v| v.parse::<i32>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_ENABLE_PIPELINED_WRITES: Lazy<bool> = Lazy::new(|| {
	let default = true;
	std::env::var("SURREAL_ROCKSDB_ENABLE_PIPELINED_WRITES")
		.map(|v| v.parse::<bool>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_ENABLE_BLOB_FILES: Lazy<bool> = Lazy::new(|| {
	let default = true;
	std::env::var("SURREAL_ROCKSDB_ENABLE_BLOB_FILES")
		.map(|v| v.parse::<bool>().unwrap_or(default))
		.unwrap_or(default)
});

pub static ROCKSDB_MIN_BLOB_SIZE: Lazy<u64> = Lazy::new(|| {
	let default = 4 * 1024;
	std::env::var("SURREAL_ROCKSDB_MIN_BLOB_SIZE")
		.map(|v| v.parse::<u64>().unwrap_or(default))
		.unwrap_or(default)
});
