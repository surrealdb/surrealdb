use std::cmp::max;

#[cfg(feature = "cli")]
use crate::parsers::{parse_bytes_u64, parse_bytes_usize, parse_duration_nanos};
use crate::system::SYSTEM_MEMORY;

const DEFAULT_ENABLE_VLOG: bool = true;
const DEFAULT_VERSIONED_INDEX: bool = false;
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024;
const DEFAULT_VLOG_THRESHOLD: usize = 4 * 1024;
const DEFAULT_GROUPED_COMMIT_TIMEOUT: u64 = 5_000_000; // 5ms in nanos
const DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD: usize = 12;
const DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE: usize = 4096;

fn default_vlog_max_file_size() -> u64 {
	let memory = *SYSTEM_MEMORY;
	if memory < 4 * 1024 * 1024 * 1024 {
		64 * 1024 * 1024
	} else if memory < 16 * 1024 * 1024 * 1024 {
		128 * 1024 * 1024
	} else if memory < 64 * 1024 * 1024 * 1024 {
		256 * 1024 * 1024
	} else {
		512 * 1024 * 1024
	}
}

fn default_block_cache_capacity() -> u64 {
	let memory = *SYSTEM_MEMORY;
	let memory = memory.saturating_div(2);
	let memory = memory.saturating_sub(1024 * 1024 * 1024);
	max(memory, 16 * 1024 * 1024)
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct SurrealKvEngineConfig {
	/// Whether to enable value log separation
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_ENABLE_VLOG",
		long = "surrealkv-enable-vlog",
		default_value_t = DEFAULT_ENABLE_VLOG,
		hide = true,
	))]
	pub enable_vlog: bool,
	/// Whether to enable versioned index (only applies when versioning is enabled)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_VERSIONED_INDEX",
		long = "surrealkv-versioned-index",
		default_value_t = DEFAULT_VERSIONED_INDEX,
		hide = true,
	))]
	pub versioned_index: bool,
	/// The block size in bytes
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_BLOCK_SIZE",
		long = "surrealkv-block-size",
		default_value_t = DEFAULT_BLOCK_SIZE,
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub block_size: usize,
	/// The maximum value log file size in bytes (dynamic based on system memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_VLOG_MAX_FILE_SIZE",
		long = "surrealkv-vlog-max-file-size",
		default_value_t = default_vlog_max_file_size(),
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub vlog_max_file_size: u64,
	/// The value log threshold in bytes - values larger than this are stored in the value log
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_VLOG_THRESHOLD",
		long = "surrealkv-vlog-threshold",
		default_value_t = DEFAULT_VLOG_THRESHOLD,
		hide = true,
		value_parser = parse_bytes_usize,
	))]
	pub vlog_threshold: usize,
	/// The block cache capacity in bytes (dynamic based on system memory)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_BLOCK_CACHE_CAPACITY",
		long = "surrealkv-block-cache-capacity",
		default_value_t = default_block_cache_capacity(),
		hide = true,
		value_parser = parse_bytes_u64,
	))]
	pub block_cache_capacity: u64,
	/// The maximum wait time in nanoseconds before forcing a grouped commit
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_GROUPED_COMMIT_TIMEOUT",
		long = "surrealkv-grouped-commit-timeout",
		default_value_t = DEFAULT_GROUPED_COMMIT_TIMEOUT,
		hide = true,
		value_parser = parse_duration_nanos,
	))]
	pub grouped_commit_timeout: u64,
	/// Threshold for deciding whether to wait for more transactions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_GROUPED_COMMIT_WAIT_THRESHOLD",
		long = "surrealkv-grouped-commit-wait-threshold",
		default_value_t = DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD,
		hide = true,
	))]
	pub grouped_commit_wait_threshold: usize,
	/// The maximum number of transactions in a single grouped commit batch
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE",
		long = "surrealkv-grouped-commit-max-batch-size",
		default_value_t = DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE,
		hide = true,
	))]
	pub grouped_commit_max_batch_size: usize,
}

impl Default for SurrealKvEngineConfig {
	fn default() -> Self {
		Self {
			enable_vlog: DEFAULT_ENABLE_VLOG,
			versioned_index: DEFAULT_VERSIONED_INDEX,
			block_size: DEFAULT_BLOCK_SIZE,
			vlog_max_file_size: default_vlog_max_file_size(),
			vlog_threshold: DEFAULT_VLOG_THRESHOLD,
			block_cache_capacity: default_block_cache_capacity(),
			grouped_commit_timeout: DEFAULT_GROUPED_COMMIT_TIMEOUT,
			grouped_commit_wait_threshold: DEFAULT_GROUPED_COMMIT_WAIT_THRESHOLD,
			grouped_commit_max_batch_size: DEFAULT_GROUPED_COMMIT_MAX_BATCH_SIZE,
		}
	}
}
