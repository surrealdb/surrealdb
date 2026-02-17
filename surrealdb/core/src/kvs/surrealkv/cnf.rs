use std::cmp::max;
use std::sync::LazyLock;

use sysinfo::System;

/// Whether to enable value log separation (default: true)
pub(super) static SURREALKV_ENABLE_VLOG: LazyLock<bool> =
	lazy_env_parse!("SURREAL_SURREALKV_ENABLE_VLOG", bool, true);

/// Whether to enable versioned index (default: false, only applies when versioning is enabled)
pub(super) static SURREALKV_VERSIONED_INDEX: LazyLock<bool> =
	lazy_env_parse!("SURREAL_SURREALKV_VERSIONED_INDEX", bool, false);

/// The block size in bytes (default: 64 KiB)
pub(super) static SURREALKV_BLOCK_SIZE: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_BLOCK_SIZE", usize, 64 * 1024);

/// The maximum value log file size in bytes (default: dynamic from 64 MiB to 512 MiB)
pub(super) static SURREALKV_VLOG_MAX_FILE_SIZE: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_VLOG_MAX_FILE_SIZE", u64, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Dynamically set the vlog max file size based on available memory
		if memory < 4 * 1024 * 1024 * 1024 {
			64 * 1024 * 1024 // For systems with < 4 GiB, use 64 MiB
		} else if memory < 16 * 1024 * 1024 * 1024 {
			128 * 1024 * 1024 // For systems with < 16 GiB, use 128 MiB
		} else if memory < 64 * 1024 * 1024 * 1024 {
			256 * 1024 * 1024 // For systems with < 64 GiB, use 256 MiB
		} else {
			512 * 1024 * 1024 // For systems with > 64 GiB, use 512 MiB
		}
	});

/// The value log threshold in bytes - values larger than this are stored in the value log (default:
/// 4 KiB)
pub(super) static SURREALKV_VLOG_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_VLOG_THRESHOLD", usize, 4 * 1024);

/// The block cache capacity in bytes (default: dynamic based on memory)
pub(super) static SURREALKV_BLOCK_CACHE_CAPACITY: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_BLOCK_CACHE_CAPACITY", u64, || {
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
		max(memory, 16 * 1024 * 1024)
	});

/// The maximum wait time in nanoseconds before forcing a grouped commit (default: 5ms).
/// This timeout ensures that transactions don't wait indefinitely under low concurrency and
/// balances commit latency against write throughput.
pub(super) static SURREALKV_GROUPED_COMMIT_TIMEOUT: LazyLock<u64> =
	lazy_env_parse!(duration, "SURREAL_SURREALKV_GROUPED_COMMIT_TIMEOUT", u64, || {
		std::time::Duration::from_millis(5).as_nanos() as u64
	});

/// Threshold for deciding whether to wait for more transactions (default: 12)
/// If the current batch size is greater or equal to this threshold (and below
/// SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE), then the coordinator will wait up to
/// SURREALKV_GROUPED_COMMIT_TIMEOUT to collect more transactions. Smaller batches are flushed
/// immediately to preserve low latency.
pub(super) static SURREALKV_GROUPED_COMMIT_WAIT_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALKV_GROUPED_COMMIT_WAIT_THRESHOLD", usize, 12);

/// The maximum number of transactions in a single grouped commit batch (default: 4096)
/// This prevents unbounded memory growth while still allowing large batches for efficiency.
/// Larger batches improve throughput but increase memory usage and commit latency.
pub(super) static SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE", usize, 4096);
