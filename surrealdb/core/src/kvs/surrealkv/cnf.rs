use std::cmp::max;
use std::sync::{LazyLock, OnceLock};

use sysinfo::System;

/// Should we sync writes to disk before acknowledgement
pub(super) static SYNC_DATA: LazyLock<bool> = lazy_env_parse!("SURREAL_SYNC_DATA", bool, false);

/// The size to store values in the tree, or a separate log file (default: 64
/// bytes)
pub(super) static SURREALKV_MAX_VALUE_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_MAX_VALUE_THRESHOLD", usize, 64);

/// The maximum size of a single data file segment (default: 512 MiB)
pub(super) static SURREALKV_MAX_SEGMENT_SIZE: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_MAX_SEGMENT_SIZE", u64, 1 << 29);

/// The size of the in-memory value cache (default: 16 MiB)
pub(super) static SURREALKV_MAX_VALUE_CACHE_SIZE: LazyLock<u64> =
	lazy_env_parse!(bytes, "SURREAL_SURREALKV_MAX_VALUE_CACHE_SIZE", u64, || {
		// Load the system attributes
		let mut system = System::new_all();
		// Refresh the system memory
		system.refresh_memory();
		// Get the available memory
		let memory = match system.cgroup_limits() {
			Some(limits) => limits.total_memory,
			None => system.total_memory(),
		};
		// Divide the total system memory by 2
		let memory = memory.saturating_div(2);
		// Subtract 1 GiB from the memory size
		let memory = memory.saturating_sub(1024 * 1024 * 1024);
		// Take the larger of 16MiB or available memory
		max(memory, 16 * 1024 * 1024)
	});

pub(super) static SKV_COMMIT_POOL: OnceLock<affinitypool::Threadpool> = OnceLock::new();

pub(super) fn commit_pool() -> &'static affinitypool::Threadpool {
	SKV_COMMIT_POOL.get_or_init(|| {
		affinitypool::Builder::new()
			.thread_name("surrealkv-commitpool")
			.thread_stack_size(5 * 1024 * 1024)
			.thread_per_core(false)
			.worker_threads(1)
			.build()
	})
}
