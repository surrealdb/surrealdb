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
