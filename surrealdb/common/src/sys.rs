use std::sync::LazyLock;

use sysinfo::System;

/// Detected total system memory in bytes, cached at first access.
/// Falls back to cgroup limits when running inside a container, and
/// uses a conservative 1 GiB default when `/proc` is inaccessible
/// (e.g. systemd `ProcSubset=pid` hardening).
pub static TOTAL_SYSTEM_MEMORY: LazyLock<u64> = LazyLock::new(|| {
	// Load the system attributes
	let mut system = System::new();
	// Refresh the system memory
	system.refresh_memory();
	// Get the total system memory
	let host_memory = system.total_memory();
	// If the total system memory is 0, use a safe default
	if host_memory == 0 {
		return 1024 * 1024 * 1024;
	}
	// Prefer cgroup limits when available (container environments)
	match system.cgroup_limits() {
		// If the limit has been configured, use it
		Some(l) if l.total_memory > 0 => l.total_memory,
		// Otherwise use the host memory
		_ => host_memory,
	}
});
