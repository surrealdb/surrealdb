use std::sync::LazyLock;

use sysinfo::System;

/// Detected total memory available to this process in bytes, cached at first
/// access.
///
/// The host's memory, lowered to the memory limit of any cgroup the process
/// runs under: the one a container's cgroup namespace presents as the root of
/// the hierarchy, and the one a process gets inside a larger hierarchy, such
/// as a systemd unit's `MemoryMax` or `MemoryHigh`, read along the process's
/// own cgroup path in `/proc/self/cgroup`. A conservative 1 GiB stands in for the host's memory
/// when `/proc/meminfo` is inaccessible (e.g. systemd `ProcSubset=pid` hardening),
/// and the cgroup limits lower it as they would the host's.
pub static TOTAL_SYSTEM_MEMORY: LazyLock<u64> = LazyLock::new(|| {
	// Load the system attributes
	let mut system = System::new();
	// Refresh the system memory
	system.refresh_memory();
	// The limit along this process's own cgroup path, read without `sysinfo`
	let own_limit = own_cgroup_memory_limit().unwrap_or(u64::MAX);
	// The total system memory, or a safe default where it cannot be read.
	// `sysinfo` reads the root cgroup limit only once the host's memory is
	// known, and asserts otherwise, so the default is lowered by this
	// process's own cgroup limit alone.
	let host_memory = system.total_memory();
	if host_memory == 0 {
		return (1024 * 1024 * 1024).min(own_limit);
	}
	// The limit at the root of the cgroup hierarchy this process sees
	let root_limit = match system.cgroup_limits() {
		Some(l) if l.total_memory > 0 => l.total_memory,
		_ => u64::MAX,
	};
	host_memory.min(root_limit).min(own_limit)
});

/// The tightest memory limit set on this process's own cgroup or any cgroup
/// above it, or `None` where none is set or none can be read.
#[cfg(target_os = "linux")]
fn own_cgroup_memory_limit() -> Option<u64> {
	let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
	cgroup_memory_limit(&membership, std::path::Path::new("/sys/fs/cgroup"))
}

#[cfg(not(target_os = "linux"))]
fn own_cgroup_memory_limit() -> Option<u64> {
	None
}

/// The tightest memory limit along the cgroup paths `membership` (the contents
/// of `/proc/<pid>/cgroup`) names, read from the cgroup filesystem mounted at
/// `mount`.
///
/// A v2 entry (`0::<path>`) is read from `memory.max` and `memory.high` under
/// `mount` (the kernel throttles a cgroup above its `memory.high`, so memory
/// sized past it is as unusable as memory past `memory.max`), and a v1 entry for
/// the `memory` controller from `memory.limit_in_bytes` under `mount/memory`. Each directory from
/// the cgroup up to the mount is read, since a limit on a parent bounds every cgroup beneath it. A
/// value that does not parse as a positive number, such as v2's `max`, sets no limit.
#[cfg(any(target_os = "linux", test))]
fn cgroup_memory_limit(membership: &str, mount: &std::path::Path) -> Option<u64> {
	let mut limit: Option<u64> = None;
	for line in membership.lines() {
		let mut fields = line.splitn(3, ':');
		let (Some(_), Some(controllers), Some(path)) =
			(fields.next(), fields.next(), fields.next())
		else {
			continue;
		};
		let (root, files): (_, &[&str]) = if controllers.is_empty() {
			(mount.to_path_buf(), &["memory.max", "memory.high"])
		} else if controllers.split(',').any(|c| c == "memory") {
			(mount.join("memory"), &["memory.limit_in_bytes"])
		} else {
			continue;
		};
		// A path with `..` names a cgroup outside this process's cgroup
		// namespace, which the mount cannot reach: only the mount's own
		// limit applies.
		let mut dir = root.clone();
		if !path.split('/').any(|p| p == "..") {
			for part in path.split('/').filter(|p| !p.is_empty() && *p != ".") {
				dir.push(part);
			}
		}
		loop {
			for file in files {
				let value = std::fs::read_to_string(dir.join(file))
					.ok()
					.and_then(|v| v.trim().parse::<u64>().ok())
					.filter(|v| *v > 0);
				if let Some(value) = value {
					limit = Some(limit.map_or(value, |l| l.min(value)));
				}
			}
			if dir == root || !dir.pop() {
				break;
			}
		}
	}
	limit
}

#[cfg(test)]
mod tests {
	use std::path::{Path, PathBuf};

	use super::cgroup_memory_limit;

	/// A scratch cgroup mount, removed when dropped.
	struct Mount(PathBuf);

	impl Mount {
		fn new(name: &str) -> Self {
			let dir = std::env::temp_dir()
				.join(format!("surrealdb-cgroup-test-{name}-{}", std::process::id()));
			let _ = std::fs::remove_dir_all(&dir);
			std::fs::create_dir_all(&dir).unwrap();
			Self(dir)
		}

		fn set(&self, dir: &str, file: &str, value: &str) {
			let dir = self.0.join(dir);
			std::fs::create_dir_all(&dir).unwrap();
			std::fs::write(dir.join(file), value).unwrap();
		}

		fn path(&self) -> &Path {
			&self.0
		}
	}

	impl Drop for Mount {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}

	#[test]
	fn a_systemd_unit_limit_below_the_root_is_read() {
		let mount = Mount::new("v2-unit");
		mount.set("", "memory.max", "max\n");
		mount.set("system.slice", "memory.max", "max\n");
		mount.set("system.slice/surreal.service", "memory.max", "68719476736\n");
		let membership = "0::/system.slice/surreal.service\n";
		assert_eq!(cgroup_memory_limit(membership, mount.path()), Some(68_719_476_736));
	}

	#[test]
	fn a_parent_limit_bounds_a_cgroup_without_one() {
		let mount = Mount::new("v2-parent");
		mount.set("user.slice", "memory.max", "1073741824\n");
		mount.set("user.slice/session-1.scope", "memory.max", "max\n");
		let membership = "0::/user.slice/session-1.scope\n";
		assert_eq!(cgroup_memory_limit(membership, mount.path()), Some(1_073_741_824));
	}

	#[test]
	fn the_tightest_limit_along_the_path_wins() {
		let mount = Mount::new("v2-tightest");
		mount.set("a", "memory.max", "1000\n");
		mount.set("a/b", "memory.max", "5000\n");
		assert_eq!(cgroup_memory_limit("0::/a/b\n", mount.path()), Some(1000));
	}

	#[test]
	fn a_memory_high_limit_is_read() {
		let mount = Mount::new("v2-high");
		mount.set("system.slice/surreal.service", "memory.max", "max\n");
		mount.set("system.slice/surreal.service", "memory.high", "8589934592\n");
		let membership = "0::/system.slice/surreal.service\n";
		assert_eq!(cgroup_memory_limit(membership, mount.path()), Some(8_589_934_592));
	}

	#[test]
	fn a_v1_memory_controller_limit_is_read() {
		let mount = Mount::new("v1");
		mount.set("memory/docker/abc", "memory.limit_in_bytes", "2147483648\n");
		let membership = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n";
		assert_eq!(cgroup_memory_limit(membership, mount.path()), Some(2_147_483_648));
	}

	#[test]
	fn a_path_outside_the_namespace_reads_only_the_mount() {
		let mount = Mount::new("v2-outside");
		mount.set("", "memory.max", "4294967296\n");
		mount.set("other.slice/foo.service", "memory.max", "1000\n");
		let membership = "0::/../other.slice/foo.service\n";
		assert_eq!(cgroup_memory_limit(membership, mount.path()), Some(4_294_967_296));
	}

	#[test]
	fn no_limit_is_none() {
		let mount = Mount::new("none");
		mount.set("", "memory.max", "max\n");
		assert_eq!(cgroup_memory_limit("0::/\n", mount.path()), None);
		assert_eq!(cgroup_memory_limit("", mount.path()), None);
		assert_eq!(cgroup_memory_limit("0::/missing/path\n", mount.path()), None);
		mount.set("zero", "memory.max", "0\n");
		assert_eq!(cgroup_memory_limit("0::/zero\n", mount.path()), None);
	}
}
