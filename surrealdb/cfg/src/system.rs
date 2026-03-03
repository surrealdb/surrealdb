use std::sync::LazyLock;

pub(crate) static SYSTEM_MEMORY: LazyLock<u64> = LazyLock::new(|| {
	let mut system = sysinfo::System::new_all();
	system.refresh_memory();
	match system.cgroup_limits() {
		Some(limits) => limits.total_memory,
		None => system.total_memory(),
	}
});
