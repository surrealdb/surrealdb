#![cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv"))]

/// Create the shared KVS blocking threadpool.
///
/// Size and pinning behaviour are driven by [`crate::cnf::KVS_THREADPOOL_SIZE`]:
///
/// * When the resolved size matches the host's logical core count *and* that count is at least 16,
///   the pool uses `affinitypool::thread_per_core` so each worker is pinned to a dedicated core.
///   This is the default on ≥16-core hosts.
/// * When the size is below 16 on a small-core host (the computed default floor), the pool is sized
///   to 16 unpinned workers — enough slack to absorb short bursts of blocking I/O without occupying
///   every core.
/// * When `SURREAL_KVS_THREADPOOL_SIZE` is set to an explicit value that does not equal the core
///   count (oversubscription or undersubscription), the pool drops pinning and uses that exact
///   worker count.
pub(super) fn initialise() {
	// Create the threadpool and ignore errors
	#[cfg(not(target_family = "wasm"))]
	{
		// Resolve the configured pool size (env-overridable; default
		// computed from `num_cpus::get()` with a 16-thread floor).
		let threads = *crate::cnf::KVS_THREADPOOL_SIZE;
		// Cache the host's logical core count once so the pinning
		// decision is consistent with the size resolution above.
		let cores = num_cpus::get();
		// Create the threadpool builder
		let builder = affinitypool::Builder::new().thread_name("surrealdb-threadpool");
		// Pin one worker per core only when the configured size exactly
		// matches the core count on a ≥16-core host. Any explicit
		// over/under-subscription drops pinning, since pinning a count
		// other than `num_cpus` is either impossible (too many) or
		// leaves cores unused (too few).
		let builder = if threads == cores && cores >= 16 {
			builder.thread_per_core(true)
		} else {
			builder.worker_threads(threads)
		};
		// Create the threadpool and ignore errors
		let _ = builder.build().build_global();
	}
}
