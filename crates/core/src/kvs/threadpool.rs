#![cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]

/// Create a new blocking threadpool
pub(super) fn initialise() {
	// Get a sensible number of worker threads
	let size = std::cmp::max(8, num_cpus::get());
	// Create the threadpool and ignore errors
	let _ = affinitypool::Builder::new()
		.thread_name("surrealdb-threadpool")
		.thread_per_core(false)
		.worker_threads(size)
		.build()
		.build_global();
}
