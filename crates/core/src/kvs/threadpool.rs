#![cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv"))]

/// Create a new blocking threadpool
#[cfg(not(target_family = "wasm"))]
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

/// Create a new blocking threadpool
#[cfg(target_family = "wasm")]
pub(super) fn initialise() {
	// Do nothing in WebAssembly
}
