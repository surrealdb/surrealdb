#![cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]

/// Create a new blocking threadpool
pub(super) fn initialise() {
	// Create the threadpool and ignore errors
	let _ = affinitypool::Builder::new()
		.thread_name("surrealdb-threadpool")
		.thread_per_core(true)
		.build()
		.build_global();
}
