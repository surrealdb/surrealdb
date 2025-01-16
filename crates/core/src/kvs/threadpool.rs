/// Create a new blocking threadpool
#[cfg(not(target_family = "wasm"))]
pub(super) fn initialise() {
	let _ = affinitypool::Builder::new()
		.thread_name("surrealdb-threadpool")
		.thread_per_core(true)
		.build()
		.build_global();
}

/// Create a new blocking threadpool
#[cfg(target_family = "wasm")]
pub(super) fn initialise() {
	// Do nothing in WebAssembly
}
