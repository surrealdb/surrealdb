//! The shared KVS blocking threadpool used by embedded storage backends to
//! run synchronous storage work off the async runtime.

/// Number of worker threads in the shared KVS blocking threadpool
/// (`surrealdb-threadpool`) used by the `kv-mem`, `kv-rocksdb`, and
/// `kv-surrealkv` storage backends to run synchronous storage work off the
/// tokio runtime.
///
/// Default: `num_cpus::get()` on hosts with at least 16 logical cores (one
/// worker per core), `16` on smaller hosts. Override with
/// `SURREAL_KVS_THREADPOOL_SIZE=<N>` (minimum `4`) to oversubscribe (more
/// concurrent blocking-IO slots, useful when many workers stall on disk
/// reads or fsyncs) or undersubscribe (cap blocking concurrency below
/// core count).
///
/// Workers are never pinned to cores; placement is left to the OS
/// scheduler.
///
/// **Minimum: 4.** Some kvs operations always run on this pool — read-only
/// `count` with sharded fan-out, `compact`, writable scans — and below ~4
/// workers their throughput collapses (sharded `COUNT(*)` becomes serial,
/// `compact` blocks all other always-pool work). Values below 4, non-numeric
/// values, and an empty string are reported via `tracing::warn!` and the
/// computed default is used instead.
#[cfg(not(target_family = "wasm"))]
pub static KVS_THREADPOOL_SIZE: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
	let default = || {
		let cores = num_cpus::get();
		if cores >= 16 {
			cores
		} else {
			16
		}
	};
	const MINIMUM_OVERRIDE: usize = 4;
	match std::env::var("SURREAL_KVS_THREADPOOL_SIZE") {
		Err(_) => default(),
		Ok(s) if s.is_empty() => default(),
		Ok(s) => match s.parse::<usize>() {
			Ok(n) if n >= MINIMUM_OVERRIDE => n,
			Ok(n) => {
				tracing::warn!(
					target: "surrealdb::kvs::threadpool",
					"SURREAL_KVS_THREADPOOL_SIZE={n} is below the minimum of {MINIMUM_OVERRIDE}; using default",
				);
				default()
			}
			Err(_) => {
				tracing::warn!(
					target: "surrealdb::kvs::threadpool",
					"SURREAL_KVS_THREADPOOL_SIZE={s:?} is not a valid integer; using default",
				);
				default()
			}
		},
	}
});

/// Create the shared KVS blocking threadpool.
///
/// The worker count is [`KVS_THREADPOOL_SIZE`]: one worker per logical core
/// on ≥16-core hosts, a floor of 16 unpinned workers on smaller ones (enough
/// slack to absorb short bursts of blocking I/O without occupying every
/// core), or the exact `SURREAL_KVS_THREADPOOL_SIZE` value when set. Worker
/// placement is left to the OS scheduler in every case.
pub fn initialise() {
	// Create the threadpool and ignore errors
	#[cfg(not(target_family = "wasm"))]
	{
		// Resolve the configured pool size (env-overridable; default
		// computed from `num_cpus::get()` with a 16-thread floor).
		let threads = *KVS_THREADPOOL_SIZE;
		// Create the threadpool and ignore errors
		let _ = affinitypool::Builder::new()
			.thread_name("surrealdb-threadpool")
			.worker_threads(threads)
			.build()
			.build_global();
	}
}
