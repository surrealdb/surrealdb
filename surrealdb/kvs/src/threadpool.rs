//! The shared KVS blocking threadpool used by embedded storage backends to
//! run synchronous storage work off the async runtime.

/// Number of worker threads in the shared KVS blocking threadpool
/// (`surrealdb-threadpool`) used by the `kv-mem`, `kv-rocksdb`, and
/// `kv-surrealkv` storage backends to run synchronous storage work off the
/// tokio runtime.
///
/// Default: `num_cpus::get()` on hosts with at least 16 logical cores
/// (matching the legacy `thread_per_core` behaviour with one pinned
/// worker per core), `16` on smaller hosts. Override with
/// `SURREAL_KVS_THREADPOOL_SIZE=<N>` (minimum `4`) to oversubscribe (more
/// concurrent blocking-IO slots, useful when many workers stall on disk
/// reads or fsyncs) or undersubscribe (cap blocking concurrency below
/// core count).
///
/// Explicit overrides drop the per-core CPU pinning that the default
/// applies on >=16-core hosts — pinning only makes sense when the
/// worker count exactly matches the core count.
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
/// Size and pinning behaviour are driven by [`KVS_THREADPOOL_SIZE`]:
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
pub fn initialise() {
	// Create the threadpool and ignore errors
	#[cfg(not(target_family = "wasm"))]
	{
		// Resolve the configured pool size (env-overridable; default
		// computed from `num_cpus::get()` with a 16-thread floor).
		let threads = *KVS_THREADPOOL_SIZE;
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
