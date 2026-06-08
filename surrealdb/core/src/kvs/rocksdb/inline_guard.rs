use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_family = "wasm"))]
use tokio::sync::Semaphore;
#[cfg(all(test, not(target_family = "wasm")))]
use tokio::sync::{SemaphorePermit, TryAcquireError};

/// Per-datastore starvation guard for the RocksDB engine. Bounds how
/// many tokio workers may run a blocking storage call inline at any
/// moment, dispatches overflow to the affinity pool, and tracks the
/// grant/divert counters surfaced via the datastore metric registry.
///
/// The cap is sized at construction as `worker_threads - reserve`
/// (saturating at 0). The worker count comes from
/// `RocksDbConfig::runtime_worker_threads`, populated either by the
/// server via `Datastore::builder().with_runtime_worker_threads(...)`
/// (from `cnf::RUNTIME_WORKER_THREADS`) or, for embedded callers, by
/// the field's `max(4, num_cpus::get())` default — itself matching the
/// server's tokio-runtime sizing default. An explicit
/// `SURREAL_RUNTIME_WORKER_THREADS` env var also wins via the shared
/// `runtime_worker_threads` `ConfigMap` key. The reserve comes from
/// `RocksDbConfig::runtime_reserve`.
pub(super) struct InlineGuard {
	#[cfg(not(target_family = "wasm"))]
	permits: Semaphore,
	/// Cached permit cap, set once at construction. Used by the `Drop`
	/// observability hook below; the live cap is otherwise read off the
	/// `Semaphore`, which doesn't expose its configured max.
	#[cfg(not(target_family = "wasm"))]
	max_permits: usize,
	granted: AtomicU64,
	diverted: AtomicU64,
}

impl InlineGuard {
	/// Construct a guard sized for the given tokio worker count and
	/// reserve. The inline permit budget is `worker_threads - reserve`,
	/// saturating at 0.
	pub(super) fn new(worker_threads: usize, reserve: usize) -> Self {
		#[cfg(not(target_family = "wasm"))]
		let cap = worker_threads.saturating_sub(reserve);
		// Silence unused warnings on wasm builds without the inline
		// permit infrastructure.
		#[cfg(target_family = "wasm")]
		let _ = (worker_threads, reserve);
		Self {
			#[cfg(not(target_family = "wasm"))]
			permits: Semaphore::new(cap),
			#[cfg(not(target_family = "wasm"))]
			max_permits: cap,
			granted: AtomicU64::new(0),
			diverted: AtomicU64::new(0),
		}
	}

	/// Try to acquire an inline-blocking permit. The returned permit is
	/// released on drop, so callers must hold it only for the duration
	/// of the synchronous storage call.
	///
	/// `Ok(permit)` — the runtime has slack, the caller may run the
	/// blocking call on its tokio worker.
	///
	/// `Err(_)` — the cap is hit; the caller should dispatch to the
	/// affinity pool instead so the runtime keeps `RUNTIME_RESERVE`
	/// workers free for async work.
	///
	/// Currently exists only to let unit tests drain the semaphore
	/// deterministically. Production paths go through
	/// `try_inline_or_offload` which folds the probe and divert into a
	/// single call.
	#[cfg(all(test, not(target_family = "wasm")))]
	fn try_inline_permit(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
		self.permits.try_acquire()
	}

	/// Run a synchronous storage op under the inline-blocking permit
	/// guard.
	///
	/// Probes `try_inline_permit()`. When the runtime has slack the op
	/// runs on the calling tokio worker (cache-hit fast path — no
	/// thread hop). When the cap is hit the op is dispatched to
	/// `affinitypool::spawn_local` so the configured reserve of tokio
	/// workers
	/// stay free for async work.
	///
	/// The granted / diverted counters are bumped in both branches so
	/// operators can observe how often the cap engages.
	///
	/// The closure is allowed to borrow non-`'static` data — the
	/// returned future's lifetime is bound by the closure's. Use this
	/// directly for ops that own all their state or have a `&mut`
	/// borrow of caller state (e.g. cursor batch advance). For ops that
	/// need to acquire the transaction inner Mutex, see
	/// `Transaction::run_blocking` which folds in the lock acquire.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn try_inline_or_offload<'a, F, R>(&self, op: F) -> R
	where
		F: FnOnce() -> R + Send + 'a,
		R: Send + 'a,
	{
		match self.permits.try_acquire() {
			Ok(_permit) => {
				self.granted.fetch_add(1, Ordering::Relaxed);
				op()
			}
			Err(_) => {
				self.diverted.fetch_add(1, Ordering::Relaxed);
				affinitypool::spawn_local(op).await
			}
		}
	}

	/// Cumulative number of storage calls that ran inline on a tokio
	/// worker (permit granted). Per-datastore counter, surfaced via the
	/// rocksdb metric registry.
	pub(super) fn granted(&self) -> u64 {
		self.granted.load(Ordering::Relaxed)
	}

	/// Cumulative number of storage calls that were dispatched to the
	/// affinity pool because the inline-blocking cap was hit. A rising
	/// delta here means the runtime would have starved without the
	/// guard.
	pub(super) fn diverted(&self) -> u64 {
		self.diverted.load(Ordering::Relaxed)
	}
}

#[cfg(not(target_family = "wasm"))]
impl Drop for InlineGuard {
	/// Emit the final inline-blocking grant/divert counters when the
	/// guard is dropped (typically when the datastore shuts down).
	///
	/// The same numbers are available throughout the datastore's life
	/// via `Datastore::collect_u64_metric("rocksdb.inline_blocking_*")`,
	/// but that API is not exposed through the public `Surreal<Any>`
	/// surface. The shutdown log lets embedded callers (benchmarks,
	/// integration tests) observe the cap engagement rate without
	/// plumbing the metric registry through every layer.
	///
	/// Only fires when at least one storage call was dispatched so
	/// short-lived datastores (test fixtures, schema introspection)
	/// stay quiet.
	fn drop(&mut self) {
		let granted = self.granted.load(Ordering::Relaxed);
		let diverted = self.diverted.load(Ordering::Relaxed);
		let total = granted.saturating_add(diverted);
		if total == 0 {
			return;
		}
		let divert_pct = (diverted as f64 * 100.0) / (total as f64);
		info!(
			target: "surrealdb::core::kvs::rocksdb::inline_guard",
			granted,
			diverted,
			divert_pct = format!("{divert_pct:.1}"),
			cap = self.max_permits,
			"inline-blocking summary at datastore shutdown"
		);
	}
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
	use super::*;

	#[test]
	fn new_saturates_at_zero() {
		// cap = workers - reserve, saturating at 0.
		assert_eq!(InlineGuard::new(0, 2).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(1, 2).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(2, 2).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(3, 2).permits.available_permits(), 1);
		assert_eq!(InlineGuard::new(16, 2).permits.available_permits(), 14);
		// Reserve = 0 means the cap matches the worker count exactly.
		assert_eq!(InlineGuard::new(12, 0).permits.available_permits(), 12);
		// Reserve > workers still saturates at 0.
		assert_eq!(InlineGuard::new(4, 8).permits.available_permits(), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn grants_when_permits_available() {
		let g = InlineGuard::new(4, 2); // 4 - 2 = 2 permits
		let out = g.try_inline_or_offload(|| 7_u32).await;
		assert_eq!(out, 7);
		assert_eq!(g.granted(), 1);
		assert_eq!(g.diverted(), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn diverts_when_permits_exhausted() {
		let g = InlineGuard::new(4, 2); // 4 - 2 = 2 permits
		let _h1 = g.try_inline_permit().expect("permit 1");
		let _h2 = g.try_inline_permit().expect("permit 2");
		let out = g.try_inline_or_offload(|| 42_u32).await;
		assert_eq!(out, 42);
		assert_eq!(g.granted(), 0);
		assert_eq!(g.diverted(), 1);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn always_diverts_when_runtime_below_reserve() {
		let g = InlineGuard::new(1, 2); // saturates to 0 permits
		let out = g.try_inline_or_offload(|| "ok").await;
		assert_eq!(out, "ok");
		assert_eq!(g.granted(), 0);
		assert_eq!(g.diverted(), 1);
	}
}
