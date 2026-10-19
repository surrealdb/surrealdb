#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_family = "wasm"))]
use std::sync::{Arc, Weak};

#[cfg(not(target_family = "wasm"))]
use parking_lot::Mutex;
#[cfg(not(target_family = "wasm"))]
use surrealdb_kvs::err::Error;
#[cfg(not(target_family = "wasm"))]
use tokio::runtime::{Handle, Id as RuntimeId, RuntimeFlavor, RuntimeMetrics};
#[cfg(not(target_family = "wasm"))]
use tokio::sync::Semaphore;
#[cfg(all(test, not(target_family = "wasm")))]
use tokio::sync::{SemaphorePermit, TryAcquireError};
use tracing::info;

#[cfg(not(target_family = "wasm"))]
use crate::in_flight::InFlight;

/// Per-datastore starvation guard for the RocksDB engine. Bounds how
/// many tokio workers may run a blocking storage call at any moment,
/// dispatches overflow off the runtime, and tracks the grant/divert
/// counters surfaced via the datastore metric registry.
///
/// Bounded calls go through [`Self::try_inline_or_offload`] and overflow
/// to the affinity pool. Calls that RocksDB can stall for an unbounded
/// time go through [`Self::run_stallable`] and overflow to tokio's
/// blocking pool, where a set of slots bounds how many of them run at once:
/// the guard's own, or one per runtime shared by every guard that shares
/// them (see [`Self::sharing_blocking_slots`]).
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
	/// Cached permit cap, set once at construction. The `Semaphore`
	/// doesn't expose its configured max, so this is what the `Drop`
	/// observability hook below reports.
	#[cfg(not(target_family = "wasm"))]
	max_permits: usize,
	/// Where [`Self::run_stallable`] takes the slots that bound its calls on
	/// tokio's blocking pool. A running call owns its slot until the call
	/// returns, including after its caller has dropped the future.
	#[cfg(not(target_family = "wasm"))]
	blocking_slots: BlockingSlots,
	/// This guard's stallable calls in flight (see
	/// [`Self::stallable_calls_in_flight`]). Never closed: calls are
	/// admitted to the datastore, not to the guard.
	#[cfg(not(target_family = "wasm"))]
	stallable_calls: Arc<InFlight>,
	/// Where this guard counts its stallable calls running inline:
	/// [`RUNNING_INLINE`], shared with every other guard, outside tests.
	#[cfg(not(target_family = "wasm"))]
	running_inline: &'static AtomicUsize,
	granted: AtomicU64,
	diverted: AtomicU64,
}

impl InlineGuard {
	/// Construct a guard sized for the given tokio worker count and
	/// reserve. The inline permit budget is `worker_threads - reserve`,
	/// saturating at 0. `blocking_slots` bounds the stallable calls that
	/// run on tokio's blocking pool at once, and is raised to 1 if 0.
	pub(super) fn new(worker_threads: usize, reserve: usize, blocking_slots: usize) -> Self {
		#[cfg(not(target_family = "wasm"))]
		let cap = worker_threads.saturating_sub(reserve);
		// Silence unused warnings on wasm builds without the inline
		// permit infrastructure.
		#[cfg(target_family = "wasm")]
		let _ = (worker_threads, reserve, blocking_slots);
		Self {
			#[cfg(not(target_family = "wasm"))]
			permits: Semaphore::new(cap),
			#[cfg(not(target_family = "wasm"))]
			max_permits: cap,
			#[cfg(not(target_family = "wasm"))]
			blocking_slots: BlockingSlots::Own(Arc::new(Semaphore::new(blocking_slots.max(1)))),
			#[cfg(not(target_family = "wasm"))]
			stallable_calls: Arc::default(),
			#[cfg(not(target_family = "wasm"))]
			running_inline: &RUNNING_INLINE,
			granted: AtomicU64::new(0),
			diverted: AtomicU64::new(0),
		}
	}

	/// Construct a guard as [`Self::new`] does, except that each of its
	/// stallable calls takes its blocking-pool slot from the set of the
	/// runtime that runs the call, shared by every guard built this way. A
	/// call occupies the blocking pool of the runtime it runs on, so the
	/// bound on the stalled calls occupying that pool holds across every
	/// datastore whose calls run there, while calls on different runtimes,
	/// whose pools are separate, never wait for each other's slots.
	///
	/// The guard holds the set of the runtime current when it is built,
	/// which the first guard built on that runtime sizes to its
	/// `blocking_slots`. A call on a runtime that has no set gives it one of
	/// the calling guard's `blocking_slots`, which lasts while calls wait for
	/// or run in its slots.
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn sharing_blocking_slots(
		worker_threads: usize,
		reserve: usize,
		blocking_slots: usize,
	) -> Self {
		let mut guard = Self::new(worker_threads, reserve, blocking_slots);
		let home = Handle::try_current()
			.ok()
			.map(|runtime| (runtime.id(), runtime_blocking_slots(runtime.id(), blocking_slots)));
		guard.blocking_slots = BlockingSlots::PerRuntime {
			home,
			count: blocking_slots,
		};
		guard
	}

	#[cfg(target_family = "wasm")]
	pub(super) fn sharing_blocking_slots(
		worker_threads: usize,
		reserve: usize,
		blocking_slots: usize,
	) -> Self {
		Self::new(worker_threads, reserve, blocking_slots)
	}

	/// The set of blocking-pool slots this guard holds: its own, or that of
	/// the runtime it was built on.
	#[cfg(all(test, not(target_family = "wasm")))]
	fn held_blocking_slots(&self) -> Option<&Arc<Semaphore>> {
		match &self.blocking_slots {
			BlockingSlots::Own(slots) => Some(slots),
			BlockingSlots::PerRuntime {
				home,
				..
			} => home.as_ref().map(|(_, slots)| slots),
		}
	}

	/// Whether this guard and `other` hold the same set of blocking-pool
	/// slots.
	#[cfg(all(test, not(target_family = "wasm")))]
	pub(super) fn shares_blocking_slots_with(&self, other: &Self) -> bool {
		match (self.held_blocking_slots(), other.held_blocking_slots()) {
			(Some(slots), Some(others)) => Arc::ptr_eq(slots, others),
			_ => false,
		}
	}

	/// How many slots of the set this guard holds are free.
	#[cfg(all(test, not(target_family = "wasm")))]
	pub(super) fn blocking_slots_available(&self) -> usize {
		self.held_blocking_slots().expect("the guard holds no set of slots").available_permits()
	}

	/// Count this guard's stallable calls in `count` rather than in
	/// [`RUNNING_INLINE`], so that a test sees only the calls it made.
	#[cfg(all(test, not(target_family = "wasm")))]
	fn counting_in(mut self, count: &'static AtomicUsize) -> Self {
		self.running_inline = count;
		self
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
	/// Only calls whose duration is bounded by their own work belong here
	/// — a point read, a scan batch, a write buffered in the transaction.
	/// An inline call holds the worker, and can hold the runtime's I/O and
	/// timer driver with it, until it returns (see [`Self::run_stallable`]).
	/// A call that RocksDB can stall for an unbounded time, such as a
	/// commit, goes through `run_stallable` instead.
	///
	/// The closure is allowed to borrow non-`'static` data — the
	/// returned future's lifetime is bound by the closure's. Use this
	/// directly for ops that own all their state or have a `&mut`
	/// borrow of caller state (e.g. cursor batch advance). For ops that
	/// need to acquire the transaction inner Mutex, see
	/// `Transaction::run_blocking` which folds in the lock acquire.
	///
	/// **Callers must not leak the returned future.** On the diverted
	/// branch the op runs on a pool worker, and only the future's
	/// destructor — which blocks until that worker has stopped — keeps
	/// the closure's borrows from dangling. Dropping the future normally
	/// (including on cancellation) upholds this; `mem::forget`,
	/// `Box::leak`, `ManuallyDrop`, or embedding it in another future
	/// that is itself leaked does not. Every call site in this crate
	/// awaits the future immediately.
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
				// SAFETY: `op` may borrow non-`'static` data, so the pool future
				// must not be leaked. It is awaited inline here, which makes it
				// part of the future this `async fn` returns — the no-leak
				// obligation therefore lands on our callers, and is documented
				// on this function.
				unsafe { affinitypool::spawn_local(op) }.await
			}
		}
	}

	/// Run a synchronous storage call that RocksDB can stall for an
	/// unbounded time, such as a commit, without leaving the runtime's I/O
	/// and timer driver unpolled while it is blocked.
	///
	/// A tokio worker blocked inside a task can take the driver down with
	/// it. A worker that the driver wakes with a single runnable task
	/// notifies no peer, and peers parked on their condvars sleep until a
	/// task is scheduled for them, so while that task blocks nothing polls
	/// the driver: timers stop firing, sockets stop being accepted and
	/// read, and signal handlers stop running. RocksDB's write admission
	/// control parks a commit for as long as the stall condition lasts, so
	/// a commit must not block a worker that way.
	///
	/// The call runs:
	///
	/// - inline on the calling thread when an inline-blocking permit is free on a multi-thread
	///   runtime and a worker is left to poll the driver (see [`driver_poller`]). That is a parked
	///   worker whenever there is one, which the call first wakes (see [`wake_a_parked_worker`])
	///   and which takes the driver when it parks again. With every worker awake, it is one not
	///   blocked in a stallable call, which polls the driver once it returns to its scheduler.
	/// - on tokio's blocking pool when the permits are exhausted, when no worker is known to be
	///   parked and every one may be blocked in a stallable call, and on every call from a
	///   current-thread runtime, whose only thread is the one that polls its driver. Each call
	///   there takes a slot of its guard's set, or of the set of the runtime running it when its
	///   guard shares them (see [`Self::sharing_blocking_slots`]); later calls wait for a slot
	///   without holding a thread, so stalled calls occupy no more of the blocking pool than the
	///   slots allow (see [`stallable_blocking_slots`]).
	/// - in place when polled outside a tokio runtime, where there is no driver to leave unpolled.
	///
	/// Stallable calls running inline are counted across every guard in the
	/// process, since datastores can share a runtime (see [`RUNNING_INLINE`]).
	/// Keeping the driver polled never waits on a blocking-pool thread, and
	/// no branch uses `block_in_place`, which panics inside a `LocalSet`.
	///
	/// The inline branch counts as granted, and the blocking pool branch as
	/// diverted. A call polled outside a runtime counts as neither.
	///
	/// The call is in flight from when the future is first polled until the
	/// call returns, on every branch and while it waits for a slot, so a
	/// datastore's shutdown waits for it (see
	/// [`Self::stallable_calls_returned`]). Dropping the future before the
	/// call starts drops `op` unrun, and the call is no longer in flight.
	/// Once the call is running on the blocking pool, dropping the future
	/// detaches it: the call runs to completion, in flight until it returns,
	/// and its result is discarded. A call still queued when the runtime
	/// shuts down is dropped unrun, and reported as [`Error::Shutdown`]. A
	/// panic in `op` resumes in the caller.
	///
	/// A task in a worker's LIFO slot cannot be stolen. When no worker is
	/// parked, the inline call leaves the calling worker's slot as it is, so
	/// a task there waits for the call to return while the rest of the
	/// runtime keeps running. Polled off the runtime's workers — inside
	/// `block_on` or a `LocalSet` — the inline call blocks only the calling
	/// thread and the tasks it runs.
	///
	/// The runtime is the one whose handle is entered
	/// ([`Handle::try_current`]): tokio has no stable way to tell which
	/// scheduler is polling the calling thread. A call polled by another
	/// executor while a multi-thread runtime's handle is entered, such as a
	/// current-thread runtime, is dispatched as though that runtime were
	/// polling it, so it can run inline and block that executor.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn run_stallable<F, R>(&self, op: F) -> Result<R, Error>
	where
		F: FnOnce() -> R + Send + 'static,
		R: Send + 'static,
	{
		// Held across every branch, and moved into the call once it runs on
		// the blocking pool, so the call is in flight until it returns.
		let in_flight = self.stallable_calls.enter();
		let Ok(runtime) = Handle::try_current() else {
			return Ok(op());
		};
		if runtime.runtime_flavor() == RuntimeFlavor::MultiThread
			&& let Ok(_permit) = self.permits.try_acquire()
		{
			let (_running, blocked) = RunningInline::start(self.running_inline);
			if let Some(poller) = driver_poller(&runtime, blocked) {
				self.granted.fetch_add(1, Ordering::Relaxed);
				if poller == Poller::Parked {
					wake_a_parked_worker(&runtime);
				}
				return Ok(op());
			}
		}
		self.diverted.fetch_add(1, Ordering::Relaxed);
		// Only `Semaphore::close` fails an acquire, and nothing closes it.
		let slot = self
			.blocking_slots
			.for_runtime(&runtime)
			.acquire_owned()
			.await
			.map_err(|_| Error::Shutdown)?;
		let call = runtime.spawn_blocking(move || {
			let _in_flight = in_flight;
			let _slot = slot;
			op()
		});
		match call.await {
			Ok(res) => Ok(res),
			Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
			Err(_) => Err(Error::Shutdown),
		}
	}

	/// Cumulative number of storage calls that ran inline on a tokio
	/// worker under an inline-blocking permit. Per-datastore counter,
	/// surfaced via the rocksdb metric registry.
	pub(super) fn granted(&self) -> u64 {
		self.granted.load(Ordering::Relaxed)
	}

	/// Cumulative number of storage calls dispatched off the tokio
	/// workers because the inline-blocking cap was hit — stallable calls
	/// to tokio's blocking pool, bounded calls to the affinity pool — plus
	/// every stallable call polled on a current-thread runtime, and every
	/// one that would have left no worker to poll the runtime's driver. A
	/// rising delta here means the runtime would have starved without the
	/// guard.
	pub(super) fn diverted(&self) -> u64 {
		self.diverted.load(Ordering::Relaxed)
	}

	/// How many of this guard's stallable calls are in flight: running
	/// inline or in place, waiting for a blocking-pool slot, or running on
	/// the blocking pool, including calls there whose callers dropped their
	/// futures.
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn stallable_calls_in_flight(&self) -> usize {
		self.stallable_calls.count()
	}

	/// Wait until none of this guard's stallable calls is in flight (see
	/// [`Self::stallable_calls_in_flight`]). A call that starts while this
	/// waits is waited for too.
	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn stallable_calls_returned(&self) {
		self.stallable_calls.drained().await;
	}
}

/// How many stallable calls may run on tokio's blocking pool at once: at
/// most `threadpool_size`, and at most half of the runtime's
/// `max_blocking_threads`, rounded down, so calls stalled there leave the
/// rest of the pool to the runtime's other blocking work, such as sorts
/// and model execution. At least one, so that a runtime with a single
/// blocking thread can still run them.
pub(super) fn stallable_blocking_slots(
	threadpool_size: usize,
	max_blocking_threads: usize,
) -> usize {
	threadpool_size.min(max_blocking_threads / 2).max(1)
}

/// Where a guard's stallable calls take their blocking-pool slots.
#[cfg(not(target_family = "wasm"))]
enum BlockingSlots {
	/// A set of the guard's own, whichever runtime runs its calls.
	Own(Arc<Semaphore>),
	/// The set of the runtime that runs each call (see
	/// [`InlineGuard::sharing_blocking_slots`]): `home` for the runtime the
	/// guard was built on, if any, and the registry's for any other, where a
	/// runtime without a set gets one of `count` slots.
	PerRuntime {
		home: Option<(RuntimeId, Arc<Semaphore>)>,
		count: usize,
	},
}

#[cfg(not(target_family = "wasm"))]
impl BlockingSlots {
	/// The set that a call running on `runtime` takes its slot from.
	fn for_runtime(&self, runtime: &Handle) -> Arc<Semaphore> {
		match self {
			Self::Own(slots) => Arc::clone(slots),
			Self::PerRuntime {
				home: Some((home, slots)),
				..
			} if *home == runtime.id() => Arc::clone(slots),
			Self::PerRuntime {
				count,
				..
			} => runtime_blocking_slots(runtime.id(), *count),
		}
	}
}

/// The blocking-pool slots of each runtime that guards built by
/// [`InlineGuard::sharing_blocking_slots`] were built on or ran calls on.
/// A set is held weakly here, and strongly by the guards built on its
/// runtime and by the calls waiting for or running in its slots, so it
/// lasts until the last of them is gone; the runtime's next set is sized
/// afresh.
#[cfg(not(target_family = "wasm"))]
static RUNTIME_BLOCKING_SLOTS: Mutex<Vec<(RuntimeId, Weak<Semaphore>)>> = Mutex::new(Vec::new());

/// The blocking-pool slots of `runtime`, giving it a set of `count` slots,
/// at least one, if it has none.
#[cfg(not(target_family = "wasm"))]
fn runtime_blocking_slots(runtime: RuntimeId, count: usize) -> Arc<Semaphore> {
	let mut sets = RUNTIME_BLOCKING_SLOTS.lock();
	// Any live set for the runtime: its holders can drop it without taking
	// the lock, so an entry can be dead before it is pruned.
	let live = sets.iter().filter(|(id, _)| *id == runtime).find_map(|(_, set)| set.upgrade());
	if let Some(set) = live {
		return set;
	}
	sets.retain(|(_, set)| set.strong_count() > 0);
	let set = Arc::new(Semaphore::new(count.max(1)));
	sets.push((runtime, Arc::downgrade(&set)));
	set
}

/// How many sets of blocking-pool slots are registered for `runtime`, live
/// or not yet pruned.
#[cfg(all(test, not(target_family = "wasm")))]
fn registered_blocking_slots(runtime: RuntimeId) -> usize {
	RUNTIME_BLOCKING_SLOTS.lock().iter().filter(|(id, _)| *id == runtime).count()
}

/// Stallable calls running inline, counted across every [`InlineGuard`] in
/// the process.
///
/// Datastores can share a runtime, and a worker blocked in one datastore's
/// call is awake to the runtime's metrics without being free to poll its
/// driver, so no guard can judge the runtime from its own calls alone.
/// Calls on other runtimes are counted too, which errs toward the blocking
/// pool.
#[cfg(not(target_family = "wasm"))]
static RUNNING_INLINE: AtomicUsize = AtomicUsize::new(0);

/// Counts a stallable call as running inline until dropped.
#[cfg(not(target_family = "wasm"))]
struct RunningInline(&'static AtomicUsize);

#[cfg(not(target_family = "wasm"))]
impl RunningInline {
	/// Count the calling thread's call in `count`, and return how many calls
	/// are counted there with it, this one included.
	fn start(count: &'static AtomicUsize) -> (Self, usize) {
		// Every call counted before this one is in the returned count, so of
		// calls counted at once, the last sees all the others. Acquire makes
		// visible the park state each of their workers published before it
		// was counted, and release does the same for this one.
		let blocked = count.fetch_add(1, Ordering::AcqRel) + 1;
		(Self(count), blocked)
	}
}

#[cfg(not(target_family = "wasm"))]
impl Drop for RunningInline {
	fn drop(&mut self) {
		// A check that misses this counts one more blocked worker than there
		// is, which errs toward the blocking pool.
		self.0.fetch_sub(1, Ordering::Relaxed);
	}
}

/// A worker left to poll a runtime's driver while the calling thread blocks.
#[cfg(not(target_family = "wasm"))]
#[derive(Debug, PartialEq, Eq)]
enum Poller {
	/// A parked worker, once [`wake_a_parked_worker`] has woken it.
	Parked,
	/// With no worker parked, an awake one that is not blocked in a
	/// stallable call.
	Awake,
}

/// The worker that will poll `runtime`'s driver while the calling thread
/// blocks, given that `blocked` stallable calls, the caller's included, are
/// running inline. `None` when no worker is known to be parked and every
/// one may be blocked in one of those calls.
///
/// A parked worker is chosen whenever there is one. An awake worker may be
/// occupied, in a bounded storage call, in another datastore's stallable
/// call or in a long poll, and reaches the driver only when it returns to
/// its scheduler. With every worker awake there is none to wake, so a
/// worker not blocked in a stallable call is relied on to return to its
/// scheduler, where it polls the driver at its next maintenance tick or
/// takes it when it parks; until then the driver waits, as it does behind
/// any long poll on a busy runtime.
///
/// Each blocked call is counted as a blocked worker, whether its thread is
/// one of `runtime`'s workers or not, so the free workers are never
/// overcounted. A worker read as awake that parks before the caller's call
/// returns takes the driver if it is free, or finds it held by a worker
/// that is polling it.
#[cfg(not(target_family = "wasm"))]
fn driver_poller(runtime: &Handle, blocked: usize) -> Option<Poller> {
	let metrics = runtime.metrics();
	let workers = metrics.num_workers();
	pick_poller(workers, blocked, any_worker_parked(&metrics, workers))
}

/// [`driver_poller`]'s choice among `workers`, given the `blocked` calls and
/// whether any worker is `parked`, which is `None` where that is unknown.
///
/// Where park state is unknown, a worker not blocked in a stallable call
/// may be parked, so the call wakes one as though it were; if every such
/// worker is awake, the wake finds none to rouse, and they are relied on as
/// awake workers are. With every worker possibly blocked there may be none
/// to wake, so the call takes the blocking pool.
#[cfg(not(target_family = "wasm"))]
fn pick_poller(workers: usize, blocked: usize, parked: Option<bool>) -> Option<Poller> {
	match parked {
		Some(true) => Some(Poller::Parked),
		Some(false) => (workers > blocked).then_some(Poller::Awake),
		None => (workers > blocked).then_some(Poller::Parked),
	}
}

/// Whether any of `metrics`' `workers` is parked. A worker's park/unpark
/// count is odd while it is parked; it is published before the worker
/// parks and after it unparks.
#[cfg(all(not(target_family = "wasm"), target_has_atomic = "64"))]
fn any_worker_parked(metrics: &RuntimeMetrics, workers: usize) -> Option<bool> {
	Some((0..workers).any(|worker| !metrics.worker_park_unpark_count(worker).is_multiple_of(2)))
}

/// Unknown: tokio publishes the park/unpark count only on targets with
/// 64-bit atomics.
#[cfg(all(not(target_family = "wasm"), not(target_has_atomic = "64")))]
fn any_worker_parked(_metrics: &RuntimeMetrics, _workers: usize) -> Option<bool> {
	None
}

/// Leave `runtime` a worker that will poll its driver while the calling
/// thread blocks.
///
/// A task spawned from a worker takes that worker's LIFO slot and moves the
/// task the slot held onto the worker's run queue, and moving a task there
/// notifies a parked worker or leaves it to a searching one. Spawning twice
/// therefore always leaves a stealable task, whatever the slot held. The
/// worker that runs it then parks: on the driver if no worker holds it, and
/// otherwise behind the worker that is polling it. A task the slot held
/// becomes stealable with it. Spawned from any other thread, each task is
/// injected, which notifies a parked worker directly.
///
/// This relies on how tokio's multi-thread scheduler places and announces
/// local work; the liveness tests in this module fail if that stops
/// holding.
#[cfg(not(target_family = "wasm"))]
fn wake_a_parked_worker(runtime: &Handle) {
	runtime.spawn(async {});
	runtime.spawn(async {});
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
	use std::future::Future;
	use std::sync::atomic::AtomicBool;
	use std::sync::mpsc;
	use std::task::{Context, Poll, Waker};
	use std::time::Duration;

	use tokio::runtime::{Builder, Runtime};

	use super::*;

	/// How long a runtime is left to settle, with every worker parked,
	/// before the call under test starts.
	const SETTLE: Duration = Duration::from_millis(200);
	/// How long each liveness probe is given to complete.
	const OBSERVE: Duration = Duration::from_millis(500);
	/// Bound on waits that only time out when the test itself is broken.
	const WAIT: Duration = Duration::from_secs(10);

	/// What a runtime still did while one of its tasks was blocked in a
	/// dispatched storage call.
	#[derive(Debug, PartialEq, Eq)]
	struct Liveness {
		/// A connection made after the call blocked was accepted.
		accepted: bool,
		/// A timer that fell due after the call blocked fired.
		fired: bool,
		/// A task spawned by the blocked task just before the call ran; true
		/// when no task was spawned.
		spawned: bool,
	}

	impl Liveness {
		const ALIVE: Self = Self {
			accepted: true,
			fired: true,
			spawned: true,
		};
	}

	type Op = Box<dyn FnOnce() + Send>;

	fn multi_thread(workers: usize) -> Runtime {
		Builder::new_multi_thread()
			.worker_threads(workers)
			.enable_all()
			.build()
			.expect("failed to build a multi-thread runtime")
	}

	fn current_thread() -> Runtime {
		Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("failed to build a current-thread runtime")
	}

	/// Dispatch a call that blocks until released through `dispatch` on
	/// `runtime`, and report what else the runtime did meanwhile.
	///
	/// The dispatching task is woken by a timer, so the worker polling it
	/// was woken by the driver: the case in which a worker blocked in a
	/// task leaves the driver unpolled. With `lifo_task` it spawns a task
	/// just before dispatching, which lands in that worker's LIFO slot;
	/// without, the slot is empty when the call starts. The probes are
	/// judged from this thread with `recv_timeout`, since the runtime's
	/// timers are among what stops, and nothing is scheduled onto the
	/// runtime from outside once the call starts: that would wake a parked
	/// worker, which takes the driver back.
	fn while_blocked<D, Fut>(runtime: Runtime, lifo_task: bool, dispatch: D) -> Liveness
	where
		D: FnOnce(Op) -> Fut + Send + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		let (accepted_tx, accepted_rx) = mpsc::channel();
		let (fired_tx, fired_rx) = mpsc::channel();
		let (spawned_tx, spawned_rx) = mpsc::channel();
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		let (returned_tx, returned_rx) = mpsc::channel();
		let listener =
			std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind a loopback listener");
		listener.set_nonblocking(true).expect("failed to make the listener non-blocking");
		let addr = listener.local_addr().expect("the listener has no local address");
		runtime.spawn(async move {
			let listener = tokio::net::TcpListener::from_std(listener)
				.expect("failed to register the listener with the runtime");
			if listener.accept().await.is_ok() {
				let _ = accepted_tx.send(());
			}
		});
		runtime.spawn(async move {
			tokio::time::sleep(SETTLE * 2).await;
			let _ = fired_tx.send(());
		});
		runtime.spawn(async move {
			tokio::time::sleep(SETTLE).await;
			if lifo_task {
				tokio::spawn(async move {
					let _ = spawned_tx.send(());
				});
			} else {
				let _ = spawned_tx.send(());
			}
			dispatch(Box::new(move || {
				let _ = started_tx.send(());
				let _ = release_rx.recv();
			}))
			.await;
			let _ = returned_tx.send(());
		});
		// A current-thread runtime runs only inside `block_on`. A
		// multi-thread one runs on its workers, and this thread's `block_on`
		// parks on its own parker rather than on the driver.
		let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
		let host = std::thread::spawn(move || {
			runtime.block_on(async {
				let _ = stop_rx.await;
			});
			runtime.shutdown_timeout(WAIT);
		});
		started_rx.recv_timeout(WAIT).expect("the dispatched call never started");
		let _client =
			std::net::TcpStream::connect(addr).expect("failed to connect to the listener");
		let liveness = Liveness {
			accepted: accepted_rx.recv_timeout(OBSERVE).is_ok(),
			fired: fired_rx.recv_timeout(OBSERVE).is_ok(),
			spawned: spawned_rx.recv_timeout(OBSERVE).is_ok(),
		};
		release_tx.send(()).expect("the dispatched call dropped its release channel");
		returned_rx.recv_timeout(WAIT).expect("the dispatched call never returned");
		stop_tx.send(()).expect("the runtime host stopped early");
		host.join().expect("the runtime host panicked");
		liveness
	}

	/// What occupies a worker, keeping it from returning to its scheduler.
	#[derive(Clone, Copy, Debug)]
	enum Occupant {
		/// A bounded call running inline.
		BoundedCall,
		/// A stallable call under a guard of its own, as another datastore
		/// sharing the runtime would run one.
		OtherGuard,
	}

	/// A count of stallable calls private to one test.
	fn private_count() -> &'static AtomicUsize {
		Box::leak(Box::new(AtomicUsize::new(0)))
	}

	/// Occupy one of `runtime`'s `workers` with a call that blocks until the
	/// returned sender releases it, counting a stallable one in `count`.
	/// Every other worker has parked when this returns.
	fn occupy_a_worker(
		runtime: &Runtime,
		workers: usize,
		occupant: Occupant,
		count: &'static AtomicUsize,
	) -> mpsc::Sender<()> {
		std::thread::sleep(SETTLE);
		let other = Arc::new(InlineGuard::new(workers, workers - 1, 1).counting_in(count)); // one permit
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		let call = move || {
			let _ = started_tx.send(());
			let _ = release_rx.recv();
		};
		runtime.spawn({
			let other = Arc::clone(&other);
			async move {
				match occupant {
					Occupant::BoundedCall => other.try_inline_or_offload(call).await,
					Occupant::OtherGuard => {
						other.run_stallable(call).await.expect("the other guard's call was refused")
					}
				}
			}
		});
		started_rx.recv_timeout(WAIT).expect("the occupying call never started");
		assert_eq!(
			(other.granted(), other.diverted()),
			(1, 0),
			"the occupying call must block a worker"
		);
		std::thread::sleep(SETTLE);
		release_tx
	}

	/// Dispatch through `g`, and release `other` once the dispatched call is.
	fn dispatch_then_release(
		g: &Arc<InlineGuard>,
		other: mpsc::Sender<()>,
	) -> impl FnOnce(Op) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static {
		let g = Arc::clone(g);
		move |op: Op| {
			Box::pin(async move {
				g.run_stallable(move || {
					op();
					let _ = other.send(());
				})
				.await
				.expect("the dispatched call was refused")
			})
		}
	}

	#[test]
	fn new_saturates_at_zero() {
		// cap = workers - reserve, saturating at 0.
		assert_eq!(InlineGuard::new(0, 2, 1).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(1, 2, 1).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(2, 2, 1).permits.available_permits(), 0);
		assert_eq!(InlineGuard::new(3, 2, 1).permits.available_permits(), 1);
		assert_eq!(InlineGuard::new(16, 2, 1).permits.available_permits(), 14);
		// Reserve = 0 means the cap matches the worker count exactly.
		assert_eq!(InlineGuard::new(12, 0, 1).permits.available_permits(), 12);
		// Reserve > workers still saturates at 0.
		assert_eq!(InlineGuard::new(4, 8, 1).permits.available_permits(), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn grants_when_permits_available() {
		let g = InlineGuard::new(4, 2, 1); // 4 - 2 = 2 permits
		let out = g.try_inline_or_offload(|| 7_u32).await;
		assert_eq!(out, 7);
		assert_eq!(g.granted(), 1);
		assert_eq!(g.diverted(), 0);
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn diverts_when_permits_exhausted() {
		let g = InlineGuard::new(4, 2, 1); // 4 - 2 = 2 permits
		let _h1 = g.try_inline_permit().expect("permit 1");
		let _h2 = g.try_inline_permit().expect("permit 2");
		let out = g.try_inline_or_offload(|| 42_u32).await;
		assert_eq!(out, 42);
		assert_eq!(g.granted(), 0);
		assert_eq!(g.diverted(), 1);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn always_diverts_when_runtime_below_reserve() {
		let g = InlineGuard::new(1, 2, 1); // saturates to 0 permits
		let out = g.try_inline_or_offload(|| "ok").await;
		assert_eq!(out, "ok");
		assert_eq!(g.granted(), 0);
		assert_eq!(g.diverted(), 1);
	}

	/// A call blocked inline under `try_inline_or_offload` leaves the
	/// driver unpolled, which is why only bounded calls go there. That the
	/// harness observes this is what gives the `run_stallable` liveness
	/// tests their meaning.
	#[test]
	fn a_call_blocked_inline_leaves_the_driver_unpolled() {
		let stranded = (0..3).any(|_| {
			let g = Arc::new(InlineGuard::new(4, 2, 1));
			let liveness = while_blocked(multi_thread(4), true, move |op| async move {
				g.try_inline_or_offload(op).await;
			});
			!liveness.accepted && !liveness.fired
		});
		assert!(
			stranded,
			"a call blocked inline no longer strands the driver, so the liveness tests pass without exercising it"
		);
	}

	/// With every other worker parked, the call wakes one before blocking,
	/// whether or not the calling worker's LIFO slot holds a task.
	#[test]
	fn stallable_call_with_every_peer_parked_wakes_one() {
		for lifo_task in [true, false] {
			let g = Arc::new(InlineGuard::new(4, 2, 1));
			let dispatch = {
				let g = Arc::clone(&g);
				move |op: Op| async move { g.run_stallable(op).await.unwrap() }
			};
			assert_eq!(
				while_blocked(multi_thread(4), lifo_task, dispatch),
				Liveness::ALIVE,
				"lifo_task = {lifo_task}"
			);
			assert_eq!((g.granted(), g.diverted()), (1, 0));
		}
	}

	/// A worker that is awake but occupied, in a bounded call or in a
	/// stallable call under another datastore's guard, reaches the driver
	/// only once it returns to its scheduler, so the call wakes a parked
	/// worker rather than rely on it.
	#[test]
	fn stallable_call_wakes_a_parked_worker_past_an_occupied_one() {
		for occupant in [Occupant::BoundedCall, Occupant::OtherGuard] {
			let count = private_count();
			let runtime = multi_thread(4);
			let other = occupy_a_worker(&runtime, 4, occupant, count);
			let g = Arc::new(InlineGuard::new(4, 2, 1).counting_in(count)); // permits free
			let dispatch = dispatch_then_release(&g, other);
			assert_eq!(
				while_blocked(runtime, true, dispatch),
				Liveness::ALIVE,
				"occupant = {occupant:?}"
			);
			assert_eq!((g.granted(), g.diverted()), (1, 0), "occupant = {occupant:?}");
		}
	}

	/// With every worker blocked in a stallable call, no parked worker is
	/// left to wake, so the call runs on the blocking pool though it holds
	/// a permit.
	#[test]
	fn stallable_call_with_every_worker_blocked_runs_on_the_blocking_pool() {
		let count = private_count();
		let runtime = multi_thread(2);
		let other = occupy_a_worker(&runtime, 2, Occupant::OtherGuard, count);
		let g = Arc::new(InlineGuard::new(2, 1, 1).counting_in(count)); // one permit, free
		let dispatch = dispatch_then_release(&g, other);
		assert_eq!(while_blocked(runtime, true, dispatch), Liveness::ALIVE);
		assert_eq!((g.granted(), g.diverted()), (0, 1));
	}

	/// Datastores can share a runtime, so every guard counts its stallable
	/// calls in the same place.
	#[test]
	fn guards_share_one_count_of_stallable_calls() {
		let (a, b) = (InlineGuard::new(4, 2, 1), InlineGuard::new(8, 0, 4));
		assert!(std::ptr::eq(a.running_inline, b.running_inline));
	}

	#[test]
	fn a_stallable_call_counts_as_blocked_until_it_returns() {
		let count = private_count();
		let runtime = multi_thread(2);
		let g = InlineGuard::new(2, 0, 1).counting_in(count); // permits free
		let during = runtime.block_on(g.run_stallable(|| count.load(Ordering::Relaxed))).unwrap();
		assert_eq!((during, count.load(Ordering::Relaxed)), (1, 0));
		assert_eq!((g.granted(), g.diverted()), (1, 0));
		runtime.shutdown_timeout(WAIT);
	}

	/// A `LocalSet` forbids `block_in_place` even on a multi-thread runtime,
	/// so the inline branch must not depend on it.
	#[test]
	fn stallable_call_inside_a_local_set_runs_inline() {
		let runtime = multi_thread(4);
		let g = InlineGuard::new(4, 2, 1); // permits free
		// Every worker parks, so the call finds no awake peer.
		std::thread::sleep(SETTLE);
		let caller = std::thread::current().id();
		let local = tokio::task::LocalSet::new();
		let call = g.run_stallable(move || std::thread::current().id() == caller);
		assert!(runtime.block_on(local.run_until(call)).expect("the call was refused"));
		assert_eq!((g.granted(), g.diverted()), (1, 0));
		runtime.shutdown_timeout(WAIT);
	}

	#[test]
	fn stallable_call_past_the_cap_runs_on_the_blocking_pool() {
		let g = Arc::new(InlineGuard::new(4, 4, 1)); // no permits
		let dispatch = {
			let g = Arc::clone(&g);
			move |op: Op| async move { g.run_stallable(op).await.unwrap() }
		};
		assert_eq!(while_blocked(multi_thread(4), true, dispatch), Liveness::ALIVE);
		assert_eq!((g.granted(), g.diverted()), (0, 1));
	}

	#[test]
	fn stallable_call_on_a_current_thread_runtime_runs_on_the_blocking_pool() {
		let g = Arc::new(InlineGuard::new(4, 2, 1)); // permits free
		let dispatch = {
			let g = Arc::clone(&g);
			move |op: Op| async move { g.run_stallable(op).await.unwrap() }
		};
		assert_eq!(while_blocked(current_thread(), true, dispatch), Liveness::ALIVE);
		assert_eq!((g.granted(), g.diverted()), (0, 1));
	}

	// Reads the park state that tokio publishes only on targets with 64-bit
	// atomics.
	#[cfg(target_has_atomic = "64")]
	#[test]
	fn driver_poller_prefers_a_parked_worker_and_counts_blocked_calls() {
		/// Stops the busy tasks when dropped, so that a failed assertion
		/// cannot leave the runtime waiting on them during unwinding.
		struct Stop(Arc<AtomicBool>);
		impl Drop for Stop {
			fn drop(&mut self) {
				self.0.store(true, Ordering::Relaxed);
			}
		}
		// A busy task never yields: a worker that returns to its scheduler
		// between polls wakes a parked peer each time it stops searching.
		fn spawn_busy(runtime: &Runtime, stop: &Stop) {
			let stop = Arc::clone(&stop.0);
			let (running_tx, running_rx) = mpsc::channel();
			runtime.spawn(async move {
				let _ = running_tx.send(());
				while !stop.load(Ordering::Relaxed) {
					std::hint::spin_loop();
				}
			});
			running_rx.recv_timeout(WAIT).expect("the busy task never ran");
			// Let a peer woken to search alongside the busy task park again.
			std::thread::sleep(SETTLE);
		}

		let runtime = multi_thread(2);
		let h = runtime.handle();
		std::thread::sleep(SETTLE);
		assert_eq!(
			driver_poller(h, 0),
			Some(Poller::Parked),
			"every worker of an idle runtime is parked"
		);
		let stop = Stop(Arc::new(AtomicBool::new(false)));
		spawn_busy(&runtime, &stop);
		assert_eq!(
			driver_poller(h, 0),
			Some(Poller::Parked),
			"a parked worker is chosen over an awake one"
		);
		spawn_busy(&runtime, &stop);
		assert_eq!(
			driver_poller(h, 1),
			Some(Poller::Awake),
			"with every worker awake, one not blocked in a stallable call is left"
		);
		assert_eq!(driver_poller(h, 2), None, "every worker may be blocked, and none is parked");
		drop(stop);
		runtime.shutdown_timeout(WAIT);
	}

	/// Where park state is unknown, a worker not blocked in a stallable call
	/// is woken as though it were parked, and with every worker possibly
	/// blocked the call takes the blocking pool.
	#[test]
	fn pick_poller_without_park_state_wakes_while_a_worker_may_be_free() {
		assert_eq!(pick_poller(4, 1, None), Some(Poller::Parked));
		assert_eq!(pick_poller(2, 2, None), None, "every worker may be blocked");
		assert_eq!(pick_poller(2, 3, None), None, "calls off the workers count as blocked too");
	}

	#[test]
	fn blocking_slots_bound_the_stallable_calls_running_off_the_workers() {
		let runtime = multi_thread(2);
		let g = Arc::new(InlineGuard::new(2, 2, 1)); // no permits, one slot
		let (first_started_tx, first_started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		let first = runtime.spawn({
			let g = Arc::clone(&g);
			async move {
				g.run_stallable(move || {
					first_started_tx.send(()).unwrap();
					release_rx.recv().unwrap();
				})
				.await
			}
		});
		first_started_rx.recv_timeout(WAIT).expect("the first call never started");
		// A running call keeps its slot after its caller is dropped.
		first.abort();
		assert!(runtime.block_on(first).unwrap_err().is_cancelled());
		let (second_ran_tx, second_ran_rx) = mpsc::channel();
		let second = runtime.spawn({
			let g = Arc::clone(&g);
			async move { g.run_stallable(move || second_ran_tx.send(()).unwrap()).await }
		});
		assert!(
			second_ran_rx.recv_timeout(OBSERVE).is_err(),
			"a second call ran while the first held the only slot"
		);
		release_tx.send(()).unwrap();
		second_ran_rx.recv_timeout(WAIT).expect("the second call never ran once the slot was free");
		runtime.block_on(second).unwrap().unwrap();
		assert_eq!((g.granted(), g.diverted()), (0, 2));
		runtime.shutdown_timeout(WAIT);
	}

	/// Run a stallable call through `g` on `runtime` that blocks until
	/// released, and return once it has started, with its caller's task and
	/// the call's release.
	fn hold_a_call(
		g: &Arc<InlineGuard>,
		runtime: &Runtime,
	) -> (tokio::task::JoinHandle<Result<(), Error>>, mpsc::Sender<()>) {
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		let caller = runtime.spawn({
			let g = Arc::clone(g);
			async move {
				g.run_stallable(move || {
					let _ = started_tx.send(());
					let _ = release_rx.recv();
				})
				.await
			}
		});
		started_rx.recv_timeout(WAIT).expect("the call never started");
		(caller, release_tx)
	}

	/// A stallable call on the blocking pool is in flight until it returns,
	/// whether or not its caller still awaits it, so a datastore's shutdown
	/// can wait for it. A call waiting for a slot is in flight too, until it
	/// is dropped unrun.
	#[test]
	fn a_detached_stallable_call_is_in_flight_until_it_returns() {
		let runtime = multi_thread(2);
		let g = Arc::new(InlineGuard::new(2, 2, 1)); // no permits, one slot
		let (detached, release) = hold_a_call(&g, &runtime);
		detached.abort();
		assert!(runtime.block_on(detached).unwrap_err().is_cancelled());
		let (queued_ran_tx, queued_ran_rx) = mpsc::channel();
		let queued = runtime.spawn({
			let g = Arc::clone(&g);
			async move { g.run_stallable(move || queued_ran_tx.send(()).unwrap()).await }
		});
		std::thread::sleep(SETTLE);
		assert_eq!(g.stallable_calls_in_flight(), 2, "a call waiting for a slot is not in flight");
		queued.abort();
		assert!(runtime.block_on(queued).unwrap_err().is_cancelled());
		assert_eq!(g.stallable_calls_in_flight(), 1, "a call dropped while queued is in flight");
		let wait = |limit| {
			runtime
				.block_on(async { tokio::time::timeout(limit, g.stallable_calls_returned()).await })
		};
		assert!(wait(SETTLE).is_err(), "the wait returned while the detached call ran");
		release.send(()).unwrap();
		wait(WAIT).expect("the wait never returned once the call did");
		assert_eq!(g.stallable_calls_in_flight(), 0);
		assert!(queued_ran_rx.try_recv().is_err(), "a call dropped while queued ran");
		runtime.shutdown_timeout(WAIT);
	}

	/// A call waiting for a blocking-pool slot is in flight, so waiting for
	/// the guard's calls covers one that takes its slot only once the last
	/// running call returns, as a commit queued when shutdown begins would.
	#[test]
	fn the_wait_covers_a_call_that_takes_its_slot_after_the_last_returns() {
		let runtime = multi_thread(2);
		let g = Arc::new(InlineGuard::new(2, 2, 1)); // no permits, one slot
		let (running, release_running) = hold_a_call(&g, &runtime);
		let (queued_started_tx, queued_started_rx) = mpsc::channel();
		let (release_queued, queued_release_rx) = mpsc::channel::<()>();
		let queued = runtime.spawn({
			let g = Arc::clone(&g);
			async move {
				g.run_stallable(move || {
					queued_started_tx.send(()).unwrap();
					queued_release_rx.recv().unwrap();
				})
				.await
			}
		});
		std::thread::sleep(SETTLE);
		let wait = runtime.spawn({
			let g = Arc::clone(&g);
			async move { g.stallable_calls_returned().await }
		});
		release_running.send(()).unwrap();
		runtime.block_on(running).unwrap().unwrap();
		queued_started_rx.recv_timeout(WAIT).expect("the queued call never took the slot");
		std::thread::sleep(SETTLE);
		assert!(!wait.is_finished(), "the wait returned before the queued call did");
		release_queued.send(()).unwrap();
		runtime.block_on(queued).unwrap().unwrap();
		runtime
			.block_on(async { tokio::time::timeout(WAIT, wait).await })
			.expect("the wait never returned once every call had")
			.unwrap();
		runtime.shutdown_timeout(WAIT);
	}

	/// A stallable call is in flight while it runs on every branch, so a
	/// datastore's shutdown waits for it wherever it runs.
	#[test]
	fn a_stallable_call_is_in_flight_on_every_branch() {
		let g = Arc::new(InlineGuard::new(2, 0, 1).counting_in(private_count())); // permits free
		let in_flight = || {
			let g = Arc::clone(&g);
			move || g.stallable_calls_in_flight()
		};
		let call = std::pin::pin!(g.run_stallable(in_flight()));
		let mut cx = Context::from_waker(Waker::noop());
		assert!(matches!(call.poll(&mut cx), Poll::Ready(Ok(1))), "in place");
		let runtime = multi_thread(2);
		assert_eq!(runtime.block_on(g.run_stallable(in_flight())).unwrap(), 1, "inline");
		assert_eq!((g.granted(), g.diverted()), (1, 0));
		let current = current_thread();
		assert_eq!(current.block_on(g.run_stallable(in_flight())).unwrap(), 1, "blocking pool");
		assert_eq!((g.granted(), g.diverted()), (1, 1));
		assert_eq!(g.stallable_calls_in_flight(), 0);
		runtime.shutdown_timeout(WAIT);
	}

	/// A call occupies the blocking pool of the runtime that runs it, so it
	/// takes that runtime's slots, whichever runtime its guard was built on
	/// and whether or not it was built on one.
	#[test]
	fn a_stallable_call_takes_the_slots_of_the_runtime_running_it() {
		let (one, other) = (multi_thread(2), multi_thread(2));
		let build = |runtime: &Runtime, slots| {
			let _entered = runtime.enter();
			Arc::new(InlineGuard::sharing_blocking_slots(2, 2, slots)) // no permits
		};
		let a = build(&one, 2);
		let c = build(&other, 3);
		let d = Arc::new(InlineGuard::sharing_blocking_slots(2, 2, 5));
		let (c_call, release_c) = hold_a_call(&c, &one);
		let (d_call, release_d) = hold_a_call(&d, &one);
		assert_eq!(a.blocking_slots_available(), 0, "calls on a runtime skipped its slots");
		assert_eq!(c.blocking_slots_available(), 3, "a call took its guard's runtime's slots");
		// A runtime with no set gets one sized by the guard whose call needs
		// it: `a`'s two slots, one of them taken, rather than the seven a
		// set created by the lookup would have.
		let fresh = multi_thread(2);
		let (fresh_call, release_fresh) = hold_a_call(&a, &fresh);
		assert_eq!(runtime_blocking_slots(fresh.handle().id(), 7).available_permits(), 1);
		for release in [release_c, release_d, release_fresh] {
			release.send(()).unwrap();
		}
		one.block_on(c_call).unwrap().unwrap();
		one.block_on(d_call).unwrap().unwrap();
		fresh.block_on(fresh_call).unwrap().unwrap();
		assert_eq!(a.blocking_slots_available(), 2);
		for runtime in [one, other, fresh] {
			runtime.shutdown_timeout(WAIT);
		}
	}

	#[test]
	fn stallable_blocking_slots_leave_the_rest_of_the_blocking_pool() {
		assert_eq!(stallable_blocking_slots(16, 512), 16);
		assert_eq!(stallable_blocking_slots(16, 16), 8, "at most half of the blocking pool");
		assert_eq!(stallable_blocking_slots(600, 512), 256);
		assert_eq!(stallable_blocking_slots(16, 1), 1, "at least one");
	}

	/// Datastores on one runtime share its blocking pool, so the guards built
	/// to share on a runtime hold one set of slots, sized by the first of
	/// them. Another runtime's pool is separate, and so is its set, and a
	/// guard built outside a runtime holds none.
	#[test]
	fn guards_built_on_one_runtime_share_its_blocking_slots() {
		let (one, other) = (current_thread(), current_thread());
		let (a, b) = {
			let _entered = one.enter();
			(
				InlineGuard::sharing_blocking_slots(4, 2, 3),
				InlineGuard::sharing_blocking_slots(8, 0, 5),
			)
		};
		assert!(a.shares_blocking_slots_with(&b));
		assert_eq!(b.blocking_slots_available(), 3, "the first guard on a runtime sizes its set");
		let c = {
			let _entered = other.enter();
			InlineGuard::sharing_blocking_slots(4, 2, 5)
		};
		assert!(!a.shares_blocking_slots_with(&c), "another runtime shared the set");
		assert_eq!(c.blocking_slots_available(), 5);
		let d = InlineGuard::sharing_blocking_slots(4, 2, 3);
		assert!(d.held_blocking_slots().is_none(), "a guard built outside a runtime holds a set");
		assert!(!a.shares_blocking_slots_with(&InlineGuard::new(4, 2, 3)));
	}

	/// A runtime's set of slots lasts while a guard or a call running in it
	/// holds it: a call that outlives its datastore still occupies a thread
	/// of the pool. Once nothing holds it, the next guard built on the
	/// runtime sizes a new set.
	#[test]
	fn a_runtimes_blocking_slots_last_while_a_guard_or_call_holds_them() {
		let runtime = multi_thread(2);
		let build = |slots| {
			let _entered = runtime.enter();
			Arc::new(InlineGuard::sharing_blocking_slots(2, 2, slots)) // no permits
		};
		let first = build(1);
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		let detached = runtime.spawn({
			let g = Arc::clone(&first);
			async move {
				g.run_stallable(move || {
					started_tx.send(()).unwrap();
					release_rx.recv().unwrap();
				})
				.await
			}
		});
		started_rx.recv_timeout(WAIT).expect("the call never started");
		detached.abort();
		assert!(runtime.block_on(detached).unwrap_err().is_cancelled());
		drop(first);
		let second = build(5);
		assert_eq!(second.blocking_slots_available(), 0, "the running call's set was replaced");
		release_tx.send(()).unwrap();
		// The slot is released after the call returns, so poll for it.
		let poll = Duration::from_millis(10);
		let released = (0..WAIT.as_millis() / poll.as_millis()).any(|_| {
			std::thread::sleep(poll);
			second.blocking_slots_available() == 1
		});
		assert!(released, "the call never released its slot to the set it was sized in");
		drop(second);
		let third = build(5);
		assert_eq!(third.blocking_slots_available(), 5, "a set outlived its guards and calls");
		// Only this test builds guards on its runtime, so the count is exact.
		assert_eq!(
			registered_blocking_slots(runtime.handle().id()),
			1,
			"a dead set stayed registered"
		);
		runtime.shutdown_timeout(WAIT);
	}

	#[test]
	fn a_panic_on_the_blocking_pool_resumes_in_the_caller() {
		let runtime = multi_thread(2);
		let g = Arc::new(InlineGuard::new(2, 2, 1)); // no permits
		let caller = runtime.spawn({
			let g = Arc::clone(&g);
			async move { g.run_stallable::<_, ()>(|| panic!("stallable call panicked")).await }
		});
		let err = runtime.block_on(caller).unwrap_err();
		assert!(err.is_panic());
		assert_eq!(err.into_panic().downcast_ref::<&str>(), Some(&"stallable call panicked"));
		assert_eq!(g.stallable_calls_in_flight(), 0, "a call that panicked is still in flight");
		runtime.shutdown_timeout(WAIT);
	}

	#[test]
	fn stallable_call_outside_a_runtime_runs_in_place() {
		let g = InlineGuard::new(4, 2, 1);
		let caller = std::thread::current().id();
		let call = std::pin::pin!(g.run_stallable(move || std::thread::current().id() == caller));
		let mut cx = Context::from_waker(Waker::noop());
		assert!(matches!(call.poll(&mut cx), Poll::Ready(Ok(true))));
		assert_eq!((g.granted(), g.diverted()), (0, 0));
	}
}
