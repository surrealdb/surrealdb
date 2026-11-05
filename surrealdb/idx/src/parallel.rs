//! Parallel jobs for index construction, run in places from the node's pool.
//!
//! An engine that builds part of a batch on several tasks hands the work to
//! [`run_jobs`] as a list of independent jobs, with the [`Ticket`] its
//! environment holds. The engine owns the decomposition: which parts of its
//! batch are independent, and how their results are combined. This module owns
//! only the execution, and the guarantees the engines rely on:
//!
//! * **Results come back in job order**, whichever task ran each job, so a result the engine
//!   derives from them depends neither on the number of tasks nor on the places granted.
//! * **A failed job fails the whole run.** The first error is returned and the remaining tasks are
//!   aborted; a panicking job resumes its panic on the caller. Nothing is dropped silently, so an
//!   engine never commits a batch one of whose jobs did not finish.
//! * **Dropping the run aborts its tasks**, so a cancelled caller does not leave work behind
//!   holding the state it shared with them.
//! * **Every worker runs in a place** of the node's pool, taken through the ticket before the
//!   worker is spawned and held across its jobs. The pool's OLTP reserve, the ticket's cap and the
//!   pool's fairness bound a run's workers together with the queries' and every other run's. No
//!   semaphore of this crate bounds them.
//! * **Workers yield at the quantum.** After every [`Ticket::quantum`] jobs a worker checks, before
//!   it claims another job, whether another registration waits for a place. If one does, the worker
//!   gives its place way ([`Ticket::give_way`]) and takes no further job.
//! * **The pool arbitrates.** At each of its quantum boundaries the calling task asks for the
//!   places the run is owed, and the pool's fairness decides who a freed place goes to, so two
//!   waiting runs never leave a freed place idle by deferring to each other. Once a place of the
//!   registration was given way, the ticket asks for none while another registration waits
//!   ([`Ticket::acquire`]), so no run on it, this one or a later one, takes back what was given
//!   way.
//! * **A run always completes.** The calling task runs jobs too, without a place, and drains
//!   whatever the workers leave, so a run that is never granted a place, or whose workers all
//!   yield, still runs every job. Once every job is claimed the run clears its waiting mark, so a
//!   place freed while its last jobs finish is not held back for it.
//!
//! Without a tokio runtime to spawn onto — on wasm, or in an embedding that
//! runs none — and whenever one task is asked for, the jobs run inline on the
//! calling task, in order. The results are the same either way.
//!
//! Jobs that read committed state read it through one [`ReadView`] for the
//! whole run rather than through the caller's transaction: on engines that run
//! one transaction's operations one at a time, reads through a single
//! transaction would queue behind each other however many tasks issue them,
//! while reads through one view do not.

use std::future::Future;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::{Key, Snapshot};
#[cfg(not(target_family = "wasm"))]
use tokio::task::{JoinError, JoinSet};

use crate::places::Ticket;
#[cfg(not(target_family = "wasm"))]
use crate::places::{Asking, Place};

/// Runs `job(i)` for every `i` in `0..jobs` on up to `tasks` tasks, the
/// calling one included, and returns the results in index order.
///
/// Every task beyond the calling one is a worker running in a place taken
/// through `places`, so a run is as wide as the places it is granted and never
/// wider than `tasks`. Jobs are handed out one at a time, so an expensive job
/// does not hold back a task's share of the cheap ones. See the module
/// documentation for the quantum, and for the error, panic and cancellation
/// guarantees.
pub(crate) async fn run_jobs<T, F, Fut>(
	places: &Ticket,
	tasks: usize,
	jobs: usize,
	job: F,
) -> Result<Vec<T>>
where
	T: Send + 'static,
	F: Fn(usize) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = Result<T>> + Send + 'static,
{
	#[cfg(not(target_family = "wasm"))]
	if tasks > 1 && jobs > 1 && tokio::runtime::Handle::try_current().is_ok() {
		return run_placed(places, tasks.min(jobs), jobs, job).await;
	}
	let _ = (places, tasks);
	let mut results = Vec::with_capacity(jobs);
	for i in 0..jobs {
		results.push(job(i).await?);
		yield_now!();
	}
	Ok(results)
}

/// What a worker returns: the jobs it ran, by index.
#[cfg(not(target_family = "wasm"))]
type Done<T> = Result<Vec<(usize, T)>>;

#[cfg(not(target_family = "wasm"))]
async fn run_placed<T, F, Fut>(places: &Ticket, tasks: usize, jobs: usize, job: F) -> Result<Vec<T>>
where
	T: Send + 'static,
	F: Fn(usize) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = Result<T>> + Send + 'static,
{
	let job = Arc::new(job);
	let next = Arc::new(AtomicUsize::new(0));
	let quantum = places.quantum();
	let workers = tasks - 1;
	// The run is one user of the ticket. A refused ask counts it as waiting,
	// so a place freed during the run is owed to the registration; it stops
	// waiting once every job is claimed, and however the run ends.
	let asking = Arc::new(places.asking());
	// Dropped on every early return below, and dropping a `JoinSet` aborts
	// the tasks still in it.
	let mut set: JoinSet<Done<T>> = JoinSet::new();
	let spawn = Spawn {
		asking: &asking,
		workers,
		job: &job,
		next: &next,
		jobs,
	};
	spawn.widen(&mut set);
	let mut results: Vec<Option<T>> = (0..jobs).map(|_| None).collect();
	let mut ran = 0;
	loop {
		while let Some(joined) = set.try_join_next() {
			collect(joined, &mut results)?;
		}
		let i = next.fetch_add(1, Ordering::Relaxed);
		if i >= jobs {
			// Every job is claimed, so the run will not ask again.
			asking.stop_waiting();
			break;
		}
		results[i] = Some(job(i).await?);
		yield_now!();
		ran += 1;
		// At each of its own quantum boundaries the calling task asks for the
		// places the run is owed; the pool decides whether it is owed them.
		if ran % quantum == 0 && set.len() < workers {
			spawn.widen(&mut set);
		}
	}
	while let Some(joined) = set.join_next().await {
		collect(joined, &mut results)?;
	}
	results
		.into_iter()
		.map(|result| {
			result.ok_or_else(|| anyhow::anyhow!("an index build job produced no result"))
		})
		.collect()
}

/// What the calling task of a run spawns its workers from.
#[cfg(not(target_family = "wasm"))]
struct Spawn<'a, F> {
	asking: &'a Arc<Asking>,
	workers: usize,
	job: &'a Arc<F>,
	next: &'a Arc<AtomicUsize>,
	jobs: usize,
}

#[cfg(not(target_family = "wasm"))]
impl<F> Spawn<'_, F> {
	/// Spawns one worker per place the ticket is granted, until `set` holds
	/// `workers` tasks, no job is left to claim, or an ask is refused. Each
	/// place is taken before its worker is spawned, so no task waits for one.
	///
	/// The pool decides whether the run is owed a place, and the ticket asks
	/// for none while a place it gave way is still wanted
	/// ([`Ticket::acquire`]). Once every job is claimed the run keeps no
	/// waiting mark, since it will not ask again.
	fn widen<T, Fut>(&self, set: &mut JoinSet<Done<T>>)
	where
		T: Send + 'static,
		F: Fn(usize) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<T>> + Send + 'static,
	{
		while set.len() < self.workers && self.next.load(Ordering::Relaxed) < self.jobs {
			let Some(place) = self.asking.acquire() else {
				// A run whose jobs were all claimed meanwhile does not wait.
				if self.next.load(Ordering::Relaxed) >= self.jobs {
					self.asking.stop_waiting();
				}
				return;
			};
			set.spawn(worker(
				place,
				Arc::clone(self.asking),
				Arc::clone(self.job),
				Arc::clone(self.next),
				self.jobs,
			));
		}
		if self.next.load(Ordering::Relaxed) >= self.jobs {
			self.asking.stop_waiting();
		}
	}
}

/// A worker: runs jobs in `place` until none is left, or until another
/// registration waits at one of its quantum boundaries. The check comes before
/// the next job is claimed, so every claimed job runs. A worker gives its
/// place way through the ticket ([`Ticket::give_way`]), and stops the run
/// waiting once it finds every job claimed.
#[cfg(not(target_family = "wasm"))]
async fn worker<T, F, Fut>(
	place: Place,
	asking: Arc<Asking>,
	job: Arc<F>,
	next: Arc<AtomicUsize>,
	jobs: usize,
) -> Done<T>
where
	F: Fn(usize) -> Fut,
	Fut: Future<Output = Result<T>>,
{
	let places = asking.ticket();
	let quantum = places.quantum();
	let mut done = Vec::new();
	loop {
		if !done.is_empty() && done.len() % quantum == 0 && places.others_wait() {
			places.give_way(place);
			return Ok(done);
		}
		let i = next.fetch_add(1, Ordering::Relaxed);
		if i >= jobs {
			// Every job is claimed, so the run will not ask again: the place
			// goes back with no claim of the run's on it.
			asking.stop_waiting();
			return Ok(done);
		}
		done.push((i, job(i).await?));
		yield_now!();
	}
}

/// Files a finished worker's results, or fails the run as the worker failed.
#[cfg(not(target_family = "wasm"))]
fn collect<T>(joined: Result<Done<T>, JoinError>, results: &mut [Option<T>]) -> Result<()> {
	let done = match joined {
		Ok(done) => done?,
		Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
		Err(err) => return Err(anyhow::anyhow!("an index build task was cancelled: {err}")),
	};
	for (i, result) in done {
		results[i] = Some(result);
	}
	Ok(())
}

/// One read view of committed state, shared by every job of a run.
///
/// The view is taken from a read-only transaction opened for the run's jobs,
/// once, alongside the transaction the run belongs to. Every job therefore
/// reads the datastore as committed when that transaction opened, and none of
/// the origin transaction's uncommitted writes. Jobs read through the view's
/// batched point reads and cursors, which run side by side however many tasks
/// issue them.
///
/// The owner calls [`Self::close`] once every job has finished, which cancels
/// the transaction the view was taken from.
pub(crate) struct ReadView {
	/// The read-only transaction the view was taken from.
	tx: Arc<Transaction>,
	/// The transaction's view.
	view: Arc<dyn Snapshot>,
}

impl ReadView {
	/// Opens a read-only transaction alongside `origin` and takes its view.
	///
	/// Returns `None` when the datastore gives no view to a read-only
	/// transaction, which is known before anything is opened, so no transaction
	/// is opened then. It also returns `None` when the transaction it opened
	/// refuses a view, after cancelling that transaction. The caller then reads
	/// through `origin`.
	pub(crate) async fn open(origin: &Transaction) -> Result<Option<Self>> {
		if !origin.view_capabilities().read_only {
			return Ok(None);
		}
		let tx = origin.open_read_snapshot().await?;
		let Some(view) = tx.snapshot() else {
			tx.cancel().await?;
			return Ok(None);
		};
		Ok(Some(Self {
			tx: Arc::new(tx),
			view,
		}))
	}

	/// Reads `keys` through the view in one batch: one value per key, in input
	/// order, `None` for a key the view does not hold.
	///
	/// The read is recorded on the transaction the view was taken from, as
	/// that transaction's own batched read would be, so its completion event
	/// accounts for what the jobs read through the view.
	pub(crate) async fn get_many(&self, keys: &[Key<'static>]) -> Result<Vec<Option<Vec<u8>>>> {
		let fetched = self.view.get_many(keys, None).await?;
		let key_bytes = keys.iter().map(|key| key.len() as u64).sum();
		self.tx.record_view_get(fetched.records, key_bytes, fetched.value_bytes);
		Ok(fetched.values)
	}

	/// Cancels the transaction the view was taken from. Called once no job
	/// reads through the view any more.
	pub(crate) async fn close(&self) {
		// The transaction is read-only, so cancelling it discards nothing, and
		// what the jobs read through it is already in their results. A cancel
		// that fails leaves the transaction to be released when its last
		// reference drops, which is the same outcome; it has nothing for the
		// run to act on.
		let _ = self.tx.cancel().await;
	}

	/// The read-only transaction the view was taken from, for the assertions
	/// on what it recorded.
	#[cfg(all(test, feature = "kv-surrealkv"))]
	pub(crate) fn tx(&self) -> &Arc<Transaction> {
		&self.tx
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::future::Future;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

	use anyhow::{Result, bail};
	use parking_lot::Mutex;

	use super::run_jobs;
	use crate::places::Ticket;
	use crate::test_env::CountingPlaces;

	/// A ticket with more places than any run here asks for.
	fn roomy() -> Ticket {
		CountingPlaces::new(64, 8).ticket()
	}

	/// The task a job runs on: a worker's id, or `None` on the calling task,
	/// which a `#[tokio::test]` body runs outside any spawned task.
	fn task() -> Option<tokio::task::Id> {
		tokio::task::try_id()
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn results_come_back_in_job_order_whatever_the_task_count() -> Result<()> {
		let places = roomy();
		for tasks in [1, 2, 3, 8, 64] {
			let squares = run_jobs(&places, tasks, 100, |i| async move { Ok(i * i) }).await?;
			assert_eq!(squares, (0..100).map(|i| i * i).collect::<Vec<_>>(), "tasks = {tasks}");
		}
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn every_job_runs_exactly_once() -> Result<()> {
		let runs: Arc<Vec<AtomicUsize>> = Arc::new((0..257).map(|_| AtomicUsize::new(0)).collect());
		let counted = Arc::clone(&runs);
		run_jobs(&roomy(), 8, 257, move |i| {
			let counted = Arc::clone(&counted);
			async move {
				counted[i].fetch_add(1, Ordering::Relaxed);
				Ok(())
			}
		})
		.await?;
		assert!(runs.iter().all(|runs| runs.load(Ordering::Relaxed) == 1));
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_failing_job_fails_the_run() {
		let res = run_jobs(&roomy(), 4, 50, |i| async move {
			if i == 37 {
				bail!("job {i} failed");
			}
			Ok(i)
		})
		.await;
		assert_eq!(res.expect_err("the run must fail").to_string(), "job 37 failed");
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[should_panic(expected = "job 11 panicked")]
	async fn a_panicking_job_panics_the_caller() {
		let _ = run_jobs(&roomy(), 4, 20, |i| async move {
			if i == 11 {
				panic!("job {i} panicked");
			}
			Ok(i)
		})
		.await;
	}

	/// A ticket never granted a place runs every job on the calling task, in
	/// job order, and leaves no waiting mark once the run ends.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn without_a_place_every_job_runs_on_the_calling_task() -> Result<()> {
		let pool = CountingPlaces::new(0, 2);
		let ran_on: Arc<Mutex<Vec<Option<tokio::task::Id>>>> = Arc::new(Mutex::new(Vec::new()));
		let seen = Arc::clone(&ran_on);
		let doubled = run_jobs(&pool.ticket(), 8, 100, move |i| {
			let seen = Arc::clone(&seen);
			async move {
				seen.lock().push(task());
				Ok(2 * i)
			}
		})
		.await?;
		assert_eq!(doubled, (0..100).map(|i| 2 * i).collect::<Vec<_>>());
		let ran_on = ran_on.lock();
		assert_eq!(ran_on.len(), 100);
		assert!(ran_on.iter().all(Option::is_none), "every job ran on the calling task");
		assert_eq!(pool.taken(), 0);
		assert!(pool.refused() > 0, "the run asked for places");
		assert!(!pool.waiting(), "the run's waiting mark is cleared when it ends");
		Ok(())
	}

	/// A run spawns a worker only for a place it holds: it holds exactly the
	/// places the pool has, its workers are exactly those it took places for,
	/// and no more than one task beyond them runs a job at once.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_run_is_as_wide_as_its_places_and_the_calling_task() -> Result<()> {
		let pool = CountingPlaces::new(2, 4);
		let running = Arc::new(AtomicUsize::new(0));
		let peak = Arc::new(AtomicUsize::new(0));
		let workers: Arc<Mutex<BTreeMap<tokio::task::Id, usize>>> = Arc::default();
		let (r, p, w) = (Arc::clone(&running), Arc::clone(&peak), Arc::clone(&workers));
		run_jobs(&pool.ticket(), 8, 400, move |i| {
			let (running, peak, workers) = (Arc::clone(&r), Arc::clone(&p), Arc::clone(&w));
			async move {
				let now = running.fetch_add(1, Ordering::SeqCst) + 1;
				peak.fetch_max(now, Ordering::SeqCst);
				if let Some(id) = task() {
					*workers.lock().entry(id).or_default() += 1;
				}
				tokio::task::yield_now().await;
				running.fetch_sub(1, Ordering::SeqCst);
				Ok(i)
			}
		})
		.await?;
		assert_eq!(pool.peak(), 2, "the run held every place the pool has, and no more");
		assert_eq!(pool.taken(), 2, "a place is taken only for a worker to run in");
		assert!(workers.lock().len() <= 2, "no worker ran without a place");
		assert!(peak.load(Ordering::SeqCst) <= 3, "two workers and the calling task at most");
		assert_eq!(pool.held(), 0);
		Ok(())
	}

	/// With another registration waiting, every worker gives its place back at
	/// its first quantum boundary and takes no further job; the calling task
	/// runs the rest, and the results still come back in job order.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_worker_yields_its_place_at_the_quantum_when_another_waits() -> Result<()> {
		const QUANTUM: usize = 4;
		let pool = CountingPlaces::new(3, QUANTUM as u32);
		pool.set_others_waiting(true);
		let workers: Arc<Mutex<BTreeMap<tokio::task::Id, usize>>> = Arc::default();
		let gated = Arc::new(AtomicBool::new(false));
		let yields_seen = Arc::new(AtomicUsize::new(usize::MAX));
		let (w, g, y, pl) =
			(Arc::clone(&workers), Arc::clone(&gated), Arc::clone(&yields_seen), Arc::clone(&pool));
		let out = run_jobs(&pool.ticket(), 8, 100, move |i| {
			let (workers, gated, yields_seen, pool) =
				(Arc::clone(&w), Arc::clone(&g), Arc::clone(&y), Arc::clone(&pl));
			async move {
				match task() {
					Some(id) => *workers.lock().entry(id).or_default() += 1,
					// The calling task's first job holds it until every worker
					// has yielded, so the workers alone decide where they stop.
					// The timeout only turns a worker that never yields into a
					// failure rather than a hang.
					None if !gated.swap(true, Ordering::SeqCst) => {
						tokio::time::timeout(
							std::time::Duration::from_secs(60),
							pool.wait_for_yields(3),
						)
						.await
						.expect("every worker yields at its first quantum boundary");
						yields_seen.store(pool.yields(), Ordering::SeqCst);
					}
					None => {}
				}
				Ok(i)
			}
		})
		.await?;
		assert_eq!(out, (0..100).collect::<Vec<_>>());
		let workers = workers.lock();
		assert_eq!(workers.len(), 3, "one worker per place");
		assert!(
			workers.values().all(|jobs| *jobs == QUANTUM),
			"each worker stopped at its first quantum boundary: {workers:?}"
		);
		assert_eq!(yields_seen.load(Ordering::SeqCst), 3, "one yield per worker");
		assert_eq!(pool.taken(), 3, "no place was taken again while another waited");
		assert_eq!(pool.held(), 0, "every yielded place went back");
		Ok(())
	}

	/// The jobs of a run whose first `gated` jobs wait at `gate`, each first
	/// counting itself into `started`, and whose other jobs only yield.
	fn gated_jobs(
		gated: usize,
		started: tokio::sync::watch::Sender<usize>,
		gate: tokio::sync::watch::Receiver<bool>,
	) -> impl Fn(usize) -> std::pin::Pin<Box<dyn Future<Output = Result<usize>> + Send>>
	+ Send
	+ Sync
	+ 'static {
		let started = Arc::new(started);
		move |i| {
			let (started, mut gate) = (Arc::clone(&started), gate.clone());
			Box::pin(async move {
				if i < gated {
					started.send_modify(|n| *n += 1);
					gate.wait_for(|open| *open).await.expect("the test holds the sender");
				}
				tokio::task::yield_now().await;
				Ok(i)
			})
		}
	}

	/// Two runs refused while a third registration holds every place both
	/// stay waiting. When a place frees, the pool owes it to the earlier of
	/// them, and that run takes it at its next quantum boundary, though the
	/// other run waits too. Its worker later gives the place way to the
	/// waiting run, and the earlier run does not take it back.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn the_earlier_of_two_waiting_runs_takes_a_freed_place() -> Result<()> {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(2, 1);
		let (_, holder) = pool.register();
		let mut held: Vec<_> = (0..2).map(|_| holder.acquire().expect("free")).collect();
		let (earlier, earlier_places) = pool.register();
		let (later, later_places) = pool.register();
		let (started, mut seen) = tokio::sync::watch::channel(0);
		let (open, gate) = tokio::sync::watch::channel(false);
		// Each run's calling task waits in its first job, both runs refused.
		let jobs = gated_jobs(2, started, gate);
		let jobs = Arc::new(jobs);
		let run = |places: Ticket| {
			let jobs = Arc::clone(&jobs);
			tokio::spawn(async move { run_jobs(&places, 4, 200, move |i| jobs(i)).await })
		};
		let (first, second) = (run(earlier_places), run(later_places));
		seen.wait_for(|n| *n >= 2).await.expect("the runs hold the sender");
		assert!(pool.waiting(earlier) && pool.waiting(later), "both runs were refused");
		drop(held.pop());
		open.send_replace(true);
		let (first, second) = (first.await??, second.await??);
		assert_eq!(first, (0..200).collect::<Vec<_>>());
		assert_eq!(second, (0..200).collect::<Vec<_>>());
		let granted = pool.granted();
		assert_eq!(granted.get(2), Some(&earlier), "the freed place went to the earlier run");
		assert_eq!(granted.get(3), Some(&later), "then to the run it was given way to");
		drop(held);
		Ok(())
	}

	/// A run whose worker gave its place back to a waiting registration does
	/// not take the place back while that registration still waits, though
	/// the pool's order would grant it: the run registered first.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_run_does_not_take_back_a_place_it_yielded_to_a_waiter() -> Result<()> {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(1, 1);
		let (run_id, places) = pool.register();
		let (waiter_id, waiter) = pool.register();
		let (started, mut seen) = tokio::sync::watch::channel(0);
		let (open, gate) = tokio::sync::watch::channel(false);
		// The calling task and the worker each wait in their first job.
		let jobs = gated_jobs(2, started, gate);
		let run = tokio::spawn(async move { run_jobs(&places, 2, 200, jobs).await });
		seen.wait_for(|n| *n >= 2).await.expect("the run holds the sender");
		assert_eq!(pool.held(run_id), 1, "the run's worker holds the one place");
		assert!(waiter.acquire().is_none(), "the waiter is refused");
		assert!(pool.waiting(waiter_id));
		open.send_replace(true);
		assert_eq!(run.await??, (0..200).collect::<Vec<_>>());
		assert_eq!(pool.taken(run_id), 1, "the yielded place was not taken back");
		assert!(waiter.acquire().is_some(), "the place is free for the waiter");
		Ok(())
	}

	/// A run refused a place stays waiting while it may ask again. Once every
	/// job is claimed it never will, so a worker that finds none left clears
	/// the mark as it gives its place back, though a slow job still runs on
	/// the calling task: a later registration takes the freed place at once.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_run_with_every_job_claimed_stops_waiting() -> Result<()> {
		use crate::test_env::SharedPlaces;

		const JOBS: usize = 20;
		let pool = SharedPlaces::new(2, 100);
		let (_, holder) = pool.register();
		let _held = holder.acquire().expect("free");
		let (run_id, places) = pool.register();
		let (_, later) = pool.register();
		let (done, mut done_seen) = tokio::sync::watch::channel(0usize);
		let (release, released) = tokio::sync::watch::channel(false);
		let done = Arc::new(done);
		let blocked = Arc::new(AtomicBool::new(false));
		// The worker's jobs wait until the calling task is in its slow job, so
		// the calling task is sure to run one.
		let (caller_in, caller_seen) = tokio::sync::watch::channel(false);
		let caller_in = Arc::new(caller_in);
		// Once the worker has run every other job and given its place back,
		// the later registration asks for that place; then the slow job ends.
		let probe = {
			let pool = Arc::clone(&pool);
			tokio::spawn(async move {
				done_seen.wait_for(|n| *n == JOBS - 1).await.expect("the run holds the sender");
				tokio::time::timeout(std::time::Duration::from_secs(30), async {
					while pool.held(run_id) > 0 {
						tokio::task::yield_now().await;
					}
				})
				.await
				.expect("the worker gives its place back");
				let granted = later.acquire().is_some();
				release.send_replace(true);
				granted
			})
		};
		let out = run_jobs(&places, 4, JOBS, move |i| {
			let (done, blocked, mut released, caller_in, mut caller_seen) = (
				Arc::clone(&done),
				Arc::clone(&blocked),
				released.clone(),
				Arc::clone(&caller_in),
				caller_seen.clone(),
			);
			async move {
				// The calling task's first job is the slow one.
				if task().is_none() && !blocked.swap(true, Ordering::SeqCst) {
					caller_in.send_replace(true);
					released.wait_for(|r| *r).await.expect("the probe holds the sender");
				} else {
					caller_seen.wait_for(|i| *i).await.expect("the run holds the sender");
					done.send_modify(|n| *n += 1);
				}
				Ok(i)
			}
		})
		.await?;
		assert_eq!(out, (0..JOBS).collect::<Vec<_>>());
		assert_eq!(pool.taken(run_id), 1, "the run was granted one place and refused another");
		assert!(probe.await?, "the later registration took the place the run gave back");
		Ok(())
	}

	/// A place given way stays given way for the registration, not only for
	/// the run that gave it: a later run on the same ticket takes no place
	/// while the registration it gave way to still waits, though this ticket
	/// registered first and would win the pool's tie.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_later_run_on_the_ticket_does_not_take_back_a_place_given_way() -> Result<()> {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(1, 1);
		let (run_id, places) = pool.register();
		let (waiter_id, waiter) = pool.register();
		let (started, mut seen) = tokio::sync::watch::channel(0);
		let (open, gate) = tokio::sync::watch::channel(false);
		let jobs = gated_jobs(2, started, gate);
		let first = {
			let places = places.clone();
			tokio::spawn(async move { run_jobs(&places, 2, 200, jobs).await })
		};
		seen.wait_for(|n| *n >= 2).await.expect("the run holds the sender");
		assert!(waiter.acquire().is_none(), "the waiter is refused");
		open.send_replace(true);
		assert_eq!(first.await??, (0..200).collect::<Vec<_>>());
		assert_eq!(pool.taken(run_id), 1, "the first run's worker gave its place way");
		let second = run_jobs(&places, 4, 50, |i| async move { Ok(i) }).await?;
		assert_eq!(second, (0..50).collect::<Vec<_>>());
		assert_eq!(pool.taken(run_id), 1, "the second run did not take the place back");
		assert!(pool.waiting(waiter_id), "the waiter still waits");
		assert!(waiter.acquire().is_some(), "the place is free for the waiter");
		Ok(())
	}

	/// Two runs share one ticket, both refused a place. The run that ends
	/// first stops asking, but the other still waits, so the registration
	/// stays marked waiting, and an older registration's workers still give
	/// way to it, until the other run stops asking too.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_ticket_stays_waiting_while_any_of_its_runs_waits() -> Result<()> {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(1, 1);
		let (_, holder) = pool.register();
		let _held = holder.acquire().expect("free");
		let (ticket_id, places) = pool.register();
		let (started, mut seen) = tokio::sync::watch::channel(0);
		let (open, gate) = tokio::sync::watch::channel(false);
		// The first run's calling task waits in its first job, still asking.
		let jobs = gated_jobs(1, started, gate);
		let first = {
			let places = places.clone();
			tokio::spawn(async move { run_jobs(&places, 2, 2, jobs).await })
		};
		seen.wait_for(|n| *n >= 1).await.expect("the run holds the sender");
		assert!(pool.waiting(ticket_id), "the first run was refused");
		let second = run_jobs(&places, 2, 4, |i| async move { Ok(i) }).await?;
		assert_eq!(second, (0..4).collect::<Vec<_>>());
		assert!(pool.waiting(ticket_id), "the first run still waits after the second ended");
		assert!(holder.others_wait(), "so the holder's workers still give way to it");
		open.send_replace(true);
		assert_eq!(first.await??, (0..2).collect::<Vec<_>>());
		assert!(!pool.waiting(ticket_id), "no run on the ticket waits any more");
		Ok(())
	}

	/// A ticket stays marked waiting exactly while one of its users waits:
	/// another user's grant, stopping or dropping leaves the mark, so an older
	/// registration still sees it waiting; giving a place way ends every
	/// user's wait at once.
	#[test]
	fn a_ticket_is_marked_waiting_while_one_of_its_users_waits() {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(1, 1);
		let (_, older) = pool.register();
		let held = older.acquire().expect("free");
		let (ticket_id, places) = pool.register();
		let (waiting, other) = (places.asking(), places.asking());
		assert!(waiting.acquire().is_none(), "refused while the older one holds the place");
		assert!(pool.waiting(ticket_id));
		drop(held);
		let granted = other.acquire().expect("the freed place");
		assert!(pool.waiting(ticket_id), "a grant to one user keeps the other's wait");
		other.stop_waiting();
		assert!(pool.waiting(ticket_id), "so does the granted user stopping");
		let third = places.asking();
		drop(third);
		assert!(pool.waiting(ticket_id), "or another user ending");
		places.give_way(granted);
		assert!(!pool.waiting(ticket_id), "giving way ends every user's wait");
		waiting.stop_waiting();
		drop(other);
		assert!(!pool.waiting(ticket_id));
		assert!(waiting.acquire().is_some(), "with no one waiting, the ticket asks again");
	}

	/// A grant to one user of a ticket while another still waits leaves the
	/// registration marked waiting in the same step: the pool's books never
	/// show it not waiting between the grant and the mark, so a place freed
	/// meanwhile stays owed to it.
	#[test]
	fn a_grant_keeps_the_ticket_waiting_for_its_other_users_at_once() {
		use crate::test_env::SharedPlaces;

		let pool = SharedPlaces::new(1, 1);
		let (_, older) = pool.register();
		let held = older.acquire().expect("free");
		let (ticket_id, places) = pool.register();
		let (waiting, other) = (places.asking(), places.asking());
		assert!(waiting.acquire().is_none(), "refused while the older one holds the place");
		drop(held);
		let _granted = other.acquire().expect("the freed place");
		assert_eq!(
			pool.granted_waiting().last(),
			Some(&true),
			"the grant itself left the registration waiting"
		);
		assert!(pool.waiting(ticket_id));
	}

	/// An up-front grant takes what is free, at once, and holds it until it
	/// is dropped; a short grant leaves no waiting mark, and a grant of none
	/// is valid.
	#[test]
	fn an_up_front_grant_takes_what_is_free_without_waiting() {
		let pool = CountingPlaces::new(5, 8);
		let places = pool.ticket();
		let first = places.acquire_places(3);
		assert_eq!(first.len(), 3);
		assert_eq!(pool.held(), 3);
		let second = places.acquire_places(4);
		assert_eq!(second.len(), 2, "min(max, free)");
		assert_eq!(pool.held(), 5);
		assert!(!pool.waiting(), "a short grant clears the mark its refusal left");
		drop(first);
		assert_eq!(pool.held(), 2);
		drop(second);
		assert_eq!(pool.held(), 0);
		let empty = CountingPlaces::new(0, 8);
		assert!(empty.ticket().acquire_places(4).is_empty());
		assert!(!empty.waiting());
		assert!(Ticket::serial().acquire_places(4).is_empty());
	}

	/// A batched read through a run's view is recorded on the transaction the
	/// view was taken from, as that transaction's own batched read would be:
	/// one get, the keys found, the bytes of every key asked for and of every
	/// value found. Those counters are what the transaction's completion event
	/// carries when the view is closed.
	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test]
	async fn reads_through_a_view_are_recorded_on_its_transaction() -> Result<()> {
		use surrealdb_kvs::{Key, TransactionType};

		use super::ReadView;
		use crate::test_env::TestIndexStore;

		let dir = temp_dir::TempDir::new()?;
		let ds =
			TestIndexStore::with_path(&format!("surrealkv:{}", dir.path().to_string_lossy())).await;
		{
			let tx = ds.transaction(TransactionType::Write).await?;
			tx.set(Key::from(b"view/a".to_vec()), b"one".to_vec()).await?;
			tx.set(Key::from(b"view/c".to_vec()), b"three!".to_vec()).await?;
			tx.commit().await?;
		}
		let origin = ds.transaction(TransactionType::Write).await?;
		let view =
			ReadView::open(&origin).await?.expect("SurrealKV gives a read-only transaction a view");
		let keys: Vec<Key<'static>> =
			[&b"view/a"[..], b"view/b", b"view/c"].iter().map(|k| Key::from(k.to_vec())).collect();
		let values = view.get_many(&keys).await?;
		assert_eq!(values, [Some(b"one".to_vec()), None, Some(b"three!".to_vec())]);
		let recorded = view.tx().metrics_snapshot_for_test();
		assert_eq!(
			(
				recorded.ops_get,
				recorded.keys_read,
				recorded.key_bytes_read,
				recorded.value_bytes_read
			),
			(1, 2, 18, 9),
			"(gets, keys found, key bytes, value bytes) recorded on the view's transaction"
		);
		view.close().await;
		origin.cancel().await?;
		Ok(())
	}

	/// A current-thread runtime interleaves the spawned tasks rather than
	/// running them in parallel, and must still produce every result.
	#[tokio::test]
	async fn a_current_thread_runtime_still_runs_every_job() -> Result<()> {
		let doubled = run_jobs(&roomy(), 4, 10, |i| async move { Ok(2 * i) }).await?;
		assert_eq!(doubled, (0..10).map(|i| 2 * i).collect::<Vec<_>>());
		Ok(())
	}
}
