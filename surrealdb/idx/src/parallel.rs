//! Bounded parallelism for index construction.
//!
//! An engine that builds part of a batch on several tasks hands the work to
//! [`run_jobs`] as a list of independent jobs. The engine owns the
//! decomposition: which parts of its batch are independent, and how their
//! results are combined. This module owns only the execution, and the
//! guarantees the engines rely on:
//!
//! * **Results come back in job order**, whichever task ran each job, so a result the engine
//!   derives from them does not depend on the number of tasks.
//! * **A failed job fails the whole run.** The first error is returned and the remaining tasks are
//!   aborted; a panicking job resumes its panic on the caller. Nothing is dropped silently, so an
//!   engine never commits a batch one of whose jobs did not finish.
//! * **Dropping the run aborts its tasks**, so a cancelled caller does not leave work behind
//!   holding the state it shared with them.
//! * **One process-wide gate bounds the jobs running at once** to the machine's available
//!   parallelism, however many runs are in flight. Background maintenance compacts every index with
//!   outstanding work concurrently, so without it each index's own task count would multiply.
//!
//! Without a tokio runtime to spawn onto — on wasm, or in an embedding that
//! runs none — and whenever one task is asked for, the jobs run inline on the
//! calling task, in order. The results are the same either way.
//!
//! Jobs that read committed state lease a read-only snapshot from a
//! [`SnapshotPool`] rather than sharing the caller's transaction: on engines
//! that run one transaction's operations one at a time, reads through a single
//! transaction would queue behind each other however many tasks issue them.

use std::future::Future;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::sync::OnceLock;
#[cfg(not(target_family = "wasm"))]
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use parking_lot::Mutex;
use surrealdb_datastore::Transaction;
#[cfg(not(target_family = "wasm"))]
use tokio::sync::Semaphore;
#[cfg(not(target_family = "wasm"))]
use tokio::task::JoinSet;

#[cfg(not(target_family = "wasm"))]
use crate::config::BuildParallelism;

/// The permits bounding how many jobs run at once across the process.
#[cfg(not(target_family = "wasm"))]
fn gate() -> &'static Arc<Semaphore> {
	static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
	GATE.get_or_init(|| Arc::new(Semaphore::new(BuildParallelism::Auto.tasks())))
}

/// Runs `job(i)` for every `i` in `0..jobs` on up to `tasks` tasks and returns
/// the results in index order.
///
/// Jobs are handed out one at a time, so an expensive job does not hold back a
/// task's share of the cheap ones, and each job runs under a permit of the
/// process-wide gate. See the module documentation for the error, panic and
/// cancellation guarantees.
pub(crate) async fn run_jobs<T, F, Fut>(tasks: usize, jobs: usize, job: F) -> Result<Vec<T>>
where
	T: Send + 'static,
	F: Fn(usize) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = Result<T>> + Send + 'static,
{
	#[cfg(not(target_family = "wasm"))]
	if tasks > 1 && jobs > 1 && tokio::runtime::Handle::try_current().is_ok() {
		return run_spawned(tasks.min(jobs), jobs, job).await;
	}
	let _ = tasks;
	let mut results = Vec::with_capacity(jobs);
	for i in 0..jobs {
		results.push(job(i).await?);
		yield_now!();
	}
	Ok(results)
}

#[cfg(not(target_family = "wasm"))]
async fn run_spawned<T, F, Fut>(tasks: usize, jobs: usize, job: F) -> Result<Vec<T>>
where
	T: Send + 'static,
	F: Fn(usize) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = Result<T>> + Send + 'static,
{
	let job = Arc::new(job);
	let next = Arc::new(AtomicUsize::new(0));
	// Dropped on every early return below, and dropping a `JoinSet` aborts
	// the tasks still in it.
	let mut set = JoinSet::new();
	for _ in 0..tasks {
		let job = Arc::clone(&job);
		let next = Arc::clone(&next);
		let gate = Arc::clone(gate());
		set.spawn(async move {
			let mut done = Vec::new();
			loop {
				let i = next.fetch_add(1, Ordering::Relaxed);
				if i >= jobs {
					return Ok::<_, anyhow::Error>(done);
				}
				let permit = gate.acquire().await?;
				done.push((i, job(i).await?));
				drop(permit);
				yield_now!();
			}
		});
	}
	let mut results: Vec<Option<T>> = (0..jobs).map(|_| None).collect();
	while let Some(joined) = set.join_next().await {
		let done = match joined {
			Ok(done) => done?,
			Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
			Err(err) => return Err(anyhow::anyhow!("an index build task was cancelled: {err}")),
		};
		for (i, result) in done {
			results[i] = Some(result);
		}
	}
	results
		.into_iter()
		.map(|result| {
			result.ok_or_else(|| anyhow::anyhow!("an index build job produced no result"))
		})
		.collect()
}

/// Read-only snapshots of one datastore, leased to parallel jobs one at a time.
///
/// A job holds its lease for as long as it reads, so jobs running at once
/// read through different transactions. Snapshots are opened on demand — never
/// more than the jobs running at once — and handed back for reuse when a lease
/// drops. Each sees the datastore as committed when it opened and none of the
/// origin transaction's uncommitted writes, so a pool serves reads of state
/// that was committed before the work began.
///
/// The owner calls [`Self::close`] once every job has finished, which cancels
/// the snapshots; a pool dropped without it lets them close as they drop.
pub(crate) struct SnapshotPool {
	/// The transaction snapshots are opened alongside, on its datastore.
	origin: Arc<Transaction>,
	/// Snapshots no lease holds.
	idle: Mutex<Vec<Transaction>>,
}

impl SnapshotPool {
	pub(crate) fn new(origin: Arc<Transaction>) -> Arc<Self> {
		Arc::new(Self {
			origin,
			idle: Mutex::new(Vec::new()),
		})
	}

	/// Leases a snapshot: an idle one if there is one, a new one otherwise.
	pub(crate) async fn lease(self: &Arc<Self>) -> Result<SnapshotLease> {
		let idle = self.idle.lock().pop();
		let tx = match idle {
			Some(tx) => tx,
			None => self.origin.open_read_snapshot().await?,
		};
		Ok(SnapshotLease {
			pool: Arc::clone(self),
			tx: Some(tx),
		})
	}

	/// Cancels every idle snapshot. Called once no lease is outstanding.
	pub(crate) async fn close(&self) {
		let idle = std::mem::take(&mut *self.idle.lock());
		for tx in idle {
			let _ = tx.cancel().await;
		}
	}
}

/// One job's snapshot, handed back to its pool on drop.
pub(crate) struct SnapshotLease {
	pool: Arc<SnapshotPool>,
	tx: Option<Transaction>,
}

impl SnapshotLease {
	/// The leased snapshot.
	pub(crate) fn tx(&self) -> &Transaction {
		self.tx.as_ref().expect("a lease holds its snapshot until it drops")
	}
}

impl Drop for SnapshotLease {
	fn drop(&mut self) {
		if let Some(tx) = self.tx.take() {
			self.pool.idle.lock().push(tx);
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use anyhow::{Result, bail};

	use super::run_jobs;

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn results_come_back_in_job_order_whatever_the_task_count() -> Result<()> {
		for tasks in [1, 2, 3, 8, 64] {
			let squares = run_jobs(tasks, 100, |i| async move { Ok(i * i) }).await?;
			assert_eq!(squares, (0..100).map(|i| i * i).collect::<Vec<_>>(), "tasks = {tasks}");
		}
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn every_job_runs_exactly_once() -> Result<()> {
		let runs: Arc<Vec<AtomicUsize>> = Arc::new((0..257).map(|_| AtomicUsize::new(0)).collect());
		let counted = Arc::clone(&runs);
		run_jobs(8, 257, move |i| {
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
		let res = run_jobs(4, 50, |i| async move {
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
		let _ = run_jobs(4, 20, |i| async move {
			if i == 11 {
				panic!("job {i} panicked");
			}
			Ok(i)
		})
		.await;
	}

	/// A leased snapshot reads what was committed before it opened and none of
	/// the origin transaction's uncommitted writes; a lease handed back is
	/// reused, and closing the pool cancels what it holds.
	#[tokio::test]
	async fn snapshots_read_committed_state_and_are_reused() -> Result<()> {
		use surrealdb_kvs::{Key, TransactionType};

		use super::SnapshotPool;
		use crate::test_env::TestIndexStore;

		let ds = TestIndexStore::new().await;
		{
			let tx = ds.transaction(TransactionType::Write).await?;
			tx.set(Key::from(b"committed".to_vec()), b"1".to_vec()).await?;
			tx.commit().await?;
		}
		let origin = Arc::new(ds.transaction(TransactionType::Write).await?);
		origin.set(Key::from(b"staged".to_vec()), b"2".to_vec()).await?;

		let pool = SnapshotPool::new(Arc::clone(&origin));
		{
			let lease = pool.lease().await?;
			assert!(!lease.tx().writeable(), "a snapshot must be read-only");
			assert_eq!(
				lease.tx().get(Key::from(b"committed".to_vec()), None).await?,
				Some(b"1".to_vec())
			);
			assert_eq!(lease.tx().get(Key::from(b"staged".to_vec()), None).await?, None);
		}
		assert_eq!(pool.idle.lock().len(), 1, "a dropped lease hands its snapshot back");
		{
			let _first = pool.lease().await?;
			let _second = pool.lease().await?;
			assert!(pool.idle.lock().is_empty(), "both snapshots are leased");
		}
		assert_eq!(pool.idle.lock().len(), 2, "a second concurrent lease opens a second snapshot");
		pool.close().await;
		assert!(pool.idle.lock().is_empty(), "closing cancels every idle snapshot");
		origin.cancel().await?;
		Ok(())
	}

	/// A current-thread runtime interleaves the spawned tasks rather than
	/// running them in parallel, and must still produce every result.
	#[tokio::test]
	async fn a_current_thread_runtime_still_runs_every_job() -> Result<()> {
		let doubled = run_jobs(4, 10, |i| async move { Ok(2 * i) }).await?;
		assert_eq!(doubled, (0..10).map(|i| 2 * i).collect::<Vec<_>>());
		Ok(())
	}
}
