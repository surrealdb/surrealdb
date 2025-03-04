#![cfg(not(target_family = "wasm"))]

use async_executor::{Executor, Task};
use futures::channel::oneshot;
use std::future::Future;
use std::panic::catch_unwind;
use std::sync::LazyLock;

/// When should use this function?
///
/// **Avoiding the Tokio Runtime Block**
/// When you run CPU-intensive (or otherwise blocking) tasks on the main Tokio runtime or its worker threads,
/// you risk blocking those threads. Because Tokio uses an asynchronous runtime,
/// having a long-running, CPU-bound task can prevent other operations (including I/O-bound operations)
/// from proceeding smoothly. By offloading CPU-intensive tasks to a separate executor in its own thread,
/// your main Tokio event loop remains free to handle other futures and I/O tasks.
///
/// **Using a Single Dedicated Thread**
/// The snippet spawns exactly one extra thread (instead of multiple) for CPU-intensive work.
/// That’s because many CPU-intensive tasks (such as cryptographic functions, image processing, data compression, etc.)
/// typically have built-in parallelization logic internally or can spawn parallel tasks themselves.
/// By default, if you anticipate that the CPU-intensive code you’re calling will handle any internal parallelization,
/// you can safely run it on a single dedicated thread without fragmenting resources.
/// Essentially, you’re avoiding contention or overhead from spawning more threads than necessary for tasks that already know
/// how to handle parallelism internally.
pub fn _single_spawn<T: Send + 'static>(
	future: impl Future<Output = T> + Send + 'static,
) -> Task<T> {
	static GLOBAL: LazyLock<Executor<'_>> = LazyLock::new(|| {
		// The name of the thread for the task executor
		let name = "surrealdb-executor".to_string();
		// Spawn a single thread for CPU intensive tasks
		std::thread::Builder::new()
			.name(name)
			.spawn(|| {
				catch_unwind(|| {
					// Run the task executor indefinitely
					futures::executor::block_on(GLOBAL.run(futures::future::pending::<()>()))
				})
				.ok();
			})
			.expect("Unable to create executor task thread");
		// Create a new executor for CPU intensive tasks
		Executor::new()
	});
	// Spawn any future onto the single-threaded executor
	GLOBAL.spawn(future)
}

pub async fn spawn<F, R>(f: F) -> R
where
	F: FnOnce() -> R + Send + 'static,
	R: Send + 'static,
{
	let (tx, rx) = oneshot::channel();
	rayon::spawn(move || {
		let result = f();
		// Ignore errors in case the receiver was dropped
		let _ = tx.send(result);
	});
	rx.await.expect("Spawned task was canceled before completing")
}
