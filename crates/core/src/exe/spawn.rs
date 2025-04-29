#![cfg(not(target_family = "wasm"))]
use futures::channel::oneshot;

/// This function spawns the passed closure onto Rayon's pool of worker threads,
/// allowing multiple CPU-intensive tasks to be executed in parallel.
///
/// When should use this function?
///
/// **Avoiding the Tokio Runtime Block**
/// When you run CPU-intensive (or otherwise blocking) tasks on the main Tokio
/// runtime or its worker threads, you risk blocking those threads. Because
/// Tokio uses an asynchronous runtime, having a long-running, CPU-bound task
/// can prevent other operations (including I/O-bound operations)
/// from proceeding smoothly. By offloading CPU-intensive tasks to a separate
/// executor in its own thread, your main Tokio event loop remains free to
/// handle other futures and I/O tasks.
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
	rx.await.expect("Receiver dropped")
}
