#![cfg(not(target_family = "wasm"))]

use async_executor::{Executor, Task};
use std::future::Future;
use std::panic::catch_unwind;
use std::sync::LazyLock;

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> Task<T> {
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
