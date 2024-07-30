#![cfg(not(target_arch = "wasm32"))]

use executor::{Executor, Task};
use std::sync::LazyLock;
use std::future::Future;
use std::panic::catch_unwind;

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> Task<T> {
	static GLOBAL: LazyLock<Executor<'_>> = LazyLock::new(|| {
		std::thread::spawn(|| {
			catch_unwind(|| {
				futures::executor::block_on(GLOBAL.run(futures::future::pending::<()>()))
			})
			.ok();
		});
		Executor::new()
	});
	GLOBAL.spawn(future)
}
