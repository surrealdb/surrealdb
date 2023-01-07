#![cfg(not(target_arch = "wasm32"))]

use executor::{Executor, Task};
use once_cell::sync::Lazy;
use std::future::Future;
use std::panic::catch_unwind;

pub fn spawn<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> Task<T> {
	static GLOBAL: Lazy<Executor<'_>> = Lazy::new(|| {
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
