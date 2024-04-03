use futures::executor::block_on;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::Duration;

/// Blocking future handling that should be avoided.
/// It is required for this code because otherwise you need to detect if you are (or are not) in tokio,
/// get the tokio runtime (needs to passed down), and then _NOT_ create a blocking future - because
/// that is still async... trust me this is the best way to synchronously handle async code without
/// knowing the runtime and without creating more async code
fn block_on_special<F: Future>(mut future: F) -> F::Output {
	// Create a RawWakerVTable that does nothing (since we don't need to wake anything for this example)
	static VTABLE: RawWakerVTable =
		RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});

	// Create a RawWaker from the VTable.
	let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
	// Convert the RawWaker to a Waker.
	let waker = unsafe { Waker::from_raw(raw_waker) };
	// Create a Context from the Waker.
	let mut context = Context::from_waker(&waker);

	// Pin the future to the stack.
	let mut future = unsafe { Pin::new_unchecked(&mut future) };

	loop {
		// Poll the future.
		match future.as_mut().poll(&mut context) {
			Poll::Ready(output) => return output, // If the future is ready, return the output.
			Poll::Pending => {
				// If the future is not ready, sleep for a small amount of time and try again.
				// This is a very crude way to wait and should not be used in production code.
				thread::sleep(Duration::from_millis(10));
			}
		}
	}
}

/// This must always be declared in a named way:
/// `let foo = DroppyBoy::new(async { ... });` or `let _foo = DroppyBoy::new(async { ... });`
/// If you assign in an unnamed way, it will be dropped immediately irresepective of optimisation
/// level.
pub struct DroppyBoy<F>
where
	F: Future + Send + 'static,
{
	f: Option<F>,
}

impl<F: Future + Send + 'static> DroppyBoy<F> {
	pub fn new(future: F) -> Self {
		Self {
			f: Some(future),
		}
	}
}

impl<F: Future + Send + 'static> Drop for DroppyBoy<F>
where
	F: Future + Send + 'static,
{
	fn drop(&mut self) {
		let dummy_future: Option<F> = None;
		let f = std::mem::replace(&mut self.f, dummy_future);
		let f: F = match f {
			Some(f) => f,
			None => panic!("DroppyBoy future has an existing clone"),
		};
		block_on_special(f);
	}
}

#[cfg(test)]
mod test {
	use crate::kvs::droppy_boy::DroppyBoy;
	use std::sync::atomic::AtomicBool;
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::sync::mpsc::channel;

	#[test]
	fn can_drop_sync() {
		let counter = Arc::new(AtomicBool::new(false));
		let counter_clone = counter.clone();
		{
			let _ = DroppyBoy::new(async move {
				counter_clone.store(true, std::sync::atomic::Ordering::Relaxed);
			});
		}
		assert!(counter.load(std::sync::atomic::Ordering::Relaxed));
	}

	#[tokio::test]
	async fn can_drop_async() {
		let (sender, mut receiver) = channel(1);
		{
			let _ = DroppyBoy::new(async move {
				sender.send(()).await.unwrap();
			});
		}
		tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await.unwrap().unwrap();
	}
}
