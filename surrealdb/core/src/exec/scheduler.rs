//! The host's scheduler: how a running program gives up its thread.
//!
//! A program yields only at its yield point (`exec::vm::fuel::yield_point`),
//! once it has spent a quantum of fuel. What yielding means is the host's: a
//! native server hands the task back to the tokio runtime, an embedder driving
//! its own event loop returns to it. The host supplies a [`Scheduler`] through
//! the datastore ([`crate::kvs::Datastore::with_scheduler`]); without one the
//! [`default`](yield_now) applies.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The future a [`Scheduler`] answers a yield with.
pub type YieldFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// How a running program gives up its thread.
///
/// The future [`Scheduler::yield_now`] answers must return `Pending` at least
/// once before it completes, and arrange for the task to be polled again: a
/// yield that completes on its first poll lets the program run on, and the
/// work between two yields is no longer bounded by the quantum.
pub trait Scheduler: Send + Sync + 'static {
	/// Give up the thread once.
	fn yield_now(&self) -> YieldFuture;
}

thread_local! {
	/// The times a program running on this thread reached its yield point.
	static YIELDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The times a program running on this thread has reached its yield point, which it does once it
/// has spent a quantum of fuel. A caller that reads it before and after it polls a future learns
/// whether the future ran a quantum's worth of work in that poll: a host can tell a long statement
/// from a short one without timing it.
pub fn yields_on_this_thread() -> u64 {
	YIELDS.with(std::cell::Cell::get)
}

/// Counts a yield of a program on this thread; see [`yields_on_this_thread`].
pub(crate) fn note_yield() {
	YIELDS.with(|yields| yields.set(yields.get().wrapping_add(1)));
}

/// Give up the thread once, as the default scheduler does: back to the tokio
/// runtime when the task runs on one, and otherwise through a future that is
/// pending once and wakes its own task, so the executor driving it polls
/// whatever else it holds first.
pub(crate) async fn yield_now() {
	#[cfg(not(target_family = "wasm"))]
	if tokio::runtime::Handle::try_current().is_ok() {
		tokio::task::yield_now().await;
		return;
	}
	YieldOnce(false).await
}

/// A future that is pending on its first poll, waking its own task, and
/// ready on the next.
pub(crate) struct YieldOnce(pub(crate) bool);

impl Future for YieldOnce {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		if self.0 {
			return Poll::Ready(());
		}
		self.0 = true;
		cx.waker().wake_by_ref();
		Poll::Pending
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use std::task::Wake;

	use super::*;

	struct Counting(std::sync::atomic::AtomicUsize);

	impl Wake for Counting {
		fn wake(self: Arc<Self>) {
			self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		}
	}

	/// The yield-once future is pending once, waking its task, then ready.
	#[test]
	fn yield_once_is_pending_once_and_wakes_its_task() {
		let woken = Arc::new(Counting(Default::default()));
		let waker = std::task::Waker::from(Arc::clone(&woken));
		let mut cx = Context::from_waker(&waker);
		let mut once = YieldOnce(false);
		assert!(Pin::new(&mut once).poll(&mut cx).is_pending());
		assert_eq!(woken.0.load(std::sync::atomic::Ordering::Relaxed), 1);
		assert!(Pin::new(&mut once).poll(&mut cx).is_ready());
	}
}
