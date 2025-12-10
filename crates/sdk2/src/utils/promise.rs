use anyhow::Result;
use std::{
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};
use tokio::sync::watch;

/// Handle for resolving/rejecting a Promise (Send + Sync).
///
/// This can be passed to background tasks to resolve the Promise.
#[derive(Clone)]
pub struct PromiseHandle<T: Clone> {
	state: Arc<watch::Sender<PromiseState<T>>>,
}

impl<T: Clone> PromiseHandle<T> {
	/// Resolves the Promise with a value.
	pub fn resolve(&self, value: T) {
		let _ = self.state.send(PromiseState::Resolved(value));
	}

	/// Rejects the Promise with an error.
	pub fn reject(&self, error: anyhow::Error) {
		let _ = self.state.send(PromiseState::Rejected(Arc::new(error)));
	}
}

/// A Promise-like struct that can be awaited and resolved/rejected later.
///
/// This is built on top of `tokio::sync::watch` and provides JavaScript-like
/// Promise semantics with support for cancellation.
///
/// Note: Promise itself is not Send because watch::Receiver is !Send,
/// but it can be cloned cheaply. Use `handle()` to get a Send-able resolver.
pub struct Promise<T: Clone> {
	handle: PromiseHandle<T>,
	rx: watch::Receiver<PromiseState<T>>,
}

#[derive(Clone)]
enum PromiseState<T: Clone> {
	Pending,
	Resolved(T),
	Rejected(Arc<anyhow::Error>),
}

impl<T: Clone> Clone for Promise<T> {
	fn clone(&self) -> Self {
		Self {
			handle: self.handle.clone(),
			rx: self.rx.clone(),
		}
	}
}

impl<T: Clone> Promise<T> {
	/// Creates a new pending Promise and its handle.
	pub fn new() -> Self {
		let (tx, rx) = watch::channel(PromiseState::Pending);
		Self {
			handle: PromiseHandle {
				state: Arc::new(tx),
			},
			rx,
		}
	}

	/// Gets a Send-able handle that can resolve/reject this promise.
	pub fn handle(&self) -> PromiseHandle<T> {
		self.handle.clone()
	}

	/// Resolves the Promise with a value.
	pub fn resolve(&self, value: T) {
		self.handle.resolve(value);
	}

	/// Rejects the Promise with an error.
	pub fn reject(&self, error: anyhow::Error) {
		self.handle.reject(error);
	}

	/// Checks if the promise has been resolved (not pending or rejected).
	pub fn is_resolved(&self) -> bool {
		matches!(*self.rx.borrow(), PromiseState::Resolved(_))
	}
}

impl<T: Clone> Future for Promise<T> {
	type Output = Result<T>;

	fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
		// Clone the state to avoid holding borrow across operations
		let state = self.rx.borrow_and_update().clone();

		match state {
			PromiseState::Resolved(value) => Poll::Ready(Ok(value)),
			PromiseState::Rejected(err) => Poll::Ready(Err(anyhow::anyhow!("{}", err))),
			PromiseState::Pending => {
				// Register waker for when the state changes
				// The watch receiver will automatically wake us when the state updates
				Poll::Pending
			}
		}
	}
}
