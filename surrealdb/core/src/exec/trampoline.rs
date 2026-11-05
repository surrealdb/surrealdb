//! Nested evaluation on a heap frame stack, so the native stack a statement
//! uses does not grow with how deeply its evaluation nests.
//!
//! A subquery, a block's statement, a function body or an `eval` query runs a
//! plan inside the evaluation that reached it. Awaited in place, each such
//! level adds the whole chain of `poll` frames beneath it to the native stack,
//! so a recursion through them is bounded by the thread's stack rather than by
//! `max_computation_depth`.
//!
//! [`nest`] hands the nested evaluation to the [`Trampolined`] stream the
//! statement runs under instead. The driver keeps each suspended evaluation
//! as a boxed future on its own stack and polls only the topmost: an
//! evaluation that nests returns `Pending` all the way up to the driver, which
//! then polls the new frame from its own `poll_next`. When a frame completes,
//! its value is left for the future that nested it, and the frame beneath is
//! polled again. The native stack a statement uses is then that of the
//! deepest single frame, whatever the nesting depth.
//!
//! A nested evaluation owns what it needs (`'static`), so a frame never
//! borrows from the one beneath it and the stack may be torn down in any
//! order.
//!
//! Only the topmost frame is polled. A frame beneath waits for the one above
//! it and makes no progress of its own, which is exactly the contract of
//! awaiting a nested evaluation in place — except for work that must keep
//! being polled while the evaluation beneath it runs: a timer that bounds it.
//! Such an operator runs its input under a [`boundary`], a driver of its own
//! inside its poll, so what nests beneath it stays under its timer.
//!
//! Outside any driver — in a task spawned for a buffered input, say —
//! [`nest`] awaits the evaluation in place.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;

/// A nested evaluation the driver runs, and whether the future that nested
/// it has since been dropped, which leaves it unwanted.
struct Frame {
	future: Pin<Box<dyn Future<Output = ()> + Send>>,
	withdrawn: Arc<AtomicBool>,
}

thread_local! {
	/// For each driver polling on this thread, innermost last: the frames
	/// nested during the poll in progress.
	static POLLING: RefCell<Vec<Vec<Frame>>> = const { RefCell::new(Vec::new()) };
}

/// Opens a driver's poll: frames nested until it is closed are the driver's.
/// Closed on drop, so a poll that panics leaves no driver open.
struct PollScope;

impl PollScope {
	fn open() -> Self {
		POLLING.with(|polling| polling.borrow_mut().push(Vec::new()));
		PollScope
	}

	/// Close the poll, taking the frames nested during it.
	fn close(self) -> Vec<Frame> {
		let nested = POLLING.with(|polling| polling.borrow_mut().pop());
		std::mem::forget(self);
		nested.expect("a poll scope is open")
	}
}

impl Drop for PollScope {
	fn drop(&mut self) {
		POLLING.with(|polling| polling.borrow_mut().pop());
	}
}

pub(crate) fn under_driver() -> bool {
	POLLING.with(|polling| !polling.borrow().is_empty())
}

/// Evaluate `evaluation` as a frame of the driver the caller is polled under,
/// or in place when there is none.
pub(crate) fn nest<T: Send + 'static>(
	evaluation: impl Future<Output = T> + Send + 'static,
) -> Nested<T> {
	nest_boxed(Box::pin(evaluation))
}

/// [`nest`] for an evaluation already boxed, as a recursion that reaches its
/// own nesting point boxes it to name its type.
pub(crate) fn nest_boxed<T: Send + 'static>(
	evaluation: Pin<Box<dyn Future<Output = T> + Send>>,
) -> Nested<T> {
	Nested {
		state: NestState::Unstarted(evaluation),
	}
}

/// The value of a nested evaluation; see [`nest`].
pub(crate) struct Nested<T> {
	state: NestState<T>,
}

enum NestState<T> {
	Unstarted(Pin<Box<dyn Future<Output = T> + Send>>),
	InPlace(Pin<Box<dyn Future<Output = T> + Send>>),
	Nested {
		slot: Arc<Mutex<Slot<T>>>,
		withdrawn: Arc<AtomicBool>,
	},
	Done,
}

/// Where a frame leaves its value for the future that nested it, and the
/// waker to wake when it does. The driver polls that future again when the
/// frame completes, but the future may be one another executor polls — a task
/// a script runtime spawned inside the frame beneath — which polls it only
/// when it is woken.
struct Slot<T> {
	value: Option<T>,
	waker: Option<std::task::Waker>,
}

impl<T: Send + 'static> Future for Nested<T> {
	type Output = T;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
		let this = &mut *self;
		loop {
			match &mut this.state {
				NestState::Unstarted(_) => {
					let NestState::Unstarted(evaluation) =
						std::mem::replace(&mut this.state, NestState::Done)
					else {
						unreachable!("matched above")
					};
					if !under_driver() {
						this.state = NestState::InPlace(evaluation);
						continue;
					}
					let slot = Arc::new(Mutex::new(Slot {
						value: None,
						waker: Some(cx.waker().clone()),
					}));
					let withdrawn = Arc::new(AtomicBool::new(false));
					let filled = Arc::clone(&slot);
					let future = Box::pin(async move {
						let result = evaluation.await;
						let waker = {
							let mut slot = filled.lock().unwrap_or_else(|e| e.into_inner());
							slot.value = Some(result);
							slot.waker.take()
						};
						if let Some(waker) = waker {
							waker.wake();
						}
					});
					POLLING.with(|polling| {
						polling.borrow_mut().last_mut().expect("under a driver").push(Frame {
							future,
							withdrawn: Arc::clone(&withdrawn),
						})
					});
					this.state = NestState::Nested {
						slot,
						withdrawn,
					};
					// The driver polls the frame; this future is polled again
					// once it has completed.
					return Poll::Pending;
				}
				NestState::InPlace(evaluation) => {
					let result = std::task::ready!(evaluation.as_mut().poll(cx));
					this.state = NestState::Done;
					return Poll::Ready(result);
				}
				NestState::Nested {
					slot,
					..
				} => {
					let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
					return match slot.value.take() {
						Some(result) => {
							drop(slot);
							this.state = NestState::Done;
							Poll::Ready(result)
						}
						// The frame is still on the driver's stack.
						None => {
							slot.waker = Some(cx.waker().clone());
							Poll::Pending
						}
					};
				}
				NestState::Done => panic!("a nested evaluation polled after it completed"),
			}
		}
	}
}

impl<T> Drop for Nested<T> {
	fn drop(&mut self) {
		if let NestState::Nested {
			withdrawn,
			..
		} = &self.state
		{
			withdrawn.store(true, Ordering::Release);
		}
	}
}

/// How a [`Trampolined`] stream runs the evaluations nested beneath it.
enum Mode {
	/// Decided on the first poll: a driver, unless it is polled under one.
	Undecided,
	/// Runs them on its own frame stack.
	Driver,
	/// Polled under another driver, which runs them.
	PassThrough,
}

/// A stream whose nested evaluations run on a heap frame stack; see the
/// module documentation.
pub(crate) struct Trampolined<S> {
	inner: S,
	/// The suspended evaluations, the one being run last.
	stack: Vec<Frame>,
	mode: Mode,
}

impl<S> Trampolined<S> {
	/// `inner`, driving what nests beneath it unless it is polled under a
	/// driver already.
	pub(crate) fn root(inner: S) -> Self {
		Self {
			inner,
			stack: Vec::new(),
			mode: Mode::Undecided,
		}
	}
}

/// `inner`, driving what nests beneath it even under another driver: for an
/// operator that must keep polling while its input's nested evaluations run.
pub(crate) fn boundary<S>(inner: S) -> Trampolined<S> {
	Trampolined {
		inner,
		stack: Vec::new(),
		mode: Mode::Driver,
	}
}

/// `work`, an evaluation that runs side by side with others under one poll,
/// driving what nests beneath it on a frame stack of its own.
///
/// A driver polls only its topmost frame, so two evaluations that nest under
/// the same driver run one after the other: the frame the first pushed waits
/// beneath the second's. Work its caller polls concurrently with other work —
/// the host calls a Surrealism module awaits together — runs under a driver
/// of its own, and each such evaluation keeps making progress.
#[cfg(feature = "surrealism")]
pub(crate) async fn concurrent<T: Send>(work: impl Future<Output = T> + Send) -> T {
	use futures::StreamExt;
	let mut driven = boundary(futures::stream::once(Box::pin(work)));
	driven.next().await.expect("a one-item stream yields its item")
}

impl<S: Stream + Unpin> Stream for Trampolined<S> {
	type Item = S::Item;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
		let this = &mut *self;
		if let Mode::Undecided = this.mode {
			this.mode = if under_driver() {
				Mode::PassThrough
			} else {
				Mode::Driver
			};
		}
		if let Mode::PassThrough = this.mode {
			return Pin::new(&mut this.inner).poll_next(cx);
		}
		loop {
			while this.stack.last().is_some_and(|frame| frame.withdrawn.load(Ordering::Acquire)) {
				this.stack.pop();
			}
			let scope = PollScope::open();
			let polled = match this.stack.last_mut() {
				Some(frame) => Polled::Frame(frame.future.as_mut().poll(cx)),
				None => Polled::Inner(Pin::new(&mut this.inner).poll_next(cx)),
			};
			let nested = scope.close();
			let nested_any = !nested.is_empty();
			if let Polled::Frame(Poll::Ready(())) = polled {
				this.stack.pop();
			}
			// The first evaluation nested runs first.
			this.stack.extend(nested.into_iter().rev());
			match polled {
				Polled::Frame(Poll::Ready(())) => continue,
				Polled::Frame(Poll::Pending) | Polled::Inner(Poll::Pending) if nested_any => {
					continue;
				}
				Polled::Frame(Poll::Pending) | Polled::Inner(Poll::Pending) => {
					return Poll::Pending;
				}
				Polled::Inner(Poll::Ready(item)) => return Poll::Ready(item),
			}
		}
	}
}

enum Polled<T> {
	Frame(Poll<()>),
	Inner(Poll<T>),
}

#[cfg(test)]
mod tests {
	use futures::StreamExt;

	use super::*;

	/// A recursion `depth` levels deep, each level nesting the next.
	fn descend(depth: u32) -> Pin<Box<dyn Future<Output = u32> + Send>> {
		Box::pin(async move {
			if depth == 0 {
				return 0;
			}
			tokio::task::yield_now().await;
			nest(descend(depth - 1)).await + 1
		})
	}

	fn stream_of(depth: u32) -> impl Stream<Item = u32> + Unpin + Send {
		Box::pin(futures::stream::once(descend(depth)))
	}

	/// A recursion far deeper than a small stack holds in place completes
	/// under a driver.
	#[test]
	fn a_driver_holds_the_native_stack_whatever_the_depth() {
		let run = std::thread::Builder::new()
			.stack_size(256 * 1024)
			.spawn(|| {
				let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
				runtime.block_on(async { Trampolined::root(stream_of(100_000)).next().await })
			})
			.unwrap();
		assert_eq!(run.join().unwrap(), Some(100_000));
	}

	/// Outside a driver an evaluation is awaited in place.
	#[tokio::test]
	async fn without_a_driver_a_nested_evaluation_runs_in_place() {
		assert_eq!(nest(async { 7 }).await, 7);
		assert_eq!(descend(50).await, 50);
	}

	/// A driver polled under another passes its nested evaluations to the
	/// outer one, and a boundary keeps its own.
	#[tokio::test]
	async fn inner_streams_pass_through_and_boundaries_drive() {
		let inner = || Trampolined::root(stream_of(20));
		let outer = Trampolined::root(Box::pin(futures::stream::once(async move {
			let a = inner().next().await;
			let b = boundary(stream_of(30)).next().await;
			(a, b)
		})));
		let mut outer = outer;
		assert_eq!(outer.next().await, Some((Some(20), Some(30))));
	}

	/// A nested evaluation awaited by a future another executor polls — one
	/// that polls a future again only when it is woken — completes: the frame
	/// wakes the future that nested it.
	#[tokio::test]
	async fn a_nested_evaluation_wakes_the_future_awaiting_it() {
		let mut stream = Trampolined::root(Box::pin(futures::stream::once(async move {
			// A local executor, run inside the frame, polling a task that
			// nests: it is re-polled only by its waker.
			let mut pool = futures::executor::LocalPool::new();
			let result = std::rc::Rc::new(std::cell::Cell::new(None));
			let out = std::rc::Rc::clone(&result);
			futures::task::LocalSpawnExt::spawn_local(&pool.spawner(), async move {
				out.set(Some(nest(async { 5 }).await + 1));
			})
			.expect("spawned");
			// Bounded, so a task that is never woken fails the test rather
			// than hanging it.
			for _ in 0..100 {
				pool.run_until_stalled();
				if let Some(value) = result.get() {
					return Some(value);
				}
				tokio::task::yield_now().await;
			}
			None
		})));
		assert_eq!(stream.next().await, Some(Some(6)));
	}

	/// A nested evaluation whose awaiting future is dropped is not run.
	#[tokio::test]
	async fn a_withdrawn_evaluation_does_not_run() {
		let ran = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&ran);
		let mut stream = Trampolined::root(Box::pin(futures::stream::once(async move {
			let nested = nest(async move {
				flag.store(true, Ordering::Release);
			});
			// Polled once, so it is nested, then dropped unfinished.
			let mut nested = Box::pin(nested);
			let _ = futures::poll!(nested.as_mut());
			drop(nested);
			1
		})));
		assert_eq!(stream.next().await, Some(1));
		assert!(!ran.load(Ordering::Acquire));
	}
}
