use std::cell::UnsafeCell;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use futures::Stream;

pub trait StreamFn<'a, Y: 'a>: FnOnce(Yielder<'a, Y>) -> <Self as StreamFn<Y>>::Fut {
	type Error;
	type Fut: Future<Output = Result<(), Self::Error>>;
}

impl<'a, T, Fut, Y: 'a, E> StreamFn<'a, Y> for T
where
	T: FnOnce(Yielder<'a, Y>) -> Fut,
	Fut: Future<Output = Result<(), E>>,
{
	type Error = E;
	type Fut = Fut;
}

enum State<'a, F, Y: 'a>
where
	F: StreamFn<'a, Y>,
{
	Call(F),
	Future(F::Fut),
	Done,
}

pub struct AsyncStream<'a, F: StreamFn<'a, Y>, Y> {
	// Order is important here, state has to be dropped before place because a deconstructor
	// inside the future might access `place` via the Yielder.
	state: State<'a, F, Y>,
	place: UnsafeCell<Option<Y>>,
	// The implementation relies on a stable position of place
	//
	// Also the struct is self reverential, It contains a future which contains a pointer to place.
	// Which is technically unsound under stacked-borrows, however self reverential structs are
	// used quite frequently (any `async{ }` block which contains a mutable reference to an item on
	// the stack is a self-referential struct) so it is improbable that it will actually break
	// anything as stacked-borrows will more likely be amended.
	//
	// Miri, for example, currently permits this when a type is `!Unpin`.
	_marker: PhantomPinned,
}

impl<'a, F, Y> Stream for AsyncStream<'a, F, Y>
where
	F: StreamFn<'a, Y>,
{
	type Item = Result<Y, F::Error>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		// Safety: Pinning is structural for `place` and `state` once it is in the `State::Future`
		// state.
		let this = unsafe { self.get_unchecked_mut() };

		// Safety: This can be done without unsafe as we have a mutable reference we can use the
		// safe method `get_mut` however because we have a limited violation of stacked borrow we
		// access place as if we only have a non-mutable access to the field.
		if let Some(x) = unsafe { &mut (*this.place.get()) }.take() {
			return Poll::Ready(Some(Ok(x)));
		}

		// First match then move, this ensures the future isn't accidentally moved while it is
		// pinned.
		if matches!(&this.state, State::Call(_)) {
			std::hint::cold_path();
			// Move out the callback, this is fine since pinning is not yet valid for state.
			let State::Call(f) = std::mem::replace(&mut this.state, State::Done) else {
				// Safety: Trivially holds as it is checked immediatly above.
				unsafe { std::hint::unreachable_unchecked() }
			};

			let yielder = Yielder {
				ptr: NonNull::from_mut(&mut this.place),
				_marker: PhantomData,
			};
			let future = f(yielder);
			this.state = State::Future(future);
		}

		if matches!(&this.state, State::Done) {
			std::hint::cold_path();
			return Poll::Ready(None);
		}

		let State::Future(future) = &mut this.state else {
			// Safety: Safe since if the state was call it will be changed to State::Future.
			// and if the state was State::Done this position would not be reachable.
			unsafe { std::hint::unreachable_unchecked() }
		};

		// Safety: Sound since the pin is structural for State::Future
		let future = unsafe { Pin::new_unchecked(future) };
		match future.poll(cx) {
			Poll::Ready(x) => {
				// Future is done, so we can drop it.
				this.state = State::Done;
				match x {
					Ok(_) => Poll::Ready(None),
					Err(e) => Poll::Ready(Some(Err(e))),
				}
			}
			Poll::Pending => {
				// Safety: See similar access of `place` field above.
				if let Some(x) = unsafe { &mut (*this.place.get()) }.take() {
					Poll::Ready(Some(Ok(x)))
				} else {
					Poll::Pending
				}
			}
		}
	}
}

unsafe impl<'a, F: Send + StreamFn<'a, Y>, Y: Send> Send for AsyncStream<'a, F, Y> {}

pub struct Yielder<'a, T> {
	ptr: NonNull<UnsafeCell<Option<T>>>,
	// Yielder functions as a mutable pointer.
	_marker: PhantomData<&'a mut T>,
}

/// The send implementation of yielder relies on
/// yielder being send across a thread while the future within AsyncStream returns a poll value at
/// the same time being impossible.
///
/// As far as I am aware that is impossible (outside of unsound affinity pool).
///
/// - Thread::scope will block the future so it cannot return a Poll value.
/// - tokio::task::block_in_place will also block the future.
/// - affinitypool::spawn_local allows it but is unsound because it relies on a drop guarentee.
///
/// If there was a method to send Yielder to a thread while also returning poll pending that would
/// also allow the future to be dropped and the pointer inside yielder being invalidated. That same
/// method could be used to invalidate references by sending them across from a future with a
/// lifetime shorter then `'static` and then dropping the future while the reference is in the
/// thread. As such a thing has to be unsound this Send implementation must be sound.
unsafe impl<Y: Send> Send for Yielder<'_, Y> {}

impl<'a, T> Yielder<'a, T> {
	/// Emit a value from the stream.
	///
	/// If the future returned by this function is dropped before it is driven to completion the
	/// value to be emitted will likely be dropped and not returned from the stream.
	///
	/// Ensure the future is propelly completed before calling emit again.
	#[inline]
	#[must_use = "The value won't be emitted unless the returned future completes."]
	pub fn emit<'b>(&'b mut self, value: T) -> YielderFuture<'b, 'a, T> {
		// Safety: The lifetime on Yielder ensures that it cannot outlife the lifetime of place.
		// So therefore derefencing the pointer is safe. Furthermore the value can be modified
		// safely as this function requires a mutable reference and therefore it has unique access.
		unsafe { &mut (*self.ptr.as_ref().get()) }.replace(value);
		YielderFuture(self)
	}
}

pub struct YielderFuture<'a, 'b, T>(&'a mut Yielder<'b, T>);

impl<T> Future for YielderFuture<'_, '_, T> {
	type Output = ();

	#[inline]
	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		// Safety: The lifetime on Yielder ensures that it cannot outlife the lifetime of place.
		// So therefore derefencing the pointer is safe.
		if unsafe { &(*this.0.ptr.as_ref().get()) }.is_some() {
			Poll::Pending
		} else {
			Poll::Ready(())
		}
	}
}

#[must_use = "A stream does nothing unless polled"]
pub fn try_async_stream<Y, F>(f: F) -> AsyncStream<'static, F, Y>
where
	F: for<'a> StreamFn<'a, Y>,
{
	AsyncStream {
		place: UnsafeCell::new(None),
		state: State::Call(f),
		_marker: PhantomPinned,
	}
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use futures::future::poll_fn;
	use futures::{StreamExt, TryStreamExt};

	use super::*;

	#[test]
	fn sequence() {
		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
			let stream = try_async_stream(async |mut yielder: Yielder<usize>| -> Result<(), ()> {
				for i in 0..10 {
					yielder.emit(i).await
				}
				Ok(())
			});

			let res = stream.try_collect::<Vec<_>>().await.unwrap();
			assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
		})
	}

	#[test]
	fn do_other_stuff_between() {
		tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
			async {
				let stream =
					try_async_stream(async |mut yielder: Yielder<usize>| -> Result<(), ()> {
						yielder.emit(0).await;
						tokio::time::sleep(Duration::from_millis(10)).await;
						yielder.emit(1).await;
						Ok(())
					});

				let res = stream.try_collect::<Vec<_>>().await.unwrap();
				assert_eq!(res, vec![0, 1])
			},
		)
	}

	#[test]
	fn wait_on_channel() {
		tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
			async {
				let (send, recv) = futures::channel::oneshot::channel::<()>();

				let stream =
					try_async_stream(async move |mut yielder: Yielder<usize>| -> Result<(), ()> {
						yielder.emit(0).await;

						recv.await.unwrap();

						yielder.emit(1).await;
						Ok(())
					});
				let mut stream = Box::pin(stream);

				assert_eq!(stream.next().await.unwrap().unwrap(), 0);
				poll_fn(|cx| {
					match stream.poll_next_unpin(cx) {
						Poll::Ready(_) => panic!("Did not wait correctly"),
						Poll::Pending => {}
					}
					Poll::Ready(())
				})
				.await;
				send.send(()).unwrap();
				assert_eq!(stream.next().await.unwrap().unwrap(), 1);
				assert_eq!(stream.next().await, None);
			},
		)
	}
}
