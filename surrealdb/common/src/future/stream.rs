use std::cell::UnsafeCell;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::FusedStream;

/// Helper trait to be able to name to returned future type.
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

pub struct AsyncStream<'a, F: StreamFn<'a, Y>, Y> {
	call: Option<F>,
	// Field order is important here, future must be dropped before place because a destructor
	// within future might access place via the yielder.
	future: Option<F::Fut>,
	// FIXME: Change this to UnsafePinned whenever it becomes stable.
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

		if let Some(f) = this.call.take() {
			// This branch is only called once, initially.
			std::hint::cold_path();

			let yielder = Yielder {
				ptr: unsafe { NonNull::new_unchecked(this.place.get()) },
				_marker: PhantomData,
			};
			let future = f(yielder);
			this.future = Some(future);
		}

		if let Some(fut) = this.future.as_mut() {
			// Safety: Sound since the pin is structural for State::Future
			let future = unsafe { Pin::new_unchecked(fut) };
			match future.poll(cx) {
				Poll::Ready(x) => {
					// Future is done, so we can drop it.
					this.future.take();
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
		} else {
			Poll::Ready(None)
		}
	}
}

impl<'a, F, Y> FusedStream for AsyncStream<'a, F, Y>
where
	F: StreamFn<'a, Y>,
{
	fn is_terminated(&self) -> bool {
		self.future.is_none()
	}
}

pub struct Yielder<'a, T> {
	ptr: NonNull<Option<T>>,
	// Yielder functions as a mutable pointer.
	// Required both to make Yielder invariant over T and to attach the lifetime.
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
		unsafe { self.ptr.replace(Some(value)) };
		YielderFuture {
			ptr: self.ptr,
			_marker: PhantomData,
		}
	}
}

pub struct YielderFuture<'a, 'b, T> {
	ptr: NonNull<Option<T>>,
	// The future is essentially a reference to the yielder.
	_marker: PhantomData<&'a mut Yielder<'b, T>>,
}

unsafe impl<Y: Send> Send for YielderFuture<'_, '_, Y> {}

impl<T> Future for YielderFuture<'_, '_, T> {
	type Output = ();

	#[inline]
	fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.get_mut();
		// Safety: The lifetime on Yielder ensures that it cannot outlife the lifetime of place.
		// So therefore derefencing the pointer is safe.
		if unsafe { this.ptr.as_ref().is_some() } {
			Poll::Pending
		} else {
			Poll::Ready(())
		}
	}
}

/// Turns a closure returing a future into a stream of values.
///
/// The closure handed to the this function will be passed a [`Yielder`] which can be used to emit
/// values.
/// Errors can be raised by returning them directly from the closure.
#[must_use = "A stream does nothing unless polled"]
pub fn try_async_stream<'s, Y, F>(f: F) -> AsyncStream<'s, F, Y>
where
	F: for<'a> StreamFn<'a, Y>,
{
	AsyncStream {
		call: Some(f),
		future: None,
		place: UnsafeCell::new(None),
		_marker: PhantomPinned,
	}
}

#[cfg(test)]
mod test {
	use std::cell::Cell;
	use std::future;
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
		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
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
		})
	}

	#[test]
	fn return_error() {
		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
			let stream =
				try_async_stream(async move |mut yielder: Yielder<usize>| -> Result<(), usize> {
					yielder.emit(0).await;

					Err(1)
				});

			let mut stream = Box::pin(stream);

			assert_eq!(stream.next().await.unwrap().unwrap(), 0);
			assert_eq!(stream.next().await.unwrap().unwrap_err(), 1);

			assert!(stream.is_terminated());

			poll_fn(|cx| {
				match stream.poll_next_unpin(cx) {
					Poll::Ready(None) => {}
					_ => panic!("wrong value"),
				}
				Poll::Ready(())
			})
			.await;
		})
	}

	#[test]
	fn immediate_done() {
		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
			let stream =
				try_async_stream(async move |_: Yielder<usize>| -> Result<(), ()> { Ok(()) });
			let mut stream = Box::pin(stream);

			assert_eq!(stream.next().await, None);
		})
	}

	#[test]
	fn drop_mid_stream() {
		thread_local! {
			static DROPPED: Cell<usize> = const{ Cell::new(0) };

		}

		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
			DROPPED.with(|x| x.set(0));

			struct Dropped;

			impl Drop for Dropped {
				fn drop(&mut self) {
					DROPPED.with(|x| x.update(|x| x + 1));
				}
			}

			let stream =
				try_async_stream(async move |mut yielder: Yielder<Dropped>| -> Result<(), ()> {
					yielder.emit(Dropped).await;

					struct DropYield<'a>(Yielder<'a, Dropped>);

					impl Drop for DropYield<'_> {
						fn drop(&mut self) {
							let f = self.0.emit(Dropped);
							#[allow(clippy::drop_non_drop)]
							std::mem::drop(f);
						}
					}

					let _drop = DropYield(yielder);
					let _ = future::pending::<()>().await;
					Ok(())
				});
			let mut stream = Box::pin(stream);

			let _: Dropped = stream.next().await.unwrap().unwrap();
			assert_eq!(DROPPED.with(|x| x.get()), 1);

			poll_fn(|cx| {
				let Poll::Pending = stream.poll_next_unpin(cx) else {
					panic!("Incorrect poll result")
				};
				Poll::Ready(())
			})
			.await;

			std::mem::drop(stream);
			assert_eq!(DROPPED.with(|x| x.get()), 2);
		})
	}

	#[test]
	fn fused_stream() {
		tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
			let stream =
				try_async_stream(async move |mut yielder: Yielder<usize>| -> Result<(), ()> {
					yielder.emit(0).await;
					yielder.emit(1).await;
					Ok(())
				});

			let mut stream = Box::pin(stream);

			poll_fn(|cx| {
				let Poll::Ready(Some(Ok(0))) = stream.poll_next_unpin(cx) else {
					panic!("Wrong value")
				};
				let Poll::Ready(Some(Ok(1))) = stream.poll_next_unpin(cx) else {
					panic!("Wrong value")
				};
				let Poll::Ready(None) = stream.poll_next_unpin(cx) else {
					panic!("Wrong value")
				};
				assert!(stream.is_terminated());
				let Poll::Ready(None) = stream.poll_next_unpin(cx) else {
					panic!("Wrong value")
				};
				Poll::Ready(())
			})
			.await;
		})
	}
}
