use std::{
	cell::UnsafeCell,
	future::{self, Future},
	mem::ManuallyDrop,
	sync::{Arc, Once},
	task::Poll,
};

use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use hyper::body::HttpBody;

use super::BoxError;

pub struct SyncCell<T: ?Sized> {
	once: Once,
	value: UnsafeCell<ManuallyDrop<T>>,
}

unsafe impl<T> Send for SyncCell<T> {}
unsafe impl<T> Sync for SyncCell<T> {}

impl<T> Drop for SyncCell<T> {
	fn drop(&mut self) {
		self.once.call_once(|| {
			// SAFETY: This can only be called once, so it is safe to access mutably,
			// and since it was not executed yet the value is still present so we need to call
			// drop,
			unsafe { ManuallyDrop::drop(&mut *self.value.get()) }
		})
	}
}

impl<T> SyncCell<T> {
	pub fn new(t: T) -> Self {
		Self {
			once: Once::new(),
			value: UnsafeCell::new(ManuallyDrop::new(t)),
		}
	}

	pub fn take(&self) -> Option<Box<T>> {
		let mut res = None;
		self.once.call_once(|| {
			// SAFETY: Since this function can only be called once, the value is still present in
			// value and we can move out of it safely.
			let value = unsafe { ManuallyDrop::take(&mut *self.value.get()) };
			res = Some(value);
		});
		res
	}

	pub fn is_taken(&self) -> bool {
		self.once.is_completed()
	}
}

// A body implementation for use with hyper.
//
// Has support for teeing streams and reusing bodies in redirects.
pub struct Body {
	kind: Kind,
}

enum Kind {
	/// A used body, will not return any values,
	Used,
	/// A single buffer
	Buffer(Bytes),
	/// A stream of buffers,
	Stream(Box<dyn Stream<Item = Result<Bytes, BoxError>>>),
	/// A `reusable` stream, allows one to reuse a stream if the body was not consumed.
	/// Used for replaying bodies in case of redirects.
	Reusable(Arc<SyncCell<dyn Stream<Item = Result<Bytes, BoxError>>>>),
}

impl Body {
	/// Create a body which is used.
	pub fn used() -> Self {
		Body {
			kind: Kind::Used,
		}
	}

	/// Create a unused body with no data.
	pub fn empty() -> Self {
		Body {
			kind: Kind::Buffer(Bytes::new()),
		}
	}

	/// Create a body which wraps a stream.
	pub fn wrap_stream<S, O, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<O, E>>,
		O: Into<Bytes>,
		E: Into<BoxError>,
	{
		let stream = stream.map_err(|e| e.into()).map_ok(|o| o.into());
		let stream = Box::new(stream);
		Self::wrap_stream_box(stream)
	}

	/// Create a body from a stream which is already boxed, avoids extra allocation.
	pub fn wrap_stream_box(stream: Box<dyn Stream<Item = Result<Bytes, BoxError>>>) -> Self {
		Body {
			kind: Kind::Stream(stream),
		}
	}

	/// Create a body from any value which can be converted to a buffer.
	pub fn from_buffer<B>(bytes: B) -> Self
	where
		Bytes: From<B>,
	{
		Body {
			kind: Kind::Buffer(Bytes::from(bytes)),
		}
	}

	/// Returns a second body which has the same value as the current but can be consumed and
	/// while maintaining the data inside if it goes unused.
	pub fn to_reuse(&mut self) -> Self {
		match self.kind {
			Kind::Used => Self::used(),
			// Already reusable
			Kind::Buffer(ref x) => Self {
				kind: Kind::Buffer(x.clone()),
			},
			Kind::Reusable(ref x) => Self {
				kind: Kind::Reusable(x.clone()),
			},
			Kind::Stream(x) => {
				let reusable = Arc::new(SyncCell::new_box(x));
				self.kind = Kind::Reusable(reusable.clone());
				Self {
					kind: Kind::Reusable(reusable),
				}
			}
		}
	}

	/// Turns the body into a buffer collecting all data from the body.
	///
	/// Returns None if the body was already used.
	pub async fn to_buffer(self) -> Option<Result<Bytes, BoxError>> {
		match self.kind {
			Kind::Used => None,
			Kind::Buffer(x) => Some(x),
			Kind::Stream(x) => Some(x.try_collect().await),
			Kind::Reusable(x) => {
				if let Some(x) = x.take() {
					Some(x.try_collect().await)
				} else {
					None
				}
			}
		}
	}

	/// Turn the body into a stream of data.
	///
	/// Returns None if the body was already used.
	pub fn to_stream(self) -> Option<Box<dyn Stream<Item = Result<Bytes, BoxError>>>> {
		match self.kind {
			Kind::Used => None,
			Kind::Buffer(x) => Some(Box::new(stream::once(future::ready(Ok(x))))),
			Kind::Stream(x) => Some(x),
			Kind::Reusable(x) => x.take(),
		}
	}

	/// Returns if the body is currently used.
	pub fn is_used(&self) -> bool {
		match self.kind {
			Kind::Used => true,
			Kind::Buffer(_) => false,
			Kind::Stream(_) => false,
			Kind::Reusable(x) => x.is_taken(),
		}
	}

	/// Copy a body into a new one, possibly creating a future to drive copying channel data
	/// between buffers.
	///
	/// Will undo reusablity.
	///
	/// If a future is returned it must be polled to completion to drive the channel forward.
	pub fn tee(&mut self) -> (Self, Option<impl Future<Output = ()> + Send + Sync>) {
		match self.kind {
			Kind::Used => (Self::used(), None),
			Kind::Buffer(ref x) => (Self::from_buffer(x), None),
			Kind::Stream(stream) => {
				let (a_tx, a_rx) = channel::unbounded();
				let (b_tx, b_rx) = channel::unbounded();
				*self = Self::wrap_stream(a_rx);
				let res = Self::wrap_stream(b_rx);
				let mut a_tx = Some(a_tx);
				let mut b_tx = Some(b_tx);
				(res, async move {
					while let Some(data) = stream.next().await {
						if let Some(ref mut s) = a_tx {
							if s.send_data(data.clone()).await.is_err() {
								if b_tx.is_none() {
									return;
								}
								a_tx = None;
							}
						}
						if let Some(ref mut s) = b_tx {
							if s.send_data(data.clone()).await.is_err() {
								if a_tx.is_none() {
									return;
								}
								b_tx = None;
							}
						}
					}
				})
			}
			Kind::Reusable(x) => {
				if let Some(x) = x.take() {
					self.kind = Kind::Stream(x);
				} else {
					self.kind = Kind::Empty;
				}
				self.tee()
			}
		}
	}
}

impl HttpBody for Body {
	type Data = Bytes;

	type Error = BoxError;

	fn poll_data(
		mut self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Result<Self::Data, Self::Error>>> {
		match self.kind {
			Kind::Used => Poll::Ready(None),
			Kind::Buffer(buffer) => {
				self.kind = Kind::Used;
				Poll::Ready(Some(Ok(buffer)))
			}
			Kind::Stream(ref mut x) => x.poll_next_unpin(cx),
			Kind::Reusable(x) => {
				if let Some(mut x) = x.take() {
					let res = x.poll_next_unpin(cx);
					self.kind = Kind::Stream(x);
					res
				} else {
					Poll::Ready(None)
				}
			}
		}
	}

	fn poll_trailers(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Result<Option<lib_http::HeaderMap>, Self::Error>> {
		Poll::Ready(Ok(None))
	}
}

impl From<String> for Body {
	fn from(value: String) -> Self {
		Body::from_buffer(value)
	}
}

impl From<Vec<u8>> for Body {
	fn from(value: Vec<u8>) -> Self {
		Body::from_buffer(value)
	}
}

impl From<&'static str> for Body {
	fn from(value: &'static str) -> Self {
		Body::from_buffer(value)
	}
}

impl From<&'static [u8]> for Body {
	fn from(value: &'static [u8]) -> Self {
		Body::from_buffer(value)
	}
}

impl From<Bytes> for Body {
	fn from(value: Bytes) -> Self {
		Body::from_buffer(value)
	}
}

impl From<&'_ Bytes> for Body {
	fn from(value: &Bytes) -> Self {
		Body::from_buffer(value)
	}
}
