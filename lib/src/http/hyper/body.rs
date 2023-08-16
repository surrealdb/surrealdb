use std::{
	cell::UnsafeCell,
	error::Error as StdError,
	future::{self, Future},
	mem::{self, ManuallyDrop},
	sync::{Arc, Once},
	task::Poll,
};

use bytes::{Bytes, BytesMut};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use hyper::body::HttpBody;

use super::{BoxError, BoxStream};

pub struct SyncCell<T: ?Sized> {
	once: Once,
	value: UnsafeCell<ManuallyDrop<T>>,
}

unsafe impl<T> Send for SyncCell<T> {}
unsafe impl<T> Sync for SyncCell<T> {}

impl<T: ?Sized> Drop for SyncCell<T> {
	fn drop(&mut self) {
		self.once.call_once(|| {
			// SAFETY: This can only be called once, so it is safe to access mutably,
			// and since it was not executed yet the value is still present so we need to call
			// drop,
			unsafe { ManuallyDrop::drop(&mut *self.value.get()) }
		})
	}
}

impl<T: Sized> SyncCell<T> {
	pub fn new(t: T) -> Self {
		Self {
			once: Once::new(),
			value: UnsafeCell::new(ManuallyDrop::new(t)),
		}
	}

	pub fn take(&self) -> Option<T> {
		let mut res = None;
		self.once.call_once(|| {
			// SAFETY: Since this function can only be called once, the value is still present in
			// value and we can move out of it safely.
			let value = unsafe { ManuallyDrop::take(&mut *self.value.get()) };
			res = Some(value);
		});
		res
	}
}

impl<T: ?Sized> SyncCell<T> {
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
	Stream(BoxStream),
	/// A `reusable` stream, allows one to reuse a stream if the body was not consumed.
	/// Used for replaying bodies in case of redirects.
	Reusable(Arc<SyncCell<BoxStream>>),
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
		S: Stream<Item = Result<O, E>> + Send + Sync + 'static,
		Bytes: From<O>,
		E: StdError + Send + Sync + 'static,
	{
		let stream =
			stream.map_err(|e| Arc::new(e) as Arc<dyn StdError + Send + Sync>).map_ok(|o| o.into());
		let stream = Box::pin(stream);
		Self::wrap_stream_box(stream)
	}

	/// Create a body from a stream which is already boxed, avoids extra allocation.
	pub fn wrap_stream_box(stream: BoxStream) -> Self {
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
		match std::mem::replace(&mut self.kind, Kind::Used) {
			Kind::Used => Self::used(),
			// Already reusable
			Kind::Buffer(x) => {
				self.kind = Kind::Buffer(x.clone());
				Self {
					kind: Kind::Buffer(x),
				}
			}
			Kind::Reusable(x) => {
				self.kind = Kind::Reusable(x.clone());
				Self {
					kind: Kind::Reusable(x),
				}
			}
			Kind::Stream(x) => {
				let reusable = Arc::new(SyncCell::new(x));
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
		let future = match self.kind {
			Kind::Used => return None,
			Kind::Buffer(x) => return Some(Ok(x)),
			Kind::Stream(x) => x,
			Kind::Reusable(x) => {
				if let Some(x) = x.take() {
					x
				} else {
					return None;
				}
			}
		};

		let res = future.try_collect().await.map(BytesMut::freeze);
		Some(res)
	}

	/// Turn the body into a stream of data.
	///
	/// Returns None if the body was already used.
	pub fn to_stream(self) -> Option<BoxStream> {
		match self.kind {
			Kind::Used => None,
			Kind::Buffer(x) => {
				let future = future::ready(Result::<_, BoxError>::Ok(x));
				let stream = Box::pin(stream::once(future));
				Some(stream as BoxStream)
			}
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
			Kind::Reusable(ref x) => x.is_taken(),
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
			Kind::Buffer(ref x) => (Self::from_buffer(x.clone()), None),
			Kind::Stream(ref mut stream) => {
				let (a_tx, a_rx) = channel::unbounded();
				let (b_tx, b_rx) = channel::unbounded();
				let stream = mem::replace(stream, Box::pin(a_rx));
				let res = Self::wrap_stream(b_rx);
				(res, Some(Self::drive_channel_futures(stream, a_tx, b_tx)))
			}
			Kind::Reusable(ref mut x) => {
				if let Some(x) = x.take() {
					self.kind = Kind::Stream(x);
				} else {
					self.kind = Kind::Used;
				}
				self.tee()
			}
		}
	}

	async fn drive_channel_futures(
		mut stream: BoxStream,
		a_tx: channel::Sender<Result<Bytes, BoxError>>,
		b_tx: channel::Sender<Result<Bytes, BoxError>>,
	) {
		let mut a_tx = Some(a_tx);
		let mut b_tx = Some(b_tx);
		while let Some(data) = stream.next().await {
			if let Some(ref mut s) = a_tx {
				if s.send(data.clone()).await.is_err() {
					if b_tx.is_none() {
						return;
					}
					a_tx = None;
				}
			}
			if let Some(ref mut s) = b_tx {
				if s.send(data.clone()).await.is_err() {
					if a_tx.is_none() {
						return;
					}
					b_tx = None;
				}
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
		match mem::replace(&mut self.kind, Kind::Used) {
			Kind::Used => Poll::Ready(None),
			Kind::Buffer(buffer) => Poll::Ready(Some(Ok(buffer))),
			Kind::Stream(mut x) => {
				let res = x.poll_next_unpin(cx);
				self.kind = Kind::Stream(x);
				res
			}
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
