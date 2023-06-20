use crate::fnc::script::fetch::{classes::BlobClass, stream::ReadableStream, RequestError};
use bytes::{Bytes, BytesMut};
use futures::{future, Stream, StreamExt, TryStreamExt};
use js::{ArrayBuffer, Class, Ctx, Error, Exception, FromJs, Result, Type, TypedArray, Value};
use std::{
	cell::{Cell, RefCell},
	pin::Pin,
	result::Result as StdResult,
	task::Poll,
};

pub type StreamItem = StdResult<Bytes, RequestError>;

#[derive(Clone)]
pub enum BodyKind {
	Buffer,
	String,
	Blob(String),
}

pub enum BodyData {
	Buffer(Bytes),
	Stream(RefCell<ReadableStream<StreamItem>>),
	// Returned when the body is already taken
	Used,
}

/// A stream returning the data from a body.
pub enum BodyStream {
	Buffer(Option<Bytes>),
	Stream(RefCell<ReadableStream<StreamItem>>),
}

impl Stream for BodyStream {
	type Item = StreamItem;

	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		match &mut *self {
			BodyStream::Buffer(ref mut x) => Poll::Ready(x.take().map(Ok)),
			BodyStream::Stream(ref mut x) => x.borrow_mut().poll_next_unpin(cx),
		}
	}
}

/// A struct representing the body mixin.
///
/// Implements [`FromJs`] for conversion from `Blob`, `ArrayBuffer`, any `TypedBuffer` and `String`.
pub struct Body {
	/// The type of body
	pub kind: BodyKind,
	/// The data of the body
	pub data: Cell<BodyData>,
}

impl Default for Body {
	fn default() -> Self {
		Body::new()
	}
}

impl Body {
	/// Create a new used body.
	pub fn new() -> Self {
		Body {
			kind: BodyKind::Buffer,
			data: Cell::new(BodyData::Used),
		}
	}

	/// Returns wther the body is alread used.
	pub fn used(&self) -> bool {
		match self.data.replace(BodyData::Used) {
			BodyData::Used => true,
			x => {
				self.data.set(x);
				false
			}
		}
	}

	/// Create a body from a buffer.
	pub fn buffer<B>(kind: BodyKind, buffer: B) -> Self
	where
		B: Into<Bytes>,
	{
		let bytes = buffer.into();
		Body {
			kind,
			data: Cell::new(BodyData::Buffer(bytes)),
		}
	}

	/// Create a body from a stream.
	pub fn stream<S>(kind: BodyKind, stream: S) -> Self
	where
		S: Stream<Item = StreamItem> + Send + Sync + 'static,
	{
		Body {
			kind,
			data: Cell::new(BodyData::Stream(RefCell::new(ReadableStream::new(stream)))),
		}
	}

	/// Returns the data from the body as a buffer.
	///
	/// if the body is a stream this future only returns when the full body is consumed.
	pub async fn to_buffer(&self) -> StdResult<Option<Bytes>, RequestError> {
		match self.data.replace(BodyData::Used) {
			BodyData::Buffer(x) => Ok(Some(x)),
			BodyData::Stream(stream) => {
				let stream = stream.into_inner();
				let mut res = BytesMut::new();
				stream
					.try_for_each(|x| {
						res.extend_from_slice(&x);
						future::ready(Ok(()))
					})
					.await?;
				Ok(Some(res.freeze()))
			}
			BodyData::Used => Ok(None),
		}
	}

	/// turns the body into a stream,
	/// returns None if the body was already used.
	pub fn to_stream(&self) -> Option<impl Stream<Item = StreamItem>> {
		match self.data.replace(BodyData::Used) {
			BodyData::Buffer(x) => Some(BodyStream::Buffer(Some(x))),
			BodyData::Stream(x) => Some(BodyStream::Stream(x)),
			BodyData::Used => None,
		}
	}

	/// Clones the body teeing any possible underlying streems
	pub fn clone_js(&self, ctx: Ctx<'_>) -> Self {
		let data = match self.data.replace(BodyData::Used) {
			BodyData::Buffer(x) => {
				let res = BodyData::Buffer(x.clone());
				self.data.set(BodyData::Buffer(x));
				res
			}
			BodyData::Stream(stream) => {
				let (tee, drive) = stream.borrow_mut().tee();
				ctx.spawn(drive);
				self.data.set(BodyData::Stream(stream));
				BodyData::Stream(RefCell::new(tee))
			}
			BodyData::Used => BodyData::Used,
		};
		Self {
			kind: self.kind.clone(),
			data: Cell::new(data),
		}
	}
}

impl<'js> FromJs<'js> for Body {
	fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = match value.type_of() {
			Type::String => {
				let string = value.as_string().unwrap().to_string()?;
				return Ok(Body::buffer(BodyKind::String, string));
			}
			Type::Object => value.as_object().unwrap(),
			x => {
				return Err(Error::FromJs {
					from: x.as_str(),
					to: "Blob, TypedArray, FormData, URLSearchParams, or String",
					message: None,
				})
			}
		};
		if let Ok(x) = Class::<BlobClass>::from_object(object.clone()) {
			let borrow = x.borrow();
			return Ok(Body::buffer(BodyKind::Blob(borrow.mime.clone()), borrow.data.clone()));
		}
		if let Ok(x) = TypedArray::<i8>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<u8>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<i16>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<u16>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<i32>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<u32>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<i64>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = TypedArray::<u64>::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}
		if let Ok(x) = ArrayBuffer::from_object(object.clone()) {
			let bytes = x
				.as_bytes()
				.ok_or_else(|| Exception::throw_type(ctx, "Buffer is already detached"))?;
			return Ok(Body::buffer(BodyKind::Buffer, Bytes::copy_from_slice(bytes)));
		}

		Err(Error::FromJs {
			from: "object",
			to: "Blob, TypedArray, FormData, URLSearchParams, or String",
			message: None,
		})
	}
}
