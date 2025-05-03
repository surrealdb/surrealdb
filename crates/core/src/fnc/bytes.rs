use crate::ctx::Context;
use crate::err::Error;
use crate::sql::stream::StreamVal;
use crate::sql::{Bytes, Uuid, Value};
use futures::stream::Stream;
use std::fmt::Display;
use std::pin::Pin;
use std::task::Poll;

pub fn len((bytes,): (Bytes,)) -> Result<Value, Error> {
	Ok(bytes.len().into())
}

pub fn stream(ctx: &Context, (bytes,): (Bytes,)) -> Result<Value, Error> {
	let Some(streams) = ctx.get_streams() else {
		return Err(Error::StreamsUnavailable);
	};

	// Convert to bytes::Bytes
	let bytes = bytes::Bytes::from(bytes.0);

	struct SingleItemStream {
		item: Option<Result<bytes::Bytes, Box<dyn Display + Send + Sync>>>,
	}

	impl Stream for SingleItemStream {
		type Item = Result<bytes::Bytes, Box<dyn Display + Send + Sync>>;

		fn poll_next(
			mut self: Pin<&mut Self>,
			_: &mut std::task::Context<'_>,
		) -> Poll<Option<Self::Item>> {
			Poll::Ready(self.item.take())
		}
	}

	// These are safe because bytes::Bytes is Send + Sync and Box<dyn Display + Send + Sync> is Send + Sync
	unsafe impl Send for SingleItemStream {}
	unsafe impl Sync for SingleItemStream {}

	let stream_value: StreamVal = Box::new(SingleItemStream {
		item: Some(Ok(bytes)),
	});

	let pointer = Uuid::new_v7();
	streams.insert(pointer.0.clone(), stream_value);
	Ok(Value::Stream(crate::sql::stream::Stream(pointer)))
}
