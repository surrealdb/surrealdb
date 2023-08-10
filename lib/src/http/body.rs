use std::{pin::Pin, str::Utf8Error};

use bytes::Bytes;
use futures::Stream;

pub enum Body {
	Buffer(Bytes),
	Stream(Pin<Box<dyn Stream<Item = Bytes>>>),
}

impl Body {
	pub fn wrap_stream<S>(s: S) -> Self
	where
		S: Stream<Item = Bytes>,
	{
		Body::Stream(Box::pin(s))
	}

	pub fn wrap_pinned_stream<S>(s: Pin<Box<dyn Stream<Item = Bytes>>>) -> Self
where {
		Body::Stream(s)
	}
}

impl From<Bytes> for Body {
	fn from(b: Bytes) -> Self {
		Body::Buffer(b)
	}
}
