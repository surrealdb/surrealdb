use super::req_impl::ClientBody;
use bytes::Bytes;
use futures::Stream;
use std::error::Error as StdError;

pub struct Body {
	inner: ClientBody,
}

impl Body {
	pub fn empty() -> Self {
		Body {
			inner: ClientBody::empty(),
		}
	}

	pub(super) fn into_client(self) -> ClientBody {
		self.inner
	}

	pub fn wrap_stream<S, O, E>(s: S) -> Self
	where
		S: Stream<Item = Result<O, E>> + Send + 'static,
		O: Into<Bytes> + 'static,
		E: Into<Box<dyn StdError + Send + Sync>> + 'static,
	{
		Self {
			inner: ClientBody::wrap_stream(s),
		}
	}
}

impl From<Bytes> for Body {
	fn from(b: Bytes) -> Self {
		Body {
			inner: ClientBody::from(b),
		}
	}
}

impl From<Vec<u8>> for Body {
	fn from(b: Vec<u8>) -> Self {
		Body {
			inner: ClientBody::from(b),
		}
	}
}

impl From<String> for Body {
	fn from(b: String) -> Self {
		Body {
			inner: ClientBody::from(b),
		}
	}
}

impl From<&'static str> for Body {
	fn from(b: &'static str) -> Self {
		Body {
			inner: ClientBody::from(b),
		}
	}
}

impl From<&'static [u8]> for Body {
	fn from(b: &'static [u8]) -> Self {
		Body {
			inner: ClientBody::from(b),
		}
	}
}
