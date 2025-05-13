use crate::err::Error;
use crate::sql::Bytesize;

use super::Bytes;
use futures::StreamExt;
use revision::Revisioned;
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::{
	fmt::{self, Display, Formatter},
	ops::DerefMut,
};
use ulid::Ulid;

#[derive(Clone)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
// The ULID is used for ordering and uniqueness of the value
pub struct Stream(pub(crate) Arc<Mutex<Option<StreamVal>>>, Ulid);

impl Stream {
	pub fn consume(&self) -> Result<StreamVal, Error> {
		let mut val = self.0.lock().map_err(|_| Error::StreamConsumed)?;
		val.take().ok_or(Error::StreamConsumed)
	}

	pub async fn consume_bytes(&self, max: Option<Bytesize>) -> Result<Vec<u8>, Error> {
		let mut stream = self.consume()?;
		let mut size: u64 = 0;
		let mut bytes: Vec<u8> = Vec::new();

		while let Some(chunk) = stream.deref_mut().next().await {
			yield_now!();
			let chunk = chunk.map_err(|_| Error::Unreachable("Invalid stream".to_string()))?;
			if let Some(max) = max {
				size += chunk.len() as u64;
				if size > max.0 {
					return Err(Error::StreamTooLarge(max));
				}
			}

			bytes.extend_from_slice(&chunk);
		}

		Ok(bytes)
	}
}

impl From<bytes::Bytes> for Stream {
	fn from(bytes: bytes::Bytes) -> Self {
		let stream_value: StreamVal = Box::new(Box::pin(futures::stream::once(async move {
			Ok(bytes) as Result<bytes::Bytes, Box<dyn Display + Send + Sync>>
		})));

		Self(Arc::new(Mutex::new(Some(stream_value))), Ulid::new())
	}
}

impl From<Bytes> for Stream {
	fn from(bytes: Bytes) -> Self {
		bytes::Bytes::from(bytes.0).into()
	}
}

impl From<StreamVal> for Stream {
	fn from(stream: StreamVal) -> Self {
		Self(Arc::new(Mutex::new(Some(stream))), Ulid::new())
	}
}

impl Revisioned for Stream {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		_writer: &mut W,
	) -> std::result::Result<(), revision::Error> {
		Err(revision::Error::Serialize(
			"serialising `Stream` directly is not supported".to_string(),
		))
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(
		_reader: &mut R,
	) -> std::result::Result<Self, revision::Error> {
		Err(revision::Error::Serialize(
			"deserialising `Stream` directly is not supported".to_string(),
		))
	}

	fn revision() -> u16 {
		1
	}
}

impl Serialize for Stream {
	fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		Err(S::Error::custom("Stream serialization is not supported"))
	}
}

impl<'de> Deserialize<'de> for Stream {
	fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Err(D::Error::custom("Stream deserialization is not supported"))
	}
}

impl Display for Stream {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"\".stream()")
	}
}

impl PartialEq for Stream {
	fn eq(&self, other: &Self) -> bool {
		PartialEq::eq(&self.1, &other.1)
	}
}

impl PartialOrd for Stream {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		PartialOrd::partial_cmp(&self.1, &other.1)
	}
}

impl Debug for Stream {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "Stream(Stream, {})", self.1)
	}
}

impl Hash for Stream {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.1.hash(state);
	}
}

#[cfg(target_family = "wasm")]
pub type StreamVal = Box<
	dyn futures::Stream<Item = Result<bytes::Bytes, Box<dyn Display + Send + Sync>>>
		+ Send
		+ Sync
		+ Unpin,
>;

#[cfg(not(target_family = "wasm"))]
pub type StreamVal = Box<
	dyn futures::Stream<Item = Result<bytes::Bytes, Box<dyn Display + Send + Sync>>> + Send + Unpin,
>;
