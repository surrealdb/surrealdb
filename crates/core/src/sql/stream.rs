use crate::sql::Bytesize;
use crate::{ctx::Context, err::Error};

use super::{Bytes, Uuid};
use futures::StreamExt;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::{
	fmt::{self, Display, Formatter},
	ops::DerefMut,
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Stream(pub(crate) Uuid);

impl Stream {
	pub fn consume(&self, ctx: &Context) -> Result<StreamVal, Error> {
		let Some(streams) = ctx.get_streams() else {
			return Err(Error::StreamsUnavailable);
		};

		if let Some(x) = streams.remove(&self.0) {
			Ok(x.1.into_inner().map_err(|_| Error::StreamLocked)?)
		} else {
			Err(Error::StreamConsumed)
		}
	}

	pub async fn consume_bytes(
		&self,
		ctx: &Context,
		max: Option<Bytesize>,
	) -> Result<Vec<u8>, Error> {
		let mut stream = self.consume(ctx)?;
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

	pub fn from_bytes(ctx: &Context, bytes: Bytes) -> Result<Self, Error> {
		let Some(streams) = ctx.get_streams() else {
			return Err(Error::StreamsUnavailable);
		};

		// Convert to bytes::Bytes
		let bytes = bytes::Bytes::from(bytes.0);

		// Create the stream and pin it
		let stream_value: StreamVal = Box::new(Box::pin(futures::stream::once(async move {
			Ok(bytes) as Result<bytes::Bytes, Box<dyn Display + Send + Sync>>
		})));

		// Create the pointer
		let pointer = Uuid::new_v7();
		// Register the stream
		streams.insert(pointer.0.clone(), Mutex::new(stream_value));
		Ok(Self(pointer))
	}

	pub fn from_stream(ctx: &Context, stream: StreamVal) -> Result<Self, Error> {
		let Some(streams) = ctx.get_streams() else {
			return Err(Error::StreamsUnavailable);
		};

		let pointer = Uuid::new_v7();
		streams.insert(pointer.0.clone(), Mutex::new(stream));
		Ok(Self(pointer))
	}
}

impl Display for Stream {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "<stream> {}", self.0)
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
