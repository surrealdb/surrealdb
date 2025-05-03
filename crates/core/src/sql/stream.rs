use crate::{ctx::Context, err::Error};

use super::Uuid;
use futures::StreamExt;
use revision::revisioned;
use serde::{Deserialize, Serialize};
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
			Ok(x.1)
		} else {
			Err(Error::StreamConsumed)
		}
	}

	pub async fn consume_bytes(&self, ctx: &Context) -> Result<Vec<u8>, Error> {
		let mut stream = self.consume(ctx)?;
		let mut bytes: Vec<u8> = Vec::new();

		while let Some(chunk) = stream.deref_mut().next().await {
			yield_now!();
			let chunk = chunk.map_err(|_| Error::Unreachable("Invalid stream".to_string()))?;
			bytes.extend_from_slice(&chunk);
		}

		Ok(bytes)
	}

	pub async fn next_n(&self, ctx: &Context, n: usize) -> Result<Vec<u8>, Error> {
		let Some(streams) = ctx.get_streams() else {
			return Err(Error::StreamsUnavailable);
		};

		if let Some(x) = streams.get(&self.0) {
			let stream = x.deref_mut();
			let mut bytes = Vec::with_capacity(n);
			let mut remaining = n;

			while remaining > 0 {
				if let Some(chunk) = stream.next().await {
					let chunk =
						chunk.map_err(|_| Error::Unreachable("Invalid stream".to_string()))?;

					let bytes_to_take = std::cmp::min(chunk.len(), remaining);

					bytes.extend_from_slice(&chunk[..bytes_to_take]);

					remaining -= bytes_to_take;
				} else {
					break;
				}
			}

			Ok(bytes)
		} else {
			Err(Error::StreamConsumed)
		}
	}
}

impl Display for Stream {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "<stream> {}", self.0)
	}
}

pub type StreamVal = Box<
	dyn futures::Stream<Item = Result<bytes::Bytes, Box<dyn Display + Send + Sync>>>
		+ Send
		+ Sync
		+ Unpin,
>;
