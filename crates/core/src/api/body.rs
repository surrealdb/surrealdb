use std::fmt::Display;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;

use crate::err::Error;
use crate::sql::Bytesize;

pub enum ApiBody {
	#[cfg(not(target_arch = "wasm32"))]
	Stream(Box<dyn Stream<Item = Result<Bytes, Box<dyn Display + Send + Sync>>> + Send + Unpin>),
	Bytes(Vec<u8>),
}

impl ApiBody {
	#[cfg(not(target_arch = "wasm32"))]
	pub fn from_stream<S, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<Bytes, E>> + Unpin + Send + 'static,
		E: Display + Send + Sync + 'static,
	{
		let mapped_stream =
			stream.map(|result| result.map_err(|e| Box::new(e) as Box<dyn Display + Send + Sync>));
		Self::Stream(Box::new(mapped_stream))
	}

	pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
		Self::Bytes(bytes.into())
	}

	pub async fn stream(self, max: Option<Bytesize>) -> Result<Vec<u8>, Error> {
		let max = max.unwrap_or(Bytesize::MAX);

		match self {
			#[cfg(not(target_arch = "wasm32"))]
			Self::Stream(mut stream) => {
				let mut size: u64 = 0;
				let mut bytes: Vec<u8> = Vec::new();

				// TODO(kearfy) Proper errors
				while let Some(chunk) = stream.next().await {
					let chunk = chunk
						.map_err(|e| Error::Unreachable(format!("failed to stream body: {e}")))?;

					size += chunk.len() as u64;
					if size > max.0 {
						return Err(Error::Unreachable(format!(
							"body size exceeds limit: {size} > {max}"
						)));
					}

					bytes.extend_from_slice(&chunk);
				}

				Ok(bytes)
			}
			Self::Bytes(bytes) => {
				let size = bytes.len() as u64;
				if size > max.0 {
					return Err(Error::Unreachable(format!(
						"body size exceeds limit: {size} > {max}"
					)));
				}

				Ok(bytes)
			}
		}
	}
}
