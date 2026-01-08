use std::fmt::Display;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use super::err::ApiError;
use crate::err::Error;
use crate::expr::Bytesize;
use crate::types::PublicValue;

pub enum ApiBody {
	Stream(Box<dyn Stream<Item = Result<Bytes, Box<dyn Display + Send + Sync>>> + Send + Unpin>),
	Native(PublicValue),
}

impl ApiBody {
	pub fn from_stream<S, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<Bytes, E>> + Unpin + Send + 'static,
		E: Display + Send + Sync + 'static,
	{
		let mapped_stream =
			stream.map(|result| result.map_err(|e| Box::new(e) as Box<dyn Display + Send + Sync>));
		Self::Stream(Box::new(mapped_stream))
	}

	pub fn from_value(value: PublicValue) -> Self {
		Self::Native(value)
	}

	pub fn is_native(&self) -> bool {
		matches!(self, Self::Native(_))
	}

	// The `max` variable is unused in WASM only
	#[cfg_attr(target_family = "wasm", expect(unused_variables))]
	pub(crate) async fn stream(self, max: Option<Bytesize>) -> Result<::bytes::Bytes, Error> {
		match self {
			#[cfg(not(target_family = "wasm"))]
			Self::Stream(mut stream) => {
				use bytes::BytesMut;

				let max = max.unwrap_or(Bytesize::MAX);
				let mut size: u64 = 0;
				let mut bytes = BytesMut::new();

				while let Some(chunk) = stream.next().await {
					yield_now!();
					let chunk = chunk.map_err(|_| Error::ApiError(ApiError::InvalidRequestBody))?;
					size += chunk.len() as u64;
					if size > max.0 {
						return Err(ApiError::RequestBodyTooLarge(max).into());
					}

					bytes.extend_from_slice(&chunk);
				}

				Ok(bytes.freeze())
			}
			_ => Err(Error::Unreachable(
				"Encountered a native body whilst trying to stream one".into(),
			)),
		}
	}

	pub(crate) async fn process(self) -> Result<PublicValue, Error> {
		if let ApiBody::Native(value) = self {
			Ok(value)
		} else {
			// TODO we need to get a max body size back somehow. Introduction of blob like value? This stream somehow needs to be postponed...
			// also if such a value would be introduced then this whole enum can be eliminated. body would always just simply be a value
			// maybe bytes could have two variants, consumed and unconsumed. To the user its simply bytes, but whenever an api request
			// is processed, the body would be unconsumed bytes, and whenever we get a file, that too could be unconsumed bytes. When the user
			// actually does something with them, they get consumed, but to the user its always simply just bytes.
			// we could expose handlebars to describe the "internal state" of the value...
			let bytes = self.stream(None).await?;
			Ok(PublicValue::Bytes(surrealdb_types::Bytes::from(bytes)))
		}
	}
}
