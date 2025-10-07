use std::fmt::Display;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::header::CONTENT_TYPE;

use super::context::InvocationContext;
use super::err::ApiError;
use super::invocation::ApiInvocation;
use crate::err::Error;
use crate::expr::Bytesize;
use crate::rpc::format::{cbor, flatbuffers, json};
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
	pub(crate) async fn stream(self, max: Option<Bytesize>) -> Result<Vec<u8>, Error> {
		match self {
			#[cfg(not(target_family = "wasm"))]
			Self::Stream(mut stream) => {
				let max = max.unwrap_or(Bytesize::MAX);
				let mut size: u64 = 0;
				let mut bytes: Vec<u8> = Vec::new();

				while let Some(chunk) = stream.next().await {
					yield_now!();
					let chunk = chunk.map_err(|_| Error::ApiError(ApiError::InvalidRequestBody))?;
					size += chunk.len() as u64;
					if size > max.0 {
						return Err(ApiError::RequestBodyTooLarge(max).into());
					}

					bytes.extend_from_slice(&chunk);
				}

				Ok(bytes)
			}
			_ => Err(Error::Unreachable(
				"Encountered a native body whilst trying to stream one".into(),
			)),
		}
	}

	pub(crate) async fn process(
		self,
		ctx: &InvocationContext,
		invocation: &ApiInvocation,
	) -> Result<PublicValue, Error> {
		if let ApiBody::Native(value) = self {
			let max = ctx.request_body_max.unwrap_or(Bytesize::MAX);
			let size = std::mem::size_of_val(&value);

			if size > max.0 as usize {
				return Err(ApiError::RequestBodyTooLarge(max).into());
			}

			if ctx.request_body_raw {
				// Convert value to bytes if raw body is requested
				match value {
					PublicValue::Bytes(b) => Ok(PublicValue::Bytes(b)),
					PublicValue::String(s) => {
						Ok(PublicValue::Bytes(surrealdb_types::Bytes::new(s.into_bytes())))
					}
					_ => Err(Error::ApiError(ApiError::InvalidRequestBody)),
				}
			} else {
				Ok(value)
			}
		} else {
			let bytes = self.stream(ctx.request_body_max).await?;

			if ctx.request_body_raw {
				Ok(PublicValue::Bytes(surrealdb_types::Bytes::new(bytes)))
			} else {
				let content_type =
					invocation.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());

				let parsed = match content_type {
					Some(super::format::JSON) => json::decode(&bytes),
					Some(super::format::CBOR) => cbor::decode(&bytes),
					Some(super::format::FLATBUFFERS) => flatbuffers::decode(&bytes),
					_ => return Ok(PublicValue::Bytes(surrealdb_types::Bytes::new(bytes))),
				};

				parsed.map_err(|_| Error::ApiError(ApiError::BodyDecodeFailure))
			}
		}
	}
}
