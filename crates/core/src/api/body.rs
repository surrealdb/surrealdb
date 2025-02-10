use std::fmt::Display;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use http::header::CONTENT_TYPE;

use crate::err::Error;
use crate::rpc::format::{cbor, json, msgpack, revision};
use crate::sql::Bytesize;
use crate::sql::Kind;
use crate::sql::Value;

use super::context::InvocationContext;
use super::err::ApiError;
use super::invocation::ApiInvocation;

pub enum ApiBody {
	#[cfg(not(target_arch = "wasm32"))]
	Stream(Box<dyn Stream<Item = Result<Bytes, Box<dyn Display + Send + Sync>>> + Send + Unpin>),
	Native(Value),
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

	pub fn from_value(value: Value) -> Self {
		Self::Native(value)
	}

	pub fn is_native(&self) -> bool {
		matches!(self, Self::Native(_))
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

	pub async fn process<'a>(
		self,
		ctx: &InvocationContext,
		invocation: &'a ApiInvocation<'a>,
	) -> Result<Value, Error> {
		if let ApiBody::Native(value) = self {
			let max = ctx.request_body_max.to_owned().unwrap_or(Bytesize::MAX);
			let size = std::mem::size_of_val(&value);

			if size > max.0 as usize {
				return Err(ApiError::RequestBodyTooLarge(max).into());
			}

			if ctx.request_body_raw {
				value.coerce_to(&Kind::Bytes)
			} else {
				Ok(value)
			}
		} else {
			let bytes = self.stream(ctx.request_body_max.to_owned()).await?;

			if ctx.request_body_raw {
				Ok(Value::Bytes(crate::sql::Bytes(bytes)))
			} else {
				let content_type =
					invocation.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());

				let parsed = match content_type {
					Some("application/json") => json::parse_value(&bytes),
					Some("application/cbor") => cbor::parse_value(bytes),
					Some("application/pack") => msgpack::parse_value(bytes),
					Some("application/surrealdb") => revision::parse_value(bytes),
					_ => return Ok(Value::Bytes(crate::sql::Bytes(bytes))),
				};

				parsed.map_err(|_| Error::ApiError(ApiError::BodyDecodeFailure))
			}
		}
	}
}
