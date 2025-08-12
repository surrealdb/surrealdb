#[cfg(not(target_family = "wasm"))]
use std::fmt::Display;

#[cfg(not(target_family = "wasm"))]
use bytes::Bytes;
#[cfg(not(target_family = "wasm"))]
use futures::Stream;
#[cfg(not(target_family = "wasm"))]
use futures::StreamExt;
use http::header::CONTENT_TYPE;

use super::context::InvocationContext;
use super::err::ApiError;
use super::invocation::ApiInvocation;
use crate::err::Error;
use crate::expr::Bytesize;
use crate::rpc::format::{cbor, json, revision};
use crate::val;
use crate::val::Value;

pub enum ApiBody {
	#[cfg(not(target_family = "wasm"))]
	Stream(Box<dyn Stream<Item = Result<Bytes, Box<dyn Display + Send + Sync>>> + Send + Unpin>),
	Native(Value),
}

impl ApiBody {
	#[cfg(not(target_family = "wasm"))]
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

	// The `max` variable is unused in WASM only
	#[cfg_attr(target_family = "wasm", expect(unused_variables))]
	pub async fn stream(self, max: Option<Bytesize>) -> Result<Vec<u8>, Error> {
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

	pub async fn process(
		self,
		ctx: &InvocationContext,
		invocation: &ApiInvocation,
	) -> Result<Value, Error> {
		#[cfg_attr(
			target_family = "wasm",
			expect(irrefutable_let_patterns, reason = "For WASM this is the only pattern.")
		)]
		if let ApiBody::Native(value) = self {
			let max = ctx.request_body_max.unwrap_or(Bytesize::MAX);
			let size = std::mem::size_of_val(&value);

			if size > max.0 as usize {
				return Err(ApiError::RequestBodyTooLarge(max).into());
			}

			if ctx.request_body_raw {
				Ok(value.coerce_to::<val::Bytes>()?.into())
			} else {
				Ok(value)
			}
		} else {
			let bytes = self.stream(ctx.request_body_max).await?;

			if ctx.request_body_raw {
				Ok(Value::Bytes(val::Bytes(bytes)))
			} else {
				let content_type =
					invocation.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());

				let parsed = match content_type {
					Some("application/json") => json::decode(&bytes),
					Some("application/cbor") => cbor::decode(&bytes),
					Some("application/surrealdb") => revision::decode(&bytes),
					_ => return Ok(Value::Bytes(crate::val::Bytes(bytes))),
				};

				parsed.map_err(|_| Error::ApiError(ApiError::BodyDecodeFailure))
			}
		}
	}
}
