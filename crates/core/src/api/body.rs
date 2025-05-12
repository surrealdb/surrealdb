use super::context::InvocationContext;
use super::err::ApiError;
use super::invocation::ApiInvocation;
use crate::ctx::Context;
use crate::err::Error;
use crate::rpc::format::cbor;
use crate::rpc::format::json;
use crate::rpc::format::revision;
use crate::sql;
use crate::sql::stream::Stream;
use crate::sql::Bytesize;
use crate::sql::Value;
use http::header::CONTENT_TYPE;

pub struct ApiBody {
	pub body: Value,
	pub native: bool,
}

impl ApiBody {
	pub async fn process(
		self,
		ctx: &Context,
		inv_ctx: &InvocationContext,
		invocation: &ApiInvocation,
	) -> Result<Value, Error> {
		if inv_ctx.request_body_raw {
			if matches!(self.body, Value::Stream(_)) {
				Ok(self.body)
			} else {
				let bytes = self.body.coerce_to::<sql::Bytes>()?;
				Ok(Value::Stream(Stream::from_bytes(ctx, bytes)?))
			}
		} else {
			let max = inv_ctx.request_body_max.unwrap_or(Bytesize::MAX);
			let bytes = match self.body {
				Value::Stream(stream) => match stream.consume_bytes(ctx, Some(max)).await {
					Err(Error::StreamTooLarge(max)) => {
						return Err(ApiError::RequestBodyTooLarge(max).into())
					}
					x => x,
				}?,
				Value::Bytes(bytes) => {
					if bytes.len() > max.0 as usize {
						return Err(ApiError::RequestBodyTooLarge(max).into());
					}

					bytes.0
				}
				value => {
					let size = std::mem::size_of_val(&value);
					if size > max.0 as usize {
						return Err(ApiError::RequestBodyTooLarge(max).into());
					}

					return Ok(value);
				}
			};

			let content_type = invocation.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());

			let parsed = match content_type {
				Some("application/json") => json::parse_value(&bytes),
				Some("application/cbor") => cbor::parse_value(bytes),
				Some("application/surrealdb") => revision::parse_value(bytes),
				_ => return Ok(Value::Bytes(crate::sql::Bytes(bytes))),
			};

			parsed.map_err(|_| Error::ApiError(ApiError::BodyDecodeFailure))
		}
	}
}
