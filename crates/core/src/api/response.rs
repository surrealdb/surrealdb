use anyhow::Result;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::{HeaderMap, StatusCode};

use super::convert;
use super::err::ApiError;
use super::invocation::ApiInvocation;
use crate::err::Error;
use crate::rpc::format::Format;
use crate::val::{Object, Value};

#[derive(Debug)]
pub struct ApiResponse {
	pub raw: Option<bool>,
	pub status: StatusCode,
	pub body: Option<Value>,
	pub headers: HeaderMap,
}

impl ApiResponse {
	/// Try to create a ApiResponse from the value as it should be returned from
	/// an API action.
	pub fn from_action_result(value: Value) -> Result<Self, Error> {
		if let Value::Object(mut opts) = value {
			let raw = opts.remove("raw").map(|v| v.cast_to()).transpose()?;
			let status = opts
				.remove("status")
				.map(|v| -> Result<StatusCode, Error> {
					// Convert to int
					let v: i64 = v.coerce_to()?;

					// Convert to u16
					let v: u16 = v
						.try_into()
						.map_err(|_| Error::ArithmeticOverflow(format!("{v} as u16")))?;

					// Convert to StatusCode
					v.try_into().map_err(|_| {
						ApiError::InvalidApiResponse(format!("{v} is not a valid HTTP status code"))
							.into()
					})
				})
				.transpose()?
				.unwrap_or(StatusCode::OK);

			let headers = opts
				.remove("headers")
				.map(|v| v.coerce_to::<Object>()?.try_into())
				.transpose()?
				.unwrap_or_default();

			let body = opts.remove("body");

			if !opts.is_empty() {
				Err(ApiError::InvalidApiResponse("Contains invalid properties".into()).into())
			} else {
				Ok(Self {
					raw,
					status,
					body,
					headers,
				})
			}
		} else {
			Err(ApiError::InvalidApiResponse("Expected an object".into()).into())
		}
	}

	pub fn into_response_value(self) -> Result<Value> {
		Ok(Value::Object(Object(map! {
			"raw".to_owned() => Value::from(self.raw.unwrap_or(false)),
			"status".to_owned() => Value::from(self.status.as_u16() as i64),
			"headers".to_owned() => Value::Object(convert::headermap_to_object(self.headers)?),
			"body".to_owned(), if let Some(body) = self.body => body,
		})))
	}
}

pub enum ResponseInstruction {
	Native,
	Raw,
	Format(Format),
}

impl ResponseInstruction {
	pub fn for_format(invocation: &ApiInvocation) -> Result<Self, Error> {
		let mime = invocation
			.headers
			.get(ACCEPT)
			.or_else(|| invocation.headers.get(CONTENT_TYPE))
			.and_then(|v| v.to_str().ok());

		let format = match mime {
			Some("application/json") => Format::Json,
			Some("application/cbor") => Format::Cbor,
			Some("application/surrealdb") => Format::Revision,
			Some(_) => return Err(Error::ApiError(ApiError::InvalidFormat)),
			_ => return Err(Error::ApiError(ApiError::MissingFormat)),
		};

		Ok(ResponseInstruction::Format(format))
	}
}
