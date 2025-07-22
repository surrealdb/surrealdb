use anyhow::Context;
use http::{
	HeaderMap, StatusCode,
	header::{ACCEPT, CONTENT_TYPE},
};

use crate::{
	err::Error,
	expr::{Object, Value},
	rpc::{V1Object, V1Value, format::Format},
};

use super::{convert, err::ApiError, invocation::ApiInvocation};

#[derive(Debug)]
pub struct ApiResponse {
	pub raw: Option<bool>,
	pub status: StatusCode,
	pub body: Option<V1Value>,
	pub headers: HeaderMap,
}

impl TryFrom<V1Value> for ApiResponse {
	type Error = Error;
	fn try_from(value: V1Value) -> Result<Self, Self::Error> {
		if let V1Value::Object(mut opts) = value {
			let raw = opts
				.remove("raw")
				.map(|v| {
					let v = Value::try_from(v)
						.map_err(|err| Error::Thrown(format!("Invalid raw value: {err}")))?;
					v.cast_to().map_err(|err| Error::Thrown(format!("Invalid raw value: {err}")))
				})
				.transpose()?;
			let status = opts
				.remove("status")
				.map(|v| -> Result<StatusCode, Error> {
					let v = Value::try_from(v)
						.map_err(|err| Error::Thrown(format!("Invalid status value: {err}")))?;
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
				.map(|v| {
					let v = Value::try_from(v)
						.map_err(|err| Error::Thrown(format!("Invalid headers value: {err}")))?;
					v.coerce_to::<Object>()?.try_into()
				})
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
}

impl TryFrom<ApiResponse> for V1Value {
	type Error = anyhow::Error;

	fn try_from(response: ApiResponse) -> Result<V1Value, Self::Error> {
		Ok(V1Value::Object(V1Object(map! {
			"raw".to_string() => V1Value::from(response.raw.unwrap_or(false)),
			"status".to_string() => V1Value::from(response.status.as_u16() as i64),
			"headers".to_string() => V1Value::Object(convert::headermap_to_object(response.headers).context("Invalid headers value")?),
			"body".to_string(), if let Some(body) = response.body => body,
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
