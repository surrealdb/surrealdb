use anyhow::Result;
use http::header::{ACCEPT, CONTENT_TYPE};
use http::{HeaderMap, StatusCode};
use surrealdb_types::SurrealValue;

use super::err::ApiError;
use super::invocation::ApiInvocation;
use crate::err::Error;
use crate::rpc::format::Format;
use crate::types::PublicValue;

#[derive(Debug, SurrealValue)]
pub struct ApiResponse {
	pub raw: Option<bool>,
	pub status: StatusCode,
	pub body: Option<PublicValue>,
	pub headers: HeaderMap,
}

impl ApiResponse {
	/// Try to create a ApiResponse from the value as it should be returned from
	/// an API action.
	pub fn from_action_result(mut opts: PublicValue) -> anyhow::Result<Self> {
		let raw =
			opts.remove("raw").into_option::<PublicValue>()?.map(|v| v.into_bool()).transpose()?;
		let status = StatusCode::from_value(opts.remove("status"))?;

		let headers = HeaderMap::from_value(opts.remove("headers"))?;

		let body = opts.remove("body").into_option()?;

		if !opts.is_empty() {
			return Err(ApiError::InvalidApiResponse("Contains invalid properties".into()).into());
		}

		Ok(Self {
			raw,
			status,
			body,
			headers,
		})
	}
}

pub enum ResponseInstruction {
	Native,
	Raw,
	Format(Format),
}

impl ResponseInstruction {
	pub(crate) fn for_format(invocation: &ApiInvocation) -> Result<Self, Error> {
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
