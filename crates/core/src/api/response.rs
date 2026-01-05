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
	pub body: PublicValue,
	pub headers: HeaderMap,
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
			Some(super::format::JSON) => Format::Json,
			Some(super::format::CBOR) => Format::Cbor,
			Some(super::format::FLATBUFFERS) => Format::Flatbuffers,
			Some(_) => return Err(Error::ApiError(ApiError::InvalidFormat)),
			_ => return Err(Error::ApiError(ApiError::MissingFormat)),
		};

		Ok(ResponseInstruction::Format(format))
	}
}
