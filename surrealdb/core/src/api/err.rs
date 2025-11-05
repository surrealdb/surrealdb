use http::StatusCode;
use thiserror::Error;

use crate::expr::Bytesize;

#[derive(Error, Debug)]
pub enum ApiError {
	#[error("Invalid request body: Expected data frame but received another frame type")]
	InvalidRequestBody,

	#[error("Invalid request body: The body exceeded the max payload size of {0}")]
	RequestBodyTooLarge(Bytesize),

	#[error("Failed to decode the request body")]
	BodyDecodeFailure,

	#[error("Failed to encode the response body")]
	BodyEncodeFailure,

	#[error("Invalid API response: {0}")]
	InvalidApiResponse(String),

	#[error("Invalid Accept or Content-Type header")]
	InvalidFormat,

	#[error("Missing Accept or Content-Type header")]
	MissingFormat,

	#[error("An unreachable error occured: {0}")]
	Unreachable(String),
}

impl ApiError {
	pub fn status_code(&self) -> StatusCode {
		match self {
			Self::InvalidRequestBody => StatusCode::BAD_REQUEST,
			Self::RequestBodyTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
			Self::BodyDecodeFailure => StatusCode::BAD_REQUEST,
			Self::BodyEncodeFailure => StatusCode::INTERNAL_SERVER_ERROR,
			Self::InvalidApiResponse(_) => StatusCode::INTERNAL_SERVER_ERROR,
			Self::InvalidFormat => StatusCode::BAD_REQUEST,
			Self::MissingFormat => StatusCode::BAD_REQUEST,
			Self::Unreachable(_) => StatusCode::INTERNAL_SERVER_ERROR,
		}
	}
}
