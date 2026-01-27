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

	// Status code errors
	#[error("Invalid HTTP status code: {0}. Must be between 100 and 599")]
	InvalidStatusCode(i64),

	// Header errors
	#[error("Invalid header name: {0}")]
	InvalidHeaderName(String),

	#[error("Invalid header value for {name}: {value}")]
	InvalidHeaderValue {
		name: String,
		value: String,
	},

	#[error("Header value contains invalid characters: {0}")]
	HeaderInjectionAttempt(String),

	// Content type errors
	#[error("Missing required Content-Type header")]
	MissingContentType,

	#[error("Unsupported Content-Type: {0}")]
	UnsupportedContentType(String),

	#[error("Expected Content-Type to be {0}")]
	InvalidContentType(String),

	#[error("No output strategy was possible for this API request")]
	NoOutputStrategy,

	// Request/Response errors
	#[error("Invalid request body: Expected {expected} but received {actual}")]
	InvalidRequestBodyType {
		expected: String,
		actual: String,
	},

	#[error("Failed to parse request in middleware: {middleware}")]
	MiddlewareRequestParseFailure {
		middleware: String,
	},

	#[error("Failed to resolve middleware function: {function}")]
	MiddlewareFunctionNotFound {
		function: String,
	},

	#[error("Failed to parse request in final action handler")]
	FinalActionRequestParseFailure,

	// Body parsing errors
	#[error("Request body must be binary data")]
	RequestBodyNotBinary,

	#[error("Permission denied: You are not allowed to access this resource")]
	PermissionDenied,
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
			Self::InvalidStatusCode(_) => StatusCode::BAD_REQUEST,
			Self::InvalidHeaderName(_) => StatusCode::BAD_REQUEST,
			Self::InvalidHeaderValue {
				..
			} => StatusCode::BAD_REQUEST,
			Self::HeaderInjectionAttempt(_) => StatusCode::BAD_REQUEST,
			Self::MissingContentType => StatusCode::BAD_REQUEST,
			Self::UnsupportedContentType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
			Self::InvalidContentType(_) => StatusCode::BAD_REQUEST,
			Self::NoOutputStrategy => StatusCode::NOT_ACCEPTABLE,
			Self::InvalidRequestBodyType {
				..
			} => StatusCode::BAD_REQUEST,
			Self::MiddlewareRequestParseFailure {
				..
			} => StatusCode::BAD_REQUEST,
			Self::MiddlewareFunctionNotFound {
				..
			} => StatusCode::INTERNAL_SERVER_ERROR,
			Self::FinalActionRequestParseFailure => StatusCode::BAD_REQUEST,
			Self::RequestBodyNotBinary => StatusCode::BAD_REQUEST,
			Self::PermissionDenied => StatusCode::FORBIDDEN,
		}
	}
}
