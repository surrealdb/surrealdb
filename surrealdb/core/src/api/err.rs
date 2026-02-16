use http::StatusCode;
use surrealdb_types::Error as TypesError;
use thiserror::Error;

use crate::api::response::status_code_for_error;
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

	#[error("Not found")]
	NotFound,
}

impl ApiError {
	pub fn status_code(&self) -> StatusCode {
		status_code_for_error(&self.to_types_error())
	}

	pub(crate) fn to_types_error(&self) -> TypesError {
		let msg = self.to_string();
		match &self {
			Self::NotFound => TypesError::not_found(msg, None),
			Self::PermissionDenied => TypesError::not_allowed(msg, None),
			Self::MiddlewareRequestParseFailure {
				..
			}
			| Self::FinalActionRequestParseFailure
			| Self::InvalidRequestBody
			| Self::BodyDecodeFailure
			| Self::InvalidFormat
			| Self::MissingFormat
			| Self::InvalidStatusCode(_)
			| Self::InvalidHeaderName(_)
			| Self::InvalidHeaderValue {
				..
			}
			| Self::HeaderInjectionAttempt(_)
			| Self::MissingContentType
			| Self::InvalidContentType(_)
			| Self::InvalidRequestBodyType {
				..
			}
			| Self::RequestBodyNotBinary => TypesError::validation(msg, None),
			_ => TypesError::internal(msg),
		}
	}

	pub fn into_types_error(self) -> TypesError {
		self.to_types_error()
	}
}
