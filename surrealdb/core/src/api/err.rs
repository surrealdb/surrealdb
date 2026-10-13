//! Failures raised by the `DEFINE API` layer.
//!
//! These describe an HTTP exchange the engine drives on behalf of a defined
//! API: the request body and its framing, the headers and content types on
//! either side, the middleware chain, and the path pattern the route is
//! matched against.
//!
//! Unlike the other layer errors, these have a second public form as well as
//! the wire one: [`ApiError::status_code`] is the HTTP status an API response
//! carries. The two are decided independently and must be kept consistent with
//! each other.

// The mappers below are the only places this layer's failures become public.
// A new variant must make both decisions explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use http::StatusCode;
use surrealdb_types::{Error as TypesError, SerializationError};
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

	#[error("An unreachable error occurred: {0}")]
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
			Self::NotFound => StatusCode::NOT_FOUND,
		}
	}
}

impl LeafError for ApiError {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Self::NotFound => TypesError::not_found(message, None),
			Self::PermissionDenied => TypesError::not_allowed(message, None),
			Self::BodyDecodeFailure | Self::InvalidApiResponse(_) => {
				TypesError::serialization(message, SerializationError::Deserialization)
			}
			Self::BodyEncodeFailure => {
				TypesError::serialization(message, SerializationError::Serialization)
			}
			Self::MiddlewareFunctionNotFound {
				..
			} => TypesError::configuration(message, None),
			Self::MiddlewareRequestParseFailure {
				..
			}
			| Self::FinalActionRequestParseFailure
			| Self::InvalidRequestBody
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
			| Self::RequestBodyNotBinary
			| Self::RequestBodyTooLarge(_)
			| Self::NoOutputStrategy
			| Self::UnsupportedContentType(_) => TypesError::validation(message, None),

			// `Unreachable` is a broken invariant, so internal is the right kind.
			Self::Unreachable(_) => internal_todo(message),
		}
	}
}
