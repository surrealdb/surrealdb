use http::{HeaderMap, HeaderValue, StatusCode};
use surrealdb_types::{Error, ErrorKind, NotAllowedError, SurrealValue};

use crate::api::X_SURREAL_REQUEST_ID;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::{PublicObject, PublicValue};
use crate::val::{Value, convert_value_to_public_value};

#[derive(Debug, Default, SurrealValue)]
#[surreal(default)]
pub struct ApiResponse {
	pub status: StatusCode,
	pub body: PublicValue,
	pub headers: HeaderMap,
	pub context: PublicObject,
	/// Server-generated request ID for tracing and logging
	pub request_id: String,
}

/// Maps public error kind to HTTP status code for API responses.
fn status_code_for_error(error: &Error) -> StatusCode {
	match error.kind() {
		ErrorKind::Validation => StatusCode::BAD_REQUEST,
		ErrorKind::NotFound => StatusCode::NOT_FOUND,
		ErrorKind::NotAllowed => match error.not_allowed_details() {
			Some(NotAllowedError::Auth(_)) => StatusCode::UNAUTHORIZED,
			_ => StatusCode::FORBIDDEN,
		},
		ErrorKind::Configuration
		| ErrorKind::Thrown
		| ErrorKind::Query
		| ErrorKind::Serialization
		| ErrorKind::AlreadyExists
		| ErrorKind::Connection
		| ErrorKind::Internal
		| _ => StatusCode::INTERNAL_SERVER_ERROR,
	}
}

impl ApiResponse {
	/// Builds an API response from an error, exposing status and message from the error kind.
	pub(crate) fn from_error(error: Error, request_id: String) -> Self {
		let status = status_code_for_error(&error);
		let body = error.message().to_string();
		Self {
			status,
			body: PublicValue::String(body),
			request_id,
			..Default::default()
		}
	}

	/// Builds an API response from an error in a security-sensitive context (e.g. initial
	/// middleware). Status and message are derived from the error kind. Internal/unknown errors
	/// are masked as 500 with no body to avoid leaking implementation details.
	pub(crate) fn from_error_secure(error: Error, request_id: String) -> Self {
		let status = status_code_for_error(&error);
		let body = match status {
			StatusCode::INTERNAL_SERVER_ERROR => PublicValue::None,
			_ => PublicValue::String(error.message().to_string()),
		};
		Self {
			status,
			body,
			request_id,
			..Default::default()
		}
	}

	/// Ensures the X-Surreal-Request-ID header is present in the response headers.
	/// Uses the request_id field from the struct.
	pub(crate) fn ensure_request_id_header(&mut self) {
		if !self.headers.contains_key(X_SURREAL_REQUEST_ID)
			&& !self.request_id.is_empty()
			&& let Ok(header_value) = HeaderValue::from_str(&self.request_id)
		{
			self.headers.insert(X_SURREAL_REQUEST_ID, header_value);
		}
	}
}

impl TryFrom<Value> for ApiResponse {
	type Error = Error;

	fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
		convert_value_to_public_value(value).map_err(|e| Error::internal(e.to_string()))?.into_t()
	}
}

impl From<ApiResponse> for Value {
	fn from(value: ApiResponse) -> Self {
		convert_public_value_to_internal(value.into_value())
	}
}
