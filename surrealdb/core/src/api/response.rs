use anyhow::Context;
use http::{HeaderMap, HeaderValue, StatusCode};
use surrealdb_types::SurrealValue;

use crate::api::X_SURREAL_REQUEST_ID;
use crate::api::err::ApiError;
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

impl ApiResponse {
	/// Builds an API response from an error, exposing status and message for known API errors.
	pub(crate) fn from_error(e: anyhow::Error, request_id: String) -> Self {
		let (status, body) = if let Some(api_error) = e.downcast_ref::<ApiError>() {
			(api_error.status_code(), api_error.to_string())
		} else {
			(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
		};

		Self {
			status,
			body: PublicValue::String(body),
			request_id,
			..Default::default()
		}
	}

	/// Builds an API response from an error in a security-sensitive context (e.g. initial
	/// middleware). Known API errors (validation, not found, etc.) are converted with correct
	/// status and message. Internal/unknown errors are masked as 500 with no body to avoid leaking
	/// implementation details.
	pub(crate) fn from_error_secure(e: anyhow::Error, request_id: String) -> Self {
		if let Some(api_error) = e.downcast_ref::<ApiError>() {
			Self {
				status: api_error.status_code(),
				body: PublicValue::String(api_error.to_string()),
				request_id,
				..Default::default()
			}
		} else {
			Self {
				status: StatusCode::INTERNAL_SERVER_ERROR,
				body: PublicValue::None,
				request_id,
				..Default::default()
			}
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
	type Error = anyhow::Error;

	fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
		convert_value_to_public_value(value)
			.context("Failed to convert value to public value")?
			.into_t()
			.context("Failed to convert public value to ApiResponse")
	}
}

impl From<ApiResponse> for Value {
	fn from(value: ApiResponse) -> Self {
		convert_public_value_to_internal(value.into_value())
	}
}
