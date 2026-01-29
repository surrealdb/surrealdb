use std::error::Error as StdError;

use axum::Json;
use axum::response::{IntoResponse, Response};
use http::{HeaderName, HeaderValue, StatusCode};
use serde::{Serialize, Serializer};
use surrealdb_core::api::X_SURREAL_REQUEST_ID;
use surrealdb_core::api::err::ApiError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("The server is unable to handle the request")]
	ServerOverloaded,

	#[error("The request body contains invalid data")]
	Request,

	#[error("There was a problem with authentication")]
	InvalidAuth,

	#[error("The specified media type is unsupported")]
	InvalidType,

	#[error("There was a problem connecting with the storage engine")]
	InvalidStorage,

	#[error("There was an error with the remote request: {0}")]
	Remote(#[from] reqwest::Error),

	#[error("The HTTP route '{0}' is forbidden")]
	ForbiddenRoute(String),

	#[error("The HTTP route '{0}' is not found")]
	NotFound(String),

	#[error("There was a problem parsing the header {0}: {1}")]
	InvalidHeader(HeaderName, String),
}

impl IntoResponse for Error {
	fn into_response(self) -> Response {
		match self {
			Error::ForbiddenRoute(_) => {
				ErrorMessage{
					code: StatusCode::FORBIDDEN,
					details: Some("Forbidden".to_string()),
					description: Some("Not allowed to do this.".to_string()),
					information: Some(self.to_string())
				}.into_response()
			}
			Error::InvalidAuth => {
				ErrorMessage{
					code: StatusCode::UNAUTHORIZED,
					details: Some("Authentication failed".to_string()),
					description: Some("Your authentication details are invalid. Reauthenticate using valid authentication parameters.".to_string()),
					information: Some("There was a problem with authentication".to_string())
				}.into_response()
			}
			Error::InvalidType => {
				ErrorMessage {
					code: StatusCode::UNSUPPORTED_MEDIA_TYPE,
					details: Some("Unsupported media type".to_string()),
					description: Some("The request needs to adhere to certain constraints. Refer to the documentation for supported content types.".to_string()),
					information: None,
				}.into_response()
			}
			Error::NotFound(_) => {
				ErrorMessage {
					code: StatusCode::NOT_FOUND,
					details: Some("Not found".to_string()),
					description: Some("The request was made to an endpoint which does not exist.".to_string()),
					information: Some(self.to_string()),
				}.into_response()
			}
			Error::InvalidStorage =>
				ErrorMessage {
					code: StatusCode::INTERNAL_SERVER_ERROR,
					details: Some("Health check failed".to_string()),
					description: Some("The database health check for this instance failed. There was an issue with the underlying storage engine.".to_string()),
					information: Some(self.to_string()),
				}.into_response(),
			_ => ErrorMessage {
				code: StatusCode::BAD_REQUEST,
				details: Some("Request problems dectected".to_string()),
				description: Some("There is a problem with your request. Refer to the documentation for further information.".to_string()),
				information: Some(format!("{self}")),
			}.into_response()
		}
	}
}

#[derive(Serialize)]
pub(super) struct ErrorMessage {
	#[serde(serialize_with = "serialize_status_code")]
	code: StatusCode,
	details: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	information: Option<String>,
}

fn serialize_status_code<S>(code: &StatusCode, s: S) -> Result<S::Ok, S::Error>
where
	S: Serializer,
{
	code.as_u16().serialize(s)
}

impl IntoResponse for ErrorMessage {
	fn into_response(self) -> Response {
		(self.code, Json(self)).into_response()
	}
}

/// Wrapper around anyhow error which implements [`IntoResponse`]
pub struct ResponseError(pub anyhow::Error);

impl<E: StdError + Send + Sync + 'static> From<E> for ResponseError {
	fn from(value: E) -> Self {
		ResponseError(anyhow::Error::new(value))
	}
}

impl IntoResponse for ResponseError {
	fn into_response(self) -> Response {
		// Check for ApiError first since it's public
		if let Some(e) = self.0.downcast_ref::<ApiError>() {
			return ErrorMessage {
				code: e.status_code(),
				details: Some("An error occurred while processing this API request".to_string()),
				description: Some(e.to_string()),
				information: None,
			}
			.into_response();
		}

		// Check for our local Error type
		if self.0.is::<Error>() {
			match self.0.downcast::<Error>() {
				Ok(e) => {
					return e.into_response();
				}
				Err(e) => {
					return ErrorMessage {
						code: StatusCode::INTERNAL_SERVER_ERROR,
						details: Some(
							"An error occurred while processing this API request".to_string(),
						),
						description: Some(e.to_string()),
						information: None,
					}
					.into_response();
				}
			}
		}

		// Handle errors based on their string representation
		let error_str = self.0.to_string();

		let error_str_lower = error_str.to_lowercase();

		// Check for authentication errors
		if error_str_lower.contains("authentication") || error_str_lower.contains("invalidauth") {
			return ErrorMessage{
				code: StatusCode::UNAUTHORIZED,
				details: Some("Authentication failed".to_string()),
				description: Some("Your authentication details are invalid. Reauthenticate using valid authentication parameters.".to_string()),
				information: Some("There was a problem with authentication".to_string())
			}.into_response();
		}

		// Check for forbidden/not allowed errors
		if error_str_lower.contains("notallowed")
			|| error_str_lower.contains("not allowed")
			|| error_str_lower.contains("forbidden")
			|| error_str_lower.contains("not enough permissions")
		{
			return ErrorMessage {
				code: StatusCode::FORBIDDEN,
				details: Some("Forbidden".to_string()),
				description: Some("Not allowed to do this.".to_string()),
				information: Some(error_str),
			}
			.into_response();
		}

		// Default error response
		ErrorMessage {
			code: StatusCode::BAD_REQUEST,
			details: Some("Request problems dectected".to_string()),
			description: Some("There is a problem with your request. Refer to the documentation for further information.".to_string()),
			information: Some(error_str),
		}.into_response()
	}
}

/// Error wrapper for the API HTTP handler that attaches a request ID to the response
/// so all errors (including those before invocation) can be traced.
pub(super) struct ApiHandlerError(pub ResponseError, pub String);

impl IntoResponse for ApiHandlerError {
	fn into_response(self) -> Response {
		let mut response = self.0.into_response();
		if !self.1.is_empty()
			&& let Ok(value) = HeaderValue::from_str(&self.1)
		{
			response.headers_mut().insert(X_SURREAL_REQUEST_ID, value);
		}
		response
	}
}
