use crate::cli::abstraction::auth::Error as SurrealAuthError;
use axum::response::{IntoResponse, Response};
use axum::Error as AxumError;
use axum::Json;
use base64::DecodeError as Base64Error;
use http::{HeaderName, StatusCode};
use opentelemetry::global::Error as OpentelemetryError;
use reqwest::Error as ReqwestError;
use serde::Serialize;
use std::io::Error as IoError;
use std::string::FromUtf8Error as Utf8Error;
use surrealdb::error::Db as SurrealDbError;
use surrealdb::iam::Error as SurrealIamError;
use surrealdb::Error as SurrealError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("The request body contains invalid data")]
	Request,

	#[error("No namespace was provided in the request")]
	NoNamespace,

	#[error("No database was provided in the request")]
	NoDatabase,

	#[error("There was a problem with authentication")]
	InvalidAuth,

	#[error("The specified media type is unsupported")]
	InvalidType,

	#[error("There was a problem connecting with the storage engine")]
	InvalidStorage,

	#[error("The operation is unsupported")]
	OperationUnsupported,

	#[error("There was a problem parsing the header {0}: {1}")]
	InvalidHeader(HeaderName, String),

	#[error("There was a problem with the database: {0}")]
	Db(#[from] SurrealError),

	#[error("Couldn't open the specified file: {0}")]
	Io(#[from] IoError),

	#[error("There was an error with the network: {0}")]
	Axum(#[from] AxumError),

	#[error("There was an error with JSON serialization: {0}")]
	Json(String),

	#[error("There was an error with CBOR serialization: {0}")]
	Cbor(String),

	#[error("There was an error with MessagePack serialization: {0}")]
	Pack(String),

	#[error("There was an error with the remote request: {0}")]
	Remote(#[from] ReqwestError),

	#[error("There was an error with auth: {0}")]
	Auth(#[from] SurrealAuthError),

	#[error("There was an error with opentelemetry: {0}")]
	Otel(#[from] OpentelemetryError),

	/// Statement has been deprecated
	#[error("{0}")]
	Other(String),

	#[error("The HTTP route '{0}' is forbidden")]
	ForbiddenRoute(String),
}

impl From<Error> for String {
	fn from(e: Error) -> String {
		e.to_string()
	}
}

impl From<Base64Error> for Error {
	fn from(_: Base64Error) -> Error {
		Error::InvalidAuth
	}
}

impl From<Utf8Error> for Error {
	fn from(_: Utf8Error) -> Error {
		Error::InvalidAuth
	}
}

impl From<serde_json::Error> for Error {
	fn from(e: serde_json::Error) -> Error {
		Error::Json(e.to_string())
	}
}

impl From<serde_pack::encode::Error> for Error {
	fn from(e: serde_pack::encode::Error) -> Error {
		Error::Pack(e.to_string())
	}
}

impl From<serde_pack::decode::Error> for Error {
	fn from(e: serde_pack::decode::Error) -> Error {
		Error::Pack(e.to_string())
	}
}

impl From<ciborium::value::Error> for Error {
	fn from(e: ciborium::value::Error) -> Error {
		Error::Cbor(format!("{e}"))
	}
}

impl From<opentelemetry::logs::LogError> for Error {
	fn from(e: opentelemetry::logs::LogError) -> Error {
		Error::Otel(OpentelemetryError::Log(e))
	}
}

impl From<opentelemetry::trace::TraceError> for Error {
	fn from(e: opentelemetry::trace::TraceError) -> Error {
		Error::Otel(OpentelemetryError::Trace(e))
	}
}

impl From<opentelemetry::metrics::MetricsError> for Error {
	fn from(e: opentelemetry::metrics::MetricsError) -> Error {
		Error::Otel(OpentelemetryError::Metric(e))
	}
}

impl<T: std::fmt::Debug> From<ciborium::de::Error<T>> for Error {
	fn from(e: ciborium::de::Error<T>) -> Error {
		Error::Cbor(format!("{e}"))
	}
}

impl<T: std::fmt::Debug> From<ciborium::ser::Error<T>> for Error {
	fn from(e: ciborium::ser::Error<T>) -> Error {
		Error::Cbor(format!("{e}"))
	}
}

impl From<surrealdb::error::Db> for Error {
	fn from(error: surrealdb::error::Db) -> Error {
		if matches!(error, surrealdb::error::Db::InvalidAuth) {
			return Error::InvalidAuth;
		}
		Error::Db(error.into())
	}
}

impl From<surrealdb::rpc::RpcError> for Error {
	fn from(value: surrealdb::rpc::RpcError) -> Self {
		use surrealdb::rpc::RpcError;
		match value {
			RpcError::InternalError(e) => Error::Db(surrealdb::Error::Db(e)),
			RpcError::Thrown(e) => Error::Other(e),
			_ => Error::Other(value.to_string()),
		}
	}
}

impl Serialize for Error {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.to_string().as_str())
	}
}

#[derive(Serialize)]
pub(super) struct Message {
	code: u16,
	details: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	information: Option<String>,
}

impl IntoResponse for Error {
	fn into_response(self) -> Response {
		match self {
			err @ Error::InvalidAuth | err @ Error::Db(SurrealError::Db(SurrealDbError::InvalidAuth)) => (
				StatusCode::UNAUTHORIZED,
				Json(Message {
					code: StatusCode::UNAUTHORIZED.as_u16(),
					details: Some("Authentication failed".to_string()),
					description: Some("Your authentication details are invalid. Reauthenticate using valid authentication parameters.".to_string()),
					information: Some(err.to_string()),
				})
			),
			err @ Error::ForbiddenRoute(_) | err @ Error::Db(SurrealError::Db(SurrealDbError::IamError(SurrealIamError::NotAllowed { .. }))) => (
				StatusCode::FORBIDDEN,
				Json(Message {
					code: StatusCode::FORBIDDEN.as_u16(),
					details: Some("Forbidden".to_string()),
					description: Some("Not allowed to do this.".to_string()),
					information: Some(err.to_string()),
				})
			),
			Error::InvalidType => (
				StatusCode::UNSUPPORTED_MEDIA_TYPE,
				Json(Message {
					code: StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
					details: Some("Unsupported media type".to_string()),
					description: Some("The request needs to adhere to certain constraints. Refer to the documentation for supported content types.".to_string()),
					information: None,
				}),
			),
			Error::InvalidStorage => (
				StatusCode::INTERNAL_SERVER_ERROR,
				Json(Message {
					code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
					details: Some("Health check failed".to_string()),
					description: Some("The database health check for this instance failed. There was an issue with the underlying storage engine.".to_string()),
					information: Some(self.to_string()),
				}),
			),
			_ => (
				StatusCode::BAD_REQUEST,
				Json(Message {
					code: StatusCode::BAD_REQUEST.as_u16(),
					details: Some("Request problems detected".to_string()),
					description: Some("There is a problem with your request. Refer to the documentation for further information.".to_string()),
					information: Some(self.to_string()),
				}),
			),
		}.into_response()
	}
}
