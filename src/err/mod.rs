use axum::Json;
use axum::response::{IntoResponse, Response};
use base64::DecodeError as Base64Error;
use http::StatusCode;
use reqwest::Error as ReqwestError;
use serde::Serialize;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use serde_pack::encode::Error as PackError;
use std::io::Error as IoError;
use std::string::FromUtf8Error as Utf8Error;
use surrealdb::Error as SurrealError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("The request body contains invalid data")]
	Request,

	#[error("There was no NS header present in the request")]
	NoNsHeader,

	#[error("There was no DB header present in the request")]
	NoDbHeader,

	#[error("There was a problem with authentication")]
	InvalidAuth,

	#[error("The specified media type is unsupported")]
	InvalidType,

	#[error("There was a problem connecting with the storage engine")]
	InvalidStorage,

	#[error("The operation is unsupported")]
	OperationUnsupported,

	#[error("There was a problem with the database: {0}")]
	Db(#[from] SurrealError),

	#[error("Couldn't open the specified file: {0}")]
	Io(#[from] IoError),

	#[error("There was an error serializing to JSON: {0}")]
	Json(#[from] JsonError),

	#[error("There was an error serializing to CBOR: {0}")]
	Cbor(#[from] CborError),

	#[error("There was an error serializing to MessagePack: {0}")]
	Pack(#[from] PackError),

	#[error("There was an error with the remote request: {0}")]
	Remote(#[from] ReqwestError),
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

impl From<surrealdb::error::Db> for Error {
	fn from(error: surrealdb::error::Db) -> Error {
		Error::Db(error.into())
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
			Error::InvalidAuth => (
				StatusCode::FORBIDDEN,
				Json(Message {
					code: 403,
					details: Some("Authentication failed".to_string()),
					description: Some("Your authentication details are invalid. Reauthenticate using valid authentication parameters.".to_string()),
					information: Some(self.to_string()),
				})
			),
			Error::InvalidType => (
				StatusCode::UNSUPPORTED_MEDIA_TYPE,
				Json(Message {
					code: 415,
					details: Some("Unsupported media type".to_string()),
					description: Some("The request needs to adhere to certain constraints. Refer to the documentation for supported content types.".to_string()),
					information: None,
				}),
			),
			Error::InvalidStorage => (
				StatusCode::INTERNAL_SERVER_ERROR,
				Json(Message {
					code: 500,
					details: Some("Health check failed".to_string()),
					description: Some("The database health check for this instance failed. There was an issue with the underlying storage engine.".to_string()),
					information: Some(self.to_string()),
				}),
			),
			_ => (
				StatusCode::BAD_REQUEST,
				Json(Message {
					code: 400,
					details: Some("Request problems detected".to_string()),
					description: Some("There is a problem with your request. Refer to the documentation for further information.".to_string()),
					information: Some(self.to_string()),
				}),
			),
		}.into_response()
	}
}
