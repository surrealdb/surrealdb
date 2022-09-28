use base64::DecodeError as Base64Error;
use jsonwebtoken::errors::Error as JWTError;
use reqwest::Error as ReqwestError;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use serde_pack::encode::Error as PackError;
use std::io::Error as IoError;
use std::string::FromUtf8Error as Utf8Error;
use surrealdb::Error as DbError;
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

	#[error("There was a problem with the database: {0}")]
	Db(#[from] DbError),

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

impl warp::reject::Reject for Error {}

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

impl From<JWTError> for Error {
	fn from(_: JWTError) -> Error {
		Error::InvalidAuth
	}
}
