use thiserror::Error;

use crate::err;

#[derive(Debug, Error)]
pub enum RpcError {
	#[error("Parse error")]
	ParseError,
	#[error("Invalid request")]
	InvalidRequest,
	#[error("Method not found")]
	MethodNotFound,
	#[error("Invalid params")]
	InvalidParams,
	#[error("Internal error: {0}")]
	InternalError(err::Error),
	#[error("Error: {0}")]
	Thrown(String),
}

impl From<err::Error> for RpcError {
	fn from(e: err::Error) -> Self {
		RpcError::InternalError(e)
	}
}

impl From<&str> for RpcError {
	fn from(e: &str) -> Self {
		RpcError::Thrown(e.to_string())
	}
}

impl From<RpcError> for err::Error {
	fn from(value: RpcError) -> Self {
		use err::Error;
		match value {
			RpcError::InternalError(e) => e,
			RpcError::Thrown(e) => Error::Thrown(e),
			_ => Error::Thrown(value.to_string()),
		}
	}
}
