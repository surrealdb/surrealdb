use thiserror::Error;

use crate::err;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RpcError {
	#[error("Parse error")]
	ParseError,
	#[error("Invalid request")]
	InvalidRequest,
	#[error("Method not found")]
	MethodNotFound,
	#[error("Method not allowed")]
	MethodNotAllowed,
	#[error("Invalid params")]
	InvalidParams,
	#[error("There was a problem with the database: {0}")]
	InternalError(err::Error),
	#[error("Live Query was made, but is not supported")]
	LqNotSuported,
	#[error("RT is enabled for the session, but LQ is not supported with the context")]
	BadLQConfig,
	#[error("Error: {0}")]
	Thrown(String),
}

impl From<err::Error> for RpcError {
	fn from(e: err::Error) -> Self {
		use err::Error;
		match e {
			Error::RealtimeDisabled => RpcError::LqNotSuported,
			_ => RpcError::InternalError(e),
		}
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
