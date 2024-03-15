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
