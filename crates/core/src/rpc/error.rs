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
	#[error("Invalid params: {0}")]
	InvalidParams(String),
	#[error("There was a problem with the database: {0}")]
	InternalError(anyhow::Error),
	#[error("Live Query was made, but is not supported")]
	LqNotSuported,
	#[error("RT is enabled for the session, but LQ is not supported by the context")]
	BadLQConfig,
	#[error("A GraphQL request was made, but GraphQL is not supported by the context")]
	BadGQLConfig,
	#[error("Error: {0}")]
	Thrown(String),
	#[error("Could not serialize surreal value: {0}")]
	Serialize(String),
	#[error("Could not deserialize surreal value: {0}")]
	Deserialize(String),
}

impl From<anyhow::Error> for RpcError {
	fn from(e: anyhow::Error) -> Self {
		use err::Error;
		match e.downcast_ref() {
			Some(Error::RealtimeDisabled) => RpcError::LqNotSuported,
			_ => RpcError::InternalError(e),
		}
	}
}

impl From<&str> for RpcError {
	fn from(e: &str) -> Self {
		RpcError::Thrown(e.to_string())
	}
}
