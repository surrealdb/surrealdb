use thiserror::Error;

use crate::err;
use crate::rpc::DbResultError;

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
		// First, try to downcast to our Error type
		if let Some(err) = e.downcast_ref::<Error>() {
			match err {
				// Live query specific error
				Error::RealtimeDisabled => return RpcError::LqNotSuported,
				// User-facing errors should be "Thrown" not "Internal"
				Error::IdMismatch {
					..
				} => return RpcError::Thrown(err.to_string()),
				// Most other database errors are also user-facing
				_ => return RpcError::Thrown(err.to_string()),
			}
		}
		// For errors that aren't database errors, treat as internal
		RpcError::InternalError(e)
	}
}

impl From<DbResultError> for RpcError {
	fn from(e: DbResultError) -> Self {
		RpcError::InternalError(anyhow::anyhow!(e.to_string()))
	}
}

impl From<&str> for RpcError {
	fn from(e: &str) -> Self {
		RpcError::Thrown(e.to_string())
	}
}
