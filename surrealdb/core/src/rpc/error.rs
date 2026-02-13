//! RPC layer error constructors using the public wire error type.
//!
//! All RPC failures are represented as [`surrealdb_types::Error`]. This module provides
//! constructor functions for the same cases that were previously [`RpcError`] variants.
//! Each sets the wire `code` for backwards compatibility.

use surrealdb_types::code;
use surrealdb_types::{AuthError, Error as TypesError, ErrorKind as TypesErrorKind};
use uuid::Uuid;

use crate::err;

/// Parse error (invalid message format).
pub fn parse_error() -> TypesError {
	TypesError::new(TypesErrorKind::Validation, "Parse error").with_code(code::PARSE_ERROR)
}

/// Invalid request structure.
pub fn invalid_request() -> TypesError {
	TypesError::new(TypesErrorKind::Validation, "Invalid request").with_code(code::INVALID_REQUEST)
}

/// Method not found.
pub fn method_not_found() -> TypesError {
	TypesError::new(TypesErrorKind::Method, "Method not found").with_code(code::METHOD_NOT_FOUND)
}

/// Method not allowed.
pub fn method_not_allowed() -> TypesError {
	TypesError::new(TypesErrorKind::Method, "Method not allowed")
		.with_code(code::METHOD_NOT_ALLOWED)
}

/// Invalid params with a custom message.
pub fn invalid_params(msg: impl Into<String>) -> TypesError {
	TypesError::new(TypesErrorKind::Validation, msg).with_code(code::INVALID_PARAMS)
}

/// Internal error (wraps anyhow).
pub fn internal_error(err: anyhow::Error) -> TypesError {
	TypesError::new(TypesErrorKind::Internal, err.to_string()).with_code(code::INTERNAL_ERROR)
}

/// Live query not supported.
pub fn lq_not_supported() -> TypesError {
	TypesError::new(TypesErrorKind::Configuration, "Live query not supported")
		.with_code(code::LIVE_QUERY_NOT_SUPPORTED)
}

/// Bad live query config.
pub fn bad_lq_config() -> TypesError {
	TypesError::new(TypesErrorKind::Configuration, "Bad live query config")
		.with_code(code::BAD_LIVE_QUERY_CONFIG)
}

/// Bad GraphQL config.
pub fn bad_gql_config() -> TypesError {
	TypesError::new(TypesErrorKind::Configuration, "Bad GraphQL config")
		.with_code(code::BAD_GRAPHQL_CONFIG)
}

/// User-thrown / database-thrown error.
pub fn thrown(msg: impl Into<String>) -> TypesError {
	TypesError::new(TypesErrorKind::Thrown, msg).with_code(code::THROWN)
}

/// Serialization error.
pub fn serialize(msg: impl Into<String>) -> TypesError {
	TypesError::new(TypesErrorKind::Serialization, msg).with_code(code::SERIALIZATION_ERROR)
}

/// Deserialization error.
pub fn deserialize(msg: impl Into<String>) -> TypesError {
	TypesError::new(TypesErrorKind::Serialization, msg).with_code(code::DESERIALIZATION_ERROR)
}

/// Session not found.
pub fn session_not_found(id: Option<Uuid>) -> TypesError {
	let message = match id {
		Some(id) => format!("Session not found: {id:?}"),
		None => "Default session not found".to_string(),
	};
	TypesError::new(TypesErrorKind::NotFound, message)
}

/// Session already exists.
pub fn session_exists(id: Uuid) -> TypesError {
	TypesError::new(TypesErrorKind::AlreadyExists, format!("Session already exists: {id}"))
}

/// Session has expired (auth detail).
pub fn session_expired() -> TypesError {
	TypesError::new(TypesErrorKind::Auth, "The session has expired")
		.with_code(code::INTERNAL_ERROR)
		.with_details(
		AuthError {
			session_expired: true,
			..Default::default()
		}
		.into_details(),
	)
}

/// Convert an anyhow error to a wire error, downcasting to database errors where possible.
pub fn types_error_from_anyhow(e: anyhow::Error) -> TypesError {
	if let Some(err) = e.downcast_ref::<err::Error>() {
		match err {
			err::Error::RealtimeDisabled => return lq_not_supported(),
			err::Error::IdMismatch {
				..
			} => return thrown(err.to_string()),
			_ => return thrown(err.to_string()),
		}
	}
	internal_error(e)
}
