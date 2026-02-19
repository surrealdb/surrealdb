//! RPC layer error constructors using the public wire error type.
//!
//! All RPC failures are represented as [`surrealdb_types::Error`]. This module provides
//! constructor functions for the same cases that were previously [`RpcError`] variants.
//! Wire codes are set by the [`surrealdb_types::Error`] constructors.

use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, Error as TypesError, NotAllowedError,
	NotFoundError, SerializationError, ValidationError,
};
use uuid::Uuid;

use crate::api::err::ApiError;
use crate::err;
use crate::err::into_types_error;

/// Parse error (invalid message format).
pub fn parse_error() -> TypesError {
	TypesError::validation("Parse error".to_string(), ValidationError::Parse)
}

/// Too many open transactions for the session or connection.
pub fn too_many_transactions() -> TypesError {
	TypesError::validation("Too many open transactions".to_string(), ValidationError::InvalidParams)
}

/// Invalid request structure.
pub fn invalid_request() -> TypesError {
	TypesError::validation("Invalid request".to_string(), ValidationError::InvalidRequest)
}

/// Method not found.
pub fn method_not_found(name: String) -> TypesError {
	TypesError::not_found(
		"Method not found".to_string(),
		NotFoundError::Method {
			name,
		},
	)
}

/// Method not allowed.
pub fn method_not_allowed(name: String) -> TypesError {
	TypesError::not_allowed(
		"Method not allowed".to_string(),
		NotAllowedError::Method {
			name,
		},
	)
}

/// Invalid params with a custom message.
pub fn invalid_params(msg: impl Into<String>) -> TypesError {
	TypesError::validation(msg.into(), ValidationError::InvalidParams)
}

/// Internal error (wraps anyhow).
pub fn internal_error(err: anyhow::Error) -> TypesError {
	TypesError::internal(err.to_string())
}

/// Live query not supported.
pub fn lq_not_supported() -> TypesError {
	TypesError::configuration(
		"Live query not supported".to_string(),
		ConfigurationError::LiveQueryNotSupported,
	)
}

/// Bad live query config.
pub fn bad_lq_config() -> TypesError {
	TypesError::configuration(
		"Bad live query config".to_string(),
		ConfigurationError::BadLiveQueryConfig,
	)
}

/// Bad GraphQL config.
pub fn bad_gql_config() -> TypesError {
	TypesError::configuration(
		"Bad GraphQL config".to_string(),
		ConfigurationError::BadGraphqlConfig,
	)
}

/// User-thrown / database-thrown error.
pub fn thrown(msg: impl Into<String>) -> TypesError {
	TypesError::thrown(msg.into())
}

/// Serialization error.
pub fn serialize(msg: impl Into<String>) -> TypesError {
	TypesError::serialization(msg.into(), SerializationError::Serialization)
}

/// Deserialization error.
pub fn deserialize(msg: impl Into<String>) -> TypesError {
	TypesError::serialization(msg.into(), SerializationError::Deserialization)
}

/// Session not found.
pub fn session_not_found(id: Option<Uuid>) -> TypesError {
	let (message, id_str) = match id {
		Some(id) => (format!("Session not found: {id:?}"), Some(id.to_string())),
		None => ("Default session not found".to_string(), None),
	};
	TypesError::not_found(
		message,
		NotFoundError::Session {
			id: id_str,
		},
	)
}

/// Session already exists.
pub fn session_exists(id: Uuid) -> TypesError {
	TypesError::already_exists(
		format!("Session already exists: {id}"),
		AlreadyExistsError::Session {
			id: id.to_string(),
		},
	)
}

/// Session has expired (auth detail).
pub fn session_expired() -> TypesError {
	TypesError::not_allowed("The session has expired".to_string(), AuthError::SessionExpired)
}

/// Convert an anyhow error to a wire error, downcasting to preserve structured
/// error information where possible.
///
/// Tries, in order:
/// 1. `TypesError` — already a wire error, return as-is.
/// 2. `ApiError` — convert via `to_types_error()`.
/// 3. `err::Error` (core database error) — convert via `into_types_error()`.
/// 4. Fallback — wrap the display string as an internal error.
pub fn types_error_from_anyhow(error: anyhow::Error) -> TypesError {
	// If the error is already a TypesError, return it directly (preserves kind/details/cause)
	match error.downcast::<TypesError>() {
		Ok(types_error) => types_error,
		Err(error) => {
			if let Some(api_error) = error.downcast_ref::<ApiError>() {
				return api_error.to_types_error();
			}
			// Try to downcast to database Error
			error
				.downcast::<err::Error>()
				.map(into_types_error)
				.unwrap_or_else(|error| TypesError::internal(error.to_string()))
		}
	}
}
