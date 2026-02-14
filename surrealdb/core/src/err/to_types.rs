//! Conversion from core [`Error`] to wire-friendly [`surrealdb_types::Error`].
//!
//! This is the single place that defines how embedded database errors are mapped to the
//! public types-layer error used over RPC and in the SDK.

use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, Error as TypesError, MethodError,
	NotFoundError, QueryError, SerializationError, ToSql,
};

use crate::err::Error;

/// Converts a core database error to the public wire-friendly error type.
///
/// Use this whenever a core `Error` crosses an API boundary (e.g. query execution result,
/// session attachment, variable attachment). For `anyhow::Error`, downcast first:
/// `e.downcast_ref::<Error>().map(to_types_error).unwrap_or_else(|| TypesError::internal(e.to_string()))`.
#[must_use]
pub fn to_types_error(e: &Error) -> TypesError {
	use Error::*;
	match e {
		// Auth
		ExpiredSession => TypesError::auth(
			"The session has expired".to_string(),
			Some(AuthError::SessionExpired),
		),
		ExpiredToken => TypesError::auth(
			"The token has expired".to_string(),
			Some(AuthError::TokenExpired),
		),
		InvalidAuth => TypesError::auth("Authentication failed".to_string(), None),
		UnexpectedAuth => {
			TypesError::auth("Unexpected authentication error".to_string(), None)
		}
		MissingUserOrPass => {
			TypesError::auth("Missing username or password".to_string(), None)
		}
		NoSigninTarget => {
			TypesError::auth("No signin target specified".to_string(), None)
		}
		InvalidPass => TypesError::auth("Invalid password".to_string(), None),
		TokenMakingFailed => {
			TypesError::auth("Failed to create authentication token".to_string(), None)
		}
		IamError(iam_err) => TypesError::auth(format!("IAM error: {iam_err}"), None),
		InvalidSignup => TypesError::auth("Signup failed".to_string(), None),

		// Validation
		NsEmpty => TypesError::validation("No namespace specified".to_string(), None),
		DbEmpty => TypesError::validation("No database specified".to_string(), None),
		InvalidQuery(_) => TypesError::validation("Invalid query syntax".to_string(), None),
		InvalidParam { .. } => {
			TypesError::validation("Invalid query variables".to_string(), None)
		}
		InvalidContent { .. } => {
			TypesError::validation("Invalid content clause".to_string(), None)
		}
		InvalidMerge { .. } => TypesError::validation("Invalid merge clause".to_string(), None),
		InvalidPatch(_) => {
			TypesError::validation("Invalid patch operation".to_string(), None)
		}
		Coerce(_) => TypesError::validation("Type coercion error".to_string(), None),
		Cast(_) => TypesError::validation("Type casting error".to_string(), None),
		TryAdd(..) | TrySub(..) | TryMul(..) | TryDiv(..) | TryRem(..) | TryPow(..) | TryNeg(_) => {
			TypesError::validation("Arithmetic operation error".to_string(), None)
		}
		TryFrom(..) => TypesError::validation("Type conversion error".to_string(), None),
		DuplicatedMatchRef { .. } => {
			TypesError::validation("Duplicated match reference".to_string(), None)
		}

		// Method
		ScriptingNotAllowed => TypesError::method(
			"Scripting functions are not allowed".to_string(),
			Some(MethodError::NotAllowed),
		),
		FunctionNotAllowed(func) => {
			TypesError::method(format!("Function '{func}' is not allowed"), Some(MethodError::NotAllowed))
		}
		NetTargetNotAllowed(target) => TypesError::method(
			format!("Network target '{target}' is not allowed"),
			Some(MethodError::NotAllowed),
		),

		// Configuration
		RealtimeDisabled => TypesError::configuration(
			"Live query not supported".to_string(),
			Some(ConfigurationError::LiveQueryNotSupported),
		),

		// Query
		QueryTimedout(d) => TypesError::query(format!("{d}"), Some(QueryError::Timedout)),
		QueryCancelled => TypesError::query(
			"The query was not executed due to a cancelled transaction".to_string(),
			Some(QueryError::Cancelled),
		),
		QueryNotExecuted { message } => {
			TypesError::query(message.clone(), Some(QueryError::NotExecuted))
		}

		// Serialization
		Unencodable => {
			TypesError::serialization("Value cannot be serialized".to_string(), None)
		}
		Storekey(_) => {
			TypesError::serialization("Key decoding error".to_string(), None)
		}
		Revision(_) => {
			TypesError::serialization("Versioned data error".to_string(), None)
		}
		Utf8Error(_) => {
			TypesError::serialization("UTF-8 decoding error".to_string(), None)
		}
		Serialization(msg) => {
			TypesError::serialization(msg.clone(), Some(SerializationError::Serialization))
		}

		// Not found
		NsNotFound { name } => TypesError::not_found(
			format!("The namespace '{name}' does not exist"),
			Some(NotFoundError::Namespace),
		),
		DbNotFound { name } => TypesError::not_found(
			format!("The database '{name}' does not exist"),
			Some(NotFoundError::Database),
		),
		TbNotFound { name } => TypesError::not_found(
			format!("The table '{name}' does not exist"),
			Some(NotFoundError::Table),
		),
		IdNotFound { rid } => TypesError::not_found(
			format!("The record '{rid}' does not exist"),
			Some(NotFoundError::Record),
		),

		// Already exists
		DbAlreadyExists { name } => TypesError::already_exists(
			format!("The database '{name}' already exists"),
			Some(AlreadyExistsError::Database),
		),
		NsAlreadyExists { name } => TypesError::already_exists(
			format!("The namespace '{name}' already exists"),
			Some(AlreadyExistsError::Namespace),
		),
		TbAlreadyExists { name } => TypesError::already_exists(
			format!("The table '{name}' already exists"),
			Some(AlreadyExistsError::Table),
		),
		RecordExists { record } => TypesError::already_exists(
			format!("Database record `{}` already exists", record.to_sql()),
			Some(AlreadyExistsError::Record),
		),
		ClAlreadyExists { .. } => {
			TypesError::internal("Cluster node already exists".to_string())
		}
		ApAlreadyExists { .. } => TypesError::internal("API already exists".to_string()),
		AzAlreadyExists { .. } => TypesError::internal("Analyzer already exists".to_string()),
		BuAlreadyExists { .. } => TypesError::internal("Bucket already exists".to_string()),
		EvAlreadyExists { .. }
		| FdAlreadyExists { .. }
		| FcAlreadyExists { .. }
		| MdAlreadyExists { .. }
		| IxAlreadyExists { .. }
		| MlAlreadyExists { .. }
		| PaAlreadyExists { .. }
		| CgAlreadyExists { .. }
		| SeqAlreadyExists { .. }
		| NtAlreadyExists { .. }
		| DtAlreadyExists { .. }
		| UserRootAlreadyExists { .. }
		| UserNsAlreadyExists { .. }
		| UserDbAlreadyExists { .. }
		| AccessRootAlreadyExists { .. }
		| AccessNsAlreadyExists { .. }
		| AccessDbAlreadyExists { .. }
		| IndexAlreadyBuilding { .. }
		| IndexingBuildingCancelled { .. } => TypesError::internal(e.to_string()),

		// Thrown
		Thrown(msg) => TypesError::thrown(msg.clone()),

		// Internal and everything else
		Kvs(kvs_err) => TypesError::internal(format!("Key-value store error: {kvs_err}")),
		Internal(msg) => TypesError::internal(msg.clone()),
		Unimplemented(msg) => TypesError::internal(format!("Unimplemented: {msg}")),
		Io(io) => TypesError::internal(format!("I/O error: {io}")),
		Http(msg) => TypesError::internal(format!("HTTP error: {msg}")),
		Channel(msg) => TypesError::internal(format!("Channel error: {msg}")),
		CorruptedIndex(_) => TypesError::internal("Index corruption detected".to_string()),
		NoIndexFoundForMatch { .. } => {
			TypesError::internal("No suitable index found".to_string())
		}
		AnalyzerError(msg) => TypesError::internal(format!("Analyzer error: {msg}")),
		HighlightError(msg) => TypesError::internal(format!("Highlight error: {msg}")),
		FstError(_) => TypesError::internal("FST error".to_string()),
		ObsError(_) => TypesError::internal("Object store error".to_string()),
		TimestampOverflow(msg) => TypesError::internal(format!("Timestamp overflow: {msg}")),
		NoRecordFound => TypesError::internal("No record found".to_string()),

		_ => TypesError::internal(e.to_string()),
	}
}
