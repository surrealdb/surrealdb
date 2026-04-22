//! Conversion from core [`Error`] to wire-friendly [`surrealdb_types::Error`].
//!
//! This is the single place that defines how embedded database errors are mapped to the
//! public types-layer error used over RPC and in the SDK.

use std::error::Error as StdError;

use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, ConnectionError, Error as TypesError,
	NotAllowedError, NotFoundError, QueryError, SerializationError, ToSql, ValidationError,
};

use crate::err::Error;
use crate::iam::Error as IamErrorKind;
use crate::kvs::Error as KvsError;

/// Converts a core database error into the public wire-friendly error type.
///
/// Takes ownership so owned data (e.g. message strings, IAM details) can be moved instead of
/// cloned. For `anyhow::Error`, use `downcast` to consume and recover the core `Error`:
/// `e.downcast::<Error>().map(into_types_error).unwrap_or_else(|e|
/// TypesError::internal(e.to_string()))`.
pub fn into_types_error(error: Error) -> TypesError {
	use Error::*;
	let message = error.to_string();
	let source = error.source().map(|s| TypesError::internal(s.to_string()));
	let mapped = match error {
		// Auth
		ExpiredSession => TypesError::not_allowed(message, AuthError::SessionExpired),
		ExpiredToken => TypesError::not_allowed(message, AuthError::TokenExpired),
		InvalidAuth => TypesError::not_allowed(message, AuthError::InvalidAuth),
		UnexpectedAuth => TypesError::not_allowed(message, AuthError::UnexpectedAuth),
		MissingUserOrPass => TypesError::not_allowed(message, AuthError::MissingUserOrPass),
		NoSigninTarget => TypesError::not_allowed(message, AuthError::NoSigninTarget),
		InvalidPass => TypesError::not_allowed(message, AuthError::InvalidPass),
		TokenMakingFailed => TypesError::not_allowed(message, AuthError::TokenMakingFailed),
		IamError(iam_err) => match iam_err {
			IamErrorKind::InvalidRole(name) => TypesError::not_allowed(
				message,
				AuthError::InvalidRole {
					name,
				},
			),
			IamErrorKind::NotAllowed {
				actor,
				action,
				resource,
			} => TypesError::not_allowed(
				message,
				AuthError::NotAllowed {
					actor,
					action,
					resource,
				},
			),
		},
		InvalidSignup => TypesError::not_allowed(message, AuthError::InvalidSignup),

		// Validation
		NsEmpty => TypesError::validation(message, ValidationError::NamespaceEmpty),
		DbEmpty => TypesError::validation(message, ValidationError::DatabaseEmpty),
		InvalidQuery(_) => TypesError::validation(message, None),
		InvalidParam {
			name,
		} => TypesError::validation(
			message,
			ValidationError::InvalidParameter {
				name,
			},
		),
		InvalidContent {
			value,
		} => TypesError::validation(
			message,
			ValidationError::InvalidContent {
				value: value.to_sql(),
			},
		),
		InvalidMerge {
			value,
		} => TypesError::validation(
			message,
			ValidationError::InvalidMerge {
				value: value.to_sql(),
			},
		),
		InvalidPatch(_) => TypesError::validation(message, None),
		Coerce(_) => TypesError::validation(message, None),
		Cast(_) => TypesError::validation(message, None),
		TryAdd(..) | TrySub(..) | TryMul(..) | TryDiv(..) | TryRem(..) | TryPow(..) | TryNeg(_)
		| TryExtend(_) => TypesError::validation(message, None),
		TryFrom(..) => TypesError::validation(message, None),
		DuplicatedMatchRef {
			..
		} => TypesError::validation(message, None),

		// Not allowed (method, scripting, function, net target)
		ScriptingNotAllowed => TypesError::not_allowed(message, NotAllowedError::Scripting),
		FunctionNotAllowed(name) => TypesError::not_allowed(
			message,
			NotAllowedError::Function {
				name,
			},
		),
		NetTargetNotAllowed(name) => TypesError::not_allowed(
			message,
			NotAllowedError::Target {
				name,
			},
		),

		// Configuration
		RealtimeDisabled => {
			TypesError::configuration(message, ConfigurationError::LiveQueryNotSupported)
		}

		// Query
		QueryTimedout(duration) => TypesError::query(
			message,
			QueryError::TimedOut {
				duration: duration.0,
			},
		),
		TransactionTimedout(duration) => TypesError::query(
			message,
			QueryError::TimedOut {
				duration: duration.0,
			},
		),
		QueryCancelled => TypesError::query(message, QueryError::Cancelled),
		QueryNotExecuted {
			message,
		} => TypesError::query(message, QueryError::NotExecuted),
		AccessRecordSignupQueryFailed | AccessRecordSigninQueryFailed => {
			TypesError::query(message, None)
		}
		AccessRecordNoSignup | AccessRecordNoSignin => TypesError::query(message, None),

		// Serialization
		Unencodable => TypesError::serialization(message, None),
		Storekey(_) => TypesError::serialization(message, None),
		Revision(_) => TypesError::serialization(message, None),
		Utf8Error(_) => TypesError::serialization(message, None),
		Serialization(..) => TypesError::serialization(message, SerializationError::Serialization),

		// Not found
		NsNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Namespace {
				name,
			},
		),
		DbNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Database {
				name,
			},
		),
		TbNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Table {
				name: name.into_string(),
			},
		),
		IdNotFound {
			rid,
		} => TypesError::not_found(
			message,
			NotFoundError::Record {
				id: rid,
			},
		),

		// Already exists
		DbAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Database {
				name,
			},
		),
		NsAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Namespace {
				name,
			},
		),
		TbAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Table {
				name,
			},
		),
		RecordExists {
			record,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Record {
				id: record.to_sql(),
			},
		),
		ClAlreadyExists {
			..
		} => TypesError::internal(message),
		ApAlreadyExists {
			..
		} => TypesError::internal(message),
		AzAlreadyExists {
			..
		} => TypesError::internal(message),
		BuAlreadyExists {
			..
		} => TypesError::internal(message),
		EvAlreadyExists {
			..
		}
		| FdAlreadyExists {
			..
		}
		| FcAlreadyExists {
			..
		}
		| MdAlreadyExists {
			..
		}
		| IxAlreadyExists {
			..
		}
		| MlAlreadyExists {
			..
		}
		| PaAlreadyExists {
			..
		}
		| CgAlreadyExists {
			..
		}
		| SeqAlreadyExists {
			..
		}
		| NtAlreadyExists {
			..
		}
		| DtAlreadyExists {
			..
		}
		| UserRootAlreadyExists {
			..
		}
		| UserNsAlreadyExists {
			..
		}
		| UserDbAlreadyExists {
			..
		}
		| AccessRootAlreadyExists {
			..
		}
		| AccessNsAlreadyExists {
			..
		}
		| AccessDbAlreadyExists {
			..
		}
		| IndexAlreadyBuilding {
			..
		}
		| IndexingBuildingCancelled {
			..
		} => TypesError::internal(message),

		// Thrown
		Thrown(..) => TypesError::thrown(message),

		// Connection/transport (remote request failure)
		Http(..) => TypesError::connection(message, ConnectionError::ConnectionFailed),

		// Not found (no record returned)
		NoRecordFound => TypesError::not_found(message, None),

		// KVS: preserve type information for wire and client retry/UX
		Kvs(kvs_err) => match kvs_err {
			KvsError::TransactionConflict(_) => {
				TypesError::query(message, QueryError::TransactionConflict)
			}
			KvsError::ConnectionFailed(_) => {
				TypesError::connection(message, ConnectionError::ConnectionFailed)
			}
			KvsError::TransactionKeyAlreadyExists => TypesError::already_exists(message, None),
			KvsError::ReadAndDeleteOnly => TypesError::not_allowed(message, None),
			KvsError::TransactionTooLarge | KvsError::TransactionKeyTooLarge => {
				TypesError::validation(message, ValidationError::InvalidParams)
			}
			KvsError::TransactionFinished
			| KvsError::TransactionReadonly
			| KvsError::TransactionConditionNotMet => TypesError::query(message, None),
			KvsError::UnsupportedVersionedQueries => TypesError::configuration(message, None),
			KvsError::Datastore(_)
			| KvsError::Transaction(_)
			| KvsError::TimestampInvalid(_)
			| KvsError::Internal(_)
			| KvsError::CompactionNotSupported => TypesError::internal(message),
		},

		// Internal and everything else
		Internal(..) => TypesError::internal(message),
		Unimplemented(..) => TypesError::internal(message),
		Io(..) => TypesError::internal(message),
		Channel(..) => TypesError::internal(message),
		CorruptedIndex(_) => TypesError::internal(message),
		NoIndexFoundForMatch {
			..
		} => TypesError::internal(message),
		AnalyzerError(..) => TypesError::internal(message),
		HighlightError(..) => TypesError::internal(message),
		FstError(_) => TypesError::internal(message),
		ObsError(_) => TypesError::internal(message),
		TimestampOverflow(..) => TypesError::internal(message),
		ApiError(error) => error.into_types_error(),

		_ => TypesError::internal(message),
	};

	if let Some(cause) = source {
		mapped.with_cause(cause)
	} else {
		mapped
	}
}
