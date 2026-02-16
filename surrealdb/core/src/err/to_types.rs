//! Conversion from core [`Error`] to wire-friendly [`surrealdb_types::Error`].
//!
//! This is the single place that defines how embedded database errors are mapped to the
//! public types-layer error used over RPC and in the SDK.

use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, Error as TypesError, NotAllowedError,
	NotFoundError, QueryError, SerializationError,
};

use crate::err::Error;
use crate::iam::Error as IamErrorKind;

/// Converts a core database error into the public wire-friendly error type.
///
/// Takes ownership so owned data (e.g. message strings, IAM details) can be moved instead of
/// cloned. For `anyhow::Error`, use `downcast` to consume and recover the core `Error`:
/// `e.downcast::<Error>().map(into_types_error).unwrap_or_else(|e|
/// TypesError::internal(e.to_string()))`.
pub fn into_types_error(error: Error) -> TypesError {
	use Error::*;
	let message = error.to_string();
	match error {
		// Auth
		ExpiredSession => TypesError::auth(message, Some(AuthError::SessionExpired)),
		ExpiredToken => TypesError::auth(message, Some(AuthError::TokenExpired)),
		InvalidAuth => TypesError::auth(message, Some(AuthError::InvalidAuth)),
		UnexpectedAuth => TypesError::auth(message, Some(AuthError::UnexpectedAuth)),
		MissingUserOrPass => TypesError::auth(message, Some(AuthError::MissingUserOrPass)),
		NoSigninTarget => TypesError::auth(message, Some(AuthError::NoSigninTarget)),
		InvalidPass => TypesError::auth(message, Some(AuthError::InvalidPass)),
		TokenMakingFailed => TypesError::auth(message, Some(AuthError::TokenMakingFailed)),
		IamError(iam_err) => match iam_err {
			IamErrorKind::InvalidRole(role) => {
				TypesError::auth(message, Some(AuthError::InvalidRole(role)))
			}
			IamErrorKind::NotAllowed {
				actor,
				action,
				resource,
			} => TypesError::auth(
				message,
				Some(AuthError::NotAllowed {
					actor,
					action,
					resource,
				}),
			),
		},
		InvalidSignup => TypesError::auth(message, Some(AuthError::InvalidSignup)),

		// Validation
		NsEmpty => TypesError::validation(message, None),
		DbEmpty => TypesError::validation(message, None),
		InvalidQuery(_) => TypesError::validation(message, None),
		InvalidParam {
			..
		} => TypesError::validation(message, None),
		InvalidContent {
			..
		} => TypesError::validation(message, None),
		InvalidMerge {
			..
		} => TypesError::validation(message, None),
		InvalidPatch(_) => TypesError::validation(message, None),
		Coerce(_) => TypesError::validation(message, None),
		Cast(_) => TypesError::validation(message, None),
		TryAdd(..) | TrySub(..) | TryMul(..) | TryDiv(..) | TryRem(..) | TryPow(..) | TryNeg(_) => {
			TypesError::validation(message, None)
		}
		TryFrom(..) => TypesError::validation(message, None),
		DuplicatedMatchRef {
			..
		} => TypesError::validation(message, None),

		// Not allowed (method, scripting, function, net target)
		ScriptingNotAllowed => TypesError::not_allowed(message, Some(NotAllowedError::Method)),
		FunctionNotAllowed(..) => TypesError::not_allowed(message, Some(NotAllowedError::Method)),
		NetTargetNotAllowed(..) => TypesError::not_allowed(message, Some(NotAllowedError::Method)),

		// Configuration
		RealtimeDisabled => {
			TypesError::configuration(message, Some(ConfigurationError::LiveQueryNotSupported))
		}

		// Query
		QueryTimedout(..) => TypesError::query(message, Some(QueryError::Timedout)),
		QueryCancelled => TypesError::query(message, Some(QueryError::Cancelled)),
		QueryNotExecuted {
			message,
		} => TypesError::query(message, Some(QueryError::NotExecuted)),

		// Serialization
		Unencodable => TypesError::serialization(message, None),
		Storekey(_) => TypesError::serialization(message, None),
		Revision(_) => TypesError::serialization(message, None),
		Utf8Error(_) => TypesError::serialization(message, None),
		Serialization(..) => {
			TypesError::serialization(message, Some(SerializationError::Serialization))
		}

		// Not found
		NsNotFound {
			..
		} => TypesError::not_found(message, Some(NotFoundError::Namespace)),
		DbNotFound {
			..
		} => TypesError::not_found(message, Some(NotFoundError::Database)),
		TbNotFound {
			..
		} => TypesError::not_found(message, Some(NotFoundError::Table)),
		IdNotFound {
			..
		} => TypesError::not_found(message, Some(NotFoundError::Record)),

		// Already exists
		DbAlreadyExists {
			..
		} => TypesError::already_exists(message, Some(AlreadyExistsError::Database)),
		NsAlreadyExists {
			..
		} => TypesError::already_exists(message, Some(AlreadyExistsError::Namespace)),
		TbAlreadyExists {
			..
		} => TypesError::already_exists(message, Some(AlreadyExistsError::Table)),
		RecordExists {
			..
		} => TypesError::already_exists(message, Some(AlreadyExistsError::Record)),
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

		// Internal and everything else
		Kvs(..) => TypesError::internal(message),
		Internal(..) => TypesError::internal(message),
		Unimplemented(..) => TypesError::internal(message),
		Io(..) => TypesError::internal(message),
		Http(..) => TypesError::internal(message),
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
		NoRecordFound => TypesError::internal(message),
		ApiError(error) => error.into_types_error(),

		_ => TypesError::internal(message),
	}
}
