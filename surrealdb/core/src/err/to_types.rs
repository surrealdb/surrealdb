//! Conversion from core [`Error`] to wire-friendly [`surrealdb_types::Error`].
//!
//! This is the single place that defines how core's own failures are mapped to
//! the public types-layer error used over RPC and in the SDK. Each of the
//! layer errors decides its own mapping, in its own `LeafError` impl; the
//! wrapper arms here delegate to those rather than re-deriving them.

// The mapper below is the only place core's own failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{
	AuthError, ConnectionError, Error as TypesError, QueryError, ValidationError,
};

use crate::err::Error;
use crate::iam::PolicyError;
use crate::kvs::Error as KvsError;

impl LeafError for Error {
	/// Classifies a core database error.
	///
	/// Takes ownership so owned data (message strings, IAM details) can be moved
	/// rather than cloned. The message and cause are supplied and attached by
	/// [`LeafError::to_types_error`]; do not compute either here.
	fn map_kind(self, message: String) -> TypesError {
		into_types_error_inner(self, message)
	}
}

/// Converts a core database error into the public wire-friendly error type.
pub fn into_types_error(error: Error) -> TypesError {
	error.to_types_error()
}

fn into_types_error_inner(error: Error, message: String) -> TypesError {
	use Error::*;
	match error {
		// Delegate, never `to_types_error`: the framing is applied once, above.
		// Calling inward would frame twice and overwrite the cause.
		Engine(error) => error.map_kind(message),
		Exec(error) => error.map_kind(message),
		ApiError(error) => error.map_kind(message),
		InvalidPath(_) => internal_todo(message),

		// Authorisation
		IamError(iam_err) => match iam_err {
			PolicyError::InvalidRole(name) => TypesError::not_allowed(
				message,
				AuthError::InvalidRole {
					name,
				},
			),
			PolicyError::NotAllowed {
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

		// Outbound HTTP: a failed request is a connection failure the caller can
		// retry, whereas a URL that will not parse is the caller's own input and
		// no retry helps.
		Http(..) => TypesError::connection(message, ConnectionError::ConnectionFailed),
		InvalidUrl(..) => internal_todo(message),

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
			// The server is shutting down. Connection-class because the
			// common case is a commit refused before it applied, which is
			// safe to retry once the client reconnects. A shutdown is
			// treated as a controlled crash, so the rare apply-but-
			// unconfirmed race carries the same crash-equivalent ambiguity a
			// client already has to tolerate on any unclean disconnect.
			KvsError::Shutdown => {
				TypesError::connection(message, ConnectionError::ConnectionFailed)
			}
			KvsError::TransactionTooLarge
			| KvsError::TransactionKeyTooLarge
			| KvsError::TransactionRangeTooLarge(_) => {
				TypesError::validation(message, ValidationError::InvalidParams)
			}
			// Kept out of connection-class deliberately: that marks the failure
			// client-side and drives SDK reconnect-and-retry, which is the one
			// thing a caller must not do with a commit that may have applied.
			// The message carries the ambiguity.
			KvsError::CommitOutcomeUnknown(_) => TypesError::internal(message),
			KvsError::TransactionFinished
			| KvsError::TransactionReadonly
			| KvsError::TransactionConditionNotMet => TypesError::query(message, None),
			KvsError::UnsupportedVersionedQueries => TypesError::configuration(message, None),
			KvsError::Datastore(_)
			| KvsError::Transaction(_)
			| KvsError::NoSavepoint
			| KvsError::TimestampInvalid(_)
			| KvsError::Internal(_)
			| KvsError::CompactionNotSupported => TypesError::internal(message),
		},
	}
}
