//! Failures raised by the datastore layer.
//!
//! These are the failures of running a query against a [`Datastore`], as
//! opposed to the failures of the storage backend underneath it
//! ([`crate::kvs::Error`]) or of the index structures above it
//! ([`crate::idx::Error`]): the session and realtime gates on the datastore's
//! entry points, the timeouts and thresholds that stop a query mid-flight, the
//! concurrent index-build protocol the datastore drives, and the on-disk
//! storage version it refuses to run against.
//!
//! [`Datastore`]: crate::kvs::Datastore

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{AuthError, ConfigurationError, Error as TypesError, QueryError};

use crate::val::Duration;

/// A failure in the datastore layer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DatastoreError {
	/// The session has expired
	#[error("The session has expired")]
	ExpiredSession,

	/// Unable to perform the realtime query
	#[error("Unable to perform the realtime query")]
	RealtimeDisabled,

	/// Invalid timeout
	#[error("Invalid timeout: {0:?} seconds")]
	InvalidTimeout(u64),

	/// The transaction timed out
	#[error("The transaction was not completed because it exceeded the timeout: {0}")]
	TransactionTimedout(Duration),

	/// The query did not execute, because the memory threshold has been reached
	#[error("The query was not executed due to the memory threshold being reached")]
	QueryBeyondMemoryThreshold,

	/// The statement buffered the configured maximum number of individual key
	/// writes and was rolled back before accumulating more.
	///
	/// A statement's physical write count can vastly exceed its logical row
	/// count through cascaded deletes, full-text term maintenance and
	/// graph-edge cleanup; this bounds that fan-out.
	#[error(
		"Transaction exceeded the maximum number of key writes ({limit}). The statement's physical write fan-out (cascaded deletes, index maintenance, graph-edge cleanup) reached the `transaction_max_write_keys` limit and was rolled back. Reduce the operation's scope, or raise or disable the limit"
	)]
	TransactionWriteKeysExceeded {
		limit: u64,
	},

	/// The query did not execute, because the transaction has failed.
	#[error("The query was not executed due to a failed transaction. {message}")]
	QueryNotExecuted {
		message: String,
	},

	/// The index has been found to be inconsistent
	#[error("Index is corrupted: {0}")]
	CorruptedIndex(&'static str),

	/// A database index entry for the specified table is already building
	#[error("Database index `{name}` is currently building")]
	IndexAlreadyBuilding {
		name: String,
	},

	/// A the index building has been cancelled
	#[error("Index building has been cancelled: {reason}")]
	IndexingBuildingCancelled {
		reason: String,
	},

	/// There was an invalid storage version stored in the database
	#[error("There was an invalid storage version stored in the database")]
	InvalidStorageVersion,

	/// There was an outdated storage version stored in the database
	#[error(
		"The data stored on disk is out-of-date with this version (Expected: {expected}, Actual: {actual}). \
		 Please follow the upgrade guides in the documentation, \
		 or use a clean storage directory if this is intended to be a new instance"
	)]
	OutdatedStorageVersion {
		expected: u16,
		actual: u16,
	},
}

impl LeafError for DatastoreError {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			DatastoreError::ExpiredSession => {
				TypesError::not_allowed(message, AuthError::SessionExpired)
			}
			DatastoreError::RealtimeDisabled => {
				TypesError::configuration(message, ConfigurationError::LiveQueryNotSupported)
			}
			DatastoreError::TransactionTimedout(duration) => TypesError::query(
				message,
				QueryError::TimedOut {
					duration: duration.0,
				},
			),
			// Shadows `message` deliberately: the payload string is what reaches
			// the client, not this error's own `Display` output.
			DatastoreError::TransactionWriteKeysExceeded {
				..
			} => TypesError::query(message, None),
			DatastoreError::QueryNotExecuted {
				message,
			} => TypesError::query(message, QueryError::NotExecuted),
			DatastoreError::CorruptedIndex(_)
			| DatastoreError::IndexAlreadyBuilding {
				..
			}
			| DatastoreError::IndexingBuildingCancelled {
				..
			} => TypesError::internal(message),
			DatastoreError::InvalidTimeout(_)
			| DatastoreError::QueryBeyondMemoryThreshold
			| DatastoreError::InvalidStorageVersion
			| DatastoreError::OutdatedStorageVersion {
				..
			} => internal_todo(message),
		}
	}
}
