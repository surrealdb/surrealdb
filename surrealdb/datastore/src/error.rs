//! Failures raised by the datastore layer.
//!
//! These are the failures of running a query against a the datastore, as
//! opposed to the failures of the storage backend underneath it
//! (the storage error) or of the index structures above it (the index error):
//! the session and realtime gates on the datastore's
//! entry points, the timeouts and thresholds that stop a query mid-flight, the
//! concurrent index-build protocol the datastore drives, and the on-disk
//! storage version it refuses to run against.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_catalog::Generation;
use surrealdb_expr::val::Duration;
use surrealdb_kvs::{FeatureLevel, Release};
use surrealdb_types::{AuthError, ConfigurationError, Error as TypesError, QueryError};
use uuid::Uuid;

/// A failure in the datastore layer.
#[derive(Debug, thiserror::Error)]
pub enum DatastoreError {
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

	/// A blocking index build outlasted the time its statement had left to
	/// wait for it.
	///
	/// Raised only once the statement has committed: its change is durable and
	/// the build carries on in the background, so the statement merely stopped
	/// waiting. Running it again while the build runs fails with
	/// [`DatastoreError::IndexAlreadyBuilding`].
	#[error(
		"The index rebuild was committed, but its build did not finish within the timeout of {0}. The build continues in the background: follow it with `INFO FOR INDEX` rather than running the statement again"
	)]
	IndexBuildOutlastedTimeout(Duration),

	/// The query was cancelled while a blocking index build its statement
	/// started was still running.
	///
	/// Raised only once the statement has committed, as
	/// [`DatastoreError::IndexBuildOutlastedTimeout`] is: the build carries on
	/// in the background, and only the statement stopped waiting for it.
	#[error(
		"The index rebuild was committed, but the query was cancelled before its build finished. The build continues in the background: follow it with `INFO FOR INDEX` rather than running the statement again"
	)]
	IndexBuildWaitCancelled,

	/// A blocking `DEFINE INDEX` or `REBUILD INDEX` stopped waiting for its
	/// index to be materialised.
	///
	/// Raised only once the statement has committed and its index is online:
	/// the index answers every query correctly, merging what is not yet folded
	/// into it at read time, and background compaction folds the rest. Only
	/// the wait stopped, cancelled with its query or ended by an error a
	/// compaction pass could not retry, which `reason` names. Running the
	/// statement again changes nothing.
	#[error(
		"The index was committed and is ready, but the wait for its pending updates to be folded in stopped: {reason}. Background compaction folds them: follow it with `INFO FOR INDEX` rather than running the statement again"
	)]
	IndexMaterialiseWaitStopped {
		/// Whether the query was cancelled, rather than a pass failing.
		cancelled: bool,
		/// Why the wait stopped.
		reason: String,
	},

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

	/// A value could not be allocated from a sequence, because a
	/// `DEFINE`/`REMOVE SEQUENCE` kept landing while it was being allocated.
	///
	/// The allocator rebuilds itself once against the definition as it stands;
	/// this is a second reset arriving during that rebuild, which means the
	/// sequence is being redefined repeatedly rather than once. Retrying is the
	/// right response, and no value was issued.
	#[error(
		"The sequence was redefined or removed while a value was being allocated from it. This transaction can be retried"
	)]
	SequenceReset,

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

	/// The data on disk has been migrated by a newer version than this build
	#[error(
		"The data stored on disk was migrated by a newer version of SurrealDB \
		 (Stored: {stored}, Running: {running}) and cannot be read by this one. \
		 The following data migrations have been applied that this version does not know about: {migrations}. \
		 Run a version at or above {stored}, or restore from an export taken before the upgrade"
	)]
	MigratedBeyondStorageVersion {
		stored: String,
		running: String,
		migrations: String,
	},

	/// An edge build refused to attach a store that needs a full build's startup first.
	#[error(
		"An edge build cannot attach this datastore (Stored: {stored}, Running: {running}): {reason}. \
		 Open it once with a full build of {running}, which runs the startup checks and migrations, then attach it again"
	)]
	NotAttachable {
		stored: String,
		running: String,
		reason: String,
	},

	/// Another node held the migration lease for too long
	#[error(
		"Timed out waiting for another node to apply pending data migrations ({migrations}). \
		 Check whether the migrating node is still running, then restart this one"
	)]
	MigrationTimedOut {
		migrations: String,
	},

	/// A write needs a release's features, and the store's feature level is below it.
	#[error(
		"This needs feature level {needed}, but the store is at {level}. Raise the feature level once every node runs {needed} or later"
	)]
	FeatureNotEnabled {
		needed: Release,
		level: FeatureLevel,
	},

	/// The feature level was asked to rise above the release this build is.
	#[error("The feature level cannot be raised to {target}: this build is {current}")]
	FeatureLevelAboveRelease {
		target: Release,
		current: Release,
	},

	/// The feature level was asked to be set below the first release that has one.
	#[error("The feature level cannot be set below {first}, the first release with one")]
	FeatureLevelBelowFirst {
		first: Release,
	},

	/// A live node runs a release below the level the store was asked to rise to.
	#[error(
		"The feature level cannot be raised to {target} while node {node} runs {}",
		.release.map_or_else(|| "a release that records none".to_string(), |r| r.to_string())
	)]
	FeatureLevelNodeBehind {
		node: Uuid,
		release: Option<Release>,
		target: Release,
	},

	/// The feature level was asked to fall below a release whose features entries already use.
	#[error("The feature level cannot be lowered to {target}: entries already need {in_use}")]
	FeatureLevelInUse {
		in_use: Release,
		target: Release,
	},

	/// The feature level was asked to fall on a store that records none.
	#[error("The feature level cannot be lowered: the store has not recorded one")]
	FeatureLevelUnset,

	/// An entry in the stored grammar sits in a store that records no feature level.
	#[error("The store holds catalog entries in the stored grammar but records no feature level")]
	FeatureLevelMissing,

	/// A configured feature level is neither `current`, `legacy` nor a release.
	#[error(
		"The configured feature level is not a release of the form major.minor, `current` or `legacy`"
	)]
	FeatureLevelPinInvalid,

	/// A table's generation ring is full: the generation after its newest is its oldest, so no
	/// further layout can be minted until the ring's oldest generation is retired.
	///
	/// `floor` is the oldest generation the table holds.
	#[error(
		"Table '{table}' has used every generation from {floor}, so no further layout can be minted until its floor moves"
	)]
	GenerationRingFull {
		table: String,
		floor: Generation,
	},

	/// A layout key already exists at the number a mint would take, though the table's ring does
	/// not hold the number: stored state that no mint of this build writes.
	#[error(
		"Table '{table}' already holds a layout for generation {generation}, which its ring does not"
	)]
	GenerationOccupied {
		table: String,
		generation: Generation,
	},

	/// An operation that needs a table's generation ring was given a table that has none.
	#[error("Table '{table}' has no generation")]
	NoGeneration {
		table: String,
	},

	/// A floor was moved to a generation the table's ring does not hold.
	#[error(
		"Table '{table}' cannot move its floor to generation {floor}: its generations are {base} to {head}"
	)]
	FloorOutsideWindow {
		table: String,
		floor: Generation,
		base: Generation,
		head: Generation,
	},

	/// A conformance claim was settled against a definition of field `field` that is no longer the
	/// table's current one.
	#[error(
		"The definition of field '{field}' of table '{table}' changed since its conformance was checked"
	)]
	ConformanceStale {
		table: String,
		field: String,
	},

	/// The store records features this build does not maintain, so it opens read-only.
	///
	/// `features` holds ledger ids and recorded names only.
	#[error(
		"The store is open read-only: it records features this build does not maintain ({features}). Run a release that knows them to write to it"
	)]
	StoreReadOnly {
		features: String,
	},

	/// A read-only transaction asked for a staleness bound above the
	/// operator's maximum.
	#[error(
		"The requested MAX STALENESS of {bound:?} is above the maximum of {maximum:?} this datastore allows"
	)]
	StalenessAboveMaximum {
		bound: std::time::Duration,
		maximum: std::time::Duration,
	},

	/// No published snapshot met the staleness bound, and the request or the
	/// fall-back policy forbids reading the current state instead.
	#[error(
		"ReadSnapshotTooStale: no snapshot within MAX STALENESS {bound:?}; the newest is {newest_age:?} old"
	)]
	ReadSnapshotTooStale {
		bound: std::time::Duration,
		newest_age: std::time::Duration,
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
			DatastoreError::TransactionTimedout(duration)
			| DatastoreError::IndexBuildOutlastedTimeout(duration) => TypesError::query(
				message,
				QueryError::TimedOut {
					duration: duration.0,
				},
			),
			DatastoreError::IndexBuildWaitCancelled
			| DatastoreError::IndexMaterialiseWaitStopped {
				cancelled: true,
				..
			} => TypesError::query(message, QueryError::Cancelled),
			DatastoreError::IndexMaterialiseWaitStopped {
				cancelled: false,
				..
			} => TypesError::query(message, None),
			// Shadows `message` deliberately: the payload string is what reaches
			// the client, not this error's own `Display` output.
			DatastoreError::TransactionWriteKeysExceeded {
				..
			} => TypesError::query(message, None),
			DatastoreError::QueryNotExecuted {
				message,
			} => TypesError::query(message, QueryError::NotExecuted),
			// Retryable, and for the same reason a write conflict is: another
			// statement changed what this one was operating on.
			DatastoreError::SequenceReset => {
				TypesError::query(message, QueryError::TransactionConflict)
			}
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
			}
			| DatastoreError::MigratedBeyondStorageVersion {
				..
			}
			| DatastoreError::MigrationTimedOut {
				..
			}
			| DatastoreError::NotAttachable {
				..
			} => internal_todo(message),
			DatastoreError::FeatureNotEnabled {
				..
			}
			| DatastoreError::FeatureLevelAboveRelease {
				..
			}
			| DatastoreError::FeatureLevelBelowFirst {
				..
			}
			| DatastoreError::FeatureLevelNodeBehind {
				..
			}
			| DatastoreError::FeatureLevelInUse {
				..
			}
			| DatastoreError::FeatureLevelUnset
			| DatastoreError::FeatureLevelMissing
			| DatastoreError::FeatureLevelPinInvalid
			| DatastoreError::StoreReadOnly {
				..
			} => TypesError::configuration(message, None),
			DatastoreError::StalenessAboveMaximum {
				..
			}
			| DatastoreError::ReadSnapshotTooStale {
				..
			} => TypesError::query(message, None),
			DatastoreError::GenerationRingFull {
				..
			}
			| DatastoreError::NoGeneration {
				..
			}
			| DatastoreError::FloorOutsideWindow {
				..
			}
			| DatastoreError::ConformanceStale {
				..
			} => TypesError::query(message, None),
			// Stored state no mint of this build writes, as a stamp outside the ring is.
			DatastoreError::GenerationOccupied {
				..
			} => TypesError::serialization(message, None),
		}
	}
}
