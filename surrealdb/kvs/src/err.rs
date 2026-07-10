use core::fmt;

use thiserror::Error;

/// Result type for KVS (Key-Value Store) layer operations
pub type Result<T> = std::result::Result<T, Error>;

/// An error originating from the KVS (Key-Value Store) layer.
///
/// This error type abstracts storage engine details and provides
/// generic error variants that can be used across all storage backends.
#[allow(dead_code, reason = "Some variants are only used by specific KV stores")]
#[derive(Error, Debug)]
pub enum Error {
	/// There was a problem with the underlying datastore
	#[error("There was a problem with the datastore: {0}")]
	Datastore(String),

	/// Failed to connect to the storage backend
	#[error("Connection to storage backend failed: {0}")]
	ConnectionFailed(String),

	/// The datastore is read-and-deletion-only due to disk saturation
	#[error(
		"The datastore is in read-and-deletion-only mode due to disk space limitations. Only read and delete operations are allowed. Deleting data will free up space and automatically restore normal operations when usage drops below the threshold"
	)]
	ReadAndDeleteOnly,

	/// There was a problem with a datastore transaction
	#[error("There was a problem with a transaction: {0}")]
	Transaction(String),

	/// The transaction is too large
	#[error("The transaction is too large")]
	TransactionTooLarge,

	/// A transactional range operation exceeded its configured key-count bound.
	///
	/// Returned by TiKV's `delr` when the range would exceed
	/// `SURREAL_TIKV_DELR_MAX_KEYS`. Callers that need to drop very large
	/// ranges should use a datastore-level `unsafe_destroy_range` instead.
	#[error("Transaction range operation exceeded the maximum key count of {0}")]
	TransactionRangeTooLarge(u32),

	/// The key being inserted in the transaction is too large
	#[error("The key being inserted is too large")]
	TransactionKeyTooLarge,

	/// A transaction conflict occurred and the operation should be retried
	#[error("Transaction conflict: {0}. This transaction can be retried")]
	TransactionConflict(String),

	/// The transaction was already cancelled or committed
	#[error("Couldn't update a finished transaction")]
	TransactionFinished,

	/// The current transaction was created as read-only
	#[error("Couldn't write to a read only transaction")]
	TransactionReadonly,

	/// The conditional value in the request was not equal
	#[error("Value being checked was not correct")]
	TransactionConditionNotMet,

	/// The key being inserted in the transaction already exists
	#[error("The key being inserted already exists")]
	TransactionKeyAlreadyExists,

	/// The underlying datastore does not support versioned queries
	#[error("The underlying datastore does not support versioned queries")]
	UnsupportedVersionedQueries,

	/// The specified timestamp is not valid for the underlying datastore
	#[error("The specified timestamp is not valid for the underlying datastore: {0}")]
	TimestampInvalid(String),

	/// There was an unknown internal error
	#[error("There was an internal error: {0}")]
	Internal(String),

	#[error("The storage layer does not support compaction requests.")]
	CompactionNotSupported,
}

impl Error {
	/// Check if this error indicates the transaction can be retried
	pub fn is_retryable(&self) -> bool {
		matches!(self, Error::TransactionConflict(_))
	}

	pub fn internal<E>(e: E) -> Self
	where
		E: fmt::Display,
	{
		Error::Internal(e.to_string())
	}
}

impl From<std::num::TryFromIntError> for Error {
	fn from(e: std::num::TryFromIntError) -> Error {
		Error::TimestampInvalid(e.to_string())
	}
}
