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
}

impl Error {
	/// Check if this error indicates the transaction can be retried
	pub fn is_retryable(&self) -> bool {
		matches!(self, Error::TransactionConflict(_))
	}
}

#[cfg(feature = "kv-mem")]
impl From<surrealmx::Error> for Error {
	fn from(e: surrealmx::Error) -> Error {
		match e {
			surrealmx::Error::TxNotWritable => Error::TransactionReadonly,
			surrealmx::Error::ValNotExpectedValue => Error::TransactionConditionNotMet,
			surrealmx::Error::TxClosed => Error::TransactionFinished,
			surrealmx::Error::KeyAlreadyExists => Error::TransactionKeyAlreadyExists,
			surrealmx::Error::KeyReadConflict => Error::TransactionConflict(e.to_string()),
			surrealmx::Error::KeyWriteConflict => Error::TransactionConflict(e.to_string()),
			_ => Error::Transaction(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-surrealkv")]
impl From<surrealkv::Error> for Error {
	fn from(e: surrealkv::Error) -> Error {
		match e {
			surrealkv::Error::TransactionWriteConflict => Error::TransactionConflict(e.to_string()),
			surrealkv::Error::TransactionReadOnly => Error::TransactionReadonly,
			surrealkv::Error::TransactionClosed => Error::TransactionFinished,
			_ => Error::Transaction(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-rocksdb")]
impl From<rocksdb::Error> for Error {
	fn from(e: rocksdb::Error) -> Error {
		match e.kind() {
			rocksdb::ErrorKind::Busy => Error::TransactionConflict(e.to_string()),
			rocksdb::ErrorKind::TryAgain => Error::TransactionConflict(e.to_string()),
			_ => Error::Transaction(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-indxdb")]
impl From<indxdb::Error> for Error {
	fn from(e: indxdb::Error) -> Error {
		match e {
			indxdb::Error::DbError => Error::Datastore(e.to_string()),
			indxdb::Error::TxError => Error::Transaction(e.to_string()),
			indxdb::Error::TxClosed => Error::TransactionFinished,
			indxdb::Error::TxNotWritable => Error::TransactionReadonly,
			indxdb::Error::KeyAlreadyExists => Error::TransactionKeyAlreadyExists,
			indxdb::Error::ValNotExpectedValue => Error::TransactionConditionNotMet,
			_ => Error::Transaction(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-tikv")]
impl From<tikv::Error> for Error {
	fn from(e: tikv::Error) -> Error {
		match e {
			tikv::Error::DuplicateKeyInsertion => Error::TransactionKeyAlreadyExists,
			tikv::Error::Grpc(_) => Error::ConnectionFailed(e.to_string()),
			tikv::Error::KeyError(ref ke) => {
				if let Some(conflict) = &ke.conflict {
					use crate::key::debug::Sprintable;
					Error::TransactionConflict(conflict.key.sprint())
				} else if ke.already_exist.is_some() {
					Error::TransactionKeyAlreadyExists
				} else if ke.abort.contains("KeyTooLarge") {
					Error::TransactionKeyTooLarge
				} else {
					Error::Transaction(e.to_string())
				}
			}
			tikv::Error::RegionError(ref re) if re.raft_entry_too_large.is_some() => {
				Error::TransactionTooLarge
			}
			_ => Error::Transaction(e.to_string()),
		}
	}
}

// Conversion from anyhow::Error for compatibility with existing code
impl From<anyhow::Error> for Error {
	fn from(e: anyhow::Error) -> Self {
		// Try to downcast to see if it's already a KVS error
		match e.downcast::<Error>() {
			Ok(e) => e,
			Err(e) => Error::Internal(e.to_string()),
		}
	}
}
