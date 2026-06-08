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
}

impl From<std::num::TryFromIntError> for Error {
	fn from(e: std::num::TryFromIntError) -> Error {
		Error::TimestampInvalid(e.to_string())
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
		// Strip trailing colon/whitespace; rocksdb's Status::Busy renders as "Resource busy: " with
		// an empty tail.
		let msg =
			e.to_string().trim_end_matches(|c: char| c == ':' || c.is_whitespace()).to_string();
		match e.kind() {
			rocksdb::ErrorKind::Busy => Error::TransactionConflict(msg),
			rocksdb::ErrorKind::TryAgain => Error::TransactionConflict(msg),
			_ => Error::Transaction(msg),
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
		const TIKV_TARGET: &str = "surrealdb::core::kvs::tikv";
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
					// Preserve aborts and retryable flags at debug-level so
					// operators can correlate generic transaction errors
					// back to the underlying TiKV cause without inflating
					// the error variant surface.
					tracing::debug!(
						target: TIKV_TARGET,
						abort = %ke.abort,
						retryable = ke.retryable,
						"TiKV KeyError",
					);
					Error::Transaction(e.to_string())
				}
			}
			tikv::Error::RegionError(ref re) => {
				// Most region errors carry a region id in their nested
				// `not_leader` / `region_not_found` / `epoch_not_match`
				// payload. Emit the most useful identifier we can find at
				// debug-level so a flood of generic Transaction errors can
				// still be traced to specific regions/stores during
				// post-mortem.
				let region_id = re
					.not_leader
					.as_ref()
					.map(|n| n.region_id)
					.or_else(|| re.region_not_found.as_ref().map(|n| n.region_id))
					.or_else(|| re.key_not_in_region.as_ref().map(|n| n.region_id))
					.or_else(|| {
						re.epoch_not_match
							.as_ref()
							.and_then(|n| n.current_regions.first().map(|r| r.id))
					});
				let store_id = re.store_not_match.as_ref().map(|n| n.request_store_id);
				let kind = if re.not_leader.is_some() {
					"not_leader"
				} else if re.region_not_found.is_some() {
					"region_not_found"
				} else if re.key_not_in_region.is_some() {
					"key_not_in_region"
				} else if re.epoch_not_match.is_some() {
					"epoch_not_match"
				} else if re.server_is_busy.is_some() {
					"server_is_busy"
				} else if re.stale_command.is_some() {
					"stale_command"
				} else if re.store_not_match.is_some() {
					"store_not_match"
				} else if re.raft_entry_too_large.is_some() {
					"raft_entry_too_large"
				} else if re.disk_full.is_some() {
					"disk_full"
				} else {
					"other"
				};
				tracing::debug!(
					target: TIKV_TARGET,
					kind,
					message = %re.message,
					region_id = ?region_id,
					store_id = ?store_id,
					"TiKV RegionError",
				);
				if re.raft_entry_too_large.is_some() {
					Error::TransactionTooLarge
				} else {
					Error::Transaction(e.to_string())
				}
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
