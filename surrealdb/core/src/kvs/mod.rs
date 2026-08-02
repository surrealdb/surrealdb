//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the `tx.rs` file.
//! This module enables the following operations on the key value store:
//! - get
//! - set
//! - delete
//! - put
//!
//! These operations can be processed by the following storage engines:
//! - `indxdb`: WASM based database to store data in the browser
//! - `rocksdb`: [RocksDB](https://github.com/facebook/rocksdb) an embeddable persistent key-value
//!   store for fast storage
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and transactional key-value
//!   database
//! - `mem`: in-memory database

pub use surrealdb_kvs::{Direction, TransactionType, Val, Version};
pub(crate) use surrealdb_kvs::{api, consts, err};
pub use surrealdb_kvs_any::{BackendProvider, Backends, ConnectContext};

pub mod export;

mod clock;
pub(crate) mod ds;

#[cfg(test)]
pub(crate) mod compat;
pub(crate) mod index;
pub(crate) mod migration;
pub(crate) mod slowlog;

#[cfg(test)]
mod tests;

pub use api::{
	GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanResult, Transactable,
};
pub use consts::{
	COUNT_BATCH_SIZE, ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV,
	INDEX_COMPACTION_QUEUE_BATCH_SIZE, INDEXING_BATCH_MAX_BYTES, INDEXING_BATCH_SIZE,
	INDEXING_PROBE_BATCH_SIZE, NORMAL_BATCH_SIZE,
};
pub use ds::{
	Builder, Datastore, LiveQueryEngine, Metric, Metrics, TransactionBuilder,
	TransactionBuilderFactory, TransactionBuilderParts,
};
pub use err::{Error, Result};
// Named for the layer rather than re-exported as `Error`: `kvs::Error` above is
// the storage backend's, and a datastore failure is a different thing.
pub(crate) use surrealdb_datastore::error::DatastoreError;
pub use surrealdb_datastore::{IntoBytes, into};
// The transaction layer and the keyspace live one crate down now; core reaches
// them through these so no call site had to change.
pub(crate) use surrealdb_datastore::{
	TransactionConfig, TransactionFactory, cache, sequences, tasklease, tr, tx, util, version,
};
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]
pub use surrealdb_kvs::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, HlcTimeStamp, HlcTimeStampImpl, IncTimeStampImpl,
	MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
pub use tr::Transactor;
pub use tx::Transaction;

pub(crate) use crate::catalog::providers::CachePolicy;

/// Recover a storage failure from an [`anyhow::Error`], whichever shape it took.
///
/// A storage error reaches `anyhow` two ways: raised bare by the transactor, or
/// wrapped in [`crate::err::Error::Kvs`] by a function typed on core's error.
/// Callers should not have to know which, so every check goes through here rather
/// than matching one shape and quietly missing the other. Matching only the
/// wrapped shape is the more dangerous mistake, because it compiles, reads
/// correctly, and turns a recognised condition into an unrecognised one.
pub(crate) fn storage_error(err: &anyhow::Error) -> Option<&self::err::Error> {
	if let Some(kvs_err) = err.downcast_ref::<self::err::Error>() {
		return Some(kvs_err);
	}
	match err.downcast_ref::<crate::err::Error>() {
		Some(crate::err::Error::Kvs(kvs_err)) => Some(kvs_err),
		_ => None,
	}
}

pub(crate) fn is_retryable_transaction_conflict(err: &anyhow::Error) -> bool {
	storage_error(err).is_some_and(self::err::Error::is_retryable)
}

/// Whether a conditional write (`put_compare_key` / `del_compare_key`) failed
/// because its condition was not met — the key already existed, was deleted,
/// or changed since the guard value was read. On last-writer-wins backends
/// (TiKV) the condition is validated at commit, so this can surface from the
/// conditional call itself or from the subsequent `commit`; callers must check
/// both. This is how the durable RPC session writes stay atomic on TiKV, where
/// a blind `set`/`clr` is last-writer-wins (see the `multiwriter_same_keys_*`
/// KV coverage).
///
/// The error arrives either as a bare [`Error`] or wrapped, which
/// [`storage_error`] unifies — mirroring [`is_retryable_transaction_conflict`].
pub(crate) fn is_conditional_write_conflict(err: &anyhow::Error) -> bool {
	fn is_condition_error(e: &self::err::Error) -> bool {
		matches!(
			e,
			self::err::Error::TransactionConditionNotMet
				| self::err::Error::TransactionKeyAlreadyExists
		)
	}
	storage_error(err).is_some_and(is_condition_error)
}

/// Whether an error reports that the storage engine is shutting down.
///
/// Shutdown-class failures are transient from the cluster's perspective: the
/// interrupted work is safe to retry after the process restarts. Callers that
/// persist failure state (such as the concurrent index builder) must not
/// record them as permanent errors.
pub(crate) fn is_shutdown_error(err: &anyhow::Error) -> bool {
	matches!(storage_error(err), Some(self::err::Error::Shutdown))
}

// The fault-injection registry descends with the transaction layer: the code that
// consults it is on both sides of the crate boundary, and a registry duplicated
// per crate would have the test inject into one and the engine read the other.
#[cfg(test)]
pub(crate) use surrealdb_datastore::testing;

#[cfg(test)]
mod retry_conflict_tests {
	use super::is_retryable_transaction_conflict;

	#[test]
	fn retryable_transaction_conflict_accepts_direct_kvs_error() {
		let err = anyhow::Error::new(super::Error::TransactionConflict("conflict".into()));

		assert!(is_retryable_transaction_conflict(&err));
	}

	#[test]
	fn retryable_transaction_conflict_accepts_wrapped_kvs_error() {
		let err = anyhow::Error::new(crate::err::Error::Kvs(super::Error::TransactionConflict(
			"conflict".into(),
		)));

		assert!(is_retryable_transaction_conflict(&err));
	}

	#[test]
	fn retryable_transaction_conflict_rejects_non_retryable_errors() {
		let err = anyhow::Error::new(super::Error::TransactionFinished);

		assert!(!is_retryable_transaction_conflict(&err));
	}
}
