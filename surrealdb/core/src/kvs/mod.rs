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
// Recognising a storage failure descends with the transaction layer: the code
// that classifies one sits on both sides of the crate boundary. Both shapes stay
// recognised from below because recognition follows the source chain instead of
// naming `crate::err::Error::Kvs`, which is the wrapper the layer above puts on
// a storage failure re-raised from a function typed on core's error.
pub(crate) use surrealdb_datastore::{
	is_retryable_transaction_conflict, is_shutdown_error, storage_error,
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
