//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the `tx.rs` file.
//! This module enables the following operations on the key value store:
//! - get
//! - set
//! - delete
//! - put
//!
//! These operations can be processed by the following storage engines:
//! - `surrealkv`: [SurrealKV](https://github.com/surrealdb/surrealkv) an embeddable persistent
//!   key-value store, backed by the file system or, in the browser, the Origin Private File System
//! - `mem`: in-memory database

pub(crate) use surrealdb_kvs::{Direction, TransactionBuilder, TransactionType, Val, consts, err};

pub(crate) mod export;
pub mod tasks;

mod clock;
pub mod copy;
pub(crate) mod ds;

#[cfg(test)]
pub(crate) mod compat;
pub(crate) mod index;
pub(crate) mod lightweight;
pub(crate) mod migration;
pub(crate) mod paging;
pub mod request;
mod rollback;
pub(crate) mod slowlog;
pub(crate) mod walk;
pub(crate) mod write_barrier;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use consts::{
	CHANGEFEED_GC_BATCH_SIZE, CHANGEFEED_GC_PASS_KEY_BUDGET, CHANGEFEED_GC_SLICE_STEPS,
	EVENT_ADOPTION_PAGE_SIZE, EVENT_CLAIM_TTL, EVENT_PROCESSING_SLICE_BATCHES,
	GRAPH_FOLD_SLICE_BATCHES, GRAPH_FOLD_SLICE_PASSES_PER_SCOPE, INDEX_COMPACTION_QUEUE_BATCH_SIZE,
	INDEX_COMPACTION_SLICE_BATCHES, INDEX_RESUME_PAGE_SIZE, INDEXING_BATCH_MAX_BYTES,
	INDEXING_BATCH_SIZE, INDEXING_PROBE_BATCH_SIZE, INDEXING_SCAN_BATCH_SIZE,
	LIVE_QUERY_ROUTER_SLICE_BATCHES, NODE_CLEANUP_SLICE_BATCHES, NODE_EXPIRE_SLICE_BATCHES,
	NORMAL_BATCH_SIZE, QUEUE_CLEANUP_CONFLICT_RETRIES, RECLAIM_BATCH_SIZE,
	RECLAIM_PASS_ENTRY_QUOTA, RECLAIM_PASS_KEY_BUDGET, RECLAIM_PASS_WALK_BATCHES,
	RPC_SESSION_GC_SLICE_BATCHES,
};
pub use ds::{
	Builder, Datastore, GraphFoldRun, IndexBuildsRun, IndexCompactionRun, LiveQueryEngine,
	ReclaimRun, StatementCacheStats, TransactionBuilderFactory, TransactionBuilderParts,
	WriterLeaseRelease,
};
pub(crate) use ds::{DocIdsReclaimPending, DocIdsReclaimStep};
pub(crate) use err::Error;
pub use request::{
	BeginRequest, Dialect, QueryRequest, QuerySource, ReadOnlyOptions, snapshot_description,
};
pub use rollback::{
	EntryFinding, GenerationFinding, LedgerFinding, RollbackCheck, RollbackVerdict,
};
// The types of the feature level API on [`Datastore`]: the releases it names and the state it
// reports.
pub use surrealdb_datastore::FeatureLevelState;
pub(crate) use surrealdb_datastore::IntoBytes;
pub use surrealdb_datastore::ReadRequest;

/// Whether `error` is a refusal to open a transaction at an isolation level the
/// datastore does not offer.
pub fn is_isolation_not_supported(error: &anyhow::Error) -> bool {
	matches!(
		error.downcast_ref::<DatastoreError>(),
		Some(DatastoreError::IsolationNotSupported { .. })
	)
}
// Named for the layer rather than re-exported as `Error`: `kvs::Error` above is
// the storage backend's, and a datastore failure is a different thing.
pub(crate) use surrealdb_datastore::error::DatastoreError;
// The transaction layer and the keyspace live one crate down, in
// `surrealdb-datastore`; these bind them to core's own `crate::kvs::…` paths.
// Depend on that crate directly to reach them from outside core.
pub(crate) use surrealdb_datastore::{
	Admission, LayoutSet, RowGate, TransactionConfig, TransactionFactory, cache, catalog_reads,
	creations, sequences, speculation, tasklease, tx, util, version,
};
// Recognising a storage failure descends with the transaction layer: the code
// that classifies one sits on both sides of the crate boundary. Both shapes stay
// recognised from below because recognition follows the source chain instead of
// naming `crate::err::Error::Kvs`, which is the wrapper the layer above puts on
// a storage failure re-raised from a function typed on core's error.
pub(crate) use surrealdb_datastore::{
	is_indeterminate_commit, is_retryable_transaction_conflict, is_shutdown_error, storage_error,
};
pub use surrealdb_kvs::{FeatureLevel, Isolation, Release, SnapshotInfo};
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
