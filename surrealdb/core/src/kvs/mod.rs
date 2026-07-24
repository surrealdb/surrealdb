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
pub(crate) use surrealdb_kvs::{api, consts, err, timestamp};
pub use surrealdb_kvs_any::{BackendProvider, Backends, ConnectContext};

pub mod export;

mod clock;
mod ds;
mod into;
mod tr;
mod tx;

pub(crate) mod util;

pub(crate) mod cache;
pub(crate) mod index;
pub(crate) mod sequences;
pub(crate) mod slowlog;
pub(crate) mod tasklease;
pub(crate) mod version;

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
pub(crate) use ds::TransactionFactory;
pub use ds::{
	Builder, Datastore, Metric, Metrics, TransactionBuilder, TransactionBuilderFactory,
	TransactionBuilderParts,
};
pub use err::{Error, Result};
pub use into::IntoBytes;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]
pub use timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, HlcTimeStamp, HlcTimeStampImpl, IncTimeStampImpl,
	MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
pub use tr::Transactor;
pub(crate) use tx::CachePolicy;
pub use tx::Transaction;

pub(crate) fn is_retryable_transaction_conflict(err: &anyhow::Error) -> bool {
	if let Some(kvs_err) = err.downcast_ref::<self::err::Error>() {
		return kvs_err.is_retryable();
	}
	matches!(
		err.downcast_ref::<crate::err::Error>(),
		Some(crate::err::Error::Kvs(kvs_err)) if kvs_err.is_retryable()
	)
}

/// Whether an error reports that the storage engine is shutting down.
///
/// Shutdown-class failures are transient from the cluster's perspective: the
/// interrupted work is safe to retry after the process restarts. Callers that
/// persist failure state (such as the concurrent index builder) must not
/// record them as permanent errors.
pub(crate) fn is_shutdown_error(err: &anyhow::Error) -> bool {
	if matches!(err.downcast_ref::<self::err::Error>(), Some(self::err::Error::Shutdown)) {
		return true;
	}
	matches!(
		err.downcast_ref::<crate::err::Error>(),
		Some(crate::err::Error::Kvs(self::err::Error::Shutdown))
	)
}

#[cfg(test)]
pub(crate) mod testing {
	use std::collections::HashMap;
	use std::sync::{Mutex, OnceLock};

	use anyhow::Result;
	use uuid::Uuid;

	#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
	pub(crate) enum RetryableConflictSite {
		ConcurrentIndexInitialCleanup,
		ConcurrentIndexInitialBatch,
		ConcurrentIndexReservationRelease,
		IndexCompactionQueueCleanup,
		FullTextCompaction,
		CountCompaction,
		HnswCompaction,
	}

	#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
	// Site names follow the `RetryableConflictSite` convention of naming the
	// subsystem and injection point, even when the prefixes coincide.
	#[allow(clippy::enum_variant_names)]
	pub(crate) enum NonRetryableErrorSite {
		ConcurrentIndexAfterReservationRegistration,
		ConcurrentIndexReservationRelease,
		ConcurrentIndexCountTailCommitted,
		ConcurrentIndexInitialBatchCommit,
		/// Simulates an initial-scan batch commit interrupted by datastore
		/// shutdown (the `Shutdown` error the storage engines surface once
		/// graceful shutdown has begun).
		ConcurrentIndexInitialBatchShutdown,
		/// Simulates the initial scan crossing the process memory threshold
		/// (the error `Building::is_beyond_threshold` raises under memory
		/// pressure).
		ConcurrentIndexInitialBatchMemoryThreshold,
	}

	static RETRYABLE_CONFLICTS: OnceLock<Mutex<HashMap<(RetryableConflictSite, Uuid), usize>>> =
		OnceLock::new();
	static NON_RETRYABLE_ERRORS: OnceLock<Mutex<HashMap<(NonRetryableErrorSite, Uuid), usize>>> =
		OnceLock::new();

	fn retryable_conflicts() -> &'static Mutex<HashMap<(RetryableConflictSite, Uuid), usize>> {
		RETRYABLE_CONFLICTS.get_or_init(|| Mutex::new(HashMap::new()))
	}

	fn non_retryable_errors() -> &'static Mutex<HashMap<(NonRetryableErrorSite, Uuid), usize>> {
		NON_RETRYABLE_ERRORS.get_or_init(|| Mutex::new(HashMap::new()))
	}

	pub(crate) fn inject_retryable_conflict(
		site: RetryableConflictSite,
		node_id: Uuid,
	) -> RetryableConflictGuard {
		inject_retryable_conflicts(site, node_id, 1)
	}

	pub(crate) fn inject_retryable_conflicts(
		site: RetryableConflictSite,
		node_id: Uuid,
		count: usize,
	) -> RetryableConflictGuard {
		assert!(count > 0);
		retryable_conflicts().lock().unwrap().insert((site, node_id), count);
		RetryableConflictGuard {
			site,
			node_id,
		}
	}

	pub(crate) fn maybe_inject_retryable_conflict(
		site: RetryableConflictSite,
		node_id: Uuid,
	) -> Result<()> {
		let mut conflicts = retryable_conflicts().lock().unwrap();
		let Some(remaining) = conflicts.get_mut(&(site, node_id)) else {
			return Ok(());
		};
		*remaining -= 1;
		if *remaining == 0 {
			conflicts.remove(&(site, node_id));
		}
		Err(super::Error::TransactionConflict(format!("injected conflict at {site:?}")).into())
	}

	pub(crate) fn retryable_conflict_count(site: RetryableConflictSite, node_id: Uuid) -> usize {
		retryable_conflicts().lock().unwrap().get(&(site, node_id)).copied().unwrap_or(0)
	}

	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) fn inject_non_retryable_error(
		site: NonRetryableErrorSite,
		node_id: Uuid,
	) -> NonRetryableErrorGuard {
		inject_non_retryable_errors(site, node_id, 1)
	}

	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) fn inject_non_retryable_errors(
		site: NonRetryableErrorSite,
		node_id: Uuid,
		count: usize,
	) -> NonRetryableErrorGuard {
		assert!(count > 0);
		non_retryable_errors().lock().unwrap().insert((site, node_id), count);
		NonRetryableErrorGuard {
			site,
			node_id,
		}
	}

	pub(crate) fn maybe_inject_non_retryable_error(
		site: NonRetryableErrorSite,
		node_id: Uuid,
	) -> Result<()> {
		let mut errors = non_retryable_errors().lock().unwrap();
		let Some(remaining) = errors.get_mut(&(site, node_id)) else {
			return Ok(());
		};
		*remaining -= 1;
		if *remaining == 0 {
			errors.remove(&(site, node_id));
		}
		// Transient-interruption sites reproduce the exact errors the builder
		// classifies specially, so tests exercise those paths end to end.
		match site {
			// The error a batch commit surfaces once graceful shutdown has
			// begun: the pre-apply gate refuses the commit with `Shutdown`.
			NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown => {
				Err(super::Error::Shutdown.into())
			}
			// The error the builder raises when the process crosses the
			// memory threshold.
			NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold => {
				Err(crate::err::Error::QueryBeyondMemoryThreshold.into())
			}
			_ => {
				Err(super::Error::Internal(format!("injected non-retryable error at {site:?}"))
					.into())
			}
		}
	}

	pub(crate) struct RetryableConflictGuard {
		site: RetryableConflictSite,
		node_id: Uuid,
	}

	impl Drop for RetryableConflictGuard {
		fn drop(&mut self) {
			retryable_conflicts().lock().unwrap().remove(&(self.site, self.node_id));
		}
	}

	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) struct NonRetryableErrorGuard {
		site: NonRetryableErrorSite,
		node_id: Uuid,
	}

	impl Drop for NonRetryableErrorGuard {
		fn drop(&mut self) {
			non_retryable_errors().lock().unwrap().remove(&(self.site, self.node_id));
		}
	}
}

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
