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

pub mod config;
pub mod export;

mod api;
mod batch;
mod clock;
mod consts;
mod direction;
mod ds;
mod err;
mod into;
mod key;
mod threadpool;
mod timestamp;
mod tr;
mod tx;
mod util;

mod indxdb;
mod mem;
mod rocksdb;
mod surrealkv;
mod tikv;

#[cfg(test)]
mod tests;

pub(crate) mod cache;
pub(crate) mod index;
pub(crate) mod sequences;
pub(crate) mod slowlog;
pub(crate) mod tasklease;
pub(crate) mod version;

pub use api::{
	GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanLimit, ScanResult, Transactable,
};
pub use consts::{
	COUNT_BATCH_SIZE, ESTIMATED_BYTES_PER_KEY, ESTIMATED_BYTES_PER_KV, INDEXING_BATCH_SIZE,
	NORMAL_BATCH_SIZE,
};
pub use direction::Direction;
pub(crate) use ds::TransactionFactory;
pub use ds::requirements::{TransactionBuilderFactoryRequirements, TransactionBuilderRequirements};
pub use ds::{
	Builder, Datastore, DatastoreFlavor, Metric, Metrics, TransactionBuilder,
	TransactionBuilderFactory, TransactionBuilderParts,
};
pub use err::{Error, Result};
pub use into::IntoBytes;
pub(crate) use key::{KVKey, KVValue, impl_kv_key_storekey, impl_kv_value_revisioned};
pub use timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, HlcTimeStamp, HlcTimeStampImpl, IncTimeStampImpl,
	MAX_TIMESTAMP_BYTES, TimeStamp, TimeStampImpl,
};
pub use tr::{LockType, TransactionType, Transactor};
pub(crate) use tx::CachePolicy;
pub use tx::Transaction;

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type Key = Vec<u8>;

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// The Version part of a key-value pair. An alias for [`u64`].
pub type Version = u64;

pub(crate) fn is_retryable_transaction_conflict(err: &anyhow::Error) -> bool {
	if let Some(kvs_err) = err.downcast_ref::<self::err::Error>() {
		return kvs_err.is_retryable();
	}
	matches!(
		err.downcast_ref::<crate::err::Error>(),
		Some(crate::err::Error::Kvs(kvs_err)) if kvs_err.is_retryable()
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
	pub(crate) enum NonRetryableErrorSite {
		ConcurrentIndexAfterReservationRegistration,
		ConcurrentIndexReservationRelease,
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

	pub(crate) fn inject_non_retryable_error(
		site: NonRetryableErrorSite,
		node_id: Uuid,
	) -> NonRetryableErrorGuard {
		inject_non_retryable_errors(site, node_id, 1)
	}

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
		Err(super::Error::Internal(format!("injected non-retryable error at {site:?}")).into())
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
