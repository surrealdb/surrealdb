//! The SurrealDB datastore layer.
//!
//! This crate owns the keyspace: which keys exist, and the durable shapes their
//! values take. How a key encodes is one layer down, in `surrealdb-kvs`; what the
//! engine does with a decoded value is one layer up.
//!
//! This crate is an internal implementation detail of SurrealDB: its API is
//! unstable and changes without notice. Depend on the `surrealdb` SDK or
//! `surrealdb-core` instead.

// `fail!` reports a broken invariant the same way in every layer; imported
// crate-wide because nested modules do not inherit a file-level `use`.
// The moved modules spell their neighbours the way core did - `catalog::X`,
// `expr::X`, `val::X` - so the same aliases keep those paths valid here.
pub use surrealdb_catalog as catalog;
pub use surrealdb_expr::{expr, val};

#[macro_use]
extern crate common;
#[macro_use]
extern crate tracing;

#[macro_use]
mod mac;

pub mod cache;
pub mod close;
pub mod config;
pub mod factory;
pub mod index_state;
pub mod into;
pub mod key;
pub mod sequences;
pub mod tasklease;
pub mod tr;
pub mod tx;
pub mod util;
pub mod version;

pub use error::DatastoreError;

/// Recover a storage failure from an [`anyhow::Error`], whichever shape it took.
///
/// A storage failure reaches `anyhow` two ways: raised bare by the transactor,
/// or held as the cause of a layer error, which is how a function typed on that
/// layer's error re-raises it. Callers should not have to know which, so every
/// check goes through here rather than matching one shape and quietly missing
/// the other. Matching only the bare shape is the more dangerous mistake,
/// because it compiles, reads correctly, and turns a recognised condition into
/// an unrecognised one.
///
/// Recognition therefore walks the source chain instead of naming the wrappers:
/// they are declared in crates above this one, so they cannot be named from
/// here, and a wrapper that did not record the storage failure as its source
/// would not carry the cause to the client either. Following `source` is the
/// same link the wire mapping already depends on.
pub fn storage_error(err: &anyhow::Error) -> Option<&surrealdb_kvs::Error> {
	err.chain().find_map(|e| e.downcast_ref::<surrealdb_kvs::Error>())
}

/// Whether a failure is a transaction conflict the caller may retry.
pub fn is_retryable_transaction_conflict(err: &anyhow::Error) -> bool {
	storage_error(err).is_some_and(surrealdb_kvs::Error::is_retryable)
}

/// Whether a failure reports that the storage engine is shutting down.
///
/// Transient from the cluster's perspective: the interrupted work is safe to
/// retry after the process restarts, so callers that persist failure state
/// (such as the concurrent index builder) must not record it as permanent.
pub fn is_shutdown_error(err: &anyhow::Error) -> bool {
	matches!(storage_error(err), Some(surrealdb_kvs::Error::Shutdown))
}

/// Whether a failure reports a commit whose outcome is unknown — the
/// transaction may or may not have been applied.
///
/// Callers must not describe it as work that did not happen, and must not
/// retry it: the write may already exist, and replaying a non-idempotent
/// statement would apply it twice.
pub fn is_indeterminate_commit(err: &anyhow::Error) -> bool {
	matches!(storage_error(err), Some(surrealdb_kvs::Error::CommitOutcomeUnknown(_)))
}

#[cfg(test)]
mod storage_error_tests {
	use super::{is_retryable_transaction_conflict, is_shutdown_error, storage_error};

	/// Stands in for the layer errors above this crate, which hold a storage
	/// failure as their cause so a function typed on their own error can
	/// re-raise it. Those types cannot be named from here, which is the whole
	/// reason recognition follows `source` rather than matching them.
	#[derive(Debug, thiserror::Error)]
	#[error("There was a problem with the key-value store: {0}")]
	struct Wrapped(#[from] surrealdb_kvs::Error);

	#[derive(Debug, thiserror::Error)]
	#[error("not a storage failure")]
	struct Foreign;

	#[test]
	fn either_shape_is_recovered() {
		for err in [
			anyhow::Error::new(surrealdb_kvs::Error::TransactionKeyAlreadyExists),
			anyhow::Error::new(Wrapped(surrealdb_kvs::Error::TransactionKeyAlreadyExists)),
		] {
			assert!(
				matches!(
					storage_error(&err),
					Some(surrealdb_kvs::Error::TransactionKeyAlreadyExists)
				),
				"a key-already-exists failure went unrecognised: {err}"
			);
		}
	}

	#[test]
	fn either_shape_classifies_the_same() {
		for err in [
			anyhow::Error::new(surrealdb_kvs::Error::TransactionConflict("busy".to_string())),
			anyhow::Error::new(Wrapped(surrealdb_kvs::Error::TransactionConflict(
				"busy".to_string(),
			))),
		] {
			assert!(is_retryable_transaction_conflict(&err), "conflict not retryable: {err}");
			assert!(!is_shutdown_error(&err), "conflict misread as a shutdown: {err}");
		}
		for err in [
			anyhow::Error::new(surrealdb_kvs::Error::Shutdown),
			anyhow::Error::new(Wrapped(surrealdb_kvs::Error::Shutdown)),
		] {
			assert!(is_shutdown_error(&err), "shutdown not recognised: {err}");
			assert!(!is_retryable_transaction_conflict(&err), "shutdown misread as retryable");
		}
	}

	#[test]
	fn a_foreign_error_is_not_a_storage_failure() {
		let err = anyhow::Error::new(Foreign);
		assert!(storage_error(&err).is_none());
		assert!(!is_retryable_transaction_conflict(&err));
		assert!(!is_shutdown_error(&err));
	}
}

pub use config::TransactionConfig;
pub use factory::TransactionFactory;
pub use into::IntoBytes;
pub use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;
pub use surrealdb_kvs::{
	Direction, Error, Key, KeyRange, Result, TransactionType, Val, Version, api, consts, err,
	timestamp,
};
pub use tr::Transactor;
pub use tx::Transaction;
pub mod error;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
pub mod values;
