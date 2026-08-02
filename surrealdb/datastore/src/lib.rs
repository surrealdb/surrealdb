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
/// Below this crate the wrapping variant does not exist, so only the bare shape
/// can arrive; the layer above adds the wrapped one and delegates here.
pub fn storage_error(err: &anyhow::Error) -> Option<&surrealdb_kvs::Error> {
	err.downcast_ref::<surrealdb_kvs::Error>()
}

/// Whether a failure is a transaction conflict the caller may retry.
pub fn is_retryable_transaction_conflict(err: &anyhow::Error) -> bool {
	storage_error(err).is_some_and(surrealdb_kvs::Error::is_retryable)
}

/// Whether a failure reports that the storage engine is shutting down.
///
/// Transient from the cluster's perspective: the interrupted work is safe to
/// retry after the process restarts, so callers that persist failure state must
/// not record it as permanent.
pub fn is_shutdown_error(err: &anyhow::Error) -> bool {
	matches!(storage_error(err), Some(surrealdb_kvs::Error::Shutdown))
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
