//! # SurrealDB KVS
//!
//! Common types and traits shared by every SurrealDB key-value store backend:
//! the [`Transactable`] transaction trait, the [`TransactionBuilder`] datastore
//! abstraction, the raw [`Key`]/[`KeyRange`]/[`Val`] byte types, and the shared
//! error, configuration, cursor, and timestamp machinery.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

pub mod api;
pub mod builder;
pub mod config;
pub mod consts;
pub mod cursor;
pub mod err;
pub mod savepoint;
pub mod threadpool;
pub mod timestamp;
mod types;

pub use api::{
	Batch, GetMultiResult, KeysResult, ScanCursorKeys, ScanCursorVals, ScanResult, Transactable,
};
pub use builder::{Metric, Metrics, TransactionBuilder};
pub use err::{Error, Result};
pub use savepoint::SavepointStack;
pub use types::{Key, KeyRange};

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// The Version part of a key-value pair. An alias for [`u64`].
pub type Version = u64;

/// Specifies whether the transaction is read-only or writeable.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum TransactionType {
	Read,
	Write,
}

/// The direction of a scan over a key range.
///
/// Lives in its own module rather than inside the cursor or scanner code
/// because every layer (backends, exec operators, indices, doc machinery)
/// needs to name it and they shouldn't all depend on the storage backend
/// or stream-scanner modules to do so.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
	/// Iterate from `range.start` toward `range.end` (lex-ascending).
	Forward,
	/// Iterate from `range.end - 1` toward `range.start` (lex-descending).
	Backward,
}
