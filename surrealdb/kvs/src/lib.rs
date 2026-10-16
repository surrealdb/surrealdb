//! # SurrealDB KVS
//!
//! Common types and traits shared by every SurrealDB key-value store backend:
//! the [`Transactable`] transaction trait, the [`TransactionBuilder`] datastore
//! abstraction, the raw [`Key`]/[`KeyRange`]/[`Val`] byte types, the
//! [`key`]/[`value`] encoding contract a typed key and its stored value are
//! written against, and the shared error, configuration, cursor, and timestamp
//! machinery.
//!
//! This crate defines how a key and a value are *declared*; it does not declare
//! any. Which keys exist is the keyspace, which lives above it.
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
pub mod destroy;
pub mod err;
pub mod key;
pub mod page;
pub mod release;
pub mod savepoint;
pub mod snapshot;
pub mod split;
pub mod threadpool;
pub mod timestamp;
mod types;
pub mod value;

pub use api::{
	Batch, CountCost, GetMultiResult, KeysResult, RangeKeyVisitor, ScanCursorKeys, ScanCursorVals,
	ScanResult, Transactable,
};
pub use builder::{Metric, Metrics, TransactionBuilder};
pub use destroy::{DestroyRange, DestroyRangeHandle};
pub use err::{Error, Result};
pub use page::Page;
pub use release::{FeatureLevel, Release};
pub use savepoint::SavepointStack;
pub use snapshot::{RangeCursor, Resume, Snapshot};
pub use split::{Estimate, SplitTarget};
pub use types::{Key, KeyRange};
pub use value::{EncodeContext, Encoded};

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
/// Defined here so every layer (backends, exec operators, indices, doc
/// machinery) can name it without depending on a storage backend or a
/// stream-scanner module.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
	/// Iterate from `range.start` toward `range.end` (lex-ascending).
	Forward,
	/// Iterate from `range.end - 1` toward `range.start` (lex-descending).
	Backward,
}

/// Names the direction as a query plan reports it. These two strings appear
/// in `EXPLAIN` output that clients read, so they are part of the public
/// surface and must not change.
impl std::fmt::Display for Direction {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Direction::Forward => f.write_str("forward"),
			Direction::Backward => f.write_str("backward"),
		}
	}
}
