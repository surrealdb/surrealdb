//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the `tx.rs` file.
//! This module enables the following operations on the key value store:
//! - get
//! - set
//! - delete
//! - put
//!
//! These operations can be processed by the following storage engines:
//! - `fdb`: [FoundationDB](https://github.com/apple/foundationdb/) a distributed database designed to handle large volumes of structured data across clusters of commodity servers
//! - `indxdb`: WASM based database to store data in the browser
//! - `rocksdb`: [RocksDB](https://github.com/facebook/rocksdb) an embeddable persistent key-value store for fast storage
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and transactional key-value database
//! - `mem`: in-memory database

pub mod export;

mod api;
mod batch;
mod cf;
mod clock;
mod ds;
mod key;
mod live;
mod node;
mod scanner;
mod stash;
mod threadpool;
mod tr;
mod tx;
pub(crate) mod version;

mod fdb;
mod indxdb;
mod mem;
mod rocksdb;
mod surrealcs;
mod surrealkv;
mod tikv;

pub(crate) mod cache;

pub(crate) mod index;
#[cfg(any(
	feature = "kv-tikv",
	feature = "kv-fdb",
	feature = "kv-indxdb",
	feature = "kv-surrealcs",
))]
mod savepoint;
pub(crate) mod slowlog;
pub(crate) mod tasklease;
#[cfg(test)]
mod tests;
mod util;

pub(crate) use key::impl_key;
pub use key::{KeyDecode, KeyDecodeOwned, KeyEncode};

pub use ds::Datastore;
pub use live::Live;
pub use tr::{Check, LockType, TransactionType, Transactor};
pub use tx::Transaction;

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type Key = Vec<u8>;

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// The Version part of a key-value pair. An alias for [`u64`].
pub type Version = u64;
