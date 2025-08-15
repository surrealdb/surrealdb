//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the
//! `tx.rs` file. This module enables the following operations on the key value
//! store:
//! - get
//! - set
//! - delete
//! - put
//!
//! These operations can be processed by the following storage engines:
//! - `fdb`: [FoundationDB](https://github.com/apple/foundationdb/) a
//!   distributed database designed to handle large volumes of structured data
//!   across clusters of commodity servers
//! - `indxdb`: WASM based database to store data in the browser
//! - `rocksdb`: [RocksDB](https://github.com/facebook/rocksdb) an embeddable
//!   persistent key-value store for fast storage
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and
//!   transactional key-value database
//! - `mem`: in-memory database

mod api;
mod batch;
pub(crate) mod cache;
mod cf;
mod clock;
mod ds;
pub mod export;
mod fdb;
mod indxdb;
mod key;
pub(crate) mod live;
mod mem;
mod node;
mod rocksdb;
pub(crate) mod savepoint;
mod scanner;
pub(crate) mod sequences;
mod stash;
mod surrealkv;
pub(crate) mod tasklease;
mod threadpool;
mod tikv;
mod tr;
mod tx;
mod util;
pub(crate) mod version;

#[cfg(test)]
mod tests;

#[cfg(not(target_family = "wasm"))]
pub(crate) mod index;

pub use ds::Datastore;
pub(crate) use key::{KVKey, KVValue, impl_kv_value_revisioned};
pub use live::Live;
pub use tr::{Check, LockType, TransactionType, Transactor};
pub use tx::Transaction;

#[cfg(not(target_family = "wasm"))]
pub(crate) use index::{ConsumeResult, IndexBuilder};

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type Key = Vec<u8>;

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// The Version part of a key-value pair. An alias for [`u64`].
pub type Version = u64;
