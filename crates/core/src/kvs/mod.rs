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

pub mod export;

mod api;
mod batch;
mod cf;
mod clock;
mod ds;
mod err;
mod into;
mod key;
mod node;
mod scanner;
mod threadpool;
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

pub use api::Transactable;
pub use clock::SizedClock;
pub use ds::requirements::{TransactionBuilderFactoryRequirements, TransactionBuilderRequirements};
pub use ds::{Datastore, DatastoreFlavor, TransactionBuilder, TransactionBuilderFactory};
pub use err::Error;
pub use into::IntoBytes;
pub(crate) use key::{KVKey, KVValue, impl_kv_key_storekey, impl_kv_value_revisioned};
pub use tr::{LockType, TransactionType, Transactor};
pub use tx::Transaction;

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type Key = Vec<u8>;

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// The Version part of a key-value pair. An alias for [`u64`].
pub type Version = u64;
