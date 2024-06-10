//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the `tx.rs` file.
//! This module enables the following operations on the key value store:
//! - get
//! - set
//! - delete
//! - put
//! These operations can be processed by the following storage engines:
//! - `fdb`: [FoundationDB](https://github.com/apple/foundationdb/) a distributed database designed to handle large volumes of structured data across clusters of commodity servers
//! - `indxdb`: WASM based database to store data in the browser
//! - `rocksdb`: [RocksDB](https://github.com/facebook/rocksdb) an embeddable persistent key-value store for fast storage
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and transactional key-value database
//! - `mem`: in-memory database

mod api;
mod batch;
mod cache;
mod clock;
mod ds;
mod export;
mod nd;
mod scanner;
mod stash;
mod tr;
mod tx;

mod fdb;
mod indxdb;
mod kv;
mod mem;
mod rocksdb;
mod surrealkv;
mod tikv;

pub(crate) mod lq_structs;

#[cfg(test)]
mod tests;

pub use self::ds::*;
pub use self::kv::*;
pub use self::tr::*;
pub use self::tx::*;
