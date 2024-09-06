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
mod live;
mod node;
mod scanner;
mod stash;
mod tr;
mod tx;
mod version;

mod fdb;
mod indxdb;
mod kv;
mod mem;
mod rocksdb;
mod surrealcs;
mod surrealkv;
mod tikv;

#[cfg(not(target_arch = "wasm32"))]
mod index;
mod savepoint;
#[cfg(test)]
mod tests;

pub use self::ds::*;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use self::index::*;
pub use self::kv::*;
pub use self::live::*;
pub use self::tr::*;
pub use self::tx::*;
