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
//! - `speedb`: [SpeedyDB](https://github.com/speedb-io/speedb) fork of rocksDB making it faster (Redis is using speedb but this is not acid transactions)
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and transactional key-value database
//! - `mem`: in-memory database
mod cache;
mod clock;
mod ds;
mod fdb;
mod indxdb;
mod kv;
mod mem;
mod rocksdb;
mod speedb;
mod surrealkv;
mod tikv;
mod tx;

pub(crate) mod lq_structs;

mod lq_cf;
mod lq_v2_doc;
mod lq_v2_fut;
#[cfg(test)]
mod tests;

pub use self::ds::*;
pub use self::kv::*;
pub use self::tx::*;
