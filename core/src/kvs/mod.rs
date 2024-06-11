//! The module defining the key value store.
//! Everything related the transaction for the key value store is defined in the `tx.rs` file.
//! This module enables the following operations on the key value store:
//! - get
//! - set
//! - delete
//! - put
//! These operations can be processed by the following storage engines:
//! - `indxdb`: WASM based database to store data in the browser
//! - `rocksdb`: [RocksDB](https://github.com/facebook/rocksdb) an embeddable persistent key-value store for fast storage
//! - `tikv`: [TiKV](https://github.com/tikv/tikv) a distributed, and transactional key-value database
//! - `mem`: in-memory database
mod cache;
mod clock;
mod ds;
mod indxdb;
mod kv;
mod mem;
mod rocksdb;
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
