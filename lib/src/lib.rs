//! This library provides an easy-to-use client for [SurrealDB](https://surrealdb.com),
//! the ultimate cloud database for tomorrow's applications. SurrealDB is a scalable,
//! distributed, collaborative, document-graph database for the realtime web.
//!
//! This library can be used to start an embedded in-memory datastore, an embedded
//! datastore persisted to disk, or for connecting to a distributed [TiKV](https://tikv.org)
//! key-value store.

#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cnf;
mod ctx;
mod dbs;
mod doc;
mod err;
mod fnc;
mod key;
mod kvs;

// SQL
pub mod sql;

// Exports
pub use dbs::Auth;
pub use dbs::Response;
pub use dbs::Session;
pub use err::Error;
pub use kvs::Datastore;
pub use kvs::Key;
pub use kvs::Transaction;
pub use kvs::Val;

// Re-exports
pub mod channel {
	pub use channel::bounded as new;
	pub use channel::Receiver;
	pub use channel::Sender;
}
