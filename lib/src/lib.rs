//! This library provides the low-level database library implementation, and query language
//! definition, for [SurrealDB](https://surrealdb.com), the ultimate cloud database for
//! tomorrow's applications. SurrealDB is a scalable, distributed, collaborative, document-graph
//! database for the realtime web.
//!
//! This library can be used to start an embedded in-memory datastore, an embedded datastore
//! persisted to disk, a browser-based embedded datastore backed by IndexedDB, or for connecting
//! to a distributed [TiKV](https://tikv.org) key-value store.

#[cfg(feature = "compute")]
#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cnf;
#[cfg(feature = "compute")]
mod ctx;
#[cfg(feature = "compute")]
mod dbs;
#[cfg(feature = "compute")]
mod doc;
mod err;
#[cfg(all(feature = "compute", feature = "parallel"))]
mod exe;
#[cfg(feature = "compute")]
mod fnc;
#[cfg(feature = "compute")]
mod key;
#[cfg(feature = "compute")]
mod kvs;

#[cfg(feature = "compute")]
pub mod env;
// SQL
pub mod sql;

// Exports
#[cfg(feature = "compute")]
pub use dbs::{Auth, Response, Session};
pub use err::Error;
#[cfg(feature = "compute")]
pub use kvs::{Datastore, Key, Transaction, Val};

// Re-exports
#[cfg(feature = "compute")]
pub mod channel {
	pub use channel::bounded as new;
	pub use channel::Receiver;
	pub use channel::Sender;
}
