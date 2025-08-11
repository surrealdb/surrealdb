//! This library provides a low-level database library implementation, a remote
//! client and a query language definition, for [SurrealDB](https://surrealdb.com), the ultimate cloud database for
//! tomorrow's applications. SurrealDB is a scalable, distributed,
//! collaborative, document-graph database for the realtime web.
//!
//! This library can be used to start an [embedded](crate::engine::local)
//! in-memory datastore, an embedded datastore persisted to disk, a
//! browser-based embedded datastore backed by IndexedDB, or for connecting to a distributed [TiKV](https://tikv.org) key-value store.
//!
//! It also enables simple and advanced querying of a
//! [remote](crate::engine::remote) SurrealDB server from server-side or
//! client-side code. All connections to SurrealDB are made over WebSockets by
//! default, and automatically reconnect when the connection is terminated.

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(target_family = "wasm", feature = "ml"))]
compile_error!("The `ml` feature is not supported on Wasm.");

#[macro_use]
extern crate tracing;

mod api;

#[doc(hidden)]
/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::{Receiver, Sender, bounded, unbounded};
}

/// Different error types for embedded and remote databases
pub mod error {
	pub use crate::api::err::Error as Api;
	pub use crate::core::err::Error as Db;
}

#[cfg(feature = "protocol-http")]
#[doc(hidden)]
pub use crate::api::headers;
#[doc(inline)]
pub use crate::api::{
	Connect, Connection, Response, Surreal, engine, method, opt,
	value::{
		self, Action, Bytes, Datetime, Notification, Number, Object, RecordId, RecordIdKey, Value,
	},
};

/// A specialized `Result` type
pub type Result<T> = anyhow::Result<T>;
pub use anyhow::Error;
// Re-exporting core so it can be imported as `crate::core` so it's not grouped with third
// party crates.
use surrealdb_core as core;
