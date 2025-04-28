//! This library provides a low-level database library implementation, a remote client
//! and a query language definition, for [SurrealDB](https://surrealdb.com), the ultimate cloud database for
//! tomorrow's applications. SurrealDB is a scalable, distributed, collaborative, document-graph
//! database for the realtime web.
//!
//! This library can be used to start an embedded in-memory datastore, an embedded datastore
//! persisted to disk, a browser-based embedded datastore backed by IndexedDB, or for connecting
//! to a distributed [TiKV](https://tikv.org) key-value store.
//!
//! It also enables simple and advanced querying of a remote SurrealDB server from
//! server-side or client-side code. All connections to SurrealDB are made over WebSockets by default,
//! and automatically reconnect when the connection is terminated.
//!
//! # Running SurrealDB embedded in Rust
//!
//! When running SurrealDB as an embedded database within Rust, using the correct release profile and memory allocator can greatly improve the performance of the database core engine. In addition using an optimised asynchronous runtime configuration can help speed up concurrent queries and increase database throughput.
//!
//! In your project’s Cargo.toml file, ensure that the release profile uses the following configuration:
//!
//! ```toml
//! [profile.release]
//! lto = true
//! strip = true
//! opt-level = 3
//! panic = 'abort'
//! codegen-units = 1
//! ```
//!
//! In your project’s Cargo.toml file, ensure that the allocator feature is among those enabled on the surrealdb dependency:
//!
//! ```toml
//! surrealdb = { version = "2", features = ["allocator", "storage-mem", "storage-surrealkv", "storage-rocksdb", "protocol-http", "protocol-ws", "rustls"] }
//! ```
//!
//! When running SurrealDB within your Rust code, ensure that the asynchronous runtime is configured correctly, making use of multiple threads, an increased stack size, and an optimised number of threads:
//!
//! ```no_run
//! // tokio = { version = "1.41.1", features = ["sync", "rt-multi-thread"] }
//! fn main() {
//!     tokio::runtime::Builder::new_multi_thread()
//!     .enable_all()
//!     .thread_stack_size(10 * 1024 * 1024) // 10MiB
//!     .build()
//!     .unwrap()
//!     .block_on(async {
//!       // Your application code
//!     })
//! }
//! ``````
//!
//! # Examples
//!
//! ```no_run
//! use std::borrow::Cow;
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use surrealdb::{Error, Surreal};
//! use surrealdb::opt::auth::Root;
//! use surrealdb::engine::remote::ws::Ws;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: String,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! // Pro tip: Replace String with Cow<'static, str> to
//! // avoid unnecessary heap allocations when inserting
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! // Install at https://surrealdb.com/install
//! // and use `surreal start --user root --pass root`
//! // to start a working database to take the following queries

//! // See the results via `surreal sql --ns namespace --db database --pretty`
//! // or https://surrealist.app/
//! // followed by the query `SELECT * FROM person;`

//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let db = Surreal::new::<Ws>("localhost:8000").await?;
//!
//!     // Signin as a namespace, database, or root user
//!     db.signin(Root {
//!         username: "root",
//!         password: "root",
//!     }).await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Option<Person> = db.create("person")
//!         .content(Person {
//!             title: "Founder & CEO".into(),
//!             name: Name {
//!                 first: "Tobie".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: true,
//!         })
//!         .await?;
//!
//!     // Create a new person with a specific ID
//!     let created: Option<Person> = db.create(("person", "jaime"))
//!         .content(Person {
//!             title: "Founder & COO".into(),
//!             name: Name {
//!                 first: "Jaime".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: false,
//!         })
//!         .await?;
//!
//!     // Update a person record with a specific ID
//!     let updated: Option<Person> = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let query = r#"
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     "#;
//!
//!     let groups = db.query(query)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(target_family = "wasm", feature = "ml"))]
compile_error!("The `ml` feature is not supported on Wasm.");

#[macro_use]
extern crate tracing;

#[doc(hidden)]
pub use surrealdb_core::*;

pub use uuid::Uuid;

#[expect(hidden_glob_reexports)]
mod api;

#[doc(hidden)]
/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::bounded;
	pub use async_channel::unbounded;
	pub use async_channel::Receiver;
	pub use async_channel::Sender;
}

/// Different error types for embedded and remote databases
pub mod error {
	pub use crate::api::err::Error as Api;
	pub use surrealdb_core::err::Error as Db;
}

#[cfg(feature = "protocol-http")]
#[doc(hidden)]
pub use crate::api::headers;

#[doc(inline)]
pub use crate::api::{
	engine, method, opt,
	value::{
		self, Action, Bytes, Datetime, Notification, Number, Object, RecordId, RecordIdKey, Value,
	},
	Connect, Connection, Response, Result, Surreal,
};

/// An error originating from the SurrealDB client library
#[derive(Debug, thiserror::Error, serde::Serialize)]
pub enum Error {
	/// An error with an embedded storage engine
	#[error("{0}")]
	Db(#[from] crate::error::Db),
	/// An error with a remote database instance
	#[error("{0}")]
	Api(#[from] crate::error::Api),
}
