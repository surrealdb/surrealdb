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
//! # Examples
//!
//! ```no_run
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use std::borrow::Cow;
//! use surrealdb::{Result, Surreal};
//! use surrealdb::opt::Root;
//! use surrealdb::protocol::Ws;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: Cow<'static, str>,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let db = Surreal::connect::<Ws>("127.0.0.1:8000").await?;
//!
//!     // Signin as a namespace, database, or root user
//!     db.signin(Root {
//!         username: "root",
//!         password: "root",
//!     }).await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("test").use_db("test").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Person = db.create("person")
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
//!     let created: Person = db.create(("person", "jaime"))
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
//!     let updated: Person = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let sql = "
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     ";
//!
//!     let groups = db.query(sql)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(test, deny(warnings))]

#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cnf;
mod ctx;
mod dbs;
mod doc;
mod err;
mod exe;
mod fnc;
mod key;
mod kvs;

pub(crate) mod api;

// ENV
#[doc(hidden)] // For internal use only
pub mod env;

// SQL
pub mod sql;

// Exports
pub use api::err::Error as ApiError;
pub use api::*;
pub use err::Error as DbError;

#[doc(hidden)] // For internal use only
pub use dbs::Auth;
#[doc(hidden)] // For internal use only
pub use dbs::Response;
#[doc(hidden)] // For internal use only
pub use dbs::Session;
#[doc(hidden)] // For internal use only
pub use kvs::Datastore;
#[doc(hidden)] // For internal use only
pub use kvs::Key;
#[doc(hidden)] // For internal use only
pub use kvs::Transaction;
#[doc(hidden)] // For internal use only
pub use kvs::Val;

// Re-exports
#[doc(hidden)] // For internal use only
pub mod channel {
	pub use channel::bounded as new;
	pub use channel::Receiver;
	pub use channel::Sender;
}

/// `SurrealDB` error
#[derive(thiserror::Error, Debug)]
pub enum Error {
	/// API error
	#[error("API error: {0}")]
	Api(#[from] ApiError),

	/// Embedded database error
	#[error("Database error: {0}")]
	Db(#[from] DbError),
}
