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
//!     let created: Vec<Person> = db.create("person")
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
#![cfg_attr(test, deny(warnings))]

#[cfg(all(target_arch = "wasm32", feature = "ml"))]
compile_error!("The `ml` feature is not supported on the `wasm32` architecture.");

#[macro_use]
extern crate tracing;

mod api;

#[cfg(feature = "protocol-http")]
#[doc(hidden)]
pub use api::headers;

#[doc(inline)]
pub use api::{
	engine, method, opt,
	value::{self, Bytes, Datetime, Number, Object, RecordId, RecordIdKey, Value},
	Connect, Connection, Response, Result, Surreal,
};

#[doc(inline)]
pub use surrealdb_core::*;

use uuid::Uuid;

#[doc(hidden)]
/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use channel::bounded;
	pub use channel::unbounded;
	pub use channel::Receiver;
	pub use channel::Sender;
}

/// Different error types for embedded and remote databases
pub mod error {
	pub use crate::api::err::Error as Api;
	pub use crate::err::Error as Db;
}

/// The action performed on a record
///
/// This is used in live query notifications.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum Action {
	Create,
	Update,
	Delete,
}

impl From<dbs::Action> for Action {
	fn from(action: dbs::Action) -> Self {
		match action {
			dbs::Action::Create => Self::Create,
			dbs::Action::Update => Self::Update,
			dbs::Action::Delete => Self::Delete,
			_ => unreachable!(),
		}
	}
}

/// A live query notification
///
/// Live queries return a stream of notifications. The notification contains an `action` that triggered the change in the database record and `data` itself.
/// For deletions the data is the record before it was deleted. For everything else, it's the newly created record or updated record depending on whether
/// the action is create or update.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub struct Notification<R> {
	pub query_id: Uuid,
	pub action: Action,
	pub data: R,
}

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
