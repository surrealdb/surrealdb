//! This SDK can be used as a client to connect to SurrealDB servers.
//!
//! # Example
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
//!
//! // See the results via `surreal sql --ns namespace --db database --pretty`
//! // or https://surrealist.app/
//! // followed by the query `SELECT * FROM person;`
//!
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

#[cfg(feature = "protocol-http")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-http")))]
pub mod http;

#[cfg(feature = "protocol-ws")]
#[cfg_attr(docsrs, doc(cfg(feature = "protocol-ws")))]
pub mod ws;


use surrealdb_types::{self};


// #[revisioned(revision = 1)]
// #[derive(Clone, Debug, Deserialize)]
// pub(crate) struct Failure {
// 	pub(crate) code: i64,
// 	pub(crate) message: String,
// }

// // #[revisioned(revision = 1)]
// // #[derive(Debug, Deserialize)]
// // pub(crate) enum Data {
// // 	Other(surrealdb_types::Value),
// // 	Query(Vec<dbs::QueryMethodResponse>),
// // 	Live(Notification),
// // }

// impl From<Failure> for Error {
// 	fn from(failure: Failure) -> Self {
// 		match failure.code {
// 			-32600 => Self::InvalidRequest(failure.message),
// 			-32602 => Self::InvalidParams(failure.message),
// 			-32603 => Self::InternalError(failure.message),
// 			-32700 => Self::ParseError(failure.message),
// 			_ => Self::Query(failure.message),
// 		}
// 	}
// }

// impl DbResponse {
// 	fn from_server_result(result: ServerResult) -> Result<Self> {
// 		match result.map_err(Error::from)? {
// 			DbResponse::Other(value) => Ok(DbResponse::Other(value)),
// 			DbResponse::Query(responses) => {
// 				let mut map =
// 					IndexMap::<usize, (Stats, QueryResult)>::with_capacity(responses.len());

// 				for (index, response) in responses.into_iter().enumerate() {
// 					let stats = Stats {
// 						execution_time: duration_from_str(&response.time),
// 					};
// 					match response.status {
// 						Status::Ok => {
// 							map.insert(index, (stats, Ok(response.result)));
// 						}
// 						Status::Err => {
// 							map.insert(
// 								index,
// 								(stats, Err(Error::Query(response.result.into_string()).into())),
// 							);
// 						}
// 					}
// 				}

// 				Ok(DbResponse::Query(api::Response {
// 					results: map,
// 					..api::Response::new()
// 				}))
// 			}
// 			// Live notifications don't call this method
// 			DbResponse::Live(..) => unreachable!(),
// 		}
// 	}
// }

// #[revisioned(revision = 1)]
// #[derive(Debug, Deserialize)]
// pub(crate) struct Response {
// 	id: Option<Value>,
// 	pub(crate) result: ServerResult,
// }
