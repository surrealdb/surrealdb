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

use std::time::Duration;

use indexmap::IndexMap;
use revision::revisioned;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;

use crate::api::conn::DbResponse;
use crate::api::err::Error;
use crate::api::method::query::QueryResult;
use crate::api::{self, Result};
use crate::core::dbs::{self, Status};
use crate::core::val;
use crate::method::Stats;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

// Converts a debug representation of `std::time::Duration` back
fn duration_from_str(duration: &str) -> Option<std::time::Duration> {
	let nanos = if let Some(duration) = duration.strip_suffix("ns") {
		duration.parse().ok()?
	} else if let Some(duration) = duration.strip_suffix("µs") {
		let micros = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MICRO, 0).ok()?;
		micros.checked_mul(multiplier)?.to_u128()?
	} else if let Some(duration) = duration.strip_suffix("ms") {
		let millis = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_MILLI, 0).ok()?;
		millis.checked_mul(multiplier)?.to_u128()?
	} else {
		let duration = duration.strip_suffix('s')?;
		let secs = duration.parse::<Decimal>().ok()?;
		let multiplier = Decimal::try_new(NANOS_PER_SEC, 0).ok()?;
		secs.checked_mul(multiplier)?.to_u128()?
	};
	let secs = nanos.checked_div(NANOS_PER_SEC as u128)?;
	let nanos = nanos % (NANOS_PER_SEC as u128);
	Some(Duration::new(secs.try_into().ok()?, nanos.try_into().ok()?))
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	#[test]
	fn duration_from_str() {
		let durations = vec![
			Duration::ZERO,
			Duration::from_nanos(1),
			Duration::from_nanos(u64::MAX),
			Duration::from_micros(1),
			Duration::from_micros(u64::MAX),
			Duration::from_millis(1),
			Duration::from_millis(u64::MAX),
			Duration::from_secs(1),
			Duration::from_secs(u64::MAX),
			Duration::MAX,
		];

		for duration in durations {
			let string = format!("{duration:?}");
			let parsed = super::duration_from_str(&string)
				.unwrap_or_else(|| panic!("Duration {string} failed to parse"));
			assert_eq!(duration, parsed, "Duration {string} not parsed correctly");
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Failure {
	pub(crate) code: i64,
	pub(crate) message: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) enum Data {
	Other(val::Value),
	Query(Vec<dbs::QueryMethodResponse>),
	Live(dbs::Notification),
}

type ServerResult = std::result::Result<Data, Failure>;

impl From<Failure> for Error {
	fn from(failure: Failure) -> Self {
		match failure.code {
			-32600 => Self::InvalidRequest(failure.message),
			-32602 => Self::InvalidParams(failure.message),
			-32603 => Self::InternalError(failure.message),
			-32700 => Self::ParseError(failure.message),
			_ => Self::Query(failure.message),
		}
	}
}

impl DbResponse {
	fn from_server_result(result: ServerResult) -> Result<Self> {
		match result.map_err(Error::from)? {
			Data::Other(value) => Ok(DbResponse::Other(value)),
			Data::Query(responses) => {
				let mut map =
					IndexMap::<usize, (Stats, QueryResult)>::with_capacity(responses.len());

				for (index, response) in responses.into_iter().enumerate() {
					let stats = Stats {
						execution_time: duration_from_str(&response.time),
					};
					match response.status {
						Status::Ok => {
							map.insert(index, (stats, Ok(response.result)));
						}
						Status::Err => {
							map.insert(
								index,
								(stats, Err(Error::Query(response.result.as_raw_string()).into())),
							);
						}
					}
				}

				Ok(DbResponse::Query(api::Response {
					results: map,
					..api::Response::new()
				}))
			}
			// Live notifications don't call this method
			Data::Live(..) => unreachable!(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
	id: Option<val::Value>,
	pub(crate) result: ServerResult,
}
