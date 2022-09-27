use std::fmt::Display;
use std::str;

use bytes::Bytes;
use serde_json::Value;
use warp::{path, Filter};

use surrealdb::sql::serde::{beg_internal_serialization, end_internal_serialization};
use surrealdb::sql::{parse, Query};
use surrealdb::Datastore;
use surrealdb::Session;

use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use crate::net::session;

const MAX: u64 = 1024 * 1024; // 1 MiB

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path!("ast" / "sql").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(session::build())
		.and(warp::header::<String>(http::header::ACCEPT.as_str()))
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and_then(handler);

	opts.or(post)
}

async fn handler(
	session: Session,
	output: String,
	sql: Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	if !session.au.is_kv() {
		return Err(warp::reject::custom(Error::Permission));
	}

	match str::from_utf8(&sql) {
		// Convert our JSON AST to a Query object
		Ok(json_ast) => match serde_json::from_str::<Query>(json_ast) {
			// Run format over it, and out put the query
			Ok(query) => {
				debug!(target: "DEBUG", "Executing: {}", query);
				Ok(output::text(format!("{}", query)))
			}
			Err(err) => Err(warp::reject::custom(Error::from(err))),
		},
		// There was an issue serialising the Query
		Err(err) => Err(warp::reject::custom(Error::from(err))),
	}
}
