use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use warp::Filter;
use warp::ws::{Message, WebSocket, Ws};

use surrealdb::Session;
use surrealdb::sql::Query;

use crate::cli::CF;
use crate::dbs::DB;
use crate::err::Error;
use crate::net::output;
use crate::net::session;

const MAX: u64 = 1024 * 1024; // 1 MiB

pub fn config() -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("ast").and(warp::path::end());
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
	// Specify route
	opts.or(post)
}

async fn handler(session: Session, sql: Bytes) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	match session.au.is_kv() {
		true => {
			// Convert the received sql query
			let sql = std::str::from_utf8(&sql).unwrap();
			// Get the AST of the query
			let ast = surrealdb::sql::parse(&sql);
			// Unwrap and send the json response back
			match ast {
				Ok(parsed_query) => {
					debug!(target: "DEBUG", "Executing AST: {}", serde_json::to_string_pretty(&parsed_query).unwrap());
					Ok(output::json(&parsed_query))
				}
				Err(err) => Err(warp::reject::custom(Error::from(err))),
			}
		}
		// There was an error with permissions
		_ => Err(warp::reject::custom(Error::InvalidAuth)),
	}
}

