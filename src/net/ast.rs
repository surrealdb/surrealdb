use bytes::Bytes;
use warp::Filter;

use surrealdb::Session;
use surrealdb::sql::serde::{beg_internal_serialization, end_internal_serialization};

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

async fn handler(
	session: Session,
	output: String,
	sql: Bytes,
) -> Result<impl warp::Reply, warp::Rejection> {
	// Check the permissions
	match session.au.is_kv() {
		true => {
			// Convert the received sql query
			let sql = std::str::from_utf8(&sql).unwrap();
			// Get the AST of the query
			let ast = surrealdb::sql::parse(&sql);
			// Handle our error if we have one
			if ast.is_err() {
				return Err(warp::reject::custom(Error::from(ast.err().unwrap())));
			}

			// Get the underlying query AST
			let ast_result = ast.unwrap();

			// Temporarily enable/disable internal serialization, so we get full AST output
			beg_internal_serialization();

			// Get the valid serialised response output
			let response = match output.as_ref() {
				"application/json" => Ok(output::json(&ast_result)),
				"application/cbor" => Ok(output::cbor(&ast_result)),
				"application/msgpack" => Ok(output::pack(&ast_result)),
				// An incorrect content-type was requested
				_ => Err(warp::reject::custom(Error::InvalidType)),
			};

			debug!(target: "DEBUG", "Executing AST: {}", serde_json::to_string_pretty(&ast_result).unwrap());

			end_internal_serialization();

			return response;
		}
		// There was an error with permissions
		_ => Err(warp::reject::custom(Error::InvalidAuth)),
	}
}

