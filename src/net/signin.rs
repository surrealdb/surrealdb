use crate::err::Error;
use bytes::Bytes;
use std::str;
use surrealdb::sql::Value;
use warp::http::Response;
use warp::Filter;

const MAX: u64 = 1024; // 1 KiB

pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("signin").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and_then(handler);
	// Specify route
	opts.or(post)
}

async fn handler(body: Bytes) -> Result<impl warp::Reply, warp::Rejection> {
	// Convert the HTTP body into text
	let data = str::from_utf8(&body).unwrap();
	// Parse the provided data as JSON
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => match crate::iam::signin::signin(vars).await {
			// Authentication was successful
			Ok(v) => Ok(Response::builder().body(v)),
			// There was an error with authentication
			Err(e) => Err(warp::reject::custom(e)),
		},
		// The provided value was not an object
		_ => Err(warp::reject::custom(Error::Request)),
	}
}
