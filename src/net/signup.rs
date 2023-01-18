use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use crate::net::session;
use bytes::Bytes;
use serde::Serialize;
use surrealdb::dbs::Session;
use surrealdb::sql::Value;
use warp::Filter;

const MAX: u64 = 1024; // 1 KiB

#[derive(Serialize)]
struct Success {
	code: u16,
	details: String,
	token: String,
}

impl Success {
	fn new(token: String) -> Success {
		Success {
			token,
			code: 200,
			details: String::from("Authentication succeeded"),
		}
	}
}

#[allow(opaque_hidden_inferred_bound)]
pub fn config() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
	// Set base path
	let base = warp::path("signup").and(warp::path::end());
	// Set opts method
	let opts = base.and(warp::options()).map(warp::reply);
	// Set post method
	let post = base
		.and(warp::post())
		.and(warp::header::optional::<String>(http::header::ACCEPT.as_str()))
		.and(warp::body::content_length_limit(MAX))
		.and(warp::body::bytes())
		.and(session::build())
		.and_then(handler);
	// Specify route
	opts.or(post)
}

async fn handler(
	output: Option<String>,
	body: Bytes,
	mut session: Session,
) -> Result<impl warp::Reply, warp::Rejection> {
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body)?;
	// Parse the provided data as JSON
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => match crate::iam::signup::signup(&mut session, vars).await {
			// Authentication was successful
			Ok(v) => match output.as_deref() {
				Some("application/json") => Ok(output::json(&Success::new(v.as_string()))),
				Some("application/cbor") => Ok(output::cbor(&Success::new(v.as_string()))),
				Some("application/msgpack") => Ok(output::pack(&Success::new(v.as_string()))),
				Some("text/plain") => Ok(output::text(v.as_string())),
				None => Ok(output::text(v.as_string())),
				// An incorrect content-type was requested
				_ => Err(warp::reject::custom(Error::InvalidType)),
			},
			// There was an error with authentication
			Err(e) => Err(warp::reject::custom(e)),
		},
		// The provided value was not an object
		_ => Err(warp::reject::custom(Error::Request)),
	}
}
