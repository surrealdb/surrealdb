use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;

use serde::Serialize;
use surrealdb::dbs::Session;
use surrealdb::sql::Value;
use tower_http::limit::RequestBodyLimitLayer;

use super::headers::Accept;
use super::AppState;

const MAX: usize = 1024; // 1 KiB

#[derive(Serialize)]
struct Success {
	code: u16,
	details: String,
	token: Option<String>,
}

impl Success {
	fn new(token: Option<String>) -> Success {
		Success {
			token,
			code: 200,
			details: String::from("Authentication succeeded"),
		}
	}
}

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/signup", options(|| async {}).post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(mut session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get a database reference
	let kvs = &state.datastore;
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body)?;
	// Parse the provided data as JSON
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			match surrealdb::iam::signup::signup(kvs, &mut session, vars).await.map_err(Error::from)
			{
				// Authentication was successful
				Ok(v) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => Ok(output::json(&Success::new(v))),
					Some(Accept::ApplicationCbor) => Ok(output::cbor(&Success::new(v))),
					Some(Accept::ApplicationPack) => Ok(output::pack(&Success::new(v))),
					// Text serialization
					Some(Accept::TextPlain) => Ok(output::text(v.unwrap_or_default())),
					// Internal serialization
					Some(Accept::Surrealdb) => Ok(output::full(&Success::new(v))),
					// Return nothing
					None => Ok(output::none()),
					// An incorrect content-type was requested
					_ => Err(Error::InvalidType),
				},
				// There was an error with authentication
				Err(err) => Err(err),
			}
		}
		// The provided value was not an object
		_ => Err(Error::Request),
	}
}
