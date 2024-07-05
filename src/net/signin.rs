use crate::dbs::DB;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::options;
use axum::Extension;
use axum::Router;
use axum::TypedHeader;
use bytes::Bytes;
use http_body::Body as HttpBody;
use serde::Serialize;
use surrealdb::dbs::Session;
use surrealdb::sql::Value;
use tower_http::limit::RequestBodyLimitLayer;

use super::headers::Accept;

const MAX: usize = 1024; // 1 KiB

#[derive(Serialize)]
struct Success {
	code: u16,
	details: String,
	token: Option<String>,
}

impl Success {
	fn new(token: String) -> Success {
		Success {
			token: Some(token),
			code: 200,
			details: String::from("Authentication succeeded"),
		}
	}
}

pub(super) fn router<S, B>() -> Router<S, B>
where
	B: HttpBody + Send + 'static,
	B::Data: Send,
	B::Error: std::error::Error + Send + Sync + 'static,
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/signin", options(|| async {}).post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(MAX))
}

async fn handler(
	Extension(mut session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get a database reference
	let kvs = DB.get().unwrap();
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body)?;
	// Parse the provided data as JSON
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			match surrealdb::iam::signin::signin(kvs, &mut session, vars).await.map_err(Error::from)
			{
				// Authentication was successful
				Ok(v) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => Ok(output::json(&Success::new(v))),
					Some(Accept::ApplicationCbor) => Ok(output::cbor(&Success::new(v))),
					Some(Accept::ApplicationPack) => Ok(output::pack(&Success::new(v))),
					// Text serialization
					Some(Accept::TextPlain) => Ok(output::text(v)),
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
