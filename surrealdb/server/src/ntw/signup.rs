use anyhow::Context as _;
use axum::extract::DefaultBodyLimit;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::iam::Token;
use surrealdb_core::syn;
use surrealdb_types::{SurrealValue, Value};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::ntw::error::Error as NetError;
use crate::ntw::input::bytes_to_utf8;

#[derive(SurrealValue)]
struct Success {
	code: u16,
	details: String,
	token: Token,
}

impl Success {
	fn new(token: Token) -> Success {
		Success {
			token,
			code: 200,
			details: String::from("Authentication succeeded"),
		}
	}
}

pub fn router<S>(max_body_size: usize) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/signup", options(|| async {}).post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(max_body_size))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(mut session): Extension<Session>,
	accept: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<Output, ResponseError> {
	// Get a database reference
	let kvs = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !kvs.allows_http_route(&RouteTarget::Signup) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signup);
		return Err(NetError::ForbiddenRoute(RouteTarget::Signup.to_string()).into());
	}
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the provided data as JSON
	match syn::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			match surrealdb_core::iam::signup::signup(kvs, &mut session, vars.into()).await {
				// Authentication was successful
				Ok(token) => {
					match accept.as_deref() {
						// Simple serialization
						Some(Accept::ApplicationJson) => {
							let success = Success::new(token).into_value().into_json_value();
							Ok(Output::json_other(&success))
						}
						Some(Accept::ApplicationCbor) => {
							let success = Success::new(token).into_value();
							Ok(Output::cbor(success))
						}
						// Text serialization
						// NOTE: Only the token is returned in a plain text response.
						Some(Accept::TextPlain) => {
							let token = match token {
								Token::Access(token) => token,
								Token::WithRefresh {
									access: token,
									..
								} => token,
							};
							Ok(Output::Text(token))
						}
						// Internal serialization
						Some(Accept::ApplicationFlatbuffers) => {
							let success = Success::new(token).into_value();
							Ok(Output::flatbuffers(&success))
						}
						// Return nothing
						None => Ok(Output::None),
						// An incorrect content-type was requested
						_ => Err(NetError::InvalidType.into()),
					}
				}
				// There was an error with authentication
				Err(err) => Err(ResponseError(err)),
			}
		}
		// The provided value was not an object
		_ => Err(NetError::Request.into()),
	}
}
