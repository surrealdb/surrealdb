use anyhow::Context as _;
use axum::extract::DefaultBodyLimit;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use serde::Serialize;
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_SIGNUP_BODY_SIZE;
use crate::core::dbs::Session;
use crate::core::dbs::capabilities::RouteTarget;
use crate::core::iam::signin::signin;
use crate::core::syn;
use crate::core::val::Value;
use crate::net::error::Error as NetError;
use crate::net::input::bytes_to_utf8;

#[derive(Serialize)]
struct Success {
	code: u16,
	details: String,
	token: Option<String>,
	refresh: Option<String>,
}

impl Success {
	fn new(token: String, refresh: Option<String>) -> Success {
		Success {
			token: Some(token),
			refresh,
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
		.route("/signin", options(|| async {}).post(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_SIGNUP_BODY_SIZE))
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
	if !kvs.allows_http_route(&RouteTarget::Signin) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signin);
		return Err(NetError::ForbiddenRoute(RouteTarget::Signin.to_string()).into());
	}
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the provided data as JSON
	match syn::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			match signin(kvs, &mut session, vars).await {
				// Authentication was successful
				Ok(v) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => {
						Ok(Output::json_other(&Success::new(v.token, v.refresh)))
					}
					Some(Accept::ApplicationCbor) => {
						Ok(Output::cbor(&Success::new(v.token, v.refresh)))
					}
					// Text serialization
					// NOTE: Only the token is returned in a plain text response.
					Some(Accept::TextPlain) => Ok(Output::Text(v.token)),
					// Internal serialization
					Some(Accept::Surrealdb) => {
						Ok(Output::bincode(&Success::new(v.token, v.refresh)))
					}
					// Return nothing
					None => Ok(Output::None),
					// An incorrect content-type was requested
					_ => Err(NetError::InvalidType.into()),
				},
				// There was an error with authentication
				Err(err) => Err(ResponseError(err)),
			}
		}
		// The provided value was not an object
		_ => Err(NetError::Request.into()),
	}
}
