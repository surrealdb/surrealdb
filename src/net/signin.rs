use super::headers::Accept;
use super::AppState;
use crate::cnf::HTTP_MAX_SIGNUP_BODY_SIZE;
use crate::err::Error;
use crate::net::input::bytes_to_utf8;
use crate::net::output;
use axum::extract::DefaultBodyLimit;
use axum::response::IntoResponse;
use axum::routing::options;
use axum::Extension;
use axum::Router;
use axum_extra::TypedHeader;
use bytes::Bytes;
use serde::Serialize;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::iam::signin::signin;
use surrealdb::sql::Value;
use tower_http::limit::RequestBodyLimitLayer;

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
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Get a database reference
	let kvs = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !kvs.allows_http_route(&RouteTarget::Signin) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signin);
		return Err(Error::ForbiddenRoute(RouteTarget::Signin.to_string()));
	}
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body)?;
	// Parse the provided data as JSON
	match surrealdb::sql::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			match signin(kvs, &mut session, vars).await.map_err(Error::from) {
				// Authentication was successful
				Ok(data) => match accept.as_deref() {
					// Simple serialization
					Some(Accept::ApplicationJson) => {
						Ok(output::json(&Success::new(data.token, data.refresh)))
					}
					Some(Accept::ApplicationCbor) => {
						Ok(output::cbor(&Success::new(data.token, data.refresh)))
					}
					Some(Accept::ApplicationPack) => {
						Ok(output::pack(&Success::new(data.token, data.refresh)))
					}
					// Text serialization
					// NOTE: Only the token is returned in a plain text response.
					Some(Accept::TextPlain) => Ok(output::text(data.token)),
					// Internal serialization
					Some(Accept::Surrealdb) => {
						Ok(output::full(&Success::new(data.token, data.refresh)))
					}
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
