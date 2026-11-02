use anyhow::Context as _;
use axum::extract::DefaultBodyLimit;
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use http::HeaderMap;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_rpc::Token;
use surrealdb_rpc::capabilities::RouteTarget;
use surrealdb_types::{SurrealValue, Value};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::auth::{
	CookiePolicy, cookie_output, require_allowed_origin, session_scope, token_cookies,
};
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_SIGNUP_BODY_SIZE;
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

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/signup", options(|| async {}).post(handler))
		.route("/signup/cookie", options(|| async {}).post(cookie_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_SIGNUP_BODY_SIZE))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(mut session): Extension<Session>,
	scope: Option<Extension<super::scope::PathScope>>,
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
			let vars = super::scope::credentials(scope.as_deref(), vars)?;
			match surrealdb_core::iam::signup::signup(kvs, &mut session, vars).await {
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

/// `signup` that returns the access token as an `HttpOnly` session cookie instead
/// of in the body, for browser clients. Same body, capability gate and size
/// limit as `/signup`; refused with 403 unless the origin gate allows the request.
async fn cookie_handler(
	Extension(state): Extension<AppState>,
	Extension(mut session): Extension<Session>,
	scope: Option<Extension<super::scope::PathScope>>,
	Extension(policy): Extension<CookiePolicy>,
	headers: HeaderMap,
	accept: Option<TypedHeader<Accept>>,
	body: Bytes,
) -> Result<(HeaderMap, Output), ResponseError> {
	// Get a database reference
	let kvs = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !kvs.allows_http_route(&RouteTarget::Signup) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Signup);
		return Err(NetError::ForbiddenRoute(RouteTarget::Signup.to_string()).into());
	}
	// The origin gate runs before any credential is checked
	require_allowed_origin(&policy, &headers, "/signup/cookie")?;
	// Convert the HTTP body into text
	let data = bytes_to_utf8(&body).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Parse the provided data as JSON
	match syn::json(data) {
		// The provided value was an object
		Ok(Value::Object(vars)) => {
			let vars = super::scope::credentials(scope.as_deref(), vars)?;
			match surrealdb_core::iam::signup::signup(kvs, &mut session, vars).await {
				// Authentication was successful
				Ok(token) => {
					let out = token_cookies(kvs, &token, &session, session_scope(&session)).await?;
					Ok((out, cookie_output(accept.as_deref())?))
				}
				// There was an error with authentication
				Err(err) => Err(ResponseError(err)),
			}
		}
		// The provided value was not an object
		_ => Err(NetError::Request.into()),
	}
}
