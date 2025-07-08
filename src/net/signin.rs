use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_SIGNUP_BODY_SIZE;
use crate::net::error::Error as NetError;
use crate::net::input::bytes_to_utf8;
use anyhow::Context as _;
use axum::Extension;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::options;
use axum_extra::TypedHeader;
use bytes::Bytes;
use serde::Serialize;
use surrealdb::dbs::Session;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::iam::signin::signin;
use surrealdb_core::iam::SigninParams;
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

	let v1_value =
		surrealdb_core::rpc::format::json::parse_value(&body).map_err(|err| NetError::Request)?;

	let signin_params = SigninParams::try_from(v1_value).map_err(|err| NetError::Request)?;

	let signin_data = signin(kvs, &mut session, signin_params).await.map_err(ResponseError)?;

	match accept.as_deref() {
		// Simple serialization
		Some(Accept::ApplicationJson) => {
			Ok(Output::json(&Success::new(signin_data.token, signin_data.refresh)))
		}
		Some(Accept::ApplicationCbor) => {
			Ok(Output::cbor(&Success::new(signin_data.token, signin_data.refresh)))
		}
		// Text serialization
		// NOTE: Only the token is returned in a plain text response.
		Some(Accept::TextPlain) => Ok(Output::Text(signin_data.token)),
		// Internal serialization
		Some(Accept::Surrealdb) => {
			Ok(Output::full(&Success::new(signin_data.token, signin_data.refresh)))
		}
		// Return nothing
		None => Ok(Output::None),
		// An incorrect content-type was requested
		_ => Err(NetError::InvalidType.into()),
	}
}
