use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{HeaderMap, Method};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::{Extension, Router};
use surrealdb_core::api::err::ApiError;
use surrealdb_core::api::request::ApiRequest;
use surrealdb_core::catalog::ApiMethod;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_types::Value;
use tower_http::limit::RequestBodyLimitLayer;
use uuid::Uuid;

use super::AppState;
use super::error::{ApiHandlerError, ResponseError};
use crate::ntw::error::Error as NetError;
use crate::ntw::params::Params;

pub fn router<S>(max_body_size: usize) -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/api/{ns}/{db}/{*path}", any(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(max_body_size))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((ns, db, path)): Path<(String, String, String)>,
	headers: HeaderMap,
	Query(Params {
		inner: query,
	}): Query<Params>,
	method: Method,
	body: Bytes,
) -> Result<impl IntoResponse, ApiHandlerError> {
	// Generate request ID at the start so it can be passed back for ALL errors and included in
	// warns
	let request_id = Uuid::new_v4().to_string();
	trace!(
		request_id = %request_id,
		method = %method,
		path = %path,
		ns = %ns,
		db = %db,
		"API request received"
	);

	// Format the full URL
	let url = format!("/api/{ns}/{db}/{path}");
	// Get a database reference
	let ds = &state.datastore;
	// Update the session with the NS & DB
	let session = session.with_ns(&ns).with_db(&db);
	// Check if capabilities allow querying the requested HTTP route
	if !ds.allows_http_route(&RouteTarget::Api) {
		warn!(
			request_id = %request_id,
			"Capabilities denied HTTP route request attempt, target: '{}'",
			&RouteTarget::Api
		);
		return Err(ApiHandlerError(
			NetError::ForbiddenRoute(RouteTarget::Api.to_string()).into(),
			request_id,
		));
	}

	let method = match method {
		Method::DELETE => ApiMethod::Delete,
		Method::GET => ApiMethod::Get,
		Method::PATCH => ApiMethod::Patch,
		Method::POST => ApiMethod::Post,
		Method::PUT => ApiMethod::Put,
		Method::TRACE => ApiMethod::Trace,
		_ => {
			warn!(
				request_id = %request_id,
				method = %method,
				"API route does not support HTTP method"
			);
			return Err(ApiHandlerError(NetError::NotFound(url).into(), request_id));
		}
	};

	// TODO we need to get a max body size back somehow. Introduction of blob like value? This
	// stream somehow needs to be postponed... also if such a value would be introduced then this
	// whole enum can be eliminated. body would always just simply be a value maybe bytes could
	// have two variants, consumed and unconsumed. To the user its simply bytes, but whenever an api
	// request is processed, the body would be unconsumed bytes, and whenever we get a file, that
	// too could be unconsumed bytes. When the user actually does something with them, they get
	// consumed, but to the user its always simply just bytes. we could expose handlebars to
	// describe the "internal state" of the value...
	let body = Value::Bytes(body.into());

	let req = ApiRequest {
		method,
		headers,
		body,
		query,
		request_id: request_id.clone(),
		..Default::default()
	};

	debug!(
		request_id = %request_id,
		path = %path,
		"Invoking API handler"
	);
	let res = ds
		.invoke_api_handler(&ns, &db, &path, &session, req)
		.await
		.map_err(|e| ApiHandlerError(ResponseError(e), request_id.clone()))?;

	trace!(
		request_id = %request_id,
		status = %res.status,
		"API handler completed"
	);
	let res_body = match res.body {
		Value::None => Vec::new(),
		Value::Bytes(x) => x.into_inner().to_vec(),
		Value::String(s) => s.into_bytes(),
		_ => {
			return Err(ApiHandlerError(
				ApiError::InvalidApiResponse(
					"HTTP API response body must be None, bytes, or string; other values are not supported".into(),
				)
				.into(),
				request_id,
			));
		}
	};

	Ok((res.status, res.headers, res_body))
}
