use std::fmt::Display;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{HeaderMap, Method};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::{Extension, Router};
use futures::StreamExt;
use surrealdb_core::api::err::ApiError;
use surrealdb_core::catalog::ApiMethod;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::{ExperimentalTarget, RouteTarget};
use surrealdb_types::Value;
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use crate::cnf::HTTP_MAX_API_BODY_SIZE;
use crate::ntw::error::Error as NetError;
use crate::ntw::params::Params;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/api/{ns}/{db}/{*path}", any(handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_API_BODY_SIZE))
}

async fn handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((ns, db, path)): Path<(String, String, String)>,
	headers: HeaderMap,
	Query(query): Query<Params>,
	method: Method,
	body: Body,
) -> Result<impl IntoResponse, ResponseError> {
	// Format the full URL
	let url = format!("/api/{ns}/{db}/{path}");
	// Get a database reference
	let ds = &state.datastore;
	// Update the session with the NS & DB
	let session = session.with_ns(&ns).with_db(&db);
	// Check if the experimental capability is enabled
	if !state.datastore.get_capabilities().allows_experimental(&ExperimentalTarget::DefineApi) {
		warn!("Experimental capability for API routes is not enabled");
		return Err(NetError::NotFound(url).into());
	}
	// Check if capabilities allow querying the requested HTTP route
	if !ds.allows_http_route(&RouteTarget::Api) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Api);
		return Err(NetError::ForbiddenRoute(RouteTarget::Api.to_string()).into());
	}

	let method = match method {
		Method::DELETE => ApiMethod::Delete,
		Method::GET => ApiMethod::Get,
		Method::PATCH => ApiMethod::Patch,
		Method::POST => ApiMethod::Post,
		Method::PUT => ApiMethod::Put,
		Method::TRACE => ApiMethod::Trace,
		_ => return Err(NetError::NotFound(url).into()),
	};

	let res = ds
		.invoke_api_handler(
			&ns,
			&db,
			&path,
			&session,
			method,
			headers,
			query.inner.clone(),
			body.into_data_stream().map(|x| {
				x.map_err(|_| {
					Box::new(anyhow::anyhow!("Failed to get body"))
						as Box<dyn Display + Send + Sync>
				})
			}),
		)
		.await
		.map_err(ResponseError)?;

	let Some(res) = res else {
		return Err(NetError::NotFound(url).into());
	};

	let res_body = match res.body {
		Value::None => Vec::new(),
		Value::Bytes(x) => x.into_inner().to_vec(),
		_ => {
			return Err(ApiError::Unreachable(
				"Found a native response instruction where this is not supported".into(),
			)
			.into());
		}
	};

	Ok((res.status, res.headers, res_body))
}
