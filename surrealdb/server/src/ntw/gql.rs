//! Experimental ISO GQL (ISO/IEC 39075) query endpoint. Accepts a raw GQL
//! query via `POST /gql` and executes it through the same session, capability,
//! and output-negotiation plumbing as the `/sql` endpoint. The language itself
//! is gated by the `gql` experimental capability at the datastore layer,
//! so requests fail with a clear error unless the server is started with
//! `--allow-experimental gql`.

use std::collections::BTreeMap;

use anyhow::Context;
use axum::extract::{DefaultBodyLimit, Query};
use axum::routing::options;
use axum::{Extension, Router};
use axum_extra::TypedHeader;
use bytes::Bytes;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_types::{Array, SurrealValue, Value, Variables};
use tower_http::limit::RequestBodyLimitLayer;

use super::AppState;
use super::error::ResponseError;
use super::headers::Accept;
use super::output::Output;
use crate::cnf::HTTP_MAX_GQL_BODY_SIZE;
use crate::ntw::error::Error as NetError;
use crate::ntw::input::bytes_to_utf8;
use crate::ntw::timeout::with_query_timeout;

pub fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/gql", options(|| async {}).post(post_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_GQL_BODY_SIZE))
}

async fn post_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	output: Option<TypedHeader<Accept>>,
	Query(params): Query<BTreeMap<String, String>>,
	gql: Bytes,
) -> Result<Output, ResponseError> {
	let vars = Variables::from(params);
	// Get a database reference
	let db = &state.datastore;
	// Check if capabilities allow querying the requested HTTP route
	if !db.allows_http_route(&RouteTarget::Gql) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Gql);
		return Err(NetError::ForbiddenRoute(RouteTarget::Gql.to_string()).into());
	}
	// Check if the user is allowed to query
	if !db.allows_query_by_subject(session.au.as_ref()) {
		return Err(NetError::ForbiddenRoute(RouteTarget::Gql.to_string()).into());
	}
	// Convert the received gql query
	let gql = bytes_to_utf8(&gql).context("Non UTF-8 request body").map_err(ResponseError)?;
	// Execute the received gql query under the configured wall-clock query
	// timeout (default off), reusing `--query-timeout` as the guard duration.
	match with_query_timeout(db.query_timeout(), db.execute_gql(gql, &session, Some(vars))).await {
		Ok(res) => match output.as_deref() {
			// Simple serialization
			None | Some(Accept::ApplicationJson) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::json_value(&v))
			}
			Some(Accept::ApplicationCbor) => {
				let v = Value::Array(Array::from(
					res.into_iter().map(|x| x.into_value()).collect::<Vec<Value>>(),
				));
				Ok(Output::cbor(v))
			}
			// Internal serialization
			Some(Accept::ApplicationFlatbuffers) => {
				let v = res.into_value();
				Ok(Output::flatbuffers(&v))
			}
			// An unsupported content-type was requested
			Some(_) => Err(NetError::InvalidType.into()),
		},
		// There was an error when executing the query
		Err(err) => Err(ResponseError(err.into())),
	}
}
