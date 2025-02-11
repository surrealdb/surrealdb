use std::sync::Arc;

use super::params::Params;
use super::AppState;
use crate::cnf::HTTP_MAX_API_BODY_SIZE;
use crate::err::Error;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Extension;
use axum::Router;
use http::header::CONTENT_TYPE;
use surrealdb::dbs::capabilities::ExperimentalTarget;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::rpc::format::cbor;
use surrealdb::rpc::format::json;
use surrealdb::rpc::format::msgpack;
use surrealdb::rpc::format::revision;
use surrealdb::rpc::format::Format;
use surrealdb::sql::statements::FindApi;
use surrealdb::sql::Value;
use surrealdb_core::api::err::ApiError;
use surrealdb_core::api::{
	body::ApiBody, invocation::ApiInvocation, method::Method as ApiMethod,
	response::ResponseInstruction,
};
use tower_http::limit::RequestBodyLimitLayer;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/api/:ns/:db/*path", any(handler))
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
) -> Result<impl IntoResponse, impl IntoResponse> {
	// Format the full URL
	let url = format!("/api/{ns}/{db}/{path}");
	// Get a database reference
	let ds = &state.datastore;
	// Check if the experimental capability is enabled
	if !state.datastore.get_capabilities().allows_experimental(&ExperimentalTarget::DefineApi) {
		warn!("Experimental capability for API routes is not enabled");
		return Err(Error::NotFound(url));
	}
	// Check if capabilities allow querying the requested HTTP route
	if !ds.allows_http_route(&RouteTarget::Api) {
		warn!("Capabilities denied HTTP route request attempt, target: '{}'", &RouteTarget::Api);
		return Err(Error::ForbiddenRoute(RouteTarget::Api.to_string()));
	}

	let method = match method {
		Method::DELETE => ApiMethod::Delete,
		Method::GET => ApiMethod::Get,
		Method::PATCH => ApiMethod::Patch,
		Method::POST => ApiMethod::Post,
		Method::PUT => ApiMethod::Put,
		Method::TRACE => ApiMethod::Trace,
		_ => return Err(Error::NotFound(url)),
	};

	let tx = Arc::new(
		ds.transaction(TransactionType::Write, LockType::Optimistic).await.map_err(Error::from)?,
	);
	let apis = tx.all_db_apis(&ns, &db).await.map_err(Error::from)?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	let (mut res, res_instruction) =
		if let Some((api, params)) = apis.as_ref().find_api(segments, method) {
			let invocation = ApiInvocation {
				params,
				method,
				headers,
				query: query.inner,
			};

			match invocation
				.invoke_with_transaction(
					tx.clone(),
					ds.clone(),
					&session,
					api,
					ApiBody::from_stream(body.into_data_stream()),
				)
				.await
			{
				Ok(Some(v)) => v,
				Err(e) => return Err(Error::from(e)),
				_ => return Err(Error::NotFound(url)),
			}
		} else {
			return Err(Error::NotFound(url));
		};

	// Commit the transaction
	tx.commit().await.map_err(Error::from)?;

	let res_body: Vec<u8> = if let Some(body) = res.body {
		match res_instruction {
			ResponseInstruction::Raw => match body {
				Value::Strand(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert("text/plain".parse().map_err(
						|_| Error::Api(ApiError::Unreachable("Expected a valid format".into())),
					)?);
					v.0.into_bytes()
				}
				Value::Bytes(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert(
						"application/octet-stream".parse().map_err(|_| {
							Error::Api(ApiError::Unreachable("Expected a valid format".into()))
						})?,
					);
					v.into()
				}
				v => {
					return Err(Error::Api(ApiError::InvalidApiResponse(format!(
						"Expected bytes or string, found {}",
						v.kindof()
					))))
				}
			},
			ResponseInstruction::Format(format) => {
				if res.headers.contains_key("Content-Type") {
					return Err(Error::Api(ApiError::InvalidApiResponse(
						"A Content-Type header was already set while this was not expected".into(),
					)));
				}

				let (header, val) = match format {
					Format::Json => ("application/cbor", json::res(body)?),
					Format::Cbor => ("application/cbor", cbor::res(body)?),
					Format::Msgpack => ("application/pack", msgpack::res(body)?),
					Format::Revision => ("application/surrealdb", revision::res(body)?),
					_ => {
						return Err(Error::Api(ApiError::Unreachable(
							"Expected a valid format".into(),
						)))
					}
				};

				res.headers.insert(
					CONTENT_TYPE,
					header.parse().map_err(|_| {
						Error::Api(ApiError::Unreachable("Expected a valid format".into()))
					})?,
				);
				val
			}
			ResponseInstruction::Native => {
				return Err(Error::Api(ApiError::Unreachable(
					"Found a native response instruction where this is not supported".into(),
				)))
			}
		}
	} else {
		Vec::new()
	};

	Ok((res.status, res.headers, res_body))
}
