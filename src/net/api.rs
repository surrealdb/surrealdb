use std::sync::Arc;

use super::AppState;
use super::error::ResponseError;
use super::params::Params;
use crate::cnf::HTTP_MAX_API_BODY_SIZE;
use crate::net::error::Error as NetError;
use axum::Extension;
use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::response::IntoResponse;
use axum::routing::any;
use http::header::CONTENT_TYPE;
use surrealdb::dbs::Session;
use surrealdb::dbs::capabilities::ExperimentalTarget;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::expr::Value;
use surrealdb::expr::statements::FindApi;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb_core::api::err::ApiError;
use surrealdb_core::api::{
	body::ApiBody, invocation::ApiInvocation, method::Method as ApiMethod, response::ResponseFormat,
};
use surrealdb_core::protocol::ToFlatbuffers;
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

	let tx = Arc::new(
		ds.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.map_err(ResponseError)?,
	);
	let apis = tx.all_db_apis(&ns, &db).await.map_err(ResponseError)?;
	let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

	let res = match apis.as_ref().find_api(segments, method) {
		Some((api, params)) => {
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
				Ok(Some(v)) => Ok(v),
				Ok(None) => Err(NetError::NotFound(url).into()),
				Err(e) => Err(ResponseError(e)),
			}
		}
		_ => Err(NetError::NotFound(url).into()),
	};

	// Handle committing or cancelling the transaction
	if res.is_ok() {
		tx.commit().await.map_err(ResponseError)?;
	} else {
		tx.cancel().await.map_err(ResponseError)?;
	}

	// Process the result
	let (mut res, res_instruction) = res?;

	let res_body: Vec<u8> = if let Some(value) = res.body {
		match res_instruction {
			ResponseFormat::Raw => {
				match value {
					Value::Strand(v) => {
						res.headers.entry(CONTENT_TYPE).or_insert("text/plain".parse().map_err(
							|_| ApiError::Unreachable("Expected a valid format".into()),
						)?);
						v.0.into_bytes()
					}
					Value::Bytes(v) => {
						res.headers.entry(CONTENT_TYPE).or_insert(
							"application/octet-stream".parse().map_err(|_| {
								ApiError::Unreachable("Expected a valid format".into())
							})?,
						);
						v.into()
					}
					v => {
						return Err(ApiError::InvalidApiResponse(format!(
							"Expected bytes or string, found {}",
							v.kindof()
						))
						.into());
					}
				}
			}
			ResponseFormat::Json => {
				if res.headers.contains_key("Content-Type") {
					return Err(ApiError::InvalidApiResponse(
						"A Content-Type header was already set while this was not expected".into(),
					)
					.into());
				}

				serde_json::to_vec(&value)
					.map_err(|e| ApiError::InvalidApiResponse(e.to_string()))?
			}
			ResponseFormat::Ipc => {
				if res.headers.contains_key("Content-Type") {
					return Err(ApiError::InvalidApiResponse(
						"A Content-Type header was already set while this was not expected".into(),
					)
					.into());
				}

				let mut builder = flatbuffers::FlatBufferBuilder::new();
				let value_fb = value.to_fb(&mut builder);
				builder.finish_minimal(value_fb);
				builder.finished_data().to_vec()
			}
		}
	} else {
		Vec::new()
	};

	Ok((res.status, res.headers, res_body))
}
