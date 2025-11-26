use std::fmt::Display;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{HeaderMap, Method};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::{Extension, Router};
use futures::StreamExt;
use http::header::CONTENT_TYPE;
use surrealdb_core::api::err::ApiError;
use surrealdb_core::api::response::ResponseInstruction;
use surrealdb_core::catalog::ApiMethod;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::{ExperimentalTarget, RouteTarget};
use surrealdb_core::rpc::RpcError;
use surrealdb_core::rpc::format::{Format, cbor, flatbuffers, json};
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

	let Some((mut res, res_instruction)) = res else {
		return Err(NetError::NotFound(url).into());
	};

	let res_body: Vec<u8> = if let Some(body) = res.body {
		match res_instruction {
			ResponseInstruction::Raw => match body {
				Value::String(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert(
						surrealdb_core::api::format::PLAIN
							.parse()
							.map_err(|_| ApiError::Unreachable("Expected a valid format".into()))?,
					);
					v.into_bytes()
				}
				Value::Bytes(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert(
						surrealdb_core::api::format::OCTET_STREAM
							.parse()
							.map_err(|_| ApiError::Unreachable("Expected a valid format".into()))?,
					);
					v.into_inner().to_vec()
				}
				v => {
					return Err(ApiError::InvalidApiResponse(format!(
						"Expected bytes or string, found {}",
						v.kind()
					))
					.into());
				}
			},
			ResponseInstruction::Format(format) => {
				if res.headers.contains_key("Content-Type") {
					return Err(ApiError::InvalidApiResponse(
						"A Content-Type header was already set while this was not expected".into(),
					)
					.into());
				}

				let (header, val) = match format {
					Format::Json => (
						surrealdb_core::api::format::JSON,
						json::encode(body).map_err(|_| RpcError::ParseError)?,
					),
					Format::Cbor => (
						surrealdb_core::api::format::CBOR,
						cbor::encode(body).map_err(|_| RpcError::ParseError)?,
					),
					Format::Flatbuffers => (
						surrealdb_core::api::format::FLATBUFFERS,
						flatbuffers::encode(&body).map_err(|_| RpcError::ParseError)?,
					),
					_ => return Err(ApiError::Unreachable("Expected a valid format".into()).into()),
				};

				res.headers.insert(
					CONTENT_TYPE,
					header
						.parse()
						.map_err(|_| ApiError::Unreachable("Expected a valid format".into()))?,
				);
				val
			}
			ResponseInstruction::Native => {
				return Err(ApiError::Unreachable(
					"Found a native response instruction where this is not supported".into(),
				)
				.into());
			}
		}
	} else {
		Vec::new()
	};

	Ok((res.status, res.headers, res_body))
}
