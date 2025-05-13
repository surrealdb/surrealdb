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
use axum::response::Response;
use axum::routing::any;
use axum::Extension;
use axum::Router;
use axum_extra::either::Either;
use futures::Stream;
use futures::StreamExt;
use http::header::CONTENT_TYPE;
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use surrealdb::dbs::capabilities::ExperimentalTarget;
use surrealdb::dbs::capabilities::RouteTarget;
use surrealdb::dbs::Session;
use surrealdb::kvs::LockType;
use surrealdb::kvs::TransactionType;
use surrealdb::rpc::format::cbor;
use surrealdb::rpc::format::json;
use surrealdb::rpc::format::revision;
use surrealdb::rpc::format::Format;
use surrealdb::sql::statements::FindApi;
use surrealdb::sql::StreamVal;
use surrealdb::sql::Value;
use surrealdb_core::api::err::ApiError;
use surrealdb_core::api::{
	invocation::ApiInvocation, method::Method as ApiMethod, response::ResponseInstruction,
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
	// Update the session with the NS & DB
	let session = session.with_ns(&ns).with_db(&db);
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

	let res = if let Some((api, params)) = apis.as_ref().find_api(segments, method) {
		let invocation = ApiInvocation {
			params,
			method,
			headers,
			query: query.inner,
		};

		let stream = Box::new(
			body.into_data_stream()
				.map(|result| result.map_err(|e| Box::new(e) as Box<dyn Display + Send + Sync>)),
		);

		match invocation
			.invoke_with_transaction(tx.clone(), ds.clone(), &session, api, stream)
			.await
		{
			Ok(Some(v)) => Ok(v),
			Err(e) => Err(Error::from(e)),
			_ => Err(Error::NotFound(url)),
		}
	} else {
		Err(Error::NotFound(url))
	};

	// Handle committing or cancelling the transaction
	if res.is_ok() {
		tx.commit().await.map_err(Error::from)?;
	} else {
		tx.cancel().await.map_err(Error::from)?;
	}

	// Process the result
	let (mut res, res_instruction) = res?;

	// Obtain the body
	let res_body: Either<Vec<u8>, StreamedResponse> = if let Some(body) = res.body {
		match res_instruction {
			ResponseInstruction::Raw => match body {
				Value::Strand(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert("text/plain".parse().map_err(
						|_| Error::Api(ApiError::Unreachable("Expected a valid format".into())),
					)?);
					Either::E1(v.0.into_bytes())
				}
				Value::Bytes(v) => {
					res.headers.entry(CONTENT_TYPE).or_insert(
						"application/octet-stream".parse().map_err(|_| {
							Error::Api(ApiError::Unreachable("Expected a valid format".into()))
						})?,
					);
					Either::E1(v.into())
				}
				Value::Stream(stream) => {
					let stream = stream
						.consume()
						.map_err(|e| Error::Api(ApiError::InvalidApiResponse(e.to_string())))?;

					Either::E2(StreamedResponse::new(stream))
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
					Format::Json => ("application/json", json::res(body)?),
					Format::Cbor => ("application/cbor", cbor::res(body)?),
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
				Either::E1(val)
			}
			ResponseInstruction::Native => {
				return Err(Error::Api(ApiError::Unreachable(
					"Found a native response instruction where this is not supported".into(),
				)))
			}
		}
	} else {
		Either::E1(Vec::new())
	};

	Ok((res.status, res.headers, res_body))
}

pub struct StreamedResponse {
	inner: StreamVal,
}

impl StreamedResponse {
	pub fn new(inner: StreamVal) -> Self {
		StreamedResponse {
			inner,
		}
	}
}

impl Stream for StreamedResponse {
	type Item = Result<bytes::Bytes, axum::Error>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		match Pin::new(&mut self.inner).poll_next(cx) {
			Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
			Poll::Ready(Some(Err(e))) => {
				let error_message = format!("{}", e);
				Poll::Ready(Some(Err(axum::Error::new(error_message))))
			}
			Poll::Ready(None) => Poll::Ready(None),
			Poll::Pending => Poll::Pending,
		}
	}
}

impl IntoResponse for StreamedResponse {
	fn into_response(self) -> Response {
		Response::new(Body::from_stream(self))
	}
}
