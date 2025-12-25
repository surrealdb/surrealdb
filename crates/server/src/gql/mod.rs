use std::convert::Infallible;
use std::task::{Context, Poll};
use std::time::Duration;

use async_graphql::http::{create_multipart_mixed_stream, is_accept_multipart_mixed};
use async_graphql::{Executor, ParseRequestError};
use async_graphql_axum::rejection::GraphQLRejection;
use async_graphql_axum::{GraphQLBatchRequest, GraphQLRequest, GraphQLResponse};
use axum::BoxError;
use axum::body::{Body, HttpBody};
use axum::extract::FromRequest;
use axum::http::{Request as HttpRequest, Response as HttpResponse};
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use http::StatusCode;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::capabilities::RouteTarget;
use surrealdb_core::gql::cache::GraphQLSchemaCache;
use surrealdb_core::gql::error::resolver_error;
use tower_service::Service;

use crate::ntw::error::Error as NetError;

/// A GraphQL service.
#[derive(Clone)]
pub struct GraphQLService {
	cache: GraphQLSchemaCache,
}

impl GraphQLService {
	/// Create a GraphQL HTTP handler.
	pub fn new() -> Self {
		GraphQLService {
			cache: GraphQLSchemaCache::default(),
		}
	}
}

impl<B> Service<HttpRequest<B>> for GraphQLService
where
	B: HttpBody<Data = Bytes> + Send + 'static,
	B::Data: Into<Bytes>,
	B::Error: Into<BoxError>,
{
	type Response = HttpResponse<Body>;
	type Error = Infallible;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
		let cache = self.cache.clone();
		let req = req.map(Body::new);

		Box::pin(async move {
			let state = req
				.extensions()
				.get::<crate::ntw::AppState>()
				.expect("state extractor should always succeed");

			let datastore = &state.datastore;

			// Check if capabilities allow querying the requested HTTP route
			if !datastore.allows_http_route(&RouteTarget::GraphQL) {
				warn!(
					"Capabilities denied HTTP route request attempt, target: '{}'",
					&RouteTarget::GraphQL
				);
				return Ok(
					NetError::ForbiddenRoute(RouteTarget::GraphQL.to_string()).into_response()
				);
			}

			let session =
				req.extensions().get::<Session>().expect("session extractor should always succeed");

			let Some(_ns) = session.ns.as_ref() else {
				return Ok(to_rejection(resolver_error("No namespace specified")).into_response());
			};
			let Some(_db) = session.db.as_ref() else {
				return Ok(to_rejection(resolver_error("No database specified")).into_response());
			};

			let schema = match cache.get_schema(datastore, session).await {
				Ok(e) => e,
				Err(e) => {
					info!(?e, "error generating schema");
					return Ok(to_rejection(e).into_response());
				}
			};

			// Clone Arc's before moving req (needed for GraphQL context)
			let datastore_ctx = datastore.clone();
			let session_ctx = std::sync::Arc::new(session.clone());

			let is_accept_multipart_mixed = req
				.headers()
				.get("accept")
				.and_then(|value| value.to_str().ok())
				.map(is_accept_multipart_mixed)
				.unwrap_or_default();

			if is_accept_multipart_mixed {
				let gql_req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await
				{
					Ok(r) => r,
					Err(err) => return Ok(err.into_response()),
				};
				// Add Datastore and Session to the GraphQL context
				let req_with_data = gql_req.into_inner().data(datastore_ctx).data(session_ctx);
				let stream = Executor::execute_stream(&schema, req_with_data, None);
				let body = Body::from_stream(
					create_multipart_mixed_stream(stream, Duration::from_secs(30))
						.map(Ok::<_, std::io::Error>),
				);
				match HttpResponse::builder()
					.header("content-type", "multipart/mixed; boundary=graphql")
					.body(body)
				{
					Ok(r) => Ok(r),
					Err(err) => {
						let mut resp = HttpResponse::new(Body::new(err.to_string()));
						*resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
						Ok(resp)
					}
				}
			} else {
				let gql_req =
					match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
						Ok(r) => r,
						Err(err) => return Ok(err.into_response()),
					};
				// Add Datastore and Session to the GraphQL context
				let req_with_data = gql_req.into_inner().data(datastore_ctx).data(session_ctx);
				Ok(GraphQLResponse(schema.execute_batch(req_with_data).await).into_response())
			}
		})
	}
}

fn to_rejection(err: impl std::error::Error + Send + Sync + 'static) -> GraphQLRejection {
	GraphQLRejection(ParseRequestError::InvalidRequest(Box::new(err)))
}
