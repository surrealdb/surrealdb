use std::{
	convert::Infallible,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};

use async_graphql::{
	http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
	Executor, ParseRequestError,
};
use async_graphql_axum::{
	rejection::GraphQLRejection, GraphQLBatchRequest, GraphQLRequest, GraphQLResponse,
};
use axum::{
	body::{Body, HttpBody},
	extract::FromRequest,
	http::{Request as HttpRequest, Response as HttpResponse},
	response::IntoResponse,
	BoxError,
};
use bytes::Bytes;
use futures_util::{future::BoxFuture, StreamExt};
use surrealdb::dbs::Session;
use surrealdb::gql::cache::{Invalidator, SchemaCache};
use surrealdb::gql::error::resolver_error;
use surrealdb::kvs::Datastore;
use tower_service::Service;

/// A GraphQL service.
#[derive(Clone)]
pub struct GraphQL<I: Invalidator> {
	cache: SchemaCache<I>,
	// datastore: Arc<Datastore>,
}

impl<I: Invalidator> GraphQL<I> {
	/// Create a GraphQL handler.
	pub fn new(invalidator: I, datastore: Arc<Datastore>) -> Self {
		let _ = invalidator;
		GraphQL {
			cache: SchemaCache::new(datastore),
			// datastore,
		}
	}
}

impl<B, I> Service<HttpRequest<B>> for GraphQL<I>
where
	B: HttpBody<Data = Bytes> + Send + 'static,
	B::Data: Into<Bytes>,
	B::Error: Into<BoxError>,
	I: Invalidator,
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
			let session =
				req.extensions().get::<Session>().expect("session extractor should always succeed");

			let Some(_ns) = session.ns.as_ref() else {
				return Ok(to_rejection(resolver_error("No namespace specified")).into_response());
			};
			let Some(_db) = session.db.as_ref() else {
				return Ok(to_rejection(resolver_error("No database specified")).into_response());
			};

			#[cfg(debug_assertions)]
			{
				let state = req
					.extensions()
					.get::<crate::net::AppState>()
					.expect("state extractor should always succeed");
				debug_assert!(Arc::ptr_eq(&state.datastore, &cache.datastore));
			}

			let executor = match cache.get_schema(session).await {
				Ok(e) => e,
				Err(e) => {
					info!(?e, "error generating schema");
					return Ok(to_rejection(e).into_response());
				}
			};

			let is_accept_multipart_mixed = req
				.headers()
				.get("accept")
				.and_then(|value| value.to_str().ok())
				.map(is_accept_multipart_mixed)
				.unwrap_or_default();

			if is_accept_multipart_mixed {
				let req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await {
					Ok(req) => req,
					Err(err) => return Ok(err.into_response()),
				};

				let stream = Executor::execute_stream(&executor, req.0, None);
				let body = Body::from_stream(
					create_multipart_mixed_stream(
						stream,
						tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
							Duration::from_secs(30),
						))
						.map(|_| ()),
					)
					.map(Ok::<_, std::io::Error>),
				);
				Ok(HttpResponse::builder()
					.header("content-type", "multipart/mixed; boundary=graphql")
					.body(body)
					.expect("BUG: invalid response"))
			} else {
				let req =
					match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
						Ok(req) => req,
						Err(err) => return Ok(err.into_response()),
					};
				Ok(GraphQLResponse(executor.execute_batch(req.0).await).into_response())
			}
		})
	}
}

fn to_rejection(err: impl std::error::Error + Send + Sync + 'static) -> GraphQLRejection {
	GraphQLRejection(ParseRequestError::InvalidRequest(Box::new(err)))
}
