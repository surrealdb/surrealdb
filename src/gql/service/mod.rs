pub mod extract;

use async_graphql_axum::{self, GraphQLBatchRequest, GraphQLRequest, GraphQLResponse};
use extract::rejection::GraphQLRejection;
use tokio::sync::RwLock;

use std::{
	convert::Infallible,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};

use async_graphql::{
	dynamic::Schema,
	http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
	Executor,
};
use axum::{
	body::{BoxBody, HttpBody, StreamBody},
	extract::FromRequest,
	http::{Request as HttpRequest, Response as HttpResponse},
	response::IntoResponse,
	BoxError,
};
use bytes::Bytes;
use futures_util::{future::BoxFuture, StreamExt};
use tower_service::Service;

use surrealdb::dbs::Session;

use super::{
	error::{resolver_error, GqlError},
	schema::{Invalidator, SchemaCache},
};

/// A GraphQL service.
#[derive(Clone)]
pub struct GraphQL<I: Invalidator>(Arc<RwLock<SchemaCache<I>>>);

impl<I: Invalidator> GraphQL<I> {
	/// Create a GraphQL handler.
	pub fn new(invalidator: I) -> Self {
		let _ = invalidator;
		let arc = Arc::new(RwLock::new(SchemaCache::new()));
		GraphQL(arc)
	}

	async fn get_schema(&self, session: &Session) -> Result<Schema, GqlError> {
		let ns = session.ns.as_ref().expect("missing ns should have been caught");
		let db = session.db.as_ref().expect("missing db should have been caught");
		{
			let guard = self.0.read().await;
			if let Some(cand) = guard.get(ns.to_owned(), db.to_owned()) {
				if I::is_valid((ns.to_owned(), db.to_owned()), &cand.1) {
					return Ok(cand.0.clone());
				}
			}
		};

		let (schema, meta) = I::generate(session).await?;

		{
			let mut guard = self.0.write().await;
			guard.insert(ns.to_owned(), db.to_owned(), schema.clone(), meta);
		}

		return Ok(schema);
	}
}

impl<B, I> Service<HttpRequest<B>> for GraphQL<I>
where
	B: HttpBody + Send + Sync + 'static,
	B::Data: Into<Bytes>,
	B::Error: Into<BoxError>,
	I: Invalidator,
{
	type Response = HttpResponse<BoxBody>;
	type Error = Infallible;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
		let cache = self.clone();
		Box::pin(async move {
			let session =
				req.extensions().get::<Session>().expect("session extractor should always succeed");
			let Some(_ns) = session.ns.as_ref() else {
				return Ok(resolver_error("No namespace specified").into_response());
			};
			let Some(_db) = session.db.as_ref() else {
				return Ok(resolver_error("No database specified").into_response());
			};

			let executor = match cache.get_schema(session).await {
				Ok(e) => e,
				Err(e) => {
					info!("error generating schema: {e:?}");
					return Ok(e.into_response());
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
				// let stream = executor.execute_stream(req.0, None);
				let stream = Executor::execute_stream(&executor, req.0, None);
				// let stream = executor.execute_stream(req.0);
				let body = StreamBody::new(
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
					.body(body.boxed_unsync())
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
