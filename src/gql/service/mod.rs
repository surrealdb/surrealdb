pub mod extract;

use async_graphql_axum;
use async_graphql_axum::*;

use std::{
	convert::Infallible,
	task::{Context, Poll},
	time::Duration,
};

use async_graphql::{
	http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
	Executor,
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
use tower_service::Service;

/// A GraphQL service.
#[derive(Clone)]
pub struct GraphQL<E> {
	executor: E,
}

impl<E> GraphQL<E> {
	/// Create a GraphQL handler.
	pub fn new(executor: E) -> Self {
		Self {
			executor,
		}
	}
}

impl<B, E> Service<HttpRequest<B>> for GraphQL<E>
where
	B: HttpBody<Data = Bytes> + Send + 'static,
	B::Data: Into<Bytes>,
	B::Error: Into<BoxError>,
	E: Executor,
{
	type Response = HttpResponse<Body>;
	type Error = Infallible;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
		let executor = self.executor.clone();
		let req = req.map(Body::new);
		Box::pin(async move {
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
				let stream = executor.execute_stream(req.0, None);
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
