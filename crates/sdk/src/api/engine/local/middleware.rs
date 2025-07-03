use std::task::{Context, Poll};

use http::HeaderValue;
use std::pin::Pin;
use surrealdb_core::dbs::SessionId;
use tower::{Layer, Service};

use dashmap::DashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct SessionManagementLayer {}

impl<S> Layer<S> for SessionManagementLayer {
	type Service = SessionManagementMiddleware<S>;

	fn layer(&self, service: S) -> Self::Service {
		SessionManagementMiddleware {
			inner: service,
		}
	}
}

#[derive(Debug, Clone)]
pub struct SessionManagementMiddleware<S> {
	inner: S,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for SessionManagementMiddleware<S>
where
	S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
	S::Future: Send + 'static,
	ReqBody: Send + 'static,
{
	type Response = S::Response;
	type Error = S::Error;
	type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
		// See: https://docs.rs/tower/latest/tower/trait.Service.html#be-careful-when-cloning-inner-services
		let clone = self.inner.clone();
		let mut inner = std::mem::replace(&mut self.inner, clone);

		Box::pin(async move {
			let session_id = req
				.headers()
				.get("session-id")
				.and_then(|id| id.to_str().ok())
				.and_then(|id| Uuid::parse_str(id).ok());
			let session_id = SessionId(session_id.unwrap_or_else(|| Uuid::new_v4()));

			req.extensions_mut().insert(session_id);

			let mut response = inner.call(req).await?;

			response
				.headers_mut()
				.insert("session-id", HeaderValue::from_str(&session_id.0.to_string()).unwrap());

			Ok(response)
		})
	}
}
