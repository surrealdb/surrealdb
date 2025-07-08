use surrealdb_protocol::proto::rpc::v1::{HealthRequest, HealthResponse};

use crate::Surreal;
use crate::api::Result;
use crate::api::method::BoxFuture;

use std::future::IntoFuture;

/// A health check future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Health {
	pub(super) client: Surreal,
}

impl Health {}

impl IntoFuture for Health
where
	Self: Send + Sync + 'static,
{
	type Output = Result<HealthResponse>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client.health(HealthRequest {}).await?;

			let response = response.into_inner();

			Ok(response)
		})
	}
}
