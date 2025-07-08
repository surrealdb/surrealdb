use surrealdb_protocol::proto::rpc::v1::{UnsetRequest, UnsetResponse};

use crate::Surreal;

use crate::api::Result;
use crate::api::method::BoxFuture;

use std::future::IntoFuture;

/// An unset future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Unset {
	pub(super) client: Surreal,
	pub(super) name: String,
}

impl Unset {}

impl IntoFuture for Unset {
	type Output = Result<UnsetResponse>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client
				.unset(UnsetRequest {
					name: self.name,
				})
				.await?;

			let response = response.into_inner();

			Ok(response)
		})
	}
}
