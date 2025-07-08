use surrealdb_protocol::proto::rpc::v1::UnsetRequest;

use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use std::borrow::Cow;
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
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client
				.unset(UnsetRequest {
					name: self.name,
				})
				.await?;

			todo!("STUB: Unset future");
		})
	}
}
