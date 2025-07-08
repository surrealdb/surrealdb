use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use std::borrow::Cow;
use std::future::IntoFuture;
use surrealdb_core::expr::Value;
use surrealdb_protocol::proto::rpc::v1::{SetRequest, SetResponse};

/// A set future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Set {
	pub(super) client: Surreal,
	pub(super) name: String,
	pub(super) value: Value,
}

impl IntoFuture for Set {
	type Output = Result<SetResponse>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			let response = client
				.set(SetRequest {
					name: self.name,
					value: Some(self.value.try_into()?),
				})
				.await?;

			let response = response.into_inner();

			Ok(response)
		})
	}
}
