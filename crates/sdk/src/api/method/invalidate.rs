use surrealdb_protocol::proto::rpc::v1::{InvalidateRequest, QueryRequest};

use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use std::borrow::Cow;
use std::future::IntoFuture;

/// A session invalidate future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Invalidate {
	pub(super) client: Surreal,
}

impl IntoFuture for Invalidate {
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut client = self.client.client.clone();
			let client = &mut client;

			client.invalidate(InvalidateRequest {}).await?;
			Ok(())
		})
	}
}
