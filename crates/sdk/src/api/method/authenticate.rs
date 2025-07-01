use surrealdb_protocol::proto::rpc::v1::AuthenticateRequest;

use crate::Surreal;
use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::opt::auth::Jwt;
use std::future::IntoFuture;

/// An authentication future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Authenticate {
	pub(super) client: Surreal,
	pub(super) token: Jwt,
}

impl IntoFuture for Authenticate {
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(mut self) -> Self::IntoFuture {
		Box::pin(async move {
			self.client.client.authenticate(AuthenticateRequest {
				token: self.token.0,
			}).await?;
			Ok(())
		})
	}
}
